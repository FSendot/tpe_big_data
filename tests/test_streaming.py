from dataclasses import replace
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cloud_analytics.config import Settings
from cloud_analytics.spark import create_spark
from cloud_analytics.usage_contract import parse_usage
from cloud_analytics.usage_stream import StreamError, paths_for, replay, run_stream


def event(identity, timestamp, **changes):
    return {"event_id": identity, "timestamp": timestamp, "org_id": "org-test",
            "resource_id": "resource-test", "service": "compute", "region": "test",
            "metric": "requests", "value": 3, "unit": "count",
            "cost_usd_increment": 1.25, "schema_version": 1, **changes}


class StreamingTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        cls.base = Settings(root / "input", root / "lake", root / "evidence", "local[2]", "1g", 2, "UTC")
        cls.spark = create_spark(cls.base)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def settings(self, root, watermark="1 days"):
        return replace(self.base, dataset_root=root / "input", lake_root=root / "lake",
                       evidence_root=root / "evidence", watermark_delay=watermark, max_files_per_trigger=1)

    def offer(self, settings, filename, rows):
        folder = settings.dataset_root / "datalake" / "landing" / "usage_events_stream"
        folder.mkdir(parents=True, exist_ok=True)
        (folder / filename).write_text("\n".join(json.dumps(r) if isinstance(r, dict) else r for r in rows) + "\n")
        return replay(settings)

    def test_superset_and_invalid_json_preserve_original(self):
        lines = [json.dumps(event("v1", "2025-07-01T00:00:00Z", value="3")),
                 json.dumps(event("v2", "2025-07-01T02:00:00+02:00", schema_version=2,
                                  carbon_kg=0.000089, genai_tokens=12)), "broken JSON",
                 json.dumps(event("bad", "not-a-date", cost_usd_increment="invalid", schema_version=3))]
        rows = parse_usage(self.spark.createDataFrame([(line,) for line in lines], "value string")).collect()
        self.assertIsNone(rows[0].carbon_kg)
        self.assertEqual(rows[1].genai_tokens, 12)
        self.assertEqual(str(rows[1].carbon_kg), "0.000089000000")
        self.assertEqual(rows[1].parse_errors, [])
        self.assertEqual(rows[0].event_time, rows[1].event_time)
        self.assertEqual(rows[2].raw_json, "broken JSON")
        self.assertIn("malformed_json", rows[2].parse_errors)
        self.assertIn("invalid_cost_usd_increment", rows[3].parse_errors)
        self.assertIn("unknown_schema_version", rows[3].parse_errors)

    def test_restart_late_data_conflicts_and_recovery_between_queries(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = self.settings(Path(directory))
            e1 = event("first", "2025-01-01T00:00:00Z")
            self.offer(settings, "01.jsonl", [e1, e1, "malformed"])
            first = run_stream(self.spark, settings)
            self.assertEqual(first["audit"]["archive_rows"], 3)
            self.assertEqual(first["audit"]["candidate_rows"], 1)
            self.assertEqual(first["audit"]["equivalent_repeat_rows"], 1)
            restarted = run_stream(self.spark, settings)
            self.assertEqual(first["audit"], restarted["audit"])
            self.assertEqual(first["archive_query"]["query_id"], restarted["archive_query"]["query_id"])
            self.assertEqual(first["dedup_query"]["query_id"], restarted["dedup_query"]["query_id"])

            self.offer(settings, "02.jsonl", [event("advance", "2025-01-05T00:00:00Z")])
            from cloud_analytics import usage_stream
            original_drain = usage_stream.drain
            def fail_dedup(frame, destination, checkpoint):
                if checkpoint.name == "dedup":
                    raise RuntimeError("simulated failure after archive")
                return original_drain(frame, destination, checkpoint)
            with patch.object(usage_stream, "drain", side_effect=fail_dedup), self.assertRaises(RuntimeError):
                run_stream(self.spark, settings)
            recovered = run_stream(self.spark, settings)
            self.assertEqual(recovered["audit"]["archive_rows"], 4)
            self.assertEqual(recovered["audit"]["candidate_rows"], 2)
            for query in ("archive_query", "dedup_query"):
                self.assertEqual(recovered[query]["query_id"], first[query]["query_id"])
            self.assertEqual(sum(p["input_rows"] for p in recovered["archive_query"]["progress"]), 0)
            self.assertEqual(sum(p["input_rows"] for p in recovered["dedup_query"]["progress"]), 1)

            # Evento dentro del margen, copia repetida, evento demasiado viejo,
            # y una variante contradictoria de un evento ya emitido.
            within = event("within", "2025-01-04T12:00:00Z", schema_version=2, carbon_kg=0.2)
            self.offer(settings, "03.jsonl", [within, within, event("late", "2025-01-02T00:00:00Z"),
                                              event("advance", "2025-01-05T00:00:00Z", cost_usd_increment=9)])
            final = run_stream(self.spark, settings)
            self.assertEqual(final["audit"]["archive_rows"], 8)
            self.assertEqual(final["audit"]["candidate_rows"], 3)
            self.assertEqual(final["audit"]["conflicting_ids"], 1)
            self.assertEqual(final["audit"]["candidate_rows_without_conflicts"], 2)
            self.assertEqual(final["audit"]["eligible_rows_not_emitted"], 2)
            self.assertGreater(sum(state["numRowsDroppedByWatermark"] for p in final["dedup_query"]["progress"]
                                   for state in p["state"]), 0)
            paths = paths_for(settings, "usage")
            self.assertEqual(self.spark.read.parquet(str(paths["archive"])).filter("event_id = 'late'").count(), 1)
            replayed = replay(settings)
            self.assertEqual(replayed["offered_files"], 0)
            with self.assertRaisesRegex(StreamError, "checkpoint_configuration_changed"):
                run_stream(self.spark, replace(settings, watermark_delay="2 days"))

    def test_replay_detects_modified_inbox_and_invalid_namespace(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = self.settings(Path(directory))
            self.offer(settings, "01.jsonl", [event("x", "2025-01-01T00:00:00Z")])
            copied = next(paths_for(settings, "usage")["inbox"].glob("*.jsonl"))
            copied.write_text("changed")
            with self.assertRaisesRegex(StreamError, "replay_file_changed"):
                replay(settings)
            with self.assertRaisesRegex(StreamError, "invalid_stream_id"):
                replay(settings, "../escape")

    def test_all_invalid_rows_are_preserved_and_missing_checkpoint_is_blocked(self):
        with tempfile.TemporaryDirectory() as directory:
            settings = self.settings(Path(directory))
            self.offer(settings, "01.jsonl", ["malformed", event("bad", "invalid-date")])
            result = run_stream(self.spark, settings)
            self.assertEqual(result["audit"]["archive_rows"], 2)
            self.assertEqual(result["audit"]["candidate_rows"], 0)
            paths = paths_for(settings, "usage")
            checkpoint_metadata = paths["checkpoints"] / "dedup" / "metadata"
            checkpoint_metadata.rename(checkpoint_metadata.with_name("saved-metadata"))
            with self.assertRaisesRegex(StreamError, "checkpoint_missing"):
                run_stream(self.spark, settings)


if __name__ == "__main__":
    unittest.main()
