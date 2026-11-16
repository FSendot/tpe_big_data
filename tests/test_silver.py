import csv
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest.mock import patch

from cloud_analytics.bronze import ingest_batch
from cloud_analytics.config import Settings
from cloud_analytics.contracts import CONTRACTS
from cloud_analytics.silver import SilverError, inspect_cut, publish_silver
from cloud_analytics.spark import create_spark
from cloud_analytics.usage_stream import paths_for, replay, run_stream
from test_batch import sample
from test_streaming import event


class SilverTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        cls.settings = Settings(root / "input", root / "lake", root / "evidence",
                                "local[2]", "1g", 2, "UTC", "1 days", 1)
        cls.spark = create_spark(cls.settings)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def seed_batch(self, landing):
        landing.mkdir(parents=True)
        for name, contract in CONTRACTS.items():
            rows = [sample(contract)]
            if name == "customers_orgs":
                org = dict(sample(contract, "org-test"), plan_tier=" PRO ")
                conflicting = dict(org, org_id="org-conflict")
                rows = [org, dict(org, plan_tier="pro"), conflicting, dict(conflicting, plan_tier="other")]
            if name == "resources":
                resource = dict(sample(contract, "resource-test"), org_id="org-test",
                                service=" COMPUTE ", region=" TEST ")
                rows = [resource, dict(resource, resource_id="ai-test", service="genai"),
                        dict(resource, resource_id="bad-reference", org_id="org-conflict")]
            with (landing / f"{name}.csv").open("w", newline="") as file:
                writer = csv.DictWriter(file, fieldnames=contract.names)
                writer.writeheader()
                writer.writerows(rows)
        return ingest_batch(self.spark, self.settings)["manifest"]["batch_id"]

    def offer(self, landing, name, rows):
        folder = landing / "usage_events_stream"
        folder.mkdir(exist_ok=True)
        (folder / name).write_text("\n".join(json.dumps(row) if isinstance(row, dict) else row for row in rows) + "\n")
        replay(self.settings, "silver-test")

    def test_complete_archive_quality_and_atomic_publication(self):
        settings = self.settings
        landing = settings.dataset_root / "datalake" / "landing"
        batch_id = self.seed_batch(landing)
        at = "2025-08-05T00:00:00Z"
        repeated = event("repeat", at, service=" COMPUTE ", unit=" COUNT ")
        self.offer(landing, "01.jsonl", [
            repeated, repeated, event("cost-conflict", at), event("dimension-conflict", at),
            event("measure-conflict", at, value="bad-a"), event("bad-variant", at),
        ])
        run_stream(self.spark, settings, "silver-test")
        old_cut = inspect_cut(self.spark, settings, "silver-test")["cut_id"]
        self.offer(landing, "02.jsonl", [
            event("repeat", "2025-08-04T21:00:00-03:00", schema_version=2, carbon_kg=0.25),
            event("cost-conflict", at, schema_version=2, cost_usd_increment=100),
            event("dimension-conflict", at, region="elsewhere"),
            event("measure-conflict", at, value="bad-b"),
            event("bad-variant", at, schema_version=3),
            event("late", "2025-07-20T00:00:00Z", metric="cpu_hours", unit="hours"),
            event("negative", at, cost_usd_increment=-0.001, metric="storage_gb_hours", unit="gb_hours"),
            event("missing-unit", at, unit="", cost_usd_increment=9000000),
            event("wrong-unit", at, unit="hours"),
            event("optional", at, resource_id="ai-test", service="genai", schema_version=2,
                  genai_tokens=0, carbon_kg=-1),
            event("wrong-org", at, org_id="unknown"),
            event("wrong-service", at, service="storage"),
            event("unknown-resource", at, resource_id="unlisted"),
            event("missing-id", at, event_id=" "),
            "malformed JSON",
        ])
        streamed = run_stream(self.spark, settings, "silver-test")
        self.assertGreater(streamed["audit"]["eligible_rows_not_emitted"], 0)
        paths = paths_for(settings, "silver-test")
        candidates = self.spark.read.parquet(str(paths["candidates"]))
        self.assertEqual(candidates.where("event_id = 'late'").count(), 0)
        with self.assertRaisesRegex(SilverError, "archive_cut_changed"):
            publish_silver(self.spark, settings, batch_id, "silver-test", old_cut)

        # Un archivo físicamente presente que nunca entró en _spark_metadata.
        source = next(paths["archive"].rglob("*.parquet"))
        shutil.copyfile(source, source.with_name("orphan.parquet"))
        cut = inspect_cut(self.spark, settings, "silver-test")
        self.assertEqual(len(cut["files"]), 2)
        args = (self.spark, settings, batch_id, "silver-test", cut["cut_id"])
        with patch.object(Path, "rename", side_effect=OSError("simulated failure at promotion")):
            with self.assertRaisesRegex(OSError, "simulated failure"):
                publish_silver(*args)
        self.assertEqual(list((settings.lake_root / "silver").iterdir()), [])
        self.assertEqual(list((settings.lake_root / ".silver-staging").iterdir()), [])
        first = publish_silver(*args)
        repeat = publish_silver(*args)
        self.assertEqual((first["status"], repeat["status"]), ("created", "reused"))
        self.assertEqual(first["manifest"], repeat["manifest"])
        root = Path(first["publication_path"])
        events = self.spark.read.parquet(str(root / "usage_events"))
        rows = {r.event_id: r for r in events.collect()}
        self.assertEqual(events.count(), 8)
        self.assertEqual(len(rows), 8)
        self.assertEqual(rows["late"].event_date, date(2025, 7, 20))
        self.assertEqual(rows["late"].cpu_hours, 3)
        self.assertEqual(rows["repeat"].requests, 3)
        self.assertEqual(rows["repeat"].source_occurrences, 3)
        self.assertEqual(rows["repeat"].schema_versions, [1, 2])
        self.assertEqual(rows["repeat"].carbon_kg, Decimal("0.25"))
        self.assertEqual(rows["repeat"].plan_tier, "pro")
        self.assertIsNone(rows["repeat"].genai_tokens)
        self.assertEqual(rows["repeat"].tokens_status, "absent")
        self.assertIsNone(rows["cost-conflict"].cost_usd_increment)
        self.assertEqual(rows["cost-conflict"].value, 3)
        self.assertEqual(rows["measure-conflict"].cost_usd_increment, Decimal("1.25"))
        self.assertIsNone(rows["measure-conflict"].value)
        self.assertEqual(rows["negative"].storage_gb_hours, 3)
        self.assertIsNone(rows["negative"].cost_usd_increment)
        self.assertEqual(rows["missing-unit"].cost_usd_increment, 9000000)
        self.assertIsNone(rows["missing-unit"].value)
        self.assertEqual(rows["optional"].genai_tokens, 0)
        self.assertIsNone(rows["optional"].carbon_kg)

        quarantine = self.spark.read.parquet(str(root / "quarantine")).collect()
        codes = {(row.record_id, row.scope, code) for row in quarantine for code in row.reasons}
        self.assertTrue({
            ("org-conflict", "master", "conflicting_master_key"),
            ("bad-reference", "master", "unknown_org"),
            ("dimension-conflict", "event", "conflicting_event_dimensions"),
            ("bad-variant", "event", "invalid_event_variant"),
            ("wrong-org", "event", "resource_org_mismatch"),
            ("wrong-service", "event", "resource_service_mismatch"),
            ("unknown-resource", "event", "unknown_resource"),
            (None, "event", "missing_event_id"),
            (None, "event", "malformed_json"),
            ("cost-conflict", "cost", "conflicting_measure"),
            ("measure-conflict", "usage", "conflicting_measure"),
            ("negative", "cost", "negative_cost_pending"),
            ("missing-unit", "usage", "missing_unit"),
            ("wrong-unit", "usage", "unit_metric_mismatch"),
            ("optional", "carbon", "negative_carbon"),
        } <= codes)
        cost_variants = [row for row in quarantine if row.record_id == "cost-conflict"]
        self.assertEqual({json.loads(row.raw_payload)["cost_usd_increment"] for row in cost_variants}, {1.25, 100})
        self.assertEqual(len({row.source_hash for row in cost_variants}), 2)
        self.assertEqual(first["manifest"]["quality"]["archive_rows"], 21)

        with next((root / "quarantine").rglob("*.parquet")).open("ab") as file:
            file.write(b"corrupted")
        with self.assertRaisesRegex(SilverError, "publication_files_mismatch"):
            publish_silver(*args)
