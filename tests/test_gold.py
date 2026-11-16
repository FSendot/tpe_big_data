from dataclasses import replace
from datetime import date
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from cloud_analytics import silver
from cloud_analytics.config import Settings
from cloud_analytics.gold import GoldError, MART, publish_gold
from cloud_analytics.spark import create_spark


EVENT_SCHEMA = """event_id string, org_id string, event_date date, service string,
    cost_usd_increment decimal(38,12), cost_status string, metric string, usage_status string,
    requests decimal(38,12), cpu_hours decimal(38,12), storage_gb_hours decimal(38,12),
    carbon_kg decimal(38,12), carbon_status string, genai_tokens bigint, tokens_status string,
    schema_versions array<int>"""
QUARANTINE_SCHEMA = "source string, scope string, record_id string, raw_payload string, source_record_hash string, reasons array<string>"
DAY = date(2025, 8, 5)


def usage(event_id, cost="0", metric="requests", units="0", carbon=None, tokens=None, service="compute", day=DAY):
    contributions = {metric: Decimal(units)} if metric else {}
    return (event_id, "org-test", day, service, Decimal(cost) if cost is not None else None,
            "valid" if cost is not None else "quarantined", metric, "valid" if metric else "quarantined",
            contributions.get("requests"), contributions.get("cpu_hours"), contributions.get("storage_gb_hours"),
            Decimal(carbon) if carbon is not None else None, "valid" if carbon is not None else "absent",
            tokens, "valid" if tokens is not None else "absent", [1] if carbon is None and tokens is None else [2])


def incidence(scope, event_id, at="2025-08-05T00:00:00Z", **changes):
    payload = json.dumps({"event_id": event_id, "timestamp": at, "org_id": " org-test ",
                          "service": " COMPUTE ", "resource_id": "resource-test", **changes})
    reasons = ["missing_unit"] if scope == "usage" else ["conflicting_event_dimensions"]
    return "usage_events", scope, event_id, payload, None, reasons


class GoldTests(unittest.TestCase):
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

    def stage_silver(self, settings, events, quarantine):
        identity = {"contract": "silver-v1", "timezone": "UTC", "test": self._testMethodName}
        publication_id = silver.digest(identity)
        root = settings.lake_root / "silver" / f"publication_id={publication_id}"
        tables = {
            "usage_events": (events, EVENT_SCHEMA),
            "quarantine": (quarantine, QUARANTINE_SCHEMA),
            "customers_orgs": ([("org-test",)], "org_id string"),
            "resources": ([("resource-test", "org-test", "compute"), ("ai-test", "org-test", "genai")],
                          "resource_id string, org_id string, service string"),
        }
        for name, (rows, schema) in tables.items():
            self.spark.createDataFrame(rows, schema).coalesce(1).write.parquet(str(root / name))
        manifest = {"publication_id": publication_id, "inputs": identity,
                    "tables": {name: {"rows": len(rows)} for name, (rows, _) in tables.items()},
                    "files": silver.publication_files(root)}
        (root / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return publication_id, root

    def test_finops_coverage_conflicts_and_reuse(self):
        settings = replace(self.base, lake_root=Path(self.directory.name) / "coverage")
        maximum_long = 9223372036854775807
        events = [
            usage("zero"),
            usage("cost-only", "2.500000000001", metric=None),
            usage("cpu", None, "cpu_hours", "4", "1.25"),
            usage("ambiguous", "1", metric=None),
            usage("storage", metric="storage_gb_hours", units="7"),
            usage("ai1", metric="cpu_hours", tokens=maximum_long, service="genai"),
            usage("ai2", metric="cpu_hours", tokens=maximum_long, service="genai"),
        ]
        conflict_a = incidence("event", "x", "2025-08-04T21:00:00-03:00")
        conflict_b = incidence("event", "x", "2025-08-06T00:00:00Z")
        anonymous = incidence("event", None)
        quarantine = [
            incidence("usage", "cost-only", metric=" REQUESTS "),
            incidence("usage", "cost-only", metric="requests"),
            incidence("usage", "ambiguous", metric="cpu_hours"),
            incidence("usage", "ambiguous", metric="requests"),
            conflict_a, conflict_a, conflict_b, conflict_b,
            incidence("event", "x", "broken-date"),
            incidence("event", "y", org_id="not-confirmed"),
            anonymous, anonymous,
            ("usage_events", "event", None, "broken JSON", "raw-hash", ["malformed_json"]),
            incidence("cost", "cpu"),
        ]
        source_id, source = self.stage_silver(settings, events, quarantine)
        (source.parent / f"publication_id={'f' * 64}").mkdir()
        first = publish_gold(self.spark, settings, source_id)
        repeat = publish_gold(self.spark, settings, source_id)
        self.assertEqual((first["status"], repeat["status"]), ("created", "reused"))
        self.assertEqual(first["manifest"], repeat["manifest"])
        manifest = first["manifest"]
        self.assertEqual(manifest["inputs"]["silver_publication_id"], source_id)
        self.assertEqual(manifest["tables"][MART], {"rows": 3, "partition_by": "event_date"})
        quality = manifest["quality"]
        self.assertEqual(quality["groups_only_in_quarantine"], 1)
        self.assertEqual(quality["groups_with_exclusions"], 2)
        self.assertEqual(quality["exclusions"]["excluded_event_ids"], 2)
        self.assertEqual(quality["exclusions"]["unlocated_event_ids"], 2)
        self.assertEqual(quality["exclusions"]["unlocated_unidentified_fingerprints"], 1)
        self.assertTrue(quality["exclusions"]["has_unlocated_exclusions"])
        table = Path(first["publication_path"]) / MART
        rows = {(r.event_date, r.service): r for r in self.spark.read.parquet(str(table)).collect()}
        compute = rows[DAY, "compute"]
        self.assertEqual(compute.silver_publication_id, source_id)
        self.assertEqual(compute.silver_event_count, 5)
        self.assertEqual(compute.excluded_event_ids, 1)
        self.assertEqual(compute.excluded_unidentified_fingerprints, 1)
        self.assertEqual(compute.unassigned_usage_events, 1)
        self.assertEqual(compute.daily_cost_usd, Decimal("3.500000000001"))
        self.assertEqual(compute.daily_cost_usd_coverage.quarantined, 1)
        self.assertEqual(compute.requests, 0)
        self.assertEqual(compute.requests_coverage.asDict(), {
            "eligible": 2, "valid": 1, "quarantined": 1, "absent": 0, "valid_fraction": .5, "state": "partial",
        })
        self.assertEqual(compute.cpu_hours, 4)
        self.assertEqual(compute.cpu_hours_coverage.eligible, 1)
        self.assertEqual(compute.storage_gb_hours, 7)
        self.assertEqual(compute.carbon_kg, Decimal("1.25"))
        self.assertEqual(compute.carbon_kg_coverage.absent, 4)
        self.assertEqual(compute.carbon_kg_coverage.state, "partial")
        exclusion_only = rows[date(2025, 8, 6), "compute"]
        self.assertEqual(exclusion_only.excluded_event_ids, 1)
        self.assertEqual(exclusion_only.silver_event_count, 0)
        self.assertEqual(exclusion_only.daily_cost_usd_coverage.state, "unavailable")
        self.assertIsNone(exclusion_only.daily_cost_usd)
        genai = rows[DAY, "genai"]
        self.assertEqual(genai.daily_cost_usd, 0)
        self.assertEqual(genai.daily_cost_usd_coverage.state, "complete")
        self.assertEqual(genai.genai_tokens, Decimal("18446744073709551614"))
        self.assertEqual(genai.carbon_kg_coverage.state, "absent")
        self.assertEqual(genai.carbon_kg_coverage.absent, 2)
        self.assertIsNone(genai.carbon_kg)
        self.assertEqual(genai.requests_coverage.state, "not_applicable")
        self.assertIsNone(genai.requests_coverage.valid_fraction)
        self.assertIsNone(genai.requests)

        source_file = next((source / "usage_events").glob("*.parquet"))
        original = source_file.read_bytes()
        source_file.write_bytes(original + b"corrupt")
        with self.assertRaisesRegex(silver.SilverError, "publication_files_mismatch"):
            publish_gold(self.spark, settings, source_id)
        source_file.write_bytes(original)
        with next(table.rglob("*.parquet")).open("ab") as file:
            file.write(b"corrupt")
        with self.assertRaisesRegex(silver.SilverError, "publication_files_mismatch"):
            publish_gold(self.spark, settings, source_id)

    def test_decimal_group_overflow_blocks_publication(self):
        settings = replace(self.base, lake_root=Path(self.directory.name) / "overflow")
        amount = "99999999999999999999999999.999999999999"
        source_id, _ = self.stage_silver(settings, [usage("max", amount), usage("extra", "0.000000000001")], [])
        with self.assertRaisesRegex(GoldError, "group_sum_overflow:daily_cost_usd"):
            publish_gold(self.spark, settings, source_id)
        self.assertEqual(list((settings.lake_root / "gold").iterdir()), [])
        self.assertEqual(list((settings.lake_root / ".gold-staging").iterdir()), [])
