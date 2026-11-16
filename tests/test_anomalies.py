from dataclasses import replace
from datetime import date, timedelta
from decimal import Decimal
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import functions as F

from cloud_analytics import gold, silver
from cloud_analytics.anomalies import (
    AnomalyError, EXCLUSION_COUNTS, MART, Parameters, evaluate, publish_anomalies,
    score_observation, source_limitation,
)
from cloud_analytics.config import Settings
from cloud_analytics.spark import create_spark


DAY = date(2025, 7, 3)
SCHEMA = """org_id string, event_date date, service string, daily_cost_usd decimal(38,12),
            daily_cost_usd_coverage struct<state:string>"""


def point(offset, cost, state="complete", org="org-test", service="compute"):
    return org, DAY + timedelta(days=offset), service, Decimal(cost) if cost is not None else None, (state,)


class AnomalyTests(unittest.TestCase):
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

    def test_peak_uses_only_prior_complete_observations_in_calendar_interval(self):
        rows = [
            point(0, "800"), point(2, "0"), point(3, "10"), point(4, "999", "partial"),
            point(6, "12"), point(7, "14"), point(8, None, "unavailable"), point(9, "100"),
            point(10, "10000"), point(11, "10000"), point(9, "10000", org="other"),
            point(8, "10000", service="genai"),
        ]
        data = self.spark.createDataFrame(rows, SCHEMA)
        params = Parameters(window_days=7, min_observations=4)
        full = evaluate(data, params)
        series = full.where("org_id = 'org-test' AND service = 'compute'")
        by_offset = {(row.event_date - DAY).days: row for row in series.collect()}
        peak = by_offset[9]
        self.assertEqual(len(by_offset), 10)
        self.assertNotIn(5, by_offset)
        self.assertEqual(peak.history_observations, 4)
        self.assertTrue(peak.is_anomaly)
        self.assertEqual(peak.historical_median_usd, 11)
        self.assertEqual(peak.historical_mad_usd, 2)
        self.assertAlmostEqual(peak.upper_threshold_usd, 11 + 3.5 * 2 / .6745)
        self.assertAlmostEqual(peak.robust_score, (100 - 11) * .6745 / 2)
        self.assertEqual(by_offset[2].daily_cost_usd, 0)
        self.assertEqual(by_offset[2].evaluation_reason, "insufficient_history")
        self.assertEqual(by_offset[2].history_observations, 1)
        self.assertIsNone(by_offset[2].is_anomaly)
        for offset in (4, 8):
            self.assertEqual(by_offset[offset].evaluation_reason, "incomplete_cost_coverage")
            self.assertIsNone(by_offset[offset].is_anomaly)
        self.assertEqual(by_offset[4].daily_cost_usd, 999)
        cutoff = DAY + timedelta(days=9)
        truncated = evaluate(data.where(F.col("event_date") <= F.lit(cutoff)), params)
        self.assertEqual(full.where(F.col("event_date") <= F.lit(cutoff)).exceptAll(truncated).count(), 0)

    def test_zero_mad_no_lower_alarm_and_exact_threshold(self):
        rows = [point(i, "10") for i in range(3)] + [point(3, "100")]
        rows += [point(i, value, service="genai") for i, value in enumerate(("10", "11", "12", "0"))]
        scored = evaluate(self.spark.createDataFrame(rows, SCHEMA), Parameters(7, 3))
        result = {r.service: r for r in scored.where(F.col("event_date") == F.lit(DAY + timedelta(days=3))).collect()}
        zero = result["compute"]
        self.assertEqual(zero.evaluation_reason, "zero_historical_mad")
        self.assertEqual(zero.historical_median_usd, 10)
        self.assertEqual(zero.historical_mad_usd, 0)
        self.assertIsNone(zero.is_anomaly)
        self.assertIsNone(zero.robust_score)
        self.assertIsNone(zero.upper_threshold_usd)
        self.assertFalse(result["genai"].is_anomaly)
        self.assertLess(result["genai"].robust_score, 0)
        decimals = [Decimal(n) for n in ("10", "11", "12")]
        exactly_at_threshold = score_observation(Decimal("12"), decimals, None, 3, Decimal("0.6745"))
        self.assertEqual(exactly_at_threshold[:2], ("at_or_below_threshold", False))

    def seed_gold(self, settings, rows, unlocated=False):
        identity = {"contract": "finops-gold-v1", "timezone": "UTC", "test": self._testMethodName}
        publication_id = silver.digest(identity)
        source = settings.lake_root / "gold" / f"publication_id={publication_id}"
        self.spark.createDataFrame(rows, SCHEMA).coalesce(1).write.parquet(str(source / gold.MART))
        exclusions = dict.fromkeys(EXCLUSION_COUNTS, 0)
        exclusions.update(has_unlocated_exclusions=unlocated, unlocated_event_ids=int(unlocated))
        manifest = {"publication_id": publication_id, "inputs": identity, "grain": list(gold.GRAIN),
                    "tables": {gold.MART: {"rows": len(rows)}}, "files": silver.publication_files(source),
                    "quality": {"exclusions": exclusions}}
        (source / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
        return publication_id, source

    def test_publication_reuse_and_gold_integrity(self):
        settings = replace(self.base, lake_root=self.base.lake_root / "reuse")
        rows = [point(i, cost) for i, cost in enumerate(("10", "11", "12", "100"))]
        gold_id, source = self.seed_gold(settings, rows)
        manifest_path = source / "manifest.json"
        source_manifest = manifest_path.read_bytes()
        (source.parent / f"publication_id={'f' * 64}").mkdir()
        parameters = Parameters(7, 3)
        first = publish_anomalies(self.spark, settings, gold_id, parameters)
        repeat = publish_anomalies(self.spark, settings, gold_id, parameters)
        self.assertEqual((first["status"], repeat["status"]), ("created", "reused"))
        self.assertEqual(first["manifest"], repeat["manifest"])
        self.assertEqual(first["manifest"]["quality"]["evaluated_rows"], 1)
        self.assertEqual(first["manifest"]["quality"]["not_evaluable_rows"], 3)
        self.assertEqual(first["manifest"]["quality"]["anomalies"], 1)
        self.assertEqual(manifest_path.read_bytes(), source_manifest)
        table = Path(first["publication_path"]) / MART
        flagged = self.spark.read.parquet(str(table)).where("is_anomaly").first()
        self.assertEqual(flagged.gold_publication_id, gold_id)
        self.assertEqual(flagged.daily_cost_usd, 100)
        self.assertEqual(flagged.parameters.min_observations, 3)
        different = publish_anomalies(self.spark, settings, gold_id, Parameters(7, 3, 100))
        self.assertNotEqual(different["manifest"]["publication_id"], first["manifest"]["publication_id"])
        self.assertEqual(different["manifest"]["quality"]["anomalies"], 0)
        with next((source / gold.MART).rglob("*.parquet")).open("ab") as file:
            file.write(b"corrupted")
        with self.assertRaisesRegex(silver.SilverError, "publication_files_mismatch"):
            publish_anomalies(self.spark, settings, gold_id, parameters)

    def test_unlocated_exclusions_block_the_cut(self):
        settings = replace(self.base, lake_root=self.base.lake_root / "unlocated")
        rows = [point(i, cost) for i, cost in enumerate(("10", "11", "12", "100"))]
        source, _ = self.seed_gold(settings, rows, unlocated=True)
        result = publish_anomalies(self.spark, settings, source, Parameters(7, 3))
        quality = result["manifest"]["quality"]
        self.assertEqual(quality["evaluated_rows"], 0)
        self.assertEqual(quality["not_evaluable_rows"], 4)
        self.assertEqual(quality["origin_quality_limitation"], "unlocated_gold_exclusions")
        self.assertEqual(quality["by_reason"][0]["evaluation_reason"], "unlocated_gold_exclusions")
        mart = self.spark.read.parquet(str(Path(result["publication_path"]) / MART))
        self.assertEqual(mart.where("is_anomaly IS NOT NULL OR history_observations <> 0").count(), 0)
        self.assertEqual(source_limitation({}), "gold_exclusion_metadata_unverified")

    def test_parameter_validation(self):
        with self.assertRaisesRegex(AnomalyError, "invalid_history_parameters"):
            Parameters(3, 4)
        with self.assertRaisesRegex(AnomalyError, "invalid_score_threshold"):
            Parameters(score_threshold=float("nan"))
