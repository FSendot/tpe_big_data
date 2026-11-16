from copy import deepcopy
from pathlib import Path
import tempfile
import unittest

from cloud_analytics.config import load_settings
from scripts.validate_integration import IntegrationError, compare_retry, validation_config


class IntegrationTests(unittest.TestCase):
    def test_isolated_config_preserves_source_and_runtime(self):
        base = Path("config/runtime.toml")
        contents = base.read_bytes()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = root / "validation.toml"
            validation_config(base, root / "lake", root / "job-reports", config)
            self.assertEqual(load_settings(config).lake_root, root / "lake")
            self.assertEqual(load_settings(config).evidence_root, root / "job-reports")
            original = load_settings(base)
            isolated = load_settings(config)
            for name in ("dataset_root", "master", "driver_memory", "timezone",
                         "shuffle_partitions", "watermark_delay", "max_files_per_trigger"):
                self.assertEqual(getattr(original, name), getattr(isolated, name))
        self.assertEqual(base.read_bytes(), contents)

    def test_retry_requires_identical_results_and_no_new_work(self):
        stable = {"validation_id": "validation", "ids": {"gold": "gold-id"},
                  "verified": {"query_ids": {"archive_query": "archive-id"}, "astra_query_results_sha256": "result"}}
        first = {**stable, "status": "passed"}
        retry = {**deepcopy(stable), "activity": {
            "batch": "reused", "silver": "reused", "gold": "reused", "anomalies": "reused", "astra": "reused",
            "new_jsonl": 0, "archive_input_rows": 0, "dedup_input_rows": 0, "astra_upserted_rows": 0,
        }}
        compare_retry(first, retry)
        retry["verified"]["astra_query_results_sha256"] = "different"
        with self.assertRaisesRegex(IntegrationError, "retry_results_changed"):
            compare_retry(first, retry)
        retry["verified"]["astra_query_results_sha256"] = "result"
        retry["activity"]["dedup_input_rows"] = 1
        with self.assertRaisesRegex(IntegrationError, "retry_reprocessed_input"):
            compare_retry(first, retry)
