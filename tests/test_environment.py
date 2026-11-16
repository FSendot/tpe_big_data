"""Controles de configuración que protegen originales y credenciales."""

import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cloud_analytics.config import load_settings
from cloud_analytics.serving import check_serving


class EnvironmentTests(unittest.TestCase):
    def settings(self, dataset, lake, evidence, partitions=2):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "runtime.toml"
            path.write_text(f'''dataset_root = "{dataset}"
lake_root = "{lake}"
evidence_root = "{evidence}"
[spark]
master = "local[2]"
driver_memory = "1g"
shuffle_partitions = {partitions}
timezone = "UTC"
''', encoding="utf-8")
            return load_settings(path)

    def test_outputs_cannot_overlap_originals(self):
        for dataset, lake in (("/data", "/data/lake"), ("/data/raw", "/data"), ("/data", "/data")):
            with self.subTest(dataset=dataset, lake=lake), self.assertRaises(ValueError):
                self.settings(dataset, lake, "/evidence")

    def test_evidence_cannot_overlap_lake(self):
        with self.assertRaises(ValueError):
            self.settings("/dataset", "/lake", "/lake/evidence")

    def test_valid_configuration(self):
        settings = self.settings("/dataset", "/lake", "/evidence")
        self.assertEqual(settings.timezone, "UTC")
        self.assertEqual(settings.shuffle_partitions, 2)

    def test_invalid_partition_count(self):
        with self.assertRaises(ValueError):
            self.settings("/dataset", "/lake", "/evidence", 0)

    def test_missing_astra_credentials_do_not_connect(self):
        with patch.dict(os.environ, {"SERVING_MODE": "astra"}, clear=True), \
                patch("cassandra.cluster.Cluster") as cluster:
            result = check_serving()
        cluster.assert_not_called()
        self.assertEqual(result["status"], "pending")

    def test_connection_errors_do_not_expose_secrets(self):
        with patch.dict(os.environ, {"SERVING_MODE": "local"}, clear=True), \
                patch("cassandra.cluster.Cluster", side_effect=RuntimeError("secret-value")):
            result = check_serving()
        self.assertEqual(result["status"], "failed")
        self.assertNotIn("secret-value", str(result))

    def test_astra_bundle_uses_writable_temporary_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            bundle = Path(directory) / "bundle.zip"
            bundle.touch()
            with patch.dict(os.environ, {"SERVING_MODE": "astra", "ASTRA_TOKEN": "test-token",
                                         "ASTRA_BUNDLE": str(bundle), "ASTRA_KEYSPACE": "test"}, clear=True), \
                    patch("cassandra.cluster.Cluster") as cluster:
                cluster.return_value.connect.return_value.execute.return_value.one.return_value.release_version = "test"
                result = check_serving()
                self.assertTrue(cluster.call_args.kwargs["cloud"]["use_default_tempdir"])
                cluster.return_value.connect.assert_called_once_with("test")
            self.assertEqual(result["status"], "passed")


if __name__ == "__main__":
    unittest.main()
