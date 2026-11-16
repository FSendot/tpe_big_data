import csv
from datetime import date
from decimal import Decimal
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from cloud_analytics.bronze import conform_bronze, ingest_batch, inspect_csv
from cloud_analytics.config import Settings
from cloud_analytics.contracts import CONTRACTS
from cloud_analytics.spark import create_spark


def sample(contract, key="id-1"):
    values = {name: {"date": "2025-07-01", "boolean": "True", "decimal(38,12)": "1.25"}.get(dtype, "sample")
              for name, dtype in contract.fields}
    values[contract.key] = key
    return values


class BatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory()
        root = Path(cls.directory.name)
        cls.base_settings = Settings(root / "inputs", root / "lake", root / "evidence",
                                     "local[2]", "1g", 2, "UTC")
        cls.spark = create_spark(cls.base_settings)
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        cls.directory.cleanup()

    def transform(self, source, rows):
        contract = CONTRACTS[source]
        raw = self.spark.createDataFrame([tuple(row.get(name) for name in contract.names) for row in rows],
                                        contract.raw_schema)
        return conform_bronze(raw, contract)

    def test_duplicate_copies_collapse_but_conflicting_variants_survive(self):
        row = sample(CONTRACTS["customers_orgs"])
        other = dict(row, org_name="different")
        result = self.transform("customers_orgs", [row, row, other]).collect()
        self.assertEqual(len(result), 2)
        self.assertEqual(sum(r.source_occurrences for r in result), 3)
        self.assertTrue(all("conflicting_key" in r.parse_errors for r in result))

    def test_invalid_types_are_distinct_from_missing_values(self):
        row = dict(sample(CONTRACTS["users"]), active="maybe", created_at="2025-02-30", last_login=None, user_id=None)
        result = self.transform("users", [row]).first()
        self.assertIsNone(result.active)
        self.assertIsNone(result.created_at)
        self.assertIsNone(result.last_login)
        self.assertEqual(set(result.parse_errors), {"invalid_active", "invalid_created_at", "missing_key"})
        self.assertEqual(result.raw.active, "maybe")

    def test_billing_preserves_negatives_and_missing_credits(self):
        row = dict(sample(CONTRACTS["billing_monthly"]), subtotal="-12.34", credits=None, taxes="bad")
        result = self.transform("billing_monthly", [row]).first()
        self.assertEqual(result.subtotal, Decimal("-12.34"))
        self.assertIsNone(result.credits)
        self.assertIsNone(result.taxes)
        self.assertEqual(result.parse_errors, ["invalid_taxes"])
        self.assertEqual(result.month, date(2025, 7, 1))

    def test_decimal_overflow_and_excess_precision_are_flagged(self):
        row = dict(sample(CONTRACTS["billing_monthly"]), subtotal="9" * 40, taxes="0.1234567890123")
        result = self.transform("billing_monthly", [row]).first()
        self.assertIsNone(result.subtotal)
        self.assertIsNone(result.taxes)
        self.assertEqual(set(result.parse_errors), {"invalid_subtotal", "invalid_taxes"})

    def test_bad_header_and_record_width_are_blocked(self):
        contract = CONTRACTS["users"]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "users.csv"
            path.write_text("wrong,header\n1,2\n")
            with self.assertRaisesRegex(ValueError, "header_mismatch"):
                inspect_csv(path, contract)
            with path.open("w", newline="") as source:
                writer = csv.writer(source)
                writer.writerow(contract.names)
                writer.writerow(["too", "short"])
            with self.assertRaisesRegex(ValueError, "column_count"):
                inspect_csv(path, contract)

    def test_atomic_publication_retry_and_tamper_detection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            settings = Settings(root / "dataset", root / "lake", root / "evidence", "local[2]", "1g", 2, "UTC")
            landing = settings.dataset_root / "datalake" / "landing"
            landing.mkdir(parents=True)
            for name, contract in CONTRACTS.items():
                row = sample(contract)
                if name == "resources":
                    row["tags_json"] = '{"label": "comma, quote \\" and newline\\n"}'
                if name == "customers_orgs":
                    row["org_name"] = "synthetic\ncompany"
                with (landing / f"{name}.csv").open("w", newline="") as source:
                    writer = csv.DictWriter(source, fieldnames=contract.names)
                    writer.writeheader()
                    writer.writerow(row)
                    writer.writerow(row)

            # Fallo al promover: no queda un lote visible parcialmente escrito.
            with patch.object(Path, "rename", side_effect=OSError("simulated publication failure")):
                with self.assertRaises(OSError):
                    ingest_batch(self.spark, settings)
            self.assertEqual(list((settings.lake_root / "bronze").iterdir()), [])
            self.assertEqual(list((settings.lake_root / ".bronze-staging").iterdir()), [])
            first = ingest_batch(self.spark, settings)
            second = ingest_batch(self.spark, settings)
            self.assertEqual(first["status"], "created")
            self.assertEqual(second["status"], "reused")
            self.assertEqual(first["manifest"], second["manifest"])
            for source in first["manifest"]["sources"].values():
                self.assertEqual(source["input_rows"], 2)
                self.assertEqual(source["bronze_rows"], 1)
                self.assertEqual(source["duplicate_rows_removed"], 1)
            data = self.spark.read.parquet(str(Path(first["batch_path"]) / "customers_orgs")).first()
            self.assertEqual(data.raw.org_name, "synthetic\ncompany")
            self.assertEqual(data.source_occurrences, 2)
            self.assertEqual(data.contract_version, "bronze-csv-v1")
            # Un snapshot cambiado produce otro ID; no se mezcla con el anterior.
            path = landing / "users.csv"
            path.write_text(path.read_text().replace("sample", "changed"))
            third = ingest_batch(self.spark, settings)
            self.assertNotEqual(first["manifest"]["batch_id"], third["manifest"]["batch_id"])
            target = next(Path(third["batch_path"]).rglob("*.parquet"))
            with target.open("ab") as out:
                out.write(b"corrupted")
            with self.assertRaisesRegex(ValueError, "batch_checksum_mismatch"):
                ingest_batch(self.spark, settings)


if __name__ == "__main__":
    unittest.main()
