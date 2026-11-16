from datetime import date
from decimal import Decimal
from types import SimpleNamespace
import unittest

from cassandra.cqltypes import DecimalType
from cassandra.util import Date as CqlDate

from cloud_analytics.finops_serving import (
    COLUMNS, COVERAGE, COVERAGE_COLUMNS, MEASURES, ServingLoadError, compare_query,
    decode_row, encode_row, matched_status, publish,
)


DAY = date(2025, 7, 3)
A = "a" * 64
B = "b" * 64


def row(publication, day=DAY, cost=Decimal("12345678901234567890123456.123456789012")):
    coverage = dict(zip(COVERAGE, (3, 2, 1, 0, 2 / 3, "partial"), strict=True))
    values = dict.fromkeys(COLUMNS)
    values.update(publication_id=publication, org_id="org-test", service="compute",
                  usage_month=day.replace(day=1), event_date=day, silver_publication_id="silver-test",
                  silver_event_count=3, unassigned_usage_events=0, excluded_event_ids=0,
                  excluded_unidentified_fingerprints=0)
    values.update(dict.fromkeys(MEASURES, None))
    values.update(daily_cost_usd=cost, requests=Decimal("0"), cpu_hours=None, genai_tokens=Decimal("0"))
    values.update(dict.fromkeys(COVERAGE_COLUMNS, coverage))
    return encode_row(values)


class MemoryStore:
    """Doble de CQL para introducir fallos sin alterar Astra."""

    schema_hash = "schema-test"

    def __init__(self):
        self.markers = {}
        self.rows = {}
        self.interrupt = False

    def status(self, publication_id):
        return self.markers.get(publication_id)

    def reserve(self, publication_id, source_sha256, expected_rows):
        self.markers.setdefault(publication_id, SimpleNamespace(
            source_sha256=source_sha256, serving_schema_sha256=self.schema_hash,
            expected_rows=expected_rows, verified_rows=None, state="loading"))

    def upsert(self, batch):
        for item in batch:
            self.rows[item[:5]] = item
            if self.interrupt:
                raise TimeoutError("simulated interruption")

    def read_partitions(self, keys):
        return ([values for pk, values in sorted(self.rows.items()) if pk[:4] == key] for key in keys)

    def complete(self, publication_id, source_sha256, count):
        marker = self.markers[publication_id]
        marker.state = "complete"
        marker.verified_rows = count


class FinopsServingTests(unittest.TestCase):
    def test_partial_failure_retry_and_idempotence(self):
        store = MemoryStore()
        source = [row(A), row(A, date(2025, 7, 4), Decimal("0"))]
        store.interrupt = True
        with self.assertRaisesRegex(ServingLoadError, "operation_failed") as failed:
            publish(store, A, "source-a", 2, lambda: iter(source))
        self.assertEqual(failed.exception.stage, "upsert")
        self.assertEqual(failed.exception.error_type, "TimeoutError")
        self.assertEqual(len(store.rows), 1)
        self.assertEqual(store.status(A).state, "loading")
        with self.assertRaisesRegex(ServingLoadError, "publication_not_ready"):
            matched_status(store, A, "source-a", 2, require_complete=True)
        with self.assertRaisesRegex(ServingLoadError, "publication_metadata_mismatch"):
            publish(store, A, "different-source", 2, lambda: iter(source))
        store.interrupt = False
        first = publish(store, A, "source-a", 2, lambda: iter(source))
        again = publish(store, A, "source-a", 2, lambda: iter(source))
        self.assertEqual((first["status"], again["status"]), ("loaded", "reused"))
        self.assertEqual(again["upserted_rows"], 0)
        self.assertEqual(first["verification"], again["verification"])
        self.assertEqual(first["verification"]["cost_zero_rows"], 1)
        self.assertEqual(first["verification"]["rows_verified"], 2)
        self.assertEqual(len(store.rows), 2)

    def test_isolation_and_readback_mismatch_block_promotion(self):
        store = MemoryStore()
        first = row(A, cost=Decimal("1.234567890123"))
        second = row(B, cost=Decimal("2"))
        publish(store, A, "source-a", 1, lambda: iter([first]))
        extra = row(B, date(2025, 7, 4))
        store.rows[extra[:5]] = extra
        with self.assertRaisesRegex(ServingLoadError, "partition_readback_mismatch"):
            publish(store, B, "source-b", 1, lambda: iter([second]))
        self.assertEqual(store.status(A).state, "complete")
        self.assertEqual(store.status(B).state, "loading")
        self.assertEqual(store.rows[first[:5]], first)
        self.assertEqual(store.rows[second[:5]], second)

    def test_native_cql_decimal_dates_nulls_and_coverage(self):
        values = row(A)
        results = list(values)
        results[3:5] = [CqlDate(item) for item in values[3:5]]
        index = COLUMNS.index("daily_cost_usd")
        results[index] = DecimalType.deserialize(DecimalType.serialize(values[index], 4), 4)
        self.assertEqual(decode_row(results), values)
        report = compare_query(iter([values]), iter([results]))
        sample = report["sample_rows"][0]
        self.assertEqual(sample["daily_cost_usd"], "12345678901234567890123456.123456789012")
        self.assertEqual(sample["requests"], "0")
        self.assertIsNone(sample["cpu_hours"])
        self.assertEqual(sample["daily_cost_usd_coverage"]["state"], "partial")
        bad_row = row(A, cost=None)
        with self.assertRaisesRegex(ServingLoadError, "query_readback_mismatch"):
            compare_query(iter([values]), iter([bad_row]))
