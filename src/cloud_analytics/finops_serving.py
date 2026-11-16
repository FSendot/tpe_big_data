"""Carga y lectura CQL del mart diario, acotadas a una publicación verificada."""

from contextlib import contextmanager
from datetime import date
from decimal import Decimal
import hashlib
from itertools import groupby, islice, zip_longest
import json
from pathlib import Path
import re

from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from pyspark.sql import functions as F

from cloud_analytics import gold, silver
from cloud_analytics.bronze import fingerprint


KEY = ("publication_id", "org_id", "service", "usage_month", "event_date")
MEASURES = tuple(gold.MEASURES)
COVERAGE = ("eligible", "valid", "quarantined", "absent", "valid_fraction", "state")
COVERAGE_COLUMNS = tuple(f"{metric}_coverage" for metric in MEASURES)
COLUMNS = (*KEY, "silver_publication_id", "silver_event_count", "unassigned_usage_events",
           *gold.EXCLUDED, *MEASURES, *COVERAGE_COLUMNS)
TABLE = gold.MART
CONTROL = "finops_publications"
SCHEMA = Path(__file__).resolve().parents[2] / "config" / "finops-serving.cql"
CONCURRENCY = 16
CHUNK_SIZE = 64
PARTITION_WHERE = " AND ".join(f"{name} = ?" for name in KEY[:-1])
PARTITION_QUERY = f"SELECT {', '.join(COLUMNS)} FROM {TABLE} WHERE {PARTITION_WHERE}"
RANGE_QUERY = PARTITION_QUERY + " AND event_date >= ? AND event_date <= ?"
DETAIL_QUERY = PARTITION_QUERY + " AND event_date = ?"


class ServingLoadError(ValueError):
    def __init__(self, stage, code, error_type=None):
        super().__init__(code)
        self.stage = stage
        self.error_type = error_type


@contextmanager
def step(stage):
    try:
        yield
    except ServingLoadError:
        raise
    except Exception as error:
        # El texto de las excepciones remotas puede contener endpoints.
        raise ServingLoadError(stage, "operation_failed", type(error).__name__) from None


def load_gold(spark, settings, publication_id):
    with step("verify_gold"):
        if not re.fullmatch(r"[0-9a-f]{64}", publication_id):
            raise ServingLoadError("verify_gold", "invalid_gold_publication_id")
        root = settings.lake_root / "gold" / f"publication_id={publication_id}"
        path = root / "manifest.json"
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if manifest["publication_id"] != publication_id or silver.digest(manifest["inputs"]) != publication_id:
            raise ServingLoadError("verify_gold", "gold_identity_mismatch")
        if (manifest["inputs"].get("contract") != "finops-gold-v1"
                or manifest["inputs"].get("timezone") != "UTC"
                or manifest.get("grain") != list(gold.GRAIN)
                or set(manifest["tables"]) != {TABLE}):
            raise ServingLoadError("verify_gold", "unsupported_gold_manifest")
        silver.verify_publication(root, manifest["inputs"])
        frame = (spark.read.parquet(str(root / TABLE))
                 .withColumns({"publication_id": F.lit(publication_id), "usage_month": F.trunc("event_date", "month")})
                 .select(*COLUMNS))
        counts = frame.agg(F.count("*").alias("rows"), F.countDistinct(*gold.GRAIN).alias("keys")).first()
        if counts.rows != counts.keys or counts.rows != manifest["tables"][TABLE]["rows"]:
            raise ServingLoadError("verify_gold", "gold_grain_count_mismatch")
        return frame, fingerprint(path), counts.rows


def chunks(iterator):
    while batch := list(islice(iterator, CHUNK_SIZE)):
        yield batch


def encode_row(row):
    """Spark Row -> valores tipados CQL, con el mismo orden en escritura y lectura."""
    return (*[row[name] for name in COLUMNS[:-len(COVERAGE_COLUMNS)]],
            *[tuple(row[name][field] for field in COVERAGE) for name in COVERAGE_COLUMNS])


def decode_row(row):
    values = list(row)
    for index in (3, 4):
        if not isinstance(values[index], date):
            values[index] = values[index].date()
    return tuple(values)


def spark_rows(frame):
    # Una clave de partición completa llega junta y con fechas ordenadas.
    # El driver conserva como máximo una partición Spark y usa lotes CQL acotados.
    ordered = frame.repartition(8, *KEY[:-1]).sortWithinPartitions(*KEY)
    return (encode_row(row) for row in ordered.toLocalIterator())


class CqlStore:
    def __init__(self, session, *, create_schema=False):
        self.session = session
        schema = SCHEMA.read_text(encoding="utf-8")
        self.schema_hash = hashlib.sha256(schema.encode()).hexdigest()
        if create_schema:
            with step("create_schema"):
                for query in schema.split(";"):
                    if query.strip():
                        self.session.execute(query.strip(), timeout=60)
        with step("prepare_cql"):
            self.status_query = self.prepare(
                f"SELECT source_sha256, serving_schema_sha256, expected_rows, verified_rows, state "
                f"FROM {CONTROL} WHERE publication_id = ?")
            if create_schema:
                self.insert = self.prepare(f"INSERT INTO {TABLE} ({', '.join(COLUMNS)}) "
                                           f"VALUES ({', '.join('?' for _ in COLUMNS)})")
                self.reserve_query = self.prepare(
                    f"INSERT INTO {CONTROL} (publication_id, source_sha256, serving_schema_sha256, expected_rows, state) "
                    "VALUES (?, ?, ?, ?, 'loading') IF NOT EXISTS", conditional=True)
                self.complete_query = self.prepare(
                    f"UPDATE {CONTROL} SET state = 'complete', verified_rows = ?, verified_at = toTimestamp(now()) "
                    "WHERE publication_id = ? IF state = 'loading' AND source_sha256 = ? AND serving_schema_sha256 = ?",
                    conditional=True)
                self.partition_query = self.prepare(PARTITION_QUERY)
            else:
                self.range_query = self.prepare(RANGE_QUERY)
                self.detail_query = self.prepare(DETAIL_QUERY)

    def prepare(self, query, conditional=False):
        statement = self.session.prepare(query)
        statement.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        if conditional:
            statement.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        return statement

    def status(self, publication_id):
        return self.session.execute(self.status_query, (publication_id,)).one()

    def reserve(self, publication_id, source_sha256, expected_rows):
        self.session.execute(self.reserve_query, (publication_id, source_sha256, self.schema_hash, expected_rows))

    def complete(self, publication_id, source_sha256, verified_rows):
        self.session.execute(self.complete_query, (verified_rows, publication_id, source_sha256, self.schema_hash))

    def concurrent(self, query, params):
        results = execute_concurrent_with_args(self.session, query, params, concurrency=CONCURRENCY,
                                               raise_on_first_error=False)
        for result in results:
            if not result.success:
                raise result.result_or_exc
        return (result.result_or_exc for result in results)

    def upsert(self, batch):
        self.concurrent(self.insert, batch)

    def read_partitions(self, keys):
        return self.concurrent(self.partition_query, keys)

    def read_range(self, publication_id, org_id, service, start, end):
        month = start.replace(day=1)
        while month <= end:
            yield from self.session.execute(self.range_query, (publication_id, org_id, service, month, start, end))
            if (month.year, month.month) == (end.year, end.month):
                break
            month = date(month.year + (month.month == 12), month.month % 12 + 1, 1)

    def read_day(self, publication_id, org_id, service, day):
        return self.session.execute(self.detail_query, (publication_id, org_id, service, day.replace(day=1), day))


def matched_status(store, publication_id, source_sha256, expected_rows, *, require_complete=False):
    marker = store.status(publication_id)
    if marker is None:
        raise ServingLoadError("publication_status", "publication_not_ready")
    if (marker.source_sha256, marker.serving_schema_sha256, marker.expected_rows) != (
        source_sha256, store.schema_hash, expected_rows
    ):
        raise ServingLoadError("publication_status", "publication_metadata_mismatch")
    if marker.state == "complete" and marker.verified_rows == expected_rows:
        return True
    if marker.state != "loading" or require_complete:
        raise ServingLoadError("publication_status", "publication_not_ready")
    return False


def verify_partitions(store, rows):
    partitions = ((key, tuple(group)) for key, group in groupby(rows, key=lambda row: row[:4]))
    count = 0
    partition_count = 0
    state_index = COLUMNS.index("daily_cost_usd_coverage")
    cost_index = COLUMNS.index("daily_cost_usd")
    partial = 0
    zeros = 0
    nulls = 0
    for batch in chunks(partitions):
        keys, expected = zip(*batch)
        actual = store.read_partitions(keys)
        for golden_rows, cql_rows in zip(expected, actual, strict=True):
            restored = tuple(decode_row(row) for row in cql_rows)
            if golden_rows != restored:
                raise ServingLoadError("verify_cql", "partition_readback_mismatch")
            count += len(restored)
            partition_count += 1
            for row in restored:
                partial += row[state_index][-1] == "partial"
                zeros += row[cost_index] == 0
                nulls += row[cost_index] is None
    return {"rows_verified": count, "partitions_verified": partition_count,
            "cost_partial_rows": partial, "cost_zero_rows": zeros, "cost_null_rows": nulls}


def publish(store, publication_id, source_sha256, expected_rows, make_rows):
    with step("reserve_publication"):
        store.reserve(publication_id, source_sha256, expected_rows)
        ready = matched_status(store, publication_id, source_sha256, expected_rows)
    written = 0
    if not ready:
        with step("upsert"):
            for batch in chunks(make_rows()):
                store.upsert(batch)
                written += len(batch)
            if written != expected_rows:
                raise ServingLoadError("upsert", "written_row_count_mismatch")
    with step("verify_cql"):
        verification = verify_partitions(store, make_rows())
        if verification["rows_verified"] != expected_rows:
            raise ServingLoadError("verify_cql", "verified_row_count_mismatch")
    if not ready:
        with step("promote_publication"):
            store.complete(publication_id, source_sha256, expected_rows)
            matched_status(store, publication_id, source_sha256, expected_rows, require_complete=True)
    return {"status": "reused" if ready else "loaded", "upserted_rows": written,
            "state": "complete", "verification": verification}


def sample_row(values):
    record = dict(zip(COLUMNS, values, strict=True))
    for name in COVERAGE_COLUMNS:
        record[name] = dict(zip(COVERAGE, record[name], strict=True))
    for name, value in record.items():
        if isinstance(value, (date, Decimal)):
            record[name] = str(value)
    return record


def compare_query(from_gold, from_astra):
    sample = []
    count = 0
    for expected, cql_row in zip_longest(from_gold, from_astra):
        restored = decode_row(cql_row) if cql_row is not None else None
        if expected != restored:
            raise ServingLoadError("verify_query", "query_readback_mismatch")
        if count < 7:
            sample.append(sample_row(restored))
        count += 1
    return {"rows_verified": count, "sample_rows": sample, "sample_limit": 7}


def query_finops(store, frame, publication_id, source_sha256, expected_rows,
                 org_id, service, start, end, detail):
    with step("publication_status"):
        matched_status(store, publication_id, source_sha256, expected_rows, require_complete=True)
    series = frame.where((F.col("org_id") == org_id) & (F.col("service") == service))
    with step("query_range"):
        golden_range = series.where(F.col("event_date").between(start, end)).orderBy("event_date")
        result_range = compare_query(map(encode_row, golden_range.toLocalIterator()),
                                     store.read_range(publication_id, org_id, service, start, end))
    with step("query_detail"):
        golden_day = series.where(F.col("event_date") == detail)
        result_day = compare_query(map(encode_row, golden_day.toLocalIterator()),
                                   store.read_day(publication_id, org_id, service, detail))
    return {"status": "verified", "state": "complete", "queries": {
        "range": {"cql": RANGE_QUERY, "org_id": org_id, "service": service,
                  "start": str(start), "end": str(end), **result_range},
        "detail": {"cql": DETAIL_QUERY, "org_id": org_id, "service": service,
                   "date": str(detail), **result_day},
    }}
