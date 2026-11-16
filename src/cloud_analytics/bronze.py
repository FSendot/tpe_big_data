"""Ingesta CSV a Bronze local: publicación de un lote completo e inmutable."""

import csv
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import tempfile

from pyspark.sql import functions as F

from cloud_analytics.contracts import CONTRACTS, CONTRACT_VERSION, contract_hash


class BronzeError(ValueError):
    """Error con código seguro: nunca incluye valores de las filas."""


def fingerprint(path):
    with path.open("rb") as source:
        return hashlib.file_digest(source, "sha256").hexdigest()


def inspect_csv(path, contract):
    """Preflight estructural: Spark puede tolerar filas cortas/largas como nulos.

    Se bloquea el archivo antes de transformar para no ocultar esa corrupción.
    Sólo guarda conteos, nunca filas ni valores en el error.
    """
    digest = fingerprint(path)
    with path.open(encoding="utf-8-sig", newline="") as source:
        reader = csv.reader(source, strict=True)
        if next(reader, None) != contract.names:
            raise BronzeError(f"header_mismatch:{path.stem}")
        rows = 0
        for row in reader:
            rows += 1
            if len(row) != len(contract.names):
                raise BronzeError(f"column_count:{path.stem}:record={rows}")
    if not rows:
        raise BronzeError(f"empty_snapshot:{path.stem}")
    if fingerprint(path) != digest:
        raise BronzeError(f"input_changed:{path.stem}")
    return {"source_file": f"datalake/landing/{path.name}", "sha256": digest,
            "bytes": path.stat().st_size, "input_rows": rows}


def read_csv(spark, path, contract):
    return (spark.read.schema(contract.raw_schema).option("header", True)
            .option("enforceSchema", False).option("mode", "FAILFAST")
            .option("multiLine", True).option("escape", '"')
            .option("ignoreLeadingWhiteSpace", False).option("ignoreTrailingWhiteSpace", False)
            .csv(str(path)))


def conform_bronze(raw, contract):
    """Deduplica sólo registros iguales; conserva variantes con la misma clave."""
    grouped = raw.groupBy(*contract.names).agg(F.count("*").alias("source_occurrences"))
    variants = (grouped.filter(F.col(contract.key).isNotNull())
                .groupBy(contract.key).agg(F.count("*").alias("key_variants")))
    frame = grouped.join(variants, contract.key, "left").fillna(0, subset=["key_variants"])
    frame = frame.withColumn("raw", F.struct(*[F.col(name) for name in contract.names]))
    frame = frame.withColumn("source_record_hash", F.sha2(F.to_json("raw", {"ignoreNullFields": "false"}), 256))
    issues = []
    for name, dtype in contract.fields:
        value = F.trim(F.col(f"raw.{name}"))
        if dtype == "string":
            # Normalización de dimensiones y JSON anidado se difiere a Silver.
            continue
        if dtype == "boolean":
            parsed = F.when(F.lower(value) == "true", F.lit(True)).when(F.lower(value) == "false", F.lit(False))
        elif dtype == "date":
            parsed = F.when(value.rlike(r"^\d{4}-\d{2}-\d{2}$"), F.expr(f"try_cast(trim(raw.`{name}`) as date)"))
        else:
            # No redondear silenciosamente más de doce decimales ni aceptar
            # notación especial. El valor original permanece disponible.
            parsed = F.when(value.rlike(r"^[+-]?\d+(\.\d{1,12})?$"),
                            F.expr(f"try_cast(trim(raw.`{name}`) as {dtype})"))
        frame = frame.withColumn(name, parsed.cast(dtype))
        issues.append(F.when(value.isNotNull() & (value != "") & F.col(name).isNull(), F.lit(f"invalid_{name}")))
    key = F.col(contract.key)
    issues.extend([
        F.when(key.isNull() | (F.trim(key) == ""), F.lit("missing_key")),
        F.when(F.col("key_variants") > 1, F.lit("conflicting_key")),
    ])
    return frame.withColumn("parse_errors", F.filter(F.array(*issues), lambda value: value.isNotNull()))


def verify_batch(batch_dir, expected_batch_id):
    manifest = json.loads((batch_dir / "manifest.json").read_text(encoding="utf-8"))
    if manifest["batch_id"] != expected_batch_id:
        raise BronzeError("batch_identity_mismatch")
    actual_files = {p.relative_to(batch_dir).as_posix() for p in batch_dir.rglob("*")
                    if p.is_file() and p.name != "manifest.json"}
    expected_files = {item["path"] for item in manifest["files"]}
    if actual_files != expected_files:
        raise BronzeError("batch_files_mismatch")
    for item in manifest["files"]:
        path = batch_dir / item["path"]
        if fingerprint(path) != item["sha256"]:
            raise BronzeError("batch_checksum_mismatch")
    return manifest


def ingest_batch(spark, settings):
    """Un solo escritor local; el lock se libera incluso si el proceso muere."""
    control = settings.lake_root / "control"
    control.mkdir(parents=True, exist_ok=True)
    with (control / "bronze.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        return _ingest_batch(spark, settings)


def _ingest_batch(spark, settings):
    paths = {name: settings.dataset_root / "datalake" / "landing" / f"{name}.csv" for name in CONTRACTS}
    inputs = {name: inspect_csv(path, CONTRACTS[name]) for name, path in paths.items()}
    identity = {"inputs": inputs, "contract_version": CONTRACT_VERSION, "contract_hash": contract_hash(),
                "implementation_hash": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
                "spark_version": spark.version}
    batch_id = hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()
    published = settings.lake_root / "bronze" / f"batch_id={batch_id}"
    if published.exists():
        manifest = verify_batch(published, batch_id)
        return {"status": "reused", "manifest": manifest, "batch_path": str(published)}

    staging_root = settings.lake_root / ".bronze-staging"
    staging_root.mkdir(parents=True, exist_ok=True)
    published.parent.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    sources = {}
    with tempfile.TemporaryDirectory(prefix="batch-", dir=staging_root) as directory:
        staging = Path(directory)
        for name, contract in CONTRACTS.items():
            frame = conform_bronze(read_csv(spark, paths[name], contract), contract)
            frame = (frame.withColumn("source_file", F.lit(inputs[name]["source_file"]))
                     .withColumn("source_hash", F.lit(inputs[name]["sha256"]))
                     .withColumn("batch_id", F.lit(batch_id))
                     .withColumn("contract_version", F.lit(CONTRACT_VERSION))
                     .withColumn("ingest_ts", F.lit(started))
                     .withColumn("ingest_date", F.lit(started.date()))).cache()
            try:
                stats = frame.agg(F.count("*").alias("rows"), F.sum("source_occurrences").alias("input_rows"),
                                  F.sum(F.when(F.size("parse_errors") > 0, 1).otherwise(0)).alias("rows_with_errors")).first()
                if stats.input_rows != inputs[name]["input_rows"]:
                    raise BronzeError(f"reader_count_mismatch:{name}")
                errors = {row.issue: row["count"] for row in
                          frame.select(F.explode("parse_errors").alias("issue")).groupBy("issue").count().collect()}
                nulls = frame.agg(*[F.sum(F.col(field).isNull().cast("long")).alias(field)
                                    for field in contract.names]).first().asDict()
                destination = staging / name
                frame.coalesce(1).write.mode("errorifexists").partitionBy("ingest_date").parquet(str(destination))
                restored = spark.read.parquet(str(destination)).select(frame.columns)
                if frame.exceptAll(restored).count() or restored.exceptAll(frame).count():
                    raise BronzeError(f"roundtrip_mismatch:{name}")
                sources[name] = {"path": name, "input_rows": stats.input_rows, "bronze_rows": stats.rows,
                                 "duplicate_rows_removed": stats.input_rows - stats.rows,
                                 "rows_with_errors": stats.rows_with_errors, "errors_by_rule": errors,
                                 "nulls_by_column": nulls, "schema": frame.schema.jsonValue()}
            finally:
                frame.unpersist()
        for name, path in paths.items():
            if fingerprint(path) != inputs[name]["sha256"]:
                raise BronzeError(f"input_changed:{name}")
        files = [{"path": p.relative_to(staging).as_posix(), "bytes": p.stat().st_size, "sha256": fingerprint(p)}
                 for p in sorted(staging.rglob("*")) if p.is_file()]
        manifest = {"batch_id": batch_id, "ingest_ts": started.isoformat(), **identity,
                    "sources": sources, "files": files}
        (staging / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
        verify_batch(staging, batch_id)
        # POSIX rename del directorio completo dentro del mismo volumen. No
        # afirmar esta garantía para HDFS u object storage sin otra validación.
        staging.rename(published)
    return {"status": "created", "manifest": manifest, "batch_path": str(published)}
