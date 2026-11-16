"""Corte explícito de Bronze y publicación conjunta de Silver y quarantine."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile
from urllib.parse import unquote, urlsplit

from pyspark.sql import functions as F

from cloud_analytics import silver_quality as quality
from cloud_analytics.bronze import fingerprint, verify_batch
from cloud_analytics.usage_stream import lock_at, paths_for


class SilverError(ValueError):
    """Código de diagnóstico apto para la evidencia agregada."""


def digest(identity):
    return hashlib.sha256(json.dumps(identity, sort_keys=True).encode()).hexdigest()


def file_entry(root, path):
    return {"path": path.relative_to(root).as_posix(), "bytes": path.stat().st_size,
            "sha256": fingerprint(path)}


def archive_snapshot(spark, paths, stream_id):
    root = paths["archive"].resolve()
    if not (root / "_spark_metadata").is_dir():
        raise SilverError("missing_archive_metadata")
    # La raíz activa MetadataLogFileIndex. Un listado por glob podría incluir
    # archivos de un intento de micro-lote que nunca se comprometió.
    archive = spark.read.parquet(str(root))
    files = []
    for url in sorted(archive.inputFiles()):
        location = urlsplit(url)
        path = Path(unquote(location.path)).resolve()
        if location.scheme != "file" or not path.is_relative_to(root) or path.suffix != ".parquet":
            raise SilverError("invalid_archive_file")
        files.append(file_entry(root, path))
    if not files:
        raise SilverError("empty_archive_cut")
    inventory = {"stream_id": stream_id, "files": files}
    return archive, {**inventory, "cut_id": digest(inventory)}


def inspect_cut(spark, settings, stream_id):
    paths = paths_for(settings, stream_id)
    with lock_at(paths["root"] / "consumer.lock"):
        return archive_snapshot(spark, paths, stream_id)[1]


def publication_files(root):
    return [file_entry(root, path) for path in sorted(root.rglob("*"))
            if path.is_file() and path != root / "manifest.json"]


def verify_publication(root, identity):
    manifest = json.loads((root / "manifest.json").read_text(encoding="utf-8"))
    if manifest["publication_id"] != digest(identity) or manifest["inputs"] != identity:
        raise SilverError("publication_identity_mismatch")
    if not all((root / name / "_SUCCESS").is_file() for name in manifest["tables"]):
        raise SilverError("incomplete_publication")
    if publication_files(root) != manifest["files"]:
        raise SilverError("publication_files_mismatch")
    return manifest


def write_table(spark, stage, name, frame, partition=None):
    frame = frame.cache()
    try:
        rows = frame.count()
        writer = frame.coalesce(2).write.mode("errorifexists")
        if rows and partition:
            writer = writer.partitionBy(partition)
        target = stage / name
        writer.parquet(str(target))
        restored = spark.read.parquet(str(target))
        if restored.count() != rows or dict(restored.dtypes) != dict(frame.dtypes):
            raise SilverError("parquet_roundtrip_mismatch")
        return restored, {"rows": rows, "partition_by": partition if rows else None}
    finally:
        frame.unpersist()


def distribution(frame, *fields):
    return [row.asDict() for row in frame.groupBy(*fields).count().orderBy(*fields).collect()]


def build_tables(spark, stage, batch, archive):
    orgs, resources, master_quarantine = quality.conform_masters(
        spark.read.parquet(str(batch / "customers_orgs")), spark.read.parquet(str(batch / "resources")))
    tables = {}
    # Usar las dimensiones ya verificadas en la etapa de enriquecimiento.
    orgs, tables["customers_orgs"] = write_table(spark, stage, "customers_orgs", orgs)
    resources, tables["resources"] = write_table(spark, stage, "resources", resources)
    variants = quality.event_variants(archive, orgs, resources).cache()
    summary = quality.summarize_events(variants).cache()
    try:
        events, tables["usage_events"] = write_table(
            spark, stage, "usage_events", quality.silver_events(summary), "event_date")
        quarantine, tables["quarantine"] = write_table(
            spark, stage, "quarantine", master_quarantine.unionByName(
                quality.event_incidents(variants, summary)), "ingest_date")
        flags = [F.col(f"{measure}_status") for measure in quality.MEASURES]
        counts = events.agg(
            F.count(F.when(F.array_contains(F.array(*flags), "quarantined"), 1)).alias("silver_events_with_incidence"),
            F.count(F.when(~F.array_contains(F.array(*flags), "valid"), 1)).alias("silver_events_without_usable_measure"),
            F.coalesce(F.sum("source_occurrences"), F.lit(0)).alias("bronze_rows_represented_in_silver")).first().asDict()
        stats = {
            "archive_rows": variants.count(),
            **counts,
            "quarantine_by_scope": distribution(quarantine, "source", "scope"),
            "quarantine_by_reason": distribution(
                quarantine.select("source", "scope", F.explode("reasons").alias("reason")),
                "source", "scope", "reason"),
            "quarantine_reason_combinations": distribution(quarantine, "source", "scope", "reasons"),
            "measure_status": {measure: distribution(events, f"{measure}_status") for measure in quality.MEASURES},
            "features_available": events.agg(*[F.count(name).alias(name) for name in
                                               ("requests", "cpu_hours", "storage_gb_hours")]).first().asDict(),
        }
        return tables, stats
    finally:
        summary.unpersist()
        variants.unpersist()


def publish_silver(spark, settings, batch_id, stream_id, cut_id):
    if not all(re.fullmatch(r"[0-9a-f]{64}", value) for value in (batch_id, cut_id)):
        raise SilverError("invalid_input_id")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise SilverError("silver_requires_utc")
    paths = paths_for(settings, stream_id)
    with lock_at(settings.lake_root / "control" / "silver.lock"), lock_at(paths["root"] / "consumer.lock"):
        batch = settings.lake_root / "bronze" / f"batch_id={batch_id}"
        verify_batch(batch, batch_id)
        archive, cut = archive_snapshot(spark, paths, stream_id)
        if cut["cut_id"] != cut_id:
            raise SilverError("archive_cut_changed")
        identity = {
            "contract": "silver-v1", "batch_id": batch_id,
            "batch_manifest_sha256": fingerprint(batch / "manifest.json"),
            "archive": cut, "spark_version": spark.version, "timezone": "UTC",
            "implementation_sha256": {
                path.name: hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                for path in (Path(__file__), Path(quality.__file__))},
        }
        publication_id = digest(identity)
        target = settings.lake_root / "silver" / f"publication_id={publication_id}"
        if target.exists():
            return {"status": "reused", "publication_path": str(target),
                    "manifest": verify_publication(target, identity)}
        staging = settings.lake_root / ".silver-staging"
        staging.mkdir(parents=True, exist_ok=True)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="publication-", dir=staging) as directory:
            stage = Path(directory)
            tables, stats = build_tables(spark, stage, batch, archive)
            manifest = {"publication_id": publication_id, "inputs": identity,
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "tables": tables, "quality": stats, "files": publication_files(stage)}
            (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            verify_publication(stage, identity)
            stage.rename(target)
    return {"status": "created", "publication_path": str(target), "manifest": manifest}
