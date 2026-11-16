"""Replay inmutable y dos consultas Structured Streaming con checkpoints propios."""

from contextlib import contextmanager
from datetime import datetime, timezone
import fcntl
import hashlib
import json
from pathlib import Path
import re
import shutil
import tempfile
import time
from uuid import uuid4

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from cloud_analytics.bronze import fingerprint
from cloud_analytics import usage_contract
from cloud_analytics.usage_contract import eligible, parse_usage


class StreamError(ValueError):
    """Código de diagnóstico sin valores de eventos ni secretos."""


def paths_for(settings, stream_id):
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", stream_id):
        raise StreamError("invalid_stream_id")
    root = settings.lake_root / "streams" / stream_id
    return {"root": root, "inbox": root / "inbox", "checkpoints": root / "checkpoints",
            "archive": settings.lake_root / "bronze" / "usage_events" / f"stream_id={stream_id}",
            "candidates": settings.lake_root / "bronze" / "usage_candidates" / f"stream_id={stream_id}"}


@contextmanager
def lock_at(path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        yield


def replay(settings, stream_id="usage", limit=None, interval=0):
    if (limit is not None and limit < 1) or interval < 0:
        raise StreamError("invalid_replay_parameters")
    paths = paths_for(settings, stream_id)
    paths["inbox"].mkdir(parents=True, exist_ok=True)
    sources = sorted((settings.dataset_root / "datalake" / "landing" / "usage_events_stream").glob("*.jsonl"))
    if not sources:
        raise StreamError("missing_jsonl_files")
    offered, reused = [], 0
    with lock_at(paths["root"] / "replay.lock"):
        for source in sources:
            digest = fingerprint(source)
            target = paths["inbox"] / f"{digest}__{source.name}"
            if target.exists():
                if fingerprint(target) != digest:
                    raise StreamError("replay_file_changed")
                reused += 1
                continue
            if limit is not None and len(offered) >= limit:
                continue
            if offered and interval:
                time.sleep(interval)
            # Sólo aparecen archivos cerrados en el directorio observado.
            with tempfile.TemporaryDirectory(prefix="replay-", dir=paths["root"]) as directory:
                staged = Path(directory) / source.name
                shutil.copyfile(source, staged)
                if fingerprint(staged) != digest or fingerprint(source) != digest:
                    raise StreamError("source_changed_during_replay")
                staged.rename(target)
            offered.append({"source_file": source.name, "sha256": digest, "bytes": target.stat().st_size})
    return {"offered_files": len(offered), "already_offered_files": reused, "files": offered,
            "interval_seconds": interval, "event_order_modified": False}


def archive_frame(spark, settings, paths):
    lines = (spark.readStream.schema(StructType([StructField("value", StringType())]))
             .option("maxFilesPerTrigger", settings.max_files_per_trigger)
             .option("pathGlobFilter", "*.jsonl").text(str(paths["inbox"])))
    name = F.regexp_extract(F.input_file_name(), r"([^/]+)$", 1)
    return (parse_usage(lines).withColumn("source_record_hash", F.sha2("raw_json", 256))
            .withColumn("stream_id", F.lit(paths["root"].name))
            .withColumn("source_hash", F.substring(name, 1, 64))
            .withColumn("source_file", F.concat(F.lit("datalake/landing/usage_events_stream/"), F.substring(name, 67, 1000))))


def drain(frame, destination, checkpoint):
    query = (frame.coalesce(1).writeStream.format("parquet").outputMode("append")
             .partitionBy("ingest_date").option("path", str(destination))
             .option("checkpointLocation", str(checkpoint)).trigger(availableNow=True).start())
    try:
        query.awaitTermination()
        progress = [{"batch_id": p["batchId"], "input_rows": p["numInputRows"],
                     "event_time": p.get("eventTime", {}),
                     "state": [{k: state.get(k, 0) for k in ("numRowsTotal", "numRowsUpdated", "numRowsDroppedByWatermark")}
                               for state in p.get("stateOperators", [])]} for p in query.recentProgress]
        return {"query_id": str(query.id), "progress": progress}
    finally:
        if query.isActive:
            query.stop()


def audit(spark, paths, schema):
    archive = spark.read.schema(schema).parquet(str(paths["archive"])).cache()
    candidates = spark.read.schema(schema).parquet(str(paths["candidates"])).cache()
    try:
        identities = (eligible(archive).groupBy("event_id")
                      .agg(F.countDistinct("content_hash").alias("variants"), F.count("*").alias("copies")))
        conflicts = identities.filter("variants > 1").select("event_id")
        # La vista de candidatos puede haber emitido un ID antes de descubrir
        # su conflicto. Ningún consumidor debe usarla sin esta exclusión.
        safe = candidates.join(conflicts, "event_id", "left_anti")
        emitted_keys = candidates.select("event_id", "content_hash").distinct()
        deferred = eligible(archive).join(emitted_keys, ["event_id", "content_hash"], "left_anti")
        errors = {row.issue: row["count"] for row in archive.select(F.explode("parse_errors").alias("issue"))
                  .groupBy("issue").count().collect()}
        report = {"archive_rows": archive.count(), "candidate_rows": candidates.count(),
                  "candidate_rows_without_conflicts": safe.count(), "conflicting_ids": conflicts.count(),
                  "equivalent_repeat_rows": identities.select(F.sum(F.col("copies") - F.col("variants"))).first()[0] or 0,
                  "eligible_rows_not_emitted": deferred.count(), "errors_by_rule": errors,
                  "schema_versions": {str(row.schema_version): row["count"] for row in archive.groupBy("schema_version").count().collect()}}
        audit_root = paths["root"] / "audits"
        audit_root.mkdir(exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="audit-", dir=paths["root"]) as directory:
            stage = Path(directory)
            conflicts.coalesce(1).write.parquet(str(stage / "conflicts"))
            deferred.coalesce(1).write.parquet(str(stage / "deferred"))
            (stage / "summary.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
            target = audit_root / str(uuid4())
            stage.rename(target)
        pointer = paths["root"] / "audit-head.tmp"
        pointer.write_text(json.dumps({"path": str(target)}), encoding="utf-8")
        pointer.replace(paths["root"] / "audit-head.json")
        return report
    finally:
        archive.unpersist()
        candidates.unpersist()


def run_stream(spark, settings, stream_id="usage"):
    paths = paths_for(settings, stream_id)
    with lock_at(paths["root"] / "consumer.lock"), lock_at(paths["root"] / "replay.lock"):
        paths["inbox"].mkdir(exist_ok=True)
        config = {"spark": spark.version, "watermark": settings.watermark_delay,
                  "shuffle_partitions": settings.shuffle_partitions,
                  "contract": usage_contract.VERSION,
                  "implementation": hashlib.sha256(Path(__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest(),
                  "parser": hashlib.sha256(Path(usage_contract.__file__).read_bytes().replace(b"\r\n", b"\n")).hexdigest()}
        identity = paths["root"] / "identity.json"
        if identity.exists():
            if json.loads(identity.read_text()) != config:
                raise StreamError("checkpoint_configuration_changed_use_new_stream_id")
        else:
            if paths["archive"].exists() or paths["candidates"].exists() or paths["checkpoints"].exists():
                raise StreamError("stream_identity_missing")
            identity.write_text(json.dumps(config, indent=2), encoding="utf-8")
        for output, checkpoint in (("archive", "archive"), ("candidates", "dedup")):
            if paths[output].exists() and not (paths["checkpoints"] / checkpoint / "metadata").exists():
                raise StreamError("checkpoint_missing_do_not_append")
        # Detectar modificaciones de archivos ya ofrecidos, incluso al reiniciar.
        manifest = []
        for path in sorted(paths["inbox"].glob("*.jsonl")):
            if fingerprint(path) != path.name[:64]:
                raise StreamError("inbox_file_changed")
            manifest.append({"name": path.name[66:], "sha256": path.name[:64], "bytes": path.stat().st_size})
        if not manifest:
            raise StreamError("empty_inbox_run_replay_first")
        # Un corte previo no acredita los nuevos archivos que vamos a procesar.
        # Si falla una etapa, no queda una auditoría antigua marcada como vigente.
        (paths["root"] / "audit-head.json").unlink(missing_ok=True)
        # Retener hasta 1000 micro-lotes en la evidencia de esta ejecución.
        spark.conf.set("spark.sql.streaming.numRecentProgressUpdates", "1000")
        archive = archive_frame(spark, settings, paths)
        archive_progress = drain(archive, paths["archive"], paths["checkpoints"] / "archive")
        source = (spark.readStream.schema(archive.schema).option("maxFilesPerTrigger", settings.max_files_per_trigger)
                  .parquet(str(paths["archive"])))
        dedup = eligible(source).withWatermark("event_time", settings.watermark_delay).dropDuplicatesWithinWatermark(["event_id"])
        dedup_progress = drain(dedup, paths["candidates"], paths["checkpoints"] / "dedup")
        return {"status": "passed", "stream_id": stream_id, "configuration": config,
                "inputs": manifest, "archive_query": archive_progress, "dedup_query": dedup_progress,
                "audit": audit(spark, paths, archive.schema), "checked_at": datetime.now(timezone.utc).isoformat()}
