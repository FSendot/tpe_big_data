"""Comprueba entorno, acceso al dataset y round-trip Parquet con datos sintéticos."""

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.metadata
import json
from pathlib import Path
import platform
import sys
from uuid import uuid4

from cloud_analytics.config import load_settings
from cloud_analytics.serving import check_serving
from cloud_analytics.spark import create_spark


def check_dataset(root):
    landing = root / "datalake" / "landing"
    names = ["customers_orgs", "users", "resources", "billing_monthly",
             "support_tickets", "marketing_touches", "nps_surveys"]
    files = [landing / f"{name}.csv" for name in names]
    events = sorted((landing / "usage_events_stream").glob("*.jsonl"))
    if any(not path.is_file() for path in files) or not events:
        raise ValueError("Se requieren los siete CSV y JSONL en datalake/landing.")
    files += events
    manifest = []
    for path in files:
        with path.open("rb") as source:
            digest = hashlib.file_digest(source, "sha256").hexdigest()
        manifest.append({"path": path.relative_to(root).as_posix(),
                         "bytes": path.stat().st_size, "sha256": digest})
    return {"status": "passed", "files": len(files), "jsonl_files": len(events),
            "bytes": sum(item["bytes"] for item in manifest), "manifest": manifest}


def check_parquet(settings):
    spark = create_spark(settings)
    spark.sparkContext.setLogLevel("WARN")
    try:
        from pyspark.sql import functions as F
        # Directorio nuevo por ejecución: no sobreescribe datos del futuro Lake.
        output = settings.lake_root / "environment_checks" / str(uuid4())
        source = spark.createDataFrame([(1, "2025-07-03", "1.25"), (2, "2025-07-04", "2.50")],
                                       "id long, event_date string, cost string").select(
            "id", F.to_date("event_date").alias("event_date"),
            F.col("cost").cast("decimal(18,2)").alias("cost"))
        source.write.mode("errorifexists").partitionBy("event_date").parquet(output.as_uri())
        restored = spark.read.parquet(output.as_uri()).select(source.columns)
        if source.exceptAll(restored).count() or restored.exceptAll(source).count():
            raise AssertionError("El round-trip Parquet cambió los registros.")
        files = list(output.rglob("*.parquet"))
        total = str(restored.agg(F.sum("cost")).first()[0])
        return {"status": "passed", "rows": restored.count(), "total_cost": total,
                "parquet_files": len(files), "bytes": sum(p.stat().st_size for p in files),
                "spark": spark.version, "master": spark.sparkContext.master,
                "java": spark.sparkContext._jvm.java.lang.System.getProperty("java.version"),
                "timezone": spark.conf.get("spark.sql.session.timeZone"),
                "storage": "local filesystem in Docker volume", "synthetic_data": True}
    finally:
        spark.stop()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--serving", action="store_true", help="Exigir también conexión CQL")
    args = parser.parse_args()
    settings = load_settings()
    report = {"checked_at": datetime.now(timezone.utc).isoformat(), "python": platform.python_version(),
              "dependencies": {name: importlib.metadata.version(name) for name in
                               ("pyspark", "py4j", "cassandra-driver")},
              "academic_validation": "docker_allowed_confirmed_by_user", "pipeline_implemented": False}
    for name, check in (("dataset", lambda: check_dataset(settings.dataset_root)),
                        ("parquet", lambda: check_parquet(settings))):
        try:
            report[name] = check()
        except Exception as error:
            report[name] = {"status": "failed", "error_type": type(error).__name__}
    report["serving"] = check_serving() if args.serving else {"status": "not_checked"}
    required = ["dataset", "parquet"] + (["serving"] if args.serving else [])
    report["status"] = "passed" if all(report[k]["status"] == "passed" for k in required) else "incomplete"
    settings.evidence_root.mkdir(parents=True, exist_ok=True)
    suffix = "serving" if args.serving else "spark"
    path = settings.evidence_root / f"{suffix}.json"
    path.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "evidence": str(path),
                      "checks": {k: report[k]["status"] for k in ("dataset", "parquet", "serving")}}))
    return 0 if report["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
