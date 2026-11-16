"""Ingesta batch de cuatro fuentes CSV a Bronze; no ejecuta Silver ni serving."""

import argparse
from datetime import datetime, timezone
import json
import platform
import re
import sys
import time

from cloud_analytics.bronze import BronzeError, ingest_batch
from cloud_analytics.config import load_settings
from cloud_analytics.spark import create_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--report-name", default="latest", help="Nombre simple del JSON de evidencia")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.report_name):
        parser.error("report-name debe contener sólo letras, números, guiones o guiones bajos")
    settings = load_settings()
    report = {"started_at": datetime.now(timezone.utc).isoformat(), "python": platform.python_version()}
    start = time.monotonic()
    spark = None
    try:
        spark = create_spark(settings)
        spark.sparkContext.setLogLevel("WARN")
        result = ingest_batch(spark, settings)
        report.update(result)
    except Exception as error:
        # El texto de errores de Spark puede incluir datos personales; no publicarlo.
        report.update(status="failed", error_type=type(error).__name__)
        if isinstance(error, BronzeError):
            report["error_code"] = str(error)
    finally:
        if spark is not None:
            spark.stop()
    report["duration_seconds"] = round(time.monotonic() - start, 3)
    directory = settings.evidence_root / "batch"
    directory.mkdir(parents=True, exist_ok=True)
    output = directory / f"{args.report_name}.json"
    output.write_text(json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "evidence": str(output),
                      "batch_id": report.get("manifest", {}).get("batch_id")}))
    return 1 if report["status"] == "failed" else 0


if __name__ == "__main__":
    sys.exit(main())
