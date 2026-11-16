"""Inspecciona un corte Bronze o publica un snapshot Silver con su quarantine."""

import argparse
import json
import re
import time

from cloud_analytics.bronze import BronzeError
from cloud_analytics.config import load_settings
from cloud_analytics.silver import SilverError, inspect_cut, publish_silver
from cloud_analytics.spark import create_spark
from cloud_analytics.usage_stream import StreamError


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-id", required=True)
    parser.add_argument("--inspect-cut", action="store_true")
    parser.add_argument("--batch-id")
    parser.add_argument("--archive-cut")
    parser.add_argument("--report-name", default="latest")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.report_name):
        parser.error("report-name debe ser un nombre simple")
    if args.inspect_cut:
        if args.batch_id or args.archive_cut:
            parser.error("--inspect-cut sólo requiere --stream-id")
    elif not args.batch_id or not args.archive_cut:
        parser.error("la publicación requiere --batch-id y --archive-cut")
    settings = load_settings()
    start = time.monotonic()
    spark = None
    try:
        spark = create_spark(settings)
        spark.sparkContext.setLogLevel("WARN")
        if args.inspect_cut:
            print(json.dumps(inspect_cut(spark, settings, args.stream_id), indent=2))
            return 0
        result = publish_silver(spark, settings, args.batch_id, args.stream_id, args.archive_cut)
    except Exception as error:
        result = {"status": "failed", "error_type": type(error).__name__}
        # Las excepciones de Spark pueden incluir registros o valores de origen.
        if isinstance(error, (SilverError, BronzeError, StreamError)):
            result["error_code"] = str(error)
    finally:
        if spark:
            spark.stop()
    if args.inspect_cut:
        print(json.dumps(result))
        return 1
    result["duration_seconds"] = round(time.monotonic() - start, 3)
    output = settings.evidence_root / "silver" / f"{args.report_name}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "evidence": str(output),
                      "publication_id": result.get("manifest", {}).get("publication_id"),
                      "error_type": result.get("error_type"), "error_code": result.get("error_code")}))
    return int(result["status"] == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
