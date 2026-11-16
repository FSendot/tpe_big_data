"""Procesa los archivos disponibles en micro-lotes y conserva los checkpoints."""

import argparse
import json
import re
import sys
from cloud_analytics.config import load_settings
from cloud_analytics.spark import create_spark
from cloud_analytics.usage_stream import StreamError, run_stream


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-id", default="usage-v1")
    parser.add_argument("--report-name", default="latest")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.report_name):
        parser.error("report-name debe ser un nombre simple")
    settings = load_settings()
    spark = None
    try:
        spark = create_spark(settings)
        spark.sparkContext.setLogLevel("WARN")
        result = run_stream(spark, settings, args.stream_id)
    except Exception as error:
        result = {"status": "failed", "error_type": type(error).__name__}
        if isinstance(error, StreamError):
            result["error_code"] = str(error)
    finally:
        if spark:
            spark.stop()
    output = settings.evidence_root / "streaming" / f"{args.report_name}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "evidence": str(output), "error_code": result.get("error_code")}))
    return 0 if result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
