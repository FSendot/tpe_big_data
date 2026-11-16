"""Publica el mart diario de FinOps a partir de una publicación Silver."""

import argparse
import json
import re
import time

from cloud_analytics.config import load_settings
from cloud_analytics.gold import GoldError, publish_gold
from cloud_analytics.silver import SilverError
from cloud_analytics.spark import create_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--silver-publication-id", required=True)
    parser.add_argument("--report-name", default="latest")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.report_name):
        parser.error("report-name debe ser un nombre simple")
    settings = load_settings()
    started = time.monotonic()
    spark = None
    try:
        spark = create_spark(settings)
        spark.sparkContext.setLogLevel("WARN")
        result = publish_gold(spark, settings, args.silver_publication_id)
    except Exception as error:
        result = {"status": "failed", "error_type": type(error).__name__}
        if isinstance(error, (GoldError, SilverError)):
            result["error_code"] = str(error)
    finally:
        if spark:
            spark.stop()
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    output = settings.evidence_root / "gold" / f"{args.report_name}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "evidence": str(output),
                      "publication_id": result.get("manifest", {}).get("publication_id"),
                      "error_type": result.get("error_type"), "error_code": result.get("error_code")}))
    return int(result["status"] == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
