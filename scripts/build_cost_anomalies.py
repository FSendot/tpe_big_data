"""Publica señales de picos diarios de costo desde una publicación Gold."""

import argparse
import json
import re
import time

from cloud_analytics.anomalies import AnomalyError, Parameters, publish_anomalies
from cloud_analytics.config import load_settings
from cloud_analytics.silver import SilverError
from cloud_analytics.spark import create_spark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--gold-publication-id", required=True)
    parser.add_argument("--window-days", type=int, default=Parameters.window_days)
    parser.add_argument("--min-observations", type=int, default=Parameters.min_observations)
    parser.add_argument("--score-threshold", type=float, default=Parameters.score_threshold)
    parser.add_argument("--report-name", default="latest")
    args = parser.parse_args()
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.report_name):
        parser.error("report-name debe ser un nombre simple")
    try:
        parameters = Parameters(args.window_days, args.min_observations, args.score_threshold)
    except AnomalyError as error:
        parser.error(str(error))
    settings = load_settings()
    started = time.monotonic()
    spark = None
    try:
        spark = create_spark(settings)
        spark.sparkContext.setLogLevel("WARN")
        result = publish_anomalies(spark, settings, args.gold_publication_id, parameters)
    except Exception as error:
        result = {"status": "failed", "error_type": type(error).__name__}
        if isinstance(error, (AnomalyError, SilverError)):
            result["error_code"] = str(error)
    finally:
        if spark:
            spark.stop()
    result["duration_seconds"] = round(time.monotonic() - started, 3)
    output = settings.evidence_root / "anomalies" / f"{args.report_name}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "evidence": str(output),
                      "publication_id": result.get("manifest", {}).get("publication_id"),
                      "error_type": result.get("error_type"), "error_code": result.get("error_code")}))
    return int(result["status"] == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
