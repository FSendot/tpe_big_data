"""Carga Gold en AstraDB o comprueba dos consultas FinOps contra el Parquet."""

import argparse
from datetime import date, datetime, timezone
import json
import re
import time

from cloud_analytics.config import load_settings
from cloud_analytics.finops_serving import (
    CqlStore, ServingLoadError, load_gold, publish, query_finops, spark_rows,
)
from cloud_analytics.serving import MissingAstraConfiguration, serving_session
from cloud_analytics.spark import create_spark


def main():
    options = argparse.ArgumentParser(add_help=False)
    options.add_argument("--gold-publication-id", required=True)
    options.add_argument("--report-name", default="latest")
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("load", parents=[options])
    query = commands.add_parser("query", parents=[options])
    query.add_argument("--org-id", required=True)
    query.add_argument("--service", required=True)
    query.add_argument("--start", required=True, type=date.fromisoformat)
    query.add_argument("--end", required=True, type=date.fromisoformat)
    query.add_argument("--detail", required=True, type=date.fromisoformat)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{64}", args.gold_publication_id):
        parser.error("gold-publication-id debe ser el SHA-256 hexadecimal de Gold")
    if not re.fullmatch(r"[a-zA-Z0-9_-]+", args.report_name):
        parser.error("report-name debe ser un nombre simple")
    if args.command == "query" and not args.start <= args.detail <= args.end:
        parser.error("la fecha de detalle debe estar dentro del rango inclusivo")

    settings = load_settings()
    started = time.monotonic()
    report = {"mode": "astra", "operation": args.command, "gold_publication_id": args.gold_publication_id,
              "started_at": datetime.now(timezone.utc).isoformat()}
    spark = None
    stage = "spark"
    try:
        spark = create_spark(settings)
        spark.sparkContext.setLogLevel("WARN")
        frame, source_hash, expected_rows = load_gold(spark, settings, args.gold_publication_id)
        report.update(gold_manifest_sha256=source_hash, expected_rows=expected_rows)
        stage = "connect_astra"
        with serving_session("astra") as session:
            stage = "serving"
            store = CqlStore(session, create_schema=args.command == "load")
            report["serving_schema_sha256"] = store.schema_hash
            if args.command == "load":
                report.update(publish(store, args.gold_publication_id, source_hash, expected_rows,
                                      lambda: spark_rows(frame)))
            else:
                report.update(query_finops(store, frame, args.gold_publication_id, source_hash, expected_rows,
                                           args.org_id, args.service, args.start, args.end, args.detail))
    except MissingAstraConfiguration:
        report.update(status="failed", stage="connect_astra", error_code="missing_astra_configuration")
    except ServingLoadError as error:
        report.update(status="failed", stage=error.stage, error_code=str(error), error_type=error.error_type)
    except Exception as error:
        report.update(status="failed", stage=stage, error_type=type(error).__name__)
    finally:
        if spark:
            spark.stop()
    report["duration_seconds"] = round(time.monotonic() - started, 3)
    output = settings.evidence_root / "serving" / f"{args.report_name}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "evidence": str(output), "stage": report.get("stage"),
                      "error_code": report.get("error_code"), "error_type": report.get("error_type")}))
    return int(report["status"] == "failed")


if __name__ == "__main__":
    raise SystemExit(main())
