"""Ejecuta los jobs existentes, comprueba sus contratos y compara reintentos."""

import argparse
from datetime import date
import json
import os
from pathlib import Path
import re
import subprocess
import sys
from time import monotonic
import tomllib
from unittest.mock import patch

from pyspark.sql import functions as F

from cloud_analytics import anomalies, gold
from cloud_analytics.bronze import fingerprint
from cloud_analytics.config import load_settings
from cloud_analytics.finops_serving import CqlStore, ServingLoadError, decode_row, load_gold, publish, spark_rows
from cloud_analytics.serving import serving_session
from cloud_analytics.silver import digest
from cloud_analytics.spark import create_spark
from cloud_analytics.usage_stream import lock_at


class IntegrationError(ValueError):
    pass


def require(condition, code):
    if not condition:
        raise IntegrationError(code)


def read_json(path):
    return json.loads(path.read_text(encoding="utf-8"))


class Jobs:
    def __init__(self, config, reports, summary):
        self.env = {**os.environ, "APP_CONFIG": str(config)}
        self.reports = reports
        self.steps = summary

    def run(self, name, script, *args, report=None, statuses=(), expected_error=None):
        command = [f"scripts/{script}.py", *args]
        step = {"step": name, "args": command}
        self.steps.append(step)
        print(f"Validación integrada · {name}", flush=True)
        result = subprocess.run([sys.executable, *command], env=self.env, text=True,
                                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        step["exit_code"] = result.returncode
        if report and (self.reports / report).is_file():
            data = read_json(self.reports / report)
        else:
            try:
                data = json.loads(result.stdout)
            except ValueError:
                data = {}
        step.update({key: data[key] for key in ("status", "stage", "error_code", "error_type") if key in data})
        if expected_error:
            require(result.returncode == 1 and data.get("error_code") == expected_error,
                    f"unexpected_recovery_outcome:{name}")
        else:
            require(result.returncode == 0 and data and (not statuses or data.get("status") in statuses),
                    f"job_failed:{name}")
        return data


def validation_config(base_path, lake, reports, target):
    raw = tomllib.loads(base_path.read_text(encoding="utf-8"))
    raw.update(lake_root=str(lake), evidence_root=str(reports))
    sections = [[f"{key} = {json.dumps(value)}" for key, value in raw.items() if not isinstance(value, dict)]]
    sections.extend([f"[{table}]", *(f"{key} = {json.dumps(value)}" for key, value in fields.items())]
                    for table, fields in raw.items() if isinstance(fields, dict))
    target.write_text("\n\n".join("\n".join(lines) for lines in sections) + "\n", encoding="utf-8")


def verify_keys(frame, keys, expected):
    counts = frame.agg(F.count("*").alias("rows"), F.countDistinct(*keys).alias("keys")).first()
    require(counts.rows == counts.keys == expected, "grain_or_row_count_mismatch")
    return counts.keys


def audit_materialized(spark, silver_result, gold_result, anomaly_result):
    silver_path = Path(silver_result["publication_path"])
    outcomes = {}
    for table, key in (("usage_events", "event_id"), ("customers_orgs", "org_id"), ("resources", "resource_id")):
        frame = spark.read.parquet(str(silver_path / table))
        outcomes[table] = verify_keys(frame, [key], silver_result["manifest"]["tables"][table]["rows"])
    gold_frame = spark.read.parquet(str(Path(gold_result["publication_path"]) / gold.MART))
    rows = gold_result["manifest"]["tables"][gold.MART]["rows"]
    outcomes[gold.MART] = verify_keys(gold_frame, gold.GRAIN, rows)
    silver_id = silver_result["manifest"]["publication_id"]
    require(not gold_frame.where(~F.col("silver_publication_id").eqNullSafe(silver_id)).limit(1).count(),
            "gold_row_lineage_mismatch")
    anomaly_frame = spark.read.parquet(str(Path(anomaly_result["publication_path"]) / anomalies.MART))
    outcomes[anomalies.MART] = verify_keys(anomaly_frame, gold.GRAIN, rows)
    gold_id = gold_result["manifest"]["publication_id"]
    require(not anomaly_frame.where(~F.col("gold_publication_id").eqNullSafe(gold_id)).limit(1).count(),
            "anomaly_row_lineage_mismatch")
    expected = gold_frame.select(*gold.GRAIN, "daily_cost_usd",
                                 F.col("daily_cost_usd_coverage.state").alias("cost_coverage_state"))
    observed = anomaly_frame.select(*expected.columns)
    require(not expected.exceptAll(observed).limit(1).count(), "anomaly_observed_cost_or_coverage_mismatch")
    return outcomes


def interrupt_astra_load(spark, settings, gold_id):
    """Escribe una fila confirmada y corta el primer intento de este ID nuevo."""
    frame, source_hash, row_count = load_gold(spark, settings, gold_id)
    require(row_count > 1, "serving_recovery_needs_multiple_rows")
    written = []
    with serving_session("astra") as session:
        store = CqlStore(session, create_schema=True)
        require(store.status(gold_id) is None, "serving_recovery_requires_new_gold_id")
        original = store.upsert

        def write_one_then_interrupt(batch):
            original(batch[:1])
            written.append(batch[0])
            raise InterruptedError("integration_test_after_first_ack")

        with patch.object(store, "upsert", side_effect=write_one_then_interrupt):
            try:
                publish(store, gold_id, source_hash, row_count, lambda: spark_rows(frame))
            except ServingLoadError as error:
                require((error.stage, error.error_type) == ("upsert", "InterruptedError"),
                        "unexpected_serving_injection_error")
            else:
                raise IntegrationError("serving_injection_did_not_interrupt")
        marker = store.status(gold_id)
        require(marker and marker.state == "loading" and marker.verified_rows is None and len(written) == 1,
                "partial_astra_marker_mismatch")
        restored = tuple(map(decode_row, next(store.read_partitions([written[0][:4]]))))
        require(restored == tuple(written), "partial_astra_readback_mismatch")
    return {"backend": "astra", "injection": "after_first_acknowledged_upsert",
            "written_and_read_back": len(restored), "expected_rows": marker.expected_rows,
            "state_after_failure": marker.state, "verified_rows_after_failure": marker.verified_rows}


def publication(result, source_key, source_id, hash_key, source_path):
    manifest = result["manifest"]
    path = Path(result["publication_path"]) / "manifest.json"
    require(manifest["inputs"][source_key] == source_id
            and manifest["inputs"][hash_key] == fingerprint(source_path), "manifest_lineage_mismatch")
    return manifest["publication_id"], path


def run_pipeline(args, settings, jobs, report):
    stream_id = f"integration-{args.validation_id}"
    batch = jobs.run("batch", "ingest_batch", report="batch/latest.json", statuses=("created", "reused"))
    batch_id = batch["manifest"]["batch_id"]
    replay = jobs.run("replay", "replay_usage", "--stream-id", stream_id)
    stream = jobs.run("streaming", "ingest_stream", "--stream-id", stream_id,
                      report="streaming/latest.json", statuses=("passed",))
    cut = jobs.run("archive_cut", "build_silver", "--stream-id", stream_id, "--inspect-cut")
    require(cut["stream_id"] == stream_id == stream["stream_id"], "stream_identity_mismatch")
    require(len(stream["inputs"]) == replay["offered_files"] + replay["already_offered_files"],
            "stream_replay_inventory_mismatch")
    silver = jobs.run("silver", "build_silver", "--stream-id", stream_id, "--batch-id", batch_id,
                      "--archive-cut", cut["cut_id"], report="silver/latest.json", statuses=("created", "reused"))
    silver_id, silver_manifest = publication(silver, "batch_id", batch_id, "batch_manifest_sha256",
                                             Path(batch["batch_path"]) / "manifest.json")
    require(silver["manifest"]["inputs"]["archive"] == cut
            and silver["manifest"]["quality"]["archive_rows"] == stream["audit"]["archive_rows"], "silver_cut_mismatch")
    daily = jobs.run("gold", "build_gold", "--silver-publication-id", silver_id,
                     report="gold/latest.json", statuses=("created", "reused"))
    gold_id, gold_manifest = publication(daily, "silver_publication_id", silver_id,
                                          "silver_manifest_sha256", silver_manifest)
    reconciliation = daily["manifest"]["reconciliation"]
    silver_events = silver["manifest"]["tables"]["usage_events"]["rows"]
    require(reconciliation["silver_events"] == reconciliation["gold_silver_events"] == silver_events
            and set(reconciliation["measures"]) == set(gold.MEASURES)
            and all(m["silver_total"] == m["gold_total"]
                    and m["silver_valid_contributions"] == m["gold_valid_contributions"]
                    for m in reconciliation["measures"].values()), "silver_gold_reconciliation_mismatch")
    signals = jobs.run("anomalies", "build_cost_anomalies", "--gold-publication-id", gold_id, "--window-days", "28",
                       "--min-observations", "7", "--score-threshold", "3.5",
                       report="anomalies/latest.json", statuses=("created", "reused"))
    anomaly_id, anomaly_manifest = publication(signals, "gold_publication_id", gold_id, "gold_manifest_sha256",
                                                gold_manifest)
    report["ids"] = {"batch": batch_id, "stream": stream_id, "archive_cut": cut["cut_id"],
                     "silver": silver_id, "gold": gold_id, "anomalies": anomaly_id}
    print("Validación integrada · auditoría Parquet", flush=True)
    spark = create_spark(settings)
    spark.sparkContext.setLogLevel("ERROR")
    try:
        grains = audit_materialized(spark, silver, daily, signals)
        if args.exercise_serving_recovery:
            print("Validación integrada · interrupción controlada en Astra", flush=True)
            report["serving_recovery"] = interrupt_astra_load(spark, settings, gold_id)
    finally:
        spark.stop()
    query_args = ("query", "--gold-publication-id", gold_id, "--org-id", args.org_id, "--service", args.service,
                  "--start", args.start.isoformat(), "--end", args.end.isoformat(), "--detail", args.detail.isoformat())
    if args.exercise_serving_recovery:
        rejected = jobs.run("query_while_loading", "serve_finops", *query_args, "--report-name", "loading",
                            report="serving/loading.json", expected_error="publication_not_ready")
        report["serving_recovery"]["query_rejected"] = rejected["error_code"]
    loaded = jobs.run("serving", "serve_finops", "load", "--gold-publication-id", gold_id,
                      report="serving/latest.json", statuses=("loaded", "reused"))
    queries = jobs.run("queries", "serve_finops", *query_args, "--report-name", "queries",
                       report="serving/queries.json", statuses=("verified",))
    for result in (loaded, queries):
        require(result["mode"] == "astra" and result["state"] == "complete"
                and result["gold_publication_id"] == gold_id
                and result["gold_manifest_sha256"] == fingerprint(gold_manifest), "astra_lineage_mismatch")
    require(loaded["verification"]["rows_verified"] == grains[gold.MART], "astra_row_count_mismatch")
    query_set = queries["queries"]
    require(query_set["range"]["rows_verified"] > 0 and query_set["detail"]["rows_verified"] == 1,
            "astra_query_requires_nonempty_results")
    if args.exercise_serving_recovery:
        report["serving_recovery"].update(state_after_retry=loaded["state"],
                                           rows_verified_after_retry=loaded["verification"]["rows_verified"])
    report["activity"] = {"batch": batch["status"], "silver": silver["status"], "gold": daily["status"],
                          "anomalies": signals["status"], "new_jsonl": replay["offered_files"],
                          "archive_input_rows": sum(x["input_rows"] for x in stream["archive_query"]["progress"]),
                          "dedup_input_rows": sum(x["input_rows"] for x in stream["dedup_query"]["progress"]),
                          "astra": loaded["status"], "astra_upserted_rows": loaded["upserted_rows"]}
    report["verified"] = {
        "csv_inputs": batch["manifest"]["inputs"], "jsonl_files": len(stream["inputs"]),
        "jsonl_inventory_sha256": digest(stream["inputs"]), "stream_audit": stream["audit"],
        "query_ids": {key: stream[key]["query_id"] for key in ("archive_query", "dedup_query")},
        "manifests_sha256": {key: fingerprint(path) for key, path in (
            ("batch", Path(batch["batch_path"]) / "manifest.json"), ("silver", silver_manifest),
            ("gold", gold_manifest), ("anomalies", anomaly_manifest))},
        "bronze_batch_rows": {key: item["bronze_rows"] for key, item in batch["manifest"]["sources"].items()},
        "unique_grains": grains, "quarantine_rows": silver["manifest"]["tables"]["quarantine"]["rows"],
        "silver_gold": reconciliation, "anomaly_quality": signals["manifest"]["quality"],
        "astra_readback": loaded["verification"], "astra_query_results_sha256": digest(query_set),
        "astra_range": {**{key: query_set["range"][key] for key in ("start", "end", "rows_verified")},
                        "sample": [{key: row[key] for key in ("event_date", "daily_cost_usd", "requests")}
                                   for row in query_set["range"]["sample_rows"]]},
        "astra_detail": {key: query_set["detail"]["sample_rows"][0][key]
                         for key in ("event_date", "daily_cost_usd", "requests", "daily_cost_usd_coverage")},
    }


def compare_retry(first, repeat):
    require(first["status"] == "passed" and first["validation_id"] == repeat["validation_id"]
            and first["ids"] == repeat["ids"] and first["verified"] == repeat["verified"], "retry_results_changed")
    activity = repeat["activity"]
    require(all(activity[key] == "reused" for key in ("batch", "silver", "gold", "anomalies", "astra"))
            and all(activity[key] == 0 for key in ("new_jsonl", "archive_input_rows", "dedup_input_rows",
                                                  "astra_upserted_rows")), "retry_reprocessed_input")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation-id", required=True)
    parser.add_argument("--report-name", required=True)
    parser.add_argument("--compare-to")
    parser.add_argument("--exercise-serving-recovery", action="store_true")
    parser.add_argument("--org-id", default="org_he7ur23g")
    parser.add_argument("--service", default="compute")
    parser.add_argument("--start", type=date.fromisoformat, default=date(2025, 7, 29))
    parser.add_argument("--end", type=date.fromisoformat, default=date(2025, 8, 4))
    parser.add_argument("--detail", type=date.fromisoformat, default=date(2025, 8, 3))
    args = parser.parse_args()
    names = [args.validation_id, args.report_name] + ([args.compare_to] if args.compare_to else [])
    if any(not re.fullmatch(r"[a-zA-Z0-9_-]{1,64}", name) for name in names):
        parser.error("IDs y nombres deben ser simples y tener hasta 64 caracteres")
    if not args.start <= args.detail <= args.end or (args.compare_to and args.exercise_serving_recovery):
        parser.error("rango o combinación de opciones inválidos")

    base_path = Path(os.environ.get("APP_CONFIG", "config/runtime.toml"))
    base = load_settings(base_path)
    root = base.lake_root / "integration" / args.validation_id
    reports = base.evidence_root / "integration"
    reports.mkdir(parents=True, exist_ok=True)
    output = reports / f"{args.report_name}.json"
    started = monotonic()
    report = {"validation_id": args.validation_id, "kind": "real_dataset_and_astra", "steps": []}
    report_lock = base.lake_root / "integration" / ".report-locks" / f"{args.report_name}.lock"
    with lock_at(report_lock), lock_at(root / "integration.lock"):
        if output.exists():
            parser.error("el informe ya existe; elegir otro nombre")
        run = root / "runs" / args.report_name
        run.mkdir(parents=True, exist_ok=False)
        config = run / "runtime.toml"
        validation_config(base_path, root / "lake", run / "job-reports", config)
        report.update(lake=str(root / "lake"), raw_job_reports=str(run / "job-reports"))
        try:
            baseline = read_json(reports / f"{args.compare_to}.json") if args.compare_to else None
            if baseline is not None:
                require(baseline.get("status") == "passed" and baseline.get("validation_id") == args.validation_id,
                        "invalid_retry_baseline")
            jobs = Jobs(config, run / "job-reports", report["steps"])
            run_pipeline(args, load_settings(config), jobs, report)
            if baseline is not None:
                compare_retry(baseline, report)
                report["retry_comparison"] = {"against": args.compare_to, "identical_ids_and_results": True}
            report["status"] = "passed"
        except Exception as error:
            report.update(status="failed", error_type=type(error).__name__)
            if isinstance(error, IntegrationError):
                report["error_code"] = str(error)
        report["duration_seconds"] = round(monotonic() - started, 3)
        output.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"status": report["status"], "evidence": str(output), "error_code": report.get("error_code")}))
    return int(report["status"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
