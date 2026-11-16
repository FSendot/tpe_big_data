"""Mart diario FinOps con cobertura por medida y publicación local inmutable."""

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import tempfile

from pyspark.sql import functions as F

from cloud_analytics import silver, silver_quality
from cloud_analytics.bronze import fingerprint
from cloud_analytics.usage_stream import lock_at


MART = "org_daily_usage_by_service"
GRAIN = ("org_id", "event_date", "service")
USAGE = ("requests", "cpu_hours", "storage_gb_hours")
MEASURES = {
    "daily_cost_usd": ("cost_usd_increment", "cost_status"),
    **{metric: (metric, "usage_status") for metric in USAGE},
    "carbon_kg": ("carbon_kg", "carbon_status"),
    "genai_tokens": ("genai_tokens", "tokens_status"),
}
EXCLUDED = ("excluded_event_ids", "excluded_unidentified_fingerprints")


class GoldError(ValueError):
    """Código de diagnóstico sin datos de las filas."""


def count_if(condition):
    return F.count(F.when(condition, 1))


def measure_value(source):
    value = F.col(source)
    # Ensanchar antes de sumar para conservar exactitud por encima de int64.
    return value.cast("decimal(38,0)") if source == "genai_tokens" else value


def quarantine_usage_metric(quarantine):
    # Silver vacía metric y unit junto con value cuando usage no es válido.
    # El payload sólo recupera la declaración para fijar el denominador.
    metric = F.lower(F.trim(F.get_json_object("raw_payload", "$.metric")))
    by_event = (quarantine.where("scope = 'usage'")
                .withColumn("_declared", F.coalesce(metric, F.lit("")))
                .groupBy("record_id").agg(F.min("_declared").alias("_min"), F.max("_declared").alias("_max")))
    return by_event.select(F.col("record_id").alias("event_id"),
                           F.when((F.col("_min") == F.col("_max")) & F.col("_min").isin(*USAGE),
                                  F.col("_min")).alias("_quarantined_metric"))


def locate_exclusions(quarantine, resources):
    variants = quarantine.where("scope = 'event'").withColumn(
        "_key", F.from_json("raw_payload", "timestamp string, org_id string, resource_id string, service string"))
    timestamp = F.col("_key.timestamp")
    event_date = F.to_date(F.when(timestamp.rlike(r"^\d{4}-\d{2}-\d{2}T.*(Z|[+-]\d{2}:\d{2})$"),
                                  F.expr("try_cast(_key.timestamp as timestamp)")))
    declared = variants.select(
        "record_id", "reasons", F.coalesce("source_record_hash", F.sha2("raw_payload", 256)).alias("_fingerprint"),
        silver_quality.normalized("_key.org_id").alias("org_id"),
        silver_quality.normalized("_key.resource_id").alias("resource_id"),
        silver_quality.normalized("_key.service", True).alias("service"), event_date.alias("event_date"))
    reference = resources.select("resource_id", "org_id", "service").withColumn("_confirmed", F.lit(True))
    return declared.join(reference, ["resource_id", "org_id", "service"], "left").withColumn(
        "_located", F.col("event_date").isNotNull() & F.coalesce("_confirmed", F.lit(False))
        & ~F.coalesce(F.array_contains("reasons", "malformed_json"), F.lit(True)))


def excluded_counts():
    return (F.countDistinct("record_id").alias(EXCLUDED[0]),
            F.countDistinct(F.when(F.col("record_id").isNull(), F.col("_fingerprint"))).alias(EXCLUDED[1]))


def exclusion_summary(locations):
    unlocated = ~F.col("_located")
    row = locations.agg(
        F.count("*").alias("event_scope_rows"),
        *excluded_counts(),
        F.countDistinct(F.when(unlocated, F.col("record_id"))).alias("unlocated_event_ids"),
        F.countDistinct(F.when(unlocated & F.col("record_id").isNull(), F.col("_fingerprint")))
        .alias("unlocated_unidentified_fingerprints"),
        count_if(unlocated & F.col("record_id").isNull() & F.col("_fingerprint").isNull())
        .alias("unlocated_rows_without_identity"),
    ).first().asDict()
    row["has_unlocated_exclusions"] = any(value > 0 for key, value in row.items() if key.startswith("unlocated_"))
    return row


def eligible_for(measure, status):
    if measure in USAGE:
        return F.col("_usage_metric") == measure
    if measure == "genai_tokens":
        return (F.col("service") == "genai") | (status == "quarantined")
    return F.lit(True)


def aggregate_gold(events, quarantine, locations):
    events = (events.join(quarantine_usage_metric(quarantine), "event_id", "left")
              .withColumn("_usage_metric", F.coalesce("metric", "_quarantined_metric")))
    aggregates = [
        F.count("*").alias("silver_event_count"),
        count_if((F.col("usage_status") == "quarantined") & F.col("_usage_metric").isNull())
        .alias("unassigned_usage_events"),
    ]
    fill_counts = ["silver_event_count", "unassigned_usage_events", *EXCLUDED]
    for measure, (source, flag) in MEASURES.items():
        status = F.col(flag)
        eligible = eligible_for(measure, status)
        valid = eligible & (status == "valid")
        aggregates.append(F.try_sum(F.when(valid, measure_value(source))).alias(measure))
        for label in ("valid", "quarantined", "absent"):
            name = f"_{measure}_{label}"
            aggregates.append(count_if(eligible & (status == label)).alias(name))
            fill_counts.append(name)
    excluded = locations.where("_located").groupBy(*GRAIN).agg(*excluded_counts())
    groups = events.groupBy(*GRAIN).agg(*aggregates).join(excluded, list(GRAIN), "full")
    groups = groups.fillna(0, subset=fill_counts)

    coverage = {}
    for measure in MEASURES:
        valid = F.col(f"_{measure}_valid")
        quarantined = F.col(f"_{measure}_quarantined")
        absent = F.col(f"_{measure}_absent")
        eligible = valid + quarantined + absent
        pending = (quarantined > 0) | (F.col(EXCLUDED[0]) > 0) | (F.col(EXCLUDED[1]) > 0)
        if measure in USAGE:
            pending = pending | (F.col("unassigned_usage_events") > 0)
        state = (F.when(valid > 0, F.when(pending | (absent > 0), "partial").otherwise("complete"))
                 .when(pending, "unavailable").when(absent > 0, "absent").otherwise("not_applicable"))
        coverage[f"{measure}_coverage"] = F.struct(
            eligible.alias("eligible"), valid.alias("valid"), quarantined.alias("quarantined"),
            absent.alias("absent"), F.try_divide(valid, eligible).alias("valid_fraction"), state.alias("state"))
    return groups.withColumns(coverage).select(
        *GRAIN, "silver_event_count", "unassigned_usage_events", *EXCLUDED, *MEASURES, *coverage)


def check_overflow(groups):
    failures = groups.agg(*[count_if(
        (F.col(f"{measure}_coverage.valid") > 0) & F.col(measure).isNull()).alias(measure)
        for measure in MEASURES]).first().asDict()
    for measure, count in failures.items():
        if count:
            raise GoldError(f"group_sum_overflow:{measure}")


def reconcile(events, gold, expected_events):
    silver_aggregates = [F.count("*").alias("event_count")]
    gold_aggregates = [F.coalesce(F.sum("silver_event_count"), F.lit(0)).alias("event_count")]
    for measure, (source, flag) in MEASURES.items():
        # Contrastar directamente con los valores válidos de Silver, sin
        # depender de la lógica de denominadores ni repetir el groupBy diario.
        value = F.when(F.col(flag) == "valid", measure_value(source))
        silver_aggregates.extend([F.try_sum(value).alias(measure), F.count(value).alias(f"{measure}_valid")])
        gold_aggregates.extend([F.try_sum(measure).alias(measure),
                                F.coalesce(F.sum(f"{measure}_coverage.valid"), F.lit(0)).alias(f"{measure}_valid")])
    source = events.agg(*silver_aggregates).first().asDict()
    result = gold.agg(*gold_aggregates).first().asDict()
    if source["event_count"] != expected_events or result["event_count"] != expected_events:
        raise GoldError("event_count_mismatch")
    report = {}
    for measure in MEASURES:
        key = f"{measure}_valid"
        if any(stats[key] > 0 and stats[measure] is None for stats in (source, result)):
            raise GoldError(f"reconciliation_sum_overflow:{measure}")
        if source[measure] != result[measure] or source[key] != result[key]:
            raise GoldError(f"reconciliation_mismatch:{measure}")
        report[measure] = {"silver_valid_contributions": source[key], "gold_valid_contributions": result[key],
                           "silver_total": source[measure], "gold_total": result[measure]}
    return {"silver_events": expected_events, "gold_silver_events": result["event_count"], "measures": report}


def build_mart(spark, stage, source, source_manifest):
    events = spark.read.parquet(str(source / "usage_events")).drop("origins")
    quarantine = (spark.read.parquet(str(source / "quarantine")).where("source = 'usage_events'")
                  .select("scope", "record_id", "raw_payload", "source_record_hash", "reasons"))
    resources = spark.read.parquet(str(source / "resources"))
    locations = locate_exclusions(quarantine, resources).cache()
    groups = (aggregate_gold(events, quarantine, locations)
              .withColumn("silver_publication_id", F.lit(source_manifest["publication_id"])).cache())
    try:
        check_overflow(groups)
        restored, table = silver.write_table(spark, stage, MART, groups, "event_date")
        reconciliation = reconcile(events, restored, source_manifest["tables"]["usage_events"]["rows"])
        states = restored.select(F.explode(F.array(*[
            F.struct(F.lit(measure).alias("measure"), F.col(f"{measure}_coverage.state").alias("state"))
            for measure in MEASURES])).alias("coverage")).select("coverage.*")
        stats = {"exclusions": exclusion_summary(locations),
                 "groups_only_in_quarantine": restored.where("silver_event_count = 0").count(),
                 "groups_with_exclusions": restored.where(f"{EXCLUDED[0]} > 0 OR {EXCLUDED[1]} > 0").count(),
                 "unassigned_usage_events": restored.agg(F.coalesce(F.sum("unassigned_usage_events"), F.lit(0))).first()[0],
                 "coverage_states": silver.distribution(states, "measure", "state")}
        return table, stats, reconciliation
    finally:
        groups.unpersist()
        locations.unpersist()


def publish_gold(spark, settings, silver_publication_id):
    if not re.fullmatch(r"[0-9a-f]{64}", silver_publication_id):
        raise GoldError("invalid_silver_publication_id")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise GoldError("gold_requires_utc")
    source = settings.lake_root / "silver" / f"publication_id={silver_publication_id}"
    manifest_path = source / "manifest.json"
    with lock_at(settings.lake_root / "control" / "gold.lock"):
        if not manifest_path.is_file():
            raise GoldError("silver_manifest_missing")
        source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (source_manifest["publication_id"] != silver_publication_id
                or silver.digest(source_manifest["inputs"]) != silver_publication_id):
            raise GoldError("silver_publication_identity_mismatch")
        if (source_manifest["inputs"].get("contract") != "silver-v1"
                or source_manifest["inputs"].get("timezone") != "UTC"
                or set(source_manifest["tables"]) != {"usage_events", "customers_orgs", "resources", "quarantine"}):
            raise GoldError("unsupported_silver_manifest")
        source_manifest = silver.verify_publication(source, source_manifest["inputs"])
        identity = {
            "contract": "finops-gold-v1", "silver_publication_id": silver_publication_id,
            "silver_manifest_sha256": fingerprint(manifest_path),
            "spark_version": spark.version, "timezone": "UTC",
            "implementation_sha256": {
                path.name: hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                for path in (Path(__file__), Path(silver.__file__), Path(silver_quality.__file__))},
        }
        publication_id = silver.digest(identity)
        target = settings.lake_root / "gold" / f"publication_id={publication_id}"
        if target.exists():
            return {"status": "reused", "publication_path": str(target),
                    "manifest": silver.verify_publication(target, identity)}
        staging = settings.lake_root / ".gold-staging"
        staging.mkdir(parents=True, exist_ok=True)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="publication-", dir=staging) as directory:
            stage = Path(directory)
            table, stats, reconciliation = build_mart(spark, stage, source, source_manifest)
            manifest = {"publication_id": publication_id, "inputs": identity, "grain": list(GRAIN),
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "tables": {MART: table}, "quality": stats, "reconciliation": reconciliation,
                        "files": silver.publication_files(stage)}
            (stage / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n", encoding="utf-8")
            manifest = silver.verify_publication(stage, identity)
            stage.rename(target)
    return {"status": "created", "publication_path": str(target), "manifest": manifest}
