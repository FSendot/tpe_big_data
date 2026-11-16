"""Conformado de maestros y eventos con validez independiente por medida."""

from pyspark.sql import Window, functions as F


ORIGIN = ("source_file", "source_hash", "source_record_hash", "ingest_ts", "ingest_date")
MEASURES = {
    "cost": ("cost_usd_increment",),
    "usage": ("value", "metric", "unit"),
    "carbon": ("carbon_kg",),
    "tokens": ("genai_tokens",),
}
CORE = ("event_time", "org_id", "resource_id", "service", "region")


def normalized(name, category=False):
    value = F.trim(F.col(name))
    return F.when(value != "", F.lower(value) if category else value)


def reasons(*rules):
    return F.filter(F.array(*rules), lambda rule: rule.isNotNull())


def issue(condition, code):
    return F.when(condition, F.lit(code))


def has_error(code):
    return F.array_contains("parse_errors", code)


def provenance(*extra):
    return F.sort_array(F.collect_set(F.struct(*ORIGIN, "contract_version", *extra))).alias("origins")


def incidents(frame, source, key, scope, codes):
    event = source == "usage_events"
    return (frame.withColumn("reasons", codes).where(F.size("reasons") > 0)
            .select(F.lit(source).alias("source"), F.col(key).alias("record_id"),
                    scope.alias("scope"), "reasons", *ORIGIN, "contract_version",
                    (F.col("stream_id") if event else F.col("batch_id")).alias("source_id"),
                    (F.lit(1).cast("long") if event else F.col("source_occurrences")).alias("source_occurrences"),
                    (F.col("raw_json") if event else F.to_json("raw")).alias("raw_payload")))


def conform_master(bronze, source, key, categories, fields, required, orgs=None):
    frame = bronze.withColumns({name: normalized(name, name in categories)
                                for name in (*categories, key)})
    if orgs is not None:
        frame = frame.withColumn("org_id", normalized("org_id")).join(
            orgs.select("org_id", F.lit(True).alias("_org_found")), "org_id", "left")
    local = F.concat(
        F.filter("parse_errors", lambda code: ~code.isin("missing_key", "conflicting_key")),
        reasons(*[issue(F.col(name).isNull(), f"missing_{name}") for name in required],
                issue(F.col("_org_found").isNull(), "unknown_org") if orgs is not None else F.lit(None)))
    payload = F.struct(*fields)
    by_key = Window.partitionBy(key)
    frame = (frame.withColumn("_local", local)
             .withColumn("_invalid", F.max((F.size("_local") > 0).cast("int")).over(by_key) > 0)
             .withColumn("_conflict", (F.min(payload).over(by_key) != F.max(payload).over(by_key))
                         & F.col(key).isNotNull())
             .withColumn("_issues", F.concat("_local", reasons(
                 issue(F.col("_conflict"), "conflicting_master_key"),
                 issue(F.col("_invalid") & (F.size("_local") == 0), "invalid_master_variant")))))
    clean = (frame.where(F.size("_issues") == 0).groupBy(key, *fields)
             .agg(provenance("source_occurrences"), F.sum("source_occurrences").alias("source_occurrences")))
    return clean, incidents(frame, source, key, F.lit("master"), F.col("_issues"))


def conform_masters(org_bronze, resource_bronze):
    orgs, org_issues = conform_master(
        org_bronze, "customers_orgs", "org_id",
        ("industry", "hq_region", "plan_tier", "lifecycle_stage"),
        ("industry", "hq_region", "plan_tier", "lifecycle_stage", "is_enterprise", "signup_date"),
        ("org_id",))
    resources, resource_issues = conform_master(
        resource_bronze, "resources", "resource_id", ("service", "region", "state"),
        ("org_id", "service", "region", "state", "created_at"),
        ("resource_id", "org_id", "service", "region"), orgs)
    return orgs, resources, org_issues.unionByName(resource_issues)


def event_variants(archive, orgs, resources):
    categories = ("service", "region", "metric", "unit")
    frame = archive.withColumns({name: normalized(name, name in categories)
                                 for name in ("event_id", "org_id", "resource_id", *categories)})
    org_lookup = orgs.select("org_id", "plan_tier", "industry", "hq_region")
    org_lookup = org_lookup.withColumn("_org_found", F.lit(True))
    resource_lookup = resources.select("resource_id", F.col("org_id").alias("_resource_org"),
                                       F.col("service").alias("_resource_service"),
                                       F.col("region").alias("_resource_region"), "state", "created_at")
    frame = frame.join(org_lookup, "org_id", "left").join(resource_lookup, "resource_id", "left")
    frame = frame.withColumn("_event_issues", reasons(
        issue(has_error("malformed_json"), "malformed_json"),
        issue(F.col("event_id").isNull(), "missing_event_id"),
        issue(F.col("event_time").isNull(), "missing_or_invalid_timestamp"),
        issue(has_error("unknown_schema_version"), "unknown_schema_version"),
        issue(F.col("org_id").isNull(), "missing_org_id"),
        issue(F.col("resource_id").isNull(), "missing_resource_id"),
        issue(F.col("service").isNull(), "missing_service"),
        issue(F.col("region").isNull(), "missing_region"),
        issue(F.col("org_id").isNotNull() & F.col("_org_found").isNull(), "unknown_org"),
        issue(F.col("resource_id").isNotNull() & F.col("_resource_org").isNull(), "unknown_resource"),
        issue(F.col("org_id") != F.col("_resource_org"), "resource_org_mismatch"),
        issue(F.col("service") != F.col("_resource_service"), "resource_service_mismatch"),
        issue(F.col("region") != F.col("_resource_region"), "resource_region_mismatch")))

    value = F.col("value")
    expected = F.create_map(*map(F.lit, ("requests", "count", "cpu_hours", "hours",
                                         "storage_gb_hours", "gb_hours")))[F.col("metric")]
    frame = frame.withColumns({
        "_cost_issues": reasons(
            issue(has_error("invalid_cost_usd_increment"), "invalid_cost"),
            issue(F.col("cost_usd_increment").isNull() & ~has_error("invalid_cost_usd_increment"), "missing_cost"),
            issue(F.col("cost_usd_increment") < 0, "negative_cost_pending")),
        "_usage_issues": reasons(
            issue(has_error("invalid_value"), "invalid_value"),
            issue(value.isNull() & ~has_error("invalid_value"), "missing_value"),
            issue(value < 0, "negative_value"),
            issue(value.isNotNull() & F.col("unit").isNull(), "missing_unit"),
            issue(expected.isNull(), "unknown_metric"),
            issue(F.col("unit") != expected, "unit_metric_mismatch")),
        "_carbon_issues": reasons(
            issue(has_error("invalid_carbon_kg"), "invalid_carbon"),
            issue(F.col("carbon_kg") < 0, "negative_carbon")),
        "_tokens_issues": reasons(
            issue(has_error("invalid_genai_tokens"), "invalid_tokens"),
            issue(F.col("genai_tokens") < 0, "negative_tokens"),
            issue(F.col("genai_tokens").isNotNull() & (F.col("service") != "genai"), "tokens_outside_genai")),
    })
    return frame.withColumns({f"_{name}": F.struct(
        *fields, F.col(f"_{name}_issues").alias("issues"),
        F.when(has_error(f"invalid_{fields[0]}"), F.col(f"raw.{fields[0]}")).alias("invalid_raw"))
        for name, fields in MEASURES.items()})


def summarize_events(variants):
    dimensions = ("plan_tier", "industry", "hq_region", "state", "created_at")
    aggregations = [
        F.count("*").alias("source_occurrences"), provenance("schema_version"),
        F.sort_array(F.collect_set("schema_version")).alias("schema_versions"),
        F.min(F.struct(*CORE, *dimensions)).alias("_dimensions"),
        F.countDistinct(F.struct(*CORE)).alias("_core_variants"),
        F.max((F.size("_event_issues") > 0).cast("int")).alias("_invalid_core"),
    ]
    for name, fields in MEASURES.items():
        payload = F.col(f"_{name}")
        if name in ("carbon", "tokens"):
            # Un campo opcional ausente no contradice un valor reportado.
            payload = F.when(F.col(fields[0]).isNotNull() | payload.invalid_raw.isNotNull(), payload)
        aggregations.extend([F.min(payload).alias(f"_{name}"),
                             F.countDistinct(payload).alias(f"_{name}_variants")])
    return variants.where(F.col("event_id").isNotNull()).groupBy("event_id").agg(*aggregations)


def silver_events(summary):
    safe = summary.where("_invalid_core = 0 AND _core_variants = 1")
    statuses, values = [], []
    for name, fields in MEASURES.items():
        payload = F.col(f"_{name}")
        variants = F.col(f"_{name}_variants")
        valid = (variants == 1) & (F.size(payload.issues) == 0)
        statuses.append(F.when(variants == 0, "absent").when(valid, "valid")
                        .otherwise("quarantined").alias(f"{name}_status"))
        values.extend(F.when(valid, payload[field]).alias(field) for field in fields)
    return (safe.select("event_id", "_dimensions.*", "schema_versions", "origins", "source_occurrences",
                        *statuses, *values)
            .withColumns({"event_date": F.to_date("event_time"),
                          "requests": F.when(F.col("metric") == "requests", F.col("value")),
                          "cpu_hours": F.when(F.col("metric") == "cpu_hours", F.col("value")),
                          "storage_gb_hours": F.when(F.col("metric") == "storage_gb_hours", F.col("value"))}))


def event_incidents(variants, summary):
    decisions = summary.select("event_id", "_invalid_core", "_core_variants",
                               *[f"_{name}_variants" for name in MEASURES])
    frame = variants.join(decisions, "event_id", "left")
    scopes = [F.struct(F.lit("event").alias("scope"), F.concat("_event_issues", reasons(
        issue(F.col("_core_variants") > 1, "conflicting_event_dimensions"),
        issue((F.col("_invalid_core") == 1) & (F.size("_event_issues") == 0),
              "invalid_event_variant"))).alias("codes"))]
    for name in MEASURES:
        reported = F.lit(True)
        if name in ("carbon", "tokens"):
            reported = F.col(MEASURES[name][0]).isNotNull() | F.col(f"_{name}.invalid_raw").isNotNull()
        codes = F.concat(f"_{name}_issues", reasons(
            issue((F.col(f"_{name}_variants") > 1) & reported, "conflicting_measure")))
        scopes.append(F.struct(F.lit(name).alias("scope"),
                               F.when((F.col("_invalid_core") == 0) & (F.col("_core_variants") == 1), codes)
                               .alias("codes")))
    expanded = frame.withColumn("_incidence", F.explode(F.array(*scopes)))
    return incidents(expanded, "usage_events", "event_id", F.col("_incidence.scope"), F.col("_incidence.codes"))
