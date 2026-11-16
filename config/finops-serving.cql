CREATE TABLE IF NOT EXISTS org_daily_usage_by_service (
    publication_id text,
    org_id text,
    service text,
    usage_month date,
    event_date date,
    silver_publication_id text,
    silver_event_count bigint,
    unassigned_usage_events bigint,
    excluded_event_ids bigint,
    excluded_unidentified_fingerprints bigint,
    daily_cost_usd decimal,
    requests decimal,
    cpu_hours decimal,
    storage_gb_hours decimal,
    carbon_kg decimal,
    genai_tokens decimal,
    daily_cost_usd_coverage tuple<bigint, bigint, bigint, bigint, double, text>,
    requests_coverage tuple<bigint, bigint, bigint, bigint, double, text>,
    cpu_hours_coverage tuple<bigint, bigint, bigint, bigint, double, text>,
    storage_gb_hours_coverage tuple<bigint, bigint, bigint, bigint, double, text>,
    carbon_kg_coverage tuple<bigint, bigint, bigint, bigint, double, text>,
    genai_tokens_coverage tuple<bigint, bigint, bigint, bigint, double, text>,
    PRIMARY KEY ((publication_id, org_id, service, usage_month), event_date)
) WITH CLUSTERING ORDER BY (event_date ASC);

CREATE TABLE IF NOT EXISTS finops_publications (
    publication_id text PRIMARY KEY,
    source_sha256 text,
    serving_schema_sha256 text,
    expected_rows bigint,
    verified_rows bigint,
    verified_at timestamp,
    state text
);
