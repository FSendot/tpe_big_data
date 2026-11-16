"""Superset explícito v1/v2. JSON original conservado antes de tipificar."""

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

VERSION = "usage-bronze-v1"
FIELDS = {
    "event_id": "string", "timestamp": "timestamp", "org_id": "string",
    "resource_id": "string", "service": "string", "region": "string",
    "metric": "string", "value": "decimal(38,12)", "unit": "string",
    "cost_usd_increment": "decimal(38,12)", "schema_version": "int",
    "carbon_kg": "decimal(38,12)", "genai_tokens": "bigint",
}
JSON_SCHEMA = StructType([StructField(name, StringType(), True) for name in FIELDS]
                         + [StructField("_corrupt_record", StringType(), True)])


def parse_usage(lines):
    frame = lines.withColumnRenamed("value", "raw_json").withColumn(
        "raw", F.from_json("raw_json", JSON_SCHEMA, {"mode": "PERMISSIVE"}))
    issues = [F.when(F.col("raw").isNull() | F.col("raw._corrupt_record").isNotNull(), F.lit("malformed_json"))]
    for name, dtype in FIELDS.items():
        value = F.col(f"raw.{name}")
        if dtype == "string":
            parsed = value
        elif dtype == "timestamp":
            # UTC explícito o offset ISO; no depender de zona horaria del equipo.
            parsed = F.when(value.rlike(r"^\d{4}-\d{2}-\d{2}T.*(Z|[+-]\d{2}:\d{2})$"),
                            F.expr(f"try_cast(raw.`{name}` as timestamp)"))
        elif dtype in {"int", "bigint"}:
            parsed = F.when(value.rlike(r"^[+-]?\d+$"), F.expr(f"try_cast(raw.`{name}` as {dtype})"))
        else:
            fraction = F.regexp_extract(value, r"^[+-]?\d+\.(\d+)", 1)
            exponent_text = F.regexp_extract(value, r"[eE]([+-]?\d+)$", 1)
            exponent = F.when(exponent_text == "", F.lit(0)).otherwise(exponent_text.cast("long"))
            # El lector JSON puede representar doubles pequeños como 8.9E-5.
            # Comprobar escala efectiva antes de castearlos, sin redondear.
            fits_scale = (F.length(F.regexp_replace(fraction, "0+$", "")) - exponent) <= 12
            parsed = F.when(value.rlike(r"^[+-]?\d+(\.\d+)?([eE][+-]?\d+)?$") & fits_scale,
                            F.expr(f"try_cast(raw.`{name}` as {dtype})"))
        frame = frame.withColumn(name, parsed.cast(dtype))
        if dtype != "string":
            issues.append(F.when(value.isNotNull() & (value != "") & F.col(name).isNull(), F.lit(f"invalid_{name}")))
    issues.extend([
        F.when(F.col("event_id").isNull() | (F.trim("event_id") == ""), F.lit("missing_event_id")),
        F.when(F.col("timestamp").isNull(), F.lit("missing_or_invalid_timestamp")),
        F.when(F.col("schema_version").isNull() | ~F.col("schema_version").isin(1, 2), F.lit("unknown_schema_version")),
    ])
    frame = frame.withColumn("parse_errors", F.filter(F.array(*issues), lambda item: item.isNotNull()))
    # Equivalencia por valores interpretados; una conversión inválida no debe
    # ocultar diferencias entre valores raw que hayan terminado ambos en null.
    signature = F.when(F.size("parse_errors") == 0,
                       F.to_json(F.struct(*[F.col(name) for name in FIELDS]), {"ignoreNullFields": "false"}))
    frame = frame.withColumn("content_hash", F.sha2(signature.otherwise(F.col("raw_json")), 256))
    return (frame.withColumn("event_time", F.col("timestamp"))
            .withColumn("contract_version", F.lit(VERSION))
            .withColumn("ingest_ts", F.current_timestamp())
            .withColumn("ingest_date", F.current_date()))


def eligible(frame):
    return frame.filter(F.col("event_id").isNotNull() & (F.trim("event_id") != "")
                        & F.col("event_time").isNotNull() & F.col("schema_version").isin(1, 2)
                        & F.col("raw._corrupt_record").isNull())
