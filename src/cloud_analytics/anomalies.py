"""Detecta picos diarios de costo sobre un único corte Gold verificado."""

from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from decimal import Decimal, localcontext
import hashlib
import json
import math
from pathlib import Path
import re
from statistics import median
import tempfile

from pyspark.sql import Window, functions as F

from cloud_analytics import gold, silver
from cloud_analytics.bronze import fingerprint
from cloud_analytics.usage_stream import lock_at


MART = "cost_anomaly_mart"
METHOD = "rolling_modified_z_score_upper"
MAD_FACTOR = Decimal("0.6745")
EXCLUSION_COUNTS = ("unlocated_event_ids", "unlocated_unidentified_fingerprints", "unlocated_rows_without_identity")
RESULT_SCHEMA = """evaluation_reason string, is_anomaly boolean, historical_median_usd double,
                   historical_mad_usd double, upper_threshold_usd double, robust_score double"""


class AnomalyError(ValueError):
    """Diagnóstico que puede publicarse en evidencia sin datos de origen."""


@dataclass(frozen=True)
class Parameters:
    window_days: int = 28
    min_observations: int = 7
    score_threshold: float = 3.5

    def __post_init__(self):
        if not 2 <= self.min_observations <= self.window_days:
            raise AnomalyError("invalid_history_parameters")
        # El umbral diagnóstico double debe ser representable incluso con
        # los costos máximos del contrato decimal(38,12) de Gold.
        if not self.score_threshold > 0 or not math.isfinite(self.score_threshold * 1e27):
            raise AnomalyError("invalid_score_threshold")


def source_limitation(manifest):
    quality = manifest.get("quality")
    exclusions = quality.get("exclusions") if isinstance(quality, dict) else None
    if not isinstance(exclusions, dict):
        return "gold_exclusion_metadata_unverified"
    flag = exclusions.get("has_unlocated_exclusions")
    if flag is True:
        return "unlocated_gold_exclusions"
    counts = [exclusions.get(key) for key in EXCLUSION_COUNTS]
    if flag is not False or any(type(count) is not int or count != 0 for count in counts):
        return "gold_exclusion_metadata_unverified"
    return None


def score_observation(cost, history, blocked, minimum, threshold):
    if blocked:
        return blocked, None, None, None, None, None
    if len(history) < minimum:
        return "insufficient_history", None, None, None, None, None
    # El flag se decide con aritmética decimal, antes de convertir los
    # valores de diagnóstico a double para el Parquet.
    with localcontext() as context:
        context.prec = 80
        reference = median(history)
        mad = median(abs(value - reference) for value in history)
        if mad == 0:
            return "zero_historical_mad", None, float(reference), 0.0, None, None
        delta = (cost - reference) * MAD_FACTOR
        is_anomaly = delta > threshold * mad
        limit = reference + threshold * mad / MAD_FACTOR
        score = delta / mad
    reason = "above_threshold" if is_anomaly else "at_or_below_threshold"
    return reason, is_anomaly, float(reference), float(mad), float(limit), float(score)


def evaluate(gold_rows, parameters, source_block=None):
    observed = gold_rows.select(*gold.GRAIN, "daily_cost_usd",
                                F.col("daily_cost_usd_coverage.state").alias("cost_coverage_state"))
    state = F.col("cost_coverage_state")
    cost = F.col("daily_cost_usd")
    if source_block:
        block = F.lit(source_block)
        history = F.array().cast("array<decimal(38,12)>")
    else:
        block = (F.when(state.isin("partial", "unavailable", "absent", "not_applicable"), "incomplete_cost_coverage")
                 .when(state.isNull() | (state != "complete"), "gold_coverage_state_unknown")
                 .when(cost.isNull() | (cost < 0), "gold_complete_cost_invalid"))
        preceding_days = (Window.partitionBy("org_id", "service")
                          .orderBy(F.datediff("event_date", F.lit(date(1970, 1, 1))))
                          .rangeBetween(-parameters.window_days, -1))
        history = F.collect_list(F.when(block.isNull(), cost)).over(preceding_days)
    threshold = Decimal(str(parameters.score_threshold))
    scorer = F.udf(lambda value, past, reason: score_observation(
        value, past, reason, parameters.min_observations, threshold), RESULT_SCHEMA)
    return (observed.withColumn("_history", history)
            .withColumn("_assessment", scorer(cost, F.col("_history"), block))
            .select("*", "_assessment.*", F.size("_history").alias("history_observations"))
            .drop("_assessment", "_history")
            .withColumn("evaluation_state", F.when(F.col("is_anomaly").isNotNull(), "evaluated")
                        .otherwise("not_evaluable")))


def verify_grain(rows, expected):
    if rows.where(F.col("org_id").isNull() | F.col("event_date").isNull() | F.col("service").isNull()).limit(1).count():
        raise AnomalyError("invalid_gold_grain")
    counts = rows.agg(F.count("*").alias("rows"), F.countDistinct(*gold.GRAIN).alias("keys")).first()
    if counts.rows != expected or counts.keys != expected:
        raise AnomalyError("gold_grain_count_mismatch")


def build_mart(spark, stage, source, source_manifest, parameters):
    rows = spark.read.parquet(str(source / gold.MART)).select(*gold.GRAIN, "daily_cost_usd", "daily_cost_usd_coverage")
    verify_grain(rows, source_manifest["tables"][gold.MART]["rows"])
    blocked = source_limitation(source_manifest)
    result = evaluate(rows, parameters, blocked).withColumns({
        "gold_publication_id": F.lit(source_manifest["publication_id"]),
        "method": F.lit(METHOD),
        "parameters": F.struct(*(F.lit(value).alias(name) for name, value in asdict(parameters).items()),
                               F.lit(float(MAD_FACTOR)).alias("mad_factor")),
    })
    restored, table = silver.write_table(spark, stage, MART, result, "event_date")
    counts = restored.agg(
        gold.count_if(F.col("is_anomaly").isNotNull()).alias("evaluated_rows"),
        gold.count_if(F.col("is_anomaly").isNull()).alias("not_evaluable_rows"),
        gold.count_if(F.col("is_anomaly")).alias("anomalies"),
    ).first().asDict()
    stats = {**counts, "origin_quality_limitation": blocked,
             "by_reason": silver.distribution(restored, "evaluation_state", "evaluation_reason", "cost_coverage_state")}
    return table, stats


def publish_anomalies(spark, settings, gold_publication_id, parameters):
    if not re.fullmatch(r"[0-9a-f]{64}", gold_publication_id):
        raise AnomalyError("invalid_gold_publication_id")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise AnomalyError("anomalies_require_utc")
    source = settings.lake_root / "gold" / f"publication_id={gold_publication_id}"
    manifest_path = source / "manifest.json"
    with lock_at(settings.lake_root / "control" / "anomalies.lock"):
        if not manifest_path.is_file():
            raise AnomalyError("gold_manifest_missing")
        source_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if (source_manifest["publication_id"] != gold_publication_id
                or silver.digest(source_manifest["inputs"]) != gold_publication_id):
            raise AnomalyError("gold_publication_identity_mismatch")
        if (source_manifest["inputs"].get("contract") != "finops-gold-v1"
                or source_manifest["inputs"].get("timezone") != "UTC"
                or source_manifest.get("grain") != list(gold.GRAIN)
                or set(source_manifest["tables"]) != {gold.MART}):
            raise AnomalyError("unsupported_gold_manifest")
        source_manifest = silver.verify_publication(source, source_manifest["inputs"])
        identity = {
            "contract": "cost-anomalies-v1", "method": METHOD, "mad_factor": str(MAD_FACTOR),
            "parameters": asdict(parameters), "gold_publication_id": gold_publication_id,
            "gold_manifest_sha256": fingerprint(manifest_path),
            "spark_version": spark.version, "timezone": "UTC",
            "implementation_sha256": {
                path.name: hashlib.sha256(path.read_bytes().replace(b"\r\n", b"\n")).hexdigest()
                for path in (Path(__file__), Path(gold.__file__), Path(silver.__file__))},
        }
        publication_id = silver.digest(identity)
        target = settings.lake_root / "gold" / "anomalies" / f"publication_id={publication_id}"
        if target.exists():
            return {"status": "reused", "publication_path": str(target),
                    "manifest": silver.verify_publication(target, identity)}
        staging = settings.lake_root / ".anomaly-staging"
        staging.mkdir(parents=True, exist_ok=True)
        target.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="publication-", dir=staging) as directory:
            stage = Path(directory)
            table, stats = build_mart(spark, stage, source, source_manifest, parameters)
            manifest = {"publication_id": publication_id, "inputs": identity, "grain": list(gold.GRAIN),
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "tables": {MART: table}, "quality": stats, "files": silver.publication_files(stage)}
            (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
            silver.verify_publication(stage, identity)
            stage.rename(target)
    return {"status": "created", "publication_path": str(target), "manifest": manifest}
