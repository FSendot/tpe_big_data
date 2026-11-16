"""Contratos Bronze v1: orden CSV y tipos de las cuatro fuentes batch."""

from dataclasses import dataclass
import hashlib
import json

from pyspark.sql.types import StringType, StructField, StructType


@dataclass(frozen=True)
class CsvContract:
    key: str
    fields: tuple

    @property
    def names(self):
        return [name for name, _ in self.fields]

    @property
    def raw_schema(self):
        return StructType([StructField(name, StringType(), True) for name in self.names])


CONTRACT_VERSION = "bronze-csv-v1"
CONTRACTS = {
    "customers_orgs": CsvContract("org_id", (
        ("org_id", "string"), ("org_name", "string"), ("industry", "string"),
        ("hq_region", "string"), ("plan_tier", "string"), ("is_enterprise", "boolean"),
        ("signup_date", "date"), ("sales_rep", "string"), ("lifecycle_stage", "string"),
        ("marketing_source", "string"), ("nps_score", "decimal(38,12)"))),
    "users": CsvContract("user_id", (
        ("user_id", "string"), ("org_id", "string"), ("email", "string"),
        ("role", "string"), ("active", "boolean"), ("created_at", "date"), ("last_login", "date"))),
    "billing_monthly": CsvContract("invoice_id", (
        ("invoice_id", "string"), ("org_id", "string"), ("month", "date"),
        ("subtotal", "decimal(38,12)"), ("credits", "decimal(38,12)"),
        ("taxes", "decimal(38,12)"), ("currency", "string"),
        ("exchange_rate_to_usd", "decimal(38,12)"))),
    "resources": CsvContract("resource_id", (
        ("resource_id", "string"), ("org_id", "string"), ("service", "string"),
        ("region", "string"), ("created_at", "date"), ("state", "string"), ("tags_json", "string"))),
}


def contract_hash():
    payload = {name: {"key": c.key, "fields": c.fields} for name, c in CONTRACTS.items()}
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
