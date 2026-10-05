"""Exploración inicial del paquete del campus, sin modificar sus archivos."""

import argparse
import csv
import hashlib
import json
import platform
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path


CSV_KEYS = {
    "customers_orgs": ["org_id"],
    "users": ["user_id"],
    "resources": ["resource_id"],
    "support_tickets": ["ticket_id"],
    "marketing_touches": ["touch_id"],
    "nps_surveys": ["org_id", "survey_date"],
    "billing_monthly": ["invoice_id"],
}


def missing(value):
    return value is None or value == ""


def number(value):
    if missing(value) or isinstance(value, bool):
        return None
    try:
        parsed = Decimal(str(value))
        return parsed if parsed.is_finite() else None
    except InvalidOperation:
        return None


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for block in iter(lambda: source.read(65536), b""):
            digest.update(block)
    return digest.hexdigest()


def profile(package):
    landing = package / "datalake" / "landing"
    csv_paths = [landing / (name + ".csv") for name in CSV_KEYS]
    events_paths = sorted((landing / "usage_events_stream").glob("*.jsonl"))
    if not events_paths or any(not path.is_file() for path in csv_paths):
        raise ValueError("Se requieren los siete CSV y al menos un JSONL en datalake/landing.")
    paths = sorted(csv_paths + events_paths)
    manifest = [
        {"path": path.relative_to(package).as_posix(), "bytes": path.stat().st_size,
         "sha256": fingerprint(path)}
        for path in paths
    ]
    tables = {}
    csv_profiles = {}
    for path in csv_paths:
        with path.open(encoding="utf-8-sig", newline="") as source:
            reader = csv.DictReader(source)
            fields = reader.fieldnames
            if not fields or len(fields) != len(set(fields)):
                raise ValueError("Encabezado ausente o repetido en " + path.name)
            if not set(CSV_KEYS[path.stem]).issubset(fields):
                raise ValueError("Falta una columna clave en " + path.name)
            rows = list(reader)
        if any(None in row or any(value is None for value in row.values()) for row in rows):
            raise ValueError("Cantidad de columnas inconsistente en " + path.name)
        keys = [tuple(row[key] for key in CSV_KEYS[path.stem]) for row in rows]
        tables[path.stem] = rows
        csv_profiles[path.name] = {
            "rows": len(rows), "columns": fields,
            "empty_by_column": {field: sum(missing(row[field]) for row in rows) for field in fields},
            "key_columns": CSV_KEYS[path.stem],
            "duplicate_key_rows": len(keys) - len(set(keys)),
            "missing_key_rows": sum(any(missing(value) for value in key) for key in keys),
        }

    orgs = {row["org_id"] for row in tables["customers_orgs"]}
    resources = {row["resource_id"]: row for row in tables["resources"]}
    for name, rows in tables.items():
        if name != "customers_orgs":
            csv_profiles[name + ".csv"]["unknown_org_rows"] = sum(row["org_id"] not in orgs for row in rows)

    counts = Counter()
    ids, signatures, groups = set(), {}, {}
    dates, versions, services = Counter(), Counter(), Counter()
    types, present, nulls = {}, Counter(), Counter()
    timestamps = []
    file_date_counts = []
    for path in events_paths:
        file_dates = set()
        with path.open(encoding="utf-8") as source:
            for line_number, line in enumerate(source, 1):
                try:
                    row = json.loads(line)
                except ValueError:
                    raise ValueError(f"JSON inválido en {path.name}:{line_number}") from None
                if not isinstance(row, dict):
                    raise ValueError(f"Se esperaba un objeto en {path.name}:{line_number}")
                counts["rows"] += 1
                for field, value in row.items():
                    present[field] += 1
                    nulls[field] += missing(value)
                    types.setdefault(field, Counter())[type(value).__name__] += 1
                event_id = row.get("event_id")
                if not isinstance(event_id, str) or not event_id.strip():
                    counts["invalid_event_id"] += 1
                else:
                    signature = hashlib.sha256(json.dumps(row, sort_keys=True).encode()).hexdigest()
                    if event_id in ids:
                        counts["duplicate_event_id_rows"] += 1
                        counts["conflicting_with_first_rows"] += signatures[event_id] != signature
                    else:
                        signatures[event_id] = signature
                    ids.add(event_id)
                try:
                    timestamp = datetime.fromisoformat(row["timestamp"].replace("Z", "+00:00"))
                    if timestamp.tzinfo is None:
                        raise ValueError("Falta zona horaria")
                    timestamp = timestamp.astimezone(timezone.utc)
                except (KeyError, TypeError, AttributeError, ValueError):
                    counts["invalid_timestamp"] += 1
                else:
                    day = timestamp.date().isoformat()
                    timestamps.append(timestamp)
                    dates[day] += 1
                    file_dates.add(day)
                    groups.setdefault((row.get("org_id"), row.get("service")), set()).add(day)
                versions[str(row.get("schema_version"))] += 1
                services[str(row.get("service"))] += 1
                counts["unknown_org"] += row.get("org_id") not in orgs
                resource = resources.get(row.get("resource_id"))
                counts["unknown_resource"] += resource is None
                counts["resource_dimension_mismatch"] += resource is not None and any(
                    row.get(field) != resource[field] for field in ("org_id", "service", "region")
                )
                cost, value = number(row.get("cost_usd_increment")), number(row.get("value"))
                counts["invalid_cost"] += cost is None
                counts["negative_cost"] += cost is not None and cost < 0
                counts["value_null"] += missing(row.get("value"))
                counts["value_numeric_string"] += isinstance(row.get("value"), str) and value is not None
                counts["unit_null"] += missing(row.get("unit"))
                counts["nonnegative_cost_with_incomplete_consumption"] += (
                    cost is not None and cost >= 0 and (missing(row.get("value")) or missing(row.get("unit")))
                )
                counts["carbon_present"] += not missing(row.get("carbon_kg"))
                counts["genai_tokens_present"] += not missing(row.get("genai_tokens"))
        file_date_counts.append(len(file_dates))
    for key in ("invalid_event_id", "duplicate_event_id_rows", "conflicting_with_first_rows", "invalid_timestamp"):
        counts.setdefault(key, 0)

    billing = tables["billing_monthly"]
    billing_pairs = {(row["org_id"], row["month"]) for row in billing}
    result = {
        "python": platform.python_version(),
        "script_sha256": fingerprint(Path(__file__)),
        "input_files": len(paths), "input_bytes": sum(item["bytes"] for item in manifest),
        "csv": csv_profiles,
        "events": {
            "files": len(events_paths), **counts, "unique_event_ids": len(ids),
            "timestamp_min": min(timestamps).isoformat() if timestamps else None,
            "timestamp_max": max(timestamps).isoformat() if timestamps else None,
            "unique_timestamps": len(set(timestamps)), "rows_by_date": dict(sorted(dates.items())),
            "dates_per_file_min": min(file_date_counts), "dates_per_file_max": max(file_date_counts),
            "org_service_groups": len(groups),
            "groups_missing_observed_dates": sum(len(days) < len(dates) for days in groups.values()),
            "schema_versions": dict(sorted(versions.items())), "services": dict(sorted(services.items())),
            "columns": {field: {"json_types": dict(sorted(types[field].items())),
                        "null_or_empty": nulls[field], "absent": counts["rows"] - present[field]}
                        for field in sorted(types)},
        },
        "billing": {
            "currencies": dict(sorted(Counter(row["currency"] for row in billing).items())),
            "months": sorted({row["month"] for row in billing}),
            "duplicate_org_month_rows": len(billing) - len(billing_pairs),
            "negative_subtotal": sum(number(row["subtotal"]) is not None and number(row["subtotal"]) < 0 for row in billing),
            "invalid_or_nonpositive_fx": sum(number(row["exchange_rate_to_usd"]) is None or number(row["exchange_rate_to_usd"]) <= 0 for row in billing),
            "usd_fx_not_one": sum(row["currency"] == "USD" and number(row["exchange_rate_to_usd"]) != 1 for row in billing),
        },
        "manifest": manifest,
    }
    if any(fingerprint(path) != item["sha256"] for path, item in zip(paths, manifest)):
        raise ValueError("Las entradas cambiaron durante la lectura; repetir con una copia estable.")
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("dataset", type=Path, help="Raíz del paquete que contiene datalake/landing")
    parser.add_argument("--output", type=Path, required=True, help="JSON de evidencia, fuera del paquete")
    args = parser.parse_args()
    package = args.dataset.resolve()
    output = args.output.resolve()
    if output == package or package in output.parents:
        parser.error("La salida debe quedar fuera del dataset original.")
    try:
        result = profile(package)
    except (OSError, ValueError) as error:
        parser.exit(1, f"No se generó evidencia: {error}\n")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Leídos {result['input_files']} archivos; {result['events']['rows']} eventos. Evidencia: {args.output}")


if __name__ == "__main__":
    main()
