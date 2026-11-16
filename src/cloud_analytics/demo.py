"""Muestra sintética fija y rutas administradas para su ejecución local."""

import csv
from dataclasses import dataclass
from datetime import date, timedelta
from itertools import chain
import json
import os
from pathlib import Path
import re
import shutil
from tempfile import TemporaryDirectory

from cloud_analytics.bronze import fingerprint
from cloud_analytics.contracts import CONTRACTS


ORG = "demo-org"
START = date(2025, 7, 24)
REGION = "us-east"
SERVICES = ("compute", "genai", "storage")
COSTS = ("0", "9", "10", "11", "10", "12", "9", "11", "100", "10", "10", "10")
FORMAT = "finops-synthetic-v1"


class DemoError(ValueError):
    """Diagnóstico sin rutas de credenciales ni datos de negocio."""


def checked_path(path):
    if not path.is_absolute() or path.resolve() != path:
        raise DemoError("unsafe_demo_path")
    return path


@dataclass(frozen=True)
class Workspace:
    demo_id: str
    root: Path
    lock: Path

    @property
    def dataset(self):
        return self.root / "dataset"

    @property
    def reports(self):
        return self.root / "reports"

    @property
    def work(self):
        return self.root / "work"

    @property
    def owner(self):
        return self.root / "work-owner.json"


def workspace_for(settings, demo_id):
    if not re.fullmatch(r"[a-z][a-z0-9-]{0,47}", demo_id):
        raise DemoError("invalid_demo_id")
    area = checked_path(settings.lake_root / "demos")
    root = checked_path(area / demo_id)
    protected = (settings.dataset_root, settings.evidence_root, Path("/run/secrets"))
    if any(root == p or root.is_relative_to(p) or p.is_relative_to(root) for p in protected):
        raise DemoError("demo_overlaps_protected_path")
    workspace = Workspace(demo_id, root, checked_path(area / ".locks" / f"{demo_id}.lock"))
    for path in (workspace.dataset, workspace.reports, workspace.work, workspace.owner):
        checked_path(path)
    return workspace


def csv_rows():
    organization = {"org_id": ORG, "org_name": "Demo sintetica", "industry": "demo", "hq_region": REGION,
                    "plan_tier": "test", "is_enterprise": "false", "signup_date": "2025-07-01",
                    "lifecycle_stage": "active"}
    user = {"user_id": "demo-user", "org_id": ORG, "email": "demo@example.invalid",
            "role": "analyst", "active": "true", "created_at": "2025-07-01", "last_login": "2025-07-24"}
    invoice = {"invoice_id": "demo-invoice", "org_id": ORG, "month": "2025-07-01", "subtotal": "25.50",
               "credits": "0", "taxes": "0", "currency": "USD", "exchange_rate_to_usd": "1"}
    resources = [{"resource_id": f"demo-{service}", "org_id": ORG, "service": service, "region": REGION,
                  "created_at": "2025-07-01", "state": "active", "tags_json": "{}"} for service in SERVICES]
    return {"customers_orgs": [organization], "users": [user], "billing_monthly": [invoice], "resources": resources}


def day(offset):
    return (START + timedelta(days=offset)).isoformat()


def event(identity, offset, cost, *, service="compute", metric="requests", unit="count", value=1, version=2):
    row = {"event_id": identity, "timestamp": f"{day(offset)}T12:00:00Z", "org_id": ORG,
           "resource_id": f"demo-{service}", "service": service, "region": REGION,
           "metric": metric, "unit": unit, "value": value, "cost_usd_increment": cost, "schema_version": version}
    if version == 2:
        row["carbon_kg"] = "0.125"
    if service == "genai":
        row["genai_tokens"] = 128
    return row


def event_parts():
    daily = [event(f"demo-daily-{i:02d}", i, cost, value=100 + i, version=1 if i < 4 else 2)
             for i, cost in enumerate(COSTS)]
    extra = [
        event("demo-negative-cost", 9, "-0.5", value=3),
        event("demo-missing-unit", 10, "2.50", unit=None, value=2),
        event("demo-genai-event", 10, "2.75", service="genai", metric="cpu_hours", unit="hours", value="1.25"),
        event("demo-storage-event", 10, "1.50", service="storage",
              metric="storage_gb_hours", unit="gb_hours", value="6.5"),
    ]
    return daily[:6], [*daily[6:], *extra, daily[0]]


def read_manifest(workspace):
    dataset = checked_path(workspace.dataset)
    path = checked_path(dataset / "manifest.json")
    if not path.is_file():
        raise DemoError("demo_manifest_missing")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if (not isinstance(manifest, dict)
            or (manifest.get("demo_id"), manifest.get("format"), manifest.get("synthetic"))
            != (workspace.demo_id, FORMAT, True)):
        raise DemoError("demo_manifest_mismatch")
    current = sorted((p for p in dataset.rglob("*") if p != path and p.is_file()), key=str)
    entries = [{"path": p.relative_to(dataset).as_posix(), "sha256": fingerprint(checked_path(p))} for p in current]
    if entries != manifest.get("files"):
        raise DemoError("demo_dataset_mismatch")
    return manifest


def generate(workspace):
    if workspace.root.exists():
        return "reused", read_manifest(workspace)
    with TemporaryDirectory(prefix=".sample-", dir=workspace.root.parent) as directory:
        stage = Path(directory) / "demo"
        dataset = stage / "dataset"
        landing = dataset / "datalake" / "landing"
        stream = landing / "usage_events_stream"
        stream.mkdir(parents=True)
        sources = csv_rows()
        parts = event_parts()
        for name, rows in sources.items():
            with (landing / f"{name}.csv").open("w", encoding="utf-8", newline="") as output:
                writer = csv.DictWriter(output, fieldnames=CONTRACTS[name].names, lineterminator="\n")
                writer.writeheader()
                writer.writerows(rows)
        for index, rows in enumerate(parts, start=1):
            body = "".join(json.dumps(row, sort_keys=True) + "\n" for row in rows)
            (stream / f"part-{index:02d}.jsonl").write_bytes(body.encode("utf-8"))
        manifest = {
            "demo_id": workspace.demo_id, "format": FORMAT, "synthetic": True,
            "provenance": "Generated by cloud_analytics.demo. No Landing source rows.",
            "first_date": day(0), "last_date": day(len(COSTS) - 1),
            "csv_rows": {name: len(rows) for name, rows in sources.items()},
            "jsonl_rows": sum(map(len, parts)),
            "queries": {"org_id": ORG, "service": "compute", "start": day(6), "end": day(11), "detail": day(9)},
            "cases": {"duplicate": "demo-daily-00", "negative_cost": "demo-negative-cost",
                      "missing_unit": "demo-missing-unit", "peak_date": day(8), "zero_cost_date": day(0)},
            "files": [{"path": p.relative_to(dataset).as_posix(), "sha256": fingerprint(p)}
                      for p in sorted(dataset.rglob("*")) if p.is_file()],
        }
        (dataset / "manifest.json").write_bytes((json.dumps(manifest, indent=2) + "\n").encode("utf-8"))
        stage.rename(workspace.root)
    return "created", manifest


def check_owner(workspace, create=False):
    owner = checked_path(workspace.owner)
    expected = {"demo_id": workspace.demo_id, "work": str(workspace.work)}
    if create and not workspace.work.exists() and not owner.exists():
        owner.write_text(json.dumps(expected) + "\n", encoding="utf-8")
    if not owner.is_file() or json.loads(owner.read_text(encoding="utf-8")) != expected:
        raise DemoError("demo_work_not_owned")


def begin_work(workspace):
    check_owner(workspace, create=True)
    checked_path(workspace.work).mkdir(exist_ok=True)


def cleanup_plan(workspace):
    read_manifest(workspace)
    work = checked_path(workspace.work)
    delete = []
    if work.exists():
        check_owner(workspace)
        if not work.is_dir() or not shutil.rmtree.avoids_symlink_attacks:
            raise DemoError("unsupported_demo_cleanup_target")
        # ismount()/st_dev no alcanzan para bind mounts dentro del mismo filesystem.
        for line in Path("/proc/self/mountinfo").read_text(encoding="utf-8").splitlines():
            mount = Path(re.sub(r"\\([0-7]{3})", lambda m: chr(int(m[1], 8)), line.split()[4]))
            if mount.is_relative_to(work):
                raise DemoError("mounted_demo_work_tree")
        device = work.stat().st_dev
        for path in chain((work,), work.rglob("*")):
            info = path.lstat()
            if path.is_symlink() or info.st_dev != device or (path.is_dir() and os.path.ismount(path)):
                raise DemoError("unsafe_demo_work_tree")
        delete.append(str(work))
    return {"demo_id": workspace.demo_id, "delete_local": delete,
            "preserve_local": [str(workspace.dataset), str(workspace.reports), str(workspace.owner)],
            "astra": "preserved_all_publication_rows_and_markers"}


def clean(workspace, apply=False):
    result = cleanup_plan(workspace)
    if apply and result["delete_local"]:
        shutil.rmtree(workspace.work)
    return result
