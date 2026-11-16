"""Genera la muestra sintética y limpia sólo el estado local de un ID demo."""

import argparse
import json

from cloud_analytics.config import load_settings
from cloud_analytics.demo import DemoError, clean, cleanup_plan, generate, workspace_for
from cloud_analytics.usage_stream import lock_at


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("generate").add_argument("--demo-id", required=True)
    cleanup = commands.add_parser("clean")
    cleanup.add_argument("--demo-id", required=True)
    flags = cleanup.add_mutually_exclusive_group()
    flags.add_argument("--dry-run", action="store_true", help="Previsualizar el plan (por defecto)")
    flags.add_argument("--apply", action="store_true", help="Eliminar sólo el estado local indicado")
    args = parser.parse_args()
    try:
        workspace = workspace_for(load_settings(), args.demo_id)
        with lock_at(workspace.lock):
            if args.command == "generate":
                status, manifest = generate(workspace)
                result = {"status": status, "dataset": str(workspace.dataset), "manifest": manifest}
            else:
                plan = cleanup_plan(workspace)
                print(json.dumps({"status": "dry-run", **plan}, indent=2), flush=True)
                if not args.apply:
                    return 0
                result = {"status": "cleaned", **clean(workspace, apply=True)}
    except (DemoError, OSError, ValueError) as error:
        result = {"status": "failed", "error_type": type(error).__name__}
        if isinstance(error, DemoError):
            result["error_code"] = str(error)
        print(json.dumps(result))
        return 1
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
