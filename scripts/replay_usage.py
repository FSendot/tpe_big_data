"""Ofrece copias completas de los JSONL, sin modificar ni ordenar eventos."""

import argparse
import json
from cloud_analytics.config import load_settings
from cloud_analytics.usage_stream import replay


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--stream-id", default="usage-v1")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--interval", type=float, default=0, help="Segundos entre archivos nuevos")
    args = parser.parse_args()
    settings = load_settings()
    result = replay(settings, args.stream_id, args.limit, args.interval)
    output = settings.evidence_root / "streaming" / f"replay-{args.stream_id}.json"
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"offered_files": result["offered_files"], "already_offered_files": result["already_offered_files"]}))


if __name__ == "__main__":
    main()
