#!/usr/bin/env python3
"""Evaluate normalized persisted-signal batch outcomes across source families."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lib.evaluation.source_equivalence import evaluate_normalized_source_equivalence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text())
    rows = payload.get("source_batches") if isinstance(payload, dict) else payload
    report = evaluate_normalized_source_equivalence(rows)
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    print(rendered, end="")
    return 0 if report["verdict"] == "meets_policy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
