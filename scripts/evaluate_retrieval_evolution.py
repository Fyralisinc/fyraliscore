#!/usr/bin/env python3
"""Evaluate saved batch artifacts against retrieval-evolution policy v1."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from lib.evaluation.retrieval_evolution import evaluate_retrieval_evolution


def _batches(payload: dict[str, Any]) -> list[dict[str, Any]]:
    batches: list[dict[str, Any]] = []
    for index, wave in enumerate(payload.get("waves") or [], start=1):
        run = (
            ((wave.get("t1_batch") or {}).get("run") or {})
            or ((wave.get("execution") or {}).get("run") or {})
        )
        if run:
            batches.append({"sequence": wave.get("sequence", index), **run})
    return batches


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("benchmark", type=Path)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    payload = json.loads(args.benchmark.read_text())
    report = evaluate_retrieval_evolution(_batches(payload))
    rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered)
    else:
        print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
