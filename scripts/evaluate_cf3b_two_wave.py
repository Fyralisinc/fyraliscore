#!/usr/bin/env python3
"""Evaluate a bounded two-wave CF3-B artifact without full-P6 overclaim."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.evaluation.epistemic_repair.cf3b_two_wave import (
    evaluate_cf3b_two_wave,
)
from services.evaluation.epistemic_repair.p6_think_runner import _write_checkpoint


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("artifact", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.artifact.read_text(encoding="utf-8"))
    report = evaluate_cf3b_two_wave(payload)
    _write_checkpoint(args.output, report)
    print(
        f"verdict={report['verdict']} "
        f"failed_gates={','.join(report['failed_gates']) or 'none'} "
        f"output={args.output}"
    )
    return 0 if report["verdict"] == "green" else 1


if __name__ == "__main__":
    raise SystemExit(main())
