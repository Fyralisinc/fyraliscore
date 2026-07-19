#!/usr/bin/env python3
"""Independently score a frozen Stage 1 company-memory execution."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.epistemic_repair.core_fast_path_gold import build_core_fast_path_gold
from lib.evaluation.epistemic_repair.core_fast_path_population import build_core_fast_path_population
from lib.evaluation.epistemic_repair.p6_population import build_p6_population
from services.evaluation.epistemic_repair.stage1_quality_scorer import score_stage1_company_memory


def _score(raw: dict) -> dict:
    cf2 = build_core_fast_path_population()
    if raw.get("population_digest") == cf2.population_digest:
        gold = build_core_fast_path_gold()
        scope = {row.signal_id: row.canonical_ref for row in gold.signals}
        expected = frozenset(row.signal_id for row in gold.signals if row.canonical_ref)
        return score_stage1_company_memory(
            raw, signals=cf2.signals, expected_scope_by_signal=scope,
            expected_claim_signal_ids=expected,
            synthesis_signal_id=gold.synthesis_signal_id,
            expected_synthesis=gold.expected_thesis,
            correction_signal_id=gold.correction_signal_id,
            expected_correction=gold.expected_corrected_thesis,
        )
    p6 = build_p6_population()
    if raw.get("population_digest") == p6.population_digest:
        scope = {row.signal_id: row.canonical_ref for row in p6.gold}
        expected = frozenset(row.signal_id for row in p6.gold if row.storyline_id)
        return score_stage1_company_memory(
            raw, signals=p6.signals, expected_scope_by_signal=scope,
            expected_claim_signal_ids=expected,
        )
    raise ValueError("unsupported Stage 1 population digest")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = json.loads(args.raw.read_text(encoding="utf-8"))
    if not raw.get("complete"):
        parser.error("raw execution is incomplete")
    report = _score(raw)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(
        f"minimum_measured_score={report['minimum_measured_score']} "
        f"all_dimensions_measured={str(report['all_dimensions_measured']).lower()} "
        f"output={args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
