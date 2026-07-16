#!/usr/bin/env python3
"""Evaluate observed Slack reconstruction results against sealed gold."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from lib.evaluation.slack_reconstruction_gold import (
    evaluate_slack_reconstruction,
    load_slack_reconstruction_gold,
    load_slack_reconstruction_observations,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_GOLD = (
    ROOT
    / "tests"
    / "fixtures"
    / "company_learning"
    / "slack_reconstruction_gold_v1.jsonl"
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    cases = load_slack_reconstruction_gold(args.gold)
    observations = load_slack_reconstruction_observations(args.observed_jsonl)
    report = evaluate_slack_reconstruction(
        cases=cases,
        observations=observations,
        run_id=args.run_id,
        system_version=args.system_version,
        artifact_refs=(
            f"gold:{args.gold.resolve()}",
            f"observations:{args.observed_jsonl.resolve()}",
        ),
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_path = args.output_dir / "slack_reconstruction_gold_report.json"
    payload = {
        "report": report.model_dump(mode="json"),
        "report_digest": report.digest,
    }
    output_path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(f"report={output_path}")
    print(
        "status={status} cases={cases} correct_rate={correct} "
        "recall={recall} contamination={contamination} "
        "abstention={abstention}".format(
            status=report.status,
            cases=report.metrics.case_count,
            correct=report.metrics.correct_case_rate,
            recall=report.metrics.mean_sufficient_set_recall,
            contamination=report.metrics.contamination_rate,
            abstention=report.metrics.abstention_under_insufficiency_rate,
        )
    )
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    parser = argparse.ArgumentParser(
        description=(
            "Score one complete observed Slack reconstruction population "
            "against sealed sufficient-context and contamination gold."
        )
    )
    parser.add_argument("--gold", type=Path, default=DEFAULT_GOLD)
    parser.add_argument("--observed-jsonl", type=Path, required=True)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("reports") / f"slack-reconstruction-gold-{timestamp}",
    )
    parser.add_argument(
        "--run-id",
        default=f"slack-reconstruction-gold-{timestamp}",
    )
    parser.add_argument("--system-version", default="local-working-tree")
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
