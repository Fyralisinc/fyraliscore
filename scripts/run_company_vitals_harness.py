#!/usr/bin/env python3
"""Render Company Understanding vitals for an existing E2E report directory."""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.company_vitals import (
    collect_db_trace_for_report_dir,
    render_vitals_markdown,
    write_vitals_artifacts,
)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    db_trace = None
    if args.database_url:
        db_trace = asyncio.run(
            collect_db_trace_for_report_dir(
                args.report_dir,
                database_url=args.database_url,
                tenant_id=args.tenant_id,
            )
        )
    result = write_vitals_artifacts(
        args.report_dir,
        output_dir=args.output_dir,
        db_trace=db_trace,
    )
    if args.print_summary:
        sys.stdout.write(render_vitals_markdown(result.scorecard))
    else:
        print(f"wrote vitals artifacts to {result.output_dir}")
        print(
            "status={status} overall_score={score} hard_failures={failures}".format(
                status=result.scorecard.get("status"),
                score=result.scorecard.get("overall_score"),
                failures=len(result.scorecard.get("hard_failures") or []),
            )
        )
    if args.fail_on_hard_gates and result.scorecard.get("hard_failures"):
        return 1
    min_score = args.min_overall_score
    score = result.scorecard.get("overall_score")
    if min_score is not None and isinstance(score, (int, float)) and score < min_score:
        print(
            f"overall_score {score:.4f} below --min-overall-score {min_score:.4f}",
            file=sys.stderr,
        )
        return 1
    return 0


def _parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Render Fyralis Company Understanding vitals from an existing "
            "end-to-end report directory."
        )
    )
    parser.add_argument(
        "--report-dir",
        type=Path,
        required=True,
        help="Existing report directory containing run_summary/storyline artifacts.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Output directory. Defaults to <report-dir>/vitals.",
    )
    parser.add_argument(
        "--database-url",
        default=None,
        help="Optional Postgres URL for DB-backed per-signal metabolism tracing.",
    )
    parser.add_argument(
        "--tenant-id",
        default=None,
        help="Override tenant id for DB-backed tracing. Defaults to report artifacts.",
    )
    parser.add_argument(
        "--fail-on-hard-gates",
        action="store_true",
        help="Exit nonzero when hard operational/safety gates fail.",
    )
    parser.add_argument(
        "--min-overall-score",
        type=float,
        default=None,
        help="Exit nonzero when the vitals overall score is below this threshold.",
    )
    parser.add_argument(
        "--print-summary",
        action="store_true",
        help="Print vitals_summary.md content to stdout.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    raise SystemExit(main())
