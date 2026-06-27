#!/usr/bin/env python3
"""Summarize sanitized BYOC launch readiness artifacts."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_launch_readiness_summary import (
    ByocLaunchReadinessSummaryInputs,
    build_byoc_launch_readiness_summary,
    render_launch_readiness_summary_json,
    render_launch_readiness_summary_yaml,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--live-test-readiness",
        type=Path,
        required=True,
        help="Sanitized BYOC live-test readiness report.",
    )
    parser.add_argument(
        "--customer-handoff-report",
        type=Path,
        required=True,
        help="Sanitized BYOC customer handoff readiness report.",
    )
    parser.add_argument(
        "--handoff-bundle-index",
        type=Path,
        required=True,
        help="Sanitized BYOC customer handoff bundle index.",
    )
    parser.add_argument(
        "--control-plane-read-smoke",
        type=Path,
        required=True,
        help="Sanitized BYOC control-plane read smoke report.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized launch readiness summary.",
    )
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit non-zero unless the customer pilot is fully ready.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    summary = build_byoc_launch_readiness_summary(
        ByocLaunchReadinessSummaryInputs(
            live_test_readiness_path=args.live_test_readiness,
            customer_handoff_report_path=args.customer_handoff_report,
            handoff_bundle_index_path=args.handoff_bundle_index,
            control_plane_read_smoke_path=args.control_plane_read_smoke,
        )
    )
    rendered = (
        render_launch_readiness_summary_json(summary)
        if args.json
        else render_launch_readiness_summary_yaml(summary)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    if summary.status == "fail":
        return 1
    if args.require_ready and summary.status != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
