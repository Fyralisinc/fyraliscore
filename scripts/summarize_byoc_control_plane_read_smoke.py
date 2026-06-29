#!/usr/bin/env python3
"""Summarize BYOC control-plane read smoke output without signed headers."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_control_plane_read_smoke_summary import (
    ByocControlPlaneReadSmokeSummaryInputs,
    build_byoc_control_plane_read_smoke_summary,
    render_control_plane_read_smoke_summary_json,
    render_control_plane_read_smoke_summary_yaml,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--control-plane-read-smoke",
        type=Path,
        required=True,
        help="Raw BYOC control-plane read smoke output to summarize.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized read smoke summary.",
    )
    parser.add_argument(
        "--require-executed",
        action="store_true",
        help="Exit non-zero unless the hosted read smoke has fully executed.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    summary = build_byoc_control_plane_read_smoke_summary(
        ByocControlPlaneReadSmokeSummaryInputs(
            control_plane_read_smoke_path=args.control_plane_read_smoke,
        )
    )
    rendered = (
        render_control_plane_read_smoke_summary_json(summary)
        if args.json
        else render_control_plane_read_smoke_summary_yaml(summary)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    if summary.status == "fail":
        return 1
    if args.require_executed and summary.status != "pass":
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
