#!/usr/bin/env python3
"""Export BYOC control-panel state schema or sanitized example JSON."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_control_panel_contract import (
    render_control_panel_schema_bundle_json,
    render_control_panel_state_example_json,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--schema",
        action="store_true",
        help="Export the control-panel state query/response schema bundle.",
    )
    mode.add_argument(
        "--example",
        action="store_true",
        help="Export a deterministic sanitized example control-panel response.",
    )
    parser.add_argument("--output", type=Path, help="Optional output file.")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    rendered = (
        render_control_panel_schema_bundle_json()
        if args.schema
        else render_control_panel_state_example_json()
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
        return 0
    print(rendered, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
