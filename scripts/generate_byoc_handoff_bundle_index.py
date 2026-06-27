#!/usr/bin/env python3
"""Generate a sanitized BYOC customer handoff bundle index."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_handoff_bundle_index import (
    ByocHandoffBundleIndexInputs,
    render_handoff_bundle_index_json,
    render_handoff_bundle_index_yaml,
    build_byoc_handoff_bundle_index,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence-package",
        type=Path,
        default=Path("deploy/byoc/evidence-package.example.yaml"),
        help="Sanitized BYOC evidence package included in the handoff.",
    )
    parser.add_argument(
        "--evidence-ledger",
        type=Path,
        default=Path("deploy/byoc/evidence-ledger.example.yaml"),
        help="Sanitized BYOC evidence ledger included in the handoff.",
    )
    parser.add_argument(
        "--customer-handoff-report",
        type=Path,
        help="Optional sanitized customer handoff readiness report to index.",
    )
    parser.add_argument(
        "--preflight-report",
        type=Path,
        help="Optional sanitized aggregate preflight report to index.",
    )
    parser.add_argument(
        "--source-onboarding-gate-report",
        type=Path,
        help="Optional sanitized source-onboarding gate report to index.",
    )
    parser.add_argument(
        "--control-plane-read-smoke-report",
        type=Path,
        help="Optional sanitized control-plane read smoke report to index.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used to render bounded relative artifact paths.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized handoff bundle index.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    index = build_byoc_handoff_bundle_index(
        ByocHandoffBundleIndexInputs(
            evidence_package_path=args.evidence_package,
            evidence_ledger_path=args.evidence_ledger,
            repo_root=args.repo_root,
            customer_handoff_report_path=args.customer_handoff_report,
            preflight_report_path=args.preflight_report,
            source_onboarding_gate_report_path=args.source_onboarding_gate_report,
            control_plane_read_smoke_report_path=args.control_plane_read_smoke_report,
        )
    )
    rendered = (
        render_handoff_bundle_index_json(index)
        if args.json
        else render_handoff_bundle_index_yaml(index)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
