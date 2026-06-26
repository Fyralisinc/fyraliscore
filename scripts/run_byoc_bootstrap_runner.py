#!/usr/bin/env python3
"""Run the local BYOC bootstrap dry-run evidence reporter."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_bootstrap_runner import (
    ByocBootstrapRunnerInputs,
    render_runner_report_json,
    render_runner_report_yaml,
    run_byoc_bootstrap_runner,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--plan",
        type=Path,
        default=Path("deploy/byoc/bootstrap-plan.example.yaml"),
        help="BYOC bootstrap dry-run plan to evaluate.",
    )
    parser.add_argument(
        "--dataplane-manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest referenced by the plan.",
    )
    parser.add_argument(
        "--permissions-manifest",
        type=Path,
        default=Path("deploy/byoc/permissions.example.yaml"),
        help="BYOC permissions manifest referenced by the plan.",
    )
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        default=Path("deploy/byoc/bootstrap-bundle.example.yaml"),
        help="BYOC bootstrap bundle manifest referenced by the plan.",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        help="Optional env file for offline post-deploy validation checks.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for digest and local file checks.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the evidence report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    report = run_byoc_bootstrap_runner(
        ByocBootstrapRunnerInputs(
            plan_path=args.plan,
            dataplane_manifest_path=args.dataplane_manifest,
            permissions_manifest_path=args.permissions_manifest,
            bootstrap_bundle_path=args.bootstrap_bundle,
            repo_root=args.repo_root,
            env_path=args.env_file,
        )
    )
    rendered = (
        render_runner_report_json(report)
        if args.json
        else render_runner_report_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
