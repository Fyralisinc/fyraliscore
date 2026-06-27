#!/usr/bin/env python3
"""Run the plan-only BYOC agent install-token rotation readiness check."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_agent_token_rotation import (
    ByocAgentTokenRotationInputs,
    render_agent_token_rotation_plan_json,
    render_agent_token_rotation_plan_yaml,
    run_byoc_agent_token_rotation_plan,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest to use for current deployment identity.",
    )
    parser.add_argument(
        "--current-install-token-secret-ref",
        help=(
            "Optional current install-token secret ref. Defaults to the "
            "manifest bootstrap_token_secret_ref and is not serialized."
        ),
    )
    parser.add_argument(
        "--next-install-token-secret-ref",
        required=True,
        help=(
            "Next install-token secret ref to plan. The ref is not serialized; "
            "only a salted digest is emitted."
        ),
    )
    parser.add_argument(
        "--agent-id",
        default="agt_localrunner001",
        help="Bounded agent id used when deriving the rotation plan id.",
    )
    parser.add_argument(
        "--overlap-seconds",
        type=int,
        default=3600,
        help="Dual-key overlap window; accepted range is 300-604800 seconds.",
    )
    parser.add_argument(
        "--activation-epoch",
        type=int,
        default=1,
        help="Positive metadata epoch for the planned rotation.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit JSON instead of YAML.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the sanitized rotation plan report.",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    report = run_byoc_agent_token_rotation_plan(
        ByocAgentTokenRotationInputs(
            manifest_path=args.manifest,
            current_install_token_secret_ref=args.current_install_token_secret_ref,
            next_install_token_secret_ref=args.next_install_token_secret_ref,
            agent_id=args.agent_id,
            overlap_seconds=args.overlap_seconds,
            activation_epoch=args.activation_epoch,
        )
    )
    rendered = (
        render_agent_token_rotation_plan_json(report)
        if args.json
        else render_agent_token_rotation_plan_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
