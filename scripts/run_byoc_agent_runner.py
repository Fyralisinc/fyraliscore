#!/usr/bin/env python3
"""Run the bounded local BYOC data-plane agent runner skeleton."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_agent_runner import (
    ByocAgentRunnerInputs,
    render_agent_runner_report_json,
    render_agent_runner_report_yaml,
    run_byoc_agent_runner,
)


DEFAULT_INSTALL_TOKEN_ENV = "FYRALIS_BYOC_INSTALL_TOKEN"
MAX_ITERATIONS = 10


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest used for the bounded agent runner.",
    )
    parser.add_argument(
        "--install-token-env",
        default=DEFAULT_INSTALL_TOKEN_ENV,
        help="Environment variable containing local install-token material.",
    )
    parser.add_argument("--agent-id", default="agt_localrunner001")
    parser.add_argument("--agent-version", default="local-runner-skeleton")
    parser.add_argument("--nonce-prefix", default="nonce-local-agent-runner")
    parser.add_argument("--starting-sequence", type=int, default=1)
    parser.add_argument("--iterations", type=int, default=1)
    parser.add_argument(
        "--validation-status",
        choices=("unknown", "passing", "degraded", "failing"),
        default="passing",
    )
    parser.add_argument(
        "--control-plane-url",
        help=(
            "Optional live control-plane base URL. Omit for the local mock "
            "contract harness."
        ),
    )
    parser.add_argument(
        "--mock-desired-revision",
        help=(
            "Local mock desired revision used to exercise apply-plan evidence. "
            "Allowed only when --control-plane-url is omitted."
        ),
    )
    parser.add_argument("--mock-config-epoch", type=int, default=0)
    parser.add_argument(
        "--bootstrap-bundle",
        type=Path,
        help=(
            "Optional BYOC bootstrap bundle used to build sanitized artifact "
            "verification evidence for apply_revision desired state."
        ),
    )
    parser.add_argument(
        "--verify-local-bundle-files",
        action="store_true",
        help="Hash local_path artifacts in --bootstrap-bundle before reporting.",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path.cwd(),
        help="Repository root used for --verify-local-bundle-files.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.timeout_seconds <= 0:
        print("--timeout-seconds must be positive", file=sys.stderr)
        return 2
    if args.starting_sequence < 0:
        print("--starting-sequence must be non-negative", file=sys.stderr)
        return 2
    if args.iterations < 1 or args.iterations > MAX_ITERATIONS:
        print(f"--iterations must be between 1 and {MAX_ITERATIONS}", file=sys.stderr)
        return 2
    if args.mock_config_epoch < 0:
        print("--mock-config-epoch must be non-negative", file=sys.stderr)
        return 2
    if args.control_plane_url and args.mock_desired_revision:
        print(
            "--mock-desired-revision is allowed only with the local mock harness",
            file=sys.stderr,
        )
        return 2
    if args.verify_local_bundle_files and args.bootstrap_bundle is None:
        print(
            "--verify-local-bundle-files requires --bootstrap-bundle",
            file=sys.stderr,
        )
        return 2

    install_token = os.environ.get(args.install_token_env, "")
    if not install_token.strip():
        print(
            f"{args.install_token_env} must contain install-token material",
            file=sys.stderr,
        )
        return 2

    report = asyncio.run(
        run_byoc_agent_runner(
            ByocAgentRunnerInputs(
                manifest_path=args.manifest,
                install_token=install_token,
                agent_id=args.agent_id,
                agent_version=args.agent_version,
                nonce_prefix=args.nonce_prefix,
                starting_sequence=args.starting_sequence,
                iterations=args.iterations,
                validation_status=args.validation_status,
                control_plane_url=args.control_plane_url,
                mock_desired_revision=args.mock_desired_revision,
                mock_config_epoch=args.mock_config_epoch,
                bootstrap_bundle_path=args.bootstrap_bundle,
                verify_local_bundle_files=args.verify_local_bundle_files,
                repo_root=args.repo_root,
                timeout_s=args.timeout_seconds,
            )
        )
    )
    rendered = (
        render_agent_runner_report_json(report)
        if args.json
        else render_agent_runner_report_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
