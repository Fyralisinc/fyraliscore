#!/usr/bin/env python3
"""Run a local BYOC data-plane agent enrollment and heartbeat probe."""
from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_agent_probe import (
    ByocAgentProbeInputs,
    render_agent_probe_report_json,
    render_agent_probe_report_yaml,
    run_byoc_agent_probe,
)


DEFAULT_INSTALL_TOKEN_ENV = "FYRALIS_BYOC_INSTALL_TOKEN"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("deploy/byoc/dataplane.example.yaml"),
        help="BYOC data-plane manifest used for the agent contract probe.",
    )
    parser.add_argument(
        "--install-token-env",
        default=DEFAULT_INSTALL_TOKEN_ENV,
        help="Environment variable containing local install-token material.",
    )
    parser.add_argument("--agent-id", default="agt_localprobe001")
    parser.add_argument("--agent-version", default="local-contract-probe")
    parser.add_argument("--nonce", default="nonce-local-agent-probe-001")
    parser.add_argument("--sequence", type=int, default=1)
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
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--output", type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(sys.argv[1:] if argv is None else argv))
    if args.timeout_seconds <= 0:
        print("--timeout-seconds must be positive", file=sys.stderr)
        return 2
    if args.sequence < 0:
        print("--sequence must be non-negative", file=sys.stderr)
        return 2

    install_token = os.environ.get(args.install_token_env, "")
    if not install_token.strip():
        print(
            f"{args.install_token_env} must contain install-token material",
            file=sys.stderr,
        )
        return 2

    report = asyncio.run(
        run_byoc_agent_probe(
            ByocAgentProbeInputs(
                manifest_path=args.manifest,
                install_token=install_token,
                agent_id=args.agent_id,
                agent_version=args.agent_version,
                nonce=args.nonce,
                sequence=args.sequence,
                validation_status=args.validation_status,
                control_plane_url=args.control_plane_url,
                timeout_s=args.timeout_seconds,
            )
        )
    )
    rendered = (
        render_agent_probe_report_json(report)
        if args.json
        else render_agent_probe_report_yaml(report)
    )
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    sys.stdout.write(rendered)
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
