#!/usr/bin/env python3
"""Run local BYOC post-deployment validation.

The command has two useful modes:

* Offline contract mode: validate the manifest plus optional env file.
* Live mode (`--require-live`): also require supplied health URLs, database
  DSN, broker endpoint, and object-store endpoint to pass.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Sequence

from services.platform.runtime.byoc_validation import (
    ByocValidationInputs,
    parse_worker_health_args,
    render_report_json,
    render_report_markdown,
    run_byoc_post_deploy_validation,
)


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--gateway-url")
    parser.add_argument(
        "--worker-health",
        action="append",
        default=[],
        metavar="NAME=URL",
        help="Worker health base URL. Repeat for every enabled worker in live mode.",
    )
    parser.add_argument("--database-url")
    parser.add_argument("--kafka-bootstrap-servers")
    parser.add_argument("--object-store-url")
    parser.add_argument("--require-live", action="store_true")
    parser.add_argument("--timeout-seconds", type=float, default=5.0)
    parser.add_argument("--json", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))
    if args.timeout_seconds <= 0:
        print("--timeout-seconds must be positive", file=sys.stderr)
        return 2
    try:
        worker_health_urls = parse_worker_health_args(args.worker_health)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    report = run_byoc_post_deploy_validation(
        ByocValidationInputs(
            manifest_path=args.manifest,
            env_path=args.env_file,
            gateway_url=args.gateway_url,
            worker_health_urls=worker_health_urls,
            database_url=args.database_url,
            kafka_bootstrap_servers=args.kafka_bootstrap_servers,
            object_store_url=args.object_store_url,
            require_live=args.require_live,
            timeout_s=args.timeout_seconds,
        )
    )
    if args.json:
        sys.stdout.write(render_report_json(report))
    else:
        sys.stdout.write(render_report_markdown(report))
    return 0 if report.required_checks_passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
