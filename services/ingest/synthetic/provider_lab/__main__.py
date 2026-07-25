"""Run the deterministic Provider Lab on a loopback interface.

The lab is deliberately unavailable in production and cannot bind to a
non-loopback address.  Production clients point their existing base-URL
overrides at this process; the application rejects every request outside the
finite provider surface registered by the 27 source adapters.
"""
from __future__ import annotations

import argparse
import ipaddress
from collections.abc import Sequence

import uvicorn

from .app import build_provider_lab_app


def _loopback_host(raw: str) -> str:
    try:
        address = ipaddress.ip_address(raw)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Provider Lab host must be a literal loopback IP address",
        ) from exc
    if not address.is_loopback:
        raise argparse.ArgumentTypeError(
            "Provider Lab may bind only to a loopback IP address",
        )
    return raw


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the loopback-only Fyralis Provider Lab",
    )
    parser.add_argument("--host", type=_loopback_host, default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument(
        "--log-level",
        choices=("critical", "error", "warning", "info", "debug", "trace"),
        default="info",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if not 1 <= args.port <= 65_535:
        _parser().error("--port must be between 1 and 65535")
    # Construction applies the production-startup guard before uvicorn opens a
    # socket.  Supplying the app object also avoids import-string reload modes
    # that could bypass this fail-closed point.
    app = build_provider_lab_app()
    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_level=args.log_level,
        access_log=False,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
