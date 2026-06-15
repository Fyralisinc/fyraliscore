#!/usr/bin/env python
"""Render runtime process manifest entries for shell scripts."""
from __future__ import annotations

import argparse
import json
import shlex
import sys
from collections.abc import Sequence

from services.platform.runtime.process_manifest import (
    RuntimeProcess,
    dogfood_processes,
    production_processes,
)


def _dogfood_command(
    process: RuntimeProcess,
    *,
    python_bin: str,
    uvicorn_bin: str,
    gateway_port: str,
    uvicorn_log_level: str,
) -> tuple[str, ...]:
    if process.name == "gateway":
        return (
            uvicorn_bin,
            "services.app.gateway.main:app",
            "--host",
            "0.0.0.0",
            "--port",
            gateway_port,
            "--log-level",
            uvicorn_log_level,
        )
    if process.command is None:
        raise ValueError(f"process {process.name!r} has no dogfood command")
    if process.command[0] == "python":
        return (python_bin, *process.command[1:])
    return process.command


def render_dogfood_tsv(args: argparse.Namespace) -> str:
    rows: list[str] = []
    for process in dogfood_processes():
        command = _dogfood_command(
            process,
            python_bin=args.python_bin,
            uvicorn_bin=args.uvicorn_bin,
            gateway_port=args.gateway_port,
            uvicorn_log_level=args.uvicorn_log_level,
        )
        rows.append(
            "\t".join(
                (
                    process.name,
                    process.cwd,
                    process.log_file or f"{process.name}.log",
                    "exec " + shlex.join(command),
                )
            )
        )
    return "\n".join(rows) + "\n"


def _production_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for process in production_processes():
        rows.append(
            {
                "name": process.name,
                "family": process.family,
                "compose_service": process.compose_service or "",
                "command": process.compose_command() or "",
                "has_healthcheck": process.has_healthcheck,
                "singleton": process.singleton,
                "description": process.description,
            }
        )
    return rows


def render_production_json() -> str:
    return json.dumps(_production_rows(), indent=2, sort_keys=True) + "\n"


def render_production_markdown() -> str:
    lines = [
        "| Process | Family | Compose service | Healthcheck | Singleton | Command |",
        "| --- | --- | --- | --- | --- | --- |",
    ]
    for row in _production_rows():
        lines.append(
            "| {name} | {family} | {compose_service} | {has_healthcheck} | "
            "{singleton} | `{command}` |".format(**row)
        )
    return "\n".join(lines) + "\n"


def _parse_args(argv: Sequence[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="mode", required=True)
    dogfood = sub.add_parser("dogfood")
    dogfood.add_argument("--python-bin", required=True)
    dogfood.add_argument("--uvicorn-bin", required=True)
    dogfood.add_argument("--gateway-port", required=True)
    dogfood.add_argument("--uvicorn-log-level", required=True)
    production = sub.add_parser("production")
    production.add_argument(
        "--format",
        choices=("markdown", "json"),
        default="markdown",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = _parse_args(list(argv or sys.argv[1:]))
    if args.mode == "dogfood":
        sys.stdout.write(render_dogfood_tsv(args))
        return 0
    if args.mode == "production":
        if args.format == "json":
            sys.stdout.write(render_production_json())
        else:
            sys.stdout.write(render_production_markdown())
        return 0
    raise AssertionError(args.mode)


if __name__ == "__main__":
    raise SystemExit(main())
