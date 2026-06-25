#!/usr/bin/env python3
"""Rehearse migrations against an explicit staging clone database."""
from __future__ import annotations

import argparse
import json
import os
import pathlib
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Callable, Mapping, Sequence


REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]


class MigrationRehearsalCliError(ValueError):
    """Operator-facing validation error."""


@dataclass(frozen=True)
class RehearsalStep:
    name: str
    command_name: str
    returncode: int
    elapsed_ms: int

    @property
    def ok(self) -> bool:
        return self.returncode == 0

    def as_json(self) -> dict[str, object]:
        return {
            "name": self.name,
            "command_name": self.command_name,
            "returncode": self.returncode,
            "elapsed_ms": self.elapsed_ms,
            "ok": self.ok,
        }


@dataclass(frozen=True)
class MigrationRehearsalResult:
    ok: bool
    started_at: str
    dsn_source: str
    migrations_dir: str
    steps: list[RehearsalStep]

    def as_json(self) -> dict[str, object]:
        return {
            "ok": self.ok,
            "started_at": self.started_at,
            "dsn_source": self.dsn_source,
            "migrations_dir": self.migrations_dir,
            "steps": [step.as_json() for step in self.steps],
        }


StepRunner = Callable[[Sequence[str], Mapping[str, str] | None, int], int]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=None,
        help="Staging clone DSN. Defaults to $STAGING_CLONE_DATABASE_URL.",
    )
    parser.add_argument(
        "--confirm-staging-clone",
        action="store_true",
        help="Required safety acknowledgement that --dsn points at a staging clone.",
    )
    parser.add_argument(
        "--migrations-dir",
        type=pathlib.Path,
        default=REPO_ROOT / "db" / "migrations",
    )
    parser.add_argument("--timeout-seconds", type=int, default=900)
    parser.add_argument(
        "--run-readiness-gates",
        action="store_true",
        help="Also run scripts/run_operational_readiness_gates.py after drift.",
    )
    parser.add_argument(
        "--status-output",
        type=pathlib.Path,
        help="Optional path for bounded JSON result.",
    )
    return parser


def _command_name(command: Sequence[str]) -> str:
    if len(command) >= 2 and pathlib.Path(command[0]).name.startswith("python"):
        return pathlib.Path(command[1]).name
    return pathlib.Path(command[0]).name


def _subprocess_runner(
    command: Sequence[str],
    env: Mapping[str, str] | None,
    timeout_seconds: int,
) -> int:
    completed = subprocess.run(
        list(command),
        cwd=REPO_ROOT,
        env=dict(env) if env is not None else None,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        timeout=timeout_seconds,
        check=False,
    )
    return completed.returncode


def _run_step(
    name: str,
    command: Sequence[str],
    *,
    env: Mapping[str, str] | None,
    timeout_seconds: int,
    runner: StepRunner,
) -> RehearsalStep:
    started = time.monotonic()
    try:
        returncode = runner(command, env, timeout_seconds)
    except subprocess.TimeoutExpired:
        returncode = 124
    return RehearsalStep(
        name=name,
        command_name=_command_name(command),
        returncode=returncode,
        elapsed_ms=int((time.monotonic() - started) * 1000),
    )


def run_rehearsal(
    args: argparse.Namespace,
    *,
    runner: StepRunner = _subprocess_runner,
) -> MigrationRehearsalResult:
    if not args.confirm_staging_clone:
        raise MigrationRehearsalCliError("--confirm-staging-clone is required")
    dsn = str(args.dsn or os.environ.get("STAGING_CLONE_DATABASE_URL") or "")
    if not dsn:
        raise MigrationRehearsalCliError(
            "STAGING_CLONE_DATABASE_URL or --dsn is required"
        )
    if args.timeout_seconds <= 0:
        raise MigrationRehearsalCliError("--timeout-seconds must be positive")

    env = os.environ.copy()
    env["DATABASE_URL"] = dsn
    env["STAGING_CLONE_DATABASE_URL"] = dsn
    env.setdefault("COMPANY_OS_ENV", "staging")
    env.setdefault("FYRALIS_ENV", "staging")
    env.setdefault("PYTHONUNBUFFERED", "1")

    steps: list[RehearsalStep] = []
    migrations_dir = pathlib.Path(args.migrations_dir)
    commands: list[tuple[str, list[str]]] = [
        (
            "apply_migrations",
            [
                sys.executable,
                "scripts/apply_db_migrations.py",
                "--dsn",
                dsn,
                "--migrations-dir",
                str(migrations_dir),
            ],
        ),
        (
            "schema_drift",
            [sys.executable, "scripts/check_schema_drift.py", "--dsn", dsn],
        ),
    ]
    if args.run_readiness_gates:
        commands.append(
            (
                "operational_readiness_gates",
                [
                    sys.executable,
                    "scripts/run_operational_readiness_gates.py",
                    "--skip-gap-harness",
                    "--skip-pytest",
                ],
            )
        )

    for name, command in commands:
        step = _run_step(
            name,
            command,
            env=env,
            timeout_seconds=args.timeout_seconds,
            runner=runner,
        )
        steps.append(step)
        if not step.ok:
            break

    return MigrationRehearsalResult(
        ok=all(step.ok for step in steps),
        started_at=datetime.now(UTC).isoformat(),
        dsn_source="argument" if args.dsn else "STAGING_CLONE_DATABASE_URL",
        migrations_dir=str(migrations_dir),
        steps=steps,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_rehearsal(args)
    except MigrationRehearsalCliError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    payload = result.as_json()
    if args.status_output is not None:
        args.status_output.write_text(
            json.dumps(payload, sort_keys=True) + "\n",
            encoding="utf-8",
        )
    print(json.dumps(payload, sort_keys=True))
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
