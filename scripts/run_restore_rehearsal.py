#!/usr/bin/env python3
"""Run a restore rehearsal command plus verification command.

The backup runner is enough for scheduled backups. Restore rehearsals need a
second verification command so the job proves the restored target is usable
before Fyralis records `restore_test=ok`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Sequence

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from scripts.run_backup_job import (  # noqa: E402
    BackupJobCliError,
    _sanitize_details,
    _validate_safe_detail_tree,
    record_result,
)
from services.platform.backup_recovery import COMPONENTS, validate_component  # noqa: E402


@dataclass(frozen=True)
class RestoreRehearsalResult:
    component: str
    status: str
    occurred_at: datetime
    duration_ms: int
    restore_command_name: str
    verify_command_name: str
    restore_exit_code: int | None
    verify_exit_code: int | None
    timed_out: bool
    details: dict[str, Any]

    def as_json(self) -> dict[str, Any]:
        return {
            "ok": self.status == "ok",
            "component": self.component,
            "check_name": "restore_test",
            "status": self.status,
            "occurred_at": self.occurred_at.isoformat(),
            "duration_ms": self.duration_ms,
            "restore_command_name": self.restore_command_name,
            "verify_command_name": self.verify_command_name,
            "restore_exit_code": self.restore_exit_code,
            "verify_exit_code": self.verify_exit_code,
            "timed_out": self.timed_out,
            "details": self.details,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", required=True, choices=COMPONENTS)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=float(os.environ.get("RESTORE_REHEARSAL_TIMEOUT_SECONDS", "7200")),
        help=(
            "Maximum runtime for each command. Defaults to "
            "$RESTORE_REHEARSAL_TIMEOUT_SECONDS or 7200."
        ),
    )
    parser.add_argument(
        "--details-json",
        default="{}",
        help="Small non-secret JSON object with provider/job metadata.",
    )
    parser.add_argument(
        "--restore-command-json",
        required=True,
        help="JSON array command that restores into an isolated target.",
    )
    parser.add_argument(
        "--verify-command-json",
        required=True,
        help="JSON array command that verifies the restored target.",
    )
    parser.add_argument(
        "--record-status",
        action="store_true",
        help="Write the result to backup_recovery_status as restore_test.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN for --record-status. Defaults to $DATABASE_URL.",
    )
    parser.add_argument(
        "--freshness-slo-seconds",
        type=int,
        help="Freshness SLO recorded with --record-status.",
    )
    parser.add_argument(
        "--status-output",
        type=pathlib.Path,
        help="Optional file path for the bounded JSON result.",
    )
    return parser


def _parse_command_json(raw: str, *, option: str) -> list[str]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BackupJobCliError(f"{option} must be valid JSON") from exc
    if not isinstance(parsed, list) or not parsed:
        raise BackupJobCliError(f"{option} must be a non-empty JSON array")
    if not all(isinstance(part, str) and part for part in parsed):
        raise BackupJobCliError(f"{option} must contain non-empty strings")
    return list(parsed)


def _command_name(command: Sequence[str]) -> str:
    return pathlib.Path(command[0]).name


def _run_command(command: Sequence[str], *, timeout_seconds: float) -> tuple[int | None, bool]:
    try:
        completed = subprocess.run(
            list(command),
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        return None, True
    return completed.returncode, False


def run_restore_rehearsal(args: argparse.Namespace) -> RestoreRehearsalResult:
    component = validate_component(str(args.component))
    if args.timeout_seconds <= 0:
        raise BackupJobCliError("--timeout-seconds must be positive")
    details = _sanitize_details(str(args.details_json))
    restore_command = _parse_command_json(
        str(args.restore_command_json),
        option="--restore-command-json",
    )
    verify_command = _parse_command_json(
        str(args.verify_command_json),
        option="--verify-command-json",
    )

    started = time.monotonic()
    occurred_at = datetime.now(timezone.utc)
    restore_exit_code, restore_timed_out = _run_command(
        restore_command,
        timeout_seconds=args.timeout_seconds,
    )
    verify_exit_code: int | None = None
    verify_timed_out = False
    if restore_exit_code == 0 and not restore_timed_out:
        verify_exit_code, verify_timed_out = _run_command(
            verify_command,
            timeout_seconds=args.timeout_seconds,
        )

    duration_ms = int((time.monotonic() - started) * 1000)
    timed_out = restore_timed_out or verify_timed_out
    status = (
        "ok"
        if restore_exit_code == 0 and verify_exit_code == 0 and not timed_out
        else "failed"
    )
    result_details = {
        **details,
        "runner": "scripts/run_restore_rehearsal.py",
        "restore_command": _command_name(restore_command),
        "verify_command": _command_name(verify_command),
        "restore_exit_code": restore_exit_code,
        "verify_exit_code": verify_exit_code,
        "timed_out": timed_out,
        "duration_ms": duration_ms,
    }
    _validate_safe_detail_tree(result_details)
    return RestoreRehearsalResult(
        component=component,
        status=status,
        occurred_at=occurred_at,
        duration_ms=duration_ms,
        restore_command_name=_command_name(restore_command),
        verify_command_name=_command_name(verify_command),
        restore_exit_code=restore_exit_code,
        verify_exit_code=verify_exit_code,
        timed_out=timed_out,
        details=result_details,
    )


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_restore_rehearsal(args)
        payload = result.as_json()
        if args.record_status:
            if not args.dsn:
                raise BackupJobCliError("DATABASE_URL or --dsn is required")
            from scripts.run_backup_job import BackupJobResult

            payload = await record_result(
                BackupJobResult(
                    component=result.component,
                    check_name="restore_test",
                    status=result.status,
                    occurred_at=result.occurred_at,
                    duration_ms=result.duration_ms,
                    command_name=result.verify_command_name,
                    exit_code=result.verify_exit_code,
                    timed_out=result.timed_out,
                    details=result.details,
                ),
                dsn=str(args.dsn),
                freshness_slo_seconds=args.freshness_slo_seconds,
            )
        if args.status_output is not None:
            args.status_output.write_text(
                json.dumps(payload, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        print(json.dumps(payload, sort_keys=True))
    except BackupJobCliError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    return 0 if result.status == "ok" else 1


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
