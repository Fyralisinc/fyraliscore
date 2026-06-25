#!/usr/bin/env python3
"""Run a production backup command and optionally record bounded status.

This wrapper is intentionally provider-neutral: RDS snapshots, Cloud SQL
exports, pgBackRest, Velero, native bucket replication, and customer-owned
backup jobs can all be represented as a concrete command. Fyralis records only
bounded operational metadata and never stores command output, object keys,
payload samples, credentials, or customer identifiers.
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
from typing import Any, Mapping, Sequence

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.backup_recovery import (  # noqa: E402
    CHECK_NAMES,
    COMPONENTS,
    default_freshness_slo_seconds,
    record_backup_recovery_status,
    validate_check_name,
    validate_component,
)


_FORBIDDEN_DETAIL_KEY_PARTS = (
    "access_token",
    "api_key",
    "credential",
    "customer",
    "email",
    "object_key",
    "object_path",
    "password",
    "payload",
    "private_key",
    "secret",
    "tenant_id",
    "token",
    "webhook",
)
_FORBIDDEN_DETAIL_VALUE_PARTS = (
    "-----BEGIN ",
    "authorization:",
    "bearer ",
    "ghp_",
    "sk-",
    "xox",
)


class BackupJobCliError(ValueError):
    """Operator-facing validation error."""


@dataclass(frozen=True)
class BackupJobResult:
    component: str
    check_name: str
    status: str
    occurred_at: datetime
    duration_ms: int
    command_name: str
    exit_code: int | None
    timed_out: bool
    details: dict[str, Any]

    def as_json(self) -> dict[str, Any]:
        return {
            "ok": self.status == "ok",
            "component": self.component,
            "check_name": self.check_name,
            "status": self.status,
            "occurred_at": self.occurred_at.isoformat(),
            "duration_ms": self.duration_ms,
            "command_name": self.command_name,
            "exit_code": self.exit_code,
            "timed_out": self.timed_out,
            "details": self.details,
        }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--component", required=True, choices=COMPONENTS)
    parser.add_argument("--check", default="backup", choices=CHECK_NAMES)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=float(os.environ.get("BACKUP_JOB_TIMEOUT_SECONDS", "3600")),
        help="Maximum command runtime. Defaults to $BACKUP_JOB_TIMEOUT_SECONDS or 3600.",
    )
    parser.add_argument(
        "--details-json",
        default="{}",
        help="Small non-secret JSON object with provider/job metadata.",
    )
    parser.add_argument(
        "--record-status",
        action="store_true",
        help="Write the result to backup_recovery_status.",
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN for --record-status. Defaults to $DATABASE_URL.",
    )
    parser.add_argument(
        "--freshness-slo-seconds",
        type=int,
        help="Freshness SLO recorded with --record-status. Defaults by check type.",
    )
    parser.add_argument(
        "--status-output",
        type=pathlib.Path,
        help="Optional file path for the bounded JSON result.",
    )
    parser.add_argument(
        "command",
        nargs=argparse.REMAINDER,
        help="Backup command after '--', for example: -- pg_dump --format=custom ...",
    )
    return parser


def _sanitize_details(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BackupJobCliError("--details-json must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise BackupJobCliError("--details-json must be a JSON object")
    encoded = json.dumps(parsed, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > 4096:
        raise BackupJobCliError("--details-json must be 4096 bytes or smaller")
    _validate_safe_detail_tree(parsed)
    return dict(parsed)


def _validate_safe_detail_tree(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            if not isinstance(key, str):
                raise BackupJobCliError(f"{path}: details keys must be strings")
            lowered = key.lower()
            if any(part in lowered for part in _FORBIDDEN_DETAIL_KEY_PARTS):
                raise BackupJobCliError(
                    f"{path}.{key}: details must not include sensitive keys",
                )
            _validate_safe_detail_tree(child, path=f"{path}.{key}")
        return
    if isinstance(value, list):
        for index, child in enumerate(value):
            _validate_safe_detail_tree(child, path=f"{path}[{index}]")
        return
    if isinstance(value, str):
        lowered = value.lower()
        if any(part.lower() in lowered for part in _FORBIDDEN_DETAIL_VALUE_PARTS):
            raise BackupJobCliError(
                f"{path}: details must not include secret-looking values",
            )


def _normalize_command(raw: Sequence[str]) -> list[str]:
    command = list(raw)
    if command and command[0] == "--":
        command = command[1:]
    if not command:
        raise BackupJobCliError("backup command is required after '--'")
    return command


def run_backup_job(args: argparse.Namespace) -> BackupJobResult:
    component = validate_component(str(args.component))
    check_name = validate_check_name(str(args.check))
    if args.timeout_seconds <= 0:
        raise BackupJobCliError("--timeout-seconds must be positive")
    details = _sanitize_details(str(args.details_json))
    command = _normalize_command(args.command)
    command_name = pathlib.Path(command[0]).name

    started = time.monotonic()
    occurred_at = datetime.now(timezone.utc)
    timed_out = False
    exit_code: int | None
    try:
        completed = subprocess.run(
            command,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=args.timeout_seconds,
        )
        exit_code = completed.returncode
    except subprocess.TimeoutExpired:
        timed_out = True
        exit_code = None

    duration_ms = int((time.monotonic() - started) * 1000)
    status = "failed" if timed_out or exit_code != 0 else "ok"
    result_details = {
        **details,
        "runner": "scripts/run_backup_job.py",
        "command": command_name,
        "duration_ms": duration_ms,
        "exit_code": exit_code,
        "timed_out": timed_out,
    }
    _validate_safe_detail_tree(result_details)
    return BackupJobResult(
        component=component,
        check_name=check_name,
        status=status,
        occurred_at=occurred_at,
        duration_ms=duration_ms,
        command_name=command_name,
        exit_code=exit_code,
        timed_out=timed_out,
        details=result_details,
    )


async def record_result(
    result: BackupJobResult,
    *,
    dsn: str,
    freshness_slo_seconds: int | None = None,
) -> dict[str, Any]:
    conn = await asyncpg.connect(dsn)
    try:
        await _register_codecs(conn)
        slo = freshness_slo_seconds
        if slo is None:
            slo = default_freshness_slo_seconds(
                result.check_name,
                component=result.component,
            )
        status = await record_backup_recovery_status(
            conn,
            component=result.component,
            check_name=result.check_name,
            status=result.status,
            occurred_at=result.occurred_at,
            freshness_slo_seconds=slo,
            details=result.details,
        )
    finally:
        await conn.close()
    payload = result.as_json()
    payload["last_attempt_at"] = status.last_attempt_at.isoformat()
    payload["last_success_at"] = (
        status.last_success_at.isoformat() if status.last_success_at else None
    )
    payload["freshness_slo_seconds"] = status.freshness_slo_seconds
    return payload


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        result = run_backup_job(args)
        payload = result.as_json()
        if args.record_status:
            if not args.dsn:
                raise BackupJobCliError("DATABASE_URL or --dsn is required")
            payload = await record_result(
                result,
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
