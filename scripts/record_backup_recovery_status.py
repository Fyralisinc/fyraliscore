#!/usr/bin/env python3
"""Record backup/restore status for housekeeper freshness metrics."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
import pathlib
import sys
from datetime import datetime, timezone
from typing import Any

import asyncpg

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.platform.backup_recovery import (  # noqa: E402
    CHECK_NAMES,
    COMPONENTS,
    STATUS_VALUES,
    default_freshness_slo_seconds,
    record_backup_recovery_status,
    validate_check_name,
    validate_component,
)


class BackupStatusCliError(ValueError):
    """Operator-facing validation error."""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="Postgres DSN. Defaults to $DATABASE_URL.",
    )
    parser.add_argument("--component", required=True, choices=COMPONENTS)
    parser.add_argument("--check", required=True, choices=CHECK_NAMES)
    parser.add_argument("--status", required=True, choices=STATUS_VALUES)
    parser.add_argument(
        "--occurred-at",
        help="ISO-8601 timestamp for the backup/check attempt. Defaults to now.",
    )
    parser.add_argument(
        "--freshness-slo-seconds",
        type=int,
        help="Freshness SLO. Defaults by check type.",
    )
    parser.add_argument(
        "--details-json",
        default="{}",
        help="Small non-secret JSON object with provider/job metadata.",
    )
    return parser


def _parse_occurred_at(value: str | None) -> datetime | None:
    if value is None:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BackupStatusCliError("--occurred-at must be ISO-8601") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def _parse_details(raw: str) -> dict[str, Any]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BackupStatusCliError("--details-json must be valid JSON") from exc
    if not isinstance(parsed, dict):
        raise BackupStatusCliError("--details-json must be a JSON object")
    return parsed


async def run_command(args: argparse.Namespace, *, conn: asyncpg.Connection) -> dict[str, Any]:
    component = validate_component(str(args.component))
    check_name = validate_check_name(str(args.check))
    slo = args.freshness_slo_seconds
    if slo is None:
        slo = default_freshness_slo_seconds(check_name, component=component)
    status = await record_backup_recovery_status(
        conn,
        component=component,
        check_name=check_name,
        status=str(args.status),
        occurred_at=_parse_occurred_at(args.occurred_at),
        freshness_slo_seconds=slo,
        details=_parse_details(args.details_json),
    )
    return {
        "ok": True,
        "component": status.component,
        "check_name": status.check_name,
        "status": status.status,
        "last_success_at": (
            status.last_success_at.isoformat() if status.last_success_at else None
        ),
        "last_attempt_at": status.last_attempt_at.isoformat(),
        "freshness_slo_seconds": status.freshness_slo_seconds,
        "details": status.details,
    }


async def _amain(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if not args.dsn:
        print("DATABASE_URL or --dsn is required", file=sys.stderr)
        return 2
    try:
        conn = await asyncpg.connect(args.dsn)
        try:
            await _register_codecs(conn)
            result = await run_command(args, conn=conn)
        finally:
            await conn.close()
    except BackupStatusCliError as exc:
        print(str(exc), file=sys.stderr)
        return 2
    print(json.dumps(result, sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(_amain(argv))


if __name__ == "__main__":
    raise SystemExit(main())
