"""Backup and restore status contract for production operations."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal

import asyncpg

from lib.observability.metrics import gauge


BackupComponent = Literal[
    "postgres",
    "object_store",
    "broker",
    "secrets",
    "application_config",
]
BackupCheckName = Literal["backup", "restore_test", "inventory"]
BackupStatusValue = Literal["ok", "failed", "unknown"]
BackupHealthState = Literal["fresh", "stale", "missing", "failed"]

COMPONENTS: tuple[BackupComponent, ...] = (
    "postgres",
    "object_store",
    "broker",
    "secrets",
    "application_config",
)
CHECK_NAMES: tuple[BackupCheckName, ...] = ("backup", "restore_test", "inventory")
STATUS_VALUES: tuple[BackupStatusValue, ...] = ("ok", "failed", "unknown")
HEALTH_STATES: tuple[BackupHealthState, ...] = (
    "fresh",
    "stale",
    "missing",
    "failed",
)

DEFAULT_FRESHNESS_SLO_SECONDS: dict[BackupCheckName, int] = {
    "backup": 36 * 60 * 60,
    "restore_test": 35 * 24 * 60 * 60,
    "inventory": 36 * 60 * 60,
}
_ENV_SLO_BY_COMPONENT_CHECK: dict[tuple[BackupComponent, BackupCheckName], str] = {
    ("postgres", "backup"): "BACKUP_POSTGRES_FRESHNESS_SLO_SECONDS",
    ("object_store", "backup"): "BACKUP_OBJECT_STORE_FRESHNESS_SLO_SECONDS",
    ("postgres", "restore_test"): "RESTORE_TEST_FRESHNESS_SLO_SECONDS",
    ("object_store", "restore_test"): "RESTORE_TEST_FRESHNESS_SLO_SECONDS",
    ("object_store", "inventory"): "BACKUP_OBJECT_STORE_FRESHNESS_SLO_SECONDS",
}

_LAST_SUCCESS_TS = gauge(
    "backup_recovery_last_success_timestamp_seconds",
    "Unix timestamp of the latest successful backup or restore check.",
    ("component", "check"),
)
_LAST_ATTEMPT_TS = gauge(
    "backup_recovery_last_attempt_timestamp_seconds",
    "Unix timestamp of the latest backup or restore check attempt.",
    ("component", "check"),
)
_AGE_SECONDS = gauge(
    "backup_recovery_last_success_age_seconds",
    "Seconds since the latest successful backup or restore check.",
    ("component", "check"),
)
_HEALTH_STATUS = gauge(
    "backup_recovery_health_status",
    "Current backup or restore check health as one-hot state gauges.",
    ("component", "check", "state"),
)


@dataclass(frozen=True, slots=True)
class BackupRecoveryStatus:
    component: BackupComponent
    check_name: BackupCheckName
    status: BackupStatusValue
    last_success_at: datetime | None
    last_attempt_at: datetime
    freshness_slo_seconds: int
    details: dict[str, Any]
    updated_at: datetime


def validate_component(value: str) -> BackupComponent:
    if value not in COMPONENTS:
        raise ValueError(f"invalid backup component {value!r}")
    return value  # type: ignore[return-value]


def validate_check_name(value: str) -> BackupCheckName:
    if value not in CHECK_NAMES:
        raise ValueError(f"invalid backup check_name {value!r}")
    return value  # type: ignore[return-value]


def validate_status(value: str) -> BackupStatusValue:
    if value not in STATUS_VALUES:
        raise ValueError(f"invalid backup status {value!r}")
    return value  # type: ignore[return-value]


def default_freshness_slo_seconds(
    check_name: BackupCheckName,
    *,
    component: BackupComponent | None = None,
) -> int:
    env_name = (
        _ENV_SLO_BY_COMPONENT_CHECK.get((component, check_name))
        if component is not None
        else None
    )
    if env_name is not None:
        raw = os.environ.get(env_name)
        try:
            if raw is not None and raw.strip():
                return max(1, int(raw))
        except ValueError:
            pass
    return DEFAULT_FRESHNESS_SLO_SECONDS[check_name]


async def record_backup_recovery_status(
    conn: asyncpg.Connection,
    *,
    component: str,
    check_name: str,
    status: str,
    freshness_slo_seconds: int | None = None,
    occurred_at: datetime | None = None,
    details: dict[str, Any] | None = None,
) -> BackupRecoveryStatus:
    component_v = validate_component(component)
    check_v = validate_check_name(check_name)
    status_v = validate_status(status)
    slo = int(
        freshness_slo_seconds
        or default_freshness_slo_seconds(check_v, component=component_v)
    )
    if slo <= 0:
        raise ValueError("freshness_slo_seconds must be positive")
    occurred = occurred_at or datetime.now(timezone.utc)
    if occurred.tzinfo is None:
        occurred = occurred.replace(tzinfo=timezone.utc)
    details_json = json.dumps(details or {}, default=str, sort_keys=True)

    row = await conn.fetchrow(
        """
        INSERT INTO backup_recovery_status (
            component, check_name, status, last_success_at, last_attempt_at,
            freshness_slo_seconds, details, updated_at
        ) VALUES (
            $1, $2, $3,
            CASE WHEN $3 = 'ok' THEN $4::timestamptz ELSE NULL::timestamptz END,
            $4::timestamptz, $5::integer, $6::jsonb, now()
        )
        ON CONFLICT (component, check_name) DO UPDATE SET
            status = EXCLUDED.status,
            last_attempt_at = EXCLUDED.last_attempt_at,
            last_success_at = CASE
                WHEN EXCLUDED.status = 'ok'
                THEN EXCLUDED.last_success_at
                ELSE backup_recovery_status.last_success_at
            END,
            freshness_slo_seconds = EXCLUDED.freshness_slo_seconds,
            details = EXCLUDED.details,
            updated_at = now()
        RETURNING component, check_name, status, last_success_at,
                  last_attempt_at, freshness_slo_seconds, details, updated_at
        """,
        component_v,
        check_v,
        status_v,
        occurred,
        slo,
        details_json,
    )
    assert row is not None
    return _hydrate(row)


async def fetch_backup_recovery_statuses(
    conn: asyncpg.Connection,
) -> list[BackupRecoveryStatus]:
    rows = await conn.fetch(
        """
        SELECT component, check_name, status, last_success_at, last_attempt_at,
               freshness_slo_seconds, details, updated_at
        FROM backup_recovery_status
        ORDER BY component, check_name
        """
    )
    return [_hydrate(row) for row in rows]


def classify_backup_health(
    status: BackupRecoveryStatus,
    *,
    now: datetime | None = None,
) -> BackupHealthState:
    if status.status == "failed":
        return "failed"
    if status.last_success_at is None:
        return "missing"
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    last_success = status.last_success_at
    if last_success.tzinfo is None:
        last_success = last_success.replace(tzinfo=timezone.utc)
    age = (now - last_success).total_seconds()
    return "stale" if age > status.freshness_slo_seconds else "fresh"


async def refresh_backup_recovery_metrics(
    conn: asyncpg.Connection,
    *,
    now: datetime | None = None,
) -> list[BackupRecoveryStatus]:
    now = now or datetime.now(timezone.utc)
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)
    statuses = await fetch_backup_recovery_statuses(conn)
    _LAST_SUCCESS_TS.reset()
    _LAST_ATTEMPT_TS.reset()
    _AGE_SECONDS.reset()
    _HEALTH_STATUS.reset()

    for status in statuses:
        labels = {"component": status.component, "check": status.check_name}
        if status.last_success_at is not None:
            last_success = _aware(status.last_success_at)
            _LAST_SUCCESS_TS.set(last_success.timestamp(), **labels)
            _AGE_SECONDS.set(max(0.0, (now - last_success).total_seconds()), **labels)
        _LAST_ATTEMPT_TS.set(_aware(status.last_attempt_at).timestamp(), **labels)
        health = classify_backup_health(status, now=now)
        for state in HEALTH_STATES:
            _HEALTH_STATUS.set(
                1.0 if state == health else 0.0,
                component=status.component,
                check=status.check_name,
                state=state,
            )
    return statuses


def _aware(value: datetime) -> datetime:
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _hydrate(row: asyncpg.Record) -> BackupRecoveryStatus:
    details = row["details"]
    if isinstance(details, str):
        details = json.loads(details)
    return BackupRecoveryStatus(
        component=validate_component(row["component"]),
        check_name=validate_check_name(row["check_name"]),
        status=validate_status(row["status"]),
        last_success_at=row["last_success_at"],
        last_attempt_at=row["last_attempt_at"],
        freshness_slo_seconds=int(row["freshness_slo_seconds"]),
        details=details if isinstance(details, dict) else {},
        updated_at=row["updated_at"],
    )


__all__ = [
    "BackupCheckName",
    "BackupComponent",
    "BackupHealthState",
    "BackupRecoveryStatus",
    "BackupStatusValue",
    "CHECK_NAMES",
    "COMPONENTS",
    "HEALTH_STATES",
    "STATUS_VALUES",
    "classify_backup_health",
    "default_freshness_slo_seconds",
    "fetch_backup_recovery_statuses",
    "record_backup_recovery_status",
    "refresh_backup_recovery_metrics",
    "validate_check_name",
    "validate_component",
    "validate_status",
]
