"""Production schema/RLS drift monitor metrics.

The authoritative drift logic lives in ``scripts.check_schema_drift`` because
CI and release promotion already use it. This module wraps that check in a
privacy-safe Prometheus contract: operators get counts by bounded category and
health status, never table names, column names, or policy text as labels.
"""
from __future__ import annotations

import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Callable, Literal, Protocol

import psycopg2

import scripts.check_schema_drift as schema_drift
from lib.observability.metrics import counter, gauge, render_default


DriftCategory = Literal[
    "extension",
    "table",
    "partition",
    "rls",
    "column",
    "index",
    "check",
    "unknown",
]
CheckStatus = Literal["ok", "drift", "error"]

DRIFT_CATEGORIES: tuple[DriftCategory, ...] = (
    "extension",
    "table",
    "partition",
    "rls",
    "column",
    "index",
    "check",
    "unknown",
)
CHECK_STATUSES: tuple[CheckStatus, ...] = ("ok", "drift", "error")


class _PsycopgConnection(Protocol):
    def cursor(self): ...
    def close(self) -> None: ...


ConnectFn = Callable[..., _PsycopgConnection]


@dataclass(frozen=True, slots=True)
class SchemaDriftSnapshot:
    status: CheckStatus
    checked_at: datetime
    duration_seconds: float
    findings_total: int
    findings_by_category: dict[DriftCategory, int]
    error: str | None = None


_FINDINGS = gauge(
    "schema_drift_findings",
    "Current schema/RLS drift finding count by bounded category.",
    ("category",),
    allowed_label_values={"category": DRIFT_CATEGORIES},
)
_STATUS = gauge(
    "schema_drift_check_status",
    "Current schema/RLS drift monitor status as one-hot gauges.",
    ("status",),
    allowed_label_values={"status": CHECK_STATUSES},
)
_LAST_CHECK_TS = gauge(
    "schema_drift_last_check_timestamp_seconds",
    "Unix timestamp of the latest schema/RLS drift check attempt.",
)
_LAST_OK_TS = gauge(
    "schema_drift_last_ok_timestamp_seconds",
    "Unix timestamp of the latest drift-free schema/RLS drift check.",
)
_DURATION_SECONDS = gauge(
    "schema_drift_check_duration_seconds",
    "Duration of the latest schema/RLS drift check.",
)
_CHECKS_TOTAL = counter(
    "schema_drift_checks_total",
    "Schema/RLS drift checks by result.",
    ("result",),
    allowed_label_values={"result": CHECK_STATUSES},
)


def classify_drift(message: str) -> DriftCategory:
    text = message.strip()
    if text.startswith("EXTENSION "):
        return "extension"
    if text.startswith("RLS "):
        return "rls"
    if text.startswith("COLUMN "):
        return "column"
    if text.startswith("INDEX "):
        return "index"
    if text.startswith("CHECK "):
        return "check"
    if text.startswith("TABLE ") and "partitioning mismatch" in text:
        return "partition"
    if text.startswith("TABLE "):
        return "table"
    return "unknown"


def run_schema_drift_check(
    dsn: str,
    *,
    connect: ConnectFn = psycopg2.connect,
    connect_timeout_seconds: int = 10,
    statement_timeout_ms: int = 30_000,
) -> SchemaDriftSnapshot:
    started = time.monotonic()
    checked_at = datetime.now(timezone.utc)
    try:
        conn = connect(dsn, connect_timeout=connect_timeout_seconds)
        try:
            _set_statement_timeout(conn, statement_timeout_ms)
            findings = schema_drift.compare(conn)
        finally:
            conn.close()
    except Exception as exc:  # noqa: BLE001 - metrics must report failures
        snapshot = SchemaDriftSnapshot(
            status="error",
            checked_at=checked_at,
            duration_seconds=time.monotonic() - started,
            findings_total=0,
            findings_by_category={category: 0 for category in DRIFT_CATEGORIES},
            error=exc.__class__.__name__,
        )
        record_schema_drift_metrics(snapshot)
        return snapshot

    counts: Counter[DriftCategory] = Counter(classify_drift(item) for item in findings)
    snapshot = SchemaDriftSnapshot(
        status="drift" if findings else "ok",
        checked_at=checked_at,
        duration_seconds=time.monotonic() - started,
        findings_total=len(findings),
        findings_by_category={
            category: int(counts.get(category, 0))
            for category in DRIFT_CATEGORIES
        },
    )
    record_schema_drift_metrics(snapshot)
    return snapshot


def record_schema_drift_metrics(snapshot: SchemaDriftSnapshot) -> None:
    _LAST_CHECK_TS.set(snapshot.checked_at.timestamp())
    _DURATION_SECONDS.set(snapshot.duration_seconds)
    for status in CHECK_STATUSES:
        _STATUS.set(1.0 if status == snapshot.status else 0.0, status=status)
    for category in DRIFT_CATEGORIES:
        _FINDINGS.set(
            float(snapshot.findings_by_category.get(category, 0)),
            category=category,
        )
    _CHECKS_TOTAL.inc(result=snapshot.status)
    if snapshot.status == "ok":
        _LAST_OK_TS.set(snapshot.checked_at.timestamp())


def render_schema_drift_metrics() -> str:
    return render_default()


def _set_statement_timeout(conn: _PsycopgConnection, timeout_ms: int) -> None:
    if timeout_ms <= 0:
        return
    with conn.cursor() as cur:
        cur.execute("SET statement_timeout = %s", (int(timeout_ms),))


__all__ = [
    "CHECK_STATUSES",
    "DRIFT_CATEGORIES",
    "SchemaDriftSnapshot",
    "classify_drift",
    "record_schema_drift_metrics",
    "render_schema_drift_metrics",
    "run_schema_drift_check",
]
