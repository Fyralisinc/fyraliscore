"""Launcher for the production schema/RLS drift monitor."""
from __future__ import annotations

import asyncio
import os

import structlog

from worker_observability import install_signal_handlers, start_worker_health
from services.platform.schema_drift_monitor import (
    render_schema_drift_metrics,
    run_schema_drift_check,
)


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = float(raw)
    except ValueError:
        return default
    return value if value > 0 else default


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


async def _main() -> None:
    log = structlog.get_logger("dogfood.schema_drift_monitor")
    dsn = os.environ["DATABASE_URL"]
    interval_s = _float_env("SCHEMA_DRIFT_CHECK_INTERVAL_S", 300.0)
    connect_timeout_s = _int_env("SCHEMA_DRIFT_CONNECT_TIMEOUT_S", 10)
    statement_timeout_ms = _int_env("SCHEMA_DRIFT_STATEMENT_TIMEOUT_MS", 30_000)

    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    health_shutdown = start_worker_health(
        "schema_drift_monitor",
        shutdown,
        render_metrics=render_schema_drift_metrics,
    )

    log.info(
        "schema_drift_monitor.starting",
        interval_s=interval_s,
        connect_timeout_s=connect_timeout_s,
        statement_timeout_ms=statement_timeout_ms,
    )
    try:
        while not shutdown.is_set():
            snapshot = await asyncio.to_thread(
                run_schema_drift_check,
                dsn,
                connect_timeout_seconds=connect_timeout_s,
                statement_timeout_ms=statement_timeout_ms,
            )
            if snapshot.status == "ok":
                log.info(
                    "schema_drift_monitor.ok",
                    duration_s=round(snapshot.duration_seconds, 3),
                )
            elif snapshot.status == "drift":
                log.warning(
                    "schema_drift_monitor.drift_detected",
                    findings_total=snapshot.findings_total,
                    findings_by_category=snapshot.findings_by_category,
                    duration_s=round(snapshot.duration_seconds, 3),
                )
            else:
                log.error(
                    "schema_drift_monitor.check_failed",
                    error=snapshot.error,
                    duration_s=round(snapshot.duration_seconds, 3),
                )
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=interval_s)
                break
            except asyncio.TimeoutError:
                pass
    finally:
        log.info("schema_drift_monitor.stopping")
        await health_shutdown()


if __name__ == "__main__":
    asyncio.run(_main())
