"""Housekeeper worker built on the existing maintenance scheduler.

Housekeeper is intentionally a registry/launcher, not a new business-logic
layer. Each descriptor delegates to an existing bounded job body.
"""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Callable

import asyncpg
import structlog

from services.workers.deadline_resolver.worker import (
    DEFAULT_POLL_INTERVAL_S,
    DeadlineResolver,
)
from services.workers.maintenance.daily import (
    archive_decayed_job,
    hourly_decay_job,
)
from services.workers.maintenance.scheduler import (
    JobDescriptor,
    MaintenanceScheduler,
)
from services.workers.maintenance.weekly import (
    relationship_maintenance_per_tenant,
)
from services.workers.housekeeper.retention import (
    run_sage_trace_retention,
    run_think_run_artifact_retention,
)


_log = structlog.get_logger(__name__)


@dataclass(slots=True)
class HousekeeperRunReport:
    jobs: list[str] = field(default_factory=list)
    completed: int = 0
    failed: int = 0
    errors: dict[str, str] = field(default_factory=dict)
    scheduler_stats: dict[str, dict[str, Any]] = field(default_factory=dict)


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _env_seconds(name: str, default: float) -> timedelta:
    raw = os.environ.get(name)
    try:
        value = float(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return timedelta(seconds=max(0.1, value))


async def _deadline_resolver_job(pool: asyncpg.Pool) -> Any:
    return await DeadlineResolver(pool).run_once()


async def _obligation_due_sweep(pool: asyncpg.Pool) -> Any:
    from services.domain.obligations import sweep_due_obligations

    async with pool.acquire() as conn:
        async with conn.transaction():
            return await sweep_due_obligations(
                conn,
                limit=int(os.environ.get("HOUSEKEEPER_OBLIGATION_SWEEP_LIMIT", "100")),
            )


async def _hourly_decay(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        return await hourly_decay_job(conn=conn)


async def _archive_decayed(pool: asyncpg.Pool) -> int:
    async with pool.acquire() as conn:
        return await archive_decayed_job(conn=conn)


async def _access_matview_refresh(pool: asyncpg.Pool) -> Any:
    from services.workers.maintenance.daily import access_matview_refresh

    async with pool.acquire() as conn:
        return await access_matview_refresh(conn=conn)


async def _relationship_maintenance(pool: asyncpg.Pool) -> Any:
    async with pool.acquire() as conn:
        async with conn.transaction():
            return await relationship_maintenance_per_tenant(conn=conn)


async def _think_run_artifact_retention(pool: asyncpg.Pool) -> Any:
    return await run_think_run_artifact_retention(pool)


async def _sage_trace_retention(pool: asyncpg.Pool) -> Any:
    return await run_sage_trace_retention(pool)


async def _backup_recovery_metrics(pool: asyncpg.Pool) -> Any:
    from services.platform.backup_recovery import refresh_backup_recovery_metrics

    async with pool.acquire() as conn:
        return await refresh_backup_recovery_metrics(conn)


async def _db_activity_metrics(pool: asyncpg.Pool) -> Any:
    from lib.observability.db_activity import refresh_db_activity_metrics

    async with pool.acquire() as conn:
        return await refresh_db_activity_metrics(conn)


async def _calibration_updater(pool: asyncpg.Pool) -> Any:
    from services.workers.calibration_updater.worker import run_once

    return await run_once(pool)


async def _edge_drift(pool: asyncpg.Pool) -> Any:
    from services.workers.edge_drift.worker import run_once

    return await run_once(pool)


async def _topology_sweeper(pool: asyncpg.Pool) -> Any:
    from services.workers.topology_sweeper.worker import run_once

    return await run_once(pool)


async def _precipitation(pool: asyncpg.Pool) -> Any:
    from services.workers.precipitation.worker import run_once

    return await run_once(pool)


async def _relationship_ontology_proposals(pool: asyncpg.Pool) -> Any:
    from services.workers.relationship_ontology_proposals.worker import run_once

    return await run_once(pool)


async def _sage_structural_features(pool: asyncpg.Pool) -> Any:
    from services.workers.sage_structural_features.worker import run_once

    return await run_once(pool)


def _descriptor(
    name: str,
    fn: Callable[[asyncpg.Pool], Any],
    interval: timedelta,
    *,
    enabled: bool = True,
    initial_delay: timedelta = timedelta(seconds=0),
) -> JobDescriptor:
    return JobDescriptor(
        name=name,
        fn=fn,
        interval=interval,
        initial_delay=initial_delay,
        enabled=enabled,
    )


def build_housekeeper_descriptors(
    *,
    include_expensive: bool | None = None,
) -> list[JobDescriptor]:
    """Build the scheduled job registry.

    Expensive jobs stay opt-in until their runtime is characterized in the
    deployment that owns the data volume.
    """

    expensive = (
        _env_bool("HOUSEKEEPER_ENABLE_EXPENSIVE_JOBS", False)
        if include_expensive is None
        else include_expensive
    )
    descriptors = [
        _descriptor(
            "deadline_resolver",
            _deadline_resolver_job,
            _env_seconds("HOUSEKEEPER_DEADLINE_RESOLVER_INTERVAL_S", DEFAULT_POLL_INTERVAL_S),
        ),
        _descriptor(
            "obligation_due_sweep",
            _obligation_due_sweep,
            _env_seconds("HOUSEKEEPER_OBLIGATION_SWEEP_INTERVAL_S", DEFAULT_POLL_INTERVAL_S),
        ),
        _descriptor(
            "hourly_decay",
            _hourly_decay,
            _env_seconds("HOUSEKEEPER_HOURLY_DECAY_INTERVAL_S", 3600),
            initial_delay=_env_seconds("HOUSEKEEPER_HOURLY_DECAY_INITIAL_DELAY_S", 5),
        ),
        _descriptor(
            "archive_decayed",
            _archive_decayed,
            _env_seconds("HOUSEKEEPER_ARCHIVE_DECAYED_INTERVAL_S", 86400),
            initial_delay=_env_seconds("HOUSEKEEPER_ARCHIVE_DECAYED_INITIAL_DELAY_S", 30),
        ),
        _descriptor(
            "access_matview_refresh",
            _access_matview_refresh,
            _env_seconds("HOUSEKEEPER_ACCESS_MATVIEW_REFRESH_INTERVAL_S", 86400),
            initial_delay=_env_seconds(
                "HOUSEKEEPER_ACCESS_MATVIEW_REFRESH_INITIAL_DELAY_S",
                45,
            ),
        ),
        _descriptor(
            "relationship_maintenance",
            _relationship_maintenance,
            _env_seconds("HOUSEKEEPER_RELATIONSHIP_MAINTENANCE_INTERVAL_S", 604800),
            initial_delay=_env_seconds(
                "HOUSEKEEPER_RELATIONSHIP_MAINTENANCE_INITIAL_DELAY_S",
                60,
            ),
        ),
        _descriptor(
            "think_run_artifact_retention",
            _think_run_artifact_retention,
            _env_seconds("HOUSEKEEPER_THINK_ARTIFACT_RETENTION_INTERVAL_S", 86400),
            initial_delay=_env_seconds(
                "HOUSEKEEPER_THINK_ARTIFACT_RETENTION_INITIAL_DELAY_S",
                180,
            ),
        ),
        _descriptor(
            "sage_trace_retention",
            _sage_trace_retention,
            _env_seconds("HOUSEKEEPER_SAGE_TRACE_RETENTION_INTERVAL_S", 86400),
            initial_delay=_env_seconds(
                "HOUSEKEEPER_SAGE_TRACE_RETENTION_INITIAL_DELAY_S",
                210,
            ),
        ),
        _descriptor(
            "backup_recovery_metrics",
            _backup_recovery_metrics,
            _env_seconds("HOUSEKEEPER_BACKUP_RECOVERY_METRICS_INTERVAL_S", 300),
            initial_delay=_env_seconds(
                "HOUSEKEEPER_BACKUP_RECOVERY_METRICS_INITIAL_DELAY_S",
                20,
            ),
        ),
        _descriptor(
            "db_activity_metrics",
            _db_activity_metrics,
            _env_seconds("HOUSEKEEPER_DB_ACTIVITY_METRICS_INTERVAL_S", 60),
            initial_delay=_env_seconds(
                "HOUSEKEEPER_DB_ACTIVITY_METRICS_INITIAL_DELAY_S",
                10,
            ),
        ),
        _descriptor(
            "calibration_updater",
            _calibration_updater,
            _env_seconds("HOUSEKEEPER_CALIBRATION_UPDATER_INTERVAL_S", 604800),
            initial_delay=_env_seconds("HOUSEKEEPER_CALIBRATION_UPDATER_INITIAL_DELAY_S", 90),
        ),
        _descriptor(
            "edge_drift",
            _edge_drift,
            _env_seconds("HOUSEKEEPER_EDGE_DRIFT_INTERVAL_S", 1800),
            initial_delay=_env_seconds("HOUSEKEEPER_EDGE_DRIFT_INITIAL_DELAY_S", 120),
        ),
        _descriptor(
            "topology_sweeper",
            _topology_sweeper,
            _env_seconds("HOUSEKEEPER_TOPOLOGY_SWEEPER_INTERVAL_S", 900),
            enabled=expensive or _env_bool("HOUSEKEEPER_ENABLE_TOPOLOGY_SWEEPER", False),
        ),
        _descriptor(
            "precipitation",
            _precipitation,
            _env_seconds("HOUSEKEEPER_PRECIPITATION_INTERVAL_S", 86400),
            enabled=expensive or _env_bool("HOUSEKEEPER_ENABLE_PRECIPITATION", False),
        ),
        _descriptor(
            "relationship_ontology_proposals",
            _relationship_ontology_proposals,
            _env_seconds("HOUSEKEEPER_RELATIONSHIP_ONTOLOGY_INTERVAL_S", 2592000),
            enabled=expensive
            or _env_bool("HOUSEKEEPER_ENABLE_RELATIONSHIP_ONTOLOGY_PROPOSALS", False),
        ),
        _descriptor(
            "sage_structural_features",
            _sage_structural_features,
            _env_seconds("HOUSEKEEPER_STRUCTURAL_FEATURES_INTERVAL_S", 3600),
            enabled=expensive or _env_bool("HOUSEKEEPER_ENABLE_STRUCTURAL_FEATURES", False),
        ),
    ]
    return descriptors


async def run_once_all(
    pool: asyncpg.Pool,
    *,
    descriptors: list[JobDescriptor] | None = None,
    job_names: list[str] | None = None,
    scheduler_factory: type[MaintenanceScheduler] = MaintenanceScheduler,
) -> HousekeeperRunReport:
    """Run selected enabled jobs once under scheduler advisory locks."""

    descriptors = descriptors or build_housekeeper_descriptors()
    selected = set(job_names or [d.name for d in descriptors if d.enabled])
    scheduler = scheduler_factory(pool=pool, descriptors=descriptors)
    report = HousekeeperRunReport(jobs=[d.name for d in descriptors if d.name in selected])
    for descriptor in descriptors:
        if descriptor.name not in selected or not descriptor.enabled:
            continue
        try:
            await scheduler.run_job_now(descriptor.name)
            report.completed += 1
        except Exception as exc:  # noqa: BLE001
            report.failed += 1
            report.errors[descriptor.name] = f"{type(exc).__name__}: {exc}"
            _log.exception("housekeeper.job_failed", job=descriptor.name)
    report.scheduler_stats = scheduler.stats()
    return report


async def run_forever(
    pool: asyncpg.Pool,
    *,
    descriptors: list[JobDescriptor] | None = None,
    shutdown: asyncio.Event | None = None,
) -> None:
    """Start the scheduler and wait until shutdown is requested."""

    scheduler = MaintenanceScheduler(
        pool=pool,
        descriptors=descriptors or build_housekeeper_descriptors(),
    )
    shutdown = shutdown or asyncio.Event()
    await scheduler.start()
    try:
        await shutdown.wait()
    finally:
        await scheduler.stop()


__all__ = [
    "HousekeeperRunReport",
    "build_housekeeper_descriptors",
    "run_forever",
    "run_once_all",
]
