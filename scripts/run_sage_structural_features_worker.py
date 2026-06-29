"""Launcher for the SAGE structural-feature recompute worker."""
from __future__ import annotations

import asyncio
import os
from uuid import UUID

import asyncpg
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env
from services.app.gateway.db_bootstrap import _register_codecs
from services.workers.sage_structural_features.worker import (
    DEFAULT_INTERVAL_S,
    run_forever,
    run_once,
)


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _env_uuid(name: str) -> UUID | None:
    raw = os.environ.get(name)
    if raw is None or not raw.strip():
        return None
    return UUID(raw.strip())


async def _main() -> None:
    log = structlog.get_logger("dogfood.sage_structural_features")
    dsn = os.environ["DATABASE_URL"]
    interval_s = float(
        os.environ.get(
            "SAGE_STRUCTURAL_FEATURES_INTERVAL_S",
            str(DEFAULT_INTERVAL_S),
        )
    )
    once = _env_bool("SAGE_STRUCTURAL_FEATURES_ONCE", False)
    tenant_id = _env_uuid("SAGE_STRUCTURAL_FEATURES_TENANT_ID")
    pool_max = positive_int_env("MAINTENANCE_POSTGRES_POOL_SIZE", default=3)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="MAINTENANCE_POSTGRES_PGBOUNCER_COMPATIBLE",
    )

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=pool_max,
        init=_register_codecs,
        **runtime_kwargs,
    )
    register_pool("sage_structural_features_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    health_shutdown = start_worker_health(
        "sage_structural_features_worker", shutdown
    )

    log.info(
        "sage_structural_features.starting",
        interval_s=interval_s,
        once=once,
        tenant_id=str(tenant_id) if tenant_id is not None else None,
    )
    try:
        if once:
            report = await run_once(pool, tenant_id=tenant_id)
            log.info(
                "sage_structural_features.once_done",
                tenants=report.tenants_scanned,
                models_written=report.models_written,
                edges_written=report.edges_written,
                errors=report.errors,
            )
        else:
            await run_forever(
                pool,
                interval_s=interval_s,
                shutdown=shutdown,
            )
    finally:
        log.info("sage_structural_features.stopping")
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
