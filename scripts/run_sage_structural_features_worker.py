"""Launcher for the SAGE structural-feature recompute worker."""
from __future__ import annotations

import asyncio
import os
import signal
from uuid import UUID

import asyncpg
import structlog

from services.gateway.db_bootstrap import _register_codecs
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

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=3,
        init=_register_codecs,
    )
    shutdown = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, shutdown.set)
        except NotImplementedError:
            pass

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
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())

