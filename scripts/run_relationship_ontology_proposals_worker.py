"""Launcher for the relationship ontology proposal aggregation worker."""
from __future__ import annotations

import asyncio
import os
import signal
from uuid import UUID

import asyncpg
import structlog

from services.gateway.db_bootstrap import _register_codecs
from services.workers.relationship_ontology_proposals.worker import (
    DEFAULT_INTERVAL_S,
    DEFAULT_LIMIT_PER_TENANT,
    DEFAULT_MIN_EXAMPLES,
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
    log = structlog.get_logger("dogfood.relationship_ontology_proposals")
    dsn = os.environ["DATABASE_URL"]
    interval_s = float(
        os.environ.get(
            "RELATIONSHIP_ONTOLOGY_PROPOSALS_INTERVAL_S",
            str(DEFAULT_INTERVAL_S),
        )
    )
    min_examples = int(
        os.environ.get(
            "RELATIONSHIP_ONTOLOGY_PROPOSALS_MIN_EXAMPLES",
            str(DEFAULT_MIN_EXAMPLES),
        )
    )
    limit_per_tenant = int(
        os.environ.get(
            "RELATIONSHIP_ONTOLOGY_PROPOSALS_LIMIT_PER_TENANT",
            str(DEFAULT_LIMIT_PER_TENANT),
        )
    )
    once = _env_bool("RELATIONSHIP_ONTOLOGY_PROPOSALS_ONCE", False)
    tenant_id = _env_uuid("RELATIONSHIP_ONTOLOGY_PROPOSALS_TENANT_ID")

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
        "relationship_ontology_proposals.starting",
        interval_s=interval_s,
        min_examples=min_examples,
        limit_per_tenant=limit_per_tenant,
        once=once,
        tenant_id=str(tenant_id) if tenant_id is not None else None,
    )
    try:
        if once:
            report = await run_once(
                pool,
                tenant_id=tenant_id,
                minimum_distinct_examples=min_examples,
                limit_per_tenant=limit_per_tenant,
            )
            log.info(
                "relationship_ontology_proposals.once_done",
                tenants=report.tenants_scanned,
                proposals_upserted=report.proposals_upserted,
                review_ready=report.review_ready,
                errors=report.errors,
            )
        else:
            await run_forever(
                pool,
                interval_s=interval_s,
                minimum_distinct_examples=min_examples,
                limit_per_tenant=limit_per_tenant,
                shutdown=shutdown,
            )
    finally:
        log.info("relationship_ontology_proposals.stopping")
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())

