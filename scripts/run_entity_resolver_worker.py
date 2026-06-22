"""Launcher for the deferred entity resolver worker."""
from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import asyncpg
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.llm.provider import build_provider
from lib.observability.metrics import render_default
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.workers.entity_resolver.worker import (
    EntityResolverWorker,
    ResolverLLMBudget,
)


@dataclass
class EntityResolverStats:
    iterations: int = 0
    observations_processed: int = 0
    failures: int = 0


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on", "y"}


def _render_stats(stats: EntityResolverStats) -> str:
    lines = [
        "# HELP entity_resolver_iterations_total Entity resolver poll iterations.",
        "# TYPE entity_resolver_iterations_total counter",
        f"entity_resolver_iterations_total {stats.iterations}",
        "# HELP entity_resolver_observations_processed_total Observations scanned by the entity resolver.",
        "# TYPE entity_resolver_observations_processed_total counter",
        f"entity_resolver_observations_processed_total {stats.observations_processed}",
        "# HELP entity_resolver_failures_total Entity resolver poll-loop failures.",
        "# TYPE entity_resolver_failures_total counter",
        f"entity_resolver_failures_total {stats.failures}",
    ]
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("dogfood.entity_resolver")
    dsn = os.environ["DATABASE_URL"]
    poll_s = float(os.environ.get("ENTITY_RESOLVER_POLL_INTERVAL_S", "30"))
    batch_size = int(os.environ.get("ENTITY_RESOLVER_BATCH_SIZE", "25"))
    budget_per_min = int(os.environ.get("ENTITY_RESOLVER_LLM_BUDGET_PER_MIN", "30"))
    once = _env_bool("ENTITY_RESOLVER_ONCE", False)

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=4,
        init=_register_codecs,
    )
    register_pool("entity_resolver_worker", pool)
    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats = EntityResolverStats()
    health_shutdown = start_worker_health(
        "entity_resolver_worker",
        shutdown,
        render_metrics=lambda: _render_stats(stats),
    )

    provider = build_provider()
    worker = EntityResolverWorker(
        pool=pool,
        llm=provider,
        alias_repo=EntityAliasRepo(pool),
        budget=ResolverLLMBudget(per_minute=budget_per_min),
    )

    log.info(
        "entity_resolver.starting",
        once=once,
        poll_s=poll_s,
        batch_size=batch_size,
        llm_provider=provider.config.provider,
        llm_model=provider.config.model,
        budget_per_min=budget_per_min,
    )
    try:
        while not shutdown.is_set():
            stats.iterations += 1
            try:
                processed = await worker.process_pending(limit=batch_size)
                stats.observations_processed += processed
                log.info("entity_resolver.poll_done", processed=processed)
            except Exception as exc:  # noqa: BLE001
                stats.failures += 1
                log.exception("entity_resolver.loop_error", error=str(exc))
            if once:
                break
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=poll_s)
                break
            except asyncio.TimeoutError:
                continue
    finally:
        log.info(
            "entity_resolver.stopping",
            iterations=stats.iterations,
            observations_processed=stats.observations_processed,
            failures=stats.failures,
        )
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
