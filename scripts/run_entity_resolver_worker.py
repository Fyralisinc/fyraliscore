"""Launcher for the deferred entity resolver worker."""
from __future__ import annotations

import asyncio
import os
from collections import Counter

import asyncpg
import structlog

from worker_observability import (
    install_signal_handlers,
    register_pool,
    start_worker_health,
)
from lib.llm.provider import build_provider
from lib.observability.metrics import render_default
from lib.shared.db import asyncpg_pool_runtime_kwargs, positive_int_env
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.workers.entity_resolver.worker import (
    EntityResolverWorker,
    ResolverDecision,
    ResolverLLMBudget,
)


_DECISIONS: tuple[ResolverDecision, ...] = (
    "resolved",
    "review",
    "dropped",
    "rate_limited",
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


def _render_stats(stats: Counter[str]) -> str:
    lines = [
        "# HELP entity_resolver_cycles_total Entity resolver poll cycles.",
        "# TYPE entity_resolver_cycles_total counter",
        f"entity_resolver_cycles_total {stats['cycles']}",
        "# HELP entity_resolver_cycle_errors_total Entity resolver poll cycle failures.",
        "# TYPE entity_resolver_cycle_errors_total counter",
        f"entity_resolver_cycle_errors_total {stats['cycle_errors']}",
        "# HELP entity_resolver_observations_processed_total Observations scanned by the entity resolver.",
        "# TYPE entity_resolver_observations_processed_total counter",
        f"entity_resolver_observations_processed_total {stats['observations_processed']}",
        "# HELP entity_resolver_phrases_seen_total Unresolved phrases processed by the entity resolver.",
        "# TYPE entity_resolver_phrases_seen_total counter",
        f"entity_resolver_phrases_seen_total {stats['phrases_seen']}",
    ]
    for decision in _DECISIONS:
        metric = f"entity_resolver_phrases_{decision}_total"
        lines.extend(
            [
                f"# HELP {metric} Entity resolver phrases with decision {decision}.",
                f"# TYPE {metric} counter",
                f"{metric} {stats[decision]}",
            ]
        )
    return "\n".join(lines) + "\n" + render_default()


async def _main() -> None:
    log = structlog.get_logger("dogfood.entity_resolver")
    dsn = os.environ["DATABASE_URL"]
    poll_s = _float_env("ENTITY_RESOLVER_POLL_INTERVAL_S", 30.0)
    batch_size = positive_int_env("ENTITY_RESOLVER_BATCH_SIZE", default=50)
    budget_per_min = positive_int_env(
        "ENTITY_RESOLVER_LLM_BUDGET_PER_MIN",
        default=30,
    )
    pool_max = positive_int_env("ENTITY_RESOLVER_POSTGRES_POOL_SIZE", default=4)
    runtime_kwargs = asyncpg_pool_runtime_kwargs(
        dsn=dsn,
        process_env_var="ENTITY_RESOLVER_POSTGRES_PGBOUNCER_COMPATIBLE",
    )

    pool = await asyncpg.create_pool(
        dsn=dsn,
        min_size=1,
        max_size=pool_max,
        init=_register_codecs,
        **runtime_kwargs,
    )
    register_pool("entity_resolver_worker", pool)

    shutdown = asyncio.Event()
    install_signal_handlers(shutdown)
    stats: Counter[str] = Counter()
    health_shutdown = start_worker_health(
        "entity_resolver_worker",
        shutdown,
        render_metrics=lambda: _render_stats(stats),
    )

    llm = build_provider()
    worker = EntityResolverWorker(
        pool=pool,
        llm=llm,
        alias_repo=EntityAliasRepo(pool),
        budget=ResolverLLMBudget(per_minute=budget_per_min),
    )

    def _record_decisions(
        _observation_id: object,
        _tenant_id: object,
        decisions: list[tuple[str, ResolverDecision]],
    ) -> None:
        stats["phrases_seen"] += len(decisions)
        for _phrase, decision in decisions:
            stats[decision] += 1

    log.info(
        "entity_resolver.starting",
        poll_interval_s=poll_s,
        batch_size=batch_size,
        budget_per_min=budget_per_min,
        llm_provider=llm.config.provider,
        llm_model=llm.config.model,
    )
    try:
        while not shutdown.is_set():
            stats["cycles"] += 1
            try:
                processed = await worker.process_pending(
                    limit=batch_size,
                    on_decisions=_record_decisions,
                )
                stats["observations_processed"] += processed
            except Exception as exc:  # noqa: BLE001
                stats["cycle_errors"] += 1
                log.exception("entity_resolver.loop_error", error=str(exc))
            try:
                await asyncio.wait_for(shutdown.wait(), timeout=poll_s)
                break
            except asyncio.TimeoutError:
                pass
    finally:
        log.info(
            "entity_resolver.stopping",
            cycles=stats["cycles"],
            observations_processed=stats["observations_processed"],
            phrases_seen=stats["phrases_seen"],
        )
        await health_shutdown()
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
