"""Launcher for services.reasoning.think.worker.ThinkWorker — one worker process.

Bridges `ThinkWorker(pool).run()` to an asyncio-driven CLI. Kept minimal
on purpose: the worker owns its own poll/dispatch loop and graceful
shutdown via SIGTERM/SIGINT.
"""
from __future__ import annotations

import asyncio
from dataclasses import replace
import os
import pathlib
import sys

import asyncpg
import structlog

# In-container the repo lives at /app but `python scripts/x.py` puts
# /app/scripts (not /app) on sys.path — same bootstrap as the other
# script launchers (run_discord_gateway_worker.py).
_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from lib.llm.provider import LLMProvider, build_provider  # noqa: E402
from lib.observability.pools import register_pool  # noqa: E402
from services.app.gateway.db_bootstrap import _register_codecs  # noqa: E402
from services.reasoning.think.worker import ThinkWorker  # noqa: E402
from services.reasoning.think.lanes import lane_names  # noqa: E402
from services.domain.entity_grounding.learned_discovery import (  # noqa: E402
    DiscoveryProviderPreflightError,
    preflight_structured_discovery,
)


def _discovery_fallback_models() -> tuple[str, ...]:
    """Explicit ordered allowlist; an empty value forbids model downgrade."""

    raw = os.environ.get("ENTITY_DISCOVERY_FALLBACK_MODELS", "")
    return tuple(dict.fromkeys(model.strip() for model in raw.split(",") if model.strip()))


async def _ready_discovery_provider(
    primary: LLMProvider,
    *,
    log: structlog.stdlib.BoundLogger,
) -> LLMProvider:
    candidates = (primary.config.model, *_discovery_fallback_models())
    failures: list[tuple[str, DiscoveryProviderPreflightError]] = []
    for index, model in enumerate(candidates):
        provider = primary if index == 0 else build_provider(replace(
            primary.config,
            model=model,
            circuit_breaker_name=f"{primary.config.provider}:entity-discovery:{model}",
        ))
        try:
            await preflight_structured_discovery(provider)
        except DiscoveryProviderPreflightError as exc:
            failures.append((model, exc))
            log.error(
                "think_worker.entity_discovery_preflight_failed",
                provider=provider.config.provider,
                model=model,
                failure_code=exc.code,
                retryable=exc.retryable,
            )
            continue
        if index:
            log.warning(
                "think_worker.entity_discovery_explicit_fallback_selected",
                primary_model=primary.config.model,
                fallback_model=model,
            )
        return provider
    rendered = "; ".join(
        f"{model}={failure.code}" for model, failure in failures
    )
    raise RuntimeError(
        "learned entity discovery is not ready; worker startup refused: " + rendered
    ) from failures[-1][1]


async def _main() -> None:
    log = structlog.get_logger("dogfood.think_worker")
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(
        dsn=dsn, min_size=2, max_size=8, init=_register_codecs,
    )
    register_pool("think_worker", pool)
    llm = build_provider()
    try:
        discovery_llm = await _ready_discovery_provider(llm, log=log)
        worker = ThinkWorker(
            pool,
            # The explicit fallback has proven the same structured capability
            # required by Think. Do not start reasoning with a primary model
            # whose transport/model preflight just failed.
            llm_provider=discovery_llm,
            mention_discovery_provider=discovery_llm,
        )
        worker.install_signal_handlers()
        log.info(
            "think_worker.starting",
            llm_provider=discovery_llm.config.provider,
            llm_model=discovery_llm.config.model,
            configured_primary_model=llm.config.model,
            learned_entity_discovery="ready_at_persisted_t1_batch_boundary",
            entity_discovery_model=discovery_llm.config.model,
            lanes=lane_names(worker.config.allowed_lanes),
            stage1_company_memory_for_t1=(
                worker.config.stage1_company_memory_for_t1
            ),
        )
        await worker.run()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(_main())
