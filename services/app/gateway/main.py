"""FastAPI app factory and module-level gateway entry point.

Route implementations live in focused router modules. This file owns the
FastAPI factory, lifespan dependency construction, middleware registration,
exception handlers, and route mounting orchestration.
"""
from __future__ import annotations

import asyncio as _asyncio
import contextlib
import os
from typing import AsyncIterator

import asyncpg
from fastapi import FastAPI

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from services.app.gateway.ceo_view_wiring import configure_ceo_view
from services.app.gateway.core_router import (
    IngestSizeError,
    ingest_size_error_handler,
)
from services.app.gateway.db_bootstrap import (
    close_gateway_pool,
    create_gateway_pool,
)
from services.app.gateway.demo_seed import ensure_demo_seed
from services.app.gateway.deps import GatewayDeps, get_gateway_deps
from services.app.gateway.logging_config import configure_structlog, get_logger
from services.app.gateway.middleware import (
    BearerAuthMiddleware,
    RateLimitMiddleware,
    RequestContextMiddleware,
    _PUBLIC_PATH_PREFIXES,
    _PUBLIC_PATHS,
)
from services.app.gateway.rate_limit import RateLimiter
from services.app.gateway.route_mounts import (
    mount_gateway_routes,
    register_gateway_routes,
)
from services.app.gateway.state_wiring import (
    close_ingestion_data_plane,
    wire_in08_state,
    wire_ingestion_data_plane,
)
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo


log = get_logger("gateway")


# Backward-compatible imports for tests and older route modules that still
# import these names from ``services.app.gateway.main``.
_deps = get_gateway_deps
_register_routes = register_gateway_routes
_wire_in08_state = wire_in08_state
_wire_ingestion_data_plane = wire_ingestion_data_plane
_close_ingestion_data_plane = close_ingestion_data_plane
_configure_ceo_view = configure_ceo_view
_ensure_demo_seed = ensure_demo_seed


def build_app(
    *,
    pool: asyncpg.Pool | None = None,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    rate_limiter: RateLimiter | None = None,
    slack_signing_secret: str | None = None,
    configure_logging: bool = True,
) -> FastAPI:
    """Build the FastAPI app. Every dependency is injectable for tests."""
    if configure_logging:
        configure_structlog(os.environ.get("LOG_LEVEL", "INFO"))

    @contextlib.asynccontextmanager
    async def _lifespan(app_: FastAPI) -> AsyncIterator[None]:
        nonlocal pool, actor_repo, alias_repo, embedder, rate_limiter
        if pool is None:
            pool = await create_gateway_pool()
        try:
            await ensure_demo_seed(pool)
        except Exception:  # noqa: BLE001 - startup must not fail here
            log.exception("demo_seed_warning")
        if actor_repo is None:
            actor_repo = ActorRepo(pool)
        if alias_repo is None:
            alias_repo = EntityAliasRepo(pool)
        if embedder is None and os.environ.get("OLLAMA_URL"):
            embedder = OllamaClient(OllamaConfig.from_env())
        if rate_limiter is None:
            rate_limiter = RateLimiter()
        app_.state.deps = GatewayDeps(
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            rate_limiter=rate_limiter,
            slack_signing_secret=(
                slack_signing_secret
                or os.environ.get("SLACK_SIGNING_SECRET")
            ),
        )
        wire_in08_state(app_, pool)

        async def _sweep_oauth_states() -> None:
            while True:
                try:
                    await _asyncio.sleep(300)
                    deleted = await pool.execute(
                        """
                        DELETE FROM oauth_install_states
                         WHERE id IN (
                            SELECT id FROM oauth_install_states
                             WHERE expires_at < now() - INTERVAL '1 hour'
                                OR (consumed_at IS NOT NULL
                                    AND consumed_at < now() - INTERVAL '1 hour')
                             LIMIT 1000
                         )
                        """,
                    )
                    log.info(
                        "oauth_install_states_sweep",
                        deleted_summary=deleted,
                    )
                except _asyncio.CancelledError:
                    raise
                except Exception as exc:  # noqa: BLE001
                    log.error(
                        "oauth_install_states_sweep_error",
                        error_type=type(exc).__name__,
                    )

        app_.state.oauth_sweep_task = _asyncio.create_task(_sweep_oauth_states())

        if getattr(app_.state, "realtime", None) is None:
            from services.app.realtime.main import (
                configure_realtime as _configure_realtime,
            )

            rt_deps = _configure_realtime(app_, pool=pool, start=False)
            await rt_deps.dispatcher.start()

        if os.environ.get("GATEWAY_CEO_VIEW_ENABLED", "1") != "0":
            try:
                await configure_ceo_view(app_, pool=pool)
            except Exception as ceo_exc:  # noqa: BLE001
                log.error(
                    "ceo_view_wiring_failed",
                    error=str(ceo_exc),
                    error_type=type(ceo_exc).__name__,
                )

        await wire_ingestion_data_plane(app_)
        try:
            yield
        finally:
            await close_ingestion_data_plane(app_)

            sweep_task = getattr(app_.state, "oauth_sweep_task", None)
            if sweep_task is not None:
                sweep_task.cancel()
                try:
                    await sweep_task
                except (BaseException,):  # noqa: BLE001
                    pass

            rt = getattr(app_.state, "realtime", None)
            if rt is not None:
                try:
                    await rt.dispatcher.stop()
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "lifespan_dispatcher_stop_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )

            ceo = getattr(app_.state, "ceo_view", None)
            if ceo is not None:
                scheduler = ceo.get("scheduler")
                if scheduler is not None:
                    try:
                        await scheduler.stop()
                    except Exception as exc:  # noqa: BLE001
                        log.warning(
                            "lifespan_scheduler_stop_failed",
                            error=str(exc),
                            error_type=type(exc).__name__,
                        )

            deps: GatewayDeps = app_.state.deps
            if deps.embedder is not None:
                try:
                    await deps.embedder.close()
                except Exception as exc:  # noqa: BLE001
                    log.warning(
                        "lifespan_embedder_close_failed",
                        error=str(exc),
                        error_type=type(exc).__name__,
                    )
            if os.environ.get("GATEWAY_OWNS_POOL", "") == "1":
                await close_gateway_pool(deps.pool)

    app = FastAPI(
        title="Company OS Gateway",
        version="0.1.0",
        lifespan=_lifespan,
    )

    if (
        pool is not None
        and actor_repo is not None
        and alias_repo is not None
        and rate_limiter is not None
    ):
        app.state.deps = GatewayDeps(
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            rate_limiter=rate_limiter,
            slack_signing_secret=slack_signing_secret,
        )
        wire_in08_state(app, pool)

    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(BearerAuthMiddleware)
    app.add_middleware(RequestContextMiddleware)

    app.add_exception_handler(IngestSizeError, ingest_size_error_handler)
    mount_gateway_routes(app, pool=pool)
    return app


app = build_app()


__all__ = [
    "app",
    "build_app",
    "GatewayDeps",
    "_PUBLIC_PATHS",
    "_PUBLIC_PATH_PREFIXES",
    "_deps",
    "_register_routes",
    "_wire_in08_state",
    "_wire_ingestion_data_plane",
    "_close_ingestion_data_plane",
    "_configure_ceo_view",
    "_ensure_demo_seed",
]
