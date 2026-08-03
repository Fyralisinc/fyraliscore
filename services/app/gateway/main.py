"""FastAPI app factory and module-level gateway entry point.

Route implementations live in focused router modules. This file owns the
FastAPI factory, lifespan dependency construction, middleware registration,
exception handlers, and route mounting orchestration.
"""
from __future__ import annotations

import asyncio
import contextlib
from dataclasses import dataclass
from typing import AsyncIterator, Awaitable, TypeVar

import asyncpg
from fastapi import FastAPI

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.shared.db import assert_pool_database_startup_safety
from services.app.gateway.ceo_view_wiring import configure_ceo_view
from services.app.gateway.core_router import (
    IngestSizeError,
    ingest_size_error_handler,
)
from services.app.gateway.db_bootstrap import (
    close_gateway_pool,
    create_gateway_pool,
)
from services.app.gateway.deps import GatewayDeps, attach_gateway_deps
from services.app.gateway.error_handlers import install_safe_error_handlers
from services.app.gateway.extensions import run_extension_startup_hooks
from services.app.gateway.logging_config import configure_structlog, get_logger
from services.app.gateway.middleware import (
    BearerAuthMiddleware,
    RateLimitMiddleware,
    RequestContextMiddleware,
    _PUBLIC_PATH_PREFIXES,
    _PUBLIC_PATHS,
)
from services.app.gateway.oauth_state_sweeper import (
    start_oauth_state_sweeper,
    stop_oauth_state_sweeper,
)
from services.app.gateway.rate_limit import RateLimiter
from services.app.gateway.route_mounts import mount_gateway_routes
from services.app.gateway.settings import GatewaySettings
from services.app.gateway.startup_status import StartupStatus
from services.app.gateway.state_wiring import (
    IntegrationRuntimeValidationError,
    IntegrationRuntimeWiring,
    IntegrationRuntimeWiringError,
    close_ingestion_data_plane,
    validate_integration_runtime_state,
    wire_integration_runtime_state,
    wire_ingestion_data_plane,
)
from services.domain.actors.repo import ActorRepo
from services.domain.entity_aliases.repo import EntityAliasRepo
from services.ingest.integrations.github.gateway_wiring import (
    close_github_gateway_state,
    wire_github_gateway_state,
)
from services.ingest.connector_platform.startup import (
    wire_source_connector_runtime,
)


log = get_logger("gateway")

_T = TypeVar("_T")


@dataclass(frozen=True, slots=True)
class _CompletedGatewayDeps:
    actor_repo: ActorRepo
    alias_repo: EntityAliasRepo
    embedder: OllamaClient | None
    rate_limiter: RateLimiter
    owns_embedder: bool


@dataclass(frozen=True, slots=True)
class _GatewayRuntime:
    """Runtime bundle created or reused for one gateway lifespan."""

    pool: asyncpg.Pool
    deps: GatewayDeps


def _complete_gateway_deps(
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo | None,
    alias_repo: EntityAliasRepo | None,
    embedder: OllamaClient | None,
    rate_limiter: RateLimiter | None,
    settings: GatewaySettings,
) -> _CompletedGatewayDeps:
    """Fill optional gateway dependencies from the runtime pool/settings."""
    owns_embedder = False
    if actor_repo is None:
        actor_repo = ActorRepo(pool)
    if alias_repo is None:
        alias_repo = EntityAliasRepo(pool)
    if embedder is None and settings.ollama_url:
        embedder = OllamaClient(OllamaConfig.from_env())
        owns_embedder = True
    if rate_limiter is None:
        rate_limiter = RateLimiter()
    return _CompletedGatewayDeps(
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        rate_limiter=rate_limiter,
        owns_embedder=owns_embedder,
    )


async def _await_startup(
    component: str,
    awaitable: Awaitable[_T],
    *,
    timeout_s: float,
) -> _T:
    try:
        return await asyncio.wait_for(awaitable, timeout=timeout_s)
    except TimeoutError as exc:
        raise TimeoutError(
            f"{component} startup exceeded {timeout_s:g}s"
        ) from exc


def _clear_app_state(app_: FastAPI, *names: str) -> None:
    for name in names:
        setattr(app_.state, name, None)


async def _close_ingestion_data_plane_for_lifespan(app_: FastAPI) -> None:
    try:
        await close_ingestion_data_plane(app_)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "lifespan_ingestion_data_plane_close_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )


async def _stop_oauth_sweeper_for_lifespan(
    app_: FastAPI,
    task: object | None,
) -> None:
    try:
        await stop_oauth_state_sweeper(task)  # type: ignore[arg-type]
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "lifespan_oauth_sweeper_stop_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
    if getattr(app_.state, "oauth_sweep_task", None) is task:
        app_.state.oauth_sweep_task = None


async def _stop_realtime_for_lifespan(app_: FastAPI, realtime: object) -> None:
    dispatcher = getattr(realtime, "dispatcher", None)
    if dispatcher is not None:
        try:
            await dispatcher.stop()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "lifespan_dispatcher_stop_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    if getattr(app_.state, "realtime", None) is realtime:
        app_.state.realtime = None


async def _stop_ceo_view_for_lifespan(app_: FastAPI, ceo_view: object) -> None:
    scheduler = ceo_view.get("scheduler") if isinstance(ceo_view, dict) else None
    if scheduler is not None:
        try:
            await scheduler.stop()
        except Exception as exc:  # noqa: BLE001
            log.warning(
                "lifespan_scheduler_stop_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    query_handler = (
        ceo_view.get("qry_handler") if isinstance(ceo_view, dict) else None
    )
    if query_handler is not None:
        close_query_handler = getattr(query_handler, "aclose", None)
        if close_query_handler is not None:
            try:
                await close_query_handler()
            except Exception as exc:  # noqa: BLE001
                log.warning(
                    "lifespan_query_handler_close_failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                )
    if getattr(app_.state, "ceo_view", None) is ceo_view:
        app_.state.ceo_view = None


async def _close_owned_embedder_for_lifespan(
    app_: FastAPI,
    embedder: OllamaClient | None,
) -> None:
    if embedder is None:
        return
    try:
        await embedder.close()
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "lifespan_embedder_close_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
    deps = getattr(app_.state, "deps", None)
    if deps is not None and deps.embedder is embedder:
        app_.state.deps = None
    app_.state.gateway_owns_embedder = False


async def _close_owned_pool_for_lifespan(
    app_: FastAPI,
    pool: asyncpg.Pool | None,
) -> None:
    try:
        await close_gateway_pool(pool)
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "lifespan_pool_close_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
    deps = getattr(app_.state, "deps", None)
    if deps is not None and deps.pool is pool:
        app_.state.deps = None
    if getattr(app_.state, "pool", None) is pool:
        app_.state.pool = None
    _clear_app_state(
        app_,
        "secret_store",
        "tenant_resolver",
        "tenant_flags",
        "gateway_runtime",
    )


def _prepare_existing_github_cleanup(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
) -> object | None:
    if not bool(getattr(app_.state, "gateway_owns_github_client", False)):
        return None
    existing_github_client = getattr(app_.state, "github_client", None)
    if existing_github_client is None:
        return None
    stack.push_async_callback(
        close_github_gateway_state,
        app_,
        client=existing_github_client,
    )
    return existing_github_client


async def _start_gateway_deps(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
    *,
    startup_status: StartupStatus,
    settings: GatewaySettings,
    pool: asyncpg.Pool | None,
    actor_repo: ActorRepo | None,
    alias_repo: EntityAliasRepo | None,
    embedder: OllamaClient | None,
    rate_limiter: RateLimiter | None,
) -> _GatewayRuntime:
    runtime_pool = pool
    runtime_actor_repo = actor_repo
    runtime_alias_repo = alias_repo
    runtime_embedder = embedder
    runtime_rate_limiter = rate_limiter

    deps = getattr(app_.state, "deps", None)
    if deps is None:
        if runtime_pool is None:
            try:
                runtime_pool = await _await_startup(
                    "db_pool",
                    create_gateway_pool(),
                    timeout_s=settings.db_startup_timeout_s,
                )
            except Exception as exc:  # noqa: BLE001
                startup_status.failed_component(
                    "db_pool",
                    required=True,
                    exc=exc,
                )
                raise
            startup_status.ok(
                "db_pool",
                required=True,
                detail="created_by_gateway",
            )
            stack.push_async_callback(
                _close_owned_pool_for_lifespan,
                app_,
                runtime_pool,
            )
        else:
            startup_status.ok(
                "db_pool",
                required=True,
                detail="injected",
            )
        if settings.is_production:
            try:
                await _await_startup(
                    "db_startup_guard",
                    assert_pool_database_startup_safety(runtime_pool),
                    timeout_s=settings.db_startup_timeout_s,
                )
            except Exception as exc:  # noqa: BLE001
                startup_status.failed_component(
                    "db_startup_guard",
                    required=True,
                    exc=exc,
                )
                raise
            startup_status.ok(
                "db_startup_guard",
                required=True,
                detail="strict_role_and_rls",
            )
        try:
            completed_deps = _complete_gateway_deps(
                pool=runtime_pool,
                actor_repo=runtime_actor_repo,
                alias_repo=runtime_alias_repo,
                embedder=runtime_embedder,
                rate_limiter=runtime_rate_limiter,
                settings=settings,
            )
            runtime_embedder = completed_deps.embedder
            deps = attach_gateway_deps(
                app_,
                pool=runtime_pool,
                actor_repo=completed_deps.actor_repo,
                alias_repo=completed_deps.alias_repo,
                embedder=runtime_embedder,
                rate_limiter=completed_deps.rate_limiter,
            )
        except Exception as exc:  # noqa: BLE001
            startup_status.failed_component("deps", required=True, exc=exc)
            raise
        app_.state.gateway_owns_embedder = completed_deps.owns_embedder
        if completed_deps.owns_embedder:
            stack.push_async_callback(
                _close_owned_embedder_for_lifespan,
                app_,
                runtime_embedder,
            )
    else:
        runtime_pool = deps.pool
        runtime_embedder = deps.embedder
        if bool(getattr(app_.state, "gateway_owns_embedder", False)):
            stack.push_async_callback(
                _close_owned_embedder_for_lifespan,
                app_,
                runtime_embedder,
            )
        startup_status.ok(
            "db_pool",
            required=True,
            detail="pre_attached",
        )

    startup_status.ok("deps", required=True)

    # ---- Ask Fyralis overlay (ported retrieval/memory feature) ----------
    # Needs the live pool at construction, so it wires here in lifespan once
    # the pool is available. Optional — a failure degrades but never blocks.
    try:
        import os
        from uuid import UUID as _AskUUID

        from services.product.ask.api import build_router as build_ask_router
        from services.product.ask.orchestrator import AskOrchestrator
        from services.product.ask.store import PostgresAskStore
        from services.reasoning.sage.reader import SynthesisReader

        _default_actor = os.environ.get("DEFAULT_ACTOR_ID")
        _default_tenant = os.environ.get("DEFAULT_TENANT_ID")
        ask_orchestrator = AskOrchestrator(
            store=PostgresAskStore(runtime_pool),
            conn_provider=runtime_pool.acquire,
            reader=SynthesisReader(pool=runtime_pool),
        )
        app_.include_router(
            build_ask_router(
                ask_orchestrator,
                default_tenant_id=_AskUUID(_default_tenant) if _default_tenant else None,
                default_viewer_id=_AskUUID(_default_actor) if _default_actor else None,
            )
        )
        app_.state.ask_orchestrator = ask_orchestrator
        startup_status.ok("ask_overlay", required=False)
    except Exception as exc:  # noqa: BLE001 - optional feature, never block startup
        startup_status.degraded("ask_overlay", required=False, exc=exc)
        log.exception("ask_overlay_mount_failed")

    runtime = _GatewayRuntime(pool=runtime_pool, deps=deps)
    app_.state.gateway_runtime = runtime
    return runtime


async def _start_extension_startup_hooks(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    startup_status: StartupStatus,
    settings: GatewaySettings,
) -> None:
    """Run startup hooks contributed by installed gateway extensions.

    Core ships none; the demo overlay seeds its config and mounts the
    simulation panel here. Optional outside production; production-enabled
    extensions must start cleanly or the gateway fails closed.
    """
    try:
        await _await_startup(
            "extension_startup_hooks",
            run_extension_startup_hooks(
                app_,
                pool,
                production=settings.is_production,
            ),
            timeout_s=settings.db_startup_timeout_s,
        )
        startup_status.ok("extensions", required=False)
    except Exception as exc:  # noqa: BLE001 - optional startup work
        if settings.is_production:
            startup_status.failed_component("extensions", required=True, exc=exc)
            log.exception("extension_startup_hooks_failed")
            raise
        startup_status.degraded("extensions", required=False, exc=exc)
        log.exception("extension_startup_hooks_warning")


async def _start_integration_runtime(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    startup_status: StartupStatus,
    settings: GatewaySettings,
) -> IntegrationRuntimeWiring:
    try:
        wiring = wire_integration_runtime_state(app_, pool)
    except IntegrationRuntimeWiringError as exc:
        startup_status.failed_component(
            exc.component,
            required=True,
            exc=exc.original,
        )
        startup_status.failed_component(
            "integration_state",
            required=True,
            detail=f"{exc.component} failed",
            exc=exc.original,
        )
        raise

    startup_status.ok(
        "integration_state.pool",
        required=True,
        detail="created" if wiring.pool_alias_created else "reused",
    )
    startup_status.ok(
        "integration_state.secret_store",
        required=True,
        detail="created" if wiring.secret_store_created else "reused",
    )
    startup_status.ok(
        "integration_state.tenant_resolver",
        required=True,
        detail="created" if wiring.tenant_resolver_created else "reused",
    )
    startup_status.ok(
        "integration_state.tenant_flags",
        required=True,
        detail="created" if wiring.tenant_flags_created else "reused",
    )

    try:
        probe_results = await validate_integration_runtime_state(
            app_.state,
            timeout_s=settings.integration_runtime_probe_timeout_s,
        )
    except IntegrationRuntimeValidationError as exc:
        startup_status.failed_component(
            exc.component,
            required=True,
            detail=exc.result.detail,
            exc=exc,
        )
        startup_status.failed_component(
            "integration_state",
            required=True,
            detail=f"{exc.component} failed",
            exc=exc,
        )
        raise
    for result in probe_results:
        startup_status.ok(result.component, required=True)
    startup_status.ok("integration_state", required=True)
    return wiring


async def _start_github_gateway_state(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
    *,
    pool: asyncpg.Pool,
    startup_status: StartupStatus,
    settings: GatewaySettings,
    cleanup_client: object | None,
) -> object | None:
    try:
        github_wiring = wire_github_gateway_state(
            app_,
            pool=pool,
            tenant_resolver=getattr(app_.state, "tenant_resolver", None),
        )
        github_client = getattr(app_.state, "github_client", None)
        if github_wiring.owns_client and github_client is not cleanup_client:
            cleanup_client = github_client
            stack.push_async_callback(
                close_github_gateway_state,
                app_,
                client=github_client,
            )
        startup_status.ok(
            "github_gateway_state",
            required=settings.require_github_integration,
        )
    except Exception as exc:  # noqa: BLE001
        partial_client = getattr(app_.state, "github_client", None)
        if (
            partial_client is not None
            and partial_client is not cleanup_client
            and bool(getattr(app_.state, "gateway_owns_github_client", False))
        ):
            await close_github_gateway_state(app_, client=partial_client)
        if settings.require_github_integration:
            startup_status.failed_component(
                "github_gateway_state",
                required=True,
                exc=exc,
            )
            raise
        startup_status.degraded(
            "github_gateway_state",
            required=False,
            detail="optional startup failed",
            exc=exc,
        )
        log.warning(
            "github_gateway_state_degraded",
            error=str(exc),
            error_type=type(exc).__name__,
        )
    return cleanup_client


def _start_oauth_sweeper(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
    *,
    pool: asyncpg.Pool,
    startup_status: StartupStatus,
    settings: GatewaySettings,
) -> None:
    try:
        app_.state.oauth_sweep_task = start_oauth_state_sweeper(
            pool,
            interval_s=settings.oauth_sweep_interval_s,
        )
        stack.push_async_callback(
            _stop_oauth_sweeper_for_lifespan,
            app_,
            app_.state.oauth_sweep_task,
        )
        startup_status.ok("oauth_sweeper", required=True)
    except Exception as exc:  # noqa: BLE001
        startup_status.failed_component(
            "oauth_sweeper",
            required=True,
            exc=exc,
        )
        raise


async def _start_realtime(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
    *,
    pool: asyncpg.Pool,
    startup_status: StartupStatus,
    settings: GatewaySettings,
) -> None:
    try:
        if getattr(app_.state, "realtime", None) is None:
            from services.app.realtime.main import (
                configure_realtime as _configure_realtime,
            )

            rt_deps = _configure_realtime(
                app_,
                pool=pool,
                start=False,
            )
            try:
                await _await_startup(
                    "realtime",
                    rt_deps.dispatcher.start(),
                    timeout_s=settings.realtime_startup_timeout_s,
                )
            except Exception:
                await _stop_realtime_for_lifespan(app_, rt_deps)
                raise
            stack.push_async_callback(
                _stop_realtime_for_lifespan,
                app_,
                rt_deps,
            )
            startup_status.ok(
                "realtime",
                required=settings.require_realtime,
                detail="started",
            )
        else:
            startup_status.ok(
                "realtime",
                required=settings.require_realtime,
                detail="pre_attached",
            )
    except Exception as exc:  # noqa: BLE001
        if settings.require_realtime:
            startup_status.failed_component(
                "realtime",
                required=True,
                exc=exc,
            )
            raise
        startup_status.degraded(
            "realtime",
            required=False,
            detail="optional startup failed",
            exc=exc,
        )
        log.warning(
            "realtime_startup_degraded",
            error=str(exc),
            error_type=type(exc).__name__,
        )


async def _start_ceo_view(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
    *,
    pool: asyncpg.Pool,
    startup_status: StartupStatus,
    settings: GatewaySettings,
) -> None:
    if not settings.ceo_view_enabled:
        startup_status.disabled(
            "ceo_view",
            detail="GATEWAY_CEO_VIEW_ENABLED=0",
        )
        return

    previous_ceo_view = getattr(app_.state, "ceo_view", None)
    try:
        await _await_startup(
            "ceo_view",
            configure_ceo_view(app_, pool=pool, settings=settings),
            timeout_s=settings.ceo_view_startup_timeout_s,
        )
        ceo_view = getattr(app_.state, "ceo_view", None)
        if ceo_view is not None and ceo_view is not previous_ceo_view:
            stack.push_async_callback(
                _stop_ceo_view_for_lifespan,
                app_,
                ceo_view,
            )
        startup_status.ok("ceo_view", required=False)
    except Exception as ceo_exc:  # noqa: BLE001
        partial_ceo_view = getattr(app_.state, "ceo_view", None)
        if (
            partial_ceo_view is not None
            and partial_ceo_view is not previous_ceo_view
        ):
            await _stop_ceo_view_for_lifespan(app_, partial_ceo_view)
        startup_status.degraded(
            "ceo_view",
            required=False,
            exc=ceo_exc,
        )
        log.error(
            "ceo_view_wiring_failed",
            error=str(ceo_exc),
            error_type=type(ceo_exc).__name__,
        )


async def _start_ingestion_data_plane(
    app_: FastAPI,
    stack: contextlib.AsyncExitStack,
    *,
    startup_status: StartupStatus,
    settings: GatewaySettings,
) -> None:
    try:
        data_plane_wiring = await wire_ingestion_data_plane(
            app_,
            settings=settings,
        )
    except Exception as exc:  # noqa: BLE001
        if settings.require_ingestion_data_plane:
            startup_status.failed_component(
                "ingestion_data_plane",
                required=True,
                exc=exc,
            )
            raise
        data_plane_wiring = False
        startup_status.degraded(
            "ingestion_data_plane",
            required=False,
            exc=exc,
        )
    data_plane_wired = bool(data_plane_wiring)
    data_plane_owned = bool(
        getattr(data_plane_wiring, "owned", data_plane_wired)
    )
    if data_plane_wired:
        if data_plane_owned:
            stack.push_async_callback(
                _close_ingestion_data_plane_for_lifespan,
                app_,
            )
        startup_status.ok(
            "ingestion_data_plane",
            required=settings.require_ingestion_data_plane,
        )
    elif "ingestion_data_plane" not in startup_status.components:
        startup_status.disabled(
            "ingestion_data_plane",
            required=settings.require_ingestion_data_plane,
            detail="not_configured",
        )


def build_app(
    *,
    pool: asyncpg.Pool | None = None,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    rate_limiter: RateLimiter | None = None,
    settings: GatewaySettings | None = None,
    configure_logging: bool = True,
) -> FastAPI:
    """Build the FastAPI app. Every dependency is injectable for tests."""
    settings = settings or GatewaySettings.from_env()
    if configure_logging:
        configure_structlog(settings.log_level)

    if pool is None and (actor_repo is not None or alias_repo is not None):
        raise ValueError(
            "pool is required when actor_repo or alias_repo is injected"
        )

    @contextlib.asynccontextmanager
    async def _lifespan(app_: FastAPI) -> AsyncIterator[None]:
        try:
            startup_status: StartupStatus = app_.state.startup_status
        except AttributeError:
            startup_status = StartupStatus()
            app_.state.startup_status = startup_status
        startup_status.reset()

        async with contextlib.AsyncExitStack() as stack:
            stack.callback(startup_status.mark_stopped)
            github_cleanup_client = _prepare_existing_github_cleanup(
                app_, stack
            )
            runtime = await _start_gateway_deps(
                app_,
                stack,
                startup_status=startup_status,
                settings=settings,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                rate_limiter=rate_limiter,
            )

            connector_wiring = wire_source_connector_runtime(app_.state)
            startup_status.ok(
                "source_connector_runtime",
                required=False,
                detail=(
                    f"connectors={len(connector_wiring.composition.registry)} "
                    f"fingerprint={connector_wiring.composition.registry_fingerprint}"
                ),
            )

            await _start_extension_startup_hooks(
                app_,
                pool=runtime.pool,
                startup_status=startup_status,
                settings=settings,
            )
            await _start_integration_runtime(
                app_,
                pool=runtime.pool,
                startup_status=startup_status,
                settings=settings,
            )
            github_cleanup_client = await _start_github_gateway_state(
                app_,
                stack,
                pool=runtime.pool,
                startup_status=startup_status,
                settings=settings,
                cleanup_client=github_cleanup_client,
            )
            _start_oauth_sweeper(
                app_,
                stack,
                pool=runtime.pool,
                startup_status=startup_status,
                settings=settings,
            )
            await _start_realtime(
                app_,
                stack,
                pool=runtime.pool,
                startup_status=startup_status,
                settings=settings,
            )
            await _start_ceo_view(
                app_,
                stack,
                pool=runtime.pool,
                startup_status=startup_status,
                settings=settings,
            )
            await _start_ingestion_data_plane(
                app_,
                stack,
                startup_status=startup_status,
                settings=settings,
            )

            startup_status.mark_ready()
            try:
                yield
            finally:
                startup_status.mark_stopping()

    app = FastAPI(
        title="Company OS Gateway",
        version="0.1.0",
        lifespan=_lifespan,
    )
    app.state.gateway_settings = settings
    app.state.startup_status = StartupStatus()

    if pool is not None:
        completed_deps = _complete_gateway_deps(
            pool=pool,
            actor_repo=actor_repo,
            alias_repo=alias_repo,
            embedder=embedder,
            rate_limiter=rate_limiter,
            settings=settings,
        )
        app.state.gateway_owns_embedder = completed_deps.owns_embedder
        deps = attach_gateway_deps(
            app,
            pool=pool,
            actor_repo=completed_deps.actor_repo,
            alias_repo=completed_deps.alias_repo,
            embedder=completed_deps.embedder,
            rate_limiter=completed_deps.rate_limiter,
        )
        app.state.gateway_runtime = _GatewayRuntime(pool=pool, deps=deps)
        wire_integration_runtime_state(app, pool)
        try:
            github_wiring = wire_github_gateway_state(
                app,
                pool=pool,
                tenant_resolver=getattr(app.state, "tenant_resolver", None),
            )
            if github_wiring.owns_client:
                app.state.gateway_owns_github_client = True
        except Exception as exc:  # noqa: BLE001
            _clear_app_state(app, "github_client", "github_replay_cache")
            app.state.gateway_owns_github_client = False
            if settings.require_github_integration:
                raise
            app.state.startup_status.degraded(
                "github_gateway_state",
                required=False,
                detail="optional pre-lifespan wiring failed",
                exc=exc,
            )
            log.warning(
                "github_gateway_state_prewire_degraded",
                error=str(exc),
                error_type=type(exc).__name__,
            )

    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(BearerAuthMiddleware)
    app.add_middleware(RequestContextMiddleware)

    install_safe_error_handlers(app)
    app.add_exception_handler(IngestSizeError, ingest_size_error_handler)
    mount_gateway_routes(app, settings=settings)
    return app


app = build_app()


__all__ = [
    "app",
    "build_app",
    "GatewayDeps",
    "GatewaySettings",
    "_PUBLIC_PATHS",
    "_PUBLIC_PATH_PREFIXES",
]
