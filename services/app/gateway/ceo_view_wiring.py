"""CEO-view and adjacent product router wiring for the gateway."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger
from services.app.gateway.settings import GatewaySettings


log = get_logger("gateway")


@dataclass(frozen=True)
class _CeoGreetingRuntime:
    scheduler: Any
    cache_repo: Any
    viewer_state_repo: Any
    stream_manager: Any
    rendering_adapter: Any
    default_tenant_uuid: UUID | None
    token: str


def _resolve_gateway_settings(
    app_: FastAPI,
    settings: GatewaySettings | None,
) -> GatewaySettings:
    resolved = settings or getattr(app_.state, "gateway_settings", None)
    if resolved is None:
        return GatewaySettings.from_env()
    return resolved


def _include_rendering_router(app_: FastAPI, *, pool: asyncpg.Pool) -> None:
    from services.product.rendering.api import (
        get_service as _rnd_get_service,
        router as rnd_router,
    )
    from services.product.rendering.core import RenderingService

    rnd_service = RenderingService.from_env(pool=pool)
    app_.include_router(rnd_router)
    app_.dependency_overrides[_rnd_get_service] = lambda: rnd_service


async def _build_ceo_greeting_runtime(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    settings: GatewaySettings,
) -> _CeoGreetingRuntime:
    from services.product.greeting.api import build_ceo_api_router
    from services.product.greeting.cache import ViewCeoCacheRepo
    from services.product.greeting.rendering_adapter import build_rendering_adapter
    from services.product.greeting.scheduler import GreetingScheduler, SchedulerConfig
    from services.product.greeting.stream import (
        StaticTenantTokenMap,
        ViewCeoStreamManager,
        build_ceo_stream_router,
    )
    from services.product.greeting.viewer_state_repo import ViewerStateRepo

    cache_repo = ViewCeoCacheRepo(pool)
    viewer_state_repo = ViewerStateRepo(pool)
    rendering_adapter = build_rendering_adapter()
    scheduler = GreetingScheduler(
        pool=pool,
        cache=cache_repo,
        rendering=rendering_adapter,
        config=SchedulerConfig(),
    )

    default_tenant_uuid = _register_default_ceo_tenant(
        scheduler=scheduler,
        settings=settings,
    )
    token_map = StaticTenantTokenMap.from_env(
        enabled=settings.view_ceo_static_tokens_enabled,
    )
    if default_tenant_uuid and settings.view_ceo_token not in token_map.tokens:
        token_map.tokens[settings.view_ceo_token] = default_tenant_uuid

    stream_manager = ViewCeoStreamManager(token_map=token_map)
    scheduler.set_stream_publisher(
        type("_SP", (), {"publish": staticmethod(stream_manager.publish)})()
    )

    if settings.start_grt_scheduler:
        await scheduler.start()

    app_.include_router(
        build_ceo_api_router(
            cache=cache_repo,
            scheduler=scheduler,
            stream_manager=stream_manager,
            viewer_state_repo=viewer_state_repo,
            default_tenant_id=default_tenant_uuid,
        )
    )
    app_.include_router(build_ceo_stream_router(stream_manager))
    return _CeoGreetingRuntime(
        scheduler=scheduler,
        cache_repo=cache_repo,
        viewer_state_repo=viewer_state_repo,
        stream_manager=stream_manager,
        rendering_adapter=rendering_adapter,
        default_tenant_uuid=default_tenant_uuid,
        token=settings.view_ceo_token,
    )


def _register_default_ceo_tenant(
    *,
    scheduler: Any,
    settings: GatewaySettings,
) -> UUID | None:
    from services.product.greeting.snapshot import FounderContext

    if not settings.default_tenant_id:
        return None

    tenant_id = UUID(settings.default_tenant_id)
    founder = FounderContext(
        tenant_id=tenant_id,
        role="ceo",
        display_name=settings.view_ceo_display_name,
        timezone_name=settings.view_ceo_timezone,
        observed_rhythms={},
    )
    scheduler.register_tenant(tenant_id, founder)
    return tenant_id


def _include_query_router(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    default_tenant_uuid: UUID | None,
) -> Any:
    from services.app.gateway.db_bootstrap import _register_codecs as _codec_hook  # noqa: F401
    from services.product.query.adapters import (
        build_cache_adapter as _build_qry_cache,
        build_rendering_adapter as _build_qry_rnd,
    )
    from services.product.query.api import build_router as build_query_router
    from services.product.query.core import QueryHandler

    deps = getattr(app_.state, "deps", None)
    qry_embedder = deps.embedder if deps is not None else None
    qry_handler = QueryHandler(
        conn_provider=pool.acquire,
        rendering_adapter=_build_qry_rnd(),
        cache_adapter=_build_qry_cache(pool=pool),
        embedder=qry_embedder,
    )
    app_.include_router(
        build_query_router(qry_handler, default_tenant_id=default_tenant_uuid),
    )
    return qry_handler


def _include_conversation_router(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    qry_handler: Any,
) -> None:
    from services.product.conversations import (
        ConversationRepo,
        ProbeHandler,
        build_router as build_conversations_router,
    )

    conv_repo = ConversationRepo(pool)
    probe_handler = ProbeHandler(
        repo=conv_repo,
        pool=pool,
        query_handler=qry_handler,
    )
    app_.include_router(
        build_conversations_router(repo=conv_repo, handler=probe_handler)
    )
    app_.state.conversations = {"repo": conv_repo, "handler": probe_handler}


def _include_debug_router(app_: FastAPI, *, settings: GatewaySettings) -> None:
    if not settings.debug_endpoints_enabled:
        return

    try:
        from services.app.gateway.debug_router import build_debug_router

        app_.include_router(build_debug_router())
    except Exception as exc:  # noqa: BLE001
        log.warning("debug_router_mount_failed", error=str(exc))


def _publish_ceo_view_state(
    app_: FastAPI,
    *,
    greeting: _CeoGreetingRuntime,
    qry_handler: Any,
) -> None:
    app_.state.ceo_view = {
        "scheduler": greeting.scheduler,
        "cache": greeting.cache_repo,
        "viewer_state_repo": greeting.viewer_state_repo,
        "stream_manager": greeting.stream_manager,
        "rendering_adapter": greeting.rendering_adapter,
        "qry_handler": qry_handler,
        "tenant_id": greeting.default_tenant_uuid,
        "token": greeting.token,
    }


async def configure_ceo_view(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    settings: GatewaySettings | None = None,
) -> None:
    """Wire rendering, greeting, query, ingress, and debug routers."""
    resolved_settings = _resolve_gateway_settings(app_, settings)
    _include_rendering_router(app_, pool=pool)
    greeting = await _build_ceo_greeting_runtime(
        app_,
        pool=pool,
        settings=resolved_settings,
    )
    qry_handler = _include_query_router(
        app_,
        pool=pool,
        default_tenant_uuid=greeting.default_tenant_uuid,
    )
    _include_conversation_router(app_, pool=pool, qry_handler=qry_handler)

    # Simulation authoring endpoints moved to the demo overlay, which contributes
    # the /simulation panel (router + slack_ui static) via its gateway extension
    # startup hook. Core no longer imports the `simulation` package.
    _include_debug_router(app_, settings=resolved_settings)
    _publish_ceo_view_state(app_, greeting=greeting, qry_handler=qry_handler)
