"""CEO-view and adjacent product router wiring for the gateway."""
from __future__ import annotations

import os

import asyncpg
from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger
from services.app.gateway.settings import GatewaySettings


log = get_logger("gateway")


async def configure_ceo_view(
    app_: FastAPI,
    *,
    pool: asyncpg.Pool,
    settings: GatewaySettings | None = None,
) -> None:
    """Wire rendering, greeting, query, ingress, and debug routers."""
    from uuid import UUID as _UUID

    settings = settings or getattr(app_.state, "gateway_settings", None)
    if settings is None:
        settings = GatewaySettings.from_env()

    # Rendering router.
    from services.product.rendering.api import (
        get_service as _rnd_get_service,
        router as rnd_router,
    )
    from services.product.rendering.core import RenderingService

    rnd_service = RenderingService.from_env(pool=pool)
    app_.include_router(rnd_router)
    app_.dependency_overrides[_rnd_get_service] = lambda: rnd_service

    # CEO greeting scheduler, HTTP API, and stream API.
    from services.product.greeting.api import build_ceo_api_router
    from services.product.greeting.cache import ViewCeoCacheRepo
    from services.product.greeting.rendering_adapter import build_rendering_adapter
    from services.product.greeting.scheduler import GreetingScheduler, SchedulerConfig
    from services.product.greeting.snapshot import FounderContext
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

    default_tenant = settings.default_tenant_id
    ceo_token = settings.view_ceo_token
    token_map = StaticTenantTokenMap.from_env()
    if default_tenant:
        tid = _UUID(default_tenant)
        founder = FounderContext(
            tenant_id=tid,
            role="ceo",
            display_name=settings.view_ceo_display_name,
            timezone_name=settings.view_ceo_timezone,
            observed_rhythms={},
        )
        scheduler.register_tenant(tid, founder)
        if ceo_token not in token_map.tokens:
            token_map.tokens[ceo_token] = tid
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
            default_tenant_id=_UUID(default_tenant) if default_tenant else None,
        )
    )
    app_.include_router(build_ceo_stream_router(stream_manager))

    # Query router.
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
    default_tenant_uuid = _UUID(default_tenant) if default_tenant else None
    app_.include_router(
        build_query_router(qry_handler, default_tenant_id=default_tenant_uuid),
    )

    # Card conversations.
    from services.product.conversations import (
        ConversationRepo,
        ProbeHandler,
        build_router as build_conversations_router,
    )

    conv_repo = ConversationRepo(pool)
    probe_handler = ProbeHandler(
        repo=conv_repo, pool=pool, query_handler=qry_handler,
    )
    app_.include_router(
        build_conversations_router(repo=conv_repo, handler=probe_handler)
    )
    app_.state.conversations = {"repo": conv_repo, "handler": probe_handler}

    # Simulation authoring endpoints moved to the demo overlay, which contributes
    # the /simulation panel (router + slack_ui static) via its gateway extension
    # startup hook. Core no longer imports the `simulation` package.

    # Gmail Pub/Sub push ingress.
    try:
        from services.app.webhooks.gmail_pubsub import (
            is_pubsub_configured,
            router as _gmail_pubsub_router,
        )

        app_.include_router(_gmail_pubsub_router)
        if is_pubsub_configured():
            log.info("gmail_pubsub_ingress_mounted", configured=True)
        else:
            log.warning("gmail_pubsub_ingress_mounted_unconfigured", configured=False)
    except Exception as exc:  # noqa: BLE001
        log.warning("gmail_pubsub_mount_failed", error=str(exc))

    # Google Calendar/Drive push ingress.
    try:
        from services.app.webhooks.google_push import router as _google_push_router

        app_.include_router(_google_push_router)
        log.info("google_push_ingress_mounted")
    except Exception as exc:  # noqa: BLE001
        log.warning("google_push_mount_failed", error=str(exc))

    # Gmail, Google Calendar, and Google Drive admin connect wizards.
    if (
        os.environ.get("GMAIL_SERVICE_ACCOUNT_JSON_FILE")
        or os.environ.get("GMAIL_SERVICE_ACCOUNT_JSON")
    ):
        try:
            from services.ingest.integrations.gmail.oauth import router as _gmail_oauth_router

            app_.include_router(_gmail_oauth_router)
            log.info("gmail_routers_mounted")
        except Exception as exc:  # noqa: BLE001
            log.warning("gmail_mount_failed", error=str(exc))

        try:
            from services.ingest.integrations.google_calendar.oauth import (
                router as _gcal_oauth_router,
            )

            app_.include_router(_gcal_oauth_router)
            log.info("google_calendar_router_mounted")
        except Exception as exc:  # noqa: BLE001
            log.warning("google_calendar_mount_failed", error=str(exc))

        try:
            from services.ingest.integrations.google_drive.oauth import (
                router as _gdrive_oauth_router,
            )

            app_.include_router(_gdrive_oauth_router)
            log.info("google_drive_router_mounted")
        except Exception as exc:  # noqa: BLE001
            log.warning("google_drive_mount_failed", error=str(exc))

    # Debug inspector.
    if env_name in ("dev", "staging", "test"):
        try:
            from services.app.gateway.debug_router import build_debug_router

            app_.include_router(build_debug_router())
        except Exception as exc:  # noqa: BLE001
            log.warning("debug_router_mount_failed", error=str(exc))

    app_.state.ceo_view = {
        "scheduler": scheduler,
        "cache": cache_repo,
        "viewer_state_repo": viewer_state_repo,
        "stream_manager": stream_manager,
        "rendering_adapter": rendering_adapter,
        "qry_handler": qry_handler,
        "tenant_id": _UUID(default_tenant) if default_tenant else None,
        "token": ceo_token,
    }
