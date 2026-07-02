"""Gateway route mounting orchestration."""
from __future__ import annotations

from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger
from services.app.gateway.settings import GatewaySettings


log = get_logger("gateway")


def register_gateway_routes(
    app: FastAPI,
    *,
    settings: GatewaySettings | None = None,
) -> None:
    """Mount route families that used to live inline in ``main.py``."""
    settings = settings or getattr(app.state, "gateway_settings", None)
    if settings is None:
        settings = GatewaySettings.from_env()

    from services.app.gateway.clarifications_router import build_clarifications_router
    from services.app.gateway.contest_router import build_contest_router
    from services.app.gateway.core_router import build_core_router
    from services.app.gateway.dashboard_router import build_dashboard_router
    from services.app.gateway.byoc_agent_router import build_byoc_agent_router
    from services.app.gateway.byoc_control_plane_router import (
        build_byoc_control_plane_router,
    )
    from services.app.gateway.byoc_control_panel_router import (
        build_byoc_control_panel_router,
    )
    from services.app.gateway.byoc_onboarding_router import (
        build_byoc_onboarding_router,
    )
    from services.app.gateway.dead_letter_router import build_dead_letter_admin_router
    from services.app.gateway.extension_router import build_extension_router
    from services.app.gateway.map_routes import register_map_routes
    from services.app.gateway.recommendations_router import (
        build_recommendations_router,
    )
    from services.app.gateway.sage_internal_router import build_sage_internal_router
    from services.app.gateway.structure_router import build_structure_router
    from services.app.gateway.substrate_router import build_substrate_router
    from services.app.gateway.today_core_router import build_today_core_router
    from services.app.gateway.whatsapp_router import build_whatsapp_router

    app.include_router(build_core_router())
    app.include_router(build_clarifications_router())
    app.include_router(build_substrate_router())
    app.include_router(build_contest_router())
    app.include_router(build_dashboard_router())
    app.include_router(build_byoc_agent_router())
    app.include_router(build_byoc_control_plane_router())
    app.include_router(build_byoc_control_panel_router())
    app.include_router(build_byoc_onboarding_router())
    app.include_router(build_dead_letter_admin_router())
    if settings.debug_endpoints_enabled:
        from services.app.gateway.document_ingest_router import build_document_ingest_router

        app.include_router(build_document_ingest_router())
    app.include_router(
        build_whatsapp_router(
            debug_endpoints_enabled=settings.debug_endpoints_enabled,
        )
    )
    app.include_router(build_sage_internal_router())
    app.include_router(build_recommendations_router())
    app.include_router(build_structure_router())
    app.include_router(build_today_core_router())
    app.include_router(build_extension_router())
    register_map_routes(app)


def mount_gateway_routes(
    app: FastAPI,
    *,
    settings: GatewaySettings | None = None,
    emit_mount_logs: bool = True,
) -> None:
    """Mount all route families whose construction does not await lifespan."""
    settings = settings or getattr(app.state, "gateway_settings", None)
    if settings is None:
        settings = GatewaySettings.from_env()

    register_gateway_routes(app, settings=settings)

    # Overlay packages (e.g. the demo) contribute their routers here via the
    # gateway extension seam — core imports nothing overlay-specific.
    from services.app.gateway.extensions import mount_extension_routers

    mount_extension_routers(app, production=settings.is_production)

    from services.app.gateway.model_page_routes import register_model_page_routes
    from services.app.gateway.spec_routes import register_spec_routes
    from services.app.gateway.today_routes import register_today_routes
    from services.app.webhooks.router import build_webhooks_router
    from services.product.decision_deltas.router import (
        build_router as build_decision_deltas_router,
    )
    from services.product.forecasts import build_router as build_forecasts_router
    from services.product.history.router import router as history_router
    from services.product.model_trace.router import router as model_trace_router
    from services.product.resolution_threads.router import (
        build_router as build_resolution_threads_router,
    )

    app.include_router(build_decision_deltas_router())
    app.include_router(build_forecasts_router())
    app.include_router(model_trace_router)
    app.include_router(history_router)
    app.include_router(build_webhooks_router())
    # Resolution Threads (ported retrieval/memory feature). The router reads
    # the pool from app.state.deps per-request, so it mounts lifespan-free.
    app.include_router(build_resolution_threads_router())

    if settings.spec_demo_routes_enabled:
        register_spec_routes(app)
    register_model_page_routes(app)
    register_today_routes(app)

    from services.ingest.integrations.router import build_integrations_router

    app.include_router(build_integrations_router())

    try:
        from services.ingest.integrations.jira.oauth import router as jira_router

        app.include_router(jira_router)
        if emit_mount_logs:
            log.info("jira_router_mounted")
    except Exception as exc:  # noqa: BLE001 - never block startup
        if emit_mount_logs:
            log.error("jira_router_mount_failed", error=str(exc))

    try:
        from services.ingest.integrations.mercury.oauth import router as mercury_router
        from services.ingest.integrations.quickbooks.oauth import router as qbo_router

        app.include_router(mercury_router)
        app.include_router(qbo_router)
        if emit_mount_logs:
            log.info("finance_install_routers_mounted")
    except Exception as exc:  # noqa: BLE001 - never block startup
        if emit_mount_logs:
            log.error("finance_install_routers_mount_failed", error=str(exc))

    if settings.finance_panel_enabled:
        try:
            from services.app.gateway.finance_router import build_finance_router

            app.include_router(build_finance_router())
        except Exception as exc:  # noqa: BLE001 - never block startup
            log.error("finance_router_mount_failed", error=str(exc))

    if settings.slack_dm_panel_enabled:
        try:
            from services.app.gateway.slack_router import build_slack_router

            app.include_router(build_slack_router())
        except Exception as exc:  # noqa: BLE001 - never block startup
            log.error("slack_router_mount_failed", error=str(exc))
