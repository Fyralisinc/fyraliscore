"""Gateway route mounting orchestration."""
from __future__ import annotations

import os

from fastapi import FastAPI

from services.app.gateway.logging_config import get_logger


log = get_logger("gateway")


def register_gateway_routes(app: FastAPI) -> None:
    """Mount route families that used to live inline in ``main.py``."""
    from services.app.gateway.contest_router import build_contest_router
    from services.app.gateway.core_router import build_core_router
    from services.app.gateway.dashboard_router import build_dashboard_router
    from services.app.gateway.map_routes import register_map_routes
    from services.app.gateway.recommendations_router import (
        build_recommendations_router,
    )
    from services.app.gateway.sage_internal_router import build_sage_internal_router
    from services.app.gateway.structure_router import build_structure_router
    from services.app.gateway.substrate_router import build_substrate_router
    from services.app.gateway.today_core_router import build_today_core_router

    app.include_router(build_core_router())
    app.include_router(build_substrate_router())
    app.include_router(build_contest_router())
    app.include_router(build_dashboard_router())
    app.include_router(build_sage_internal_router())
    app.include_router(build_recommendations_router())
    app.include_router(build_structure_router())
    app.include_router(build_today_core_router())
    register_map_routes(app)


def mount_gateway_routes(app: FastAPI) -> None:
    """Mount all route families whose construction does not await lifespan."""
    register_gateway_routes(app)

    from services.product.demo.router import demo_router as demo_router

    app.include_router(demo_router)

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

    app.include_router(build_decision_deltas_router())
    app.include_router(build_forecasts_router())
    app.include_router(model_trace_router)
    app.include_router(history_router)
    app.include_router(build_webhooks_router())

    register_spec_routes(app)
    register_model_page_routes(app)
    register_today_routes(app)

    from services.ingest.integrations.router import build_integrations_router

    app.include_router(build_integrations_router())

    try:
        from services.ingest.integrations.jira.oauth import router as jira_router

        app.include_router(jira_router)
        log.info("jira_router_mounted")
    except Exception as exc:  # noqa: BLE001 - never block startup
        log.error("jira_router_mount_failed", error=str(exc))

    try:
        from services.ingest.integrations.mercury.oauth import router as mercury_router
        from services.ingest.integrations.quickbooks.oauth import router as qbo_router

        app.include_router(mercury_router)
        app.include_router(qbo_router)
        log.info("finance_install_routers_mounted")
    except Exception as exc:  # noqa: BLE001 - never block startup
        log.error("finance_install_routers_mount_failed", error=str(exc))

    if os.environ.get("FINANCE_PANEL_ENABLED", "1") != "0":
        try:
            from services.app.gateway.finance_router import build_finance_router

            app.include_router(build_finance_router())
        except Exception as exc:  # noqa: BLE001 - never block startup
            log.error("finance_router_mount_failed", error=str(exc))

    if os.environ.get("SLACK_DM_PANEL_ENABLED", "1") != "0":
        try:
            from services.app.gateway.slack_router import build_slack_router

            app.include_router(build_slack_router())
        except Exception as exc:  # noqa: BLE001 - never block startup
            log.error("slack_router_mount_failed", error=str(exc))

    from services.ingest.github_intel.api import build_github_intel_router

    app.include_router(build_github_intel_router())
