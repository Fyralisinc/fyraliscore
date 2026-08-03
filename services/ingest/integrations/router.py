"""services/ingest/integrations/router.py — FastAPI router for /integrations/*.

Mounted by `services/app/gateway/main.py::build_app`. Owns the OAuth
install + callback surface for each provider Fyralis integrates with;
Slack is the first (IN-08), with GitHub / Linear / Stripe to follow
under IN-09+ on the same pattern.

The router is intentionally provider-prefix-segmented:
    /integrations/slack/install
    /integrations/slack/callback
    /integrations/github/install   (future)
    ...
so the gateway's public-path allowlist can target individual routes
rather than blanket-publish `/integrations/*` (ClickUp body's
"single-route, not blanket public" wording).
"""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Request
from starlette.responses import Response

from lib.observability.product_workflow_events import (
    ProductWorkflowEvent,
    ProductWorkflowOutcome,
    record_product_workflow_event,
)

from services.ingest.integrations.discord import oauth as discord_oauth
from services.ingest.integrations.github import oauth as github_oauth
from services.ingest.integrations.notion import oauth as notion_oauth
from services.ingest.integrations.slack import oauth as slack_oauth
from services.ingest.connector_platform.oauth_ingress import (
    execute_oauth_callback,
    execute_oauth_install,
)


def build_integrations_router() -> APIRouter:
    """Construct the integrations router with all provider sub-routes
    wired. Stateless — all deps are read off `request.app.state`."""
    router = APIRouter(prefix="/integrations", tags=["integrations"])

    @router.get("/slack/install")
    async def slack_install(request: Request):
        return await execute_oauth_install(
            request,
            provider="slack",
            legacy_handler=lambda: slack_oauth.install_handler(request),
        )

    @router.get("/slack/callback")
    async def slack_callback(request: Request):
        return await _callback_with_metrics(
            lambda _request: execute_oauth_callback(
                request,
                provider="slack",
                legacy_handler=lambda: slack_oauth.callback_handler(request),
            ),
            request,
        )

    @router.get("/discord/install")
    async def discord_install(request: Request):
        return await discord_oauth.install_handler(request)

    @router.get("/discord/callback")
    async def discord_callback(request: Request):
        return await _callback_with_metrics(discord_oauth.callback_handler, request)

    @router.get("/github/install")
    async def github_install(request: Request):
        return await github_oauth.install_handler(request)

    @router.get("/github/callback")
    async def github_callback(request: Request):
        return await _callback_with_metrics(github_oauth.callback_handler, request)

    @router.get("/notion/install")
    async def notion_install(request: Request):
        return await execute_oauth_install(
            request,
            provider="notion",
            legacy_handler=lambda: notion_oauth.install_handler(request),
        )

    @router.get("/notion/callback")
    async def notion_callback(request: Request):
        return await _callback_with_metrics(
            lambda _request: execute_oauth_callback(
                request,
                provider="notion",
                legacy_handler=lambda: notion_oauth.callback_handler(request),
            ),
            request,
        )

    return router


async def _callback_with_metrics(
    handler: Callable[[Request], Awaitable[Any]],
    request: Request,
) -> Any:
    try:
        response = await handler(request)
    except Exception:
        _record_source_event("source_install_failed", "error")
        raise
    event, outcome = _callback_metric_result(response)
    _record_source_event(event, outcome)
    return response


def _callback_metric_result(
    response: Any,
) -> tuple[ProductWorkflowEvent, ProductWorkflowOutcome]:
    status_code = int(getattr(response, "status_code", 500) or 500)
    location = ""
    if isinstance(response, Response):
        location = response.headers.get("location", "")
    if "/installed" in location:
        return "source_install_completed", "success"
    if "/install-error" in location:
        return "source_install_failed", "bad_request"
    if 200 <= status_code < 400:
        return "source_install_completed", "success"
    if status_code == 403:
        return "source_install_failed", "forbidden"
    if status_code == 404:
        return "source_install_failed", "not_found"
    if status_code == 409:
        return "source_install_failed", "conflict"
    if status_code < 500:
        return "source_install_failed", "bad_request"
    return "source_install_failed", "error"


def _record_source_event(
    event: ProductWorkflowEvent,
    outcome: ProductWorkflowOutcome,
) -> None:
    record_product_workflow_event(
        workflow="source_onboarding",
        event=event,
        outcome=outcome,
    )


__all__ = ["build_integrations_router"]
