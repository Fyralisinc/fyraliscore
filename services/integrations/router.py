"""services/integrations/router.py — FastAPI router for /integrations/*.

Mounted by `services/gateway/main.py::build_app`. Owns the OAuth
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

from fastapi import APIRouter, Request

from services.integrations.slack import oauth as slack_oauth


def build_integrations_router() -> APIRouter:
    """Construct the integrations router with all provider sub-routes
    wired. Stateless — all deps are read off `request.app.state`."""
    router = APIRouter(prefix="/integrations", tags=["integrations"])

    @router.get("/slack/install")
    async def slack_install(request: Request):
        return await slack_oauth.install_handler(request)

    @router.get("/slack/callback")
    async def slack_callback(request: Request):
        return await slack_oauth.callback_handler(request)

    return router


__all__ = ["build_integrations_router"]
