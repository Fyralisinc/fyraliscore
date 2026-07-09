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
from fastapi.responses import HTMLResponse
from starlette.responses import Response

from lib.shared.product_workflow_metrics import (
    ProductWorkflowEvent,
    ProductWorkflowOutcome,
    record_product_workflow_event,
)

from services.ingest.integrations.discord import oauth as discord_oauth
from services.ingest.integrations.github import oauth as github_oauth
from services.ingest.integrations.notion import oauth as notion_oauth
from services.ingest.integrations.slack import oauth as slack_oauth


def build_integrations_router() -> APIRouter:
    """Construct the integrations router with all provider sub-routes
    wired. Stateless — all deps are read off `request.app.state`."""
    router = APIRouter(prefix="/integrations", tags=["integrations"])

    @router.get("/slack/install")
    async def slack_install(request: Request):
        return await slack_oauth.install_handler(request)

    @router.get("/slack/callback")
    async def slack_callback(request: Request):
        return await _callback_with_metrics(slack_oauth.callback_handler, request)

    @router.get("/slack/installed", response_class=HTMLResponse)
    async def slack_installed():
        return _oauth_landing_page(
            provider_name="Slack",
            status="connected",
            detail="Slack has authorized Fyralis for this customer-cloud deployment.",
        )

    @router.get("/slack/install-error", response_class=HTMLResponse)
    async def slack_install_error(request: Request):
        reason = request.query_params.get("reason", "unknown")
        return _oauth_landing_page(
            provider_name="Slack",
            status="error",
            detail=(
                "Slack authorization did not complete. Return to the onboarding "
                f"UI and retry. Reason: {reason}."
            ),
            status_code=400,
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
        return await notion_oauth.install_handler(request)

    @router.get("/notion/callback")
    async def notion_callback(request: Request):
        return await _callback_with_metrics(notion_oauth.callback_handler, request)

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


def _oauth_landing_page(
    *,
    provider_name: str,
    status: str,
    detail: str,
    status_code: int = 200,
) -> HTMLResponse:
    is_success = status == "connected"
    title = f"{provider_name} {'connected' if is_success else 'connection issue'}"
    accent = "#1f9d55" if is_success else "#c2410c"
    safe_title = _html_escape(title)
    safe_detail = _html_escape(detail)
    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{safe_title}</title>
  <style>
    body {{
      margin: 0;
      min-height: 100vh;
      display: grid;
      place-items: center;
      background: #0b1020;
      color: #f8fafc;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }}
    main {{
      width: min(92vw, 520px);
      border: 1px solid #26334d;
      border-radius: 8px;
      background: #111827;
      padding: 32px;
      box-shadow: 0 20px 60px rgb(0 0 0 / 35%);
    }}
    .status {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: {accent};
      font-size: 13px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0;
    }}
    .dot {{
      width: 10px;
      height: 10px;
      border-radius: 50%;
      background: {accent};
    }}
    h1 {{
      margin: 14px 0 10px;
      font-size: 28px;
      line-height: 1.15;
      letter-spacing: 0;
    }}
    p {{
      margin: 0;
      color: #cbd5e1;
      font-size: 15px;
      line-height: 1.6;
    }}
  </style>
</head>
<body>
  <main>
    <div class="status"><span class="dot"></span>{_html_escape(status)}</div>
    <h1>{safe_title}</h1>
    <p>{safe_detail} You can return to the Fyralis onboarding UI.</p>
  </main>
</body>
</html>"""
    return HTMLResponse(content=html, status_code=status_code)


def _html_escape(value: str) -> str:
    return (
        value.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#x27;")
    )


__all__ = ["build_integrations_router"]
