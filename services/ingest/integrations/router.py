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

from lib.shared.product_workflow_metrics import (
    ProductWorkflowEvent,
    ProductWorkflowOutcome,
    record_product_workflow_event,
)

from services.ingest.source_contract import (
    OAUTH_INGRESS_CATALOG,
    resolve_callable_reference,
)


def build_integrations_router() -> APIRouter:
    """Mount every contract-declared shared OAuth install/callback route."""

    router = APIRouter(prefix="/integrations", tags=["integrations"])
    for ingress in OAUTH_INGRESS_CATALOG.values():
        if ingress.mount_mode != "shared_router":
            continue
        install_handler = resolve_callable_reference(
            ingress.install_handler_binding
        )
        callback_handler = resolve_callable_reference(
            ingress.callback_handler_binding
        )
        router.add_api_route(
            _relative_integration_path(ingress.install_path),
            _install_endpoint(install_handler),
            methods=["GET"],
            name=f"{ingress.source_id}_oauth_install",
        )
        router.add_api_route(
            _relative_integration_path(ingress.callback_path),
            _callback_endpoint(callback_handler),
            methods=["GET"],
            name=f"{ingress.source_id}_oauth_callback",
        )

    return router


def _relative_integration_path(route_path: str) -> str:
    prefix = "/integrations"
    if not route_path.startswith(f"{prefix}/"):
        raise RuntimeError(
            f"shared OAuth route must live below {prefix}: {route_path!r}"
        )
    return route_path.removeprefix(prefix)


def _install_endpoint(
    handler: Callable[[Request], Awaitable[Any]],
) -> Callable[[Request], Awaitable[Any]]:
    async def endpoint(request: Request) -> Any:
        return await handler(request)

    return endpoint


def _callback_endpoint(
    handler: Callable[[Request], Awaitable[Any]],
) -> Callable[[Request], Awaitable[Any]]:
    async def endpoint(request: Request) -> Any:
        return await _callback_with_metrics(handler, request)

    return endpoint


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
