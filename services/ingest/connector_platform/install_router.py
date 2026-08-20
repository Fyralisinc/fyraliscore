"""Contract-only installation ingress for every source connector."""

from __future__ import annotations

from typing import Any, Awaitable, Callable

from fastapi import APIRouter, Request
from starlette.responses import Response

from lib.observability.product_workflow_events import (
    ProductWorkflowEvent,
    ProductWorkflowOutcome,
    record_product_workflow_event,
)
from services.ingest.connector_platform.oauth_ingress import (
    execute_configuration_install,
    execute_oauth_callback,
    execute_oauth_install,
)


def build_install_router() -> APIRouter:
    router = APIRouter(prefix="/integrations", tags=["integrations"])

    @router.get("/{source}/install")
    async def oauth_install(source: str, request: Request):
        return await execute_oauth_install(request, provider=source)

    @router.get("/{source}/callback")
    async def oauth_callback(source: str, request: Request):
        return await _with_metrics(
            lambda _request: execute_oauth_callback(request, provider=source),
            request,
        )

    @router.post("/{source}/configure")
    async def configure(source: str, request: Request):
        return await _with_metrics(
            lambda _request: execute_configuration_install(request, provider=source),
            request,
        )

    return router


async def _with_metrics(
    handler: Callable[[Request], Awaitable[Any]],
    request: Request,
) -> Any:
    try:
        response = await handler(request)
    except Exception:
        _record("source_install_failed", "error")
        raise
    event, outcome = _metric_result(response)
    _record(event, outcome)
    return response


def _metric_result(
    response: Any,
) -> tuple[ProductWorkflowEvent, ProductWorkflowOutcome]:
    status_code = int(getattr(response, "status_code", 500) or 500)
    location = (
        response.headers.get("location", "") if isinstance(response, Response) else ""
    )
    if "/installed" in location or 200 <= status_code < 400:
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


def _record(
    event: ProductWorkflowEvent,
    outcome: ProductWorkflowOutcome,
) -> None:
    record_product_workflow_event(
        workflow="source_onboarding",
        event=event,
        outcome=outcome,
    )


__all__ = ["build_install_router"]
