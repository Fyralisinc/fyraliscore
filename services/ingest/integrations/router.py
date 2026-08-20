"""Supplemental provider OAuth install and callback routes."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import APIRouter, Request
from starlette.responses import Response

from lib.observability.product_workflow_events import (
    ProductWorkflowEvent,
    ProductWorkflowOutcome,
    record_product_workflow_event,
)
from services.ingest.integrations.facebook_pages import oauth as facebook_pages_oauth
from services.ingest.integrations.instagram import oauth as instagram_oauth


def build_integrations_router() -> APIRouter:
    router = APIRouter(prefix="/integrations", tags=["integrations"])

    @router.get("/facebook_pages/install")
    async def facebook_pages_install(request: Request):
        return await facebook_pages_oauth.install_handler(request)

    @router.get("/facebook_pages/callback")
    async def facebook_pages_callback(request: Request):
        return await _callback_with_metrics(
            facebook_pages_oauth.callback_handler, request
        )

    @router.get("/instagram/install")
    async def instagram_install(request: Request):
        return await instagram_oauth.install_handler(request)

    @router.get("/instagram/callback")
    async def instagram_callback(request: Request):
        return await _callback_with_metrics(instagram_oauth.callback_handler, request)

    return router


async def _callback_with_metrics(
    handler: Callable[[Request], Awaitable[Any]],
    request: Request,
) -> Any:
    try:
        response = await handler(request)
    except Exception:
        _record("source_install_failed", "error")
        raise
    event, outcome = _callback_metric_result(response)
    _record(event, outcome)
    return response


def _callback_metric_result(
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


__all__ = ["build_integrations_router"]
