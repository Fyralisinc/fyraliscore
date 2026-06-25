from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import RedirectResponse
from starlette.testclient import TestClient

from lib.observability import counter, reset_default_for_tests
from services.app.gateway.product_workflow_metrics import (
    PRODUCT_WORKFLOW_EVENT_OUTCOMES,
    PRODUCT_WORKFLOW_EVENTS,
    PRODUCT_WORKFLOWS,
)
from services.ingest.integrations.router import build_integrations_router


def _events():
    return counter(
        "product_workflow_events_total",
        "lookup",
        ("workflow", "event", "outcome"),
        allowed_label_values={
            "workflow": PRODUCT_WORKFLOWS,
            "event": PRODUCT_WORKFLOW_EVENTS,
            "outcome": PRODUCT_WORKFLOW_EVENT_OUTCOMES,
        },
    )


@pytest.fixture(autouse=True)
def _clean_metrics():
    reset_default_for_tests()
    yield
    reset_default_for_tests()


def _make_app() -> FastAPI:
    app = FastAPI()
    app.include_router(build_integrations_router())
    return app


def test_oauth_callback_records_source_onboarding_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.router import slack_oauth

    async def _callback_handler(request: Request) -> RedirectResponse:
        return RedirectResponse("/integrations/slack/installed?team=t123")

    monkeypatch.setattr(slack_oauth, "callback_handler", _callback_handler)

    client = TestClient(_make_app())
    response = client.get("/integrations/slack/callback", follow_redirects=False)

    assert response.status_code in {302, 307}
    assert (
        _events().get(
            workflow="source_onboarding",
            event="source_install_completed",
            outcome="success",
        )
        == 1
    )


def test_oauth_callback_records_source_onboarding_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from services.ingest.integrations.router import slack_oauth

    async def _callback_handler(request: Request) -> RedirectResponse:
        return RedirectResponse("/integrations/slack/install-error?reason=state_invalid")

    monkeypatch.setattr(slack_oauth, "callback_handler", _callback_handler)

    client = TestClient(_make_app())
    response = client.get("/integrations/slack/callback", follow_redirects=False)

    assert response.status_code in {302, 307}
    assert (
        _events().get(
            workflow="source_onboarding",
            event="source_install_failed",
            outcome="bad_request",
        )
        == 1
    )
