from __future__ import annotations

import pytest
from fastapi import FastAPI
from starlette.testclient import TestClient

from lib.observability import counter, histogram, render_default, reset_default_for_tests
from services.app.gateway.middleware import RequestContextMiddleware
from services.app.gateway.product_workflow_metrics import (
    PRODUCT_WORKFLOW_EVENT_OUTCOMES,
    PRODUCT_WORKFLOW_EVENTS,
    PRODUCT_WORKFLOWS,
    STATUS_CLASSES,
    classify_product_workflow,
    record_product_workflow_event,
    record_product_workflow_request,
    status_class,
)


def _requests():
    return counter(
        "product_workflow_requests_total",
        "lookup",
        ("workflow", "status_class"),
        allowed_label_values={
            "workflow": PRODUCT_WORKFLOWS,
            "status_class": STATUS_CLASSES,
        },
    )


def _duration():
    return histogram(
        "product_workflow_request_duration_seconds",
        "lookup",
        ("workflow",),
        allowed_label_values={"workflow": PRODUCT_WORKFLOWS},
    )


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


@pytest.mark.parametrize(
    ("route", "workflow"),
    [
        ("/v1/today", "today"),
        ("/today/deltas/{delta_id}/apply", "today"),
        ("/v1/ask/sessions/{session_id}/messages", "ask"),
        ("/view/ceo/ask", "ask"),
        ("/v1/recommendations/{recommendation_id}/act", "recommendations"),
        ("/v1/cards/{card_id}/probe", "recommendations"),
        ("/v1/forecasts/detail/{forecast_id}", "forecasts"),
        ("/v1/decision_deltas/{delta_id}/accept", "decision_review"),
        ("/v1/resolution_threads/{thread_id}/evaluate", "decision_review"),
        ("/map/snapshot", "model_map"),
        ("/model/items/{item_id}/trace", "model_map"),
        ("/v1/model/{node_id}/trace", "model_map"),
        ("/view/ceo/home", "ceo_view"),
        ("/integrations/gmail/status", "source_onboarding"),
        ("/finance/{source}/status", "source_onboarding"),
        ("/dashboard/revenue-at-risk", "dashboard"),
        ("/models", "substrate"),
        ("/rendering/card", "rendering"),
        ("/v1/history/summary", "history"),
    ],
)
def test_classify_product_workflow(route: str, workflow: str) -> None:
    assert classify_product_workflow(route) == workflow


@pytest.mark.parametrize(
    "route",
    [
        "/webhooks/slack",
        "/debug/whatsapp",
        "/internal/synthesis-reader/read",
        "/healthz",
        "/readyz",
        "/metrics",
        "/ingest/slack",
        "/v1/spec/forecasts",
        "unmatched",
    ],
)
def test_classify_product_workflow_ignores_non_product_routes(route: str) -> None:
    assert classify_product_workflow(route) is None


@pytest.mark.parametrize(
    ("code", "klass"),
    [(199, "1xx"), (200, "2xx"), (302, "3xx"), (404, "4xx"), (500, "5xx")],
)
def test_status_class(code: int, klass: str) -> None:
    assert status_class(code) == klass


def test_record_product_workflow_request_emits_bounded_metrics() -> None:
    workflow = record_product_workflow_request(
        route_template="/v1/ask/sessions/{session_id}/messages",
        status_code=500,
        duration_seconds=0.25,
    )
    rendered = render_default()

    assert workflow == "ask"
    assert _requests().get(workflow="ask", status_class="5xx") == 1
    assert _duration().get_count(workflow="ask") == 1
    assert 'product_workflow_requests_total{workflow="ask",status_class="5xx"} 1' in rendered
    assert "session_id" not in rendered


def test_record_product_workflow_event_emits_bounded_metrics() -> None:
    record_product_workflow_event(
        workflow="recommendations",
        event="recommendation_action",
        outcome="success",
    )
    rendered = render_default()

    assert (
        _events().get(
            workflow="recommendations",
            event="recommendation_action",
            outcome="success",
        )
        == 1
    )
    assert (
        'product_workflow_events_total{workflow="recommendations",'
        'event="recommendation_action",outcome="success"} 1'
    ) in rendered


def test_record_product_workflow_event_emits_source_onboarding_metrics() -> None:
    record_product_workflow_event(
        workflow="source_onboarding",
        event="source_status_checked",
        outcome="not_found",
    )
    rendered = render_default()

    assert (
        _events().get(
            workflow="source_onboarding",
            event="source_status_checked",
            outcome="not_found",
        )
        == 1
    )
    assert (
        'product_workflow_events_total{workflow="source_onboarding",'
        'event="source_status_checked",outcome="not_found"} 1'
    ) in rendered


def test_record_product_workflow_event_rejects_unbounded_values() -> None:
    with pytest.raises(ValueError, match="declared allowlist"):
        record_product_workflow_event(
            workflow="recommendations",
            event="recommendation_action",  # type: ignore[arg-type]
            outcome="unknown",  # type: ignore[arg-type]
        )


def test_request_context_middleware_records_product_workflow_metrics() -> None:
    app = FastAPI()
    app.add_middleware(RequestContextMiddleware)

    @app.get("/v1/today")
    def today() -> dict[str, str]:
        return {"ok": "true"}

    @app.get("/webhooks/slack")
    def webhook() -> dict[str, str]:
        return {"ok": "true"}

    client = TestClient(app)
    before = _requests().get(workflow="today", status_class="2xx")
    before_duration = _duration().get_count(workflow="today")

    assert client.get("/v1/today").status_code == 200
    assert client.get("/webhooks/slack").status_code == 200

    assert _requests().get(workflow="today", status_class="2xx") - before == 1
    assert _duration().get_count(workflow="today") - before_duration == 1
    assert _requests().get(workflow="source_onboarding", status_class="2xx") == 0
