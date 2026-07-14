"""Bounded product workflow metrics for the gateway.

The raw HTTP families keep route-template detail for diagnosis. These families
collapse those routes into a small, stable workflow enum for product SLOs and
dashboards.
"""
from __future__ import annotations

from typing import Literal

from lib.observability import counter, histogram


ProductWorkflow = Literal[
    "today",
    "ask",
    "recommendations",
    "forecasts",
    "decision_review",
    "model_map",
    "ceo_view",
    "source_onboarding",
    "dashboard",
    "substrate",
    "rendering",
    "history",
]
StatusClass = Literal["1xx", "2xx", "3xx", "4xx", "5xx"]
ProductWorkflowEvent = Literal[
    "recommendation_action",
    "recommendation_dismissal",
    "recommendation_watch_started",
    "recommendation_watch_cleared",
    "recommendation_triage",
    "hypothesis_ratification",
    "source_install_completed",
    "source_install_failed",
    "source_onboarding_started",
    "source_onboarding_completed",
    "source_onboarding_failed",
    "source_status_checked",
    "source_uninstalled",
    "forecast_created",
    "forecast_detail_reviewed",
    "forecast_accuracy_reviewed",
    "forecast_ask_answered",
]
ProductWorkflowOutcome = Literal[
    "success",
    "bad_request",
    "forbidden",
    "not_found",
    "conflict",
    "error",
]

PRODUCT_WORKFLOWS: tuple[ProductWorkflow, ...] = (
    "today",
    "ask",
    "recommendations",
    "forecasts",
    "decision_review",
    "model_map",
    "ceo_view",
    "source_onboarding",
    "dashboard",
    "substrate",
    "rendering",
    "history",
)
STATUS_CLASSES: tuple[StatusClass, ...] = ("1xx", "2xx", "3xx", "4xx", "5xx")
PRODUCT_WORKFLOW_EVENTS: tuple[ProductWorkflowEvent, ...] = (
    "recommendation_action",
    "recommendation_dismissal",
    "recommendation_watch_started",
    "recommendation_watch_cleared",
    "recommendation_triage",
    "hypothesis_ratification",
    "source_install_completed",
    "source_install_failed",
    "source_onboarding_started",
    "source_onboarding_completed",
    "source_onboarding_failed",
    "source_status_checked",
    "source_uninstalled",
    "forecast_created",
    "forecast_detail_reviewed",
    "forecast_accuracy_reviewed",
    "forecast_ask_answered",
)
PRODUCT_WORKFLOW_EVENT_OUTCOMES: tuple[ProductWorkflowOutcome, ...] = (
    "success",
    "bad_request",
    "forbidden",
    "not_found",
    "conflict",
    "error",
)

_REQUESTS = counter(
    "product_workflow_requests_total",
    "Gateway requests collapsed into bounded product workflow classes.",
    ("workflow", "status_class"),
    allowed_label_values={
        "workflow": PRODUCT_WORKFLOWS,
        "status_class": STATUS_CLASSES,
    },
)
_DURATION = histogram(
    "product_workflow_request_duration_seconds",
    "Gateway product workflow latency by bounded workflow class.",
    ("workflow",),
    allowed_label_values={"workflow": PRODUCT_WORKFLOWS},
)
_EVENTS = counter(
    "product_workflow_events_total",
    "Bounded product workflow business events by outcome.",
    ("workflow", "event", "outcome"),
    allowed_label_values={
        "workflow": PRODUCT_WORKFLOWS,
        "event": PRODUCT_WORKFLOW_EVENTS,
        "outcome": PRODUCT_WORKFLOW_EVENT_OUTCOMES,
    },
)


def classify_product_workflow(route_template: str) -> ProductWorkflow | None:
    route = str(route_template or "").rstrip("/") or "/"

    if route == "/v1/today" or route.startswith("/v1/today/"):
        return "today"
    if route == "/today" or route.startswith("/today/"):
        return "today"
    if route.startswith("/v1/artifacts/"):
        return "today"

    if route == "/v1/ask" or route.startswith("/v1/ask/"):
        return "ask"
    if route == "/view/ceo/ask":
        return "ask"

    if route == "/v1/recommendations" or route.startswith("/v1/recommendations/"):
        return "recommendations"
    if route.startswith("/v1/cards/"):
        return "recommendations"

    if route == "/v1/forecasts" or route.startswith("/v1/forecasts/"):
        return "forecasts"

    if route == "/v1/decision_deltas" or route.startswith("/v1/decision_deltas/"):
        return "decision_review"
    if route == "/v1/resolution_threads" or route.startswith(
        "/v1/resolution_threads/"
    ):
        return "decision_review"
    if route.startswith("/contest/") or route.startswith("/clarifications"):
        return "decision_review"

    if route.startswith("/map/") or route.startswith("/model/"):
        return "model_map"
    if route.startswith("/v1/model/"):
        return "model_map"

    if route == "/view/ceo/home" or route == "/view/ceo/force-refresh":
        return "ceo_view"

    if route.startswith("/integrations/") or route.startswith("/finance/"):
        return "source_onboarding"
    if route.startswith("/slack/") and route.endswith("/install"):
        return "source_onboarding"

    if route == "/dashboard" or route.startswith("/dashboard/"):
        return "dashboard"

    if route in {
        "/observations",
        "/models",
        "/commitments",
        "/goals",
        "/decisions",
        "/resources",
    }:
        return "substrate"

    if route == "/rendering" or route.startswith("/rendering/"):
        return "rendering"

    if route == "/v1/history" or route.startswith("/v1/history/"):
        return "history"

    return None


def status_class(status_code: int | str) -> StatusClass:
    try:
        code = int(status_code)
    except (TypeError, ValueError):
        code = 500
    if code < 200:
        return "1xx"
    if code < 300:
        return "2xx"
    if code < 400:
        return "3xx"
    if code < 500:
        return "4xx"
    return "5xx"


def record_product_workflow_request(
    *,
    route_template: str,
    status_code: int | str,
    duration_seconds: float,
) -> ProductWorkflow | None:
    workflow = classify_product_workflow(route_template)
    if workflow is None:
        return None
    _REQUESTS.inc(workflow=workflow, status_class=status_class(status_code))
    _DURATION.observe(max(0.0, float(duration_seconds)), workflow=workflow)
    return workflow


def record_product_workflow_event(
    *,
    workflow: ProductWorkflow,
    event: ProductWorkflowEvent,
    outcome: ProductWorkflowOutcome = "success",
) -> None:
    _EVENTS.inc(workflow=workflow, event=event, outcome=outcome)


__all__ = [
    "PRODUCT_WORKFLOW_EVENT_OUTCOMES",
    "PRODUCT_WORKFLOW_EVENTS",
    "PRODUCT_WORKFLOWS",
    "STATUS_CLASSES",
    "classify_product_workflow",
    "record_product_workflow_event",
    "record_product_workflow_request",
    "status_class",
]
