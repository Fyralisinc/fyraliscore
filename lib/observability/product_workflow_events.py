"""Layer-neutral bounded product workflow event metrics."""

from __future__ import annotations

from typing import Literal

from lib.observability import counter


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
PRODUCT_WORKFLOW_EVENT_OUTCOMES: tuple[ProductWorkflowOutcome, ...] = (
    "success",
    "bad_request",
    "forbidden",
    "not_found",
    "conflict",
    "error",
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
    "ProductWorkflow",
    "ProductWorkflowEvent",
    "ProductWorkflowOutcome",
    "record_product_workflow_event",
]
