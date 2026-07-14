"""Compatibility import for product workflow metrics.

The metrics contract is shared by gateway and ingestion paths, so the concrete
implementation lives in ``lib.shared`` to keep service layer boundaries clean.
"""
from __future__ import annotations

from lib.shared.product_workflow_metrics import (
    PRODUCT_WORKFLOW_EVENT_OUTCOMES,
    PRODUCT_WORKFLOW_EVENTS,
    PRODUCT_WORKFLOWS,
    STATUS_CLASSES,
    ProductWorkflow,
    ProductWorkflowEvent,
    ProductWorkflowOutcome,
    StatusClass,
    classify_product_workflow,
    record_product_workflow_event,
    record_product_workflow_request,
    status_class,
)

__all__ = [
    "PRODUCT_WORKFLOW_EVENT_OUTCOMES",
    "PRODUCT_WORKFLOW_EVENTS",
    "PRODUCT_WORKFLOWS",
    "STATUS_CLASSES",
    "ProductWorkflow",
    "ProductWorkflowEvent",
    "ProductWorkflowOutcome",
    "StatusClass",
    "classify_product_workflow",
    "record_product_workflow_event",
    "record_product_workflow_request",
    "status_class",
]
