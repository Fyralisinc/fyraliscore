"""Cadence adapter for Sage topology optimization.

Routes, scheduled jobs, and tests should call this module instead of
constructing `TopologyOptimizer` directly when they just need one
default-wired optimization pass. The optimizer remains the algorithm;
this module owns adapter concerns such as trigger normalization and
source labels.
"""
from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import asyncpg

from services.reasoning.sage.topology_optimizer.optimizer import TopologyOptimizer
from services.reasoning.sage.topology_optimizer.types import OptimizationRunReport


TRIGGER_VALIDATED_DIFF = "validated_synthesis_diff_applied"
TRIGGER_DIFF_FAILED_VALIDATION = "reasoning_diff_failed_validation"
TRIGGER_USER_CONTESTED_NODE = "user_contested_node"
TRIGGER_USER_ACCEPTED_NODE = "user_accepted_node"
TRIGGER_PREDICTION_CONFIRMED = "prediction_confirmed"
TRIGGER_PREDICTION_FALSIFIED = "prediction_falsified"
TRIGGER_OMITTED_EVIDENCE_REQUESTED = "omitted_evidence_later_requested"
TRIGGER_INQUIRY_INSUFFICIENT = "inquiry_session_ended_insufficient"
TRIGGER_BACKGROUND_REGION_SCAN = "background_region_scan_complete"

SCHEDULED_TRIGGER = TRIGGER_BACKGROUND_REGION_SCAN
_LEGACY_TRIGGER_ALIASES = {
    "": SCHEDULED_TRIGGER,
    "scheduled": SCHEDULED_TRIGGER,
    "background": SCHEDULED_TRIGGER,
    "background_region_scan": SCHEDULED_TRIGGER,
}


def normalize_trigger_event(value: str | None) -> str:
    raw = (value or "").strip()
    return _LEGACY_TRIGGER_ALIASES.get(raw, raw or SCHEDULED_TRIGGER)


@dataclass(frozen=True, slots=True)
class OptimizationCadenceRequest:
    tenant_id: UUID
    inquiry_session_id: UUID
    trigger_event: str = SCHEDULED_TRIGGER
    source: str = "unspecified"

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "trigger_event",
            normalize_trigger_event(self.trigger_event),
        )
        object.__setattr__(
            self,
            "source",
            (self.source or "unspecified").strip() or "unspecified",
        )


async def run_optimization_pass(
    *,
    pool: asyncpg.Pool | None,
    request: OptimizationCadenceRequest,
    conn: asyncpg.Connection | None = None,
) -> OptimizationRunReport:
    optimizer = TopologyOptimizer(pool=pool, tenant_id=request.tenant_id)
    return await optimizer.optimize(
        inquiry_session_id=request.inquiry_session_id,
        trigger_event=request.trigger_event,
        conn=conn,
    )


__all__ = [
    "OptimizationCadenceRequest",
    "SCHEDULED_TRIGGER",
    "TRIGGER_BACKGROUND_REGION_SCAN",
    "TRIGGER_DIFF_FAILED_VALIDATION",
    "TRIGGER_INQUIRY_INSUFFICIENT",
    "TRIGGER_OMITTED_EVIDENCE_REQUESTED",
    "TRIGGER_PREDICTION_CONFIRMED",
    "TRIGGER_PREDICTION_FALSIFIED",
    "TRIGGER_USER_ACCEPTED_NODE",
    "TRIGGER_USER_CONTESTED_NODE",
    "TRIGGER_VALIDATED_DIFF",
    "normalize_trigger_event",
    "run_optimization_pass",
]
