"""Authoritative outcome facts and repair-trigger helpers."""

from services.reasoning.oracle.outcome_facts import (
    AUTHORITATIVE_TRUST_TIER,
    OutcomeFact,
    RepairEnqueueResult,
    enqueue_outcome_representation_repair,
    human_correction_outcome_fact,
    representation_repair_payload_for_outcome,
)

__all__ = [
    "AUTHORITATIVE_TRUST_TIER",
    "OutcomeFact",
    "RepairEnqueueResult",
    "enqueue_outcome_representation_repair",
    "human_correction_outcome_fact",
    "representation_repair_payload_for_outcome",
]
