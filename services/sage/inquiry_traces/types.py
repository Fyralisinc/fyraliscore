"""Row models for the SAGE inquiry-trace gap-filler tables (mig 0049).

Pydantic models mirror the column lists in
db/migrations/0049_sage_inquiry_trace_gap_fillers.sql. They exist so
the repo surface is type-checked and so callers can construct insertable
payloads without juggling raw dicts.

Enum frozensets (`OMISSION_REASONS`, `OUTCOME_EVENT_TYPES`) duplicate
the SQL CHECK constraints so the repo can fail fast at the Python layer
before hitting Postgres.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------
# Enums — must stay in sync with the SQL CHECK constraints.
# ---------------------------------------------------------------------

OMISSION_REASONS: frozenset[str] = frozenset({
    "generic_hub",
    "redundant",
    "out_of_scope",
    "low_confidence",
    "budget_exhausted",
    "access_denied",
    "stale",
    "other",
})

OUTCOME_EVENT_TYPES: frozenset[str] = frozenset({
    "retrieved_evidence_used_in_packet",
    "retrieved_evidence_omitted",
    "omitted_evidence_later_requested",
    "node_used_in_valid_diff",
    "path_used_in_valid_diff",
    "reader_decision_used_in_valid_diff",
    "reader_decision_low_value",
    "outcome_quality_assessed",
    "validation_failed_due_to_missing_evidence",
    "validation_failed_due_to_bad_reference",
    "user_accepted_node",
    "user_contested_node",
    "model_later_confirmed",
    "model_later_falsified",
    "recommendation_acted_on",
    "recommendation_ignored",
})


# ---------------------------------------------------------------------
# Row models
# ---------------------------------------------------------------------


class RetrievalPlanRow(BaseModel):
    """One planned retrieval program for a question.

    Created BEFORE the plan executes. `intents` / `paths` / `budgets` /
    `success_conditions` are JSONB on the wire; we expose them as the
    natural Python shapes (list / dict) and let the repo serialize.
    """

    model_config = ConfigDict(extra="forbid")

    id: UUID | None = None
    tenant_id: UUID | None = None
    inquiry_session_id: UUID
    question_id: str
    plan_revision: int = 0
    intents: list[dict[str, Any]] = Field(default_factory=list)
    paths: list[dict[str, Any]] = Field(default_factory=list)
    budgets: dict[str, Any] = Field(default_factory=dict)
    success_conditions: list[dict[str, Any]] = Field(default_factory=list)
    notes: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class OmittedEvidenceRow(BaseModel):
    """Evidence retrieved but not included in the final packet.

    `omission_reason` is one of `OMISSION_REASONS`. `retrieval_paths`
    captures which pathway surfaced the item so the topology optimizer
    can punish noisy paths.
    """

    model_config = ConfigDict(extra="forbid")

    id: UUID | None = None
    tenant_id: UUID | None = None
    inquiry_session_id: UUID
    question_id: str | None = None
    source_type: str
    source_ref: str
    source_ref_id: UUID | None = None
    retrieval_paths: list[dict[str, Any]] = Field(default_factory=list)
    omission_reason: str
    reason_detail: str | None = None
    score: float = 0.0
    metadata: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


class OutcomeEventRow(BaseModel):
    """Typed outcome event per spec §15.1.

    `event_type` is one of `OUTCOME_EVENT_TYPES`. `payload` is JSONB
    so each event type can carry its own shape.
    """

    model_config = ConfigDict(extra="forbid")

    id: UUID | None = None
    tenant_id: UUID | None = None
    inquiry_session_id: UUID
    event_type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime | None = None


__all__ = [
    "OMISSION_REASONS",
    "OUTCOME_EVENT_TYPES",
    "OmittedEvidenceRow",
    "OutcomeEventRow",
    "RetrievalPlanRow",
]
