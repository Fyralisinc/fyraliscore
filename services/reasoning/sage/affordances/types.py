"""Pydantic shapes for SAGE retrieval affordance profiles (Phase 9).

A `RetrievalAffordanceProfile` is *derived* metadata about a Model
(= SAGE Node) — it answers "what kinds of questions does this Model help
answer, what hypotheses does it support or weaken, what abstractions
does it commonly participate in, what signals should activate it, and
what evidence should be projected if it becomes relevant".

Canonical Model fields (proposition, confidence, falsifier) are never
written through this surface; affordances are populated by heuristics
(`policy.derive_default_profile_from_model`) and later evolved via
reinforcement signals from inquiry / valid-diff outcomes.

Schema reference: db/migrations/0086_sage_retrieval_affordance_profiles.sql.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# Closed v1 vocabulary of question primitives the synthesis layer
# recognizes. Kept as a `Literal` so typos surface in CI, but the
# Postgres column is a plain `text[]` so future primitives can be
# rolled out without a schema migration.
QuestionPrimitive = Literal[
    "DEPENDENCY",
    "CONSTRAINT",
    "CAUSE",
    "ACTION",
    "OWNERSHIP",
    "COUNTEREVIDENCE",
    "PATTERN",
    "GOAL_IMPACT",
    "RECURRENCE",
]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


class RetrievalAffordanceProfile(_Strict):
    """Pydantic mirror of the `retrieval_affordance_profiles` row.

    `model_id` is the primary key — Fyralis Models stand in for SAGE
    Nodes, and a profile is 1:1 with its parent Model (ON DELETE
    CASCADE in SQL).
    """

    model_id: UUID
    tenant_id: UUID
    answers_question_primitives: list[str] = Field(default_factory=list)
    supports_hypothesis_types: list[str] = Field(default_factory=list)
    weakens_hypothesis_types: list[str] = Field(default_factory=list)
    common_composition_types: list[str] = Field(default_factory=list)
    action_affordances: list[str] = Field(default_factory=list)
    activation_signatures: dict[str, Any] = Field(default_factory=dict)
    projection_policy: dict[str, Any] = Field(default_factory=dict)
    utility_score: float = 0.0
    decay_after: datetime | None = None
    last_reinforced_at: datetime | None = None
    created_at: datetime | None = None
    last_updated_at: datetime | None = None


__all__ = ["QuestionPrimitive", "RetrievalAffordanceProfile"]
