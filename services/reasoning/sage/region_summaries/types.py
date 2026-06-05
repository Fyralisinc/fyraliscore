"""services.reasoning.sage.region_summaries.types — Pydantic shapes for region summaries.

Mirrors the columns of `region_sufficient_state` (migration 0088).
Nested JSON list elements are typed so callers serialize structured
items rather than freeform dicts; the schema itself remains JSONB so
the closed shapes can evolve without a migration.

Spec reference: fyralis-sage-synthesis-self-evolution.md §12 / Phase 11.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# Refresh triggers from Phase 11. Must stay in lockstep with the SQL
# CHECK constraint on `last_refreshed_reason`.
RefreshReason = Literal[
    "validated_model_update",
    "high_impact_signal",
    "prediction_error",
    "user_contestation",
    "scheduled",
    "region_anomaly",
]


class _Strict(BaseModel):
    """Forbid-extra base for the nested JSON shapes.

    Extra keys would otherwise round-trip silently through JSONB and
    accumulate drift; surfacing them here forces every new field through
    a deliberate edit.
    """

    model_config = ConfigDict(extra="forbid", frozen=False)


# ---------------------------------------------------------------------
# Nested JSON shapes
# ---------------------------------------------------------------------


class Hypothesis(_Strict):
    """An active hypothesis the region is currently weighing.

    `id` is opaque (string, not UUID) so call-sites can mint short
    handles like "h_sso_blocked" that survive across refreshes.
    """

    id: str
    statement: str
    confidence: float = 0.5
    support_model_ids: list[UUID] = Field(default_factory=list)


class Constraint(_Strict):
    """A binding constraint currently shaping the region's behavior."""

    id: str
    statement: str
    source_model_ids: list[UUID] = Field(default_factory=list)


class Counterevidence(_Strict):
    """A piece of evidence that pushes back on the active narrative."""

    statement: str
    source_model_ids: list[UUID] = Field(default_factory=list)
    weight: float = 0.5


class Unknown(_Strict):
    """An unresolved question / known unknown for the region."""

    question: str
    blocking_for: list[str] = Field(default_factory=list)


class Frontier(_Strict):
    """A next-best inquiry frontier the planner should consider.

    `expected_information_gain` is a planner hint in [0, 1]; the
    refresher can leave it at 0 when it can't score.
    """

    target: str
    rationale: str
    expected_information_gain: float = 0.0


class FalsificationWatch(_Strict):
    """A condition that, if observed, would falsify a current hypothesis."""

    hypothesis_id: str
    condition: str
    check_after: Optional[datetime] = None


# ---------------------------------------------------------------------
# Row type
# ---------------------------------------------------------------------


class RegionSufficientState(BaseModel):
    """Pydantic mirror of a `region_sufficient_state` row.

    `region_id` is the primary key but does NOT FK into a regions
    table (none exists yet); see migration 0088 header. Loose-ref
    arrays (`affected_goals`, `affected_commitments`,
    `member_model_ids`) tolerate dangling ids — readers filter on
    fetch rather than relying on cascade.
    """

    model_config = ConfigDict()

    region_id: UUID
    tenant_id: UUID
    region_label: Optional[str] = None
    summary: str

    active_hypotheses: list[Hypothesis] = Field(default_factory=list)
    active_constraints: list[Constraint] = Field(default_factory=list)
    known_counterevidence: list[Counterevidence] = Field(default_factory=list)
    unresolved_unknowns: list[Unknown] = Field(default_factory=list)

    affected_goals: list[UUID] = Field(default_factory=list)
    affected_commitments: list[UUID] = Field(default_factory=list)
    member_model_ids: list[UUID] = Field(default_factory=list)

    priority_score: float = 0.0
    prediction_error_score: float = 0.0

    next_best_frontiers: list[Frontier] = Field(default_factory=list)
    falsification_watch: list[FalsificationWatch] = Field(default_factory=list)

    last_refreshed_reason: Optional[RefreshReason] = None

    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


__all__ = [
    "Constraint",
    "Counterevidence",
    "FalsificationWatch",
    "Frontier",
    "Hypothesis",
    "RefreshReason",
    "RegionSufficientState",
    "Unknown",
]


# Internal helpers shared between repo and refresh — kept here so the
# Pydantic shapes own the canonical (de)serialization rules.

def hypothesis_to_jsonable(h: Hypothesis) -> dict[str, Any]:
    return h.model_dump(mode="json")


def constraint_to_jsonable(c: Constraint) -> dict[str, Any]:
    return c.model_dump(mode="json")


def counterevidence_to_jsonable(c: Counterevidence) -> dict[str, Any]:
    return c.model_dump(mode="json")


def unknown_to_jsonable(u: Unknown) -> dict[str, Any]:
    return u.model_dump(mode="json")


def frontier_to_jsonable(f: Frontier) -> dict[str, Any]:
    return f.model_dump(mode="json")


def falsification_to_jsonable(w: FalsificationWatch) -> dict[str, Any]:
    return w.model_dump(mode="json")
