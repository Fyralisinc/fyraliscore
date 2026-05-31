"""services.sage.model_predictions.types — Pydantic row types.

Mirrors the columns of `model_predictions` and `model_prediction_errors`
(migration 0054). These types are the exchange format between the
repo (`repo.py`), the pure residual helper (`residual.py`), and any
worker / API caller that reads from or writes to the Phase 12 surface.

IMPORTANT — these are the INTERNAL Model-substrate prediction types.
The CEO/Forecasts surface (`predictions` table, migration 0041) uses
unrelated Pydantic shapes in services.forecasts (or similar). Do not
cross-import.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


# Closed status vocabularies. Kept as Literals so typos surface in
# CI; the Postgres column is `TEXT` with a CHECK so the wire-level
# enforcement matches.
PredictionStatus = Literal[
    "active",
    "confirmed",
    "falsified",
    "expired",
    "superseded",
]

PredictionErrorStatus = Literal[
    "open",
    "triaged",
    "inquiry_scheduled",
    "inquiry_complete",
    "resolved",
    "ignored",
]


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=False)


class ExpectedObservation(_Strict):
    """Structured expectation payload stored in
    `model_predictions.expected_observation`.

    All fields are optional except `kind` so callers can express
    everything from a coarse "anything about Acme that suggests
    unblock" down to a precise "ARR delta on Globex renewal > 0
    before 2026-07-01".

    Extension fields (anything not declared here) are accepted via the
    `extras` bag — Pydantic's `extra='forbid'` on this class keeps the
    declared keys tight, but the repo serializes the whole model with
    `model_dump(exclude_none=True)` and merges `extras` into the JSONB
    payload at write time. Callers that need to round-trip arbitrary
    extension fields should put them in `extras`.
    """

    kind: str = Field(..., description="What kind of observation to expect (e.g. 'state_change', 'metric_delta', 'evidence_arrival').")
    scope_entities: list[dict[str, Any]] = Field(default_factory=list)
    scope_actors: list[UUID] = Field(default_factory=list)
    value_constraint: Optional[dict[str, Any]] = Field(
        default=None,
        description="Optional structured comparator, e.g. {'op': 'gt', 'value': 0} or {'op': 'eq', 'value': 'unblocked'}.",
    )
    falsification_rule: Optional[str] = Field(
        default=None,
        description="Natural-language fallback for the residual detector when the structured constraint is absent.",
    )
    extras: dict[str, Any] = Field(
        default_factory=dict,
        description="Free-form extension fields merged into the JSONB payload at write time.",
    )


class ModelPrediction(_Strict):
    """Pydantic mirror of a `model_predictions` row.

    `id` and the timestamp columns are optional on the way IN (the
    repo fills them when callers omit them); they are always present
    on the way OUT.
    """

    id: Optional[UUID] = None
    tenant_id: UUID
    model_id: UUID
    prediction: str
    expected_observation: ExpectedObservation
    check_after: Optional[datetime] = None
    status: PredictionStatus = "active"
    confidence: Optional[float] = Field(default=None, ge=0.0, le=1.0)
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    resolved_at: Optional[datetime] = None
    resolved_by_observation_id: Optional[UUID] = None


class ModelPredictionError(_Strict):
    """Pydantic mirror of a `model_prediction_errors` row.

    Created when `residual.detect_prediction_error` decides the
    incoming observation contradicts a live prediction.
    """

    id: Optional[UUID] = None
    tenant_id: UUID
    model_id: UUID
    prediction_id: Optional[UUID] = None
    observed_signal_id: Optional[UUID] = None
    error_summary: str
    severity: float = Field(..., ge=0.0, le=1.0)
    impact_score: float = Field(..., ge=0.0, le=1.0)
    status: PredictionErrorStatus = "open"
    triage_notes: Optional[dict[str, Any]] = None
    created_at: Optional[datetime] = None


__all__ = [
    "ExpectedObservation",
    "ModelPrediction",
    "ModelPredictionError",
    "PredictionErrorStatus",
    "PredictionStatus",
]
