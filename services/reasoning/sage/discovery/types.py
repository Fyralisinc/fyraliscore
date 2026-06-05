"""Pydantic shapes for SAGE discovery shortcuts + negative memory (Phase 10).

These types live in the **Discovery Utility Layer** (doc §2). They
record *learned retrieval utility* and *learned dead-ends* — they are
NOT canonical truth. The field names are chosen to keep that
distinction obvious at every call site:

  * `Signature` is the inquiry-shape used to key shortcuts and negative
    memory. It is intentionally loose JSON-style so callers can match
    by JSONB containment (@>) on partial signatures.

  * `DiscoveryShortcut.utility_score` / `success_count` / `failure_count`
    are mutable retrieval bookkeeping — they do not encode any
    causal/truth claim about the target.

  * `NegativeMemory.rejected_claim` / `rejected_path` describe what
    Fyralis learned to stop chasing. Every row carries `expires_at`
    because company reality changes (doc §14).

Schema reference: db/migrations/0087_sage_discovery_and_negative_memory.sql.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


# Memory-type discriminator for `negative_memory.memory_type`. Mirrors
# the SQL CHECK constraint in migration 0087.
NegativeMemoryType = Literal[
    "rejected_hypothesis",
    "noisy_path",
    "failed_shortcut",
    "low_value_node",
]


class _Strict(BaseModel):
    """Pydantic base — forbid unknown fields so typos surface in CI."""

    model_config = ConfigDict(extra="forbid", frozen=False)


class Signature(_Strict):
    """Inquiry-shape signature used to key shortcuts + negative memory.

    Shape per doc §11.2:

        {
          "signal_type": "enterprise_customer_blocker",
          "entities": ["customer", "SSO"],
          "question_primitive": "DEPENDENCY"
        }

    Stored as JSONB. Callers may match exactly (==) or by containment
    (@>) for partial signatures — e.g. "any shortcut whose signature
    mentions question_primitive=DEPENDENCY".

    All three top-level fields are optional individually because a
    fast-path probe may only have a signal_type, while a deep-inquiry
    probe will populate all three. At least one must be present.
    """

    signal_type: str | None = None
    entities: list[str] = Field(default_factory=list)
    question_primitive: str | None = None

    @model_validator(mode="after")
    def _at_least_one_field(self) -> "Signature":
        if (
            self.signal_type is None
            and not self.entities
            and self.question_primitive is None
        ):
            raise ValueError(
                "Signature must have at least one of signal_type, "
                "entities, or question_primitive"
            )
        return self

    def to_jsonable(self) -> dict[str, Any]:
        """Serialize for JSONB storage — drop empty/None fields so
        containment matching does the right thing."""
        out: dict[str, Any] = {}
        if self.signal_type is not None:
            out["signal_type"] = self.signal_type
        if self.entities:
            out["entities"] = list(self.entities)
        if self.question_primitive is not None:
            out["question_primitive"] = self.question_primitive
        return out


class DiscoveryShortcut(_Strict):
    """Pydantic mirror of a `discovery_shortcuts` row.

    A row says: "when this inquiry signature appears, this
    model/region/affordance has historically been useful to inspect."

    It is NOT a causal edge, NOT a truth claim, NOT evidence — see
    doc §2 / §11.
    """

    id: UUID
    tenant_id: UUID
    from_signature: dict[str, Any]
    to_model_id: UUID | None = None
    to_region_id: UUID | None = None
    to_affordance: str | None = None
    utility_score: float = 0.0
    success_count: int = 0
    failure_count: int = 0
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    expires_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None

    @model_validator(mode="after")
    def _at_least_one_target(self) -> "DiscoveryShortcut":
        if (
            self.to_model_id is None
            and self.to_region_id is None
            and self.to_affordance is None
        ):
            raise ValueError(
                "DiscoveryShortcut must have at least one of "
                "to_model_id, to_region_id, or to_affordance"
            )
        return self


class NegativeMemory(_Strict):
    """Pydantic mirror of a `negative_memory` row.

    Records a rejected hypothesis, a noisy retrieval path, a failed
    shortcut, or a low-value node so future inquiry does not waste
    cycles re-discovering it. Every row carries `expires_at` (NOT NULL
    in SQL): doc §14 mandates expiry because company reality changes.
    """

    id: UUID
    tenant_id: UUID
    memory_type: NegativeMemoryType
    signature: dict[str, Any]
    rejected_claim: str | None = None
    rejected_path: dict[str, Any] | list[Any] | None = None
    reason: str
    evidence_snapshot_hash: str | None = None
    confidence: float | None = None
    created_at: datetime | None = None
    expires_at: datetime


__all__ = [
    "DiscoveryShortcut",
    "NegativeMemory",
    "NegativeMemoryType",
    "Signature",
]
