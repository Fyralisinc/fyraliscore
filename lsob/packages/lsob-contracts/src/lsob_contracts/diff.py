"""Company-OS-compatible diff schema used by SUTs and Layer 6 evaluator."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ClaimOp(_Base):
    op: Literal["upsert_claim", "retract_claim"] = "upsert_claim"
    claim_id: str
    proposition: str
    proposition_kind: str
    asserted_confidence: float = Field(ge=0.0, le=1.0)
    falsifier: str | None = None
    evidence_signal_ids: list[str] = Field(default_factory=list)
    entities: list[str] = Field(default_factory=list)


class ActOp(_Base):
    op: Literal["transition", "create", "dissolve"] = "transition"
    entity_ref: str
    from_state: str | None = None
    to_state: str
    reason: str | None = None


class ResourceOp(_Base):
    op: Literal["allocate", "release", "reallocate"]
    resource_ref: str
    target_ref: str | None = None
    amount: float | None = None


class DiffOp(_Base):
    diff_id: str
    produced_at: datetime
    trigger_id: str | None = None
    claim_ops: list[ClaimOp] = Field(default_factory=list)
    act_ops: list[ActOp] = Field(default_factory=list)
    resource_ops: list[ResourceOp] = Field(default_factory=list)
    rationale: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
