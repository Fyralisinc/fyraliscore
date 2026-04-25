"""Belief-query and evaluation-context shapes used by evaluators."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from lsob_contracts.models import Corpus, EntityRef


class _Base(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)


class BeliefQuery(_Base):
    query_id: str
    entity_ref: EntityRef
    timestamp: datetime
    proposition_kind: str | None = None
    k: int = 10


class Belief(_Base):
    claim_id: str
    proposition: str
    proposition_kind: str
    asserted_confidence: float = Field(ge=0.0, le=1.0)
    last_updated: datetime
    entities: list[str] = Field(default_factory=list)
    evidence_signal_ids: list[str] = Field(default_factory=list)


class AtRiskItem(_Base):
    entity_ref: EntityRef
    risk_score: float = Field(ge=0.0, le=1.0)
    risk_kind: str
    rationale: str | None = None


class AtRiskReport(_Base):
    timestamp: datetime
    items: list[AtRiskItem] = Field(default_factory=list)


class EvaluationContext(_Base):
    corpus: Corpus
    sut: Any
    ground_truth_checkpoint: datetime
    run_id: str
    extras: dict[str, Any] = Field(default_factory=dict)
