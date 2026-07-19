"""The minimal Observation-to-Model company-memory loop.

Connectors and ingestion own source normalization and immutable Observation
persistence. Entity extraction/resolution runs before this boundary. This
composition root owns the rest of Stage 1: retrieve relevant Models and
Observations, build one context packet, ask the LLM for Model changes, validate
them, and apply accepted changes atomically.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.llm.provider import LLMProvider
from lib.shared.errors import InvariantViolation
from lib.shared.ids import uuid7
from services.reasoning.retrieval.assembler import AccessContext
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.execution_policy import STAGE1_COMPANY_MEMORY_POLICY
from services.reasoning.think.reason import ThinkRunOutcome, think


@dataclass(frozen=True, slots=True)
class Stage1CompanyMemoryBatch:
    """Resolved Observation coordinates for one Stage 1 reasoning pass."""

    tenant_id: UUID
    observation_ids: tuple[UUID, ...]
    seed_entity_ids: tuple[dict[str, Any], ...] = ()
    scope_actors: tuple[UUID, ...] = ()
    seed_natural_text: str | None = None
    seed_occurred_at: datetime | None = None
    trigger_id: UUID = field(default_factory=uuid7)

    def __post_init__(self) -> None:
        if not self.observation_ids:
            raise InvariantViolation(
                "STAGE1_EMPTY_OBSERVATION_BATCH",
                "Stage 1 company memory requires at least one Observation",
            )
        if len(set(self.observation_ids)) != len(self.observation_ids):
            raise InvariantViolation(
                "STAGE1_DUPLICATE_OBSERVATION",
                "Stage 1 company memory requires unique Observation ids",
            )

    def to_trigger(self) -> TriggerContext:
        return TriggerContext(
            kind="T1",
            subkind="event_batch",
            tenant_id=self.tenant_id,
            observation_id=self.observation_ids[0],
            observation_ids=list(self.observation_ids),
            seed_entity_ids=list(self.seed_entity_ids),
            scope_actors=list(self.scope_actors),
            seed_natural_text=self.seed_natural_text,
            seed_occurred_at=self.seed_occurred_at,
            seed_signature={
                "trigger_id": str(self.trigger_id),
                "batch": True,
                "batch_size": len(self.observation_ids),
                "execution_profile": "stage1_company_memory",
            },
        )


async def process_stage1_company_memory(
    batch: Stage1CompanyMemoryBatch,
    pool: asyncpg.Pool,
    *,
    llm_provider: LLMProvider,
    embedder: Any | None = None,
    access_context: AccessContext | None = None,
) -> ThinkRunOutcome:
    """Run the complete Stage 1 loop for one resolved Observation batch."""

    return await think(
        batch.to_trigger(),
        pool,
        llm_provider=llm_provider,
        embedder=embedder,
        access_context=access_context,
        triggering_content=batch.seed_natural_text,
        reason_for_trigger="Stage 1 company-memory update",
        trigger_kind_subkind="T1:event_batch",
        execution_policy=STAGE1_COMPANY_MEMORY_POLICY,
    )


__all__ = [
    "Stage1CompanyMemoryBatch",
    "process_stage1_company_memory",
]
