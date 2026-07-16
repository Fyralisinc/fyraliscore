"""Narrow admitted-belief adapter over the canonical Models repository."""

from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.source_semantics import ProposedBeliefAssertion
from lib.shared.types import ModelCreate, ModelRow
from services.domain.models.repo import ModelsRepo


class EpistemicApplier:
    """Apply one already-validated asserted/report belief proposal."""

    writer_id = "EpistemicApplier"

    def __init__(self, models_repo: ModelsRepo) -> None:
        self._models_repo = models_repo

    async def apply_asserted_report(
        self,
        conn: asyncpg.Connection,
        *,
        proposal: ProposedBeliefAssertion,
        source_observation_id: UUID,
        occurred_at: datetime,
        selected_scope_entity: dict[str, Any],
        embedding: list[float],
    ) -> ModelRow:
        model = ModelCreate(
            id=proposal.proposed_model_id,
            tenant_id=proposal.tenant_id,
            born_from_event_id=source_observation_id,
            proposition=proposal.proposition,
            natural=proposal.natural,
            embedding=embedding,
            scope_entities=[selected_scope_entity],
            scope_temporal={
                "source_occurred_at": occurred_at.isoformat(),
                "interpretation_id": str(proposal.interpretation_id),
            },
            confidence=proposal.confidence,
            confidence_at_assertion=proposal.confidence,
            evidential_weight=0.5,
            domain_tags=["source_semantic"],
        )
        return await self._models_repo.insert(model, conn=conn)


__all__ = ["EpistemicApplier"]
