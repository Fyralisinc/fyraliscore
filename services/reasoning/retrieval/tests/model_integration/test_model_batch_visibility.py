from __future__ import annotations

import hashlib
import random
import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.domain.models.repo import ModelsRepo
from services.domain.observations.events import notify_scope
from services.reasoning.retrieval.config import RetrievalConfig
from services.reasoning.retrieval.primary import TriggerContext, primary_retrieve


pytestmark = [pytest.mark.integration]


def _make_embedding(text: str, *, dim: int = 768) -> list[float]:
    seed = int.from_bytes(
        hashlib.sha256(text.encode("utf-8")).digest()[:8], "big"
    )
    rng = random.Random(seed)
    vec = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = sum(x * x for x in vec) ** 0.5
    if norm == 0:
        return vec
    return [x / norm for x in vec]


def _state_proposition(subject: str, assertion: str) -> dict[str, str]:
    return {"kind": "state", "subject": subject, "assertion": assertion}


def _draft(
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    model_id: uuid.UUID,
    natural: str,
    actor_id: uuid.UUID,
    proposition: dict[str, Any],
    scope_entities: list[dict[str, Any]],
) -> ModelCreate:
    return ModelCreate(
        id=model_id,
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition=proposition,
        natural=natural,
        embedding=_make_embedding(natural),
        scope_actors=[actor_id],
        scope_entities=scope_entities,
        scope_temporal={"type": "now"},
        confidence=0.6,
        confidence_at_assertion=0.6,
    )


@pytest.mark.timeout(180)
async def test_insert_many_large_end_to_end_batch_is_retrieval_visible(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    hero_customer = str(uuid7())
    hero_scope = [
        {"type": "customer_resource", "id": hero_customer},
        {"type": "workflow", "id": "renewal-risk"},
    ]
    noise_scope = [
        {"type": "customer_resource", "id": str(uuid7())},
        {"type": "workflow", "id": "routine-work"},
    ]

    drafts: list[ModelCreate] = []
    relevant_ids: set[uuid.UUID] = set()
    for idx in range(360):
        relevant = idx < 120
        model_id = uuid7()
        if relevant:
            relevant_ids.add(model_id)
        natural = (
            f"Beacon renewal risk evidence {idx}: ownership, onboarding, "
            "and SOC2 readiness are gating the renewal."
            if relevant
            else f"Routine account status {idx}: unrelated operational notes."
        )
        drafts.append(
            _draft(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                model_id=model_id,
                natural=natural,
                proposition=_state_proposition(
                    subject="Beacon renewal" if relevant else f"Noise account {idx}",
                    assertion=natural,
                ),
                scope_entities=hero_scope if relevant else noise_scope,
            )
        )

    with notify_scope():
        rows = await repo.insert_many(drafts, conn=tx_conn)

    assert len(rows) == 360
    assert sum(1 for row in rows if row.id in relevant_ids) == 120

    addressed = await tx_conn.fetchval(
        """
        SELECT count(*)::int
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
          AND proposition ? 'semantic_address'
        """,
        tenant,
        [row.id for row in rows],
    )
    assert addressed == 360

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "customer", "id": hero_customer}],
        seed_natural_text="Beacon renewal risk ownership onboarding SOC2",
        seed_occurred_at=datetime(2026, 6, 4, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=_make_embedding(
            "Beacon renewal risk ownership onboarding SOC2"
        ),
        semantic_k=80,
    )
    result = await primary_retrieve(
        trigger,
        tx_conn,
        models_repo=repo,
        top_n=80,
        config=RetrievalConfig(semantic_k=80, semantic_hnsw_ef_search=120),
    )

    returned_ids = [model.id for model in result.models]
    assert len(returned_ids) >= 60
    assert len(relevant_ids & set(returned_ids[:40])) >= 35
    assert len(relevant_ids & set(returned_ids[:80])) >= 60
