from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pytest

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate
from services.models.batch import plan_model_batch
from services.models.repo import ModelsRepo
from services.models.tests.conftest import make_embedding, state_proposition
from services.observations.events import notify_scope
from services.retrieval.config import RetrievalConfig
from services.retrieval.primary import TriggerContext, primary_retrieve


def _draft(
    *,
    tenant: uuid.UUID | None = None,
    born_from_event: uuid.UUID | None = None,
    model_id: uuid.UUID | None = None,
    proposition: dict[str, Any] | None = None,
    natural: str = "Batch model says Alice owns the renewal risk.",
    actor_id: uuid.UUID | None = None,
    scope_entities: list[dict[str, Any]] | None = None,
    supporting_model_ids: list[uuid.UUID] | None = None,
    contributing_models: list[uuid.UUID] | None = None,
) -> ModelCreate:
    tenant_id = tenant or uuid.uuid4()
    event_id = born_from_event or uuid.uuid4()
    return ModelCreate(
        id=model_id,
        tenant_id=tenant_id,
        born_from_event_id=event_id,
        proposition=proposition or state_proposition(
            subject=natural[:60],
            assertion=natural,
        ),
        natural=natural,
        embedding=make_embedding(natural),
        scope_actors=[actor_id] if actor_id else [],
        scope_entities=scope_entities or [],
        scope_temporal={"type": "now"},
        confidence=0.6,
        confidence_at_assertion=0.6,
        supporting_model_ids=supporting_model_ids or [],
        contributing_models=contributing_models or [],
    )


def _situation_prop(member_ids: list[uuid.UUID]) -> dict[str, Any]:
    return {
        "kind": "belief",
        "claim_role": "situation",
        "abstraction_level": "composite",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "mixed",
        "situation": "Renewal risk is jointly driven by delivery and ownership.",
        "summary": "Renewal risk is jointly driven by delivery and ownership.",
        "member_model_ids": [str(mid) for mid in member_ids],
        "relationship_summary": "The member claims share one renewal mechanism.",
        "pressure_type": "revenue",
        "shared_mechanism": "Delivery and ownership both gate the same renewal.",
    }


def test_plan_model_batch_assigns_ids_and_orders_dependency_strata() -> None:
    tenant = uuid.uuid4()
    event = uuid.uuid4()
    left = uuid.uuid4()
    right = uuid.uuid4()
    dependent = uuid.uuid4()
    situation = uuid.uuid4()

    plan = plan_model_batch([
        _draft(
            tenant=tenant,
            born_from_event=event,
            model_id=situation,
            proposition=_situation_prop([left, right]),
            natural="The renewal risk has become a shared situation.",
        ),
        _draft(
            tenant=tenant,
            born_from_event=event,
            model_id=dependent,
            natural="Dependent model is supported by the left model.",
            supporting_model_ids=[left],
        ),
        _draft(
            tenant=tenant,
            born_from_event=event,
            model_id=right,
            natural="Right member says onboarding is late.",
        ),
        _draft(
            tenant=tenant,
            born_from_event=event,
            model_id=left,
            natural="Left member says ownership is ambiguous.",
        ),
    ])

    assert [planned.id for planned in plan.models] == [
        situation,
        dependent,
        right,
        left,
    ]
    assert [{planned.id for planned in stratum} for stratum in plan.strata] == [
        {right, left},
        {situation, dependent},
    ]


def test_plan_model_batch_rejects_duplicate_ids() -> None:
    model_id = uuid.uuid4()

    with pytest.raises(ValidationError) as exc:
        plan_model_batch([
            _draft(model_id=model_id),
            _draft(model_id=model_id, natural="Duplicate id draft."),
        ])

    assert "duplicate Model ids" in exc.value.message


def test_plan_model_batch_rejects_self_dependency() -> None:
    model_id = uuid.uuid4()

    with pytest.raises(ValidationError) as exc:
        plan_model_batch([
            _draft(
                model_id=model_id,
                supporting_model_ids=[model_id],
            )
        ])

    assert "self-dependency" in exc.value.message


def test_plan_model_batch_rejects_intra_batch_cycles_before_write() -> None:
    left = uuid.uuid4()
    right = uuid.uuid4()

    with pytest.raises(ValidationError) as exc:
        plan_model_batch([
            _draft(model_id=left, supporting_model_ids=[right]),
            _draft(model_id=right, supporting_model_ids=[left]),
        ])

    assert "dependency cycle" in exc.value.message


pytestmark = [pytest.mark.integration]


async def test_insert_many_orders_dependencies_and_preserves_side_effects(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    left = uuid7()
    right = uuid7()
    dependent = uuid7()
    situation = uuid7()

    drafts = [
        _draft(
            tenant=tenant,
            born_from_event=born_from_event,
            actor_id=actor_id,
            model_id=situation,
            proposition=_situation_prop([left, right]),
            natural="Composite situation: Beacon renewal has shared pressure.",
        ),
        _draft(
            tenant=tenant,
            born_from_event=born_from_event,
            actor_id=actor_id,
            model_id=dependent,
            proposition={
                "kind": "prediction",
                "expected": "Beacon renewal slips without ownership clarity.",
                "resolution": "Beacon renewal status by quarter end.",
            },
            natural="Beacon renewal may slip without ownership clarity.",
            supporting_model_ids=[left],
            contributing_models=[right],
        ),
        _draft(
            tenant=tenant,
            born_from_event=born_from_event,
            actor_id=actor_id,
            model_id=right,
            natural="Beacon onboarding timeline is late.",
        ),
        _draft(
            tenant=tenant,
            born_from_event=born_from_event,
            actor_id=actor_id,
            model_id=left,
            natural="Beacon renewal ownership is ambiguous.",
        ),
    ]

    with notify_scope():
        rows = await repo.insert_many(drafts, conn=tx_conn)

    assert [row.id for row in rows] == [situation, dependent, right, left]
    assert all(row.proposition.get("semantic_address") for row in rows)
    assert all(row.proposition.get("belief_address") for row in rows)

    address_rows = await tx_conn.fetch(
        """
        SELECT model_id, fingerprint, obligation_keys, answerable_primitives
        FROM model_belief_addresses
        WHERE tenant_id = $1
          AND model_id = ANY($2::uuid[])
        """,
        tenant,
        [situation, dependent, right, left],
    )
    assert len(address_rows) == 4
    assert all(row["fingerprint"] for row in address_rows)
    assert any(
        "COUNTEREVIDENCE" in (row["answerable_primitives"] or [])
        for row in address_rows
    )
    assert any(
        any(str(key).startswith("spo:") for key in (row["obligation_keys"] or []))
        for row in address_rows
    )

    composition_rows = await tx_conn.fetch(
        """
        SELECT member_model_id
        FROM model_composition_members
        WHERE tenant_id = $1 AND composite_model_id = $2
        """,
        tenant,
        situation,
    )
    assert {row["member_model_id"] for row in composition_rows} == {left, right}

    edge_rows = await tx_conn.fetch(
        """
        SELECT source_model_id, target_model_id, edge_kind, status
        FROM model_edges
        WHERE tenant_id = $1
          AND target_model_id = $2
        ORDER BY edge_kind
        """,
        tenant,
        dependent,
    )
    assert {
        (row["source_model_id"], row["target_model_id"], row["edge_kind"])
        for row in edge_rows
    } == {
        (left, dependent, "supports"),
        (right, dependent, "contributes_to_resolution"),
    }
    assert {row["status"] for row in edge_rows} == {"active"}

    state_changes = await tx_conn.fetchval(
        """
        SELECT count(*)::int
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'state_change'
          AND content->>'entity_kind' = 'model'
          AND (content->>'entity_id')::uuid = ANY($2::uuid[])
        """,
        tenant,
        [situation, dependent, right, left],
    )
    assert state_changes == 4


async def test_insert_many_rejects_cycle_without_partial_writes(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    left = uuid7()
    right = uuid7()

    with pytest.raises(ValidationError), notify_scope():
        await repo.insert_many(
            [
                _draft(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    model_id=left,
                    natural="Left cyclic model.",
                    supporting_model_ids=[right],
                ),
                _draft(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    model_id=right,
                    natural="Right cyclic model.",
                    supporting_model_ids=[left],
                ),
            ],
            conn=tx_conn,
        )

    count = await tx_conn.fetchval(
        "SELECT count(*)::int FROM models WHERE id = ANY($1::uuid[])",
        [left, right],
    )
    assert count == 0


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
        drafts.append(_draft(
            tenant=tenant,
            born_from_event=born_from_event,
            actor_id=actor_id,
            model_id=model_id,
            natural=natural,
            proposition=state_proposition(
                subject="Beacon renewal" if relevant else f"Noise account {idx}",
                assertion=natural,
            ),
            scope_entities=hero_scope if relevant else noise_scope,
        ))

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
        precomputed_seed_vector=make_embedding(
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
