"""
Stress coverage for the Model layer as an integrated memory substrate.

These tests intentionally compose the hot-path invariants instead of
checking isolated CRUD behavior: heterogeneous proposition inserts,
scope sidecars, generated columns, audit/state-change emission, edge
dual-writes, archive cascades, reconsolidation, topology candidates,
and tenant isolation.
"""
from __future__ import annotations

import json
import uuid
from collections import Counter
from typing import Any

import asyncpg
import pytest

from lib.shared.errors import FalsifierInadequateError, ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate, ModelRow
from services.domain.models.propositions import canonicalize_proposition
from services.domain.models.repo import ModelsRepo
from services.domain.models.tests.conftest import every_kind_proposition, make_embedding
from services.domain.observations.events import notify_scope


pytestmark = [pytest.mark.integration]


def _adequate_falsifier(label: str) -> dict[str, str]:
    return {
        "kind": "observation_pattern",
        "pattern": (
            f"Any authoritative observation showing the {label} claim is "
            "not true under its stated scope."
        ),
        "within_window": "4 weeks",
    }


def _mc(
    *,
    tenant: uuid.UUID,
    born_from_event: uuid.UUID,
    proposition: dict[str, Any],
    natural: str,
    embedding: list[float],
    confidence: float = 0.62,
    actor_id: uuid.UUID | None = None,
    **kwargs: Any,
) -> ModelCreate:
    scope_actors = kwargs.pop("scope_actors", [actor_id] if actor_id else [])
    return ModelCreate(
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition=proposition,
        natural=natural,
        embedding=embedding,
        scope_actors=scope_actors,
        scope_temporal=kwargs.pop(
            "scope_temporal",
            {"valid_from": "2026-05-24T00:00:00Z", "valid_until": None},
        ),
        confidence=confidence,
        confidence_at_assertion=kwargs.pop("confidence_at_assertion", confidence),
        **kwargs,
    )


async def _seed_actor_and_signal(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    name: str,
) -> tuple[uuid.UUID, uuid.UUID]:
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, specification_id, created_at, last_seen_at
        ) VALUES (
            $1, $2, 'human_internal', $3, $4, 'active',
            '{}'::jsonb, NULL, now(), NULL
        )
        """,
        actor_id,
        tenant,
        name,
        f"{name.lower().replace(' ', '.')}@example.com",
    )
    observation_id = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, now(), 'signal', 'test:stress',
            $3, '{}'::jsonb, $4,
            NULL, TRUE, 'authoritative',
            $5, '[]'::jsonb
        )
        """,
        observation_id,
        tenant,
        actor_id,
        f"{name} stress seed",
        f"stress-signal-{observation_id}",
    )
    return actor_id, observation_id


def _recommendation_proposition(
    *,
    target_actor_id: uuid.UUID,
    commitment_id: uuid.UUID,
) -> dict[str, Any]:
    return {
        "kind": "recommendation",
        "target_act_ref": {"type": "commitment", "id": str(commitment_id)},
        "proposed_change": {
            "operation": "transition",
            "payload": {"new_state": "paused"},
        },
        "expected_impact": 125000.0,
        "qualitative_impact": "free capacity before the renewal deadline",
        "target_actor_id": str(target_actor_id),
    }


async def _seed_commitment(
    conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    owner_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> uuid.UUID:
    commitment_id = uuid7()
    await conn.execute(
        """
        INSERT INTO commitments (
            id, tenant_id, title, description, state, owner_id,
            created_by_event_id
        ) VALUES (
            $1, $2, 'Stabilize Nimbus renewal path',
            'Stress-test commitment for model-layer recommendations',
            'active', $3, $4
        )
        """,
        commitment_id,
        tenant,
        owner_id,
        born_from_event,
    )
    return commitment_id


async def test_model_layer_stress_heterogeneous_inserts_preserve_core_invariants(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    customer_id = uuid7()
    commitment_id = await _seed_commitment(
        tx_conn,
        tenant=tenant,
        owner_id=actor_id,
        born_from_event=born_from_event,
    )
    base_propositions = [
        *every_kind_proposition(),
        _recommendation_proposition(
            target_actor_id=actor_id,
            commitment_id=commitment_id,
        ),
    ]
    propositions = [
        dict(proposition)
        for _ in range(3)
        for proposition in base_propositions
    ]
    raw_confidences = [0.05, 0.31, 0.62, 0.68, 0.95]
    inserted: list[ModelRow] = []

    with notify_scope():
        for idx, proposition in enumerate(propositions):
            confidence = raw_confidences[idx % len(raw_confidences)]
            kind = proposition["kind"]
            row = await repo.insert(
                _mc(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    proposition=proposition,
                    natural=(
                        f"{kind} stress model {idx}: Nimbus renewal revenue "
                        "is blocked by delayed SOC2 security evidence, "
                        "capacity pressure, and a deadline-bound launch."
                    ),
                    embedding=make_embedding(f"stress:{kind}:{idx}"),
                    confidence=confidence,
                    falsifier=(
                        _adequate_falsifier(kind)
                        if confidence > 0.7 else None
                    ),
                    scope_actors=[actor_id, actor_id],
                    scope_entities=[
                        {"type": "customer", "id": str(customer_id)},
                        {"type": "customer", "id": str(customer_id)},
                        {"type": "commitment", "id": str(commitment_id)},
                        {"type": "legacy-string", "id": "not-a-uuid"},
                    ],
                    reading_contestable=(idx % 2 == 0),
                    visible_to_subjects=(idx % 3 != 0),
                ),
                conn=tx_conn,
            )
            inserted.append(row)

    assert {row.proposition_kind for row in inserted} == {
        canonicalize_proposition(proposition)["kind"]
        for proposition in propositions
    }
    assert all(row.status == "active" for row in inserted)
    assert all(0.05 <= row.confidence <= 0.95 for row in inserted)
    assert [row.confidence_at_assertion for row in inserted] == [
        raw_confidences[idx % len(raw_confidences)]
        for idx in range(len(inserted))
    ]

    model_count = await tx_conn.fetchval(
        "SELECT count(*) FROM models WHERE tenant_id = $1",
        tenant,
    )
    assert model_count == len(inserted)

    kind_counts = await tx_conn.fetch(
        """
        SELECT proposition_kind, count(*)::int AS n
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
        GROUP BY proposition_kind
        """,
        tenant,
        [row.id for row in inserted],
    )
    assert {row["proposition_kind"]: row["n"] for row in kind_counts} == dict(
        Counter(
            canonicalize_proposition(proposition)["kind"]
            for _ in range(3)
            for proposition in base_propositions
        )
    )

    state_change_count = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM observations
        WHERE tenant_id = $1
          AND kind = 'state_change'
          AND content->>'state_change_kind' = 'insert_model'
          AND (content->>'entity_id')::uuid = ANY($2::uuid[])
        """,
        tenant,
        [row.id for row in inserted],
    )
    audit_count = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM audit_events
        WHERE tenant_id = $1
          AND model_id = ANY($2::uuid[])
          AND cause_type = 'create'
        """,
        tenant,
        [row.id for row in inserted],
    )
    assert state_change_count == len(inserted)
    assert audit_count == len(inserted)

    first = inserted[0]
    sidecar_actor_count = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM model_scope_actors
        WHERE tenant_id = $1 AND model_id = $2
        """,
        tenant,
        first.id,
    )
    sidecar_entity_rows = await tx_conn.fetch(
        """
        SELECT entity_type, entity_id
        FROM model_scope_entities
        WHERE tenant_id = $1 AND model_id = $2
        ORDER BY entity_type, entity_id
        """,
        tenant,
        first.id,
    )
    assert sidecar_actor_count == 1
    assert [(row["entity_type"], row["entity_id"]) for row in sidecar_entity_rows] == [
        ("commitment", commitment_id),
        ("customer", customer_id),
    ]

    found_by_scope = await repo.search_by_scope(
        tenant_id=tenant,
        scope_entities=[{"type": "customer", "id": str(customer_id)}],
        limit=100,
        conn=tx_conn,
    )
    assert {row.id for row in inserted}.issubset({row.id for row in found_by_scope})

    found_predictions = await repo.search_by_embedding(
        inserted[2].embedding,
        tenant_id=tenant,
        kind="prediction",
        k=20,
        conn=tx_conn,
    )
    assert found_predictions
    assert {row.proposition_kind for row in found_predictions} == {"prediction"}

    before_confidence = inserted[0].confidence
    await tx_conn.execute("UPDATE models SET activation = 0.40 WHERE id = $1", first.id)
    retrieved = await repo.retrieve([first.id], conn=tx_conn)
    assert retrieved[0].retrieval_count == 1
    assert retrieved[0].activation == pytest.approx(0.55)
    assert retrieved[0].confidence == pytest.approx(before_confidence)
    assert retrieved[0].confidence_at_assertion == first.confidence_at_assertion
    assert await tx_conn.fetchval(
        "SELECT activation FROM models WHERE tenant_id = $1 AND id = $2",
        tenant,
        first.id,
    ) == pytest.approx(0.40)

    with pytest.raises(FalsifierInadequateError):
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition={
                    "kind": "state",
                    "subject": "unsupported certainty",
                    "assertion": "will never fail",
                },
                natural="unsupported high-confidence certainty",
                embedding=make_embedding("stress:falsifier-required"),
                confidence=0.95,
            ),
            conn=tx_conn,
        )


async def test_model_layer_stress_edges_cycles_and_archive_cascades(
    repo: ModelsRepo,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    with notify_scope():
        supporter = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition={
                    "kind": "state",
                    "subject": "SOC2 packet",
                    "assertion": "is delayed",
                },
                natural="SOC2 packet is delayed and blocks Nimbus renewal.",
                embedding=make_embedding("stress:edge:supporter"),
            ),
            conn=tx_conn,
        )
        contributor = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition={
                    "kind": "state",
                    "subject": "capacity",
                    "assertion": "is overloaded",
                },
                natural="Capacity is overloaded before the launch deadline.",
                embedding=make_embedding("stress:edge:contributor"),
            ),
            conn=tx_conn,
        )
        dependent = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition={
                    "kind": "prediction",
                    "expected": "Nimbus launch slips",
                    "resolution": "launch status by deadline",
                },
                natural="Nimbus launch slips unless compliance and capacity unblock.",
                embedding=make_embedding("stress:edge:dependent"),
                supporting_model_ids=[supporter.id],
                contributing_models=[contributor.id],
            ),
            conn=tx_conn,
        )

    dependent_arrays = await tx_conn.fetchrow(
        """
        SELECT supporting_model_ids, contributing_models
        FROM models
        WHERE id = $1
        """,
        dependent.id,
    )
    assert dependent_arrays is not None
    assert dependent_arrays["supporting_model_ids"] == [supporter.id]
    assert dependent_arrays["contributing_models"] == [contributor.id]

    edge_rows = await tx_conn.fetch(
        """
        SELECT source_model_id, target_model_id, edge_kind, status, detected_by
        FROM model_edges
        WHERE tenant_id = $1
        ORDER BY edge_kind, source_model_id
        """,
        tenant,
    )
    assert {
        (row["source_model_id"], row["target_model_id"], row["edge_kind"])
        for row in edge_rows
    } == {
        (supporter.id, dependent.id, "supports"),
        (contributor.id, dependent.id, "contributes_to_resolution"),
    }
    assert {row["status"] for row in edge_rows} == {"active"}
    assert {row["detected_by"] for row in edge_rows} == {"llm_explicit"}

    future_id = uuid7()
    with notify_scope():
        child_that_references_future = await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                proposition={
                    "kind": "state",
                    "subject": "future dependency",
                    "assertion": "already depends on proposed future model",
                },
                natural="A future dependency already points at the pending model.",
                embedding=make_embedding("stress:edge:future-child"),
                supporting_model_ids=[future_id],
            ),
            conn=tx_conn,
        )
    with pytest.raises(ValidationError, match="cycle"):
        await repo.insert(
            _mc(
                tenant=tenant,
                born_from_event=born_from_event,
                actor_id=actor_id,
                id=future_id,
                proposition={
                    "kind": "state",
                    "subject": "future parent",
                    "assertion": "would close a cycle",
                },
                natural="The proposed future parent would close the support cycle.",
                embedding=make_embedding("stress:edge:future-parent"),
                supporting_model_ids=[child_that_references_future.id],
            ),
            conn=tx_conn,
        )

    with notify_scope():
        archived = await repo.archive(supporter.id, reason="manual", conn=tx_conn)
    assert archived.status == "archived"

    support_edge_status = await tx_conn.fetchval(
        """
        SELECT status
        FROM model_edges
        WHERE tenant_id = $1
          AND source_model_id = $2
          AND target_model_id = $3
          AND edge_kind = 'supports'
        """,
        tenant,
        supporter.id,
        dependent.id,
    )
    reeval_row = await tx_conn.fetchrow(
        """
        SELECT model_id, cause_model_id, cause_kind, processed_at
        FROM model_reeval_queue
        WHERE tenant_id = $1
          AND model_id = $2
          AND cause_model_id = $3
        """,
        tenant,
        dependent.id,
        supporter.id,
    )
    assert support_edge_status == "inert"
    assert reeval_row is not None
    assert reeval_row["cause_kind"] == "supporting_archived"
    assert reeval_row["processed_at"] is None


async def test_model_layer_stress_insert_time_topology_is_bounded_and_tenant_safe(
    fresh_db: asyncpg.Pool,
    tx_conn: asyncpg.Connection,
    tenant: uuid.UUID,
    other_tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event: uuid.UUID,
) -> None:
    topology_repo = ModelsRepo(
        fresh_db,
        embedder=None,
        run_topology_on_insert=True,
    )
    customer_id = uuid7()
    base_embedding = make_embedding("Nimbus renewal SOC2 compliance blocker")
    tenant_models: list[ModelRow] = []

    tenant_texts = [
        "Nimbus renewal revenue is blocked by delayed SOC2 security evidence.",
        "Nimbus contract trust is at risk because compliance audit is late.",
        "Nimbus launch delivery depends on blocked security approval.",
        "Nimbus champion confidence is dropping before the renewal deadline.",
        "Nimbus support capacity is overloaded by the audit response backlog.",
        "Nimbus expansion forecast worsens if legal approval slips.",
        "Nimbus roadmap launch cannot proceed until privacy evidence lands.",
        "Nimbus escalation requires a decision on security owner bandwidth.",
        "Nimbus invoice timing is at risk from the delayed compliance packet.",
        "Nimbus customer trust improves if audit evidence unlocks launch.",
    ]

    with notify_scope():
        for idx, text in enumerate(tenant_texts):
            row = await topology_repo.insert(
                _mc(
                    tenant=tenant,
                    born_from_event=born_from_event,
                    actor_id=actor_id,
                    proposition={
                        "kind": "concern",
                        "about": "Nimbus renewal",
                        "nature": text,
                        "raised_by": "stress-suite",
                    },
                    natural=text,
                    embedding=base_embedding,
                    confidence=0.68,
                    scope_entities=[{"type": "customer", "id": str(customer_id)}],
                    scope_temporal={
                        "valid_from": "2026-05-24T00:00:00Z",
                        "valid_until": "2026-06-15T00:00:00Z",
                    },
                ),
                conn=tx_conn,
            )
            tenant_models.append(row)

    other_actor, other_event = await _seed_actor_and_signal(
        tx_conn,
        tenant=other_tenant,
        name="Other Tenant Alice",
    )
    other_models: list[ModelRow] = []
    with notify_scope():
        for idx in range(4):
            row = await topology_repo.insert(
                _mc(
                    tenant=other_tenant,
                    born_from_event=other_event,
                    actor_id=other_actor,
                    proposition={
                        "kind": "concern",
                        "about": "Nimbus renewal",
                        "nature": f"Other tenant blocker {idx}",
                        "raised_by": "stress-suite",
                    },
                    natural=(
                        "Nimbus renewal revenue is blocked by delayed SOC2 "
                        f"security evidence in another tenant {idx}."
                    ),
                    embedding=base_embedding,
                    confidence=0.68,
                    scope_entities=[{"type": "customer", "id": str(customer_id)}],
                ),
                conn=tx_conn,
            )
            other_models.append(row)

    tenant_ids = [row.id for row in tenant_models]
    other_ids = [row.id for row in other_models]
    candidate_rows = await tx_conn.fetch(
        """
        SELECT id, candidate_kind, source_model_id, target_model_id,
               member_model_ids, judgment_leverage_score, source, basis,
               metadata
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND source = 'latent_topology'
        ORDER BY judgment_leverage_score DESC, created_at DESC
        """,
        tenant,
    )
    assert len(candidate_rows) >= 3
    assert len(candidate_rows) <= 8 * len(tenant_models)
    assert all(
        row["basis"] in ("topology_suggested", "causal_hypothesis", "ontology_gap")
        for row in candidate_rows
    )
    assert all(0.0 <= row["judgment_leverage_score"] <= 1.0 for row in candidate_rows)

    tenant_id_set = set(tenant_ids)
    for row in candidate_rows:
        metadata = row["metadata"]
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        assert metadata["topology"]["kind"] == "latent_relationship_field"
        if row["candidate_kind"] == "edge":
            assert row["source_model_id"] in tenant_id_set
            assert row["target_model_id"] in tenant_id_set
        else:
            assert set(row["member_model_ids"]).issubset(tenant_id_set)

    cross_tenant_leaks = await tx_conn.fetchval(
        """
        SELECT count(*)
        FROM relationship_candidates
        WHERE tenant_id = $1
          AND (
            source_model_id = ANY($2::uuid[])
            OR target_model_id = ANY($2::uuid[])
            OR member_model_ids && $2::uuid[]
          )
        """,
        tenant,
        other_ids,
    )
    assert cross_tenant_leaks == 0

    trigger_rows = await tx_conn.fetch(
        """
        SELECT payload
        FROM think_trigger_queue
        WHERE tenant_id = $1
          AND trigger_kind = 'T4'
          AND trigger_subkind = 'latent_relationship_candidate'
        """,
        tenant,
    )
    queued_candidate_ids = {
        str(row["id"])
        for row in await tx_conn.fetch(
            """
            SELECT id
            FROM relationship_candidates
            WHERE tenant_id = $1
              AND source IN ('latent_topology', 'relationship_candidate_service')
            """,
            tenant,
        )
    }
    for row in trigger_rows:
        payload = row["payload"]
        if isinstance(payload, str):
            payload = json.loads(payload)
        assert payload["relationship_candidate_id"] in queued_candidate_ids

    found = await topology_repo.search_by_embedding(
        base_embedding,
        tenant_id=tenant,
        k=30,
        conn=tx_conn,
    )
    assert {row.id for row in found}.issubset(tenant_id_set)
    assert not ({row.id for row in found} & set(other_ids))
