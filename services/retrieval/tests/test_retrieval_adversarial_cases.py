"""
Adversarial retrieval cases that exercise production invariants across
primary retrieval and individual pathways.

These are intentionally varied in size: tiny hand-built boundary tests,
medium fixture-backed scope tests, and merged primary-retrieve tests.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate

from services.models.repo import ModelsRepo
from services.retrieval.pathways import (
    pathway_a_structural,
    pathway_b_semantic,
    pathway_c_temporal,
)
from services.retrieval.primary import TriggerContext, primary_retrieve
from services.retrieval.tests._fixtures import build_fixture, make_embedding


pytestmark = pytest.mark.integration


async def _insert_actor(conn, tenant: UUID, name: str = "adv") -> UUID:
    aid = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email, status,
            metadata, created_at, last_seen_at
        ) VALUES (
            $1, $2, 'human_internal', $3, NULL, 'active',
            '{}'::jsonb, now(), NULL
        )
        """,
        aid, tenant, f"{name}-{aid}",
    )
    return aid


async def _insert_observation(
    conn,
    tenant: UUID,
    *,
    occurred_at: datetime,
    actor_id: UUID | None = None,
    text: str = "retrieval adversarial observation",
    mentions: list[dict] | None = None,
) -> UUID:
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'retrieval:adversarial',
            'retrieval:adversarial', $4, '{}'::jsonb, $5,
            $6, FALSE, 'authoritative', $7, $8::jsonb
        )
        """,
        oid,
        tenant,
        occurred_at,
        actor_id,
        text,
        make_embedding(text),
        f"retrieval-adv-{oid}",
        json.dumps(mentions or []),
    )
    return oid


async def _insert_model(
    conn,
    pool,
    tenant: UUID,
    *,
    born_from_event_id: UUID,
    natural: str,
    scope_actors: list[UUID] | None = None,
    scope_entities: list[dict] | None = None,
) -> UUID:
    repo = ModelsRepo(pool, embedder=None)
    row = await repo.insert(
        ModelCreate(
            tenant_id=tenant,
            born_from_event_id=born_from_event_id,
            proposition={
                "kind": "state",
                "subject": "retrieval-adversarial",
                "assertion": natural,
            },
            natural=natural,
            embedding=make_embedding(natural),
            scope_actors=scope_actors or [],
            scope_entities=scope_entities or [],
            scope_temporal={"type": "now"},
            confidence=0.7,
            confidence_at_assertion=0.7,
        ),
        conn=conn,
    )
    return row.id


async def test_semantic_retrieval_never_bleeds_across_tenants_with_same_text(
    tx_conn, fresh_db, tenant, other_tenant
):
    text = "same exact model text in two tenants"
    now = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    actor_a = await _insert_actor(tx_conn, tenant, "tenant-a")
    actor_b = await _insert_actor(tx_conn, other_tenant, "tenant-b")
    obs_a = await _insert_observation(
        tx_conn, tenant, occurred_at=now, actor_id=actor_a, text=text
    )
    obs_b = await _insert_observation(
        tx_conn, other_tenant, occurred_at=now, actor_id=actor_b, text=text
    )
    mid_a = await _insert_model(
        tx_conn, fresh_db, tenant,
        born_from_event_id=obs_a, natural=text,
    )
    mid_b = await _insert_model(
        tx_conn, fresh_db, other_tenant,
        born_from_event_id=obs_b, natural=text,
    )

    result = await pathway_b_semantic(
        text,
        tenant,
        tx_conn,
        k=10,
        precomputed_vector=make_embedding(text),
    )
    ids = {m.id for m in result.models}
    assert mid_a in ids
    assert mid_b not in ids
    assert all(m.tenant_id == tenant for m in result.models)


async def test_primary_semantic_scope_blocks_same_embedding_wrong_scope(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(
        tx_conn,
        tenant,
        pool=fresh_db,
        n_observations=40,
        n_models=40,
        n_commitments=12,
        n_goals=6,
        n_customers=3,
    )
    text = "scope collision identical semantic candidate"
    in_scope_commit = fs.commitment_ids[0]
    wrong_scope_commit = fs.commitment_ids[1]
    born = fs.observation_ids[0]
    in_scope = await _insert_model(
        tx_conn,
        fresh_db,
        tenant,
        born_from_event_id=born,
        natural=text,
        scope_entities=[{"type": "commitment", "id": str(in_scope_commit)}],
    )
    wrong_scope = await _insert_model(
        tx_conn,
        fresh_db,
        tenant,
        born_from_event_id=born,
        natural=text,
        scope_entities=[{"type": "commitment", "id": str(wrong_scope_commit)}],
    )

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(in_scope_commit)}],
        seed_natural_text=text,
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding(text),
    )
    result = await primary_retrieve(trigger, tx_conn, top_n=20)
    pr_b = next(p for p in result.pathway_results if p.source_pathway == "B")
    ids = {m.id for m in pr_b.models}
    assert in_scope in ids
    assert wrong_scope not in ids
    assert pr_b.notes["scope_filter"]["applied"] is True


async def test_archived_models_are_excluded_even_when_semantically_exact(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    scoped_ids = fs.scope_by_commitment[fs.hero_commitment_id]
    archived_id = scoped_ids[0]
    await tx_conn.execute(
        """
        UPDATE models
        SET status = 'archived',
            archived_at = now(),
            archive_reason = 'adversarial status filter'
        WHERE id = $1
        """,
        archived_id,
    )
    row = await tx_conn.fetchrow(
        'SELECT "natural", embedding FROM models WHERE id = $1',
        archived_id,
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[
            {"type": "commitment", "id": str(fs.hero_commitment_id)}
        ],
        seed_natural_text=row["natural"],
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=[float(x) for x in row["embedding"]],
    )
    result = await primary_retrieve(trigger, tx_conn, top_n=40)
    assert archived_id not in {m.id for m in result.models}
    for pr in result.pathway_results:
        assert archived_id not in {m.id for m in pr.models}


async def test_bad_semantic_vector_skips_but_preserves_structural_retrieval(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[
            {"type": "commitment", "id": str(fs.hero_commitment_id)}
        ],
        seed_natural_text="wrong dimensional vector",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=[0.0] * 17,
    )
    result = await primary_retrieve(trigger, tx_conn)
    assert "A" in result.notes["pathways_run"]
    assert any(s["pathway"] == "B" for s in result.notes["pathways_skipped"])
    assert result.models


async def test_pathway_a_malformed_seed_mix_uses_valid_seed(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    result = await pathway_a_structural(
        [
            None,
            {"type": "commitment", "id": "not-a-uuid"},
            {"type": "bogus", "id": str(uuid7())},
            {"id": str(uuid7())},
            {"type": "commitment", "id": str(fs.hero_commitment_id)},
        ],
        tenant,
        tx_conn,
    )
    assert result.notes["seeds_accepted"] == 1
    assert fs.hero_commitment_id in {
        c.id for c in result.acts["commitments"]
    }
    assert result.models


async def test_temporal_window_boundaries_are_inclusive(
    tx_conn, fresh_db, tenant
):
    actor = await _insert_actor(tx_conn, tenant, "temporal")
    seed = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)
    window = timedelta(minutes=30)
    before = await _insert_observation(
        tx_conn,
        tenant,
        occurred_at=seed - window - timedelta(microseconds=1),
        actor_id=actor,
        text="before temporal window",
    )
    at_start = await _insert_observation(
        tx_conn,
        tenant,
        occurred_at=seed - window,
        actor_id=actor,
        text="at temporal window start",
    )
    at_end = await _insert_observation(
        tx_conn,
        tenant,
        occurred_at=seed + window,
        actor_id=actor,
        text="at temporal window end",
    )
    after = await _insert_observation(
        tx_conn,
        tenant,
        occurred_at=seed + window + timedelta(microseconds=1),
        actor_id=actor,
        text="after temporal window",
    )

    result = await pathway_c_temporal(seed, window, tenant, tx_conn)
    ids = {o.id for o in result.observations}
    assert at_start in ids
    assert at_end in ids
    assert before not in ids
    assert after not in ids


async def test_primary_top_n_is_unique_and_bounded_across_overlapping_pathways(
    tx_conn, fresh_db, tenant
):
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[
            {"type": "commitment", "id": str(fs.hero_commitment_id)}
        ],
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("alice ships reliably"),
    )
    result = await primary_retrieve(trigger, tx_conn, top_n=7)
    ids = [m.id for m in result.models]
    assert len(ids) <= 7
    assert len(ids) == len(set(ids))
    assert set(ids).issubset(set(result.model_scores.keys()))
