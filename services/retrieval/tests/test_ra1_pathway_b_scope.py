"""
RA-1 — Pathway B scope filter tests.

Source: RETRIEVAL-DESIGN-AUDIT §3 argument 1.

Verification criteria (AUDIT-FIXES-IMPLEMENTATION-PLAN §2 RA-1):
  1. Model scoped only to an entity (not an actor) is retrieved for a
     signal mentioning that entity.
  2. Model scoped only to an actor is retrieved for a signal
     mentioning that actor.
  3. Mixed scope (actor + entity) — both paths contribute.
  4. Backward-compat: when neither event_actors nor event_entities is
     supplied, pathway B behaves as before (no scope filter).
"""
from __future__ import annotations

from uuid import UUID

import pytest

from lib.shared.ids import uuid7
from lib.shared.types import ModelCreate

from services.models.repo import ModelsRepo

from services.retrieval.pathways import pathway_b_semantic
from services.retrieval.tests._fixtures import make_embedding


pytestmark = pytest.mark.integration


async def _make_actor(conn, tenant) -> UUID:
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
        aid, tenant, f"ra1-actor-{aid}",
    )
    return aid


async def _make_commitment(conn, tenant, owner_id: UUID, born_obs: UUID) -> UUID:
    cid = uuid7()
    await conn.execute(
        """
        INSERT INTO commitments (
          id, tenant_id, title, state, owner_id, due_date,
          ambition_level, priority, external_counterparty_ref,
          created_by_event_id
        ) VALUES ($1, $2, $3, 'active', $4, NULL, 'base', 5, NULL, $5)
        """,
        cid, tenant, f"ra1-commit-{cid}", owner_id, born_obs,
    )
    return cid


async def _make_observation(conn, tenant) -> UUID:
    from datetime import datetime, timezone
    oid = uuid7()
    await conn.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            source_actor_ref, actor_id, content, content_text,
            embedding, embedding_pending, trust_tier,
            external_id, entities_mentioned
        ) VALUES (
            $1, $2, $3, 'signal', 'ra1:test', 'ra1:test', NULL,
            '{}'::jsonb, 'ra1 obs', $4, FALSE, 'authoritative',
            $5, '[]'::jsonb
        )
        """,
        oid, tenant, datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        make_embedding(f"ra1-obs-{oid}"), f"ra1-obs-{oid}",
    )
    return oid


async def _make_model(
    conn,
    tenant,
    pool,
    *,
    natural: str,
    scope_actors: list[UUID],
    scope_entities: list[dict],
) -> UUID:
    repo = ModelsRepo(pool, embedder=None)
    obs = await _make_observation(conn, tenant)
    mc = ModelCreate(
        tenant_id=tenant,
        born_from_event_id=obs,
        proposition={
            "kind": "state",
            "subject": "ra1-subject",
            "assertion": natural,
        },
        natural=natural,
        embedding=make_embedding(natural),
        scope_actors=scope_actors,
        scope_entities=scope_entities,
        scope_temporal={"type": "now"},
        confidence=0.6,
        confidence_at_assertion=0.6,
    )
    row = await repo.insert(mc, conn=conn)
    return row.id


async def test_ra1_pathway_b_entity_only_scope_retrieved(
    tx_conn, fresh_db, tenant
):
    """Model scoped to an entity (no actor scope) must surface when
    event_entities contains that entity."""
    actor = await _make_actor(tx_conn, tenant)
    obs = await _make_observation(tx_conn, tenant)
    commit_id = await _make_commitment(tx_conn, tenant, actor, obs)

    # Entity-only scope (empty scope_actors)
    entity_scope = [{"type": "commitment", "id": str(commit_id)}]
    natural = "RA1 entity-scoped model proposition text"
    mid = await _make_model(
        tx_conn, tenant, fresh_db,
        natural=natural,
        scope_actors=[],
        scope_entities=entity_scope,
    )

    vec = make_embedding(natural)
    # Event does NOT mention actor; only the entity.
    result = await pathway_b_semantic(
        natural, tenant, tx_conn,
        k=20,
        precomputed_vector=vec,
        event_actors=[],
        event_entities=[{"type": "commitment", "id": str(commit_id)}],
    )
    ids = {m.id for m in result.models}
    assert mid in ids, (
        "entity-only scoped Model was not returned via entity scope match"
    )
    assert result.notes["scope_filter"]["applied"] is True


async def test_ra1_pathway_b_actor_only_scope_retrieved(
    tx_conn, fresh_db, tenant
):
    """Model scoped to an actor (no entity scope) must surface when
    event_actors contains that actor."""
    actor = await _make_actor(tx_conn, tenant)

    natural = "RA1 actor-scoped model proposition"
    mid = await _make_model(
        tx_conn, tenant, fresh_db,
        natural=natural,
        scope_actors=[actor],
        scope_entities=[],
    )

    vec = make_embedding(natural)
    result = await pathway_b_semantic(
        natural, tenant, tx_conn,
        k=20,
        precomputed_vector=vec,
        event_actors=[actor],
        event_entities=[],
    )
    ids = {m.id for m in result.models}
    assert mid in ids, (
        "actor-only scoped Model was not returned via actor scope match"
    )


async def test_ra1_pathway_b_mixed_scope_retrieved(
    tx_conn, fresh_db, tenant
):
    """Mixed scope: entity match OR actor match both contribute.

    Create two Models:
      - A: scoped to actor only
      - B: scoped to entity only
    Event carries actor A and entity B. Both should surface.
    """
    actor_a = await _make_actor(tx_conn, tenant)
    actor_other = await _make_actor(tx_conn, tenant)
    obs = await _make_observation(tx_conn, tenant)
    commit_b = await _make_commitment(tx_conn, tenant, actor_other, obs)

    natural_a = "RA1 mixed scope actor-a model"
    natural_b = "RA1 mixed scope entity-b model"
    mid_a = await _make_model(
        tx_conn, tenant, fresh_db,
        natural=natural_a,
        scope_actors=[actor_a],
        scope_entities=[],
    )
    mid_b = await _make_model(
        tx_conn, tenant, fresh_db,
        natural=natural_b,
        scope_actors=[],
        scope_entities=[{"type": "commitment", "id": str(commit_b)}],
    )

    # Query vector neutral; use some combination of both propositions.
    vec = make_embedding("RA1 mixed scope probe")
    result = await pathway_b_semantic(
        "probe", tenant, tx_conn,
        k=50,
        precomputed_vector=vec,
        event_actors=[actor_a],
        event_entities=[{"type": "commitment", "id": str(commit_b)}],
    )
    ids = {m.id for m in result.models}
    assert mid_a in ids, "actor-scoped Model missing in mixed scope"
    assert mid_b in ids, "entity-scoped Model missing in mixed scope"


async def test_ra1_pathway_b_scope_filter_excludes_unrelated(
    tx_conn, fresh_db, tenant
):
    """Models whose scope does not overlap either dimension must be
    filtered out when scope is applied."""
    actor_a = await _make_actor(tx_conn, tenant)
    actor_other = await _make_actor(tx_conn, tenant)
    obs = await _make_observation(tx_conn, tenant)
    commit_irrel = await _make_commitment(tx_conn, tenant, actor_other, obs)

    natural = "RA1 irrelevant scope model"
    mid = await _make_model(
        tx_conn, tenant, fresh_db,
        natural=natural,
        scope_actors=[actor_other],
        scope_entities=[{"type": "commitment", "id": str(commit_irrel)}],
    )
    # Event references a different actor and different entity.
    event_actor = await _make_actor(tx_conn, tenant)
    obs2 = await _make_observation(tx_conn, tenant)
    event_commit = await _make_commitment(tx_conn, tenant, event_actor, obs2)

    vec = make_embedding(natural)
    result = await pathway_b_semantic(
        natural, tenant, tx_conn,
        k=20,
        precomputed_vector=vec,
        event_actors=[event_actor],
        event_entities=[{"type": "commitment", "id": str(event_commit)}],
    )
    ids = {m.id for m in result.models}
    assert mid not in ids, (
        "Model with non-overlapping scope leaked through scope filter"
    )


async def test_ra1_pathway_b_no_scope_args_preserves_prior_behavior(
    tx_conn, fresh_db, tenant
):
    """Backward-compat: when caller passes neither event_actors nor
    event_entities, no scope filter is applied (pre-RA-1 behavior)."""
    actor = await _make_actor(tx_conn, tenant)

    natural = "RA1 backward compat probe"
    mid = await _make_model(
        tx_conn, tenant, fresh_db,
        natural=natural,
        scope_actors=[actor],
        scope_entities=[],
    )
    vec = make_embedding(natural)
    result = await pathway_b_semantic(
        natural, tenant, tx_conn,
        k=20,
        precomputed_vector=vec,
    )
    ids = {m.id for m in result.models}
    assert mid in ids
    assert result.notes["scope_filter"]["applied"] is False
