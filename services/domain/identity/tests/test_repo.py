from __future__ import annotations

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.domain.identity import IdentityAssertionCreate, IdentityAssertionRepository


pytestmark = pytest.mark.integration


async def _seed_actor(conn: asyncpg.Connection, tenant_id, name: str):
    actor_id = uuid7()
    await conn.execute(
        """
        INSERT INTO actors (
          id, tenant_id, type, display_name, status, metadata, created_at
        ) VALUES ($1, $2, 'human_internal', $3, 'active', '{}'::jsonb, now())
        """,
        actor_id,
        tenant_id,
        name,
    )
    return actor_id


def _proposal(tenant_id, actor_id, *, kind: str = "same_as"):
    return IdentityAssertionCreate(
        tenant_id=tenant_id,
        source_identity_key="actor:stateless:slack:slack:U_AMBIGUOUS",
        source_identity_ref={
            "kind": "source_actor",
            "installation_scope": "stateless:slack",
            "source_channel": "slack",
            "source_actor_ref": "U_AMBIGUOUS",
        },
        candidate_entity_ref={"type": "actor", "id": str(actor_id)},
        assertion_kind=kind,
        confidence=0.8,
        decision_provenance={"producer": "identity-test"},
    )


async def test_candidates_remain_ambiguous_until_decided_and_projection_versions(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = IdentityAssertionRepository()
    async with fresh_db.acquire() as conn:
        first_actor = await _seed_actor(conn, tenant_id, "First")
        second_actor = await _seed_actor(conn, tenant_id, "Second")
        first = await repo.propose(_proposal(tenant_id, first_actor), conn=conn)
        second = await repo.propose(_proposal(tenant_id, second_actor), conn=conn)

        candidates = await repo.current_candidates(
            tenant_id=tenant_id,
            source_identity_key=first.source_identity_key,
            conn=conn,
        )
        assert {candidate.id for candidate in candidates} == {first.id, second.id}

        accepted_first = await repo.decide(
            first.id,
            tenant_id=tenant_id,
            decision="accepted",
            provenance={"decider": "human:test"},
            conn=conn,
        )
        assert accepted_first.status == "accepted"
        projected = await conn.fetchval(
            """
            SELECT actor_id FROM actor_identity_mappings
             WHERE tenant_id = $1 AND installation_scope = 'stateless:slack'
               AND source_channel = 'slack' AND source_actor_ref = 'U_AMBIGUOUS'
            """,
            tenant_id,
        )
        assert projected == first_actor

        accepted_second = await repo.decide(
            second.id,
            tenant_id=tenant_id,
            decision="accepted",
            provenance={"decider": "human:test", "reason": "correction"},
            conn=conn,
        )
        assert accepted_second.supersedes_assertion_id == first.id
        assert await conn.fetchval(
            "SELECT status FROM identity_assertions WHERE id = $1", first.id
        ) == "superseded"
        assert await conn.fetchval(
            """
            SELECT actor_id FROM actor_identity_mappings
             WHERE tenant_id = $1 AND installation_scope = 'stateless:slack'
               AND source_channel = 'slack' AND source_actor_ref = 'U_AMBIGUOUS'
            """,
            tenant_id,
        ) == second_actor


async def test_negative_links_and_dependents_are_preserved(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    repo = IdentityAssertionRepository()
    async with fresh_db.acquire() as conn:
        actor_id = await _seed_actor(conn, tenant_id, "Wrong Candidate")
        negative = await repo.propose(
            _proposal(tenant_id, actor_id, kind="not_same_as"), conn=conn
        )
        accepted = await repo.decide(
            negative.id,
            tenant_id=tenant_id,
            decision="accepted",
            provenance={"decider": "human:test"},
            conn=conn,
        )
        dependent_id = uuid7()
        await repo.register_dependent(
            accepted.id,
            tenant_id=tenant_id,
            dependent_kind="episode_membership",
            dependent_id=dependent_id,
            conn=conn,
        )
        await repo.register_dependent(
            accepted.id,
            tenant_id=tenant_id,
            dependent_kind="episode_membership",
            dependent_id=dependent_id,
            conn=conn,
        )
        dependents = await repo.list_dependents(
            [accepted.id], tenant_id=tenant_id, conn=conn
        )

    assert accepted.assertion_kind == "not_same_as"
    assert accepted.status == "accepted"
    assert len(dependents) == 1
    assert dependents[0]["dependent_id"] == dependent_id
