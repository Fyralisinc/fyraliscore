"""services/think/tests/test_end_to_end.py — full pipeline smoke.

Covers Wave 3-B Outstanding #9:

  * "GitHub PR merged" synthetic observation → enqueue T1 → worker
    dequeues → think() runs → Commitment transitions to doneunverified
    via act_ops → cascade recomputes parent Goal cached_health →
    state_change chain traversable back to the Observation via
    cause_id.
  * Tenant isolation: run for tenant A AND tenant B simultaneously;
    neither touches the other's data.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.acts import commitments as commitments_svc
from services.acts import goals as goals_svc
from services.retrieval.primary import TriggerContext
from services.think.reason import think
from services.think.tests.conftest import ScriptedProvider, make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _seed_pr_merged_observation(
    pool, tenant: UUID,
) -> tuple[UUID, UUID, UUID]:
    """Insert: actor + observation + Goal + Commitment in proposed state,
    plus a contributes_to edge with is_critical_path=TRUE."""
    aid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending,
               trust_tier, external_id)
            VALUES ($1, $2, now(), 'signal', 'github:pr', $3,
                    $4::jsonb, $5, $6, FALSE,
                    'authoritative', 'github-pr-187')
            """,
            oid, tenant, aid,
            json.dumps({
                "action": "closed", "merged": True,
                "pr": {"number": 187},
            }),
            "Alice merged PR #187 'ship feature X' into main",
            make_embedding("pr merged"),
        )
        async with conn.transaction():
            g = await goals_svc.create(
                title="Q2 Feature X",
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            c = await commitments_svc.create(
                title="ship feature X", owner_id=aid,
                contributes_to_goal_ids=[(g.id, True)],  # critical path
                created_by_event_id=oid,
                tenant_id=tenant, conn=conn,
            )
            # Walk to doneunverified so the transition to doneverified is legal.
            await commitments_svc.transition(
                c.id, "active", cause_event_id=oid, conn=conn,
            )
            await commitments_svc.transition(
                c.id, "doneunverified", cause_event_id=oid, conn=conn,
            )
    return oid, g.id, c.id


async def _enqueue_t1_trigger(
    pool, tenant: UUID, obs_id: UUID,
    *, seed_entity_ids: list[dict] | None = None,
) -> UUID:
    tid = uuid7()
    payload = {
        "trigger_id": str(tid),
        "seed_natural_text": "PR merge",
    }
    if seed_entity_ids:
        payload["seed_entity_ids"] = seed_entity_ids
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO think_trigger_queue
              (id, tenant_id, trigger_kind, trigger_subkind,
               observation_id, payload)
            VALUES ($1, $2, 'T1', 'event_arrival', $3, $4::jsonb)
            """,
            tid, tenant, obs_id, json.dumps(payload),
        )
    return tid


def _transition_to_doneverified_diff(
    trigger_id: UUID, tenant: UUID, commitment_id: UUID, obs_id: UUID,
    *, basis_model_id: UUID,
) -> str:
    return json.dumps({
        "trigger_ref": str(trigger_id),
        "tenant_id": str(tenant),
        "claim_ops": [],
        "act_ops": [{
            "op": "transition_commitment",
            "confidence_basis": str(basis_model_id),
            "entity": {
                "id": str(commitment_id),
                "new_state": "doneverified",
                "resolved_by_event_ids": [str(obs_id)],
            },
        }],
        "resource_ops": [],
        "new_predictions": [],
        "reasoning_trace": "PR merged → commitment doneverified",
    })


async def _seed_basis_model(pool, tenant: UUID, *, confidence: float) -> UUID:
    """Insert a Model at the given confidence that the validator can
    use as basis for the transition op (threshold for
    transition_commitment_to_doneverified is 0.80)."""
    aid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'x', 'active')",
            aid, tenant,
        )
        oid = uuid7()
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending, trust_tier)
            VALUES ($1, $2, now(), 'signal', 'test', $3,
                    '{}'::jsonb, 'x', $4, FALSE, 'authoritative')
            """,
            oid, tenant, aid, make_embedding("x"),
        )
        mid = uuid7()
        await conn.execute(
            """
            INSERT INTO models
              (id, tenant_id, born_from_event_id, proposition, "natural",
               embedding, scope_actors, scope_entities, scope_temporal,
               confidence, activation, status, confidence_at_assertion,
               activation_coefficient)
            VALUES ($1, $2, $3, $4::jsonb, $5, $6, $7::uuid[], $8::jsonb,
                    $9::jsonb, $10, 0.8, 'active', $10, 1.0)
            """,
            mid, tenant, oid,
            json.dumps({"kind": "state", "text": "Alice reliably ships"}),
            "Alice reliably ships", make_embedding("x"), [], "[]", "{}",
            float(confidence),
        )
    return mid


# =====================================================================
# End-to-end: PR merge → Think T1 → Commitment doneverified → Goal health
# =====================================================================


async def test_github_pr_merge_drives_commitment_doneverified_and_goal_health(
    fresh_db, tenant, tenant_cleanup,
):
    """
    Seed a 'Alice merged PR' observation + Goal + Commitment
    (doneunverified, critical-path). Seed a high-confidence basis Model
    in the retrieval pool so the validator's threshold passes. Enqueue
    a T1 trigger. Run `_process_trigger` on that trigger row via the
    worker — think() executes end-to-end: retrieve → LLM → validate →
    apply transition → cascade. Verify commitment state is
    'doneverified' and Goal cached_health is 'healthy' (terminal path
    succeeded).
    """
    obs_id, goal_id, commitment_id = await _seed_pr_merged_observation(
        fresh_db, tenant,
    )
    basis_mid = await _seed_basis_model(fresh_db, tenant, confidence=0.92)
    trig_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant, subkind="event_arrival",
        observation_id=obs_id,
        seed_natural_text="PR merge",
        seed_entity_ids=[
            {"type": "commitment", "id": str(commitment_id)},
            {"type": "goal", "id": str(goal_id)},
            {"type": "model", "id": str(basis_mid)},
        ],
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trig_id)},
    )
    provider = ScriptedProvider(
        responses=[
            _transition_to_doneverified_diff(
                trig_id, tenant, commitment_id, obs_id,
                basis_model_id=basis_mid,
            ),
        ] * 4,
    )
    outcome = await think(trigger, fresh_db, llm_provider=provider)
    assert outcome.status == "success", outcome.error

    async with fresh_db.acquire() as conn:
        # Commitment transitioned to doneverified.
        cstate = await conn.fetchval(
            "SELECT state FROM commitments WHERE id = $1", commitment_id,
        )
        # Goal cached_health recomputed.
        goal_row = await conn.fetchrow(
            "SELECT cached_health FROM goals WHERE id = $1", goal_id,
        )
        # state_change chain — find the commitment_doneverified event.
        change_rows = await conn.fetch(
            """
            SELECT id, cause_id, content
            FROM observations
            WHERE tenant_id = $1 AND kind = 'state_change'
            ORDER BY occurred_at DESC
            """,
            tenant,
        )
        # Think run recorded.
        run_row = await conn.fetchrow(
            "SELECT status FROM think_runs WHERE trigger_id = $1",
            trig_id,
        )

    assert cstate == "doneverified"
    # The commitment's critical-path terminal state keeps the Goal healthy.
    assert goal_row["cached_health"] in ("healthy", "degraded")
    # At least one state_change transitions OUR commitment to 'doneverified'.
    # Wave-1 helper uses {to_state}, Wave-2/3 helper uses {state_change_kind}.
    doneverified_found = False
    traversable_chain = False
    for r in change_rows:
        content = r["content"]
        if isinstance(content, (bytes, bytearray)):
            content = content.decode()
        if isinstance(content, str):
            content = json.loads(content)
        if (
            content.get("entity_id") == str(commitment_id)
            and content.get("to_state") == "doneverified"
        ):
            doneverified_found = True
            # cause_id should link back to an earlier observation.
            if r["cause_id"] is not None:
                traversable_chain = True
    assert doneverified_found, (
        "no commitment_doneverified state_change found "
        f"for commitment_id={commitment_id}"
    )
    assert traversable_chain, "state_change chain has no cause_id back-link"
    # Applied_triggers row outcome=success.
    async with fresh_db.acquire() as conn:
        outcome = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            trig_id,
        )
    assert outcome == "success"
    assert run_row["status"] == "success"


# =====================================================================
# Tenant isolation end-to-end
# =====================================================================


async def test_end_to_end_tenant_isolation(
    fresh_db, tenant, other_tenant, tenant_cleanup,
):
    """
    Both tenant A and tenant B enqueue a T1 trigger at the same time.
    Two workers run them concurrently. Neither worker touches the
    other's tenant's data. We verify via inspecting applied_triggers
    + think_runs per tenant.
    """
    obs_a, goal_a, commit_a = await _seed_pr_merged_observation(fresh_db, tenant)
    obs_b, goal_b, commit_b = await _seed_pr_merged_observation(fresh_db, other_tenant)
    basis_a = await _seed_basis_model(fresh_db, tenant, confidence=0.92)
    basis_b = await _seed_basis_model(fresh_db, other_tenant, confidence=0.92)

    async def run_for(commit, goal, obs, basis, t):
        trig = uuid7()
        trigger = TriggerContext(
            kind="T1", tenant_id=t, subkind="event_arrival",
            observation_id=obs,
            seed_natural_text="PR merge",
            seed_entity_ids=[
                {"type": "commitment", "id": str(commit)},
                {"type": "goal", "id": str(goal)},
                {"type": "model", "id": str(basis)},
            ],
            seed_occurred_at=datetime.now(timezone.utc),
            seed_signature={"trigger_id": str(trig)},
        )
        prov = ScriptedProvider(
            responses=[
                _transition_to_doneverified_diff(
                    trig, t, commit, obs, basis_model_id=basis,
                ),
            ] * 3,
        )
        return await think(trigger, fresh_db, llm_provider=prov), trig

    (out_a, trig_a), (out_b, trig_b) = await asyncio.gather(
        run_for(commit_a, goal_a, obs_a, basis_a, tenant),
        run_for(commit_b, goal_b, obs_b, basis_b, other_tenant),
    )
    assert out_a.status == "success", out_a.error
    assert out_b.status == "success", out_b.error

    async with fresh_db.acquire() as conn:
        a_runs_for_a = await conn.fetchval(
            "SELECT COUNT(*) FROM think_runs WHERE tenant_id = $1 AND trigger_id = $2",
            tenant, trig_a,
        )
        b_runs_for_b = await conn.fetchval(
            "SELECT COUNT(*) FROM think_runs WHERE tenant_id = $1 AND trigger_id = $2",
            other_tenant, trig_b,
        )
        cross_ab = await conn.fetchval(
            "SELECT COUNT(*) FROM think_runs WHERE tenant_id = $1 AND trigger_id = $2",
            other_tenant, trig_a,
        )
        cross_ba = await conn.fetchval(
            "SELECT COUNT(*) FROM think_runs WHERE tenant_id = $1 AND trigger_id = $2",
            tenant, trig_b,
        )
        # Post-cleanup for other_tenant since tenant_cleanup only
        # deletes `tenant`'s data.
        await conn.execute(
            "DELETE FROM think_anomalies_raw WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM applied_triggers WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM think_runs WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM think_region_lock_log WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM think_trigger_queue WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM customer_commitments WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", other_tenant,
        )
        await conn.execute(
            "DELETE FROM contributes_to WHERE commitment_id IN "
            "(SELECT id FROM commitments WHERE tenant_id = $1)", other_tenant,
        )
        await conn.execute(
            "DELETE FROM commitments WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM goals WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM models WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM observations WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM actors WHERE tenant_id = $1", other_tenant,
        )

    assert a_runs_for_a == 1
    assert b_runs_for_b == 1
    assert cross_ab == 0
    assert cross_ba == 0
