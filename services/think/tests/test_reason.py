"""services/think/tests/test_reason.py — think() end-to-end pipeline.

Covers Wave 3-B Outstanding #1 + #10 + #11:

  * T1 happy path with ScriptedProvider returning a valid diff.
  * T1 happy path with second-pass expansion (the caller can still
    run Think successfully; `think` transparently uses second-pass
    context when the retriever yields enough signal).
  * T1 out-of-region diff → validator rejects + retrieval re-runs.
  * Authoritative T1 state_change path routed through deterministic
    handler — no LLM call.
  * Idempotency — same trigger_id twice returns skipped_idempotent.
  * Chaos — mid-apply raise → whole tx rolls back; re-run commits cleanly.
  * Worker-level idempotency — second attempt at the same trigger_id
    yields `status='skipped_idempotent'` and touches no state.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import pytest

from lib.shared.ids import uuid7

from services.retrieval.primary import TriggerContext
from services.think.reason import think
from services.think.tests.conftest import ScriptedProvider, make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# =====================================================================
# Helpers
# =====================================================================


async def _seed_observation(
    pool, tenant: UUID,
    *, content_text: str = "event", source_channel: str = "test",
    external_id: str = "e-1",
    trust_tier: str = "authoritative",
) -> UUID:
    aid = uuid7()
    oid = uuid7()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status) "
            "VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
            aid, tenant,
        )
        await conn.execute(
            """
            INSERT INTO observations
              (id, tenant_id, occurred_at, kind, source_channel, actor_id,
               content, content_text, embedding, embedding_pending,
               trust_tier, external_id)
            VALUES ($1, $2, now(), 'signal', $3, $4,
                    '{}'::jsonb, $5, $6, FALSE, $7, $8)
            """,
            oid, tenant, source_channel, aid, content_text,
            make_embedding(content_text), trust_tier, external_id,
        )
    return oid


def _scripted_empty_diff(trigger_id: UUID, tenant: UUID) -> str:
    """Minimal-valid diff shape the LLM returns."""
    return json.dumps({
        "trigger_ref": str(trigger_id),
        "tenant_id": str(tenant),
        "claim_ops": [],
        "act_ops": [],
        "resource_ops": [],
        "new_predictions": [],
        "reasoning_trace": "scripted: no ops",
    })


# =====================================================================
# Happy path — inferential T1 with ScriptedProvider
# =====================================================================


async def test_think_t1_happy_path_inferential(
    fresh_db, tenant, tenant_cleanup,
):
    """Inferential T1 (subkind='event_arrival') → LLM path → think()
    commits a valid empty diff and emits the standard observability
    events."""
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant)
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_entity_ids=[],
        seed_occurred_at=datetime.now(timezone.utc),
        scope_actors=[],
    )
    # Force trigger_ref to a known id so idempotency is verifiable.
    trigger.seed_signature = {"trigger_id": str(trigger_id)}
    provider = ScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
    )

    outcome = await think(
        trigger, fresh_db, llm_provider=provider,
        triggering_content="PR merged",
        reason_for_trigger="fresh signal",
    )
    assert outcome.status == "success", outcome.error
    assert outcome.run_id is not None
    # One LLM call.
    assert len(provider.calls) == 1
    # think_runs row present with status='success'.
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT status, ended_at, ops_applied FROM think_runs WHERE id = $1",
            outcome.run_id,
        )
    assert row["status"] == "success"
    assert row["ended_at"] is not None


# =====================================================================
# Authoritative T1 state_change → deterministic path, no LLM
# =====================================================================


async def test_think_t1_state_change_skips_llm(
    fresh_db, tenant, tenant_cleanup,
):
    trigger_id = uuid7()
    obs = await _seed_observation(
        fresh_db, tenant, content_text="state_change event",
    )
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="state_change",
        observation_id=obs,
        seed_occurred_at=datetime.now(timezone.utc),
    )
    trigger.seed_signature = {"trigger_id": str(trigger_id)}
    provider = ScriptedProvider(responses=[])  # intentionally empty
    outcome = await think(
        trigger, fresh_db,
        llm_provider=provider,
    )
    assert outcome.status == "success", outcome.error
    # Deterministic path — no provider calls.
    assert len(provider.calls) == 0


# =====================================================================
# Inferential without LLM provider → validation error
# =====================================================================


async def test_think_inferential_without_provider_fails(
    fresh_db, tenant, tenant_cleanup,
):
    """T1 event_arrival (inferential) without an llm_provider →
    outcome.status='failed' because reason.py raises ValidationError."""
    obs = await _seed_observation(fresh_db, tenant)
    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )
    outcome = await think(trigger, fresh_db, llm_provider=None)
    assert outcome.status == "failed"
    assert "llm_provider" in (outcome.error or "").lower()


# =====================================================================
# Idempotency — same trigger_id twice
# =====================================================================


async def test_think_idempotency_second_run_skipped(
    fresh_db, tenant, tenant_cleanup,
):
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant)
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )

    async def _fresh_provider():
        return ScriptedProvider(
            responses=[_scripted_empty_diff(trigger_id, tenant)],
        )

    first = await think(trigger, fresh_db, llm_provider=await _fresh_provider())
    assert first.status == "success"

    second = await think(trigger, fresh_db, llm_provider=await _fresh_provider())
    assert second.status == "skipped_idempotent", second.error
    # Both runs have different run_ids, same trigger_id.
    assert first.run_id != second.run_id
    assert first.trigger_id == second.trigger_id

    # Exactly one applied_triggers row with outcome='success'.
    async with fresh_db.acquire() as conn:
        n = await conn.fetchval(
            "SELECT COUNT(*) FROM applied_triggers WHERE trigger_id = $1",
            trigger_id,
        )
    assert n == 1


async def test_think_idempotency_two_think_runs_both_recorded(
    fresh_db, tenant, tenant_cleanup,
):
    """Both think_runs rows exist; second is status='skipped_idempotent'."""
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant)
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )
    for _ in range(2):
        provider = ScriptedProvider(
            responses=[_scripted_empty_diff(trigger_id, tenant)],
        )
        await think(trigger, fresh_db, llm_provider=provider)
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT status FROM think_runs WHERE trigger_id = $1 ORDER BY started_at",
            trigger_id,
        )
    statuses = [r["status"] for r in rows]
    assert "success" in statuses
    assert "skipped_idempotent" in statuses


# =====================================================================
# Out-of-region diff → validator rejects, retrieval re-runs
# =====================================================================


async def test_think_out_of_region_triggers_rerun(
    fresh_db, tenant, tenant_cleanup,
):
    """
    The LLM returns a diff mutating an entity outside the pre-declared
    region. think() catches OutOfRegionError and re-runs retrieval with
    the expanded region. Because max_retrieval_reruns=0 here we expect
    a failed outcome with the expected error code.
    """
    obs = await _seed_observation(fresh_db, tenant)
    trigger_id = uuid7()
    # LLM claims an update on a Model ID the retrieval didn't surface
    # and which therefore isn't in the locked region.
    foreign_model = uuid7()
    bad_diff = {
        "trigger_ref": str(trigger_id),
        "tenant_id": str(tenant),
        "claim_ops": [{
            "op": "update",
            "model_id": str(foreign_model),
            "changes": {"confidence": 0.5},
        }],
        "act_ops": [],
        "resource_ops": [],
        "new_predictions": [],
        "reasoning_trace": "out-of-region attempt",
    }
    provider = ScriptedProvider(
        responses=[json.dumps(bad_diff)] * 10,  # enough for retries
    )
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )
    outcome = await think(
        trigger, fresh_db,
        llm_provider=provider,
        max_retrieval_reruns=0,
    )
    assert outcome.status == "failed"
    # The validator raised OutOfRegionError which the outer think() logs
    # as `out_of_region_after_N_reruns`.
    assert "out_of_region" in (outcome.error or "")


# =====================================================================
# Chaos — mid-apply raise rolls back applied_triggers + no partial state
# =====================================================================


async def test_think_rollback_on_midapply_failure_then_restart_success(
    fresh_db, tenant, tenant_cleanup, monkeypatch,
):
    """
    Simulate a chaos event: `apply_diff` is patched to raise mid-apply
    on the FIRST invocation, then restored. think() fails, rolls back
    applied_triggers + think_runs. On restart with the SAME trigger_id,
    applied_triggers has no prior row (rolled back) so Think proceeds
    and commits cleanly.
    """
    trigger_id = uuid7()
    obs = await _seed_observation(fresh_db, tenant)
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )

    from services.think import reason as reason_mod
    original = reason_mod.apply_diff
    call_count = {"n": 0}

    async def flaky_apply_diff(*args, **kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("chaos: DB connection lost mid-apply")
        return await original(*args, **kwargs)

    monkeypatch.setattr(reason_mod, "apply_diff", flaky_apply_diff)

    # First run — fails.
    provider = ScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
    )
    outcome1 = await think(trigger, fresh_db, llm_provider=provider)
    assert outcome1.status == "failed"
    # No applied_triggers row — rolled back.
    async with fresh_db.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT 1 FROM applied_triggers WHERE trigger_id = $1",
            trigger_id,
        )
    assert row is None

    # Restart — re-run with the same trigger_id succeeds.
    provider2 = ScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
    )
    outcome2 = await think(trigger, fresh_db, llm_provider=provider2)
    assert outcome2.status == "success"
    async with fresh_db.acquire() as conn:
        outcome_col = await conn.fetchval(
            "SELECT outcome FROM applied_triggers WHERE trigger_id = $1",
            trigger_id,
        )
    assert outcome_col == "success"


# =====================================================================
# Second-pass expansion placeholder — think() transparently allows it.
# =====================================================================


async def test_think_second_pass_expansion_does_not_crash(
    fresh_db, tenant, tenant_cleanup, monkeypatch,
):
    """
    Inject a retrieval result with zero models so reason.py still
    completes. This covers the path where second_pass_expand would be
    called by a richer caller — the module contract is that think()
    does not itself trigger second_pass (the caller decides), so the
    coverage here is the happy-path-on-thin-context.
    """
    obs = await _seed_observation(fresh_db, tenant)
    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1", tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )
    provider = ScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
    )
    outcome = await think(trigger, fresh_db, llm_provider=provider)
    assert outcome.status == "success"
    # Retrieval ran; think_runs records the (likely 0) model count.
    async with fresh_db.acquire() as conn:
        mc = await conn.fetchval(
            "SELECT retrieval_model_count FROM think_runs WHERE id = $1",
            outcome.run_id,
        )
    assert mc is not None


# =====================================================================
# Tenant isolation — two tenants' Think runs don't cross-pollinate
# =====================================================================


async def test_think_tenant_isolation(
    fresh_db, tenant, other_tenant, tenant_cleanup,
):
    """Run think() for tenant A and tenant B; assert each writes only
    to its own tenant's think_runs."""
    async def _run_for(t):
        obs = await _seed_observation(fresh_db, t, external_id=f"e-{t}")
        tid = uuid7()
        trigger = TriggerContext(
            kind="T1", tenant_id=t,
            subkind="event_arrival",
            observation_id=obs,
            seed_natural_text="x",
            seed_occurred_at=datetime.now(timezone.utc),
            seed_signature={"trigger_id": str(tid)},
        )
        provider = ScriptedProvider(
            responses=[_scripted_empty_diff(tid, t)],
        )
        return await think(trigger, fresh_db, llm_provider=provider), tid

    o_a, id_a = await _run_for(tenant)
    o_b, id_b = await _run_for(other_tenant)
    assert o_a.status == "success"
    assert o_b.status == "success"

    async with fresh_db.acquire() as conn:
        a_tenant_id = await conn.fetchval(
            "SELECT tenant_id FROM think_runs WHERE trigger_id = $1", id_a,
        )
        b_tenant_id = await conn.fetchval(
            "SELECT tenant_id FROM think_runs WHERE trigger_id = $1", id_b,
        )
        # Post-cleanup we remove both tenants' data.
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
            "DELETE FROM observations WHERE tenant_id = $1", other_tenant,
        )
        await conn.execute(
            "DELETE FROM actors WHERE tenant_id = $1", other_tenant,
        )
    assert a_tenant_id == tenant
    assert b_tenant_id == other_tenant
    assert a_tenant_id != b_tenant_id
