"""services/reasoning/think/tests/test_reason.py — think() end-to-end pipeline.

Covers Wave 3-B Outstanding #1 + #10 + #11:

  * T1 happy path with ScriptedProvider returning a valid diff.
  * T1 happy path with second-pass expansion (the caller can still
    run Think successfully; `think` transparently uses second-pass
    context when the retriever yields enough signal).
  * T1 hallucinated model update → validator rejects without a region retry.
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
from types import SimpleNamespace
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.reasoning.retrieval.primary import TriggerContext
import services.reasoning.think.context_planner as context_planner_mod
import services.reasoning.think.reason as reason_mod
from services.reasoning.think.reason import ThinkRunOutcome, think
from services.reasoning.think.tests.conftest import ScriptedProvider, make_embedding


async def test_narrow_inferential_transactions_are_default(monkeypatch):
    monkeypatch.delenv("THINK_NARROW_INFERENTIAL_TX", raising=False)
    assert reason_mod._narrow_inferential_transaction_enabled() is True
    monkeypatch.setenv("THINK_NARROW_INFERENTIAL_TX", "0")
    assert reason_mod._narrow_inferential_transaction_enabled() is False
    monkeypatch.setenv("THINK_NARROW_INFERENTIAL_TX", "1")
    assert reason_mod._narrow_inferential_transaction_enabled() is True


async def test_think_attempt_passes_read_pool_only_for_narrow_runs(monkeypatch):
    class FakeTransaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_args):
            return False

    class FakeConn:
        def transaction(self):
            return FakeTransaction()

    class FakeAcquire:
        async def __aenter__(self):
            return FakeConn()

        async def __aexit__(self, *_args):
            return False

    class FakePool:
        def acquire(self):
            return FakeAcquire()

    trigger = TriggerContext(kind="T1", tenant_id=uuid7())
    record = reason_mod.ThinkRunRecord(
        id=uuid7(),
        tenant_id=trigger.tenant_id,
        trigger_id=uuid7(),
        trigger_kind="T1",
    )
    seen: list[object | None] = []

    async def fake_run_once(**kwargs):
        seen.append(kwargs.get("read_pool"))
        return ThinkRunOutcome(
            run_id=record.id,
            trigger_id=record.trigger_id,
            trigger_kind=record.trigger_kind,
            status="success",
        )

    monkeypatch.setattr(reason_mod, "is_authoritative", lambda _trigger: False)
    monkeypatch.setattr(reason_mod, "_run_once", fake_run_once)

    fake_pool = FakePool()
    monkeypatch.setattr(
        reason_mod,
        "_narrow_inferential_transaction_enabled",
        lambda: True,
    )
    await reason_mod._run_think_attempt(
        fake_pool,
        trigger=trigger,
        llm_provider=None,
        embedder=None,
        access_context=None,
        triggering_content=None,
        reason_for_trigger=None,
        record=record,
        expanded_region=None,
        reason_cache={},
    )

    monkeypatch.setattr(
        reason_mod,
        "_narrow_inferential_transaction_enabled",
        lambda: False,
    )
    await reason_mod._run_think_attempt(
        fake_pool,
        trigger=trigger,
        llm_provider=None,
        embedder=None,
        access_context=None,
        triggering_content=None,
        reason_for_trigger=None,
        record=record,
        expanded_region=None,
        reason_cache={},
    )

    assert seen == [fake_pool, None]


async def test_representation_repair_payloads_prioritize_severe_audit_gaps(monkeypatch):
    monkeypatch.setenv("THINK_REPRESENTATION_REPAIR_MAX_TRIGGERS", "2")
    tenant_id = uuid7()
    trigger_id = uuid7()
    run_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=obs_id,
        model_id=model_id,
        seed_entity_ids=[{"type": "customer", "id": str(uuid7())}],
        seed_signature={"cascade_depth": 1},
    )
    audit = SimpleNamespace(
        tenant_id=tenant_id,
        trigger_id=trigger_id,
        run_id=run_id,
        trigger_kind="T1:event_batch",
        source_channels=["github:webhook", "slack:event"],
        warnings=[
            {"code": "missing_source_coverage", "message": "source gap"},
            {"code": "non_repair_warning", "message": "ignored"},
            {
                "code": "prediction_lifecycle_not_exercised",
                "message": "prediction gap",
            },
            {
                "code": "truth_pressure_absent_for_contestable_memory",
                "message": "truth gap",
            },
        ],
    )

    payloads = reason_mod._representation_repair_payloads_from_audit(trigger, audit)

    assert [payload["audit_warning_code"] for payload in payloads] == [
        "prediction_lifecycle_not_exercised",
        "truth_pressure_absent_for_contestable_memory",
    ]
    first = payloads[0]
    assert first["repair_intent"] == "exercise_prediction_lifecycle"
    assert first["source_trigger_id"] == str(trigger_id)
    assert first["source_run_id"] == str(run_id)
    assert first["observation_ids"] == [str(obs_id)]
    assert first["model_ids"] == [str(model_id)]
    assert first["cascade_depth"] == 2
    assert "github:webhook" in first["seed_natural_text"]


async def test_representation_repair_payloads_skip_justified_noise_noop(monkeypatch):
    monkeypatch.setenv("THINK_REPRESENTATION_REPAIR_MAX_TRIGGERS", "3")
    tenant_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant_id,
        observation_id=uuid7(),
    )
    audit = SimpleNamespace(
        tenant_id=tenant_id,
        trigger_id=uuid7(),
        run_id=uuid7(),
        trigger_kind="T1:event_batch",
        source_channels=["slack:noise"],
        model_adaptiveness=0,
        edge_adaptiveness=0,
        metrics={
            "context_use_grade": "justified_noop_context_used",
            "reasoning_trace": "discard_as_noise: lunch logistics only",
            "state_changes_emitted": 0,
        },
        warnings=[
            {"code": "missing_source_coverage", "message": "source gap"},
            {"code": "missing_discovered_pattern_coverage", "message": "pattern gap"},
        ],
    )

    assert reason_mod._representation_repair_payloads_from_audit(trigger, audit) == []


async def test_representation_repair_payloads_keep_material_noop_gap(monkeypatch):
    monkeypatch.setenv("THINK_REPRESENTATION_REPAIR_MAX_TRIGGERS", "1")
    tenant_id = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=tenant_id, observation_id=uuid7())
    audit = SimpleNamespace(
        tenant_id=tenant_id,
        trigger_id=uuid7(),
        run_id=uuid7(),
        trigger_kind="T1:event_batch",
        source_channels=["github:webhook"],
        model_adaptiveness=0,
        edge_adaptiveness=0,
        metrics={
            "context_use_grade": "no_selected_context",
            "reasoning_trace": "No mutation, but material evidence was unresolved.",
            "state_changes_emitted": 0,
        },
        warnings=[{"code": "missing_source_coverage", "message": "source gap"}],
    )

    payloads = reason_mod._representation_repair_payloads_from_audit(trigger, audit)

    assert len(payloads) == 1
    assert payloads[0]["audit_warning_code"] == "missing_source_coverage"


async def test_representation_repair_payloads_do_not_loop_on_repair_trigger():
    trigger = TriggerContext(kind="T4", tenant_id=uuid7(), subkind="representation_repair")
    audit = SimpleNamespace(
        trigger_id=uuid7(),
        run_id=uuid7(),
        trigger_kind="T4:representation_repair",
        warnings=[{"code": "prediction_lifecycle_not_exercised"}],
    )

    assert reason_mod._representation_repair_payloads_from_audit(trigger, audit) == []


async def test_enqueue_representation_repair_triggers_dedupes_existing(monkeypatch):
    tenant_id = uuid7()
    trigger = TriggerContext(kind="T1", tenant_id=tenant_id, observation_id=uuid7())
    audit = SimpleNamespace(
        tenant_id=tenant_id,
        trigger_id=uuid7(),
        run_id=uuid7(),
        trigger_kind="T1:event_batch",
        source_channels=[],
        warnings=[{"code": "missing_curiosity_coverage", "message": "curiosity gap"}],
    )
    existing_id = uuid7()

    class FakeConn:
        async def fetchval(self, *_args, **_kwargs):
            return existing_id

    async def fail_enqueue(*_args, **_kwargs):
        raise AssertionError("enqueue_trigger should not run for deduped repair")

    monkeypatch.setattr(reason_mod, "enqueue_trigger", fail_enqueue)

    queued = await reason_mod._enqueue_representation_repair_triggers(
        conn=FakeConn(),
        trigger=trigger,
        audit=audit,
    )

    assert queued == [
        {
            "id": str(existing_id),
            "repair_key": f"{audit.trigger_id}:missing_curiosity_coverage",
            "audit_warning_code": "missing_curiosity_coverage",
            "deduped": True,
        }
    ]


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture(autouse=True)
def _deterministic_question_planning(monkeypatch):
    """Pin inquiry question-planning to deterministic for these tests.

    Dev's reason.py refactor added an env-gated LLM question-planning step
    (services.platform.execution.inquiry, default on). These tests assert the
    *reasoning* contract (the model-update diff call), not question planning,
    so the ScriptedProvider must only see the reasoning call. Dev's LLM
    question-planning path is covered separately by
    tests/unit/think/test_question_planning_quality.py. Production keeps dev's
    default (planning on); this only scopes the test environment.
    """
    monkeypatch.setenv("INQUIRY_LLM_QUESTION_PLANNING_ENABLED", "0")


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
    ops_applied = row["ops_applied"]
    if isinstance(ops_applied, str):
        ops_applied = json.loads(ops_applied)
    assert ops_applied["think_stage_timings"]
    assert ops_applied["think_non_llm_stage_timings_ms_total"] >= 0
    assert {
        note["stage"]
        for note in ops_applied["think_stage_timings"]
        if isinstance(note, dict)
    } >= {"context_plan", "main_llm_reason", "apply_and_adjudication"}


async def test_think_noise_only_t1_fast_path_skips_retrieval_and_llm(
    fresh_db, tenant, tenant_cleanup, monkeypatch,
):
    trigger_id = uuid7()
    content_text = (
        "General operational chatter: lunch logistics, duplicated dashboard "
        "links, and a non-actionable reminder. This should not dominate memory."
    )
    obs = await _seed_observation(
        fresh_db,
        tenant,
        content_text=content_text,
        source_channel="slack:storyline-noise",
        external_id="noise-1",
    )
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_batch",
        observation_id=obs,
        observation_ids=[obs],
        seed_natural_text=(
            "Evidence window containing 1 source signal:\n"
            f"- signal: {content_text}"
        ),
        seed_signature={
            "trigger_id": str(trigger_id),
            "source_channels": ["slack:storyline-noise"],
            "batch_signal_fragments": [{"text": content_text}],
        },
    )
    provider = ScriptedProvider(responses=[_scripted_empty_diff(trigger_id, tenant)])

    async def fail_prepare_reasoning_run_state(**_kwargs):
        raise AssertionError("noise-only T1 should not run retrieval/context planning")

    monkeypatch.setattr(
        reason_mod,
        "prepare_reasoning_run_state",
        fail_prepare_reasoning_run_state,
    )

    outcome = await think(
        trigger,
        fresh_db,
        llm_provider=provider,
        triggering_content=content_text,
        reason_for_trigger="noise batch",
    )

    assert outcome.status == "success", outcome.error
    assert provider.calls == []
    async with fresh_db.acquire() as conn:
        run = await conn.fetchrow(
            """
            SELECT status, llm_latency_ms, retrieval_model_count,
                   retrieval_observation_count, validation_error_count,
                   ops_applied
            FROM think_runs
            WHERE id = $1
            """,
            outcome.run_id,
        )
        negative = await conn.fetchrow(
            """
            SELECT memory_type, signature, rejected_path, reason
            FROM negative_memory
            WHERE tenant_id = $1
            """,
            tenant,
        )

    assert run["status"] == "success"
    assert run["llm_latency_ms"] == 0
    assert run["retrieval_model_count"] == 0
    assert run["retrieval_observation_count"] == 0
    assert run["validation_error_count"] == 0
    ops_applied = run["ops_applied"]
    if isinstance(ops_applied, str):
        ops_applied = json.loads(ops_applied)
    assert ops_applied["negative_memory_inserts"] == 1
    assert negative is not None
    assert negative["memory_type"] == "noisy_path"
    signature = negative["signature"]
    if isinstance(signature, str):
        signature = json.loads(signature)
    assert signature["signal_type"] == "noise_noop"
    rejected_path = negative["rejected_path"]
    if isinstance(rejected_path, str):
        rejected_path = json.loads(rejected_path)
    assert rejected_path["route"] == "t1_noise_noop"
    assert rejected_path["observation_ids"] == [str(obs)]
    assert negative["reason"] == "noise_only_trigger_discarded_without_durable_write"


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


async def test_think_retries_deadlock_without_recording_failed_run(
    fresh_db, tenant, monkeypatch,
):
    """Deadlock/serialization failures retry the transaction boundary."""
    obs = await _seed_observation(fresh_db, tenant)
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="contention event",
        seed_signature={"trigger_id": str(obs)},
    )
    calls = 0

    async def fake_run_once(**kwargs):
        nonlocal calls
        calls += 1
        record = kwargs["record"]
        if calls == 1:
            raise asyncpg.exceptions.DeadlockDetectedError("deadlock detected")
        return ThinkRunOutcome(
            run_id=record.id,
            trigger_id=record.trigger_id,
            trigger_kind=record.trigger_kind,
            status="success",
        )

    monkeypatch.setenv("THINK_TRANSACTION_RETRY_ATTEMPTS", "1")
    monkeypatch.setattr(reason_mod, "_run_once", fake_run_once)

    outcome = await think(trigger, fresh_db, llm_provider=None)

    assert outcome.succeeded
    assert calls == 2
    async with fresh_db.acquire() as conn:
        failures = await conn.fetchval(
            """
            SELECT COUNT(*)::bigint
            FROM think_runs
            WHERE tenant_id = $1
              AND trigger_id = $2
              AND status = 'failed'
            """,
            tenant,
            obs,
        )
    assert int(failures or 0) == 0


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
# Hallucinated model reference → validator rejects without region retry
# =====================================================================


async def test_think_hallucinated_model_reference_fails_without_region_retry(
    fresh_db, tenant, tenant_cleanup,
):
    """
    The LLM returns a diff mutating a model ID that does not exist for
    this tenant. The validator should treat this as a hard invalid
    reference, not as an out-of-region retrieval expansion opportunity.
    """
    obs = await _seed_observation(fresh_db, tenant)
    trigger_id = uuid7()
    # LLM claims an update on a Model ID the tenant does not own.
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
        "reasoning_trace": "invalid model reference attempt",
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
    assert "ValidationFailure" in (outcome.error or "")
    assert outcome.exception is not None
    assert any(
        "not found" in err
        for err in getattr(outcome.exception, "context", {}).get("errors", [])
    )
    assert "out_of_region" not in (outcome.error or "")
    assert len(provider.calls) == 1


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

    from services.reasoning.think import reason as reason_mod
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


async def test_think_invokes_second_pass_when_retrieval_decision_runs(
    fresh_db, tenant, tenant_cleanup, monkeypatch,
):
    obs = await _seed_observation(fresh_db, tenant)
    trigger_id = uuid7()
    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        subkind="event_arrival",
        observation_id=obs,
        seed_natural_text="x",
        seed_occurred_at=datetime.now(timezone.utc),
        seed_signature={"trigger_id": str(trigger_id)},
    )
    provider = ScriptedProvider(
        responses=[_scripted_empty_diff(trigger_id, tenant)],
    )

    from services.reasoning.retrieval.second_pass import SecondPassDecision

    calls = {"n": 0, "dimensions": []}

    def always_run_second_pass(*args, **kwargs):
        return SecondPassDecision(
            run=True,
            trigger_condition="test_forced",
            suggested_dimensions=["supporting_evidence"],
            reason_detail={"forced": True},
        )

    async def fake_second_pass(first_result, dimensions, conn, **kwargs):
        calls["n"] += 1
        calls["dimensions"] = list(dimensions)
        first_result.notes["second_pass"] = {
            "dimensions_processed": list(dimensions)
        }
        return first_result

    # Second-pass logic moved from reason.py into context_planner in dev's
    # refactor; patch it at its new home.
    monkeypatch.setattr(
        context_planner_mod, "should_run_second_pass", always_run_second_pass
    )
    monkeypatch.setattr(
        context_planner_mod, "second_pass_expand", fake_second_pass
    )

    outcome = await think(trigger, fresh_db, llm_provider=provider)
    assert outcome.status == "success", outcome.error
    assert calls == {"n": 1, "dimensions": ["supporting_evidence"]}


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
