"""End-to-end tests for the imaginary-node pattern.

These walk the full closed loop the user described in the design memo:

    audit chain discontinuity
        → detect_dynamic_signals  (substrate detector)
        → emit_missing_transition_triggers  (T3 trigger emission)
        → T3 deterministic handler  (imputer runs, hypothesis Model
                                     would be inserted via applier)
        → /v1/recommendations lists the hypothesis
        → ratify_hypothesis(action=...)  (user-facing surface)
        → for non-dismiss: T2 trigger → T2 deterministic handler →
          substrate mutation

Each test follows that chain end-to-end and asserts that the substrate
ends up in the right shape. The Think applier itself is exercised by
its own suites; this file applies the handler-returned RawDiffs
inline via a deliberately thin SQL adapter so we test *integration*,
not re-validate the applier's invariants.

The four tests fork at the ratify step — one per action — so when
something breaks the failure points at the specific lineage.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.ids import uuid7
from services.reasoning.dynamics import (
    detect_dynamic_signals,
    emit_missing_transition_triggers,
)
from services.product.recommendations.handlers import ratify_hypothesis
from services.product.recommendations.repo import list_for_actor
from services.product.recommendations.tests.conftest import seed_observation
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.deterministic import (
    USER_CORRECTED_FACT_CONFIDENCE,
    USER_RATIFIED_HYPOTHESIS_CONFIDENCE,
    USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING,
    deterministic_handler,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------
# Substrate seeders + thin RawDiff applier (test-only)
# ---------------------------------------------------------------------


async def _seed_substrate_model(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    actor_id: UUID,
    obs_id: UUID,
    model_id: UUID,
    natural: str = "Commitment to ship the dashboard rewrite by Q3",
) -> None:
    embedding = [0.0] * 768
    embedding[0] = 1.0
    await pool.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, confidence_at_assertion, falsifier,
          signal_readings, supporting_event_ids, supporting_model_ids,
          contributing_models, status, activation, last_retrieved_at
        ) VALUES (
          $1, $2, $3,
          '{"kind":"belief","subject":"commitment","assertion":"on track"}'::jsonb,
          $4, $5, ARRAY[$6]::uuid[], $7::jsonb,
          '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
          0.65, 0.65, NULL,
          '[]'::jsonb, ARRAY[$3]::uuid[], '{}'::uuid[],
          '{}'::uuid[], 'active', 0.9, now()
        )
        """,
        model_id, tenant, obs_id,
        natural, embedding, actor_id,
        json.dumps(
            [{"type": "commitment", "id": str(uuid7())}]
        ),
    )


async def _seed_discontinuity(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    actor_id: UUID,
    obs_id: UUID,
    model_id: UUID,
    now: datetime | None = None,
) -> tuple[int, int]:
    now = now or datetime.now(timezone.utc)
    prev = await pool.fetchval(
        """
        INSERT INTO audit_events (
          model_id, tenant_id, occurred_at, cause_id, cause_type,
          previous_state, new_state, changed_fields
        )
        VALUES ($1, $2, $3, $4, 'field_update',
                '{"status":"active"}'::jsonb,
                '{"status":"review"}'::jsonb,
                ARRAY['status']::text[])
        RETURNING event_id
        """,
        model_id, tenant, now - timedelta(hours=6), obs_id,
    )
    nxt = await pool.fetchval(
        """
        INSERT INTO audit_events (
          model_id, tenant_id, occurred_at, cause_id, cause_type,
          previous_state, new_state, changed_fields
        )
        VALUES ($1, $2, $3, $4, 'field_update',
                '{"status":"blocked"}'::jsonb,
                '{"status":"live"}'::jsonb,
                ARRAY['status']::text[])
        RETURNING event_id
        """,
        model_id, tenant, now - timedelta(hours=1), obs_id,
    )
    return prev, nxt


async def _apply_insert_op(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    entry: dict[str, Any],
) -> UUID:
    """Apply a ClaimOp.insert by SQL-inserting the Model. Mirrors the
    columns the substrate's INSERT pipeline writes — but without the
    falsifier-adequacy / calibration / reconciliation passes that are
    tested in services/domain/models/tests and services/reasoning/think/tests."""
    mid = uuid7()
    embedding = [0.0] * 768
    embedding[0] = 1.0
    await pool.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, confidence_at_assertion, falsifier,
          signal_readings, supporting_event_ids, supporting_model_ids,
          contributing_models, status, activation
        ) VALUES (
          $1, $2, $3, $4::jsonb, $5, $6,
          $7::uuid[], $8::jsonb, $9::jsonb,
          $10, $10, $11::jsonb,
          '[]'::jsonb, $12::uuid[], $13::uuid[],
          '{}'::uuid[], 'active', 1.0
        )
        """,
        mid, tenant, entry["born_from_event_id"],
        json.dumps(entry["proposition"]), entry["natural"],
        embedding,
        list(entry["scope_actors"]),
        json.dumps(entry["scope_entities"]),
        json.dumps(entry["scope_temporal"]),
        float(entry["confidence"]),
        json.dumps(entry["falsifier"]),
        list(entry["supporting_event_ids"]),
        list(entry["supporting_model_ids"]),
    )
    return mid


async def _apply_update_op(
    pool: asyncpg.Pool,
    *,
    model_id: UUID,
    changes: dict[str, Any],
) -> None:
    """Apply ClaimOp.update by SQL-updating allowed columns inline.
    Mirrors `_ALLOWED_MODEL_UPDATE_COLUMNS` semantics narrowly enough
    for the e2e — anything not handled here would surface as a
    KeyError at test time."""
    set_clauses: list[str] = []
    params: list[Any] = []
    i = 1
    for k, v in changes.items():
        if k == "signal_readings":
            set_clauses.append(f'"{k}" = ${i}::jsonb')
            params.append(json.dumps(v))
        elif k == "last_confirmed_at":
            # T2 handler emits an ISO string for jsonb-friendliness;
            # parse it back to datetime so asyncpg accepts it.
            set_clauses.append(f'"{k}" = ${i}')
            if isinstance(v, str):
                v = datetime.fromisoformat(v.replace("Z", "+00:00"))
            params.append(v)
        else:
            set_clauses.append(f'"{k}" = ${i}')
            params.append(v)
        i += 1
    sql = (
        f"UPDATE models SET {', '.join(set_clauses)} WHERE id = ${i}"
    )
    params.append(model_id)
    await pool.execute(sql, *params)


async def _apply_archive_op(
    pool: asyncpg.Pool,
    *,
    model_id: UUID,
    reason: str,
) -> None:
    await pool.execute(
        """
        UPDATE models
        SET status         = 'archived',
            archived_at    = now(),
            archive_reason = $2
        WHERE id = $1
        """,
        model_id, reason,
    )


async def _trigger_context_for_row(
    pool: asyncpg.Pool, trig_id: UUID,
) -> TriggerContext:
    row = await pool.fetchrow(
        """
        SELECT tenant_id, trigger_kind, trigger_subkind,
               observation_id, model_id, payload
        FROM think_trigger_queue WHERE id = $1
        """,
        trig_id,
    )
    payload = (
        json.loads(row["payload"])
        if isinstance(row["payload"], (str, bytes, bytearray))
        else row["payload"]
    )
    region_spec = payload.get("region_spec")
    if isinstance(region_spec, dict):
        ctx_region: dict[str, Any] | None = region_spec
    else:
        ctx_region = None
    # seed_signature mirrors what services/reasoning/think/worker.py builds in
    # production (payload + trigger_id).
    seed_sig = {**payload, "trigger_id": str(trig_id)}
    return TriggerContext(
        kind=row["trigger_kind"],
        tenant_id=row["tenant_id"],
        subkind=row["trigger_subkind"],
        observation_id=row["observation_id"],
        model_id=row["model_id"],
        region_spec=ctx_region,
        seed_signature=seed_sig,
    )


# ---------------------------------------------------------------------
# Shared fixture
# ---------------------------------------------------------------------


@pytest_asyncio.fixture
async def loop_setup(
    gateway_pool: asyncpg.Pool, tenant_id: UUID, seeded_actor: UUID,
):
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    source_model_id = uuid7()
    await _seed_substrate_model(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
        obs_id=obs_id, model_id=source_model_id,
    )
    await _seed_discontinuity(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
        obs_id=obs_id, model_id=source_model_id,
    )

    state = {
        "tenant_id": tenant_id,
        "actor_id": seeded_actor,
        "obs_id": obs_id,
        "source_model_id": source_model_id,
    }
    yield state

    # Best-effort cleanup. The fresh_db harness truncates anyway, but
    # explicit deletes keep test-to-test isolation cheap.
    await gateway_pool.execute(
        "DELETE FROM think_trigger_queue WHERE tenant_id = $1", tenant_id,
    )
    await gateway_pool.execute(
        "DELETE FROM audit_events WHERE tenant_id = $1", tenant_id,
    )
    await gateway_pool.execute(
        "DELETE FROM models WHERE tenant_id = $1", tenant_id,
    )


async def _drive_detect_emit_impute(
    gateway_pool: asyncpg.Pool, state: dict,
) -> UUID:
    """Run detect → emit → T3 handler → manual insert. Returns the
    newly-inserted hypothesis Model id."""
    tenant_id: UUID = state["tenant_id"]
    source_model_id: UUID = state["source_model_id"]

    # 1) Detector finds the discontinuity
    async with gateway_pool.acquire() as conn:
        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[source_model_id],
        )
    assert any(s.dynamic_kind == "missing_transition" for s in signals), (
        "detector failed to fire on substrate discontinuity"
    )

    # 2) Emitter enqueues a T3 trigger
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            emitted = await emit_missing_transition_triggers(
                conn, tenant_id=tenant_id, signals=signals,
            )
    assert len(emitted) == 1
    t3_trig_id = emitted[0]

    # 3) T3 handler computes a hypothesis ClaimOp.insert
    t3_ctx = await _trigger_context_for_row(gateway_pool, t3_trig_id)
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            t3_diff = await deterministic_handler(
                t3_ctx, ContextBundle(), conn,
            )
    assert len(t3_diff.claim_ops) == 1
    assert t3_diff.claim_ops[0].op == "insert"

    # 4) Apply the insert (test-only inline applier) → hypothesis Model
    entry = t3_diff.claim_ops[0].entry
    hyp_id = await _apply_insert_op(
        gateway_pool, tenant=tenant_id, entry=entry,
    )
    # 5) Surface in the action list
    async with gateway_pool.acquire() as conn:
        views = await list_for_actor(
            tenant_id=tenant_id,
            target_actor_id=state["actor_id"],
            conn=conn,
        )
    assert any(v.id == hyp_id for v in views), (
        "hypothesis Model not in action list after imputation"
    )
    return hyp_id


# =====================================================================
# Approve — full closed loop
# =====================================================================


async def test_e2e_loop_approve(
    gateway_pool: asyncpg.Pool, loop_setup,
) -> None:
    state = loop_setup
    hyp_id = await _drive_detect_emit_impute(gateway_pool, state)

    # 6) User clicks Approve via ratify endpoint
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            ratify = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=state["actor_id"],
                tenant_id=state["tenant_id"],
                action="approve",
                conn=conn,
            )
    assert ratify.archived is False
    assert ratify.trigger_id is not None

    # 7) T2:hypothesis_approved handler runs, returns update RawDiff
    t2_ctx = await _trigger_context_for_row(gateway_pool, ratify.trigger_id)
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            t2_diff = await deterministic_handler(
                t2_ctx, ContextBundle(), conn,
            )
    assert len(t2_diff.claim_ops) == 1
    update_op = t2_diff.claim_ops[0]
    assert update_op.op == "update"
    assert update_op.model_id == hyp_id

    # 8) Apply the update → substrate reflects user-ratified state
    await _apply_update_op(
        gateway_pool, model_id=hyp_id, changes=update_op.changes,
    )

    row = await gateway_pool.fetchrow(
        "SELECT confidence, confirmed_count, signal_readings, status "
        "FROM models WHERE id = $1",
        hyp_id,
    )
    assert row["status"] == "active"
    assert (
        USER_RATIFIED_HYPOTHESIS_CONFIDENCE
        <= float(row["confidence"])
        <= USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING
    )
    assert row["confirmed_count"] >= 1
    readings = (
        json.loads(row["signal_readings"])
        if isinstance(row["signal_readings"], (str, bytes, bytearray))
        else row["signal_readings"]
    )
    assert any(
        r.get("kind") == "ratification"
        and r.get("ratification_kind") == "hypothesis_approved"
        for r in readings
    )


# =====================================================================
# Correct — full closed loop
# =====================================================================


async def test_e2e_loop_correct(
    gateway_pool: asyncpg.Pool, loop_setup,
) -> None:
    state = loop_setup
    hyp_id = await _drive_detect_emit_impute(gateway_pool, state)

    correction = {
        "natural": (
            "What actually happened was that the team paused the "
            "commitment after the standup, not blocked it."
        ),
        "proposition_overrides": {"domain_tags": ["execution"]},
    }
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            ratify = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=state["actor_id"],
                tenant_id=state["tenant_id"],
                action="correct",
                correction=correction,
                conn=conn,
            )
    assert ratify.archived is False
    assert ratify.trigger_id is not None
    assert ratify.captured_observation_id is not None

    t2_ctx = await _trigger_context_for_row(gateway_pool, ratify.trigger_id)
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            t2_diff = await deterministic_handler(
                t2_ctx, ContextBundle(), conn,
            )
    assert len(t2_diff.claim_ops) == 2
    archive_op, insert_op = t2_diff.claim_ops
    assert archive_op.op == "archive"
    assert archive_op.model_id == hyp_id
    assert insert_op.op == "insert"

    await _apply_archive_op(
        gateway_pool, model_id=hyp_id, reason=archive_op.reason,
    )
    fact_id = await _apply_insert_op(
        gateway_pool, tenant=state["tenant_id"], entry=insert_op.entry,
    )

    # Substrate state: hypothesis archived; corrected fact-Model active.
    hyp_status = await gateway_pool.fetchval(
        "SELECT status FROM models WHERE id = $1", hyp_id,
    )
    assert hyp_status == "archived"

    fact_row = await gateway_pool.fetchrow(
        "SELECT confidence, proposition FROM models WHERE id = $1",
        fact_id,
    )
    assert float(fact_row["confidence"]) == USER_CORRECTED_FACT_CONFIDENCE
    prop = (
        json.loads(fact_row["proposition"])
        if isinstance(fact_row["proposition"], (str, bytes, bytearray))
        else fact_row["proposition"]
    )
    assert prop["was_system_hypothesis"] is True
    assert prop["lineage"]["source_hypothesis_id"] == str(hyp_id)


# =====================================================================
# Other — full closed loop
# =====================================================================


async def test_e2e_loop_other(
    gateway_pool: asyncpg.Pool, loop_setup,
) -> None:
    state = loop_setup
    hyp_id = await _drive_detect_emit_impute(gateway_pool, state)

    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            ratify = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=state["actor_id"],
                tenant_id=state["tenant_id"],
                action="other",
                explanation=(
                    "Several intertwined things happened that don't "
                    "fit cleanly into a single corrected claim."
                ),
                conn=conn,
            )
    assert ratify.archived is False
    assert ratify.trigger_id is not None
    assert ratify.captured_observation_id is not None

    t2_ctx = await _trigger_context_for_row(gateway_pool, ratify.trigger_id)
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            t2_diff = await deterministic_handler(
                t2_ctx, ContextBundle(), conn,
            )
    assert len(t2_diff.claim_ops) == 1
    archive_op = t2_diff.claim_ops[0]
    assert archive_op.op == "archive"
    assert archive_op.reason == "hypothesis_user_other"

    await _apply_archive_op(
        gateway_pool, model_id=hyp_id, reason=archive_op.reason,
    )
    hyp_status = await gateway_pool.fetchval(
        "SELECT status, archive_reason FROM models WHERE id = $1",
        hyp_id,
    )
    assert hyp_status == "archived"
    # The explanation observation lives in the substrate for future Think
    # ingest — confirm it's there.
    explanation_obs = await gateway_pool.fetchval(
        "SELECT id FROM observations WHERE id = $1",
        ratify.captured_observation_id,
    )
    assert explanation_obs is not None


# =====================================================================
# Dismiss — full closed loop (no T2 handler)
# =====================================================================


async def test_e2e_loop_dismiss(
    gateway_pool: asyncpg.Pool, loop_setup,
) -> None:
    state = loop_setup
    hyp_id = await _drive_detect_emit_impute(gateway_pool, state)

    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            ratify = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=state["actor_id"],
                tenant_id=state["tenant_id"],
                action="dismiss",
                explanation="noise",
                conn=conn,
            )
    assert ratify.archived is True
    assert ratify.trigger_id is None

    row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1",
        hyp_id,
    )
    assert row["status"] == "archived"
    assert row["archive_reason"] == "hypothesis_dismissed_by_user"

    # No Think trigger was emitted for dismiss.
    queued = await gateway_pool.fetchval(
        "SELECT count(*) FROM think_trigger_queue "
        "WHERE tenant_id = $1 AND trigger_kind = 'T2' "
        "  AND model_id = $2",
        state["tenant_id"], hyp_id,
    )
    assert queued == 0


# =====================================================================
# Re-detection idempotency: after dismiss, the SAME discontinuity must
# not loop forever re-emitting triggers. This is the protective check
# on the imaginary-node pattern's reactive design.
# =====================================================================


async def test_e2e_redetect_does_not_loop_after_dismiss(
    gateway_pool: asyncpg.Pool, loop_setup,
) -> None:
    state = loop_setup
    hyp_id = await _drive_detect_emit_impute(gateway_pool, state)

    # User dismisses the hypothesis. The substrate's discontinuity
    # (audit chain gap) is NOT corrected — only the hypothesis Model is
    # archived. A naive design re-detects the gap and re-emits forever.
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=state["actor_id"],
                tenant_id=state["tenant_id"],
                action="dismiss",
                explanation="noise",
                conn=conn,
            )

    # Re-run detect + emit. The detector still sees the gap (the
    # substrate state didn't change), but the emitter's dedup keys off
    # the previously queued T3 row while it remains pending.
    async with gateway_pool.acquire() as conn:
        signals = await detect_dynamic_signals(
            conn,
            tenant_id=state["tenant_id"],
            model_ids=[state["source_model_id"]],
        )
        # The prior T3 is still in the queue (no Think worker ran
        # against it in this test), so a re-emit is suppressed by dedup.
        async with conn.transaction():
            second = await emit_missing_transition_triggers(
                conn, tenant_id=state["tenant_id"], signals=signals,
            )
    assert second == [], (
        "emitter dedup failed: should not re-enqueue when prior "
        "T3 row is still pending"
    )

    # After explicit completion (the worker's terminal state), the
    # emitter still suppresses immediate re-fire of the same completed
    # gap under the default completed-gap suppression window. A lower
    # level trigger_emitter test covers the override path that permits
    # delayed or explicitly configured re-emission.
    await gateway_pool.execute(
        "UPDATE think_trigger_queue SET completed_at = now() "
        "WHERE tenant_id = $1 AND trigger_subkind = 'missing_transition'",
        state["tenant_id"],
    )
    async with gateway_pool.acquire() as conn:
        signals = await detect_dynamic_signals(
            conn,
            tenant_id=state["tenant_id"],
            model_ids=[state["source_model_id"]],
        )
        async with conn.transaction():
            third = await emit_missing_transition_triggers(
                conn, tenant_id=state["tenant_id"], signals=signals,
            )
    assert third == [], (
        "after dismissal and recent completion, the same substrate "
        "discontinuity should not immediately loop"
    )
