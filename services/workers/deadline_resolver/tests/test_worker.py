"""Tests for services/workers/deadline_resolver.

Per BUILD-PLAN §5 Prompt 4.A, minimum 15 tests. We ship 18:

  1. observation_pattern with confirmatory obs → confirmed enqueue
  2. observation_pattern with violating obs → violated enqueue
  3. observation_pattern with no match in window → inconclusive enqueue
  4. commitment_outcome in contradicting state → violated
  5. commitment_outcome in terminal non-contradicting state → confirmed
  6. commitment_outcome still active → inconclusive
  7. prediction_deadline "Commitment C in state doneverified" (match)
     → confirmed
  8. prediction_deadline same check with wrong state → violated
  9. resource_threshold crossed → violated
 10. explicit_contestation contesting actors posted in window → violated
 11. multiple due predictions in one poll → all enqueued in one cycle
 12. already-resolved predictions (status != 'active') skipped
 13. resolver does NOT write to models — Model row unchanged; only
     think_trigger_queue mutated
 14. tenant isolation — tenant A predictions don't enqueue for tenant B
 15. idempotency: same prediction polled twice in the same minute →
     1 trigger enqueued, not 2
 16. deadline in far future — not picked up
 17. property test: random resolution_criteria + observation fixtures →
     determination is consistent (same input → same output)
 18. benchmark: 1000 due predictions processed in one cycle < 10s

No mocks. Real Postgres. Tenant isolation is the hermetic boundary.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from hypothesis import HealthCheck, given, settings, strategies as st

from lib.shared.ids import uuid7
from services.workers.deadline_resolver.evaluators import (
    EvaluationContext,
    evaluate_falsifier,
)
from services.workers.deadline_resolver.worker import (
    DEFAULT_POLL_INTERVAL_S,
    DeadlineResolver,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _past(minutes: int = 5) -> datetime:
    return datetime.now(timezone.utc) - timedelta(minutes=minutes)


def _future(days: int = 30) -> datetime:
    return datetime.now(timezone.utc) + timedelta(days=days)


async def _count_triggers(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID | None = None,
    model_id: UUID | None = None,
) -> int:
    clauses = ["trigger_kind = 'T2'", "trigger_subkind = 'prediction_overdue'"]
    params: list = []
    if tenant_id is not None:
        params.append(tenant_id)
        clauses.append(f"tenant_id = ${len(params)}")
    if model_id is not None:
        params.append(model_id)
        clauses.append(f"model_id = ${len(params)}")
    sql = (
        "SELECT count(*)::int AS n FROM think_trigger_queue WHERE "
        + " AND ".join(clauses)
    )
    async with pool.acquire() as conn:
        return await conn.fetchval(sql, *params)


async def _fetch_trigger(
    pool: asyncpg.Pool,
    *,
    model_id: UUID,
) -> asyncpg.Record | None:
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """
            SELECT id, tenant_id, trigger_kind, trigger_subkind,
                   model_id, payload
            FROM think_trigger_queue
            WHERE model_id = $1
              AND trigger_kind = 'T2'
              AND trigger_subkind = 'prediction_overdue'
            """,
            model_id,
        )


async def _setup_basic(
    resolver_db, tenant_id, seeders
) -> tuple[UUID, UUID]:
    actor_id = await seeders.actor(resolver_db, tenant_id)
    born = await seeders.observation(
        resolver_db, tenant_id, actor_id=actor_id
    )
    return actor_id, born


# =====================================================================
# 1. observation_pattern — confirmatory matches → confirmed
# =====================================================================


async def test_observation_pattern_confirmed(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    # Prediction said "alice ships prs consistently"; obs arrived in-window.
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "alice ships prs consistently every week",
        "within_window": "30 days",
        "direction": "confirms",
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
        created_at=_past(minutes=60 * 24 * 7),  # 1 week ago
    )
    # Supporting obs in-window.
    await seeders.observation(
        resolver_db, tenant_id,
        content_text="alice ships prs consistently this week",
        occurred_at=_past(minutes=60 * 24),
    )

    resolver = DeadlineResolver(resolver_db)
    result = await resolver.run_once()

    assert result.enqueued >= 1
    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    assert trig is not None
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "confirmed"
    assert payload["falsifier_kind"] == "observation_pattern"


# =====================================================================
# 2. observation_pattern — violating matches
# =====================================================================


async def test_observation_pattern_violated(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    # The pattern phrasing IS the violation itself (spec §10 example).
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "alice committed fewer than 3 PRs this period",
        "within_window": "4 weeks",
        # direction default is 'violates'
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
        created_at=_past(minutes=60 * 24 * 7),
    )
    await seeders.observation(
        resolver_db, tenant_id,
        content_text="alice committed fewer than 3 PRs this week",
        occurred_at=_past(minutes=60),
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "violated"


# =====================================================================
# 3. observation_pattern — no matches in window → inconclusive
# =====================================================================


async def test_observation_pattern_inconclusive(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    falsifier = {
        "kind": "observation_pattern",
        "pattern": "alice ships many prs consistently every week",
        "within_window": "1 day",
        "direction": "confirms",
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
        created_at=_past(minutes=60 * 24 * 2),  # 2 days ago; window is 1 day
    )
    # Obs OUTSIDE the 1-day window (fired 2 days ago).
    await seeders.observation(
        resolver_db, tenant_id,
        content_text="alice ships many prs consistently",
        occurred_at=_past(minutes=60 * 24 * 3),
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "inconclusive"


# =====================================================================
# 4. commitment_outcome — in contradicting state → violated
# =====================================================================


async def test_commitment_outcome_violated(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="blocked",
    )
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(cid),
        "contradicting_state": ["blocked", "closed"],
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "violated"


# =====================================================================
# 5. commitment_outcome — terminal non-contradicting → confirmed
# =====================================================================


async def test_commitment_outcome_confirmed(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="doneverified",
    )
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(cid),
        "contradicting_state": ["blocked", "closed"],
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "confirmed"


# =====================================================================
# 6. commitment_outcome — still active → inconclusive
# =====================================================================


async def test_commitment_outcome_inconclusive(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="active",
    )
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(cid),
        "contradicting_state": ["blocked", "closed"],
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "inconclusive"


# =====================================================================
# 7. prediction_deadline — "Commitment X in state doneverified" (match)
# =====================================================================


async def test_prediction_deadline_commitment_state_match(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="doneverified",
    )
    falsifier = {
        "kind": "prediction_deadline",
        "evaluate_at": _past().isoformat(),
        "check": f"Commitment {cid} in state doneverified",
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "confirmed"


# =====================================================================
# 8. prediction_deadline — same check, wrong state → violated
# =====================================================================


async def test_prediction_deadline_wrong_state(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="blocked",
    )
    falsifier = {
        "kind": "prediction_deadline",
        "evaluate_at": _past().isoformat(),
        "check": f"Commitment {cid} in state doneverified",
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "violated"


# =====================================================================
# 9. resource_threshold — crossed → violated
# =====================================================================


async def test_resource_threshold_violated(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    rid = await seeders.resource(
        resolver_db, tenant_id,
        kind="capacity",
        identity="eng-capacity",
        current_value={"available_capacity": 0.10},
        born_from_event_id=born,
    )
    falsifier = {
        "kind": "resource_threshold",
        "resource_ref": str(rid),
        "threshold": "available_capacity < 0.20",
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "violated"


# =====================================================================
# 10. explicit_contestation — contesting actors posted → violated
# =====================================================================


async def test_explicit_contestation_violated(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    contestor = await seeders.actor(resolver_db, tenant_id, display_name="Bob")
    falsifier = {
        "kind": "explicit_contestation",
        "contesting_actors": [str(contestor)],
        "within_days": 90,
    }
    pred_id = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier=falsifier,
        # Pin the prediction's created_at 2 days ago so the contestation
        # observation can land after it but still before "now".
        created_at=_past(minutes=60 * 24 * 2),
    )
    # Contestation observation in-window (yesterday).
    await seeders.observation(
        resolver_db, tenant_id,
        kind="contestation",
        content={"contested_model_id": str(pred_id), "reason": "wrong"},
        content_text=f"bob contested prediction {pred_id}",
        actor_id=contestor,
        occurred_at=_past(minutes=60 * 24),
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    trig = await _fetch_trigger(resolver_db, model_id=pred_id)
    payload = trig["payload"]
    if isinstance(payload, str):
        payload = json.loads(payload)
    assert payload["provisional_outcome"] == "violated"


# =====================================================================
# 11. multiple due predictions in one cycle
# =====================================================================


async def test_multiple_predictions_one_cycle(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    ids = []
    for i in range(5):
        cid = await seeders.commitment(
            resolver_db, tenant_id,
            owner_id=actor_id,
            born_from_event_id=born,
            state="doneverified",
            title=f"Ship PR {i}",
        )
        pred = await seeders.prediction(
            resolver_db, tenant_id,
            born_from_event_id=born,
            evaluate_at=_past(minutes=5 + i),
            falsifier={
                "kind": "commitment_outcome",
                "commitment_ref": str(cid),
                "contradicting_state": ["blocked"],
            },
            natural=f"prediction {i}",
        )
        ids.append(pred)

    resolver = DeadlineResolver(resolver_db)
    result = await resolver.run_once()

    assert result.enqueued >= 5
    for pid in ids:
        assert await _count_triggers(resolver_db, model_id=pid) == 1


# =====================================================================
# 12. already-resolved predictions skipped (status != 'active')
# =====================================================================


async def test_archived_prediction_skipped(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="doneverified",
    )
    pred = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier={
            "kind": "commitment_outcome",
            "commitment_ref": str(cid),
            "contradicting_state": ["blocked"],
        },
        status="archived",
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    assert await _count_triggers(resolver_db, model_id=pred) == 0


# =====================================================================
# 13. resolver does NOT write to models — only think_trigger_queue
# =====================================================================


async def test_resolver_does_not_mutate_models(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id,
        born_from_event_id=born,
        state="doneverified",
    )
    pred = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier={
            "kind": "commitment_outcome",
            "commitment_ref": str(cid),
            "contradicting_state": ["blocked"],
        },
    )
    async with resolver_db.acquire() as conn:
        before = await conn.fetchrow(
            "SELECT status, confidence, resolved_at, resolution_outcome, "
            "confirmed_count, contested_count, archived_at "
            "FROM models WHERE id = $1",
            pred,
        )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    async with resolver_db.acquire() as conn:
        after = await conn.fetchrow(
            "SELECT status, confidence, resolved_at, resolution_outcome, "
            "confirmed_count, contested_count, archived_at "
            "FROM models WHERE id = $1",
            pred,
        )
    assert dict(before) == dict(after)
    # But we DID create a trigger row.
    assert await _count_triggers(resolver_db, model_id=pred) == 1


# =====================================================================
# 14. tenant isolation
# =====================================================================


async def test_tenant_isolation(
    resolver_db, tenant_id, other_tenant_id, seeders
):
    actor_a, born_a = await _setup_basic(resolver_db, tenant_id, seeders)
    actor_b, born_b = await _setup_basic(
        resolver_db, other_tenant_id, seeders
    )
    cid_a = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_a, born_from_event_id=born_a,
        state="doneverified",
    )
    pred_a = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born_a,
        evaluate_at=_past(),
        falsifier={
            "kind": "commitment_outcome",
            "commitment_ref": str(cid_a),
            "contradicting_state": ["blocked"],
        },
    )
    cid_b = await seeders.commitment(
        resolver_db, other_tenant_id,
        owner_id=actor_b, born_from_event_id=born_b,
        state="doneverified",
    )
    pred_b = await seeders.prediction(
        resolver_db, other_tenant_id,
        born_from_event_id=born_b,
        evaluate_at=_past(),
        falsifier={
            "kind": "commitment_outcome",
            "commitment_ref": str(cid_b),
            "contradicting_state": ["blocked"],
        },
    )

    resolver = DeadlineResolver(resolver_db)
    result = await resolver.run_once()

    # Both tenants were scanned.
    assert result.tenants_scanned >= 2
    # Each tenant's trigger only got enqueued with that tenant's id.
    trig_a = await _fetch_trigger(resolver_db, model_id=pred_a)
    trig_b = await _fetch_trigger(resolver_db, model_id=pred_b)
    assert trig_a is not None
    assert trig_b is not None
    assert trig_a["tenant_id"] == tenant_id
    assert trig_b["tenant_id"] == other_tenant_id


# =====================================================================
# 15. idempotency — two runs within a minute → 1 trigger
# =====================================================================


async def test_idempotency_same_cycle(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id, born_from_event_id=born,
        state="doneverified",
    )
    pred = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_past(),
        falsifier={
            "kind": "commitment_outcome",
            "commitment_ref": str(cid),
            "contradicting_state": ["blocked"],
        },
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()
    await resolver.run_once()

    assert await _count_triggers(resolver_db, model_id=pred) == 1


# =====================================================================
# 16. deadline in far future — not picked up
# =====================================================================


async def test_future_deadline_skipped(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id, born_from_event_id=born,
        state="doneverified",
    )
    pred = await seeders.prediction(
        resolver_db, tenant_id,
        born_from_event_id=born,
        evaluate_at=_future(days=30),
        falsifier={
            "kind": "commitment_outcome",
            "commitment_ref": str(cid),
            "contradicting_state": ["blocked"],
        },
    )

    resolver = DeadlineResolver(resolver_db)
    await resolver.run_once()

    assert await _count_triggers(resolver_db, model_id=pred) == 0


# =====================================================================
# 17. property test — evaluator determinism
# =====================================================================


@settings(
    max_examples=20,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(
    state=st.sampled_from(["active", "blocked", "doneverified", "closed", "proposed"]),
    contradicting=st.sampled_from(
        [
            ["blocked"],
            ["blocked", "closed"],
            ["closed"],
            ["proposed"],
        ]
    ),
)
async def test_property_commitment_outcome_deterministic(
    resolver_db, tenant_id, seeders, state, contradicting
):
    """Same falsifier + commitment state → same outcome, every time.

    Hypothesis-driven: sample (state, contradicting) combos and verify
    the evaluator returns the spec-defined outcome.
    """
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id, born_from_event_id=born,
        state=state,
    )
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(cid),
        "contradicting_state": contradicting,
    }
    terminal = {"doneverified", "closed"}
    expected: str
    if state in contradicting:
        expected = "violated"
    elif state in terminal:
        expected = "confirmed"
    else:
        expected = "inconclusive"

    # Direct evaluator: no queue side-effects, pure DB read.
    async with resolver_db.acquire() as conn:
        ctx = EvaluationContext(
            conn=conn,
            tenant_id=tenant_id,
            prediction_id=uuid7(),
            prediction_created_at=_past(minutes=60),
        )
        # Two consecutive calls must match — determinism.
        outcome1 = await evaluate_falsifier(falsifier, ctx)
        outcome2 = await evaluate_falsifier(falsifier, ctx)
    assert outcome1 == outcome2
    assert outcome1 == expected


# =====================================================================
# 18. benchmark — 1000 due predictions in one cycle < 10s
# =====================================================================


@pytest.mark.slow
async def test_benchmark_1000_predictions(
    resolver_db, tenant_id, seeders
):
    actor_id, born = await _setup_basic(resolver_db, tenant_id, seeders)
    cid = await seeders.commitment(
        resolver_db, tenant_id,
        owner_id=actor_id, born_from_event_id=born,
        state="doneverified",
    )
    falsifier = {
        "kind": "commitment_outcome",
        "commitment_ref": str(cid),
        "contradicting_state": ["blocked"],
    }
    # 1000 predictions, all due. Bulk-insert via UNNEST so setup costs
    # milliseconds, not seconds — a short setup window makes TRUNCATE
    # victimization by parallel agents unlikely.
    N = 1000
    async with resolver_db.acquire() as conn:
        from pgvector.asyncpg import register_vector
        try:
            await register_vector(conn)
        except Exception:
            pass
        emb = seeders.det_embedding("bench")
        now = datetime.now(timezone.utc)
        eval_at = now - timedelta(minutes=5)
        ids = [uuid7() for _ in range(N)]
        # One INSERT using UNNEST to broadcast arrays. Everything except
        # `id` is constant across rows; we pass them as literals in the
        # SELECT.
        await conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, activation, falsifier,
                signal_readings, reading_contestable,
                supporting_event_ids, supporting_model_ids, evidential_weight,
                status, evaluate_at, resolution_criteria,
                contributing_models, visible_to_subjects,
                confidence_at_assertion, activation_coefficient
            )
            SELECT
                id::uuid, $2, $3,
                $4::jsonb, 'bench', $5,
                '{}'::uuid[], '[]'::jsonb, '{"type":"now"}'::jsonb,
                0.6, 1.0, $6::jsonb,
                '[]'::jsonb, TRUE,
                '{}'::uuid[], '{}'::uuid[], 0.5,
                'active', $7, NULL,
                '{}'::uuid[], TRUE,
                0.6, 1.0
            FROM UNNEST($1::uuid[]) AS t(id)
            """,
            ids,
            tenant_id,
            born,
            json.dumps({"kind": "prediction", "expected": "bench", "resolution": "x"}),
            emb,
            json.dumps(falsifier),
            eval_at,
        )

    resolver = DeadlineResolver(
        resolver_db, max_per_tenant_per_cycle=N + 100
    )
    t0 = time.perf_counter()
    result = await resolver.run_once()
    elapsed = time.perf_counter() - t0

    assert result.enqueued >= N, f"expected >= {N} enqueued; got {result.enqueued}"
    # Budget: spec says < 10s.
    assert elapsed < 10.0, f"cycle took {elapsed:.2f}s; budget 10s"


# =====================================================================
# Bonus: run/stop lifecycle smoke test
# =====================================================================


async def test_run_loop_stops_on_event(resolver_db, tenant_id, seeders):
    """Confirm run() honors stop_event and sleeps on the interval."""
    resolver = DeadlineResolver(
        resolver_db, poll_interval_s=0.05
    )
    stop = asyncio.Event()
    task = asyncio.create_task(resolver.run(stop))
    await asyncio.sleep(0.12)  # let it tick once or twice
    stop.set()
    await asyncio.wait_for(task, timeout=2.0)


def test_default_poll_interval_constant():
    assert DEFAULT_POLL_INTERVAL_S == 60
