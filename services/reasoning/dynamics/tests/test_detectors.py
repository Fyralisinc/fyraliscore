from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.dynamics import (
    detect_dynamic_signals,
    fetch_missing_transition_discontinuity,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


def _embedding() -> str:
    return "[" + ",".join("0" for _ in range(768)) + "]"


async def test_detect_dynamic_signals_finds_audit_oscillation(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO actors (id, tenant_id, type, display_name, status)
            VALUES ($1, $2, 'human_internal', 'Alice', 'active')
            """,
            actor_id,
            tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, actor_id,
              content, content_text, embedding, embedding_pending, trust_tier,
              external_id, entities_mentioned
            ) VALUES (
              $1, $2, $3, 'signal', 'test', $4,
              '{}'::jsonb, 'model signal', NULL, TRUE, 'authoritative',
              $5, '[]'::jsonb
            )
            """,
            obs_id,
            tenant_id,
            now,
            actor_id,
            f"dynamic-{obs_id}",
        )
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, confidence_at_assertion, falsifier, signal_readings,
              supporting_event_ids, supporting_model_ids, contributing_models,
              status, activation, last_retrieved_at
            ) VALUES (
              $1, $2, $3,
              '{"kind":"state","subject":"x","assertion":"y"}'::jsonb,
              'oscillating model', $4, ARRAY[$5]::uuid[], '[]'::jsonb,
              '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
              0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
              '{}'::uuid[], '{}'::uuid[], 'active', 0.9, now()
            )
            """,
            model_id,
            tenant_id,
            obs_id,
            _embedding(),
            actor_id,
        )
        first_event_id = await conn.fetchval(
            """
            INSERT INTO audit_events (
              model_id, tenant_id, occurred_at, cause_id, cause_type,
              previous_state, new_state, changed_fields
            )
            VALUES (
              $1, $2, $3, $4, 'field_update',
              NULL, '{"status":"active"}'::jsonb, ARRAY['status']::text[]
            )
            RETURNING event_id
            """,
            model_id,
            tenant_id,
            now - timedelta(days=2),
            obs_id,
        )
        await conn.execute(
            """
            INSERT INTO audit_events (
              model_id, tenant_id, occurred_at, cause_id, cause_type,
              previous_state, new_state, changed_fields, re_asserts_event_id
            )
            VALUES (
              $1, $2, $3, $4, 'field_update',
              '{"status":"contested_false"}'::jsonb,
              '{"status":"active"}'::jsonb,
              ARRAY['status']::text[], $5
            )
            """,
            model_id,
            tenant_id,
            now - timedelta(days=1),
            obs_id,
            first_event_id,
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            actor_ids=[actor_id],
            reference_time=now,
        )

    assert any(s.dynamic_kind == "oscillating" for s in signals)
    assert any(str(model_id) in s.summary for s in signals)


# =====================================================================
# Missing-transition (imaginary-node) detector
#
# The substrate invariant: for consecutive audit_events on the same Model,
# `event_i.new_state == event_j.previous_state` (modulo volatile fields).
# A discontinuity means a mutation happened off-system between the two
# observed states. The detector emits a signal carrying the bracketed
# events; `fetch_missing_transition_discontinuity` reifies the latest
# discontinuity's enriched payload for the imputer.
#
# Test bar:
#   - integration: end-to-end against a seeded audit chain
#   - adversarial: race-on-occurred_at, volatile-only churn,
#     create-with-NULL-previous_state, single event, no discontinuity
#   - property-based: invariants on the pure helpers
# =====================================================================


def _seed_actor_sql() -> str:
    return (
        "INSERT INTO actors (id, tenant_id, type, display_name, status)"
        " VALUES ($1, $2, 'human_internal', 'Alice', 'active')"
    )


def _seed_observation_sql() -> str:
    return """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, actor_id,
          content, content_text, embedding, embedding_pending, trust_tier,
          external_id, entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'test', $4,
          '{}'::jsonb, 'seed observation', NULL, TRUE, 'authoritative',
          $5, '[]'::jsonb
        )
        """


def _seed_model_sql() -> str:
    return """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, confidence_at_assertion, falsifier, signal_readings,
          supporting_event_ids, supporting_model_ids, contributing_models,
          status, activation, last_retrieved_at
        ) VALUES (
          $1, $2, $3,
          '{"kind":"belief","subject":"x","assertion":"y"}'::jsonb,
          'seed model', $4, ARRAY[$5]::uuid[], '[]'::jsonb,
          '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
          0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
          '{}'::uuid[], '{}'::uuid[], 'active', 0.9, now()
        )
        """


def _embedding768() -> str:
    return "[" + ",".join("0" for _ in range(768)) + "]"


async def _seed_substrate(
    conn: asyncpg.Connection,
    tenant_id, actor_id, obs_id, model_id,
) -> None:
    await conn.execute(_seed_actor_sql(), actor_id, tenant_id)
    await conn.execute(
        _seed_observation_sql(),
        obs_id,
        tenant_id,
        datetime.now(timezone.utc),
        actor_id,
        f"detector-{obs_id}",
    )
    await conn.execute(
        _seed_model_sql(),
        model_id,
        tenant_id,
        obs_id,
        _embedding768(),
        actor_id,
    )


async def _insert_audit_event(
    conn: asyncpg.Connection,
    *,
    model_id,
    tenant_id,
    occurred_at: datetime,
    cause_id,
    cause_type: str,
    previous_state: dict | None,
    new_state: dict | None,
    changed_fields: list[str] | None = None,
    re_asserts_event_id: int | None = None,
) -> int:
    prev_json = json.dumps(previous_state) if previous_state is not None else None
    new_json = json.dumps(new_state) if new_state is not None else None
    return await conn.fetchval(
        """
        INSERT INTO audit_events (
          model_id, tenant_id, occurred_at, cause_id, cause_type,
          previous_state, new_state, changed_fields, re_asserts_event_id
        )
        VALUES (
          $1, $2, $3, $4, $5,
          $6::jsonb, $7::jsonb,
          $8::text[], $9
        )
        RETURNING event_id
        """,
        model_id, tenant_id, occurred_at, cause_id, cause_type,
        prev_json, new_json,
        changed_fields or [],
        re_asserts_event_id,
    )


# -----------------------------------------------------------------
# Integration: end-to-end discontinuity emits exactly one signal.
# -----------------------------------------------------------------


async def test_missing_transition_detects_substrate_discontinuity(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        # event_i: state = A.  event_j: previous_state = B (NOT A).
        # The substrate is supposed to maintain
        # event_i.new_state == event_j.previous_state. A != B is the
        # smoking gun for an unrecorded mutation in between.
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active", "score": 5},
            new_state={"status": "active", "score": 5, "owner": "alice"},
            changed_fields=["owner"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "blocked", "score": 5, "owner": "alice"},
            new_state={"status": "blocked", "score": 8, "owner": "alice"},
            changed_fields=["score"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    missing = [s for s in signals if s.dynamic_kind == "missing_transition"]
    assert len(missing) == 1
    signal = missing[0]
    assert signal.subject_model_ids == (model_id,)
    assert 0.0 <= signal.strength <= 1.0
    assert 0.0 <= signal.confidence <= 1.0
    # Bracketed observations land in evidence_event_ids.
    assert signal.evidence_event_ids == (obs_id, obs_id)
    # The summary cites the model id and the missing-transition framing.
    assert str(model_id) in signal.summary
    assert "discontinuity" in signal.summary or "unrecorded" in signal.summary


# -----------------------------------------------------------------
# Negative: consistent substrate (invariant holds) emits NO signal.
# -----------------------------------------------------------------


async def test_missing_transition_silent_when_substrate_consistent(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        # Substrate invariant respected: event_i.new_state ==
        # event_j.previous_state exactly. Detector must stay silent.
        state_a = {"status": "active", "score": 5}
        state_b = {"status": "blocked", "score": 5}
        state_c = {"status": "blocked", "score": 9}

        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state=state_a, new_state=state_b,
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state=state_b, new_state=state_c,
            changed_fields=["score"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    assert not [s for s in signals if s.dynamic_kind == "missing_transition"]


# -----------------------------------------------------------------
# Adversarial: confidence_update churn must not fire (excluded by
# cause_type filter — pure confidence updates aren't state mutations).
# -----------------------------------------------------------------


async def test_missing_transition_ignores_confidence_update_events(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        # Two confidence_update events with massively inconsistent state
        # JSONB. The detector excludes confidence_update from
        # consideration, so even an apparent discontinuity here must NOT
        # emit a signal.
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="confidence_update",
            previous_state={"confidence": 0.5},
            new_state={"confidence": 0.7},
            changed_fields=["confidence"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="confidence_update",
            previous_state={"confidence": 0.99, "status": "phantom"},
            new_state={"confidence": 0.95, "status": "phantom"},
            changed_fields=["confidence"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    assert not [s for s in signals if s.dynamic_kind == "missing_transition"]


# -----------------------------------------------------------------
# Adversarial: only volatile fields churn -> no false positive.
# -----------------------------------------------------------------


async def test_missing_transition_ignores_volatile_field_churn(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        # event_i.new_state and event_j.previous_state differ ONLY in
        # volatile fields. Detector must skip — these are not material
        # state mutations, they're reconsolidation noise.
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active", "activation": 0.1},
            new_state={"status": "blocked", "activation": 0.3},
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={
                "status": "blocked", "activation": 0.9,
                "last_retrieved_at": "2026-05-31T00:00:00Z",
            },
            new_state={"status": "blocked", "activation": 0.95},
            changed_fields=["activation"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    assert not [s for s in signals if s.dynamic_kind == "missing_transition"]


# -----------------------------------------------------------------
# Adversarial: single audit event -> no signal (need a pair).
# -----------------------------------------------------------------


async def test_missing_transition_silent_on_single_event(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="create",
            previous_state=None,
            new_state={"status": "active"},
            changed_fields=["status"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    assert not [s for s in signals if s.dynamic_kind == "missing_transition"]


# -----------------------------------------------------------------
# Adversarial: same-instant audit events (race). Detector must order
# deterministically by event_id and still produce the right verdict.
# -----------------------------------------------------------------


async def test_missing_transition_handles_same_instant_events(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)
    same_ts = now - timedelta(hours=2)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        # Two events at exactly the same occurred_at with an A->B->C diff
        # that does NOT preserve the substrate invariant. Detector orders
        # by (occurred_at, event_id) so the earlier event_id is the prior.
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=same_ts,
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "review"},
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=same_ts,
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "blocked"},
            new_state={"status": "done"},
            changed_fields=["status"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    missing = [s for s in signals if s.dynamic_kind == "missing_transition"]
    assert len(missing) == 1


# -----------------------------------------------------------------
# Adversarial: multiple consecutive discontinuities -> N-1 signals.
# -----------------------------------------------------------------


async def test_missing_transition_emits_signal_per_discontinuity(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=10),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "review"},
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "approved"},  # gap 1
            new_state={"status": "shipped"},
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "rolled_back"},  # gap 2
            new_state={"status": "active"},
            changed_fields=["status"],
        )

        signals = await detect_dynamic_signals(
            conn,
            tenant_id=tenant_id,
            model_ids=[model_id],
            reference_time=now,
        )

    missing = [s for s in signals if s.dynamic_kind == "missing_transition"]
    assert len(missing) == 2


# -----------------------------------------------------------------
# fetch_missing_transition_discontinuity returns LATEST gap.
# -----------------------------------------------------------------


async def test_fetch_returns_latest_discontinuity(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)

        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(days=2),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "review", "tag": "alpha"},
            changed_fields=["status", "tag"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=12),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "rolled_back", "tag": "beta"},
            new_state={"status": "live", "tag": "beta"},
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "live", "tag": "beta"},  # consistent
            new_state={"status": "live", "tag": "gamma"},
            changed_fields=["tag"],
        )

        disc = await fetch_missing_transition_discontinuity(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            since=now - timedelta(days=7),
        )

    assert disc is not None
    assert disc.model_id == model_id
    # Latest discontinuity is the one between event 1 and 2
    # (not between 2 and 3, which is consistent).
    assert "status" in disc.differing_fields
    # The detector should bracket the 2-day-old event (older=prev) and the
    # 12-hour event (newer=next).
    assert disc.prev_state == {"status": "review", "tag": "alpha"}
    assert disc.next_state == {"status": "rolled_back", "tag": "beta"}


async def test_fetch_returns_none_when_consistent(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)
        # Two events, invariant holds.
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "blocked"},
            changed_fields=["status"],
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "blocked"},
            new_state={"status": "active"},
            changed_fields=["status"],
        )

        disc = await fetch_missing_transition_discontinuity(
            conn,
            tenant_id=tenant_id,
            model_id=model_id,
            since=now - timedelta(days=7),
        )

    assert disc is None


# Property-based tests on the pure helpers (no DB) live in
# `test_detector_helpers.py` so they run without DATABASE_URL.
