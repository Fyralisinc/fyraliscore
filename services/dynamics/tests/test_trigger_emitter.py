"""Integration tests for the missing-transition T3 trigger emitter.

The emitter is the bridge between the substrate detector (signals) and
the Think queue (trigger rows). These tests confirm:
  - The right T3:missing_transition row is enqueued with the right
    payload shape (region_spec + seed_entity_ids + seed_model_ids).
  - Idempotency: a second emit for the same (model, prev_event_id) is
    a silent no-op rather than a duplicate row.
  - Completion semantics: completed triggers don't block re-emission
    when a new discontinuity appears.
  - Non-missing_transition signals are silently ignored.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.dynamics import (
    DynamicSignal,
    T3_MISSING_TRANSITION_SUBKIND,
    detect_dynamic_signals,
    emit_missing_transition_triggers,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------
# Fixture helpers (mirror test_detectors.py to keep coupling local).
# ---------------------------------------------------------------------


def _embedding768() -> str:
    return "[" + ",".join("0" for _ in range(768)) + "]"


async def _seed_substrate(
    conn: asyncpg.Connection,
    tenant_id, actor_id, obs_id, model_id,
) -> None:
    await conn.execute(
        "INSERT INTO actors (id, tenant_id, type, display_name, status)"
        " VALUES ($1, $2, 'human_internal', 'Alice', 'active')",
        actor_id, tenant_id,
    )
    await conn.execute(
        """
        INSERT INTO observations (
          id, tenant_id, occurred_at, kind, source_channel, actor_id,
          content, content_text, embedding, embedding_pending, trust_tier,
          external_id, entities_mentioned
        ) VALUES (
          $1, $2, $3, 'signal', 'test', $4,
          '{}'::jsonb, 'seed observation', NULL, TRUE, 'authoritative',
          $5, '[]'::jsonb
        )
        """,
        obs_id, tenant_id, datetime.now(timezone.utc), actor_id,
        f"emit-{obs_id}",
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
          '{"kind":"belief","subject":"x","assertion":"y"}'::jsonb,
          'seed model', $4, ARRAY[$5]::uuid[], '[]'::jsonb,
          '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
          0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
          '{}'::uuid[], '{}'::uuid[], 'active', 0.9, now()
        )
        """,
        model_id, tenant_id, obs_id, _embedding768(), actor_id,
    )


async def _insert_audit_event(
    conn,
    *,
    model_id, tenant_id,
    occurred_at: datetime,
    cause_id,
    cause_type: str,
    previous_state: dict | None,
    new_state: dict | None,
    changed_fields: list[str] | None = None,
) -> int:
    return await conn.fetchval(
        """
        INSERT INTO audit_events (
          model_id, tenant_id, occurred_at, cause_id, cause_type,
          previous_state, new_state, changed_fields
        )
        VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7::jsonb, $8::text[])
        RETURNING event_id
        """,
        model_id, tenant_id, occurred_at, cause_id, cause_type,
        json.dumps(previous_state) if previous_state is not None else None,
        json.dumps(new_state) if new_state is not None else None,
        changed_fields or [],
    )


async def _seed_discontinuity(
    conn, tenant_id, actor_id, obs_id, model_id, now: datetime,
) -> tuple[int, int]:
    """Seed a Model with a 2-event audit chain whose snapshots disagree
    in the substrate-invariant sense. Returns (prev_event_id,
    next_event_id)."""
    await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)
    prev = await _insert_audit_event(
        conn,
        model_id=model_id, tenant_id=tenant_id,
        occurred_at=now - timedelta(hours=6),
        cause_id=obs_id, cause_type="field_update",
        previous_state={"status": "active"},
        new_state={"status": "review"},
        changed_fields=["status"],
    )
    nxt = await _insert_audit_event(
        conn,
        model_id=model_id, tenant_id=tenant_id,
        occurred_at=now - timedelta(hours=1),
        cause_id=obs_id, cause_type="field_update",
        previous_state={"status": "blocked"},   # discontinuity
        new_state={"status": "live"},
        changed_fields=["status"],
    )
    return prev, nxt


# =====================================================================
# Happy path: detected signal → enqueued trigger row
# =====================================================================


async def test_emit_enqueues_trigger_for_missing_transition(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        prev_event_id, next_event_id = await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        signals = await detect_dynamic_signals(
            conn, tenant_id=tenant_id, model_ids=[model_id],
            reference_time=now,
        )
        emitted = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=signals,
            reference_time=now,
        )

        assert len(emitted) == 1
        trig_id = emitted[0]

        row = await conn.fetchrow(
            """
            SELECT trigger_kind, trigger_subkind, model_id, payload,
                   completed_at, observation_id
            FROM think_trigger_queue
            WHERE id = $1
            """,
            trig_id,
        )

    assert row is not None
    assert row["trigger_kind"] == "T3"
    assert row["trigger_subkind"] == T3_MISSING_TRANSITION_SUBKIND
    assert row["model_id"] == model_id
    assert row["completed_at"] is None
    payload = (
        json.loads(row["payload"])
        if isinstance(row["payload"], (str, bytes, bytearray))
        else row["payload"]
    )
    assert payload["seed_model_ids"] == [str(model_id)]
    assert payload["seed_entity_ids"] == [
        {"type": "model", "id": str(model_id)}
    ]
    region = payload["region_spec"]
    assert region["anomaly_kind"] == "missing_transition"
    assert region["prev_event_id"] == prev_event_id
    assert region["next_event_id"] == next_event_id
    assert region["differing_fields"] == ["status"]
    assert region["gap_seconds"] > 0


# =====================================================================
# Idempotency: second emit for the same gap is silent
# =====================================================================


async def test_emit_is_idempotent_against_pending_queue_row(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        signals = await detect_dynamic_signals(
            conn, tenant_id=tenant_id, model_ids=[model_id],
            reference_time=now,
        )
        first = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=signals,
            reference_time=now,
        )
        second = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=signals,
            reference_time=now,
        )

        rows = await conn.fetch(
            """
            SELECT id FROM think_trigger_queue
            WHERE tenant_id = $1 AND model_id = $2
              AND trigger_subkind = $3
            """,
            tenant_id, model_id, T3_MISSING_TRANSITION_SUBKIND,
        )

    assert len(first) == 1
    assert second == []
    assert len(rows) == 1, "dedup must not have created a second row"


# =====================================================================
# Completed triggers should NOT block re-emission of fresh discontinuities
# =====================================================================


async def test_emit_allows_new_trigger_after_completion(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        signals = await detect_dynamic_signals(
            conn, tenant_id=tenant_id, model_ids=[model_id],
            reference_time=now,
        )
        first = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=signals,
            reference_time=now,
        )
        # Mark the trigger completed.
        await conn.execute(
            "UPDATE think_trigger_queue SET completed_at = now() "
            "WHERE id = $1",
            first[0],
        )
        # Now re-emit: dedup keys off completed_at IS NULL, so a fresh
        # discontinuity (e.g., re-detected because the prior was never
        # resolved by ratification) should emit again.
        second = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=signals,
            reference_time=now,
        )

        rows = await conn.fetch(
            "SELECT id, completed_at FROM think_trigger_queue "
            "WHERE tenant_id = $1 AND model_id = $2",
            tenant_id, model_id,
        )

    assert len(first) == 1
    assert len(second) == 1
    assert first[0] != second[0]
    assert len(rows) == 2


# =====================================================================
# Non-missing_transition signals are silently ignored
# =====================================================================


async def test_emit_ignores_other_signal_kinds(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(
            conn, tenant_id, actor_id, obs_id, model_id,
        )
        # Bare DynamicSignal envelopes for the kinds the emitter must
        # ignore. The emitter doesn't query the substrate for these so
        # no audit fixtures are needed.
        signals = [
            DynamicSignal(
                dynamic_kind="oscillating",
                summary="x",
                strength=0.8,
                confidence=0.7,
                subject_model_ids=(model_id,),
            ),
            DynamicSignal(
                dynamic_kind="stale",
                summary="y",
                strength=0.4,
                confidence=0.6,
                subject_model_ids=(model_id,),
            ),
            DynamicSignal(
                dynamic_kind="phase_shift",
                summary="z",
                strength=0.5,
                confidence=0.6,
                subject_model_ids=(model_id,),
            ),
        ]
        emitted = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=signals,
            reference_time=now,
        )

        count = await conn.fetchval(
            "SELECT count(*) FROM think_trigger_queue WHERE tenant_id = $1",
            tenant_id,
        )

    assert emitted == []
    assert count == 0


# =====================================================================
# Empty / pathological inputs
# =====================================================================


async def test_emit_returns_empty_for_no_signals(
    fresh_db: asyncpg.Pool,
) -> None:
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        emitted = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=[],
        )
    assert emitted == []


async def test_emit_skips_signal_with_no_subject_model(
    fresh_db: asyncpg.Pool,
) -> None:
    """A malformed signal with empty subject_model_ids must be skipped
    rather than raising — the emitter is on the Think hot path."""
    tenant_id = uuid7()
    async with fresh_db.acquire() as conn:
        emitted = await emit_missing_transition_triggers(
            conn,
            tenant_id=tenant_id,
            signals=[
                DynamicSignal(
                    dynamic_kind="missing_transition",
                    summary="x",
                    strength=0.5,
                    confidence=0.5,
                    subject_model_ids=(),  # malformed
                )
            ],
        )
    assert emitted == []


async def test_emit_skips_when_discontinuity_already_resolved(
    fresh_db: asyncpg.Pool,
) -> None:
    """Race condition: between when the signal was produced and when the
    emitter runs `fetch_missing_transition_discontinuity`, a corrective
    audit event landed and closed the gap. Emitter must skip silently."""
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        # Seed consistent-substrate Model so the enriched-discontinuity
        # fetch returns None.
        await _seed_substrate(
            conn, tenant_id, actor_id, obs_id, model_id,
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "review"},
        )
        await _insert_audit_event(
            conn,
            model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "review"},   # consistent
            new_state={"status": "live"},
        )
        # Hand-craft a stale missing_transition signal even though the
        # substrate is now consistent.
        signal = DynamicSignal(
            dynamic_kind="missing_transition",
            summary="stale signal",
            strength=0.7, confidence=0.5,
            subject_model_ids=(model_id,),
        )
        emitted = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=[signal],
            reference_time=now,
        )

    assert emitted == []


# =====================================================================
# Within-batch dedup
# =====================================================================


async def test_emit_dedupes_within_signal_batch(
    fresh_db: asyncpg.Pool,
) -> None:
    """A signal batch containing the same missing_transition signal
    twice must enqueue exactly one trigger."""
    tenant_id = uuid7()
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        signal = DynamicSignal(
            dynamic_kind="missing_transition",
            summary="x", strength=0.7, confidence=0.5,
            subject_model_ids=(model_id,),
        )
        emitted = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_id, signals=[signal, signal, signal],
            reference_time=now,
        )

    assert len(emitted) == 1


# =====================================================================
# Tenant isolation
# =====================================================================


async def test_emit_does_not_dedup_across_tenants(
    fresh_db: asyncpg.Pool,
) -> None:
    """Each tenant has its own dedup namespace — a pending trigger in
    tenant A must not suppress emission in tenant B."""
    tenant_a = uuid7()
    tenant_b = uuid7()
    actor_a = uuid7()
    actor_b = uuid7()
    obs_a = uuid7()
    obs_b = uuid7()
    model_a = uuid7()
    model_b = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_discontinuity(conn, tenant_a, actor_a, obs_a, model_a, now)
        await _seed_discontinuity(conn, tenant_b, actor_b, obs_b, model_b, now)

        sigs_a = await detect_dynamic_signals(
            conn, tenant_id=tenant_a, model_ids=[model_a],
            reference_time=now,
        )
        sigs_b = await detect_dynamic_signals(
            conn, tenant_id=tenant_b, model_ids=[model_b],
            reference_time=now,
        )
        emitted_a = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_a, signals=sigs_a, reference_time=now,
        )
        emitted_b = await emit_missing_transition_triggers(
            conn, tenant_id=tenant_b, signals=sigs_b, reference_time=now,
        )

    assert len(emitted_a) == 1
    assert len(emitted_b) == 1
    assert emitted_a[0] != emitted_b[0]
