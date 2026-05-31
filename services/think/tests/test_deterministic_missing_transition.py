"""Integration tests for the T3:missing_transition deterministic handler.

Cover the imaginary-node pattern's Phase-2 surface:
  - is_authoritative routes T3:missing_transition through the
    deterministic path (no LLM).
  - The handler loads source Model + discontinuity, runs the imputer,
    emits a `missing_transition_detected` state_change observation, and
    returns a RawDiff with one ClaimOp(op='insert').
  - Idempotency / negative cases: missing model_id, archived source,
    resolved discontinuity, no audit chain.
  - The synthesized observation carries the right metadata for audit.
  - The claim_op.entry shape passes the substrate's structural
    requirements (born_from_event_id present, falsifier well-formed,
    scope inherited, confidence in the system-hypothesized band).
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.dynamics.hypothesis_imputer import (
    SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING,
    SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR,
)
from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext
from services.think.deterministic import (
    deterministic_handler,
    is_authoritative,
)
from services.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


def _embedding768() -> list[float]:
    return make_embedding("missing-transition-handler-seed")


async def _seed_substrate(
    conn: asyncpg.Connection,
    tenant_id: UUID, actor_id: UUID, obs_id: UUID, model_id: UUID,
    *,
    status: str = "active",
    model_natural: str = "Commitment to ship the dashboard rewrite by Q3",
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
        f"t3-mt-{obs_id}",
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
          '{"kind":"belief","subject":"commitment","assertion":"on track"}'::jsonb,
          $7, $4, ARRAY[$5]::uuid[],
          '[{"type":"commitment","id":"00000000-0000-7d00-0000-000000000001"}]'::jsonb,
          '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
          0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
          '{}'::uuid[], '{}'::uuid[], $6, 0.9, now()
        )
        """,
        model_id, tenant_id, obs_id, _embedding768(), actor_id, status,
        model_natural,
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
        previous_state={"status": "blocked"},  # discontinuity
        new_state={"status": "live"},
        changed_fields=["status"],
    )
    return prev, nxt


def _build_trigger(
    *,
    tenant_id: UUID,
    model_id: UUID | None,
    prev_event_id: int | None,
    next_event_id: int | None,
    prev_iso: str | None = None,
    next_iso: str | None = None,
    differing_fields: list[str] | None = None,
    trigger_id: UUID | None = None,
) -> TriggerContext:
    region_spec: dict = {
        "anomaly_kind": "missing_transition",
        "prev_event_id": prev_event_id,
        "next_event_id": next_event_id,
        "differing_fields": differing_fields or ["status"],
    }
    if prev_iso:
        region_spec["prev_event_occurred_at"] = prev_iso
    if next_iso:
        region_spec["next_event_occurred_at"] = next_iso
    return TriggerContext(
        kind="T3",
        tenant_id=tenant_id,
        subkind="missing_transition",
        model_id=model_id,
        region_spec=region_spec,
        seed_signature=(
            {"trigger_id": str(trigger_id)} if trigger_id else None
        ),
    )


# =====================================================================
# is_authoritative
# =====================================================================


async def test_is_authoritative_t3_missing_transition_true() -> None:
    t = TriggerContext(
        kind="T3", tenant_id=uuid7(), subkind="missing_transition",
    )
    assert is_authoritative(t) is True


async def test_is_authoritative_t3_other_subkind_remains_false() -> None:
    """Adjacent-coverage guard: my change must NOT broaden T3 generally."""
    for sub in ("anomaly", "belief_contestation", "reading_contestation"):
        t = TriggerContext(kind="T3", tenant_id=uuid7(), subkind=sub)
        assert is_authoritative(t) is False, sub


# =====================================================================
# Handler happy path
# =====================================================================


async def test_handler_returns_claim_op_insert_for_real_discontinuity(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        prev_event_id, next_event_id = await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        trigger = _build_trigger(
            tenant_id=tenant_id,
            model_id=model_id,
            prev_event_id=prev_event_id,
            next_event_id=next_event_id,
            prev_iso=(now - timedelta(hours=6)).isoformat(),
            next_iso=(now - timedelta(hours=1)).isoformat(),
            trigger_id=uuid7(),
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    assert diff.tenant_id == tenant_id
    assert len(diff.claim_ops) == 1
    op = diff.claim_ops[0]
    assert op.op == "insert"
    entry = op.entry
    assert entry is not None
    # Born-from event is a fresh state_change observation, not the
    # source Model's birth event.
    assert entry["born_from_event_id"] is not None
    # Confidence is in the system-hypothesized band.
    assert (
        SYSTEM_HYPOTHESIS_CONFIDENCE_FLOOR
        <= entry["confidence"]
        <= SYSTEM_HYPOTHESIS_CONFIDENCE_CEILING
    )
    assert entry["confidence"] == entry["confidence_at_assertion"]
    # Proposition has the hypothesis grammar carriers.
    prop = entry["proposition"]
    assert prop["kind"] == "belief"
    assert prop["legacy_kind"] == "hypothesis"
    assert prop["is_system_hypothesis"] is True
    assert "hypothesis_text" in prop
    # Falsifier well-formed.
    f = entry["falsifier"]
    assert f["kind"] == "observation_pattern"
    assert len(f["pattern"]) >= 20
    # Scope inherited from source model (scope_entities had a commitment).
    assert any(e.get("type") == "commitment" for e in entry["scope_entities"])
    # Source model included as supporting evidence.
    assert model_id in entry["supporting_model_ids"]
    # Reasoning trace mentions the model and confidence — useful for
    # debugging deterministic decisions.
    assert diff.reasoning_trace is not None
    assert str(model_id) in diff.reasoning_trace


async def test_handler_emits_synthetic_observation_for_born_event(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """The handler must emit a `missing_transition_detected` state_change
    observation that becomes the hypothesis Model's born_from_event_id.
    This is the load-bearing audit-chain link the ratification surface
    relies on."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        prev_event_id, _ = await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        before_count = await conn.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1 "
            "AND kind = 'state_change'",
            tenant_id,
        )
        trigger = _build_trigger(
            tenant_id=tenant_id, model_id=model_id,
            prev_event_id=prev_event_id, next_event_id=None,
            trigger_id=uuid7(),
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
        born = diff.claim_ops[0].entry["born_from_event_id"]

        row = await conn.fetchrow(
            """
            SELECT id, kind, content
            FROM observations
            WHERE id = $1
            """,
            born,
        )
        after_count = await conn.fetchval(
            "SELECT count(*) FROM observations WHERE tenant_id = $1 "
            "AND kind = 'state_change'",
            tenant_id,
        )

    assert row is not None
    assert row["kind"] == "state_change"
    content = (
        json.loads(row["content"])
        if isinstance(row["content"], (str, bytes, bytearray))
        else row["content"]
    )
    assert content["state_change_kind"] == "missing_transition_detected"
    assert content["entity_id"] == str(model_id)
    assert content["entity_kind"] == "model"
    md = content["metadata"]
    assert md["differing_fields"] == ["status"]
    assert md["prev_event_id"] == prev_event_id
    assert md["imputer_source"] == "missing_transition_detector_v1"
    assert after_count == before_count + 1


# =====================================================================
# Negative paths
# =====================================================================


async def test_handler_returns_empty_for_missing_model_id(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    trigger = _build_trigger(
        tenant_id=tenant, model_id=None,
        prev_event_id=None, next_event_id=None,
        trigger_id=uuid7(),
    )
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []
    assert "no-op" in (diff.reasoning_trace or "")


async def test_handler_returns_empty_for_unknown_source_model(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    trigger = _build_trigger(
        tenant_id=tenant, model_id=uuid7(),  # never seeded
        prev_event_id=None, next_event_id=None,
        trigger_id=uuid7(),
    )
    async with fresh_db.acquire() as conn:
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []
    assert "not found" in (diff.reasoning_trace or "")


async def test_handler_returns_empty_for_archived_source_model(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """Discontinuities on archived Models are settled by definition —
    no ratifiable hypothesis should fork off."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)
    async with fresh_db.acquire() as conn:
        await _seed_substrate(
            conn, tenant_id, actor_id, obs_id, model_id, status="archived",
        )
        await _insert_audit_event(
            conn, model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "review"},
        )
        await _insert_audit_event(
            conn, model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "blocked"},
            new_state={"status": "live"},
        )
        trigger = _build_trigger(
            tenant_id=tenant_id, model_id=model_id,
            prev_event_id=None, next_event_id=None,
            trigger_id=uuid7(),
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []
    assert "archived" in (diff.reasoning_trace or "")


async def test_handler_returns_empty_when_discontinuity_resolved(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """Race: trigger enqueued for a discontinuity that has since been
    resolved by a corrective audit event. The handler must no-op."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        await _seed_substrate(conn, tenant_id, actor_id, obs_id, model_id)
        # All-consistent chain — no discontinuity.
        await _insert_audit_event(
            conn, model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=6),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "active"},
            new_state={"status": "review"},
        )
        await _insert_audit_event(
            conn, model_id=model_id, tenant_id=tenant_id,
            occurred_at=now - timedelta(hours=1),
            cause_id=obs_id, cause_type="field_update",
            previous_state={"status": "review"},   # consistent
            new_state={"status": "live"},
        )
        trigger = _build_trigger(
            tenant_id=tenant_id, model_id=model_id,
            prev_event_id=None, next_event_id=None,
            trigger_id=uuid7(),
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    assert diff.claim_ops == []
    assert "resolved before processing" in (diff.reasoning_trace or "")


# =====================================================================
# Idempotency: re-running the handler against the same trigger payload
# produces an additional state_change observation (handler is not
# pure-idempotent — applied_triggers is the upstream idempotency layer).
# This test documents the contract so future refactors don't tighten it
# without thinking.
# =====================================================================


async def test_handler_is_not_pure_idempotent_relies_on_applied_triggers(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    now = datetime.now(timezone.utc)

    async with fresh_db.acquire() as conn:
        prev_event_id, _ = await _seed_discontinuity(
            conn, tenant_id, actor_id, obs_id, model_id, now,
        )
        trigger = _build_trigger(
            tenant_id=tenant_id, model_id=model_id,
            prev_event_id=prev_event_id, next_event_id=None,
            trigger_id=uuid7(),
        )
        diff_1 = await deterministic_handler(trigger, ContextBundle(), conn)
        diff_2 = await deterministic_handler(trigger, ContextBundle(), conn)

        born_1 = diff_1.claim_ops[0].entry["born_from_event_id"]
        born_2 = diff_2.claim_ops[0].entry["born_from_event_id"]
        # Two distinct observations — the handler doesn't dedup by itself.
        assert born_1 != born_2
        # The contract: idempotency lives one layer up. The applier
        # (services/think/applier.py) uses applied_triggers.trigger_ref
        # to prevent re-application. Both diffs share the same trigger_ref.
        assert diff_1.trigger_ref == diff_2.trigger_ref
