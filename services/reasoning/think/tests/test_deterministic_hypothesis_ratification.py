"""Integration tests for the T2 hypothesis-ratification deterministic
handlers.

Covers the three Think-routed ratification subkinds that the ratify
endpoint emits when the user clicks Approve / Correct / Other on a
hypothesis card:

  - T2:hypothesis_approved  → bump confidence into the user-ratified
    band + add a ratification signal_readings entry + bump
    confirmed_count.
  - T2:hypothesis_corrected → archive the hypothesis Model and insert
    a new fact-Model carrying the user's correction (with
    was_system_hypothesis=True provenance).
  - T2:hypothesis_other    → archive the hypothesis Model.

The dismiss action is handled inline in the recommendations layer and
has no Think handler — see `test_ratify.py` for its coverage.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.memory_grammar import derive_memory_grammar
from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.deterministic import (
    USER_CORRECTED_FACT_CONFIDENCE,
    USER_CORRECTED_FACT_CONFIDENCE_CEILING,
    USER_RATIFIED_HYPOTHESIS_CONFIDENCE,
    USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING,
    deterministic_handler,
    is_authoritative,
)
from services.reasoning.think.tests.conftest import make_embedding


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------


async def _seed_hypothesis_model(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID, actor_id: UUID, obs_id: UUID, model_id: UUID,
    confidence: float = 0.30,
    status: str = "active",
    confirmed_count: int = 0,
    signal_readings: list[dict] | None = None,
    natural: str = "Hypothesized intermediate state X happened",
) -> None:
    """Insert a hypothesis Model directly via SQL."""
    proposition = {
        "kind": "belief",
        "legacy_kind": "hypothesis",
        "hypothesis_text": natural,
        "is_system_hypothesis": True,
        "imputation_source": "missing_transition_detector_v1",
        "bracketed_event_ids": [None, None],
        "differing_fields": ["status"],
    }
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
        f"hyp-ratify-{obs_id}",
    )
    await conn.execute(
        """
        INSERT INTO models (
          id, tenant_id, born_from_event_id, proposition, "natural",
          embedding, scope_actors, scope_entities, scope_temporal,
          confidence, confidence_at_assertion, falsifier,
          signal_readings, supporting_event_ids, supporting_model_ids,
          contributing_models, status, activation, last_retrieved_at,
          confirmed_count, contested_count
        ) VALUES (
          $1, $2, $3,
          $4::jsonb, $5, $6, ARRAY[$7]::uuid[], '[]'::jsonb,
          '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
          $8, $8, NULL,
          $9::jsonb, ARRAY[$3]::uuid[], '{}'::uuid[],
          '{}'::uuid[], $10, 0.9, now(),
          $11, 0
        )
        """,
        model_id, tenant_id, obs_id,
        json.dumps(proposition), natural,
        make_embedding(natural), actor_id,
        confidence,
        json.dumps(signal_readings or []),
        status, confirmed_count,
    )


def _ratify_trigger(
    *,
    tenant_id: UUID,
    model_id: UUID,
    subkind: str,
    actor_id: UUID | None = None,
    observation_id: UUID | None = None,
    correction: dict | None = None,
    explanation: str | None = None,
    captured_observation_id: UUID | None = None,
) -> TriggerContext:
    seed_signature: dict[str, Any] = {
        "trigger_id": str(uuid7()),
        "actor_id": str(actor_id) if actor_id else None,
    }
    if correction is not None:
        seed_signature["correction"] = correction
    if explanation is not None:
        seed_signature["explanation"] = explanation
    if captured_observation_id is not None:
        seed_signature["captured_observation_id"] = str(captured_observation_id)
    return TriggerContext(
        kind="T2",
        tenant_id=tenant_id,
        subkind=subkind,
        model_id=model_id,
        observation_id=observation_id,
        seed_signature=seed_signature,
    )


# =====================================================================
# is_authoritative
# =====================================================================


async def test_is_authoritative_t2_hypothesis_subkinds_true() -> None:
    for sub in ("hypothesis_approved", "hypothesis_corrected", "hypothesis_other"):
        t = TriggerContext(kind="T2", tenant_id=uuid7(), subkind=sub)
        assert is_authoritative(t) is True, sub


async def test_is_authoritative_t2_arbitrary_hypothesis_subkind_false() -> None:
    """My change must NOT broaden T2 generally to anything starting
    with `hypothesis_`."""
    t = TriggerContext(
        kind="T2", tenant_id=uuid7(), subkind="hypothesis_bogus",
    )
    assert is_authoritative(t) is False


# =====================================================================
# T2 hypothesis_approved
# =====================================================================


async def test_approved_bumps_confidence_into_ratified_band(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()

    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id, confidence=0.30,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_approved", actor_id=actor_id,
            observation_id=obs_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    assert len(diff.claim_ops) == 1
    op = diff.claim_ops[0]
    assert op.op == "update"
    assert op.model_id == model_id
    changes = op.changes
    assert changes is not None
    new_conf = changes["confidence"]
    # Hits the band.
    assert (
        USER_RATIFIED_HYPOTHESIS_CONFIDENCE
        <= new_conf
        <= USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING
    )
    # Signal reading appended.
    assert any(
        r.get("kind") == "ratification"
        and r.get("ratification_kind") == "hypothesis_approved"
        and r.get("actor_id") == str(actor_id)
        for r in changes["signal_readings"]
    )
    # Bookkeeping.
    assert changes["confirmed_count"] == 1
    assert "last_confirmed_at" in changes


async def test_approved_idempotent_does_not_lower_confidence(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """If the hypothesis is already in the user-ratified band (e.g.,
    via a prior approval), a second approval must not decrease
    confidence. Idempotency in the imaginary-node ratification path."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()

    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id,
            confidence=USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_approved", actor_id=actor_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    new_conf = diff.claim_ops[0].changes["confidence"]
    assert new_conf >= USER_RATIFIED_HYPOTHESIS_CONFIDENCE
    assert new_conf <= USER_RATIFIED_HYPOTHESIS_CONFIDENCE_CEILING


async def test_approved_preserves_existing_signal_readings(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """Existing signal_readings (e.g., from prior contestation) must
    be preserved; the new ratification reading is appended."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    prior = [
        {"kind": "actor_attestation", "actor_id": str(uuid7()),
         "occurred_at": "2026-05-01T00:00:00Z", "claim": "I saw it happen"}
    ]

    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id,
            signal_readings=prior,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_approved", actor_id=actor_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    readings = diff.claim_ops[0].changes["signal_readings"]
    # Original entry preserved.
    assert any(r.get("kind") == "actor_attestation" for r in readings)
    # New ratification entry appended.
    assert any(r.get("kind") == "ratification" for r in readings)


async def test_approved_no_op_on_archived_model(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id, status="archived",
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_approved", actor_id=actor_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []


async def test_approved_no_op_on_non_hypothesis_model(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """A non-hypothesis Model must not be mutated by the approve
    handler — guards against trigger routing bugs."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()

    async with fresh_db.acquire() as conn:
        # Seed a non-hypothesis Model (state kind → claim_role='fact').
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status)"
            " VALUES ($1, $2, 'human_internal', 'Carol', 'active')",
            actor_id, tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, actor_id,
              content, content_text, embedding, embedding_pending,
              trust_tier, external_id, entities_mentioned
            ) VALUES (
              $1, $2, now(), 'signal', 'test', $3,
              '{}'::jsonb, 'x', NULL, TRUE, 'authoritative',
              $4, '[]'::jsonb
            )
            """,
            obs_id, tenant_id, actor_id, f"non-hyp-{obs_id}",
        )
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, confidence_at_assertion, falsifier,
              signal_readings, supporting_event_ids, supporting_model_ids,
              contributing_models, status, activation
            ) VALUES (
              $1, $2, $3,
              '{"kind":"belief","subject":"x","assertion":"y"}'::jsonb,
              'fact model', $4, ARRAY[$5]::uuid[], '[]'::jsonb,
              '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
              0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
              '{}'::uuid[], '{}'::uuid[], 'active', 0.5
            )
            """,
            model_id, tenant_id, obs_id,
            make_embedding("fact model"), actor_id,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_approved", actor_id=actor_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []
    assert "not a hypothesis" in (diff.reasoning_trace or "")


# =====================================================================
# T2 hypothesis_corrected
# =====================================================================


async def test_corrected_archives_hypothesis_and_inserts_fact(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    captured_obs_id = uuid7()

    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id,
        )
        # Seed the captured correction observation (mirrors what the
        # ratify handler writes inline).
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, actor_id,
              content, content_text, embedding, embedding_pending,
              trust_tier, external_id, entities_mentioned
            ) VALUES (
              $1, $2, now(), 'state_change', 'internal:state_change',
              $3, '{}'::jsonb, 'correction', NULL, TRUE,
              'authoritative', NULL, '[]'::jsonb
            )
            """,
            captured_obs_id, tenant_id, actor_id,
        )
        correction = {
            "natural": "The team moved the commitment to paused, not blocked.",
            "proposition_overrides": {"domain_tags": ["execution"]},
        }
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_corrected", actor_id=actor_id,
            observation_id=obs_id,
            correction=correction,
            captured_observation_id=captured_obs_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    assert len(diff.claim_ops) == 2
    archive_op = diff.claim_ops[0]
    assert archive_op.op == "archive"
    assert archive_op.model_id == model_id
    assert archive_op.reason == "hypothesis_user_corrected"

    insert_op = diff.claim_ops[1]
    assert insert_op.op == "insert"
    entry = insert_op.entry
    assert entry["confidence"] == USER_CORRECTED_FACT_CONFIDENCE
    assert entry["confidence"] <= USER_CORRECTED_FACT_CONFIDENCE_CEILING
    prop = entry["proposition"]
    assert prop["kind"] == "belief"
    assert "legacy_kind" not in prop, (
        "corrected fact-Model must NOT inherit hypothesis legacy_kind"
    )
    assert prop["was_system_hypothesis"] is True
    assert prop["lineage"]["source_hypothesis_id"] == str(model_id)
    assert prop["lineage"]["correction_actor_id"] == str(actor_id)
    # New Model derives to claim_role='fact'.
    grammar = derive_memory_grammar(prop, natural=entry["natural"])
    assert grammar.claim_role == "fact"
    # The override domain_tags propagated.
    assert "execution" in prop.get("domain_tags", [])
    # Source hypothesis is the lead supporting Model.
    assert entry["supporting_model_ids"][0] == model_id
    # Born from the user's correction observation.
    assert entry["born_from_event_id"] == captured_obs_id


async def test_corrected_rejects_user_overriding_kind_or_lineage(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    """User payload must NOT override `kind` or `lineage` — those are
    provenance-critical fields."""
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    captured_obs_id = uuid7()

    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id,
        )
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, actor_id,
              content, content_text, embedding, embedding_pending,
              trust_tier, external_id, entities_mentioned
            ) VALUES (
              $1, $2, now(), 'state_change', 'internal:state_change',
              $3, '{}'::jsonb, 'c', NULL, TRUE,
              'authoritative', NULL, '[]'::jsonb
            )
            """,
            captured_obs_id, tenant_id, actor_id,
        )
        correction = {
            "natural": "Real claim",
            "proposition_overrides": {
                "kind": "prediction",  # MUST be ignored
                "lineage": {"forged": True},  # MUST be ignored
                "domain_tags": ["customers"],
            },
        }
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_corrected", actor_id=actor_id,
            correction=correction,
            captured_observation_id=captured_obs_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    prop = diff.claim_ops[1].entry["proposition"]
    assert prop["kind"] == "belief"  # NOT 'prediction'
    assert "forged" not in str(prop["lineage"])
    # Non-protected override still applies.
    assert "customers" in prop.get("domain_tags", [])


async def test_corrected_no_op_without_correction_payload(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_corrected", actor_id=actor_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []


# =====================================================================
# T2 hypothesis_other
# =====================================================================


async def test_other_archives_hypothesis_only(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    async with fresh_db.acquire() as conn:
        await _seed_hypothesis_model(
            conn, tenant_id=tenant_id, actor_id=actor_id,
            obs_id=obs_id, model_id=model_id,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_other", actor_id=actor_id,
            explanation="Multiple unrecorded things happened.",
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)

    assert len(diff.claim_ops) == 1
    archive_op = diff.claim_ops[0]
    assert archive_op.op == "archive"
    assert archive_op.model_id == model_id
    assert archive_op.reason == "hypothesis_user_other"


async def test_other_no_op_on_non_hypothesis(
    fresh_db, tenant, tenant_cleanup,
) -> None:
    tenant_id = tenant
    actor_id = uuid7()
    obs_id = uuid7()
    model_id = uuid7()
    async with fresh_db.acquire() as conn:
        await conn.execute(
            "INSERT INTO actors (id, tenant_id, type, display_name, status)"
            " VALUES ($1, $2, 'human_internal', 'D', 'active')",
            actor_id, tenant_id,
        )
        await conn.execute(
            """
            INSERT INTO observations (
              id, tenant_id, occurred_at, kind, source_channel, actor_id,
              content, content_text, embedding, embedding_pending,
              trust_tier, external_id, entities_mentioned
            ) VALUES (
              $1, $2, now(), 'signal', 'test', $3,
              '{}'::jsonb, 'x', NULL, TRUE, 'authoritative',
              $4, '[]'::jsonb
            )
            """,
            obs_id, tenant_id, actor_id, f"non-hyp-other-{obs_id}",
        )
        await conn.execute(
            """
            INSERT INTO models (
              id, tenant_id, born_from_event_id, proposition, "natural",
              embedding, scope_actors, scope_entities, scope_temporal,
              confidence, confidence_at_assertion, falsifier,
              signal_readings, supporting_event_ids, supporting_model_ids,
              contributing_models, status, activation
            ) VALUES (
              $1, $2, $3,
              '{"kind":"belief","subject":"x","assertion":"y"}'::jsonb,
              'fact', $4, ARRAY[$5]::uuid[], '[]'::jsonb,
              '{"valid_from":"2026-01-01T00:00:00Z","valid_until":null}'::jsonb,
              0.6, 0.6, NULL, '[]'::jsonb, ARRAY[$3]::uuid[],
              '{}'::uuid[], '{}'::uuid[], 'active', 0.5
            )
            """,
            model_id, tenant_id, obs_id,
            make_embedding("fact"), actor_id,
        )
        trigger = _ratify_trigger(
            tenant_id=tenant_id, model_id=model_id,
            subkind="hypothesis_other", actor_id=actor_id,
        )
        diff = await deterministic_handler(trigger, ContextBundle(), conn)
    assert diff.claim_ops == []
