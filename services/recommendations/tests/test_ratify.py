"""Integration tests for the hypothesis ratification handler.

Cover the four ratification actions (approve / correct / other / dismiss)
as direct handler calls. Three of them (approve/correct/other) emit T2
triggers and leave the hypothesis Model in place; dismiss archives
inline. Each test asserts:
  - The right state-change observation lands in the substrate.
  - The right (or no) T2 trigger is enqueued with the right payload.
  - The hypothesis Model's status reflects the action's mutation
    semantics (archived for dismiss; still active for the others —
    Think handlers will mutate them later).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import pytest
import pytest_asyncio

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from services.recommendations.handlers import (
    ARCHIVE_REASON_HYPOTHESIS_DISMISSED,
    AlreadyArchivedError,
    RatifyResult,
    ratify_hypothesis,
)
from services.recommendations.tests.conftest import seed_observation


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


# ---------------------------------------------------------------------
# Hypothesis seeder
# ---------------------------------------------------------------------


async def _seed_hypothesis(
    pool: asyncpg.Pool,
    *,
    tenant: UUID,
    target_actor_id: UUID,
    born_from_event: UUID,
    confidence: float = 0.30,
    hypothesis_text: str = "Hypothesized intermediate state X happened",
) -> UUID:
    """Insert a hypothesis Model that satisfies the substrate's grammar
    (claim_role derives to 'hypothesis' via legacy_kind)."""
    mid = uuid7()
    embedding = [0.0] * 768
    embedding[0] = 1.0
    proposition: dict[str, Any] = {
        "kind": "belief",
        "legacy_kind": "hypothesis",
        "hypothesis_text": hypothesis_text,
        "is_system_hypothesis": True,
        "imputation_source": "missing_transition_detector_v1",
        "bracketed_event_ids": [None, None],
        "differing_fields": ["status"],
    }
    await pool.execute(
        """
        INSERT INTO models (
            id, tenant_id, born_from_event_id,
            proposition, "natural", embedding,
            scope_actors, scope_entities, scope_temporal,
            confidence, activation,
            confidence_at_assertion, activation_coefficient,
            status
        ) VALUES (
            $1, $2, $3,
            $4::jsonb, $5, $6,
            $7::uuid[], '[]'::jsonb, $8::jsonb,
            $9, 1.0,
            $9, 1.0,
            'active'
        )
        """,
        mid, tenant, born_from_event,
        json.dumps(proposition),
        hypothesis_text,
        embedding,
        [target_actor_id],
        json.dumps({"valid_from": "2026-04-26T00:00:00Z", "valid_until": None}),
        confidence,
    )
    return mid


@pytest_asyncio.fixture
async def hypothesis_setup(
    gateway_pool: asyncpg.Pool,
    tenant_id: UUID,
    seeded_actor: UUID,
):
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    hyp_id = await _seed_hypothesis(
        gateway_pool, tenant=tenant_id,
        target_actor_id=seeded_actor, born_from_event=obs_id,
    )
    yield {
        "tenant_id": tenant_id,
        "actor_id": seeded_actor,
        "obs_id": obs_id,
        "hypothesis_id": hyp_id,
    }
    # Cleanup
    await gateway_pool.execute(
        "DELETE FROM think_trigger_queue WHERE tenant_id = $1",
        tenant_id,
    )
    await gateway_pool.execute(
        "DELETE FROM models WHERE id = $1", hyp_id,
    )


# =====================================================================
# Dismiss — direct archive, no T2 trigger
# =====================================================================


async def test_dismiss_archives_inline_with_correct_reason(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            result = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=hypothesis_setup["actor_id"],
                tenant_id=hypothesis_setup["tenant_id"],
                action="dismiss",
                explanation="not a real gap",
                conn=conn,
            )

    assert isinstance(result, RatifyResult)
    assert result.action == "dismiss"
    assert result.archived is True
    assert result.trigger_id is None

    row = await gateway_pool.fetchrow(
        "SELECT status, archive_reason FROM models WHERE id = $1",
        hyp_id,
    )
    assert row["status"] == "archived"
    assert row["archive_reason"] == ARCHIVE_REASON_HYPOTHESIS_DISMISSED

    # No T2 trigger should have been emitted.
    count = await gateway_pool.fetchval(
        "SELECT count(*) FROM think_trigger_queue "
        "WHERE tenant_id = $1 AND model_id = $2",
        hypothesis_setup["tenant_id"], hyp_id,
    )
    assert count == 0

    # The dismiss state_change observation lands in the audit trail.
    obs_count = await gateway_pool.fetchval(
        """
        SELECT count(*) FROM observations
        WHERE tenant_id = $1 AND kind = 'state_change'
          AND content @> '{"state_change_kind": "hypothesis_dismissed"}'::jsonb
        """,
        hypothesis_setup["tenant_id"],
    )
    assert obs_count >= 1


# =====================================================================
# Approve — T2 trigger, model still active
# =====================================================================


async def test_approve_emits_t2_trigger_and_leaves_model_active(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            result = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=hypothesis_setup["actor_id"],
                tenant_id=hypothesis_setup["tenant_id"],
                action="approve",
                conn=conn,
            )

    assert result.action == "approve"
    assert result.archived is False
    assert result.trigger_id is not None

    # Model still active — Think will mutate it.
    status = await gateway_pool.fetchval(
        "SELECT status FROM models WHERE id = $1", hyp_id,
    )
    assert status == "active"

    # T2 trigger landed with the right shape.
    trig_row = await gateway_pool.fetchrow(
        """
        SELECT trigger_kind, trigger_subkind, model_id, payload, observation_id
        FROM think_trigger_queue WHERE id = $1
        """,
        result.trigger_id,
    )
    assert trig_row["trigger_kind"] == "T2"
    assert trig_row["trigger_subkind"] == "hypothesis_approved"
    assert trig_row["model_id"] == hyp_id
    assert trig_row["observation_id"] is not None
    payload = (
        json.loads(trig_row["payload"])
        if isinstance(trig_row["payload"], (str, bytes, bytearray))
        else trig_row["payload"]
    )
    assert payload["actor_id"] == str(hypothesis_setup["actor_id"])
    assert payload["trigger_id"] == str(result.trigger_id)
    assert payload["ratification_observation_id"] is not None


# =====================================================================
# Correct — T2 trigger, captures correction observation
# =====================================================================


async def test_correct_emits_t2_trigger_and_captures_correction_obs(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    correction = {
        "natural": (
            "What actually happened was that the team moved the commitment "
            "to paused, not blocked, after the standup."
        ),
        "proposition_overrides": {
            "domain_tags": ["execution"],
        },
    }
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            result = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=hypothesis_setup["actor_id"],
                tenant_id=hypothesis_setup["tenant_id"],
                action="correct",
                correction=correction,
                conn=conn,
            )

    assert result.action == "correct"
    assert result.archived is False
    assert result.trigger_id is not None
    assert result.captured_observation_id is not None

    payload = await gateway_pool.fetchval(
        "SELECT payload FROM think_trigger_queue WHERE id = $1",
        result.trigger_id,
    )
    payload_dict = (
        json.loads(payload)
        if isinstance(payload, (str, bytes, bytearray))
        else payload
    )
    assert "correction" in payload_dict
    assert payload_dict["correction"]["natural"].startswith(
        "What actually happened"
    )
    assert payload_dict["correction"]["proposition_overrides"] == {
        "domain_tags": ["execution"],
    }
    assert payload_dict["captured_observation_id"] == str(
        result.captured_observation_id
    )

    obs_row = await gateway_pool.fetchrow(
        "SELECT kind, content FROM observations WHERE id = $1",
        result.captured_observation_id,
    )
    assert obs_row is not None
    assert obs_row["kind"] == "state_change"
    content = (
        json.loads(obs_row["content"])
        if isinstance(obs_row["content"], (str, bytes, bytearray))
        else obs_row["content"]
    )
    assert content["state_change_kind"] == "hypothesis_correction_authored"
    assert content["metadata"]["correction_natural"].startswith(
        "What actually happened"
    )


async def test_correct_rejects_missing_correction_payload(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(ValidationError, match="correction"):
                await ratify_hypothesis(
                    model_id=hyp_id,
                    actor_id=hypothesis_setup["actor_id"],
                    tenant_id=hypothesis_setup["tenant_id"],
                    action="correct",
                    correction=None,
                    conn=conn,
                )


async def test_correct_rejects_empty_natural(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(ValidationError, match="correction"):
                await ratify_hypothesis(
                    model_id=hyp_id,
                    actor_id=hypothesis_setup["actor_id"],
                    tenant_id=hypothesis_setup["tenant_id"],
                    action="correct",
                    correction={"natural": "   "},
                    conn=conn,
                )


# =====================================================================
# Other — T2 trigger, captures explanation observation
# =====================================================================


async def test_other_emits_t2_trigger_and_captures_explanation(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            result = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=hypothesis_setup["actor_id"],
                tenant_id=hypothesis_setup["tenant_id"],
                action="other",
                explanation="Several things happened that don't fit cleanly.",
                conn=conn,
            )

    assert result.action == "other"
    assert result.archived is False
    assert result.trigger_id is not None
    assert result.captured_observation_id is not None

    trig_row = await gateway_pool.fetchrow(
        "SELECT trigger_subkind, payload FROM think_trigger_queue "
        "WHERE id = $1",
        result.trigger_id,
    )
    assert trig_row["trigger_subkind"] == "hypothesis_other"
    payload = (
        json.loads(trig_row["payload"])
        if isinstance(trig_row["payload"], (str, bytes, bytearray))
        else trig_row["payload"]
    )
    assert payload["explanation"].startswith("Several things")

    obs_row = await gateway_pool.fetchrow(
        "SELECT content FROM observations WHERE id = $1",
        result.captured_observation_id,
    )
    content = (
        json.loads(obs_row["content"])
        if isinstance(obs_row["content"], (str, bytes, bytearray))
        else obs_row["content"]
    )
    assert content["state_change_kind"] == "hypothesis_other_explanation"


async def test_other_rejects_empty_explanation(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(ValidationError, match="explanation"):
                await ratify_hypothesis(
                    model_id=hyp_id,
                    actor_id=hypothesis_setup["actor_id"],
                    tenant_id=hypothesis_setup["tenant_id"],
                    action="other",
                    explanation="   ",
                    conn=conn,
                )


# =====================================================================
# Action validation
# =====================================================================


async def test_unknown_action_raises_validation_error(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(ValidationError, match="unknown ratify action"):
                await ratify_hypothesis(
                    model_id=hyp_id,
                    actor_id=hypothesis_setup["actor_id"],
                    tenant_id=hypothesis_setup["tenant_id"],
                    action="bogus",  # type: ignore[arg-type]
                    conn=conn,
                )


# =====================================================================
# Negative paths on the substrate
# =====================================================================


async def test_unknown_model_raises_validation_error(
    gateway_pool, tenant_id, seeded_actor,
) -> None:
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            with pytest.raises(ValidationError, match="hypothesis model"):
                await ratify_hypothesis(
                    model_id=uuid7(),
                    actor_id=seeded_actor,
                    tenant_id=tenant_id,
                    action="approve",
                    conn=conn,
                )


async def test_already_archived_hypothesis_raises(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    # Archive the hypothesis directly.
    await gateway_pool.execute(
        "UPDATE models SET status = 'archived', archived_at = now(), "
        "archive_reason = 'manual' WHERE id = $1",
        hyp_id,
    )
    try:
        async with gateway_pool.acquire() as conn:
            async with conn.transaction():
                with pytest.raises(AlreadyArchivedError):
                    await ratify_hypothesis(
                        model_id=hyp_id,
                        actor_id=hypothesis_setup["actor_id"],
                        tenant_id=hypothesis_setup["tenant_id"],
                        action="approve",
                        conn=conn,
                    )
    finally:
        # Allow the fixture cleanup to delete the row.
        pass


async def test_recommendation_kind_model_not_acceptable_as_hypothesis(
    gateway_pool, tenant_id, seeded_actor,
) -> None:
    """A claim_role='recommendation' Model must NOT be ratifiable via
    the hypothesis loader — the load function filters by claim_role."""
    from services.recommendations.tests.conftest import (
        seed_recommendation_model,
        make_recommendation_proposition,
    )
    obs_id = await seed_observation(
        gateway_pool, tenant=tenant_id, actor_id=seeded_actor,
    )
    prop = make_recommendation_proposition(
        target_actor_id=seeded_actor,
        target_type="commitment",
        target_id=uuid7(),
    )
    rec_id = await seed_recommendation_model(
        gateway_pool,
        tenant=tenant_id, target_actor_id=seeded_actor,
        born_from_event=obs_id, proposition=prop,
    )
    try:
        async with gateway_pool.acquire() as conn:
            async with conn.transaction():
                with pytest.raises(ValidationError, match="hypothesis model"):
                    await ratify_hypothesis(
                        model_id=rec_id,
                        actor_id=seeded_actor,
                        tenant_id=tenant_id,
                        action="approve",
                        conn=conn,
                    )
    finally:
        await gateway_pool.execute(
            "DELETE FROM models WHERE id = $1", rec_id,
        )


# =====================================================================
# Audit-chain integrity: ratification observation cites the hypothesis'
# born_from_event_id as cause_id, so the chain remains walkable.
# =====================================================================


async def test_ratification_observation_chains_to_hypothesis_birth(
    gateway_pool, hypothesis_setup,
) -> None:
    hyp_id = hypothesis_setup["hypothesis_id"]
    born_event = await gateway_pool.fetchval(
        "SELECT born_from_event_id FROM models WHERE id = $1",
        hyp_id,
    )
    async with gateway_pool.acquire() as conn:
        async with conn.transaction():
            result = await ratify_hypothesis(
                model_id=hyp_id,
                actor_id=hypothesis_setup["actor_id"],
                tenant_id=hypothesis_setup["tenant_id"],
                action="approve",
                conn=conn,
            )
    ratification_obs = await gateway_pool.fetchval(
        "SELECT observation_id FROM think_trigger_queue WHERE id = $1",
        result.trigger_id,
    )
    cause = await gateway_pool.fetchval(
        "SELECT cause_id FROM observations WHERE id = $1",
        ratification_obs,
    )
    assert cause == born_event
