"""tests/unit/sage/test_evidence_projection.py — Phase 8 evidence projection.

Integration tests for `services.sage.evidence_projection.EvidenceProjector`.

Despite living under tests/unit, these tests touch a real Postgres
because the projector reads `models` + `observations` rows directly.
They follow the same `gateway_pool` + `_seed_*` pattern as
`tests/unit/sage/test_inquiry_traces_repo.py` (per-test fresh DB via
TRUNCATE). The `pytest.mark.integration` marker keeps them out of any
pure-unit selection that runs without a database.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.evidence_projection import (
    TOKEN_ESTIMATE,
    EvidenceProjector,
    ProjectionBudget,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers — direct raw-SQL inserts that bypass repo side effects so we
# can place rows in arbitrary states for projection tests.
# ---------------------------------------------------------------------


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _zero_vec() -> list[float]:
    # 768-dim zero vector — pgvector accepts it; we never run a vector
    # search in these tests so the value doesn't matter.
    return [0.0] * 768


async def _seed_observation(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    occurred_at: datetime | None = None,
    kind: str = "signal",
    source_channel: str = "harness",
    content_text: str = "synthetic observation",
    trust_tier: str = "authoritative",
) -> UUID:
    obs_id = uuid7()
    occurred_at = occurred_at or _now()
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO observations (
                id, tenant_id, occurred_at, ingested_at, kind,
                source_channel, source_actor_ref, actor_id,
                content, content_text,
                embedding, embedding_pending,
                trust_tier, external_id, cause_id, entities_mentioned
            ) VALUES (
                $1, $2, $3, $3, $4,
                $5, NULL, NULL,
                $6::jsonb, $7,
                $8, FALSE,
                $9, NULL, NULL, '[]'::jsonb
            )
            """,
            obs_id, tenant_id, occurred_at, kind,
            source_channel,
            json.dumps({"content_text": content_text}),
            content_text,
            _zero_vec(),
            trust_tier,
        )
    return obs_id


async def _seed_model(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    born_from_event_id: UUID,
    supporting_event_ids: list[UUID] | None = None,
    supporting_model_ids: list[UUID] | None = None,
    signal_readings: list[dict] | None = None,
    falsifier: dict | None = None,
    confidence: float = 0.6,
    natural: str = "synthetic model",
) -> UUID:
    mid = uuid7()
    scope_temporal = {
        "valid_from": _now().isoformat(),
        "valid_until": None,
    }
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO models (
                id, tenant_id, born_from_event_id,
                proposition, "natural", embedding,
                scope_actors, scope_entities, scope_temporal,
                confidence, confidence_at_assertion, activation, falsifier,
                signal_readings, reading_contestable,
                supporting_event_ids, supporting_model_ids, evidential_weight,
                status, archived_at, archive_reason,
                evaluate_at, resolution_criteria, contributing_models,
                visible_to_subjects
            ) VALUES (
                $1, $2, $3,
                $4::jsonb, $5, $6,
                '{}'::uuid[], '[]'::jsonb, $7::jsonb,
                $8, $8, 1.0, $9::jsonb,
                $10::jsonb, TRUE,
                $11::uuid[], $12::uuid[], 0.5,
                'active', NULL, NULL,
                NULL, NULL, '{}'::uuid[],
                TRUE
            )
            """,
            mid, tenant_id, born_from_event_id,
            json.dumps({"kind": "state", "subject": natural}),
            natural,
            _zero_vec(),
            json.dumps(scope_temporal),
            confidence,
            json.dumps(falsifier) if falsifier is not None else None,
            json.dumps(signal_readings or []),
            supporting_event_ids or [],
            supporting_model_ids or [],
        )
    return mid


# ---------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_selected_model_ids_returns_empty_projection(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[],
        question_primitive="STATUS",
    )
    assert result.projected == ()
    assert result.omitted == ()
    assert result.coverage == {
        "counterevidence_share": 0.0,
        "freshness_share": 0.0,
        "falsification_share": 0.0,
    }


@pytest.mark.asyncio
async def test_counterevidence_promoted_to_raw_excerpt_on_contradiction(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """When the question primitive is CONTRADICTION, decisive
    counterevidence is rendered at raw_excerpt include_level."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="seed for model",
    )
    support_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="positive update",
    )
    counter_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="customer says feature is broken",
    )
    model_id = await _seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[support_obs],
        signal_readings=[
            {
                "event_id": str(counter_obs),
                "kind": "contradiction",
                "weight": -0.8,
            },
        ],
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="CONTRADICTION",
    )
    counter = [p for p in result.projected if p.evidence_id == counter_obs]
    assert len(counter) == 1
    assert counter[0].reason == "decisive_counterevidence"
    assert counter[0].include_level == "raw_excerpt"
    assert counter[0].token_estimate == TOKEN_ESTIMATE["raw_excerpt"]


@pytest.mark.asyncio
async def test_support_demoted_to_evidence_card_on_dependency(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """For a DEPENDENCY question, decisive support renders as
    evidence_card (not raw_excerpt, not summary_only)."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )
    support_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="critical-path dependency confirmed",
        trust_tier="authoritative",
    )
    model_id = await _seed_model(
        gateway_pool,
        tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[support_obs],
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="DEPENDENCY",
    )
    support = [
        p for p in result.projected
        if p.evidence_id == support_obs and p.reason == "decisive_support"
    ]
    assert len(support) == 1
    assert support[0].include_level == "evidence_card"


@pytest.mark.asyncio
async def test_always_includes_at_least_one_counterevidence(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """Acceptance criterion §1792: when counterevidence exists, the
    projection must include at least one piece of it — even with a
    tiny budget that would otherwise crowd it out."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )
    # Three supporting obs + one counter — the supports dominate by
    # count, so a budget-stressed projector might drop the counter.
    supports = [
        await _seed_observation(
            gateway_pool, tenant_id=tenant_id,
            content_text=f"support-{i}",
            trust_tier="authoritative",
        )
        for i in range(3)
    ]
    counter_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="counter",
    )
    model_id = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=supports,
        signal_readings=[
            {
                "event_id": str(counter_obs),
                "kind": "contradiction",
                "weight": -0.5,
            },
        ],
    )

    # Tiny budget — would normally drop low-scoring picks.
    projector = EvidenceProjector(
        budget=ProjectionBudget(max_total_tokens=50),
    )
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    counter_ids = {
        p.evidence_id for p in result.projected
        if p.reason == "decisive_counterevidence"
    }
    assert counter_obs in counter_ids


@pytest.mark.asyncio
async def test_max_evidence_per_node_cap_enforced(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """If a model produces more candidates than max_evidence_per_node,
    the projector drops the lowest-scoring ones (preserving any
    counterevidence)."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )
    supports = [
        await _seed_observation(
            gateway_pool, tenant_id=tenant_id,
            content_text=f"support-{i}",
            trust_tier="authoritative",
        )
        for i in range(5)
    ]
    counter_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="counter",
    )
    model_id = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=supports,
        signal_readings=[
            {
                "event_id": str(counter_obs),
                "kind": "contradiction",
                "weight": -0.9,
            },
        ],
    )

    projector = EvidenceProjector(
        budget=ProjectionBudget(max_evidence_per_node=2),
    )
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    by_model = [p for p in result.projected if p.node_id == model_id]
    assert len(by_model) <= 2
    # Counterevidence must survive the cap.
    assert any(
        p.reason == "decisive_counterevidence" for p in by_model
    )


@pytest.mark.asyncio
async def test_max_total_tokens_budget_demotes_to_ref_only(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A very tight token budget forces include_level demotion all the
    way down to ref_only (or omission for overflow)."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )
    supports = [
        await _seed_observation(
            gateway_pool, tenant_id=tenant_id,
            content_text=f"support-{i}",
            trust_tier="authoritative",
        )
        for i in range(3)
    ]
    model_id = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=supports,
    )

    # Budget = 16 tokens. Two ref_only picks (8 each) just fit.
    projector = EvidenceProjector(
        budget=ProjectionBudget(max_total_tokens=16),
    )
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    # Every surviving pick must be ref_only — no other level fits.
    assert all(p.include_level == "ref_only" for p in result.projected)
    total_tokens = sum(p.token_estimate for p in result.projected)
    assert total_tokens <= 16


@pytest.mark.asyncio
async def test_coverage_counterevidence_share_computed(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """coverage.counterevidence_share = counter / total over the
    projected set."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )
    support_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="support",
        trust_tier="authoritative",
    )
    counter_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="counter",
    )
    model_id = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[support_obs],
        signal_readings=[
            {
                "event_id": str(counter_obs),
                "kind": "contradiction",
                "weight": -0.7,
            },
        ],
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    total = len(result.projected)
    assert total >= 2
    counter_count = sum(
        1 for p in result.projected
        if p.reason == "decisive_counterevidence"
    )
    expected = counter_count / total
    assert result.coverage["counterevidence_share"] == pytest.approx(expected)
    assert 0.0 <= result.coverage["freshness_share"] <= 1.0
    assert 0.0 <= result.coverage["falsification_share"] <= 1.0


@pytest.mark.asyncio
async def test_two_models_one_with_counterevidence_still_included(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """When two Models are selected and only one carries
    counterevidence, the projection still surfaces that
    counterevidence (it must not be drowned out by the second model's
    support evidence)."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )

    # Model A — pure support.
    support_a = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="model-a support",
        trust_tier="authoritative",
    )
    model_a = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[support_a],
        natural="model-a",
    )

    # Model B — support + counter.
    support_b = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="model-b support",
        trust_tier="authoritative",
    )
    counter_b = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="model-b counter",
    )
    model_b = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[support_b],
        signal_readings=[
            {
                "event_id": str(counter_b),
                "kind": "contradiction",
                "weight": -0.6,
            },
        ],
        natural="model-b",
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_a, model_b],
        question_primitive="STATUS",
    )
    # Counterevidence from model B survives.
    counter_picks = [
        p for p in result.projected
        if p.reason == "decisive_counterevidence"
    ]
    assert len(counter_picks) >= 1
    assert any(p.evidence_id == counter_b for p in counter_picks)
    # Counter is attributed to model_b (its source node).
    assert all(p.node_id == model_b for p in counter_picks)
    # And we still see model A's support.
    assert any(
        p.node_id == model_a and p.reason == "decisive_support"
        for p in result.projected
    )


@pytest.mark.asyncio
async def test_falsification_relevant_observation_picked(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """When falsifier.kind = 'observation_pattern' and the pattern
    appears in a supporting observation, that observation is surfaced
    with reason=falsification_relevant."""
    seed_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
    )
    matching_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="customer reports churn risk this week",
        trust_tier="authoritative",
    )
    other_obs = await _seed_observation(
        gateway_pool, tenant_id=tenant_id,
        content_text="unrelated note",
        trust_tier="authoritative",
    )
    model_id = await _seed_model(
        gateway_pool, tenant_id=tenant_id,
        born_from_event_id=seed_obs,
        supporting_event_ids=[other_obs, matching_obs],
        falsifier={"kind": "observation_pattern", "pattern": "churn"},
    )

    projector = EvidenceProjector()
    result = await projector.project(
        pool=gateway_pool,
        tenant_id=tenant_id,
        selected_model_ids=[model_id],
        question_primitive="STATUS",
    )
    falsif = [
        p for p in result.projected
        if p.reason == "falsification_relevant"
    ]
    assert len(falsif) == 1
    assert falsif[0].evidence_id == matching_obs
    assert result.coverage["falsification_share"] > 0.0
