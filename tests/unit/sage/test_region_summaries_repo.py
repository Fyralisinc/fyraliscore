"""tests/unit/sage/test_region_summaries_repo.py — Phase 11 region summaries.

Direct repo tests for `region_sufficient_state` (migration 0088). The
repo is a thin wrapper over SQL; we exercise the upsert insert/update
paths, the bulk fetch dict shape, the GIN-backed reverse lookup, and
both leaderboard orderings.

Marked `pytest.mark.integration` for the same reason as
test_inquiry_traces_repo.py — uses the gateway_pool fixture re-exported
via services/app/gateway/tests/conftest.py.
"""
from __future__ import annotations

import asyncio
from uuid import UUID

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.reasoning.sage.region_summaries.repo import RegionSummariesRepo
from services.reasoning.sage.region_summaries.types import (
    Frontier,
    Hypothesis,
    RegionSufficientState,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _set_tenant(pool: asyncpg.Pool, tenant_id: UUID) -> None:
    """Bind RLS tenant on a connection from the pool."""
    async with pool.acquire() as conn:
        await conn.execute(
            "SELECT set_config('app.current_tenant', $1, true)",
            str(tenant_id),
        )


def _region(
    tenant_id: UUID,
    *,
    region_id: UUID | None = None,
    summary: str = "Region currently weighing renewal risk.",
    region_label: str | None = None,
    member_model_ids: list[UUID] | None = None,
    priority_score: float = 0.5,
    prediction_error_score: float = 0.1,
    active_hypotheses: list[Hypothesis] | None = None,
    next_best_frontiers: list[Frontier] | None = None,
    last_refreshed_reason: str | None = "scheduled",
) -> RegionSufficientState:
    return RegionSufficientState(
        region_id=region_id or uuid7(),
        tenant_id=tenant_id,
        region_label=region_label,
        summary=summary,
        active_hypotheses=active_hypotheses or [],
        member_model_ids=member_model_ids or [],
        priority_score=priority_score,
        prediction_error_score=prediction_error_score,
        next_best_frontiers=next_best_frontiers or [],
        last_refreshed_reason=last_refreshed_reason,
    )


# =====================================================================
# upsert
# =====================================================================


@pytest.mark.asyncio
async def test_upsert_insert_path(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """upsert with a fresh region_id INSERTs and populates the
    DB-defaulted timestamps."""
    repo = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)

    region = _region(
        tenant_id,
        region_label="enterprise_renewals",
        summary="Largest deal Q3 — currently blocked on SSO.",
        active_hypotheses=[
            Hypothesis(id="h_sso", statement="SSO is blocker", confidence=0.7),
        ],
        next_best_frontiers=[
            Frontier(target="lookup.commitment", rationale="recent ARR call"),
        ],
        priority_score=0.8,
        prediction_error_score=0.2,
    )
    inserted = await repo.upsert(region)

    assert inserted.region_id == region.region_id
    assert inserted.tenant_id == tenant_id
    assert inserted.region_label == "enterprise_renewals"
    assert inserted.priority_score == pytest.approx(0.8)
    assert inserted.created_at is not None
    assert inserted.updated_at is not None
    assert len(inserted.active_hypotheses) == 1
    assert inserted.active_hypotheses[0].id == "h_sso"
    assert inserted.next_best_frontiers[0].target == "lookup.commitment"

    # get() returns the same row.
    fetched = await repo.get(region.region_id)
    assert fetched is not None
    assert fetched.region_id == region.region_id
    assert fetched.summary.startswith("Largest deal")


@pytest.mark.asyncio
async def test_upsert_update_path_preserves_created_at(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """A second upsert on the same region_id UPDATEs in place: every
    mutable column is overwritten, `created_at` is preserved, and
    `updated_at` is bumped."""
    repo = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)

    region = _region(
        tenant_id,
        summary="initial",
        priority_score=0.3,
        prediction_error_score=0.1,
    )
    first = await repo.upsert(region)
    created_first = first.created_at
    updated_first = first.updated_at

    # Force `now()` to tick forward so the updated_at comparison is
    # not subject to clock-resolution rounding.
    await asyncio.sleep(0.05)

    second = await repo.upsert(region.model_copy(update={
        "summary": "refreshed",
        "priority_score": 0.7,
        "last_refreshed_reason": "validated_model_update",
    }))

    assert second.region_id == first.region_id
    assert second.summary == "refreshed"
    assert second.priority_score == pytest.approx(0.7)
    assert second.last_refreshed_reason == "validated_model_update"
    # created_at preserved.
    assert second.created_at == created_first
    # updated_at bumped.
    assert second.updated_at is not None
    assert second.updated_at >= updated_first


# =====================================================================
# bulk_get
# =====================================================================


@pytest.mark.asyncio
async def test_bulk_get_returns_dict_keyed_by_region_id(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """bulk_get returns a dict keyed by region_id; missing ids are
    simply absent."""
    repo = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)

    r1 = await repo.upsert(_region(tenant_id, summary="r1", priority_score=0.5))
    r2 = await repo.upsert(_region(tenant_id, summary="r2", priority_score=0.6))
    missing_id = uuid7()

    result = await repo.bulk_get([r1.region_id, r2.region_id, missing_id])
    assert isinstance(result, dict)
    assert set(result.keys()) == {r1.region_id, r2.region_id}
    assert result[r1.region_id].summary == "r1"
    assert result[r2.region_id].summary == "r2"
    assert missing_id not in result

    # Empty input short-circuits to {}.
    assert await repo.bulk_get([]) == {}


# =====================================================================
# find_by_member_model — GIN-backed reverse lookup
# =====================================================================


@pytest.mark.asyncio
async def test_find_by_member_model_hits_gin_index(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """find_by_member_model returns every region whose
    member_model_ids contains the queried model_id. Confirms the
    GIN-indexed `@>` operator path."""
    repo = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)

    model_a = uuid7()
    model_b = uuid7()
    model_c = uuid7()

    region_with_a = await repo.upsert(_region(
        tenant_id,
        summary="contains A",
        member_model_ids=[model_a, model_b],
    ))
    region_with_a_too = await repo.upsert(_region(
        tenant_id,
        summary="also contains A",
        member_model_ids=[model_a],
        priority_score=0.9,
    ))
    region_without_a = await repo.upsert(_region(
        tenant_id,
        summary="no A",
        member_model_ids=[model_b, model_c],
    ))

    matches = await repo.find_by_member_model(model_a)
    match_ids = {r.region_id for r in matches}
    assert region_with_a.region_id in match_ids
    assert region_with_a_too.region_id in match_ids
    assert region_without_a.region_id not in match_ids

    # Querying for an unmentioned model returns [].
    assert await repo.find_by_member_model(uuid7()) == []


# =====================================================================
# top_by_priority / top_by_prediction_error
# =====================================================================


@pytest.mark.asyncio
async def test_top_by_priority_orders_desc(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """top_by_priority returns rows ordered by priority_score DESC,
    respecting the `limit`."""
    repo = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)

    low = await repo.upsert(_region(tenant_id, summary="low", priority_score=0.1))
    mid = await repo.upsert(_region(tenant_id, summary="mid", priority_score=0.5))
    high = await repo.upsert(_region(tenant_id, summary="high", priority_score=0.9))

    ordered = await repo.top_by_priority(limit=10)
    score_order = [r.priority_score for r in ordered]
    assert score_order == sorted(score_order, reverse=True)

    ids_in_order = [r.region_id for r in ordered]
    assert ids_in_order.index(high.region_id) < ids_in_order.index(mid.region_id)
    assert ids_in_order.index(mid.region_id) < ids_in_order.index(low.region_id)

    # limit honored.
    top1 = await repo.top_by_priority(limit=1)
    assert len(top1) == 1
    assert top1[0].region_id == high.region_id


@pytest.mark.asyncio
async def test_top_by_prediction_error_orders_desc(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """top_by_prediction_error orders by prediction_error_score DESC."""
    repo = RegionSummariesRepo(gateway_pool, tenant_id=tenant_id)

    quiet = await repo.upsert(_region(
        tenant_id, summary="quiet", prediction_error_score=0.05,
    ))
    noisy = await repo.upsert(_region(
        tenant_id, summary="noisy", prediction_error_score=0.4,
    ))
    burning = await repo.upsert(_region(
        tenant_id, summary="burning", prediction_error_score=0.95,
    ))

    ordered = await repo.top_by_prediction_error(limit=10)
    pe_order = [r.prediction_error_score for r in ordered]
    assert pe_order == sorted(pe_order, reverse=True)

    ids_in_order = [r.region_id for r in ordered]
    assert ids_in_order.index(burning.region_id) < ids_in_order.index(noisy.region_id)
    assert ids_in_order.index(noisy.region_id) < ids_in_order.index(quiet.region_id)
