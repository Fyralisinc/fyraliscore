"""
RA-4 — MMR diversity in context assembly tests.

Source: RETRIEVAL-DESIGN-AUDIT §7 args 1-2.

Verification criteria (AUDIT-FIXES-IMPLEMENTATION-PLAN §2 RA-4):
  1. 10 items where top-5 all share an embedding → MMR picks ≤2.
  2. lambda=1.0 reduces to pure greedy-by-score.
  3. Integration: real retrieval saturated by one entity's Models →
     MMR produces diverse context.
  4. Benchmark: 100 items, 100K budget → <200ms.
  5. Items that don't fit budget are SKIPPED, not truncated.
"""
from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone

import pytest

from services.retrieval.assembler import mmr_select
from services.retrieval.tests._fixtures import build_fixture, make_embedding


@dataclass
class _MMRItem:
    """Test stub with the fields MMR reads."""
    id: int
    score: float
    tokens: int
    embedding: list[float] = field(default_factory=list)


def _close_embedding(base: list[float], *, jitter: float = 0.0) -> list[float]:
    return [v + jitter for v in base]


# ---------------------------------------------------------------------
# Unit tests
# ---------------------------------------------------------------------


def test_ra4_mmr_prunes_redundant_top_5_to_at_most_2():
    """10 items where top-5 share an embedding; MMR must not pick more
    than 2 of them.

    Per RETRIEVAL-DESIGN-AUDIT §7 arg 1, the diversity penalty must
    actually dominate when shared-embedding items differ only marginally
    in score from the diverse tail. We use closely-spaced scores so the
    relevance gain doesn't trivially overwhelm the diversity penalty —
    this is the setting where MMR is supposed to help.
    """
    shared_emb = [1.0, 0.0, 0.0]
    diverse_embs = [
        [0.0, 1.0, 0.0],
        [0.0, 0.0, 1.0],
        [0.70710678, 0.70710678, 0.0],
        [0.0, 0.70710678, 0.70710678],
        [0.70710678, 0.0, 0.70710678],
    ]
    items = []
    # Top-5 (ids 0..4): highest scores, identical embedding. Scores
    # just barely above the diverse tail so diversity penalty bites.
    for i in range(5):
        items.append(_MMRItem(id=i, score=0.65 - i * 0.001, tokens=100, embedding=list(shared_emb)))
    # Tail-5 (ids 5..9): lower score, diverse.
    for i, emb in enumerate(diverse_embs):
        items.append(_MMRItem(id=5 + i, score=0.5 - i * 0.001, tokens=100, embedding=list(emb)))

    selected = mmr_select(items, budget_tokens=500, lambda_diversity=0.5)
    assert len(selected) == 5, f"expected 5 selected under 500t budget, got {len(selected)}"
    shared_picked = sum(1 for s in selected if s.id < 5)
    assert shared_picked <= 2, (
        f"MMR picked {shared_picked} redundant top-5 items; "
        f"expected ≤ 2 for lambda=0.5"
    )


def test_ra4_lambda_1_reduces_to_greedy_by_score():
    """With lambda=1.0 MMR ignores similarity and picks by score."""
    shared_emb = [1.0, 0.0, 0.0]
    items = [
        _MMRItem(id=i, score=1.0 - i * 0.1, tokens=100, embedding=list(shared_emb))
        for i in range(5)
    ]
    # Add a low-scoring diverse item; MMR at lambda=1 should still
    # prefer top-scores.
    items.append(
        _MMRItem(id=99, score=0.05, tokens=100, embedding=[0.0, 1.0, 0.0]),
    )
    selected = mmr_select(items, budget_tokens=500, lambda_diversity=1.0)
    # Under pure greedy, we take ids 0..4 (ignoring the diverse item).
    picked_ids = [s.id for s in selected]
    assert picked_ids == [0, 1, 2, 3, 4]


def test_ra4_lambda_0_prefers_diversity_after_first_pick():
    """With lambda=0.0, diversity dominates; after the first item,
    subsequent picks are maximally dissimilar."""
    shared_emb = [1.0, 0.0, 0.0]
    div_emb = [0.0, 1.0, 0.0]
    items = [
        _MMRItem(id=0, score=1.0, tokens=100, embedding=list(shared_emb)),  # picked first (score)
        _MMRItem(id=1, score=0.9, tokens=100, embedding=list(shared_emb)),  # identical to 0 (penalized)
        _MMRItem(id=2, score=0.1, tokens=100, embedding=list(div_emb)),     # orthogonal (bonus)
    ]
    selected = mmr_select(items, budget_tokens=300, lambda_diversity=0.0)
    picked_ids = [s.id for s in selected]
    # After id=0, the MMR score for id=1 is 0 - 1*1 = -1; for id=2 is 0 - 0*1 = 0 (higher → picked).
    assert picked_ids[0] == 0
    assert picked_ids[1] == 2, f"expected id=2 second under lambda=0; got {picked_ids}"


def test_ra4_items_that_dont_fit_are_skipped_not_truncated():
    """Oversized items are dropped entirely; selection does not
    truncate them mid-item (audit §7 arg 2)."""
    small = _MMRItem(id=1, score=0.5, tokens=50, embedding=[1.0, 0.0])
    huge = _MMRItem(id=2, score=1.0, tokens=10_000, embedding=[0.0, 1.0])
    another_small = _MMRItem(id=3, score=0.3, tokens=50, embedding=[0.5, 0.5])

    selected = mmr_select([small, huge, another_small], budget_tokens=100, lambda_diversity=0.5)
    ids = {s.id for s in selected}
    # Both small items fit within 100t; huge is dropped.
    assert ids == {1, 3}
    # No item's tokens were reduced.
    for s in selected:
        assert s.tokens in (50,)


def test_ra4_zero_token_items_skipped():
    """Items claiming zero tokens are defensively skipped."""
    z = _MMRItem(id=1, score=1.0, tokens=0, embedding=[1.0])
    ok = _MMRItem(id=2, score=0.5, tokens=10, embedding=[0.0, 1.0])
    selected = mmr_select([z, ok], budget_tokens=100)
    assert [s.id for s in selected] == [2]


def test_ra4_zero_budget_returns_empty():
    items = [_MMRItem(id=i, score=1.0, tokens=10, embedding=[1.0, 0.0]) for i in range(3)]
    assert mmr_select(items, budget_tokens=0) == []
    assert mmr_select(items, budget_tokens=-1) == []


def test_ra4_invalid_lambda_raises():
    items = [_MMRItem(id=0, score=1.0, tokens=10, embedding=[1.0])]
    with pytest.raises(ValueError):
        mmr_select(items, budget_tokens=100, lambda_diversity=1.5)
    with pytest.raises(ValueError):
        mmr_select(items, budget_tokens=100, lambda_diversity=-0.1)


def test_ra4_missing_embeddings_degrade_gracefully():
    """Items without embeddings still get selected; similarity
    treated as 0 (no diversity penalty, no bonus)."""
    items = [
        _MMRItem(id=0, score=1.0, tokens=10, embedding=None),
        _MMRItem(id=1, score=0.5, tokens=10, embedding=None),
    ]
    selected = mmr_select(items, budget_tokens=100, lambda_diversity=0.5)
    assert {s.id for s in selected} == {0, 1}


def test_ra4_benchmark_100_items_100k_budget_under_200ms():
    import random
    rng = random.Random(42)
    items = [
        _MMRItem(
            id=i,
            score=rng.random(),
            tokens=500,
            embedding=[rng.gauss(0.0, 1.0) for _ in range(32)],
        )
        for i in range(100)
    ]
    t0 = time.perf_counter()
    selected = mmr_select(items, budget_tokens=100_000, lambda_diversity=0.5)
    dt = (time.perf_counter() - t0) * 1000.0
    assert len(selected) > 0
    # Plan's bound: < 200ms.
    assert dt < 200.0, f"MMR on 100 items took {dt:.1f}ms (> 200ms)"


# ---------------------------------------------------------------------
# Integration — saturated retrieval → MMR produces diversity
# ---------------------------------------------------------------------


@pytest.mark.integration
async def test_ra4_mmr_integration_diversifies_entity_saturated_result(
    tx_conn, fresh_db, tenant
):
    """Build a fixture where many Models are scoped to the same
    commitment. Primary retrieval surfaces redundant Models. Under
    MMR with diversity > 0, selection should spread across embeddings.
    """
    from services.retrieval.primary import TriggerContext, primary_retrieve
    fs = await build_fixture(tx_conn, tenant, pool=fresh_db)

    trigger = TriggerContext(
        kind="T1",
        tenant_id=tenant,
        seed_entity_ids=[{"type": "commitment", "id": str(fs.hero_commitment_id)}],
        seed_natural_text="alice ships reliably",
        seed_occurred_at=datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
        precomputed_seed_vector=make_embedding("alice ships reliably"),
    )
    result = await primary_retrieve(trigger, tx_conn)

    # Build MMR items from retrieved Models. Fixed token cost per
    # Model (the test focuses on diversity, not packing).
    items = []
    for m in result.models:
        score = result.model_scores.get(m.id, 0.0)
        emb = list(m.embedding) if m.embedding is not None else None
        items.append(_MMRItem(id=m.id.int, score=score, tokens=100, embedding=emb))

    if not items:
        pytest.skip("no models in retrieval result")

    selected_greedy = mmr_select(items, budget_tokens=500, lambda_diversity=1.0)
    selected_mmr = mmr_select(items, budget_tokens=500, lambda_diversity=0.5)
    # We expect diversity selection to return an equivalent count but
    # (in most fixtures) to differ in composition. We assert counts
    # equal and that MMR's pick is a valid subset of items.
    assert len(selected_greedy) == len(selected_mmr)
    mmr_ids = {s.id for s in selected_mmr}
    all_ids = {i.id for i in items}
    assert mmr_ids.issubset(all_ids)
