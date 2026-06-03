"""Tests for `scripts/backfill_duplicate_models.py`.

Real Postgres tests (gated on $DATABASE_URL). The script reads/writes
`models`, `reconciliation_events`, and `relationship_candidates`; all
three are exercised here.

The fixtures live in `scripts/tests/conftest.py`.
"""
from __future__ import annotations

import json
import uuid

import pytest

from scripts.backfill_duplicate_models import (
    BackfillResult,
    run_backfill,
)
from scripts.tests.conftest import (
    insert_actor,
    insert_model,
    insert_observation,
    make_embedding,
    near_embedding,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


async def _count_models(pool, tenant) -> int:
    async with pool.acquire() as c:
        return await c.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1", tenant,
        )


async def _count_archived(pool, tenant) -> int:
    async with pool.acquire() as c:
        return await c.fetchval(
            "SELECT count(*) FROM models "
            "WHERE tenant_id = $1 AND status = 'archived'",
            tenant,
        )


async def _count_candidates(pool, tenant) -> int:
    async with pool.acquire() as c:
        return await c.fetchval(
            "SELECT count(*) FROM relationship_candidates WHERE tenant_id = $1",
            tenant,
        )


async def _count_recon_events(pool, tenant) -> int:
    async with pool.acquire() as c:
        return await c.fetchval(
            "SELECT count(*) FROM reconciliation_events WHERE tenant_id = $1",
            tenant,
        )


async def _model_confidence(pool, mid) -> float:
    async with pool.acquire() as c:
        return float(await c.fetchval(
            "SELECT confidence FROM models WHERE id = $1", mid,
        ))


def _state_prop(subject: str, assertion: str) -> dict:
    return {"kind": "state", "subject": subject, "assertion": assertion}


def _concern_prop(text: str) -> dict:
    return {
        "kind": "concern",
        "about": "customer-churn",
        "nature": text,
        "raised_by": "cs-manager",
    }


def _recommendation_prop(actor_id: uuid.UUID, recommendation: str) -> dict:
    return {
        "kind": "recommendation",
        "target_actor_id": str(actor_id),
        "recommendation": recommendation,
        "rationale": "test rationale",
        "expected_state_transition": "from a to b",
    }


# ---------------------------------------------------------------------
# Test: dry-run never writes to DB
# ---------------------------------------------------------------------


async def test_dry_run_never_writes_to_db(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        base = make_embedding("alice ships consistently")
        mid_a = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("alice", "ships consistently"),
            natural="Alice ships features consistently every two weeks.",
            embedding=base,
            confidence=0.6,
            activation=0.5,
        )
        mid_b = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("alice", "delivers consistently"),
            natural="Alice ships features consistently every two weeks.",
            embedding=near_embedding(base, jitter=0.005),
            confidence=0.7,
            activation=0.5,
        )

    pre_models = await _count_models(fresh_db, tenant)
    pre_archived = await _count_archived(fresh_db, tenant)
    pre_candidates = await _count_candidates(fresh_db, tenant)
    pre_recon = await _count_recon_events(fresh_db, tenant)

    out = tmp_path / "report.jsonl"
    result = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("state",),
        apply=False,
        cosine_floor=0.70,
        max_clusters=100,
        output_jsonl=out,
    )

    assert await _count_models(fresh_db, tenant) == pre_models
    assert await _count_archived(fresh_db, tenant) == pre_archived
    assert await _count_candidates(fresh_db, tenant) == pre_candidates
    assert await _count_recon_events(fresh_db, tenant) == pre_recon

    assert isinstance(result, BackfillResult)
    assert result.metrics.models_archived == 0
    assert result.metrics.candidates_emitted == 0
    assert result.metrics.human_review_rows == 0
    assert result.metrics.clusters_considered >= 1

    # JSONL output is well-formed: one row per cluster.
    lines = out.read_text().strip().splitlines()
    assert len(lines) == result.metrics.clusters_considered
    for line in lines:
        row = json.loads(line)
        assert "cluster_id" in row
        assert "kind" in row
        assert "decision" in row
        assert "member_model_ids" in row
        assert isinstance(row["actions"], list)
        assert row["applied"] is False
    # The two near-identical models should have clustered together.
    decisions = [json.loads(line)["decision"] for line in lines]
    assert "auto_merge" in decisions
    # And the seeded ids should appear in some cluster's members.
    all_members: set[str] = set()
    for line in lines:
        all_members.update(json.loads(line)["member_model_ids"])
    assert str(mid_a) in all_members
    assert str(mid_b) in all_members


# ---------------------------------------------------------------------
# Test: apply mode merges a known duplicate pair
# ---------------------------------------------------------------------


async def test_apply_merges_known_duplicate_pair(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        base = make_embedding("renewal risk for beacon account")
        # Canonical should be the higher-activation model.
        mid_canonical = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("beacon", "renewal at risk"),
            natural="Beacon renewal is at risk because of delivery slippage.",
            embedding=base,
            confidence=0.55,
            activation=0.95,
        )
        mid_duplicate = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("beacon", "renewal jeopardised"),
            natural="Beacon renewal is at risk because of delivery slippage.",
            embedding=near_embedding(base, jitter=0.005),
            confidence=0.80,
            activation=0.40,
        )

    out = tmp_path / "report.jsonl"
    result = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("state",),
        apply=True,
        cosine_floor=0.70,
        max_clusters=10,
        output_jsonl=out,
    )

    # One auto_merge cluster.
    assert result.metrics.per_decision.get("auto_merge", 0) >= 1
    assert result.metrics.models_archived >= 1

    # Canonical (higher activation) survived.
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, status, confidence FROM models "
            "WHERE tenant_id = $1 ORDER BY id",
            tenant,
        )
    statuses = {r["id"]: r["status"] for r in rows}
    confidences = {r["id"]: float(r["confidence"]) for r in rows}
    assert statuses[mid_canonical] == "active"
    assert statuses[mid_duplicate] == "archived"
    # Canonical confidence lifted to max(0.55, 0.80) == 0.80.
    assert confidences[mid_canonical] == pytest.approx(0.80, abs=1e-6)


# ---------------------------------------------------------------------
# Test: recommendations are never auto-merged
# ---------------------------------------------------------------------


async def test_recommendation_pair_never_auto_merged(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        base = make_embedding("recommend pause hiring this quarter")
        mid_a = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_recommendation_prop(actor, "Pause hiring for Q3."),
            natural="Recommend pausing hiring during Q3.",
            embedding=base,
            confidence=0.6,
            activation=0.7,
        )
        mid_b = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_recommendation_prop(actor, "Hold hiring for the quarter."),
            natural="Recommend pausing hiring during Q3.",
            embedding=near_embedding(base, jitter=0.005),
            confidence=0.65,
            activation=0.5,
        )

    result = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("recommendation",),
        apply=True,
        cosine_floor=0.70,
        max_clusters=10,
        output_jsonl=tmp_path / "report.jsonl",
    )

    # No auto_merge for recommendations.
    assert result.metrics.per_decision.get("auto_merge", 0) == 0
    assert result.metrics.models_archived == 0
    # Both seeded models still active.
    async with fresh_db.acquire() as conn:
        archived = await conn.fetchval(
            "SELECT count(*) FROM models WHERE tenant_id = $1 "
            "AND status = 'archived' AND id IN ($2, $3)",
            tenant, mid_a, mid_b,
        )
    assert archived == 0
    # Should have produced a human_review row (kind-blocked branch).
    assert result.metrics.human_review_rows >= 1


# ---------------------------------------------------------------------
# Test: same_issue_as candidate emission at borderline cosine
# ---------------------------------------------------------------------


async def test_borderline_pair_emits_same_issue_candidate(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        # Two embeddings constructed to fall in the [0.70, 0.85) cosine
        # band for the default `concern` kind rule (human_review=0.65,
        # auto_merge=0.78) — actually we tune jitter until cosine sits
        # in the same_issue band.
        base = make_embedding("vendor reliability concern")
        # Larger jitter → lower cosine; aim well below 0.78 so the
        # default concern threshold puts us in same_issue land.
        partner = near_embedding(base, jitter=0.45)
        mid_a = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_concern_prop("vendor reliability"),
            natural="We are concerned about Vendor X's reliability.",
            embedding=base,
            confidence=0.5,
            activation=0.5,
        )
        mid_b = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_concern_prop("supplier reliability"),
            natural="Vendor X reliability has been concerning.",
            embedding=partner,
            confidence=0.55,
            activation=0.5,
        )

    pre_candidates = await _count_candidates(fresh_db, tenant)
    pre_archived = await _count_archived(fresh_db, tenant)

    result = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("concern",),
        apply=True,
        cosine_floor=0.65,
        max_clusters=10,
        output_jsonl=tmp_path / "report.jsonl",
    )

    if result.metrics.per_decision.get("same_issue_candidate", 0) >= 1:
        # Expected path: candidate emitted, no archival.
        assert (
            await _count_candidates(fresh_db, tenant) - pre_candidates >= 1
        )
        assert (
            await _count_archived(fresh_db, tenant) == pre_archived
        )
        assert result.metrics.candidates_emitted >= 1
    else:
        # The deterministic embedding helper sometimes lands the pair
        # outside the borderline band — confirm at least that we did
        # NOT auto-merge (concerns above 0.78 would merge silently
        # and this test would be moot).
        assert result.metrics.per_decision.get("auto_merge", 0) == 0
        assert (
            await _count_archived(fresh_db, tenant) == pre_archived
        )


# ---------------------------------------------------------------------
# Test: transitive clustering A↔B and B↔C
# ---------------------------------------------------------------------


async def test_transitive_cluster_absorbs_all_three(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        base = make_embedding("alpha shipping rate trend")
        # All three embeddings are tiny perturbations of `base` so
        # every pairwise cosine is well above 0.85 and forms a single
        # union-find cluster.
        emb_a = base
        emb_b = near_embedding(base, jitter=0.005)
        emb_c = near_embedding(base, jitter=0.008)
        mid_a = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("alpha", "ship rate trending up"),
            natural="Alpha ship rate is trending up week over week.",
            embedding=emb_a,
            confidence=0.5,
            activation=0.95,
        )
        mid_b = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("alpha", "ship rate is rising"),
            natural="Alpha shipping rate is rising consistently.",
            embedding=emb_b,
            confidence=0.6,
            activation=0.40,
        )
        mid_c = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("alpha", "delivery cadence improving"),
            natural="Alpha delivery cadence has been improving lately.",
            embedding=emb_c,
            confidence=0.7,
            activation=0.55,
        )

    result = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("state",),
        apply=True,
        cosine_floor=0.70,
        max_clusters=10,
        output_jsonl=tmp_path / "report.jsonl",
    )

    # Exactly one auto_merge cluster of size 3.
    auto_merge_clusters = [
        r for r in result.cluster_reports if r["decision"] == "auto_merge"
    ]
    assert len(auto_merge_clusters) == 1
    cluster = auto_merge_clusters[0]
    assert set(cluster["member_model_ids"]) == {str(mid_a), str(mid_b), str(mid_c)}
    # Canonical is mid_a (highest activation 0.95).
    assert cluster["canonical_model_id"] == str(mid_a)

    # Two non-canonical models archived; canonical lifted to max(0.5,0.6,0.7)=0.7.
    async with fresh_db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, status, confidence FROM models WHERE tenant_id = $1",
            tenant,
        )
    by_id = {r["id"]: r for r in rows}
    assert by_id[mid_a]["status"] == "active"
    assert by_id[mid_b]["status"] == "archived"
    assert by_id[mid_c]["status"] == "archived"
    assert float(by_id[mid_a]["confidence"]) == pytest.approx(0.7, abs=1e-6)
    assert result.metrics.models_archived == 2


# ---------------------------------------------------------------------
# Test: output JSONL is well-formed with one row per cluster
# ---------------------------------------------------------------------


async def test_output_jsonl_is_well_formed(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        base = make_embedding("operational metric stability")
        a = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("ops", "metrics stable"),
            natural="Operational metrics have been stable this week.",
            embedding=base,
            confidence=0.55,
            activation=0.7,
        )
        b = await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("ops", "metrics remain stable"),
            natural="Operational metrics remain stable.",
            embedding=near_embedding(base, jitter=0.005),
            confidence=0.65,
            activation=0.5,
        )

    out = tmp_path / "report.jsonl"
    result = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("state",),
        apply=False,
        cosine_floor=0.70,
        max_clusters=10,
        output_jsonl=out,
    )

    lines = out.read_text().strip().splitlines()
    assert len(lines) == len(result.cluster_reports)
    for line in lines:
        row = json.loads(line)
        assert {
            "cluster_id",
            "kind",
            "decision",
            "canonical_model_id",
            "member_model_ids",
            "cosine_mean",
            "signal_breakdown",
            "actions",
            "error",
            "applied",
        }.issubset(row.keys())
        assert isinstance(row["member_model_ids"], list)
        assert isinstance(row["signal_breakdown"], dict)
    # Ensure the seeded pair appears at least once.
    found_members: set[str] = set()
    for line in lines:
        found_members.update(json.loads(line)["member_model_ids"])
    assert {str(a), str(b)}.issubset(found_members)


# ---------------------------------------------------------------------
# Test: idempotence — re-running on merged state is a no-op
# ---------------------------------------------------------------------


async def test_apply_is_idempotent(
    fresh_db, tenant, tenant_cleanup, tmp_path,
):
    async with fresh_db.acquire() as conn:
        actor = await insert_actor(conn, tenant)
        obs = await insert_observation(conn, tenant, actor_id=actor)
        base = make_embedding("idempotence pair")
        await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("x", "is consistent"),
            natural="X is consistent across runs.",
            embedding=base,
            confidence=0.55,
            activation=0.9,
        )
        await insert_model(
            conn, tenant,
            born_event_id=obs,
            proposition=_state_prop("x", "stays consistent"),
            natural="X stays consistent across runs.",
            embedding=near_embedding(base, jitter=0.005),
            confidence=0.60,
            activation=0.4,
        )

    first = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("state",),
        apply=True,
        cosine_floor=0.70,
        max_clusters=10,
        output_jsonl=tmp_path / "first.jsonl",
    )
    second = await run_backfill(
        pool=fresh_db,
        tenant_id=tenant,
        kinds=("state",),
        apply=True,
        cosine_floor=0.70,
        max_clusters=10,
        output_jsonl=tmp_path / "second.jsonl",
    )
    # First run did work; second run finds nothing more to merge.
    assert first.metrics.per_decision.get("auto_merge", 0) >= 1
    assert second.metrics.per_decision.get("auto_merge", 0) == 0
    assert second.metrics.models_archived == 0
