"""Tests for services.greeting.snapshot.

Phase-2 exit gate: snapshot composer produces valid SubstrateSnapshot
for a dogfood tenant with fixture data.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from services.greeting.snapshot import (
    SnapshotComposer,
    SubstrateSnapshot,
    QueryGridSnapshot,
    _time_of_day_bucket,
)
from services.greeting.tests.conftest import (
    TENANT_A,
    seed_actor,
    seed_anomaly,
    seed_commitment,
    seed_goal,
    seed_model,
    seed_resource,
    seed_state_change,
)


pytestmark = pytest.mark.integration


async def test_empty_tenant_produces_valid_snapshot(greeting_db):
    composer = SnapshotComposer(greeting_db)
    snap = await composer.compose_greeting_snapshot(TENANT_A)
    assert isinstance(snap, SubstrateSnapshot)
    assert snap.tenant_id == TENANT_A
    assert snap.top_models == []
    assert snap.active_commitments == []
    assert snap.customer_resources == []
    assert snap.recent_state_changes == []
    assert snap.anomalies == []
    assert snap.time_of_day_bucket in (
        "early_morning", "morning", "afternoon", "evening", "late"
    )


async def test_greeting_snapshot_picks_up_seeds(greeting_db):
    # Seed enough substrate to exercise every field.
    actor = await seed_actor(greeting_db)
    model_id = await seed_model(
        greeting_db, natural="Acme renewal structurally unsafe", confidence=0.84
    )
    goal_id = await seed_goal(greeting_db)
    commit_id = await seed_commitment(
        greeting_db,
        title="ship billing refactor",
        state="blocked",
        due_days=2,
        is_critical_path=True,
        goal_id=goal_id,
    )
    res_id = await seed_resource(
        greeting_db,
        kind="relational",
        identity="Acme",
        utilization_state="depleted",
        health="critical",
    )
    anomaly_id = await seed_anomaly(
        greeting_db,
        kind="customer_health_degraded",
        significance=0.85,
        region={"resource_id": str(res_id)},
    )
    await seed_state_change(
        greeting_db, entity_id=model_id, entity_kind="model"
    )

    composer = SnapshotComposer(greeting_db)
    snap = await composer.compose_greeting_snapshot(TENANT_A)

    model_ids = [m.id for m in snap.top_models]
    assert model_id in model_ids

    commit_titles = [c.title for c in snap.active_commitments]
    assert "ship billing refactor" in commit_titles

    resource_ids = [r.id for r in snap.customer_resources]
    assert res_id in resource_ids

    anomaly_ids = [a.id for a in snap.anomalies]
    assert anomaly_id in anomaly_ids

    assert any(
        sc.entity_id == model_id for sc in snap.recent_state_changes
    )


async def test_observation_cards_deduplicate_same_subject(greeting_db):
    """Week 7-8 CONCERN fix — Week-6 review flagged three observation
    cards all on the same subject when Think produces many Models about
    one topic. The diversity filter on `_card_candidates('observation')`
    should keep only one card per subject-token cluster.
    """
    # Three near-identical Acme-renewal Models — mimics the acme_tuesday
    # Think output.
    await seed_model(
        greeting_db,
        natural="Acme renewal is structurally unsafe as of Sunday",
        confidence=0.85,
    )
    await seed_model(
        greeting_db,
        natural="Acme renewal confidence dropped after deliverable slips",
        confidence=0.82,
    )
    await seed_model(
        greeting_db,
        natural="Acme renewal meeting is scheduled for Thursday",
        confidence=0.8,
    )
    # One distinct subject so diversity has somewhere to go.
    await seed_model(
        greeting_db,
        natural="Vertex Labs expansion probability is drifting downward",
        confidence=0.78,
    )

    composer = SnapshotComposer(greeting_db)
    snaps = await composer.compose_card_snapshot(TENANT_A, "observation")
    assert snaps, "expected at least one observation card snapshot"
    # The pinned candidates (position 0 of top_models on each snapshot)
    # must NOT all be Acme-renewal.
    pinned_naturals = [
        s.top_models[0].natural.lower() for s in snaps if s.top_models
    ]
    acme_hits = sum(1 for n in pinned_naturals if "acme" in n)
    assert acme_hits <= 1, (
        "observation-card diversity filter failed: "
        f"got {acme_hits} Acme cards in {pinned_naturals!r}"
    )


async def test_card_snapshot_pins_candidate(greeting_db):
    goal_id = await seed_goal(greeting_db)
    # Two blocked critical-path commitments — only the top should lead.
    c1 = await seed_commitment(
        greeting_db,
        title="first blocker",
        state="blocked",
        due_days=1,
        priority=1,
        is_critical_path=True,
        goal_id=goal_id,
    )
    _c2 = await seed_commitment(
        greeting_db,
        title="second blocker",
        state="blocked",
        due_days=5,
        priority=5,
        is_critical_path=True,
        goal_id=goal_id,
    )

    composer = SnapshotComposer(greeting_db)
    snaps = await composer.compose_card_snapshot(TENANT_A, "decision")
    assert snaps
    # First snapshot's pinned candidate sits at index 0 in active_commitments.
    first = snaps[0]
    assert first.active_commitments[0].id == c1


async def test_query_grid_snapshot_shape(greeting_db):
    # Seed a hot anomaly so situation queries appear.
    await seed_anomaly(greeting_db, significance=0.9)
    composer = SnapshotComposer(greeting_db)
    grid = await composer.compose_query_grid_snapshot(TENANT_A)
    assert isinstance(grid, QueryGridSnapshot)
    assert len(grid.situation_queries) >= 1
    assert len(grid.evergreen_queries) == 4
    for q in grid.situation_queries:
        assert q["hot"] is True
    for q in grid.evergreen_queries:
        assert q["hot"] is False


async def test_snapshot_to_json_is_serialisable(greeting_db):
    await seed_model(greeting_db, confidence=0.8)
    composer = SnapshotComposer(greeting_db)
    snap = await composer.compose_greeting_snapshot(TENANT_A)
    import json

    # Just ensure it round-trips.
    j = snap.to_json()
    s = json.dumps(j)
    assert "top_models" in s


def test_time_of_day_bucket():
    fixed = datetime(2026, 4, 22, tzinfo=timezone.utc)
    assert _time_of_day_bucket(fixed.replace(hour=7)) == "early_morning"
    assert _time_of_day_bucket(fixed.replace(hour=10)) == "morning"
    assert _time_of_day_bucket(fixed.replace(hour=14)) == "afternoon"
    assert _time_of_day_bucket(fixed.replace(hour=20)) == "evening"
    assert _time_of_day_bucket(fixed.replace(hour=23)) == "late"
    assert _time_of_day_bucket(fixed.replace(hour=2)) == "late"
