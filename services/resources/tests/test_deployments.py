"""Tests for services/resources/deployments.py."""
from __future__ import annotations

import asyncio

import pytest

from lib.shared.errors import InvariantViolation

from services.resources import repo, deployments
from services.resources.tests.conftest import (
    TENANT_A,
    make_commitment,
    make_observation,
)


pytestmark = pytest.mark.asyncio


async def _make_capacity(pool, event_id, total=10):
    return await repo.create(
        kind="capacity",
        identity="eng_team",
        current_value={
            "total_units": total,
            "deployed_units": 0,
            "available_units": total,
            "unit_type": "engineer",
        },
        tenant_id=TENANT_A,
        created_by_event_id=event_id,
    )


async def test_deploy_and_release_round_trip(resources_db, event_id):
    r = await _make_capacity(resources_db, event_id, total=5)
    cmt = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    dep = await deployments.deploy(
        r.id, cmt, quantity={"units": 2}, source_event_id=ev,
    )
    assert dep.deployed_quantity["units"] == 2
    after_deploy = await repo.get(r.id)
    assert after_deploy.current_value["deployed_units"] == 2
    assert after_deploy.current_value["available_units"] == 3

    await deployments.release(
        (r.id, cmt), source_event_id=ev,
    )
    after_release = await repo.get(r.id)
    assert after_release.current_value["deployed_units"] == 0
    assert after_release.current_value["available_units"] == 5


async def test_active_deployments_filter(resources_db, event_id):
    r = await _make_capacity(resources_db, event_id, total=5)
    c1 = await make_commitment(resources_db)
    c2 = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    await deployments.deploy(r.id, c1, quantity={"units": 1}, source_event_id=ev)
    await deployments.deploy(r.id, c2, quantity={"units": 1}, source_event_id=ev)
    await deployments.release((r.id, c1), source_event_id=ev)

    active = await deployments.active_deployments_for(r.id)
    assert {d.commitment_id for d in active} == {c2}


async def test_deploy_insufficient_raises_r1(resources_db, event_id):
    r = await _make_capacity(resources_db, event_id, total=2)
    c1 = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    with pytest.raises(InvariantViolation) as ei:
        await deployments.deploy(
            r.id, c1, quantity={"units": 5}, source_event_id=ev,
        )
    assert ei.value.invariant == "R1"


async def test_deploy_zero_units_raises_r2(resources_db, event_id):
    r = await _make_capacity(resources_db, event_id)
    c1 = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    with pytest.raises(InvariantViolation) as ei:
        await deployments.deploy(
            r.id, c1, quantity={"units": 0}, source_event_id=ev,
        )
    assert ei.value.invariant == "R2"


async def test_release_exceeding_deployed_raises_r3(resources_db, event_id):
    r = await _make_capacity(resources_db, event_id, total=5)
    c1 = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    await deployments.deploy(r.id, c1, quantity={"units": 2}, source_event_id=ev)
    with pytest.raises(InvariantViolation) as ei:
        await deployments.release(
            (r.id, c1), actual_quantity={"units": 5}, source_event_id=ev,
        )
    assert ei.value.invariant == "R3"


async def test_release_idempotent(resources_db, event_id):
    r = await _make_capacity(resources_db, event_id)
    c1 = await make_commitment(resources_db)
    ev = await make_observation(resources_db)
    await deployments.deploy(r.id, c1, quantity={"units": 1}, source_event_id=ev)
    await deployments.release((r.id, c1), source_event_id=ev)
    # Second release is idempotent — returns same row, no further tx.
    result = await deployments.release((r.id, c1), source_event_id=ev)
    assert result.released_at is not None


async def test_concurrent_deploys_serialize_correctly(resources_db, event_id):
    """
    10 concurrent deploys each requesting 1 unit against a 10-unit pool
    all succeed; deployed_units ends at 10. Serialization via
    `SELECT ... FOR UPDATE` in record_transaction.
    """
    r = await _make_capacity(resources_db, event_id, total=10)
    commitments = [await make_commitment(resources_db) for _ in range(10)]
    ev = await make_observation(resources_db)

    async def one(cmt_id):
        try:
            await deployments.deploy(
                r.id, cmt_id, quantity={"units": 1}, source_event_id=ev,
            )
            return True
        except InvariantViolation:
            return False

    results = await asyncio.gather(*(one(c) for c in commitments))
    assert all(results)
    reloaded = await repo.get(r.id)
    assert reloaded.current_value["deployed_units"] == 10
    assert reloaded.current_value["available_units"] == 0
    assert reloaded.utilization_state == "depleted"


async def test_concurrent_deploys_over_capacity(resources_db, event_id):
    """
    With only 3 units and 10 concurrent deploys asking for 1 each,
    exactly 3 succeed and 7 fail with R1. No partial state.
    """
    r = await _make_capacity(resources_db, event_id, total=3)
    commitments = [await make_commitment(resources_db) for _ in range(10)]
    ev = await make_observation(resources_db)

    async def one(cmt_id):
        try:
            await deployments.deploy(
                r.id, cmt_id, quantity={"units": 1}, source_event_id=ev,
            )
            return "ok"
        except InvariantViolation as e:
            assert e.invariant == "R1"
            return "fail"

    results = await asyncio.gather(*(one(c) for c in commitments))
    ok_count = sum(1 for x in results if x == "ok")
    fail_count = sum(1 for x in results if x == "fail")
    assert ok_count == 3
    assert fail_count == 7
    reloaded = await repo.get(r.id)
    assert reloaded.current_value["deployed_units"] == 3
    assert reloaded.current_value["available_units"] == 0
