from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import os
from uuid import uuid4

import asyncpg
import pytest

from services.evaluation.epistemic_repair.p2_runner import _admission
from services.evaluation.epistemic_repair.p7_evolution import (
    arm_allows_canonical_mutation,
    arm_allows_reasoning,
    arm_memory_visible,
    bridge_validated_think_lifecycle,
)
from lib.shared.errors import InvariantViolation
from services.domain.truth_kernel import build_default_truth_kernel


pytestmark = pytest.mark.asyncio


async def _setup(conn: asyncpg.Connection):
    tenant_id = uuid4()
    await conn.execute(
        "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
        tenant_id,
        f"p7-evolution-{tenant_id}",
    )
    admitted = await build_default_truth_kernel().admit(
        tx=conn, command=_admission(tenant_id, 7701)
    )
    observation_id = uuid4()
    now = datetime.now(timezone.utc)
    await conn.execute(
        "INSERT INTO observations(id,tenant_id,occurred_at,kind,source_channel,"
        "content,content_text,embedding_pending,trust_tier,entities_mentioned) "
        "VALUES($1,$2,$3,'signal','email:message',$4::jsonb,$5,TRUE,'ordinary','[]')",
        observation_id,
        tenant_id,
        now,
        json.dumps({"text": "The accountable owner says the dependency is still open."}),
        "The accountable owner says the dependency is still open.",
    )
    return tenant_id, admitted, observation_id, now


async def _think_run(
    conn: asyncpg.Connection,
    *,
    tenant_id,
    model_id,
    observation_id,
    now,
    status="success",
):
    run_id = uuid4()
    trigger_id = uuid4()
    await conn.execute(
        "INSERT INTO think_runs(id,tenant_id,trigger_id,trigger_kind,lane,"
        "started_at,ended_at,status,ops_applied) "
        "VALUES($1,$2,$3,'T1:event_arrival','batch_memory',$4,$5,$6,$7::jsonb)",
        run_id,
        tenant_id,
        trigger_id,
        now,
        now + timedelta(seconds=2),
        status,
        json.dumps({
            "memory_lifecycle_ops": [{
                "op": "reconcile",
                "action": "falsify",
                "model_id": str(model_id),
                "rationale": "new authoritative evidence contradicts the accepted state",
                "evidence_event_ids": [str(observation_id)],
                "evidence_model_ids": [],
            }]
        }),
    )
    return run_id


async def test_successful_think_falsification_advances_canonical_head() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        tenant_id, admitted, observation_id, now = await _setup(conn)
        run_id = await _think_run(
            conn,
            tenant_id=tenant_id,
            model_id=admitted.model_id,
            observation_id=observation_id,
            now=now,
        )
        receipts = await bridge_validated_think_lifecycle(
            conn,
            tenant_id=tenant_id,
            arm="corrupted",
            batch_number=6,
            think_run_id=run_id,
            corruption_model_ids=frozenset({admitted.model_id}),
        )
        assert len(receipts) == 1
        assert receipts[0].resulting_lifecycle == "falsified"
        assert receipts[0].within_two_batch_recovery_bound is True
        assert await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 "
            "AND id=$2",
            tenant_id,
            admitted.model_id,
        ) == 0
        assert await conn.fetchval(
            "SELECT lifecycle FROM model_truth_heads WHERE tenant_id=$1 AND model_id=$2",
            tenant_id,
            admitted.model_id,
        ) == "falsified"
    finally:
        await tx.rollback()
        await conn.close()


@pytest.mark.parametrize("arm", ("frozen", "observation_only"))
async def test_forbidden_arms_fail_closed_on_lifecycle_mutation(arm: str) -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        tenant_id, admitted, observation_id, now = await _setup(conn)
        run_id = await _think_run(
            conn,
            tenant_id=tenant_id,
            model_id=admitted.model_id,
            observation_id=observation_id,
            now=now,
        )
        with pytest.raises(InvariantViolation, match="cannot mutate canonical memory"):
            await bridge_validated_think_lifecycle(
                conn,
                tenant_id=tenant_id,
                arm=arm,
                batch_number=6,
                think_run_id=run_id,
            )
    finally:
        await tx.rollback()
        await conn.close()


async def test_missing_counterevidence_and_failed_runs_fail_closed() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL required")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        tenant_id, admitted, _, now = await _setup(conn)
        missing = uuid4()
        run_id = await _think_run(
            conn,
            tenant_id=tenant_id,
            model_id=admitted.model_id,
            observation_id=missing,
            now=now,
        )
        with pytest.raises(InvariantViolation, match="same-tenant observations"):
            await bridge_validated_think_lifecycle(
                conn,
                tenant_id=tenant_id,
                arm="adaptive",
                batch_number=6,
                think_run_id=run_id,
            )
        failed_id = await _think_run(
            conn,
            tenant_id=tenant_id,
            model_id=admitted.model_id,
            observation_id=missing,
            now=now,
            status="failed",
        )
        with pytest.raises(InvariantViolation, match="successful durable Think run"):
            await bridge_validated_think_lifecycle(
                conn,
                tenant_id=tenant_id,
                arm="adaptive",
                batch_number=6,
                think_run_id=failed_id,
            )
    finally:
        await tx.rollback()
        await conn.close()


async def test_arm_policy_is_exact() -> None:
    assert arm_allows_reasoning("frozen", 3)
    assert not arm_allows_reasoning("frozen", 4)
    assert arm_allows_reasoning("observation_only", 1)
    assert arm_allows_reasoning("memory_hidden", 12)
    assert not arm_allows_canonical_mutation("frozen", 4)
    assert not arm_allows_canonical_mutation("observation_only", 1)
    assert arm_allows_canonical_mutation("adaptive", 12)
    assert arm_memory_visible("adaptive")
    assert not arm_memory_visible("memory_hidden")
