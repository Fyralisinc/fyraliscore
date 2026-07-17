import ast
import os
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from lib.shared.errors import InvariantViolation
from lib.evaluation.epistemic_repair.p6_think_runner import (
    _complete_and_reopen_barrier, _snapshot,
)


def test_production_think_runner_cannot_import_or_read_sealed_gold() -> None:
    source = Path(
        "lib/evaluation/epistemic_repair/p6_think_runner.py"
    ).read_text()
    tree = ast.parse(source)
    imported = {
        alias.name
        for node in ast.walk(tree)
        if isinstance(node, (ast.Import, ast.ImportFrom))
        for alias in node.names
    }
    attributes = {
        node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)
    }
    assert "P6Gold" not in imported
    assert not {"gold", "synthesis_signal_by_storyline", "thesis_by_storyline"} & attributes


def test_production_think_runner_requires_real_batch_worker() -> None:
    source = Path(
        "lib/evaluation/epistemic_repair/p6_think_runner.py"
    ).read_text()
    assert "_process_one_t1_batch" in source
    assert "ThinkWorker" in source
    assert "t1_batch_max_size=25" in source
    assert "DeepSeek" not in source
    assert "P6 production proof requires a clean pinned worktree" in source


@pytest.mark.asyncio
async def test_p6_barrier_separates_eventual_work_and_rejects_truth_pending() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the P6 barrier DB proof")
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    tenant_id, completed_trigger, pending_trigger = uuid4(), uuid4(), uuid4()
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                tenant_id, "p6-barrier-proof",
            )
            await conn.execute("""
                INSERT INTO think_trigger_queue
                  (id,tenant_id,trigger_kind,payload,completed_at)
                VALUES ($1,$2,'observation','{}'::jsonb,now())
            """, completed_trigger, tenant_id)
            await conn.execute("""
                INSERT INTO pending_post_commit_actions
                  (tenant_id,trigger_id,action_kind,action_payload)
                VALUES ($1,$2,'materialize_projections','{}'::jsonb)
            """, tenant_id, completed_trigger)
        snapshot = await _snapshot(pool, tenant_id)
        assert snapshot["pending_work"] == {
            "truth_critical": {
                "total": 0, "by_queue": {"think_trigger_queue": 0},
            },
            "eventual_derived": {
                "total": 1,
                "by_action_kind": {"materialize_projections": 1},
            },
        }
        async with pool.acquire() as conn, conn.transaction():
            receipt, current = await _complete_and_reopen_barrier(
                conn, tenant_id=tenant_id, batch_number=1,
                previous_model_versions=set(),
            )
        assert current == set()
        assert receipt["barrier_version"] == 1
        assert receipt["truth_critical_pending_count"] == 0
        assert receipt["reopened_exactly"] is True
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO think_trigger_queue(id,tenant_id,trigger_kind,payload)
                VALUES ($1,$2,'observation','{}'::jsonb)
            """, pending_trigger, tenant_id)
            async with conn.transaction():
                with pytest.raises(InvariantViolation, match="truth-critical"):
                    await _complete_and_reopen_barrier(
                        conn, tenant_id=tenant_id, batch_number=2,
                        previous_model_versions=set(),
                    )
    finally:
        async with pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM pending_post_commit_actions WHERE tenant_id=$1",
                tenant_id,
            )
            await conn.execute(
                "DELETE FROM think_trigger_queue WHERE tenant_id=$1", tenant_id,
            )
            await conn.execute("DELETE FROM tenants WHERE id=$1", tenant_id)
        await pool.close()
