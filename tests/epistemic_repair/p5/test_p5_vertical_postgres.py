from __future__ import annotations

import copy
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.evaluation.epistemic_repair.p5_oracles import build_p5_artifact
from lib.evaluation.epistemic_repair.p5_population import build_p5_population
from lib.evaluation.epistemic_repair.p5_runner import run_p5_vertical


pytestmark = pytest.mark.asyncio


async def _run_artifact():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the P5 PostgreSQL proof")
    conn = await asyncpg.connect(dsn)
    transaction = conn.transaction()
    await transaction.start()
    try:
        return await run_p5_vertical(conn, tenant_id=uuid4())
    finally:
        await transaction.rollback()
        await conn.close()


async def test_p5_vertical_closes_all_gates_from_queried_database_evidence() -> None:
    artifact = await _run_artifact()

    assert artifact.phase_exit_ready
    assert all(gate.status == "pass" for gate in artifact.hard_gates.values())
    assert all(metric.threshold_met for metric in artifact.continuous_metrics.values())
    assert len(artifact.signal_receipts) == 75
    assert artifact.database_evidence["observation_count"] == 75
    assert artifact.database_evidence["context_decision_count"] == 75
    assert artifact.database_evidence["grounding_trace_count"] == 3
    assert artifact.database_evidence["source_semantic_belief_admission_count"] == 3
    assert artifact.database_evidence["model_truth_version_count"] == 4
    assert artifact.database_evidence["accepted_model_count"] == 2
    assert artifact.database_evidence["accepted_relation_count"] == 0
    assert artifact.database_evidence["repair_obligation_count"] >= 1
    assert artifact.database_evidence["barrier_count"] == 3
    assert artifact.database_evidence["cross_tenant_contamination_count"] == 0


async def test_oracle_rejects_missing_signal_row_and_forged_accepted_state() -> None:
    artifact = await _run_artifact()
    population = build_p5_population()
    arguments = {
        "population": population,
        "signals": artifact.signal_receipts,
        "vertical": artifact.vertical_receipt,
        "barriers": artifact.barrier_receipts,
        "zero_seed_initial_model_count": artifact.zero_seed_initial_model_count,
        "provider_call_count": artifact.provider_call_count,
        "timings_ms": artifact.timings_ms,
    }

    missing_signal = copy.deepcopy(artifact.database_evidence)
    missing_signal["signal_rows"] = missing_signal["signal_rows"][:-1]
    with pytest.raises(ValueError, match="duplicate, missing, or extra"):
        build_p5_artifact(database_evidence=missing_signal, **arguments)

    forged_visibility = copy.deepcopy(artifact.database_evidence)
    forged_visibility["accepted_model_version_ids"].append(
        artifact.vertical_receipt.batch_1_model_version_id
    )
    with pytest.raises(ValueError, match="lifecycle/accepted-view"):
        build_p5_artifact(database_evidence=forged_visibility, **arguments)

    forged_preflight = copy.deepcopy(artifact.database_evidence)
    forged_preflight["preflight"]["accepted_model_count"] = 1
    with pytest.raises(ValueError, match="zero-seed"):
        build_p5_artifact(database_evidence=forged_preflight, **arguments)
