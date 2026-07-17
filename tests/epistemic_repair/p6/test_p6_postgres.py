from __future__ import annotations

import os
from uuid import uuid4

import asyncpg
import pytest
from pydantic import ValidationError

from lib.evaluation.epistemic_repair.p6_runner import P6Artifact, run_p6_mixed_stream


pytestmark = pytest.mark.asyncio


async def test_p6_mixed_stream_closes_db_backed_gates() -> None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for P6 PostgreSQL proof")
    conn = await asyncpg.connect(dsn)
    tx = conn.transaction()
    await tx.start()
    try:
        artifact = await run_p6_mixed_stream(conn, tenant_id=uuid4(),
                                             commit_sha="test-commit")
    finally:
        await tx.rollback()
        await conn.close()
    assert not artifact.phase_exit_ready
    assert len(artifact.signal_fates) == 300
    assert len(artifact.batch_snapshots) == 12
    assert artifact.database_evidence["observation_count"] == 300
    assert artifact.database_evidence["decision_count"] == 300
    assert artifact.database_evidence["barrier_count"] == 12
    assert artifact.database_evidence["accepted_model_count"] == 4
    assert artifact.calibration_status == "insufficient_population"
    assert artifact.hard_gates["P6-HG-03-zero-high-consequence-incidents"].status == "fail"
    assert artifact.hard_gates["P6-HG-08-four-coherent-theses"].status == "fail"
    assert artifact.continuous_metrics["sufficient_context_recall"].status == "fail"
    assert artifact.continuous_metrics["boundary_b_cubed_f1"].status == "unmeasured"

    forged = artifact.model_dump(mode="json")
    forged["database_evidence"]["observation_count"] = 299
    with pytest.raises(ValidationError, match="digest mismatch"):
        P6Artifact.model_validate(forged)
