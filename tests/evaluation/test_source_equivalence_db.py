from __future__ import annotations

import json
from uuid import uuid4

import pytest

from services.source_equivalence_db_vertical import run_source_equivalence_db_vertical


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_source_equivalence_runs_through_persisted_worker_batch(fresh_db, tmp_path):
    tenant_id = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id,name) VALUES ($1,'source-equivalence-db-test')",
        tenant_id,
    )
    output = tmp_path / "source-equivalence.json"
    result = await run_source_equivalence_db_vertical(
        pool=fresh_db, tenant_id=tenant_id, output_path=output,
    )

    assert json.loads(output.read_text())["objective_sha256"] == result[
        "objective_sha256"
    ]
    assert result["population"] == {
        "signal_batches": 1, "signals": 4, "sources": 4,
    }
    assert result["worker"] == {
        "claimed": 4, "belief_applied": 4, "no_admission": 0,
        "terminal_failures": 0,
    }
    report = result["evaluation"]
    assert report["measurements"]["entity_outcome_similarity"] == 1.0
    assert report["measurements"]["model_outcome_similarity"] == 1.0
    assert report["measurements"]["source_authority_fidelity"] == 1.0
    assert report["measurements"]["source_coordinate_fidelity"] == 1.0
    assert report["measurements"]["conversational_boundary_fidelity"] == 1.0
    assert report["measurements"]["learning_outcome_lineage"] == 1.0
    assert report["measurements"]["relation_outcome_exposure"] == 0.0
    assert report["measurements"]["semantic_outcome_coverage"] == pytest.approx(2 / 3)
    assert report["observed_quality_score"] == 1.0
    assert report["continuous_score"] == pytest.approx(2 / 3)
    assert report["checks"]["relation_outcomes_exposed"] is False
    assert report["verdict"] == "below_policy"
    assert report["proof_gaps"] == ["relation_outcome_population_unexposed"]
