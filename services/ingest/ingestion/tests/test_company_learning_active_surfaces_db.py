from __future__ import annotations

import json
from pathlib import Path

import asyncpg

from lib.evaluation.company_learning_active_surfaces import (
    validate_active_learning_surfaces_artifact,
)
from scripts.run_company_learning_active_surfaces_db import (
    ARTIFACT_NAME,
    run_active_surfaces_experiment,
)


async def test_active_learning_surfaces_produce_complete_postgres_evidence(
    gateway_pool: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    evidence = await run_active_surfaces_experiment(
        pool=gateway_pool,
        output_dir=tmp_path,
        run_id="pytest-active-surfaces",
        system_version="pytest-system",
    )

    assert evidence.report.status == "observed"
    assert evidence.report.structured_identity.observed_case_count == 4
    assert evidence.report.structured_identity.violating_case_count == 0
    assert evidence.report.source_salience.observed_case_count == 5
    assert evidence.report.source_salience.violating_case_count == 0
    assert evidence.report.source_salience.salience_direction_rate.point_estimate == 1.0
    payload = json.loads((tmp_path / ARTIFACT_NAME).read_text())
    assert validate_active_learning_surfaces_artifact(payload) == evidence
