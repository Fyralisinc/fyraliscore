from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from scripts.run_company_learning_retention_db import (
    ARTIFACT_NAME,
    run_company_learning_retention_experiment,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_retention_survives_unrelated_learning_and_worker_restarts(
    resolver_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    report = await run_company_learning_retention_experiment(
        pool=resolver_db,
        output_dir=tmp_path,
        run_id="pytest-company-learning-retention",
        system_version="pytest",
    )

    assert report.status == "observed"
    assert report.exact_retention_rate == 1.0
    assert report.variant_retention_rate == 1.0
    assert report.corrected_retention_rate == 1.0
    assert report.overall_forgetting_rate == 0.0
    assert report.restart_survival_rate == 1.0
    assert report.correction_authority_rate == 1.0
    assert report.unsafe_globalization_rate == 0.0
    assert report.negative_control_safety_rate == 1.0
    assert report.collision_control_safety_rate == 1.0
    assert report.source_immutability_rate == 1.0
    assert report.model_consistency_rate == 1.0
    assert report.evidence_lineage_consistency_rate == 1.0
    assert report.hard_safety_incident_rate == 0.0
    assert report.retention_horizon_auc == 1.0
    assert [row.cycle_count for row in report.horizon_metrics] == [0, 4, 16]
    assert report.family_counts["contextual_phrase_negative"] == 1
    assert report.family_counts["same_type_acronym_collision"] == 1

    payload = json.loads((tmp_path / ARTIFACT_NAME).read_text(encoding="utf-8"))
    assert payload["report_digest"] == report.digest
    assert payload["report"] == report.model_dump(mode="json")
    assert "deferred:remaining-five-collision-families" in report.artifact_refs
