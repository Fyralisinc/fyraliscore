"""Integrated correction propagation through the executable assurance harness."""

from __future__ import annotations

import json
from pathlib import Path

import asyncpg
import pytest

from lib.evaluation.correction_assurance import (
    validate_correction_assurance_artifact,
)
from scripts.run_company_learning_correction_harness import (
    run_company_learning_correction_harness,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_direct_correction_fence_is_atomic_isolated_and_idempotent(
    fresh_db: asyncpg.Pool,
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "correction-assurance"
    artifact = await run_company_learning_correction_harness(
        pool=fresh_db,
        output_dir=output_dir,
        run_id="pytest-correction-end-state",
        system_version="pytest",
    )

    assert artifact.status == "working"
    assert artifact.metrics.expected_dependency_count == 6
    assert artifact.metrics.dependency_discovery_rate == 1.0
    assert artifact.metrics.immediate_fence_rate == 1.0
    assert artifact.metrics.direct_repair_rate == 1.0
    assert artifact.metrics.recursive_repair_rate == 1.0
    assert artifact.metrics.relation_retirement_rate == 1.0
    assert artifact.metrics.projection_invalidation_rate == 1.0
    assert artifact.metrics.projection_rebuild_rate == 1.0
    assert artifact.metrics.residual_unsafe_debt_count == 0
    assert artifact.metrics.replay_idempotent is True
    assert artifact.metrics.source_immutable is True
    assert artifact.metrics.tenant_isolated is True
    assert artifact.metrics.converged is True
    assert set(artifact.component_digests) == {"evidence", "audit"}

    persisted = validate_correction_assurance_artifact(
        json.loads(
            (output_dir / "correction_assurance.json").read_text(
                encoding="utf-8"
            )
        )
    )
    assert persisted == artifact
