from __future__ import annotations

from datetime import UTC, datetime

import pytest

from services.ingest.connector_runtime.resilience import (
    REQUIRED_RESILIENCE_SCENARIOS,
    ResilienceEvidence,
    assert_fleet_resilience_certified,
)


def test_resilience_certification_requires_every_scenario_per_version() -> None:
    now = datetime(2026, 8, 3, tzinfo=UTC)
    evidence = tuple(
        ResilienceEvidence(
            connector_id="fyralis/example",
            connector_version="1.0.0",
            scenario=scenario,
            passed=True,
            observed_at=now,
            evidence_ref=f"test://{scenario.value}",
        )
        for scenario in REQUIRED_RESILIENCE_SCENARIOS
    )
    assert_fleet_resilience_certified(
        evidence,
        connector_versions={"fyralis/example": "1.0.0"},
        now=now,
    )
    with pytest.raises(ValueError, match="missing"):
        assert_fleet_resilience_certified(
            evidence[:-1],
            connector_versions={"fyralis/example": "1.0.0"},
            now=now,
        )
