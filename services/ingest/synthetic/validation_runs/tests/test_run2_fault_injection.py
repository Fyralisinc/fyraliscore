"""Focused contract tests for the Run 2 fault matrix."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import pytest

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.fault_profiles import FLAKY
from services.ingest.synthetic.validation_runs.run2_fault_injection import (
    _historical_live_targets,
    run2_scenarios,
)


def test_run2_covers_every_history_capable_source_with_exact_counts() -> None:
    scenarios = run2_scenarios(tenants_per_source=1)
    expected_sources = tuple(
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is not None
    )

    assert tuple(scenario.source for scenario in scenarios) == expected_sources
    assert len(scenarios) == 26
    assert all(scenario.expected_observation_count > 0 for scenario in scenarios)
    assert all(scenario.fault_profile == FLAKY for scenario in scenarios)


def test_run2_live_target_uses_the_resolved_fixture() -> None:
    tenant_id = uuid4()
    outcome = SimpleNamespace(
        tenant_id=tenant_id,
        fixture={"email": "resolved@provider-lab.test"},
        scenario=SimpleNamespace(
            source="gmail",
            tenant_slug="fault-gmail",
            fixture_params={"email": "stale@example.test"},
        ),
    )

    target = _historical_live_targets([outcome])[0]

    assert target.tenant_id == tenant_id
    assert target.email == "resolved@provider-lab.test"


def test_run2_live_target_fails_closed_without_resolved_fixture() -> None:
    outcome = SimpleNamespace(
        tenant_id=uuid4(),
        fixture=None,
        scenario=SimpleNamespace(
            source="gmail",
            tenant_slug="fault-gmail",
        ),
    )

    with pytest.raises(RuntimeError, match="resolved fixture"):
        _historical_live_targets([outcome])
