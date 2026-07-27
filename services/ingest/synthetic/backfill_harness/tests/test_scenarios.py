"""Unit tests for BackfillScenario validation + dataclass shape."""
from __future__ import annotations

import pytest

from services.ingest.synthetic.backfill_harness import BackfillScenario
from services.ingest.synthetic.fault_profiles import HAPPY_PATH, RATE_LIMITED


def test_scenario_requires_valid_source() -> None:
    with pytest.raises(ValueError, match="source must be one of"):
        BackfillScenario(tenant_slug="t1", source="email")


def test_scenario_requires_non_empty_slug() -> None:
    with pytest.raises(ValueError, match="tenant_slug is required"):
        BackfillScenario(tenant_slug="", source="gmail")


def test_scenario_defaults() -> None:
    s = BackfillScenario(tenant_slug="t1", source="gmail")
    assert s.installation_key is None
    assert s.resolved_installation_key == "t1"
    assert s.identity == ("gmail", "t1", "t1")
    assert s.fixture_params == {}
    assert s.fault_profile == HAPPY_PATH
    assert s.expected_observation_count == 0


def test_scenario_accepts_custom_fault_profile() -> None:
    s = BackfillScenario(
        tenant_slug="t1", source="slack", fault_profile=RATE_LIMITED,
    )
    assert s.fault_profile == RATE_LIMITED


def test_scenario_supports_exact_sibling_installation_identity() -> None:
    first = BackfillScenario(
        tenant_slug="shared",
        source="slack",
        installation_key="shared-installation-0",
    )
    second = BackfillScenario(
        tenant_slug="shared",
        source="slack",
        installation_key="shared-installation-1",
    )

    assert first.identity != second.identity
    assert first.tenant_slug == second.tenant_slug


def test_scenario_rejects_empty_installation_key() -> None:
    with pytest.raises(ValueError, match="installation_key must be non-empty"):
        BackfillScenario(
            tenant_slug="shared",
            source="slack",
            installation_key="",
        )


def test_scenario_is_frozen() -> None:
    s = BackfillScenario(tenant_slug="t1", source="gmail")
    with pytest.raises(Exception):  # FrozenInstanceError
        s.tenant_slug = "t2"  # type: ignore[misc]
