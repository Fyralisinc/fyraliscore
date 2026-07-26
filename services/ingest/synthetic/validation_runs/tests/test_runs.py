"""Contract-derived validation-run catalog coverage."""

from __future__ import annotations

import pytest

from services.ingest.source_certification.runtime import (
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
)
from services.ingest.source_contract.catalog import (
    CANONICAL_SOURCE_IDS,
    SOURCE_DEFINITIONS,
    source_definition,
)
from services.ingest.synthetic.validation_runs.runs import (
    certification_history_scenarios,
)


def test_run1_covers_all_26_history_capable_canonical_sources() -> None:
    expected_sources = tuple(
        source.source_id for source in SOURCE_DEFINITIONS if source.history is not None
    )

    scenarios = certification_history_scenarios(tenants_per_source=1)

    assert len(CANONICAL_SOURCE_IDS) == 27
    assert len(expected_sources) == 26
    assert tuple(scenario.source for scenario in scenarios) == expected_sources


def test_run1_ids_and_slugs_are_stable_in_canonical_order() -> None:
    first = certification_history_scenarios(tenants_per_source=2)
    second = certification_history_scenarios(tenants_per_source=2)

    assert first == second
    assert [(scenario.source, scenario.tenant_slug) for scenario in first] == [
        (source.source_id, f"val-{source.source_id}-{tenant_index}")
        for source in SOURCE_DEFINITIONS
        if source.history is not None
        for tenant_index in range(2)
    ]
    assert all(scenario.fixture_params == {} for scenario in first)


def test_whatsapp_is_the_explicit_history_exclusion() -> None:
    unsupported = tuple(
        source.source_id for source in SOURCE_DEFINITIONS if source.history is None
    )
    scenario_sources = {
        scenario.source
        for scenario in certification_history_scenarios(tenants_per_source=1)
    }

    assert unsupported == ("whatsapp",)
    assert source_definition("whatsapp").history is None
    assert "whatsapp" not in scenario_sources


def test_certification_history_counts_are_positive_exact_and_deterministic() -> None:
    first = certification_history_scenarios(tenants_per_source=1)
    second = certification_history_scenarios(tenants_per_source=1)

    assert first == second
    assert all(scenario.expected_observation_count > 0 for scenario in first)
    for scenario in first:
        installation_id = f"x3-{scenario.tenant_slug}-{scenario.source}"
        fixture = resolve_fixture_factory(scenario.source)(
            fixture_params=scenario.fixture_params,
            installation_id=installation_id,
        )
        assert scenario.expected_observation_count == (
            resolve_fixture_count_oracle(scenario.source)(fixture)
        )


def test_certification_history_has_no_zero_count_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import services.ingest.synthetic.validation_runs.runs as runs

    real_resolver = runs.resolve_fixture_count_oracle

    def _resolve(source_id: str):  # noqa: ANN202
        if source_id == "slack":
            return lambda _fixture: 0
        return real_resolver(source_id)

    monkeypatch.setattr(runs, "resolve_fixture_count_oracle", _resolve)

    with pytest.raises(
        ValueError,
        match="expected_observation_count",
    ):
        certification_history_scenarios(tenants_per_source=1)


def test_all_history_count_oracles_fail_closed_on_unknown_fixture_shape() -> None:
    history_sources = (
        source.source_id for source in SOURCE_DEFINITIONS if source.history is not None
    )

    for source_id in history_sources:
        with pytest.raises(
            ValueError,
            match=rf"{source_id} fixture has no exact Observation count",
        ):
            resolve_fixture_count_oracle(source_id)({})
