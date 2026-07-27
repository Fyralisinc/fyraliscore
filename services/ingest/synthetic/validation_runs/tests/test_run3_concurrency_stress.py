"""Contract coverage for the Run 3 concurrency matrix."""
from __future__ import annotations

from collections import Counter

import pytest

from services.ingest.source_certification.runtime import (
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.backfill_harness.scenarios import (
    BackfillScenario,
)
from services.ingest.synthetic.validation_runs import (
    run3_concurrency_stress as run3_module,
)


def test_run3_has_two_tenants_and_two_installs_for_every_history_source() -> None:
    scenarios = run3_module.run3_scenarios()
    expected_sources = tuple(
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is not None
    )

    assert len(expected_sources) == 26
    assert len(scenarios) == 104
    assert tuple(dict.fromkeys(s.source for s in scenarios)) == expected_sources
    assert Counter(s.source for s in scenarios) == Counter(
        {source_id: 4 for source_id in expected_sources},
    )
    assert all(s.installation_key is not None for s in scenarios)
    assert len({s.identity for s in scenarios}) == len(scenarios)
    assert "whatsapp" not in {s.source for s in scenarios}


def test_run3_uses_each_sources_exact_fixture_count_oracle() -> None:
    first = run3_module.run3_scenarios()
    second = run3_module.run3_scenarios()

    assert first == second
    for scenario in first:
        installation_id = (
            f"x3-{scenario.resolved_installation_key}-{scenario.source}"
        )
        fixture = resolve_fixture_factory(scenario.source)(
            fixture_params=scenario.fixture_params,
            installation_id=installation_id,
        )
        assert scenario.expected_observation_count > 0
        assert scenario.expected_observation_count == (
            resolve_fixture_count_oracle(scenario.source)(fixture)
        )


@pytest.mark.parametrize("invalid_count", [0, -1, True])
def test_run3_fails_closed_on_non_positive_or_non_exact_count(
    monkeypatch: pytest.MonkeyPatch,
    invalid_count: int,
) -> None:
    monkeypatch.setattr(
        run3_module,
        "certification_history_scenarios",
        lambda *, tenants_per_source, installations_per_tenant: [
            BackfillScenario(
                tenant_slug=(
                    f"invalid-{tenants_per_source}-"
                    f"{installations_per_tenant}"
                ),
                source="slack",
                expected_observation_count=invalid_count,
            ),
        ],
    )

    with pytest.raises(
        ValueError,
        match="positive exact integer",
    ):
        run3_module.run3_scenarios()


def test_run3_source_grouping_preserves_contract_scenario_order() -> None:
    scenarios = run3_module.run3_scenarios()

    grouped = run3_module._group_scenarios(scenarios)

    assert tuple(grouped) == tuple(
        dict.fromkeys(scenario.source for scenario in scenarios),
    )
    assert all(len(source_scenarios) == 4 for source_scenarios in grouped.values())
    assert [
        scenario for source_scenarios in grouped.values()
        for scenario in source_scenarios
    ] == scenarios


def test_working_signal_bound_tracks_exact_shard_fanout() -> None:
    assert run3_module._working_signal_bound(
        planned_shards=408,
        installations=104,
    ) == 512
    assert run3_module._working_signal_bound(
        planned_shards=1,
        installations=1,
    ) == 2

    with pytest.raises(ValueError, match="non-negative"):
        run3_module._working_signal_bound(
            planned_shards=-1,
            installations=1,
        )
