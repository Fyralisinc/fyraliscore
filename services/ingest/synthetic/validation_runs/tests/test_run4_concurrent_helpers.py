"""Focused contract/accounting tests for concurrent validation Run 4."""

from __future__ import annotations

from collections import Counter
from uuid import UUID, uuid4

import pytest

from services.ingest.source_certification.runtime import (
    resolve_fixture_count_oracle,
    resolve_fixture_factory,
)
from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.backfill_harness.harness import TenantOutcome
from services.ingest.synthetic.backfill_harness.scenarios import (
    BackfillScenario,
)
from services.ingest.synthetic.validation_runs.composition import (
    LiveDispatchResult,
    LiveTarget,
)
from services.ingest.synthetic.validation_runs.reports import RunReport
from services.ingest.synthetic.validation_runs import (
    run4_concurrent as run4_module,
)


_TENANT = UUID("aaaaaaaa-1111-7777-8888-bbbbbbbbbbbb")
_WHATSAPP_TENANT = UUID("cccccccc-2222-7777-8888-dddddddddddd")


def _scenario(
    *,
    source: str = "gmail",
    slug: str = "val-gmail-0",
    expected: int = 5,
    fixture_params: dict | None = None,
) -> BackfillScenario:
    return BackfillScenario(
        tenant_slug=slug,
        source=source,
        expected_observation_count=expected,
        fixture_params=fixture_params or {},
    )


def _outcome(
    *,
    tenant_id: UUID = _TENANT,
    source: str = "gmail",
    slug: str = "val-gmail-0",
    expected: int = 5,
    fixture_params: dict | None = None,
    fixture: dict | None = None,
    completion_signal_count: int = 1,
) -> TenantOutcome:
    return TenantOutcome(
        tenant_id=tenant_id,
        scenario=_scenario(
            source=source,
            slug=slug,
            expected=expected,
            fixture_params=fixture_params,
        ),
        fixture=fixture,
        completion_signal_count=completion_signal_count,
    )


def _target(
    source: str,
    *,
    tenant_id: UUID | None = None,
    slug: str | None = None,
) -> LiveTarget:
    return LiveTarget(
        tenant_id=tenant_id or uuid4(),
        source=source,
        slug=slug or f"run4-{source}",
    )


def _report() -> RunReport:
    return run4_module._run4_report(
        scenarios=[],
        live_only_tenant_count=0,
    )


def test_run4_has_two_tenants_for_every_historical_contract_source() -> None:
    scenarios = run4_module.run4_scenarios()
    expected_sources = tuple(
        definition.source_id
        for definition in SOURCE_DEFINITIONS
        if definition.history is not None
    )

    assert len(expected_sources) == 26
    assert len(scenarios) == 52
    assert tuple(dict.fromkeys(s.source for s in scenarios)) == expected_sources
    assert Counter(s.source for s in scenarios) == Counter(
        {source_id: 2 for source_id in expected_sources},
    )
    assert all(s.expected_observation_count > 0 for s in scenarios)
    assert "whatsapp" not in {scenario.source for scenario in scenarios}


def test_run4_uses_every_sources_exact_fixture_count_oracle() -> None:
    scenarios = run4_module.run4_scenarios()

    for scenario in scenarios:
        installation_id = f"x3-{scenario.tenant_slug}-{scenario.source}"
        fixture = resolve_fixture_factory(scenario.source)(
            fixture_params=scenario.fixture_params,
            installation_id=installation_id,
        )
        assert scenario.expected_observation_count == (
            resolve_fixture_count_oracle(scenario.source)(fixture)
        )


@pytest.mark.parametrize("invalid_count", [0, -1, True])
def test_run4_fails_closed_on_non_positive_or_non_exact_count(
    monkeypatch: pytest.MonkeyPatch,
    invalid_count: int,
) -> None:
    historical_sources, _ = run4_module._contract_source_membership()
    scenarios = [
        _scenario(
            source=source,
            slug=f"run4-{source}-{tenant_index}",
            expected=(invalid_count if source == "slack" else 1),
        )
        for source in historical_sources
        for tenant_index in range(2)
    ]
    monkeypatch.setattr(
        run4_module,
        "certification_history_scenarios",
        lambda *, tenants_per_source: scenarios,
    )

    with pytest.raises(ValueError, match="positive exact integer"):
        run4_module.run4_scenarios()


def test_run4_report_is_dynamic_for_contract_wide_tenant_count() -> None:
    scenarios = run4_module.run4_scenarios()

    report = run4_module._run4_report(
        scenarios=scenarios,
        live_only_tenant_count=2,
    )

    assert report.run_number == 4
    assert report.tenant_count == 54
    assert "production clients" in report.run_name
    assert "Provider Lab" in report.run_name
    assert "54 tenants" in report.run_name
    assert "27 canonical sources" in report.run_name


def test_historical_live_target_uses_resolved_fixture_not_scenario_params() -> None:
    outcome = _outcome(
        fixture_params={"email": "unresolved@example.test"},
        fixture={"email": "resolved@provider-lab.test"},
    )

    target = run4_module._live_targets_from_outcomes([outcome])[0]

    assert target.tenant_id == _TENANT
    assert target.source == "gmail"
    assert target.slug == "val-gmail-0"
    assert target.email == "resolved@provider-lab.test"


def test_historical_live_target_fails_closed_without_resolved_fixture() -> None:
    with pytest.raises(RuntimeError, match="resolved certification fixture"):
        run4_module._live_targets_from_outcomes(
            [
                _outcome(fixture=None),
            ]
        )


def test_every_historical_fixture_resolves_to_a_live_target() -> None:
    outcomes = []
    for scenario in run4_module.run4_scenarios():
        installation_id = f"x3-{scenario.tenant_slug}-{scenario.source}"
        fixture = resolve_fixture_factory(scenario.source)(
            fixture_params=scenario.fixture_params,
            installation_id=installation_id,
        )
        outcomes.append(
            TenantOutcome(
                scenario=scenario,
                tenant_id=uuid4(),
                fixture=fixture,
            )
        )

    targets = run4_module._live_targets_from_outcomes(outcomes)

    assert len(targets) == 52
    assert Counter(target.source for target in targets) == Counter(
        {
            definition.source_id: 2
            for definition in SOURCE_DEFINITIONS
            if definition.history is not None
        },
    )


def test_combined_totals_include_history_and_live_only_dispatch() -> None:
    historical = _outcome(fixture={"email": "resolved@example.test"}, expected=7)
    historical_target = _target(
        "gmail",
        tenant_id=historical.tenant_id,
        slug=historical.scenario.tenant_slug,
    )
    whatsapp_target = _target(
        "whatsapp",
        tenant_id=_WHATSAPP_TENANT,
        slug="val-whatsapp-live-0",
    )
    live = LiveDispatchResult(
        dispatched_by_tenant={
            historical.tenant_id: run4_module._LIVE_EVENTS_PER_TENANT,
            whatsapp_target.tenant_id: run4_module._LIVE_EVENTS_PER_TENANT,
        },
        dispatched_by_source={
            "gmail": run4_module._LIVE_EVENTS_PER_TENANT,
            "whatsapp": run4_module._LIVE_EVENTS_PER_TENANT,
        },
    )

    totals = run4_module._expected_combined_observation_totals(
        [historical],
        [historical_target, whatsapp_target],
        live,
    )

    assert totals == {
        historical.tenant_id: 7 + run4_module._LIVE_EVENTS_PER_TENANT,
        whatsapp_target.tenant_id: run4_module._LIVE_EVENTS_PER_TENANT,
    }


def test_combined_totals_fail_when_one_live_target_was_not_dispatched() -> None:
    historical = _outcome(fixture={"email": "resolved@example.test"})
    target = _target("gmail", tenant_id=historical.tenant_id)

    with pytest.raises(RuntimeError, match="per-source live dispatch"):
        run4_module._expected_combined_observation_totals(
            [historical],
            [target],
            LiveDispatchResult(),
        )


@pytest.mark.asyncio
async def test_source_results_follow_canonical_catalog_order() -> None:
    targets = [_target(definition.source_id) for definition in SOURCE_DEFINITIONS]
    expected_total = {
        target.tenant_id: run4_module._LIVE_EVENTS_PER_TENANT for target in targets
    }

    class _Pool:
        async def fetchval(self, _query: str, tenant_ids: list[UUID]) -> int:
            return sum(expected_total[tenant_id] for tenant_id in tenant_ids)

    report = _report()
    await run4_module._append_source_results(
        report,
        pool=_Pool(),  # type: ignore[arg-type]
        targets=targets,
        expected_total=expected_total,
    )

    assert tuple(result.source for result in report.source_results) == tuple(
        definition.source_id for definition in SOURCE_DEFINITIONS
    )
    assert len(report.source_results) == 27
    assert all(result.tenants == 1 for result in report.source_results)
    assert all(result.ok for result in report.source_results)


def test_live_dispatch_and_http_status_assertions_are_contract_derived() -> None:
    targets = [_target(definition.source_id) for definition in SOURCE_DEFINITIONS]
    required_http = run4_module._required_http_status_sources(
        {target.source for target in targets},
    )
    statuses = {
        source: run4_module._contract_http_ack_status(source)
        for source in required_http
    }
    report = _report()

    run4_module._append_live_dispatch_assertions(
        report,
        targets=targets,
        live_result=LiveDispatchResult(
            dispatched_by_source={
                target.source: run4_module._LIVE_EVENTS_PER_TENANT for target in targets
            },
            http_status_by_source={
                source: status
                for source, status in statuses.items()
                if status is not None
            },
        ),
    )

    assert len(required_http) > 4
    assert all(assertion.passed for assertion in report.assertions)
    assert run4_module._contract_http_ack_status("slack") == {202}
    assert run4_module._contract_http_ack_status("gmail") == {200}


def test_http_status_assertion_rejects_inline_ack_on_kafka_cutover() -> None:
    targets = [_target("slack")]
    report = _report()

    run4_module._append_live_dispatch_assertions(
        report,
        targets=targets,
        live_result=LiveDispatchResult(
            dispatched_by_source={
                "slack": run4_module._LIVE_EVENTS_PER_TENANT,
            },
            http_status_by_source={"slack": {200}},
        ),
    )

    status_assertion = next(
        assertion
        for assertion in report.assertions
        if assertion.name == "assert_http_ack_statuses_follow_ingress_contract"
    )
    assert status_assertion.passed is False
    assert "'actual': [200]" in status_assertion.detail


def test_completion_assertion_excludes_live_only_target() -> None:
    historical = _outcome(
        fixture={"email": "resolved@example.test"},
        completion_signal_count=1,
    )
    historical_target = _target(
        "gmail",
        tenant_id=historical.tenant_id,
        slug=historical.scenario.tenant_slug,
    )
    whatsapp_target = _target(
        "whatsapp",
        tenant_id=_WHATSAPP_TENANT,
        slug="val-whatsapp-live-0",
    )
    expected_total = {
        historical.tenant_id: 10,
        whatsapp_target.tenant_id: 5,
    }
    report = _report()

    run4_module._assert_run4(
        report,
        [historical.scenario],
        [historical],
        [historical_target, whatsapp_target],
        expected_total,
        expected_total,
        {"in_progress": 5, "backlog": 0},
        live_start=1.0,
        backfill_done_at=2.0,
    )

    completion = next(
        assertion
        for assertion in report.assertions
        if assertion.name.startswith(
            "assert_completion_fires_exactly_once_per_historical_tenant",
        )
    )
    assert completion.passed is True
    assert "live-only targets correctly excluded" in completion.detail
