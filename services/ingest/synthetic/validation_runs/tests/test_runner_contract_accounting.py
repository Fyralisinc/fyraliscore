"""Focused, non-E2E tests for Run 1's contract-derived accounting."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.backfill_harness.harness import TenantOutcome
from services.ingest.synthetic.backfill_harness.scenarios import BackfillScenario
from services.ingest.synthetic.validation_runs import composition as C
from services.ingest.synthetic.validation_runs.runner import (
    _contract_source_membership,
    _expected_source_observations,
    _live_targets_from_outcomes,
    _require_exact_fixture_counts,
    _run1_coverage_rows,
    _run_ok,
    _validate_run1_scenario_membership,
)
from services.ingest.synthetic.validation_runs.reports import RunReport


def _scenario(
    source: str,
    *,
    expected: int,
    tenant_slug: str | None = None,
    fixture_params: dict | None = None,
) -> BackfillScenario:
    return BackfillScenario(
        tenant_slug=tenant_slug or f"runner-{source}",
        source=source,
        fixture_params=fixture_params or {},
        expected_observation_count=expected,
    )


def _outcome(scenario: BackfillScenario, *, fixture: dict | None) -> TenantOutcome:
    return TenantOutcome(
        scenario=scenario,
        tenant_id=uuid4(),
        fixture=fixture,
    )


def test_run1_membership_matches_every_history_capable_contract_source() -> None:
    historical_definitions = tuple(
        definition
        for definition in SOURCE_DEFINITIONS
        if definition.history is not None
    )
    scenarios = [
        _scenario(
            definition.source_id,
            expected=1,
            tenant_slug=f"runner-{definition.source_id}-{tenant_index}",
        )
        for definition in historical_definitions
        for tenant_index in range(2)
    ]

    source_ids = _validate_run1_scenario_membership(
        scenarios,
        tenants_per_source=2,
    )

    expected = tuple(
        definition.source_id
        for definition in historical_definitions
    )
    assert source_ids == expected
    assert len(source_ids) == 26
    assert len(scenarios) == 2 * len(source_ids)


def test_live_target_uses_the_resolved_fixture_not_scenario_params() -> None:
    scenario = _scenario(
        "gmail",
        expected=3,
        fixture_params={"email": "unresolved@example.test"},
    )
    outcome = _outcome(
        scenario,
        fixture={"email": "resolved@provider-lab.test"},
    )

    targets = _live_targets_from_outcomes([outcome])

    assert targets[0].tenant_id == outcome.tenant_id
    assert targets[0].email == "resolved@provider-lab.test"


def test_live_target_fails_closed_without_a_resolved_fixture() -> None:
    outcome = _outcome(_scenario("gmail", expected=3), fixture=None)

    with pytest.raises(RuntimeError, match="resolved certification fixture"):
        _live_targets_from_outcomes([outcome])


def test_exact_count_validation_rejects_an_unresolved_oracle() -> None:
    unresolved = _scenario("jira", expected=0)

    with pytest.raises(RuntimeError, match="exact source-owned fixture"):
        _require_exact_fixture_counts([unresolved])


def test_per_source_accounting_uses_oracle_live_and_declared_replay_counts() -> None:
    slack_outcomes = [
        _outcome(_scenario("slack", expected=3, tenant_slug="slack-a"), fixture={}),
        _outcome(_scenario("slack", expected=4, tenant_slug="slack-b"), fixture={}),
    ]

    expected = _expected_source_observations(
        "slack",
        slack_outcomes,
        events_per_tenant=2,
        replay={"slack": {"dispatched_unique": 1, "observed": 1}},
    )

    assert expected == 3 + 4 + (2 * 2) + 1


def test_per_source_accounting_requires_each_declared_replay_result() -> None:
    outcome = _outcome(_scenario("github", expected=2), fixture={})

    with pytest.raises(RuntimeError, match="replay accounting is missing"):
        _expected_source_observations(
            "github",
            [outcome],
            events_per_tenant=1,
            replay={},
        )


def test_coverage_is_canonical_and_capability_scoped_for_live_only_source() -> None:
    historical, live_only = _contract_source_membership()
    live_sources = (*historical, *live_only)

    rows = _run1_coverage_rows(
        historical_sources=historical,
        live_sources=live_sources,
        live_only_sources=live_only,
        twin_covered_sources=("gmail", "github", "slack"),
        signature_covered_sources=C.HMAC_SOURCES,
        replay_covered_sources=("gmail", "github", "slack"),
    )
    by_source = {row[0]: row for row in rows}

    assert tuple(row[0] for row in rows) == tuple(
        definition.source_id for definition in SOURCE_DEFINITIONS
    )
    assert len(rows) == 27
    assert live_only == ("whatsapp",)
    assert by_source["whatsapp"] == (
        "whatsapp",
        "— (history=None)",
        "✅",
        "— (not in TWIN_SOURCES)",
        "✅",
        "— (not in REPLAY_SOURCES)",
    )
    assert by_source["slack"][1:] == ("✅", "✅", "✅", "✅", "✅")
    assert by_source["gmail"][3] == "✅"
    assert by_source["gmail"][4] == "— (not in HMAC_SOURCES)"
    assert by_source["discord"][3:] == (
        "— (not in TWIN_SOURCES)",
        "— (not in HMAC_SOURCES)",
        "— (not in REPLAY_SOURCES)",
    )


def test_partial_verdict_is_not_a_passing_certification_state() -> None:
    report = RunReport(
        run_name="fault recovery",
        run_number=2,
        tenant_count=1,
        started_at=dt.datetime.now(tz=dt.timezone.utc),
        wall_seconds=1.0,
        verdict="PARTIAL",
    )

    assert _run_ok(report) is False
    report.verdict = "READY"
    assert _run_ok(report) is True
