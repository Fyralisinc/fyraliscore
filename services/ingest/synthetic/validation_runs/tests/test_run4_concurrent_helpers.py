from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

from services.ingest.synthetic.validation_runs.run4_concurrent import (
    _LIVE_EVENTS_PER_TENANT,
    _expected_combined_observation_totals,
    _live_targets_from_outcomes,
    _run4_report,
)


_TENANT = UUID("aaaaaaaa-1111-7777-8888-bbbbbbbbbbbb")


def _outcome(
    *,
    tenant_id: UUID = _TENANT,
    source: str = "gmail",
    slug: str = "r4-gmail-0",
    expected: int = 5,
    fixture_params: dict | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        tenant_id=tenant_id,
        scenario=SimpleNamespace(
            source=source,
            tenant_slug=slug,
            expected_observation_count=expected,
            fixture_params=fixture_params or {"email": "r4-gmail-0@val.example"},
        ),
    )


def test_run4_report_uses_real_client_identity_for_run5() -> None:
    report = _run4_report(scenarios=[object(), object()], real_clients=True)

    assert report.run_number == 5
    assert report.tenant_count == 2
    assert "REAL clients" in report.run_name


def test_expected_combined_observation_totals_adds_live_events() -> None:
    totals = _expected_combined_observation_totals([
        _outcome(expected=7),
    ])

    assert totals == {_TENANT: 7 + _LIVE_EVENTS_PER_TENANT}


def test_live_targets_from_outcomes_preserves_scenario_addressing() -> None:
    target = _live_targets_from_outcomes([
        _outcome(
            source="github",
            slug="r4-github-0",
            expected=6,
            fixture_params={"org_or_user": "r4gh0"},
        ),
    ])[0]

    assert target.tenant_id == _TENANT
    assert target.source == "github"
    assert target.slug == "r4-github-0"
    assert target.installation_id == "x3-r4-github-0-github"
    assert target.repo_full_name == "r4gh0/live-r4-github-0"
