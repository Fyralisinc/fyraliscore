from __future__ import annotations

from collections.abc import Callable

import pytest

from scripts import sandbox_ingest


def test_provider_base_url_uses_one_source_scoped_lab_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787/")

    assert (
        sandbox_ingest.provider_base_url("slack")
        == "http://127.0.0.1:8787/slack"
    )
    assert (
        sandbox_ingest.provider_base_url("quickbooks_online")
        == "http://127.0.0.1:8787/quickbooks"
    )


@pytest.mark.parametrize(
    ("source_name", "expected_segment"),
    (
        ("google_calendar", "gcal"),
        ("calendar", "gcal"),
        ("gcal", "gcal"),
        ("google_drive", "gdrive"),
        ("drive", "gdrive"),
        ("gdrive", "gdrive"),
    ),
)
def test_provider_base_url_resolves_google_catalog_aliases(
    monkeypatch: pytest.MonkeyPatch,
    source_name: str,
    expected_segment: str,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://localhost:9191")

    assert sandbox_ingest.provider_base_url(source_name) == (
        f"http://localhost:9191/{expected_segment}"
    )


def test_rebased_provider_url_preserves_production_api_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:9191")

    assert sandbox_ingest.rebased_provider_url("slack", "slack_api") == (
        "http://127.0.0.1:9191/slack/api"
    )
    assert sandbox_ingest.rebased_provider_url(
        "google_calendar",
        "google_calendar_api",
    ) == "http://127.0.0.1:9191/gcal/calendar/v3"
    assert sandbox_ingest.rebased_provider_url(
        "google_drive",
        "google_drive_api",
    ) == "http://127.0.0.1:9191/gdrive/drive/v3"


def test_ingester_discovery_uses_canonical_contract_ids() -> None:
    assert "google_calendar" in sandbox_ingest.INGESTERS
    assert "google_drive" in sandbox_ingest.INGESTERS
    assert "calendar" not in sandbox_ingest.INGESTERS
    assert "drive" not in sandbox_ingest.INGESTERS
    assert all(
        isinstance(ingester, Callable)
        for ingester in sandbox_ingest.INGESTERS.values()
    )


def test_legacy_per_provider_port_registry_is_gone() -> None:
    assert not hasattr(sandbox_ingest, "PORTS")
    assert "SANDBOX_MOCK_HOST" not in sandbox_ingest.__dict__
