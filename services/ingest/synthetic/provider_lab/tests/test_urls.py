from __future__ import annotations

import pytest

from services.ingest.synthetic.provider_lab.urls import provider_lab_base_url


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in (
        "FYRALIS_ENV",
        "COMPANY_OS_ENV",
        "APP_ENV",
        "ENVIRONMENT",
        "PROVIDER_LAB_URL",
    ):
        monkeypatch.delenv(name, raising=False)


def test_catalog_sources_and_aliases_map_to_provider_lab_paths(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787/")

    assert provider_lab_base_url("quickbooks_online") == (
        "http://127.0.0.1:8787/quickbooks"
    )
    assert provider_lab_base_url("calendar") == "http://127.0.0.1:8787/gcal"
    assert provider_lab_base_url("facebook") == (
        "http://127.0.0.1:8787/facebook"
    )


def test_catalog_source_url_requires_provider_lab_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("PROVIDER_LAB_URL", raising=False)

    with pytest.raises(RuntimeError, match="PROVIDER_LAB_URL is unset"):
        provider_lab_base_url("slack")
