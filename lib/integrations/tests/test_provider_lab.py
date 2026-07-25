from __future__ import annotations

import pytest

from lib.integrations.provider_lab import (
    provider_lab_enabled,
    provider_lab_endpoint_overrides,
    provider_lab_endpoint_url,
)


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


def test_endpoint_bases_share_one_loopback_origin(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787/")

    assert provider_lab_enabled() is True
    assert provider_lab_endpoint_url("google_calendar_api") == (
        "http://127.0.0.1:8787/gcal/calendar/v3"
    )
    assert provider_lab_endpoint_url("discord_gateway_bot") == (
        "http://127.0.0.1:8787/discord/api/v10/gateway/bot"
    )


def test_subprocess_overrides_are_explicit_per_endpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://localhost:9191")

    overrides = provider_lab_endpoint_overrides()

    assert overrides["GITHUB_API_BASE_URL"] == "http://localhost:9191/github"
    assert overrides["GMAIL_API_BASE_URL"] == (
        "http://localhost:9191/gmail/gmail/v1"
    )
    assert overrides["FACEBOOK_GRAPH_API_BASE_URL"] == (
        "http://localhost:9191/facebook"
    )


@pytest.mark.parametrize(
    "value",
    (
        "https://provider.example",
        "http://127.0.0.1:8787/prefix",
        "http://user:secret@127.0.0.1:8787",
    ),
)
def test_provider_lab_rejects_non_loopback_or_non_origin_urls(
    monkeypatch: pytest.MonkeyPatch,
    value: str,
) -> None:
    monkeypatch.setenv("PROVIDER_LAB_URL", value)

    with pytest.raises(RuntimeError, match="PROVIDER_LAB_URL"):
        provider_lab_enabled()


@pytest.mark.parametrize("env_var", ("FYRALIS_ENV", "APP_ENV", "ENVIRONMENT"))
def test_provider_lab_is_forbidden_in_production(
    monkeypatch: pytest.MonkeyPatch,
    env_var: str,
) -> None:
    monkeypatch.setenv(env_var, "production")
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://127.0.0.1:8787")

    with pytest.raises(RuntimeError, match="must be unset in production"):
        provider_lab_endpoint_url("github_api")
