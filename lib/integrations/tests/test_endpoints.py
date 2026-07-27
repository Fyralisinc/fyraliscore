"""Outbound endpoint-resolver + per-client base-URL wiring tests."""

from __future__ import annotations

from uuid import uuid4

import pytest

from lib.integrations.endpoint_contract import PROVIDER_ENDPOINT_DEFINITIONS
from lib.integrations.endpoints import all_endpoints, endpoint


@pytest.fixture(autouse=True)
def _clear_runtime_env(monkeypatch):
    for key in ("FYRALIS_ENV", "COMPANY_OS_ENV", "APP_ENV", "ENVIRONMENT"):
        monkeypatch.delenv(key, raising=False)
    for key in [
        *(definition.override_env for definition in PROVIDER_ENDPOINT_DEFINITIONS),
        "PROVIDER_LAB_URL",
    ]:
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------
# Resolver precedence.
# ---------------------------------------------------------------------
def test_prod_defaults(monkeypatch):
    for k in (
        "GMAIL_API_BASE_URL",
        "GITHUB_API_BASE_URL",
        "SLACK_API_BASE_URL",
        "DISCORD_API_BASE_URL",
        "PROVIDER_LAB_URL",
    ):
        monkeypatch.delenv(k, raising=False)
    assert endpoint("gmail_api") == "https://gmail.googleapis.com/gmail/v1"
    assert endpoint("gmail_pubsub_api") == "https://pubsub.googleapis.com/v1"
    assert endpoint("github_api") == "https://api.github.com"
    assert endpoint("slack_api") == "https://slack.com/api"
    assert endpoint("discord_api") == "https://discord.com/api/v10"


def test_per_source_env_override(monkeypatch):
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://localhost:9100/github/")
    assert (
        endpoint("github_api") == "http://localhost:9100/github"
    )  # trailing / trimmed


def test_provider_lab_root_is_not_an_endpoint_override(monkeypatch):
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://localhost:9100")
    assert endpoint("github_api") == "https://api.github.com"
    assert endpoint("gmail_api") == "https://gmail.googleapis.com/gmail/v1"


@pytest.mark.parametrize("env_var", ["FYRALIS_ENV", "APP_ENV", "ENVIRONMENT"])
def test_provider_lab_is_forbidden_in_production(monkeypatch, env_var):
    monkeypatch.setenv(env_var, "production")
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://localhost:9100")

    with pytest.raises(RuntimeError, match="PROVIDER_LAB_URL"):
        endpoint("github_api")


def test_per_source_override_remains_explicit_with_provider_lab_set(monkeypatch):
    monkeypatch.setenv("PROVIDER_LAB_URL", "http://localhost:9100")
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://other:1/gh")
    assert endpoint("github_api") == "http://other:1/gh"


def test_unknown_endpoint_raises():
    with pytest.raises(KeyError):
        endpoint("nope")


def test_all_endpoints_snapshot():
    snap = all_endpoints()
    assert set(snap) >= {
        "gmail_api",
        "github_api",
        "slack_api",
        "discord_api",
        "discord_gateway_bot",
        "google_token",
        "google_directory",
    }


# ---------------------------------------------------------------------
# Each client picks up the override in its stored base.
# ---------------------------------------------------------------------
def test_gmail_client_uses_resolver(monkeypatch):
    monkeypatch.setenv(
        "GMAIL_API_BASE_URL",
        "http://provider-lab/gmail/gmail/v1",
    )
    from services.ingest.integrations.gmail.client import GmailClient

    c = GmailClient(http=None)  # init stores base only; no network
    assert c._base == "http://provider-lab/gmail/gmail/v1"


def test_github_client_uses_resolver(monkeypatch):
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://provider-lab/github")
    from services.ingest.integrations.github.client import GithubClient

    c = GithubClient(pool=None)
    assert c._api_base_url == "http://provider-lab/github"


def test_github_client_explicit_param_wins(monkeypatch):
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://env/github")
    from services.ingest.integrations.github.client import GithubClient

    c = GithubClient(pool=None, api_base_url="http://explicit/gh")
    assert c._api_base_url == "http://explicit/gh"


def test_slack_client_uses_resolver(monkeypatch):
    monkeypatch.setenv("SLACK_API_BASE_URL", "http://provider-lab/slack/api")
    from services.ingest.integrations.slack.client import SlackClient

    c = SlackClient(
        pool=None,
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        team_id="T1",
    )
    assert c._api_base == "http://provider-lab/slack/api"


def test_discord_client_uses_resolver(monkeypatch):
    monkeypatch.setenv(
        "DISCORD_API_BASE_URL",
        "http://provider-lab/discord/api/v10",
    )
    from services.ingest.integrations.discord.client import DiscordClient

    c = DiscordClient(
        pool=None,
        secret_store=None,
        tenant_id=uuid4(),
        installation_row_id=uuid4(),
        guild_id="G1",
    )
    assert c._api_base == "http://provider-lab/discord/api/v10"
