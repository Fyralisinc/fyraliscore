"""Outbound endpoint-resolver + per-client base-URL wiring tests.

Proves the codebase shift to configurable outbound APIs: every source
client resolves its base URL through lib.integrations.endpoints, so a
local spammer can be plugged in via env alone (no code change).
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from lib.integrations.endpoints import all_endpoints, endpoint


# ---------------------------------------------------------------------
# Resolver precedence.
# ---------------------------------------------------------------------
def test_prod_defaults(monkeypatch):
    for k in ("GMAIL_API_BASE_URL", "GITHUB_API_BASE_URL", "SLACK_API_BASE_URL",
              "DISCORD_API_BASE_URL", "SYNTHETIC_SOURCE_API_BASE"):
        monkeypatch.delenv(k, raising=False)
    assert endpoint("gmail_api") == "https://gmail.googleapis.com/gmail/v1"
    assert endpoint("github_api") == "https://api.github.com"
    assert endpoint("slack_api") == "https://slack.com/api"
    assert endpoint("discord_api") == "https://discord.com/api/v10"


def test_per_source_env_override(monkeypatch):
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://localhost:9100/github/")
    assert endpoint("github_api") == "http://localhost:9100/github"  # trailing / trimmed


def test_single_host_spammer_base(monkeypatch):
    monkeypatch.delenv("GMAIL_API_BASE_URL", raising=False)
    monkeypatch.delenv("GITHUB_API_BASE_URL", raising=False)
    monkeypatch.setenv("SYNTHETIC_SOURCE_API_BASE", "http://localhost:9100")
    assert endpoint("github_api") == "http://localhost:9100/github"
    assert endpoint("gmail_api") == "http://localhost:9100/gmail/gmail/v1"
    assert endpoint("slack_api") == "http://localhost:9100/slack/api"
    assert endpoint("discord_gateway_bot") == (
        "http://localhost:9100/discord/api/v10/gateway/bot")


def test_per_source_wins_over_spammer_base(monkeypatch):
    monkeypatch.setenv("SYNTHETIC_SOURCE_API_BASE", "http://localhost:9100")
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://other:1/gh")
    assert endpoint("github_api") == "http://other:1/gh"


def test_unknown_endpoint_raises():
    with pytest.raises(KeyError):
        endpoint("nope")


def test_all_endpoints_snapshot():
    snap = all_endpoints()
    assert set(snap) >= {"gmail_api", "github_api", "slack_api", "discord_api",
                         "discord_gateway_bot", "google_token", "google_directory"}


# ---------------------------------------------------------------------
# Each client picks up the override in its stored base.
# ---------------------------------------------------------------------
def test_gmail_client_uses_resolver(monkeypatch):
    monkeypatch.setenv("GMAIL_API_BASE_URL", "http://spammer/gmail/gmail/v1")
    from services.integrations.gmail.client import GmailClient
    c = GmailClient(http=None)  # init stores base only; no network
    assert c._base == "http://spammer/gmail/gmail/v1"


def test_github_client_uses_resolver(monkeypatch):
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://spammer/github")
    from services.integrations.github.client import GithubClient
    c = GithubClient(pool=None)
    assert c._api_base_url == "http://spammer/github"


def test_github_client_explicit_param_wins(monkeypatch):
    monkeypatch.setenv("GITHUB_API_BASE_URL", "http://env/github")
    from services.integrations.github.client import GithubClient
    c = GithubClient(pool=None, api_base_url="http://explicit/gh")
    assert c._api_base_url == "http://explicit/gh"


def test_slack_client_uses_resolver(monkeypatch):
    monkeypatch.setenv("SLACK_API_BASE_URL", "http://spammer/slack/api")
    from services.integrations.slack.client import SlackClient
    c = SlackClient(pool=None, secret_store=None, tenant_id=uuid4(),
                    installation_row_id=uuid4(), team_id="T1")
    assert c._api_base == "http://spammer/slack/api"


def test_discord_client_uses_resolver(monkeypatch):
    monkeypatch.setenv("DISCORD_API_BASE_URL", "http://spammer/discord/api/v10")
    from services.integrations.discord.client import DiscordClient
    c = DiscordClient(pool=None, secret_store=None, tenant_id=uuid4(),
                      installation_row_id=uuid4(), guild_id="G1")
    assert c._api_base == "http://spammer/discord/api/v10"
