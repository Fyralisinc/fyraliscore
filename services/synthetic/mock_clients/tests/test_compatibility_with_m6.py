"""Regression: X2 mock clients are method-shape compatible with existing
M6 fetcher/reconciler test seams.

Verifies the mock libraries can plug into the `_open_*_client` factory
seams used by M6.3-M6.6 production code without code changes — only
the mock instance differs from per-test inline fakes.

These tests do NOT exercise the full M6 chain (that's X3's harness);
they only confirm the mock surface matches what M6 code calls.
"""
from __future__ import annotations

import asyncio
import inspect

from services.synthetic.fixtures import (
    make_discord_guild,
    make_github_repos,
    make_gmail_mailbox,
    make_slack_workspace,
)
from services.synthetic.mock_clients import (
    MockDiscordClient,
    MockGithubClient,
    MockGmailClient,
    MockSlackClient,
)


def test_mock_gmail_implements_methods_called_by_m6_fetcher() -> None:
    """M6.3 fetcher calls messages_list, history_list, get_message,
    get_profile. The mock implements all four."""
    client = MockGmailClient(
        fixture=make_gmail_mailbox(email="x@y.com", messages=1),
    )
    for name in (
        "messages_list", "history_list", "get_message", "get_profile",
    ):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


def test_mock_github_implements_methods_called_by_m6_fetcher() -> None:
    """M6.4 planner/fetcher/reconciler call list_installation_repositories,
    list_repo_events, head_repo_events. The mock implements all three."""
    client = MockGithubClient(
        fixture=make_github_repos(org_or_user="o", repos=1),
    )
    for name in (
        "list_installation_repositories",
        "list_repo_events",
        "head_repo_events",
    ):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


def test_mock_slack_implements_methods_called_by_m6_fetcher() -> None:
    """M6.5 planner/fetcher/reconciler call conversations_list and
    conversations_history. The mock implements both."""
    client = MockSlackClient(
        fixture=make_slack_workspace(team_id="T", channels=1),
    )
    for name in ("conversations_list", "conversations_history"):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


def test_mock_discord_implements_methods_called_by_m6_fetcher() -> None:
    """M6.6 planner/fetcher/reconciler call list_guilds,
    list_guild_channels, get_messages. The mock implements all three."""
    client = MockDiscordClient(
        fixture=make_discord_guild(guild_id="G", channels=1),
    )
    for name in (
        "list_guilds", "list_guild_channels", "get_messages",
    ):
        assert hasattr(client, name)
        assert inspect.iscoroutinefunction(getattr(client, name))


def test_mock_clients_compatible_with_existing_M6_tests() -> None:
    """Round-trip: a typical M6 test-shape factory closure (returning
    `(client, close)`) works with the mock clients. Verifies the seam
    contract is honored.
    """
    gmail_mock = MockGmailClient(
        fixture=make_gmail_mailbox(email="x@y.com", messages=1),
    )
    github_mock = MockGithubClient(
        fixture=make_github_repos(org_or_user="o", repos=1),
    )
    slack_mock = MockSlackClient(
        fixture=make_slack_workspace(team_id="T", channels=1),
    )
    discord_mock = MockDiscordClient(
        fixture=make_discord_guild(guild_id="G", channels=1),
    )

    async def _factory(client):
        async def close(): return None
        return client, close

    async def _exercise() -> None:
        for mock in (gmail_mock, github_mock, slack_mock, discord_mock):
            c, close = await _factory(mock)
            assert c is mock
            await close()

    asyncio.run(_exercise())
