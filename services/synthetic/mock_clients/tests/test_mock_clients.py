"""X2 mock client tests.

Per A21: each mock client must
  (a) serve fixture data correctly on the happy path,
  (b) advance cursor state across pages,
  (c) raise the source's real error types on configured faults,
  (d) reflect stateful reconciler probes (history_id, etag, after-ts,
      after-snowflake).
"""
from __future__ import annotations

import asyncio

import pytest

from lib.shared.errors import DiscordApiError, GithubApiError
from services.integrations.gmail.client import (
    GoogleApiError, GoogleRateLimited,
)
from services.integrations.slack.client import SlackApiError
from services.synthetic.fault_profiles import (
    AUTH_EXPIRED,
    FLAKY,
    HAPPY_PATH,
    RATE_LIMITED,
    FaultProfile,
)
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


# =====================================================================
# Gmail.
# =====================================================================
def test_mock_gmail_serves_fixture_data_correctly() -> None:
    fixture = make_gmail_mailbox(email="alice@x.com", messages=3)
    client = MockGmailClient(fixture=fixture)

    async def _run() -> None:
        resp = await client.messages_list(
            user_email="alice@x.com", scope="gmail.metadata",
        )
        assert len(resp["messages"]) == 3
        # Hydrate each message.
        msg = await client.get_message(
            user_email="alice@x.com", scope="gmail.metadata",
            message_id=resp["messages"][0]["id"],
        )
        assert msg["id"] == resp["messages"][0]["id"]

    asyncio.run(_run())


def test_mock_gmail_advances_cursor_state_across_pages() -> None:
    fixture = make_gmail_mailbox(
        email="alice@x.com", messages=12, page_size=5,
    )
    client = MockGmailClient(fixture=fixture)

    async def _run() -> None:
        page1 = await client.messages_list(
            user_email="alice@x.com", scope="gmail.metadata",
        )
        assert len(page1["messages"]) == 5
        assert "nextPageToken" in page1

        page2 = await client.messages_list(
            user_email="alice@x.com", scope="gmail.metadata",
            page_token=page1["nextPageToken"],
        )
        assert len(page2["messages"]) == 5
        assert "nextPageToken" in page2

        page3 = await client.messages_list(
            user_email="alice@x.com", scope="gmail.metadata",
            page_token=page2["nextPageToken"],
        )
        assert len(page3["messages"]) == 2
        assert "nextPageToken" not in page3

    asyncio.run(_run())


def test_mock_gmail_rate_limits_after_threshold() -> None:
    fixture = make_gmail_mailbox(email="alice@x.com", messages=100)
    client = MockGmailClient(
        fixture=fixture,
        profile=FaultProfile(rate_limit_after_n_requests=2),
    )

    async def _run() -> None:
        await client.get_profile(user_email="x", scope="s")  # 1
        await client.get_profile(user_email="x", scope="s")  # 2
        with pytest.raises(GoogleRateLimited):
            await client.get_profile(user_email="x", scope="s")  # 3 → fault

    asyncio.run(_run())


def test_mock_gmail_returns_correct_error_types_on_fault() -> None:
    fixture = make_gmail_mailbox(email="alice@x.com", messages=1)

    async def _run() -> None:
        c1 = MockGmailClient(
            fixture=fixture,
            profile=FaultProfile(random_5xx_probability=1.0),
        )
        with pytest.raises(GoogleApiError):
            await c1.get_profile(user_email="x", scope="s")

        # auth-expires triggers strictly AFTER the threshold elapses,
        # so the first call (which seeds _first_call_at) must succeed
        # and a subsequent call must fail.
        c2 = MockGmailClient(
            fixture=fixture,
            profile=FaultProfile(auth_expires_after_n_seconds=0.001),
        )
        await c2.get_profile(user_email="x", scope="s")
        await asyncio.sleep(0.01)
        with pytest.raises(GoogleApiError):
            await c2.get_profile(user_email="x", scope="s")

    asyncio.run(_run())


def test_mock_gmail_stateful_history_id_progression() -> None:
    """Reconciler scenario: fixture has events past initial fetch; the
    mock's `get_profile` reports a higher `historyId` than the cursor's
    `final_history_id`. Verifies the gap-detection probe surface."""
    fixture = make_gmail_mailbox(
        email="alice@x.com", messages=5, history_events=3,
        starting_history_id=1000,
    )
    client = MockGmailClient(fixture=fixture)

    async def _run() -> None:
        prof = await client.get_profile(
            user_email="alice@x.com", scope="gmail.metadata",
        )
        # 1000 + 3 history events = 1003.
        assert prof["historyId"] == "1003"

    asyncio.run(_run())


# =====================================================================
# GitHub.
# =====================================================================
def test_mock_github_serves_fixture_data_correctly() -> None:
    fixture = make_github_repos(org_or_user="octo", repos=2)
    client = MockGithubClient(fixture=fixture)

    async def _run() -> None:
        repos = await client.list_installation_repositories("99")
        assert len(repos) == 2
        owner, name = repos[0].split("/")
        events, etag, next_page = await client.list_repo_events(
            owner=owner, repo=name, event_type="issues",
        )
        assert len(events) > 0
        assert etag.startswith('W/"')

    asyncio.run(_run())


def test_mock_github_advances_cursor_state_across_pages() -> None:
    fixture = make_github_repos(
        org_or_user="octo", repos=1, events_per_repo=70, per_page=30,
    )
    client = MockGithubClient(fixture=fixture)

    async def _run() -> None:
        owner, name = fixture["repos"][0]["full_name"].split("/")
        p1, _, np1 = await client.list_repo_events(
            owner=owner, repo=name, event_type="issues", page=1,
        )
        p2, _, np2 = await client.list_repo_events(
            owner=owner, repo=name, event_type="issues", page=np1 or 2,
        )
        p3, _, np3 = await client.list_repo_events(
            owner=owner, repo=name, event_type="issues", page=np2 or 3,
        )
        assert len(p1) == 30
        assert len(p2) == 30
        assert len(p3) == 10
        assert np3 is None

    asyncio.run(_run())


def test_mock_github_rate_limits_after_threshold() -> None:
    fixture = make_github_repos(org_or_user="o", repos=1)
    client = MockGithubClient(
        fixture=fixture,
        profile=FaultProfile(rate_limit_after_n_requests=1),
    )

    async def _run() -> None:
        await client.list_installation_repositories("99")  # 1
        with pytest.raises(GithubApiError):
            await client.list_installation_repositories("99")  # 2 → fault

    asyncio.run(_run())


def test_mock_github_returns_correct_error_types_on_fault() -> None:
    fixture = make_github_repos(org_or_user="o", repos=1)
    client = MockGithubClient(
        fixture=fixture,
        profile=FaultProfile(random_5xx_probability=1.0),
    )

    async def _run() -> None:
        with pytest.raises(GithubApiError):
            await client.list_installation_repositories("99")

    asyncio.run(_run())


def test_mock_github_stateful_etag_progression() -> None:
    """Reconciler fast-path: head_repo_events returns has_changes=False
    when the etag matches the current state."""
    fixture = make_github_repos(org_or_user="o", repos=1, events_per_repo=5)
    client = MockGithubClient(fixture=fixture)

    async def _run() -> None:
        owner, name = fixture["repos"][0]["full_name"].split("/")
        has_changes, etag = await client.head_repo_events(
            owner=owner, repo=name, event_type="issues",
        )
        # First call: no etag → has_changes=True.
        assert has_changes is True
        # Same etag → no changes.
        has_changes_2, etag_2 = await client.head_repo_events(
            owner=owner, repo=name, event_type="issues", etag=etag,
        )
        assert has_changes_2 is False
        assert etag_2 == etag

    asyncio.run(_run())


# =====================================================================
# Slack.
# =====================================================================
def test_mock_slack_serves_fixture_data_correctly() -> None:
    fixture = make_slack_workspace(team_id="T_X", channels=2)
    client = MockSlackClient(fixture=fixture)

    async def _run() -> None:
        channels = await client.conversations_list()
        assert len(channels) == 2
        cid = channels[0]["id"]
        msgs, cursor = await client.conversations_history(channel=cid)
        assert len(msgs) > 0

    asyncio.run(_run())


def test_mock_slack_advances_cursor_state_across_pages() -> None:
    fixture = make_slack_workspace(
        team_id="T_X", channels=1, messages_per_channel=25, page_size=10,
    )
    client = MockSlackClient(fixture=fixture)

    async def _run() -> None:
        cid = fixture["channels"][0]["id"]
        p1, c1 = await client.conversations_history(channel=cid)
        p2, c2 = await client.conversations_history(channel=cid, cursor=c1)
        p3, c3 = await client.conversations_history(channel=cid, cursor=c2)
        assert len(p1) == 10
        assert len(p2) == 10
        assert len(p3) == 5
        assert c3 is None

    asyncio.run(_run())


def test_mock_slack_rate_limits_after_threshold() -> None:
    fixture = make_slack_workspace(team_id="T_X", channels=1)
    client = MockSlackClient(
        fixture=fixture,
        profile=FaultProfile(rate_limit_after_n_requests=0),
    )

    async def _run() -> None:
        with pytest.raises(SlackApiError):
            await client.conversations_list()

    asyncio.run(_run())


def test_mock_slack_returns_correct_error_types_on_fault() -> None:
    fixture = make_slack_workspace(team_id="T_X", channels=1)
    client = MockSlackClient(
        fixture=fixture,
        profile=FaultProfile(transient_network_error_probability=1.0),
    )

    async def _run() -> None:
        with pytest.raises(SlackApiError):
            await client.conversations_list()

    asyncio.run(_run())


def test_mock_slack_stateful_oldest_ts_filter() -> None:
    """Reconciler gap-detection probe: passing `oldest` filters
    messages to those strictly newer than the given ts."""
    fixture = make_slack_workspace(
        team_id="T_X", channels=1, messages_per_channel=10,
    )
    client = MockSlackClient(fixture=fixture)

    async def _run() -> None:
        cid = fixture["channels"][0]["id"]
        # Without `oldest`: all messages.
        all_msgs, _ = await client.conversations_history(
            channel=cid, limit=100,
        )
        assert len(all_msgs) == 10
        # With `oldest` set to a middle ts: only newer messages.
        mid_ts = all_msgs[5]["ts"]
        newer, _ = await client.conversations_history(
            channel=cid, oldest=mid_ts, limit=100,
        )
        assert len(newer) == 5

    asyncio.run(_run())


# =====================================================================
# Discord.
# =====================================================================
def test_mock_discord_serves_fixture_data_correctly() -> None:
    fixture = make_discord_guild(guild_id="G_X", channels=2)
    client = MockDiscordClient(fixture=fixture)

    async def _run() -> None:
        guilds = await client.list_guilds()
        assert guilds == [{"id": "G_X"}]
        channels = await client.list_guild_channels("G_X")
        assert len(channels) == 2
        msgs = await client.get_messages(channel_id=channels[0]["id"])
        assert len(msgs) > 0

    asyncio.run(_run())


def test_mock_discord_advances_cursor_state_across_pages() -> None:
    fixture = make_discord_guild(
        guild_id="G_X", channels=1, messages_per_channel=12,
    )
    client = MockDiscordClient(fixture=fixture)

    async def _run() -> None:
        cid = fixture["channels"][0]["id"]
        p1 = await client.get_messages(channel_id=cid, limit=5)
        assert len(p1) == 5
        oldest_id = p1[-1]["id"]
        p2 = await client.get_messages(
            channel_id=cid, before=oldest_id, limit=5,
        )
        assert len(p2) == 5
        # All ids strictly less than `oldest_id`.
        assert all(int(m["id"]) < int(oldest_id) for m in p2)

    asyncio.run(_run())


def test_mock_discord_rate_limits_after_threshold() -> None:
    fixture = make_discord_guild(guild_id="G_X", channels=1)
    client = MockDiscordClient(
        fixture=fixture,
        profile=FaultProfile(rate_limit_after_n_requests=0),
    )

    async def _run() -> None:
        with pytest.raises(DiscordApiError):
            await client.list_guilds()

    asyncio.run(_run())


def test_mock_discord_returns_correct_error_types_on_fault() -> None:
    fixture = make_discord_guild(guild_id="G_X", channels=1)
    client = MockDiscordClient(
        fixture=fixture,
        profile=FaultProfile(random_5xx_probability=1.0),
    )

    async def _run() -> None:
        with pytest.raises(DiscordApiError):
            await client.list_guilds()

    asyncio.run(_run())


def test_mock_discord_stateful_after_snowflake_filter() -> None:
    """Reconciler gap-detection probe: passing `after` returns only
    messages with snowflake id > after."""
    fixture = make_discord_guild(
        guild_id="G_X", channels=1, messages_per_channel=10,
    )
    client = MockDiscordClient(fixture=fixture)

    async def _run() -> None:
        cid = fixture["channels"][0]["id"]
        all_msgs = await client.get_messages(channel_id=cid, limit=100)
        assert len(all_msgs) == 10
        # `after` = a middle snowflake: only ids greater than it.
        mid_id = sorted([int(m["id"]) for m in all_msgs])[5]
        newer = await client.get_messages(
            channel_id=cid, after=str(mid_id), limit=100,
        )
        assert all(int(m["id"]) > mid_id for m in newer)

    asyncio.run(_run())


# =====================================================================
# Determinism.
# =====================================================================
def test_fixture_generators_are_deterministic() -> None:
    """Same params → identical fixtures across calls."""
    a = make_gmail_mailbox(email="a@x.com", messages=5)
    b = make_gmail_mailbox(email="a@x.com", messages=5)
    assert a == b

    c = make_github_repos(org_or_user="o", repos=3, events_per_repo=2)
    d = make_github_repos(org_or_user="o", repos=3, events_per_repo=2)
    assert c == d

    e = make_slack_workspace(team_id="T", channels=2)
    f = make_slack_workspace(team_id="T", channels=2)
    assert e == f

    g = make_discord_guild(guild_id="G", channels=2)
    h = make_discord_guild(guild_id="G", channels=2)
    assert g == h


def test_preset_profiles_have_distinct_shapes() -> None:
    assert HAPPY_PATH != RATE_LIMITED
    assert HAPPY_PATH.rate_limit_after_n_requests is None
    assert RATE_LIMITED.rate_limit_after_n_requests == 50
    assert FLAKY.random_5xx_probability == 0.10
    assert AUTH_EXPIRED.auth_expires_after_n_seconds == 30.0
