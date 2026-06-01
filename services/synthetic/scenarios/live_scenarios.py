"""Live-ingestion scenarios (Y1 Gmail Pub/Sub; Y2 adds Discord Gateway).

Each scenario is a frozen dataclass; per-tenant burst patterns are
expressed as `[(delay_ms, message_count), ...]` lists so a scenario
can encode "1 message every 1s for 10s" (steady-state) or "50
messages in 5s, then quiet for 30s" (bursty).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from services.synthetic.fault_profiles import HAPPY_PATH, FaultProfile


# Per-tenant burst pattern: each tuple is (delay_ms, message_count).
# The generator sleeps `delay_ms`, then dispatches `message_count`
# new messages.
BurstPattern = list[tuple[int, int]]


@dataclass(frozen=True)
class PerTenantBurst:
    """One tenant's mailbox + burst configuration."""

    tenant_slug: str
    mailbox_email: str
    burst_pattern: BurstPattern


@dataclass(frozen=True)
class LivePubSubScenario:
    """Multi-tenant Gmail Pub/Sub live-ingestion scenario.

    Attributes:
      tenants:
        List of PerTenantBurst. Each tenant's bursts run sequentially
        within the tenant (matches real-world per-mailbox ordering);
        across tenants, bursts run concurrently.
      replay_probability:
        [0.0, 1.0] — fraction of notifications duplicated. Tests
        verify push handler idempotency (no double-counted observations).
      fault_profile:
        FaultProfile applied to mock Gmail clients during the run.
    """

    tenants: list[PerTenantBurst]
    replay_probability: float = 0.0
    fault_profile: FaultProfile = HAPPY_PATH


# Presets (Y1.3).
STEADY_STATE_PUBSUB = LivePubSubScenario(
    tenants=[
        PerTenantBurst(
            tenant_slug="steady",
            mailbox_email="steady@x.com",
            burst_pattern=[(1000, 1)] * 10,  # 1 msg/s × 10
        ),
    ],
)

BURSTY_PUBSUB = LivePubSubScenario(
    tenants=[
        PerTenantBurst(
            tenant_slug="bursty",
            mailbox_email="bursty@x.com",
            burst_pattern=[(0, 50), (30000, 0)],  # 50 in burst, idle 30s
        ),
    ],
)

MIXED_PUBSUB = LivePubSubScenario(
    tenants=[
        PerTenantBurst(
            tenant_slug=f"mixed-{i}",
            mailbox_email=f"mixed-{i}@x.com",
            burst_pattern=(
                [(500, 2)] * 5 if i % 2 == 0
                else [(0, 10), (5000, 1)]
            ),
        )
        for i in range(5)
    ],
)


# =====================================================================
# Discord Gateway scenarios (Y2).
# =====================================================================
MessagePattern = list[tuple[int, int]]


@dataclass(frozen=True)
class GatewayChannelEntry:
    """One (guild, channel)'s message-injection configuration."""

    tenant_slug: str
    guild_id: str
    channel_id: str
    message_pattern: MessagePattern


@dataclass(frozen=True)
class LiveGatewayScenario:
    """Multi-channel Discord Gateway live-event scenario.

    Attributes:
      tenants:
        List of `GatewayChannelEntry`. Within a (guild, channel),
        events fire sequentially (matches per-channel ordering).
        Across (guild, channel) pairs, events run concurrently.
      fault_profile:
        FaultProfile applied to mock Discord clients during the run.
    """

    tenants: list[GatewayChannelEntry]
    fault_profile: FaultProfile = HAPPY_PATH


# Gateway scenario presets (Y2.4).
SINGLE_ACTIVE_CHANNEL = LiveGatewayScenario(
    tenants=[
        GatewayChannelEntry(
            tenant_slug="single",
            guild_id="1504477009927999569",
            channel_id="channel_test_001",
            message_pattern=[(1000, 1)] * 10,
        ),
    ],
)

MULTI_CHANNEL_PER_GUILD = LiveGatewayScenario(
    tenants=[
        GatewayChannelEntry(
            tenant_slug=f"multi-ch-{i}",
            guild_id="1504477009927999569",
            channel_id=f"channel_multi_{i}",
            message_pattern=(
                [(500, 1)] * 5 if i == 0
                else [(200, 2)] * 5 if i == 1
                else [(0, 3), (1000, 2)]
            ),
        )
        for i in range(3)
    ],
)

HIGH_VOLUME_BURST = LiveGatewayScenario(
    tenants=[
        GatewayChannelEntry(
            tenant_slug="burst",
            guild_id="1504477009927999569",
            channel_id="channel_burst",
            message_pattern=[(0, 100)],
        ),
    ],
)


# =====================================================================
# Slack webhook scenarios (Z1-slack).
# =====================================================================
@dataclass(frozen=True)
class SlackTenantTraffic:
    """One Slack tenant's webhook-traffic configuration.

    `team_id` is the Slack workspace identifier; it MUST match a seeded
    `provider_installations` row (`provider='slack'`,
    `installation_id=team_id`) so the webhook router resolves the
    tenant. `channel_id` is the Slack channel the synthetic messages
    target. The driver derives the resolved `tenant_id` from the DB at
    dispatch time — scenarios carry only the logical Slack identifiers
    (parallels `PerTenantBurst` / `GatewayChannelEntry`)."""

    tenant_slug: str
    team_id: str
    channel_id: str
    message_pattern: MessagePattern


@dataclass(frozen=True)
class LiveSlackScenario:
    """Multi-tenant Slack webhook live-ingestion scenario.

    Attributes:
      tenants:
        List of `SlackTenantTraffic`. Within a tenant, messages fire
        sequentially with the configured per-step delays (matches
        per-channel ordering); across tenants, dispatch concurrently.
      replay_probability:
        [0.0, 1.0] — fraction of messages re-delivered with the same
        Slack `ts` (at-least-once delivery). Tests verify the
        `(source_channel, external_id, occurred_at)` dedup holds so
        replays don't double-count observations.
      fault_profile:
        FaultProfile applied to the mock Slack client. NOTE: the Slack
        webhook ingest path does NOT query the Slack API, so the
        profile is inert for webhook dispatch — it's accepted for
        signature parity with the Pub/Sub / Gateway scenarios."""

    tenants: list[SlackTenantTraffic]
    replay_probability: float = 0.0
    fault_profile: FaultProfile = HAPPY_PATH


# Slack scenario presets (Z1.4). Suffix matches the file convention
# (`_PUBSUB` for Gmail) so the names don't collide with Discord's.
STEADY_STATE_SLACK = LiveSlackScenario(
    tenants=[
        SlackTenantTraffic(
            tenant_slug="steady",
            team_id="T_STEADY",
            channel_id="C_STEADY",
            message_pattern=[(2000, 1)] * 10,  # 1 msg / 2s × 10
        ),
    ],
)

BURSTY_SLACK = LiveSlackScenario(
    tenants=[
        SlackTenantTraffic(
            tenant_slug="bursty",
            team_id="T_BURSTY",
            channel_id="C_BURSTY",
            # 30 in a burst, idle 30s, then another burst.
            message_pattern=[(0, 30), (30000, 0), (0, 30)],
        ),
    ],
)

MIXED_SLACK = LiveSlackScenario(
    tenants=[
        SlackTenantTraffic(
            tenant_slug=f"mixed-slack-{i}",
            team_id=f"T_MIXED_{i}",
            channel_id=f"C_MIXED_{i}",
            message_pattern=(
                [(500, 2)] * 5 if i % 2 == 0
                else [(0, 10), (5000, 1)]
            ),
        )
        for i in range(5)
    ],
)


# =====================================================================
# GitHub webhook scenarios (Z1-github).
# =====================================================================
@dataclass(frozen=True)
class GithubTenantTraffic:
    """One GitHub tenant's webhook-traffic configuration.

    `installation_id` is the GitHub App installation id; it MUST match
    a seeded `provider_installations` row (`provider='github'`,
    `installation_id=<this>`) so the webhook router resolves the
    tenant. `repo_full_name` (`owner/repo`) is the repository the
    synthetic events target (must pass the installation's
    `selected_repositories` filter — NULL = all repos). `event_pattern`
    is `[(delay_ms, event_count), ...]` like the Slack / Gateway
    scenarios."""

    tenant_slug: str
    installation_id: str
    repo_full_name: str
    event_pattern: MessagePattern


@dataclass(frozen=True)
class LiveGithubScenario:
    """Multi-tenant GitHub webhook live-ingestion scenario.

    Attributes:
      tenants:
        List of `GithubTenantTraffic`. Within a tenant, events fire
        sequentially with configured delays; across tenants they run
        concurrently.
      event_type:
        Which event the scenario dispatches: ``"issues"`` or
        ``"pull_request"``.
      replay_probability:
        [0.0, 1.0] — fraction of events re-delivered with the same
        delivery id + node_id (at-least-once delivery). Verifies the
        router replay cache + observation-layer `external_id` dedup
        prevent double-counting.
      fault_profile:
        FaultProfile applied to the mock GitHub client. NOTE: the
        webhook ingest path does NOT query the GitHub API, so the
        profile is inert for webhook dispatch (accepted for parity)."""

    tenants: list[GithubTenantTraffic]
    event_type: str = "issues"
    replay_probability: float = 0.0
    fault_profile: FaultProfile = HAPPY_PATH


# GitHub scenario presets (Z1.4).
STEADY_STATE_GITHUB = LiveGithubScenario(
    tenants=[
        GithubTenantTraffic(
            tenant_slug="steady",
            installation_id="900001",
            repo_full_name="octo/steady",
            event_pattern=[(2000, 1)] * 10,
        ),
    ],
)

BURSTY_GITHUB = LiveGithubScenario(
    tenants=[
        GithubTenantTraffic(
            tenant_slug="bursty",
            installation_id="900002",
            repo_full_name="octo/bursty",
            event_pattern=[(0, 30), (30000, 0), (0, 30)],
        ),
    ],
)

MIXED_GITHUB = LiveGithubScenario(
    tenants=[
        GithubTenantTraffic(
            tenant_slug=f"mixed-gh-{i}",
            installation_id=f"90010{i}",
            repo_full_name=f"octo/mixed-{i}",
            event_pattern=(
                [(500, 2)] * 5 if i % 2 == 0
                else [(0, 10), (5000, 1)]
            ),
        )
        for i in range(5)
    ],
)
