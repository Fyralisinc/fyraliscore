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
