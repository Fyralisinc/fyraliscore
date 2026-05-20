"""Y1+Y2 live-ingestion scenarios.

`LivePubSubScenario` describes a multi-tenant Gmail Pub/Sub run; Y2
adds `LiveGatewayScenario` for Discord. Same shape as X3's
`BackfillScenario` for API symmetry across the synthetic suite.
"""
from services.synthetic.scenarios.live_scenarios import (
    BURSTY_PUBSUB,
    BURSTY_SLACK,
    GatewayChannelEntry,
    HIGH_VOLUME_BURST,
    LiveGatewayScenario,
    LivePubSubScenario,
    LiveSlackScenario,
    MIXED_PUBSUB,
    MIXED_SLACK,
    MULTI_CHANNEL_PER_GUILD,
    PerTenantBurst,
    SINGLE_ACTIVE_CHANNEL,
    SlackTenantTraffic,
    STEADY_STATE_PUBSUB,
    STEADY_STATE_SLACK,
)


__all__ = [
    "BURSTY_PUBSUB",
    "BURSTY_SLACK",
    "GatewayChannelEntry",
    "HIGH_VOLUME_BURST",
    "LiveGatewayScenario",
    "LivePubSubScenario",
    "LiveSlackScenario",
    "MIXED_PUBSUB",
    "MIXED_SLACK",
    "MULTI_CHANNEL_PER_GUILD",
    "PerTenantBurst",
    "SINGLE_ACTIVE_CHANNEL",
    "SlackTenantTraffic",
    "STEADY_STATE_PUBSUB",
    "STEADY_STATE_SLACK",
]
