"""Y1+Y2 live-ingestion scenarios.

`LivePubSubScenario` describes a multi-tenant Gmail Pub/Sub run; Y2
adds `LiveGatewayScenario` for Discord. Same shape as X3's
`BackfillScenario` for API symmetry across the synthetic suite.
"""
from services.synthetic.scenarios.live_scenarios import (
    BURSTY_PUBSUB,
    MIXED_PUBSUB,
    STEADY_STATE_PUBSUB,
    LivePubSubScenario,
    PerTenantBurst,
)


__all__ = [
    "BURSTY_PUBSUB",
    "LivePubSubScenario",
    "MIXED_PUBSUB",
    "PerTenantBurst",
    "STEADY_STATE_PUBSUB",
]
