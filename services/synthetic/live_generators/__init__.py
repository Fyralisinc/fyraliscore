"""Y1+Y2 live-ingestion synthetic generators.

Per A23: in-process generators that drive the live-ingestion code
paths (Gmail Pub/Sub via FastAPI ASGI; Discord Gateway via direct
event-handler invocation) with synthetic traffic. Coordinate X2 mock
clients' state with notification / event dispatch as one logical
operation.

Composition contract: these generators are usable side-by-side with
the X3 backfill harness. Common pattern:

    1. Install a tenant via X3 (writes install + onboarding_triggers).
    2. Run the M6 backfill via X3's run().
    3. Drive ongoing live notifications / events via Y1/Y2.
    4. Assert that backfill observations + live observations coexist
       coherently (no duplicates across paths; observation count
       matches backfill+live total).
"""
from services.synthetic.live_generators.discord_gateway import (
    DiscordGatewayGenerator,
    GuildBinding,
    SimulatedEventResult,
)
from services.synthetic.live_generators.gmail_pubsub import (
    GmailPubSubGenerator,
    SimulatedPushResult,
)


__all__ = [
    "DiscordGatewayGenerator",
    "GmailPubSubGenerator",
    "GuildBinding",
    "SimulatedEventResult",
    "SimulatedPushResult",
]
