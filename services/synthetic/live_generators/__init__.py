"""Y1+Y2+Z1 live-ingestion synthetic generators.

Per A23 / A24 / A25: in-process generators that drive the
live-ingestion code paths with synthetic traffic, coordinating X2 mock
client state with notification / event / webhook dispatch as one
logical operation:

  - Gmail Pub/Sub via FastAPI ASGI (Y1, A23).
  - Discord Gateway via direct event-handler invocation (Y2, A24).
  - Slack webhooks via FastAPI ASGI (Z1-slack, A25).
  - GitHub webhooks via FastAPI ASGI (Z1-github, A25).

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
from services.synthetic.live_generators.github_webhook import (
    GithubScenarioResult,
    GithubWebhookGenerator,
    GithubWebhookResult,
)
from services.synthetic.live_generators.slack_webhook import (
    SimulatedWebhookResult,
    SlackWebhookGenerator,
)


__all__ = [
    "DiscordGatewayGenerator",
    "GithubScenarioResult",
    "GithubWebhookGenerator",
    "GithubWebhookResult",
    "GmailPubSubGenerator",
    "GuildBinding",
    "SimulatedEventResult",
    "SimulatedPushResult",
    "SimulatedWebhookResult",
    "SlackWebhookGenerator",
]
