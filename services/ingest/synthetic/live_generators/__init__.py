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
from services.ingest.synthetic.live_generators.discord_gateway import (
    DiscordGatewayGenerator,
    GuildBinding,
    SimulatedEventResult,
)
from services.ingest.synthetic.live_generators.gmail_pubsub import (
    GmailPubSubGenerator,
    SimulatedPushResult,
)
from services.ingest.synthetic.live_generators.github_webhook import (
    GithubScenarioResult,
    GithubWebhookGenerator,
    GithubWebhookResult,
)
from services.ingest.synthetic.live_generators.slack_webhook import (
    SimulatedWebhookResult,
    SlackWebhookGenerator,
)
from services.ingest.synthetic.live_generators.hmac_webhook import (
    HMAC_PROVIDERS,
    HmacWebhookGenerator,
    HmacWebhookResult,
)
from services.ingest.synthetic.live_generators.google_push import (
    GooglePushGenerator,
    GooglePushResult,
)
from services.ingest.synthetic.live_generators.notion_webhook import (
    NotionWebhookGenerator,
    NotionWebhookResult,
)
from services.ingest.synthetic.live_generators.telegram_gateway import (
    TelegramGatewayGenerator,
    TelegramGatewayResult,
)
from services.ingest.synthetic.live_generators.signal_gateway import (
    SignalGatewayGenerator,
    SignalGatewayResult,
)
from services.ingest.synthetic.live_generators.aws_poll import (
    AwsPollGenerator,
    AwsPollResult,
)
from services.ingest.synthetic.live_generators.carta_poll import (
    CartaPollGenerator,
    CartaPollResult,
)


__all__ = [
    "AwsPollGenerator",
    "AwsPollResult",
    "CartaPollGenerator",
    "CartaPollResult",
    "DiscordGatewayGenerator",
    "GithubScenarioResult",
    "GithubWebhookGenerator",
    "GithubWebhookResult",
    "GmailPubSubGenerator",
    "GooglePushGenerator",
    "GooglePushResult",
    "GuildBinding",
    "HMAC_PROVIDERS",
    "HmacWebhookGenerator",
    "HmacWebhookResult",
    "NotionWebhookGenerator",
    "NotionWebhookResult",
    "SignalGatewayGenerator",
    "SignalGatewayResult",
    "SimulatedEventResult",
    "SimulatedPushResult",
    "SimulatedWebhookResult",
    "SlackWebhookGenerator",
    "TelegramGatewayGenerator",
    "TelegramGatewayResult",
]
