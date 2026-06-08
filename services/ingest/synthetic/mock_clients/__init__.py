"""X2 mock client libraries for synthetic backfill testing.

Per A21: in-process Python classes replacing production per-source
clients at the `_open_*_client` factory seams. Stateful per session
(cursor / history_id / etag tracking). Programmatically generated
fixtures (parameterizable). Fault injection via FaultProfile.

Each mock implements ONLY the methods M6 backfill code calls. Methods
that exist on the production client but aren't used by planners /
fetchers / reconcilers are not mirrored — the scope is "what M6
needs," not "the full provider SDK."

Wiring at test time:

    from services.ingest.synthetic.mock_clients.gmail import MockGmailClient
    from services.ingest.synthetic.fixtures.gmail_generator import (
        make_gmail_mailbox,
    )
    from services.ingest.synthetic.fault_profiles import HAPPY_PATH

    fixture = make_gmail_mailbox(email="alice@x.com", messages=10)
    client = MockGmailClient(fixture=fixture, profile=HAPPY_PATH)

    async def _open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(gmail_fetcher_mod, "_open_gmail_client", _open)
"""
from services.ingest.synthetic.mock_clients.aws import MockAwsClient
from services.ingest.synthetic.mock_clients.brex import MockBrexClient
from services.ingest.synthetic.mock_clients.carta import MockCartaClient
from services.ingest.synthetic.mock_clients.deel import MockDeelClient
from services.ingest.synthetic.mock_clients.discord import MockDiscordClient
from services.ingest.synthetic.mock_clients.figma import MockFigmaClient
from services.ingest.synthetic.mock_clients.fireflies import MockFirefliesClient
from services.ingest.synthetic.mock_clients.gmail import MockGmailClient
from services.ingest.synthetic.mock_clients.github import MockGithubClient
from services.ingest.synthetic.mock_clients.gusto import MockGustoClient
from services.ingest.synthetic.mock_clients.google_calendar import (
    MockGoogleCalendarClient,
)
from services.ingest.synthetic.mock_clients.google_drive import (
    MockGoogleDriveClient,
)
from services.ingest.synthetic.mock_clients.grafana import MockGrafanaClient
from services.ingest.synthetic.mock_clients.jira import MockJiraClient
from services.ingest.synthetic.mock_clients.mercury import MockMercuryClient
from services.ingest.synthetic.mock_clients.miro import MockMiroClient
from services.ingest.synthetic.mock_clients.notion import MockNotionClient
from services.ingest.synthetic.mock_clients.quickbooks import MockQuickBooksClient
from services.ingest.synthetic.mock_clients.ramp import MockRampClient
from services.ingest.synthetic.mock_clients.signal import MockSignalClient
from services.ingest.synthetic.mock_clients.slack import MockSlackClient
from services.ingest.synthetic.mock_clients.slack_user import MockSlackUserClient
from services.ingest.synthetic.mock_clients.telegram import MockTelegramClient


__all__ = [
    "MockAwsClient",
    "MockBrexClient",
    "MockCartaClient",
    "MockDeelClient",
    "MockDiscordClient",
    "MockFigmaClient",
    "MockFirefliesClient",
    "MockGithubClient",
    "MockGmailClient",
    "MockGoogleCalendarClient",
    "MockGoogleDriveClient",
    "MockGrafanaClient",
    "MockGustoClient",
    "MockJiraClient",
    "MockMercuryClient",
    "MockMiroClient",
    "MockNotionClient",
    "MockQuickBooksClient",
    "MockRampClient",
    "MockSignalClient",
    "MockSlackClient",
    "MockSlackUserClient",
    "MockTelegramClient",
]
