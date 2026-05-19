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

    from services.synthetic.mock_clients.gmail import MockGmailClient
    from services.synthetic.fixtures.gmail_generator import (
        make_gmail_mailbox,
    )
    from services.synthetic.fault_profiles import HAPPY_PATH

    fixture = make_gmail_mailbox(email="alice@x.com", messages=10)
    client = MockGmailClient(fixture=fixture, profile=HAPPY_PATH)

    async def _open(install):
        async def close(): return None
        return client, close
    monkeypatch.setattr(gmail_fetcher_mod, "_open_gmail_client", _open)
"""
from services.synthetic.mock_clients.discord import MockDiscordClient
from services.synthetic.mock_clients.gmail import MockGmailClient
from services.synthetic.mock_clients.github import MockGithubClient
from services.synthetic.mock_clients.slack import MockSlackClient


__all__ = [
    "MockDiscordClient",
    "MockGithubClient",
    "MockGmailClient",
    "MockSlackClient",
]
