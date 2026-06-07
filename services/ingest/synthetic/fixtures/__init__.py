"""Programmatically-generated fixtures for X2 mock clients.

Each generator produces a deterministic fixture for the same input
parameters (seeded RNG for any randomness). Tests pass these into the
mock client constructors.
"""
from services.ingest.synthetic.fixtures.discord_generator import make_discord_guild
from services.ingest.synthetic.fixtures.gmail_generator import make_gmail_mailbox
from services.ingest.synthetic.fixtures.github_generator import make_github_repos
from services.ingest.synthetic.fixtures.google_calendar_generator import (
    make_google_calendar,
)
from services.ingest.synthetic.fixtures.google_drive_generator import (
    make_google_drive,
)
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.fixtures.jira_generator import make_jira
from services.ingest.synthetic.fixtures.mercury_generator import make_mercury
from services.ingest.synthetic.fixtures.notion_generator import make_notion
from services.ingest.synthetic.fixtures.quickbooks_generator import make_quickbooks
from services.ingest.synthetic.fixtures.slack_dm_generator import (
    make_slack_dm_workspace,
)
from services.ingest.synthetic.fixtures.slack_generator import make_slack_workspace


__all__ = [
    "make_discord_guild",
    "make_github_repos",
    "make_gmail_mailbox",
    "make_google_calendar",
    "make_google_drive",
    "make_grafana",
    "make_jira",
    "make_mercury",
    "make_notion",
    "make_quickbooks",
    "make_slack_dm_workspace",
    "make_slack_workspace",
]
