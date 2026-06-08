"""Programmatically-generated fixtures for X2 mock clients.

Each generator produces a deterministic fixture for the same input
parameters (seeded RNG for any randomness). Tests pass these into the
mock client constructors.
"""
from services.ingest.synthetic.fixtures.ashby_generator import make_ashby
from services.ingest.synthetic.fixtures.aws_generator import make_aws
from services.ingest.synthetic.fixtures.brex_generator import make_brex
from services.ingest.synthetic.fixtures.carta_generator import make_carta
from services.ingest.synthetic.fixtures.deel_generator import make_deel
from services.ingest.synthetic.fixtures.discord_generator import make_discord_guild
from services.ingest.synthetic.fixtures.figma_generator import make_figma
from services.ingest.synthetic.fixtures.fireflies_generator import make_fireflies
from services.ingest.synthetic.fixtures.gmail_generator import make_gmail_mailbox
from services.ingest.synthetic.fixtures.github_generator import make_github_repos
from services.ingest.synthetic.fixtures.gusto_generator import make_gusto
from services.ingest.synthetic.fixtures.google_calendar_generator import (
    make_google_calendar,
)
from services.ingest.synthetic.fixtures.google_drive_generator import (
    make_google_drive,
)
from services.ingest.synthetic.fixtures.grafana_generator import make_grafana
from services.ingest.synthetic.fixtures.hibob_generator import make_hibob
from services.ingest.synthetic.fixtures.jira_generator import make_jira
from services.ingest.synthetic.fixtures.linkedin_generator import make_linkedin
from services.ingest.synthetic.fixtures.mercury_generator import make_mercury
from services.ingest.synthetic.fixtures.miro_generator import make_miro
from services.ingest.synthetic.fixtures.notion_generator import make_notion
from services.ingest.synthetic.fixtures.quickbooks_generator import make_quickbooks
from services.ingest.synthetic.fixtures.ramp_generator import make_ramp
from services.ingest.synthetic.fixtures.signal_generator import make_signal
from services.ingest.synthetic.fixtures.slack_dm_generator import (
    make_slack_dm_workspace,
)
from services.ingest.synthetic.fixtures.slack_generator import make_slack_workspace
from services.ingest.synthetic.fixtures.telegram_generator import make_telegram


__all__ = [
    "make_ashby",
    "make_aws",
    "make_brex",
    "make_carta",
    "make_deel",
    "make_discord_guild",
    "make_figma",
    "make_fireflies",
    "make_github_repos",
    "make_gmail_mailbox",
    "make_google_calendar",
    "make_google_drive",
    "make_grafana",
    "make_gusto",
    "make_hibob",
    "make_jira",
    "make_linkedin",
    "make_mercury",
    "make_miro",
    "make_notion",
    "make_quickbooks",
    "make_ramp",
    "make_signal",
    "make_slack_dm_workspace",
    "make_slack_workspace",
    "make_telegram",
]
