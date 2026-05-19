"""Programmatically-generated fixtures for X2 mock clients.

Each generator produces a deterministic fixture for the same input
parameters (seeded RNG for any randomness). Tests pass these into the
mock client constructors.
"""
from services.synthetic.fixtures.discord_generator import make_discord_guild
from services.synthetic.fixtures.gmail_generator import make_gmail_mailbox
from services.synthetic.fixtures.github_generator import make_github_repos
from services.synthetic.fixtures.slack_generator import make_slack_workspace


__all__ = [
    "make_discord_guild",
    "make_github_repos",
    "make_gmail_mailbox",
    "make_slack_workspace",
]
