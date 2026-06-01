"""Central resolver for outbound source-API base URLs.

Every outbound integration client (gmail, github, slack, discord REST +
gateway) resolves its base URL through `endpoint(name)` instead of a
hardcoded module constant. Production defaults are the real provider URLs;
each can be overridden via an env var so the whole pipeline can be pointed
at a **local source-mock server (the "spammer")** for load testing without
touching any client code — pure config.

Resolution is done at client-construction time (not import time) so a test
or a run can set the env var first.

Two override granularities:
  - per-source env var (e.g. `GMAIL_API_BASE_URL`) — explicit, wins.
  - `SYNTHETIC_SOURCE_API_BASE` — a single base that points ALL sources at
    one spammer host; each source is served under a conventional sub-path
    (`/gmail`, `/github`, `/slack`, `/discord`). Convenience for the local
    spammer; a per-source env var always takes precedence over it.

Auth/token endpoints that matter for outbound:
  - `google_token` — the DWD token-exchange URL. (Also data-driven via the
    service-account JSON's `token_uri`; this is the code-level default.)
  - github App-JWT / installation-token calls use `github_api` (same host).
"""
from __future__ import annotations

import os


# name -> production default URL.
_PROD: dict[str, str] = {
    "gmail_api": "https://gmail.googleapis.com/gmail/v1",
    "google_directory": "https://admin.googleapis.com/admin/directory/v1",
    "google_token": "https://oauth2.googleapis.com/token",
    "github_api": "https://api.github.com",
    "slack_api": "https://slack.com/api",
    "discord_api": "https://discord.com/api/v10",
    "discord_gateway_bot": "https://discord.com/api/v10/gateway/bot",
    # IN-14 Notion: single host; the /v1 prefix lives on each request path
    # in services/ingest/integrations/notion/client.py.
    "notion_api": "https://api.notion.com",
    # IN-15 Google Calendar: the v3 REST base. Uses the same DWD service
    # account as Gmail (auth lives in services/ingest/integrations/gmail/dwd.py).
    "google_calendar_api": "https://www.googleapis.com/calendar/v3",
    # IN-16 Google Drive: the v3 REST base. Uses the same DWD service account
    # as Gmail/Calendar (auth lives in services/ingest/integrations/gmail/dwd.py).
    "google_drive_api": "https://www.googleapis.com/drive/v3",
    # IN-17 Jira: NOTE — Jira Cloud has NO single global host; each tenant's
    # site is its own `https://<site>.atlassian.net`, carried per-install on
    # jira_installations.base_url and used directly in production. This entry
    # exists ONLY so the local spammer sub-path convention (`/jira`) resolves
    # uniformly; the prod default is intentionally empty (never used in
    # production — build_jira_client passes the per-install base_url instead).
    "jira_api": "",
    # Finance: Mercury banking API. Single global host.
    "mercury_api": "https://api.mercury.com/api/v1",
    # Finance: QuickBooks Online. NOTE — the realm-scoped path
    # (`/v3/company/{realmId}`) is per-install on quickbooks_installations.base_url
    # and used directly in production. This entry is the host prefix used ONLY so
    # the local spammer sub-path convention (`/quickbooks`) resolves uniformly;
    # build_quickbooks_client passes the per-install base_url in production.
    "quickbooks_api": "https://quickbooks.api.intuit.com",
}

# name -> explicit per-source env var (highest precedence).
_ENV: dict[str, str] = {
    "gmail_api": "GMAIL_API_BASE_URL",
    "google_directory": "GOOGLE_DIRECTORY_BASE_URL",
    "google_token": "GOOGLE_TOKEN_URI",
    "github_api": "GITHUB_API_BASE_URL",
    "slack_api": "SLACK_API_BASE_URL",
    "discord_api": "DISCORD_API_BASE_URL",
    "discord_gateway_bot": "DISCORD_GATEWAY_BOT_URL",
    "notion_api": "NOTION_API_BASE_URL",
    "google_calendar_api": "GOOGLE_CALENDAR_API_BASE_URL",
    "google_drive_api": "GOOGLE_DRIVE_API_BASE_URL",
    "jira_api": "JIRA_API_BASE_URL",
    "mercury_api": "MERCURY_API_BASE_URL",
    "quickbooks_api": "QUICKBOOKS_API_BASE_URL",
}

# name -> sub-path under SYNTHETIC_SOURCE_API_BASE when that single-host
# override is used.
_SPAMMER_SUBPATH: dict[str, str] = {
    "gmail_api": "/gmail/gmail/v1",
    "google_directory": "/gmail/admin/directory/v1",
    "google_token": "/gmail/token",
    "github_api": "/github",
    "slack_api": "/slack/api",
    "discord_api": "/discord/api/v10",
    "discord_gateway_bot": "/discord/api/v10/gateway/bot",
    "notion_api": "/notion",
    "google_calendar_api": "/gcal/calendar/v3",
    "google_drive_api": "/gdrive/drive/v3",
    "jira_api": "/jira",
    "mercury_api": "/mercury",
    "quickbooks_api": "/quickbooks",
}

_SPAMMER_BASE_ENV = "SYNTHETIC_SOURCE_API_BASE"


def endpoint(name: str) -> str:
    """Resolve the base URL for `name`. Precedence: per-source env var →
    single-host spammer base → production default. Trailing slash trimmed."""
    if name not in _PROD:
        raise KeyError(f"unknown endpoint name: {name!r}")
    explicit = os.environ.get(_ENV[name])
    if explicit:
        return explicit.rstrip("/")
    spammer_base = os.environ.get(_SPAMMER_BASE_ENV)
    if spammer_base:
        return (spammer_base.rstrip("/") + _SPAMMER_SUBPATH[name]).rstrip("/")
    return _PROD[name]


def all_endpoints() -> dict[str, str]:
    """Snapshot of all resolved endpoints — for startup logging / diagnostics."""
    return {name: endpoint(name) for name in _PROD}


__all__ = ["all_endpoints", "endpoint"]
