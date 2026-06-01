#!/usr/bin/env python3
"""Bootstrap helper for testing IN-07 + IN-08 + IN-09 on the merged
`integration/ingestion-hardening` branch.

Mints a fresh tenant + actor + session bearer, then issues OAuth state
tokens for Slack and Discord and prints the OAuth authorize URLs.
Click the URLs in a browser, complete the respective consent screen,
and the callbacks will seed `provider_installations` +
`encrypted_secrets` automatically. After both installs land, send a
message in your Slack channel / invoke `/fyralis ask …` in Discord
and check `observations`.

The script intentionally does NOT call out to Slack/Discord APIs —
it only writes to the local DB.
"""
from __future__ import annotations

import asyncio
import os
import pathlib
import sys
from urllib.parse import urlencode
from uuid import UUID

_REPO_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

import asyncpg

from lib.shared.ids import uuid7
from services.gateway.auth import create_session
from services.integrations.discord.oauth import (
    issue_state_token as discord_issue_state_token,
)
from services.integrations.slack.oauth import (
    issue_state_token as slack_issue_state_token,
)


_TENANT_NAME = "integration-hardening-test"
_ACTOR_DISPLAY = "Prajwal (integration test)"
_ACTOR_EMAIL = "rachin.kalakheti@gmail.com"


async def _get_or_create_tenant(pool: asyncpg.Pool) -> UUID:
    row = await pool.fetchrow(
        "SELECT id FROM tenants WHERE name = $1", _TENANT_NAME,
    )
    if row is not None:
        return row["id"]
    tid = uuid7()
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)", tid, _TENANT_NAME,
    )
    return tid


async def _get_or_create_actor(pool: asyncpg.Pool, tenant_id: UUID) -> UUID:
    row = await pool.fetchrow(
        "SELECT id FROM actors WHERE tenant_id = $1 AND email = $2",
        tenant_id, _ACTOR_EMAIL,
    )
    if row is not None:
        return row["id"]
    aid = uuid7()
    await pool.execute(
        """
        INSERT INTO actors
            (id, tenant_id, type, display_name, email, status, metadata)
        VALUES ($1, $2, 'human_internal', $3, $4, 'active', '{}'::jsonb)
        """,
        aid, tenant_id, _ACTOR_DISPLAY, _ACTOR_EMAIL,
    )
    return aid


def _build_slack_url(state_token: str) -> str:
    qs = urlencode({
        "client_id": os.environ["SLACK_CLIENT_ID"],
        "scope": "channels:history,groups:history,im:history,mpim:history,users:read,team:read,chat:write,app_mentions:read",
        "redirect_uri": os.environ["SLACK_REDIRECT_URI"],
        "state": state_token,
    })
    return f"https://slack.com/oauth/v2/authorize?{qs}"


def _build_discord_url(state_token: str) -> str:
    qs = urlencode({
        "client_id": os.environ["DISCORD_CLIENT_ID"],
        "scope": "applications.commands bot",
        "permissions": "3072",
        "redirect_uri": os.environ["DISCORD_REDIRECT_URI"],
        "response_type": "code",
        "state": state_token,
    })
    return f"https://discord.com/api/oauth2/authorize?{qs}"


async def main() -> None:
    dsn = os.environ["DATABASE_URL"]
    pool = await asyncpg.create_pool(dsn, min_size=1, max_size=2)
    try:
        tenant_id = await _get_or_create_tenant(pool)
        actor_id = await _get_or_create_actor(pool, tenant_id)
        token_str, _ctx = await create_session(
            pool, actor_id=actor_id, tenant_id=tenant_id,
        )
        slack_state = await slack_issue_state_token(tenant_id, pool)
        discord_state = await discord_issue_state_token(tenant_id, pool)

        slack_url = _build_slack_url(slack_state)
        discord_url = _build_discord_url(discord_state)

        print("================================================================")
        print("integration/ingestion-hardening — manual test bootstrap")
        print("================================================================")
        print(f"Tenant ID : {tenant_id}")
        print(f"Actor ID  : {actor_id}")
        print()
        print(f"Bearer    : {token_str}")
        print("  (24h TTL; keep handy for /v1/* admin calls if needed.)")
        print()
        print("Step 1 — Install Slack: open this URL in a browser, click")
        print("         'Allow', wait for the success redirect.")
        print()
        print(slack_url)
        print()
        print("Step 2 — Install Discord: open this URL, pick the server,")
        print("         click 'Authorize'.")
        print()
        print(discord_url)
        print()
        print("Step 3 — Test ingestion:")
        print("   • Slack:   send a message in any channel the app is in.")
        print("   • Discord: invoke `/fyralis ask <question>` in the server.")
        print()
        print("Then check observations:")
        print(f"   psql 'postgresql://company_os:company_os@localhost:5433/company_os' \\")
        print(f"     -c \"SELECT source_channel, content_text, external_id \\")
        print(f"          FROM observations WHERE tenant_id='{tenant_id}' \\")
        print(f"          ORDER BY occurred_at DESC LIMIT 10;\"")
        print()
    finally:
        await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
