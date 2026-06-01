"""IN-08 US6: outbound Slack client tests."""
from __future__ import annotations

from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import respx
from cryptography.fernet import Fernet

from lib.shared.ids import uuid7
from lib.shared.secrets import FernetSecretStore
from services.integrations.slack.client import SlackApiError, SlackClient


pytestmark = pytest.mark.integration


_TEAM_ID = "T_CLIENT_TEST"
_BOT_TOKEN = "xoxb-fake-bot-token"


async def _seed_install(fresh_db: asyncpg.Pool, secret_store) -> tuple[UUID, UUID]:
    tenant_id = uuid4()
    await fresh_db.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, $2)",
        tenant_id, f"client-{tenant_id.hex[:8]}",
    )
    install_row_id = uuid7()
    bot_ref = await secret_store.put(
        _BOT_TOKEN.encode("utf-8"),
        label=f"slack_bot_token:{_TEAM_ID}",
        tenant_id=tenant_id,
    )
    await fresh_db.execute(
        "INSERT INTO provider_installations "
        "(id, tenant_id, provider, installation_id, secret_ref, enabled) "
        "VALUES ($1, $2, 'slack', $3, $4, TRUE)",
        install_row_id, tenant_id, _TEAM_ID, bot_ref,
    )
    return tenant_id, install_row_id


def _make_client(fresh_db, secret_store, tenant_id, install_row_id) -> SlackClient:
    return SlackClient(
        pool=fresh_db,
        secret_store=secret_store,
        tenant_id=tenant_id,
        installation_row_id=install_row_id,
        team_id=_TEAM_ID,
    )


async def test_users_info_uses_installation_bot_token(
    fresh_db: asyncpg.Pool,
) -> None:
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    tenant, install = await _seed_install(fresh_db, store)
    client = _make_client(fresh_db, store, tenant, install)

    with respx.mock(base_url="https://slack.com") as router:
        route = router.get("/api/users.info").respond(
            200, json={"ok": True, "user": {"id": "U1", "real_name": "Alice"}},
        )
        result = await client.users_info("U1")
        await client.aclose()

    assert result["user"]["real_name"] == "Alice"
    # Verify the auth header carried the installation's bot token.
    assert route.calls.last.request.headers["authorization"] == f"Bearer {_BOT_TOKEN}"


async def test_chat_postmessage_serializes_kwargs(fresh_db: asyncpg.Pool) -> None:
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    tenant, install = await _seed_install(fresh_db, store)
    client = _make_client(fresh_db, store, tenant, install)

    with respx.mock(base_url="https://slack.com") as router:
        route = router.post("/api/chat.postMessage").respond(
            200, json={"ok": True, "ts": "123.456", "channel": "C1"},
        )
        result = await client.chat_post_message(
            channel="C1", text="hello", thread_ts="999.0",
        )
        await client.aclose()

    assert result["ts"] == "123.456"
    import json as _json
    body = _json.loads(route.calls.last.request.content)
    assert body == {"channel": "C1", "text": "hello", "thread_ts": "999.0"}


async def test_conversations_info_returns_record(fresh_db: asyncpg.Pool) -> None:
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    tenant, install = await _seed_install(fresh_db, store)
    client = _make_client(fresh_db, store, tenant, install)

    with respx.mock(base_url="https://slack.com") as router:
        router.get("/api/conversations.info").respond(
            200, json={"ok": True, "channel": {"id": "C1", "name": "eng"}},
        )
        r = await client.conversations_info("C1")
        await client.aclose()
    assert r["channel"]["name"] == "eng"


async def test_429_retry_after_honored(fresh_db: asyncpg.Pool) -> None:
    """429 with Retry-After: 0 succeeds on retry."""
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    tenant, install = await _seed_install(fresh_db, store)
    client = _make_client(fresh_db, store, tenant, install)

    with respx.mock(base_url="https://slack.com") as router:
        route = router.get("/api/users.info")
        route.side_effect = [
            httpx.Response(429, headers={"Retry-After": "0"}),
            httpx.Response(200, json={"ok": True, "user": {"id": "U1"}}),
        ]
        result = await client.users_info("U1")
        await client.aclose()

    assert result["user"]["id"] == "U1"


async def test_429_budget_exhausted_raises(fresh_db: asyncpg.Pool) -> None:
    """Continuous 429s eventually surface as SlackApiError."""
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    tenant, install = await _seed_install(fresh_db, store)
    client = SlackClient(
        pool=fresh_db,
        secret_store=store,
        tenant_id=tenant,
        installation_row_id=install,
        team_id=_TEAM_ID,
        max_attempts=2,
        wall_budget_s=1.0,
    )

    with respx.mock(base_url="https://slack.com") as router:
        router.get("/api/users.info").respond(
            429, headers={"Retry-After": "0"},
        )
        with pytest.raises(SlackApiError) as exc_info:
            await client.users_info("U1")
        await client.aclose()

    assert "429" in exc_info.value.message


async def test_slack_ok_false_raises_api_error(fresh_db: asyncpg.Pool) -> None:
    store = FernetSecretStore(fresh_db, master_kek=Fernet.generate_key())
    tenant, install = await _seed_install(fresh_db, store)
    client = _make_client(fresh_db, store, tenant, install)

    with respx.mock(base_url="https://slack.com") as router:
        router.get("/api/users.info").respond(
            200, json={"ok": False, "error": "user_not_found"},
        )
        with pytest.raises(SlackApiError) as exc_info:
            await client.users_info("U_GONE")
        await client.aclose()

    assert exc_info.value.context.get("slack_error") == "user_not_found"
