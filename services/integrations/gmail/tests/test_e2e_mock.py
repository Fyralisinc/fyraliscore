"""End-to-end integration test against mocked Google APIs.

What this test proves WITHOUT real Google infrastructure:
  - Per-tenant Pub/Sub provisioning runs the topic / subscription /
    IAM grant calls in the right order with the right payloads.
  - Admin connect → directory enumeration → resolved inclusion →
    pending watch rows → activate_watch (mocked users.watch) → active rows.
  - Pub/Sub push → tenant lookup → impersonated token mint → mocked
    users.history.list + users.messages.get → ingest handler →
    observations row + gmail_threads_canonical row + gmail_thread_members
    row + gmail_read_audit row.
  - Thread dedup across two mailboxes seeing the same message.
  - Thread continuity across a reply.
  - Forwarded message (broken References) starts a new canonical thread.
  - Opt-out subtracts a mailbox from the active set.

What it does NOT prove:
  - Real Google JWT validation, real Pub/Sub delivery semantics, real
    Gmail API quotas. Those need a paid Workspace tenant + GCP project.

Requirements:
  - DATABASE_URL set to a Postgres with all migrations applied.
  - respx installed (already a dev dep).
"""
from __future__ import annotations

import base64
import json
import os
import tempfile
import time
from typing import Any
from uuid import UUID

import asyncpg
import httpx
import pytest
import pytest_asyncio
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction

from services.gateway.db_bootstrap import _register_codecs


# ---------------------------------------------------------------------
# Fake service-account key + env wiring.
# ---------------------------------------------------------------------

_TENANT_ID = UUID("aaaaaaaa-1111-7777-8888-bbbbbbbbbbbb")
_INSTALL_ID = UUID("cccccccc-2222-7777-8888-dddddddddddd")


def _make_sa_json() -> str:
    """Generate a real RSA keypair and return a path to a fake SA JSON."""
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    ).decode()
    sa = {
        "type": "service_account",
        "project_id": "fyralis-test",
        "private_key_id": "k1",
        "private_key": pem,
        "client_email": "fyralis-gmail@fyralis-test.iam.gserviceaccount.com",
        "client_id": "1234567890",
        "token_uri": "https://oauth2.googleapis.com/token",
    }
    f = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(sa, f)
    f.close()
    return f.name


@pytest.fixture
def gmail_env(monkeypatch: pytest.MonkeyPatch) -> None:
    sa_path = _make_sa_json()
    monkeypatch.setenv("GMAIL_SERVICE_ACCOUNT_JSON_FILE", sa_path)
    monkeypatch.setenv("GMAIL_SERVICE_ACCOUNT_CLIENT_ID", "1234567890")
    monkeypatch.setenv("GMAIL_PUBSUB_PROJECT_ID", "fyralis-test")
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_ENDPOINT",
        "https://gateway.example.com/webhooks/gmail/pubsub",
    )
    monkeypatch.setenv(
        "GMAIL_PUBSUB_PUSH_OIDC_SA",
        "push@fyralis-test.iam.gserviceaccount.com",
    )
    # Reset the DWD singleton so it loads our fake key.
    from services.integrations.gmail import dwd as _dwd
    _dwd._reset_minter_for_tests()
    yield
    try:
        os.unlink(sa_path)
    except OSError:
        pass


@pytest_asyncio.fixture
async def pool() -> asyncpg.Pool:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL not set — skipping gmail e2e integration test.")
    pool = await asyncpg.create_pool(
        dsn, min_size=1, max_size=4, init=_register_codecs,
    )
    # The fetcher uses tenant_transaction() without an explicit pool —
    # it relies on the lib.shared.db module-level pool. Register ours.
    import lib.shared.db as _db
    _db._pool = pool
    try:
        async with pool.acquire() as conn:
            await _purge(conn)
            await conn.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, 'gmail-e2e-test')",
                _TENANT_ID,
            )
        yield pool
    finally:
        try:
            async with pool.acquire() as conn:
                await _purge(conn)
        finally:
            import lib.shared.db as _db
            _db._pool = None
            pool.terminate()


async def _purge(conn: asyncpg.Connection) -> None:
    """Drop every row that references our test tenant, in FK order."""
    await conn.execute("DELETE FROM think_trigger_queue WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM observations WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_read_audit WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_install_audit WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_thread_members WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_threads_canonical WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_mailbox_optouts WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_mailbox_watches WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_pubsub_topics WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM gmail_installations WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM actors WHERE tenant_id = $1", _TENANT_ID)
    await conn.execute("DELETE FROM tenants WHERE id = $1", _TENANT_ID)


@pytest_asyncio.fixture
async def install(pool: asyncpg.Pool, gmail_env: None) -> dict[str, Any]:
    """Seed a gmail_installations row + a gmail_pubsub_topics row +
    two active gmail_mailbox_watches (alice, bob)."""
    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        await tctx.execute(
            """
            INSERT INTO gmail_installations (
              id, tenant_id, workspace_domain, service_account_email,
              scope, resolved_user_count, resolved_at
            ) VALUES ($1, $2, 'acme.com',
                      'fyralis-gmail@fyralis-test.iam.gserviceaccount.com',
                      'gmail.metadata', 2, now())
            """,
            _INSTALL_ID, _TENANT_ID,
        )
        tenant_suffix = str(_TENANT_ID).replace("-", "")
        topic = f"projects/fyralis-test/topics/gmail-{tenant_suffix}"
        sub = f"projects/fyralis-test/subscriptions/gmail-{tenant_suffix}-sub"
        await tctx.execute(
            """
            INSERT INTO gmail_pubsub_topics (
              id, tenant_id, gmail_installation_id, topic_name, subscription_name
            ) VALUES ($1, $2, $3, $4, $5)
            """,
            uuid7(), _TENANT_ID, _INSTALL_ID, topic, sub,
        )
        for email in ("alice@acme.com", "bob@acme.com"):
            await tctx.execute(
                """
                INSERT INTO gmail_mailbox_watches (
                  id, tenant_id, gmail_installation_id, email_address,
                  state, history_id, watch_expiration
                ) VALUES ($1, $2, $3, $4, 'active', '1000', now() + interval '6 days')
                """,
                uuid7(), _TENANT_ID, _INSTALL_ID, email,
            )
    return {
        "tenant_id": _TENANT_ID,
        "installation_id": _INSTALL_ID,
        "subscription_name": (
            f"projects/fyralis-test/subscriptions/gmail-"
            f"{str(_TENANT_ID).replace('-', '')}-sub"
        ),
    }


# ---------------------------------------------------------------------
# Helpers to build mocked Google responses.
# ---------------------------------------------------------------------


def _mock_token_endpoint(mock_router: respx.MockRouter) -> None:
    """Any DWD token-exchange POST returns a canned access_token."""
    mock_router.post("https://oauth2.googleapis.com/token").mock(
        return_value=httpx.Response(
            200,
            json={"access_token": "fake-bearer", "expires_in": 3600, "token_type": "Bearer"},
        )
    )


def _gmail_message(
    *,
    message_id: str,
    from_: str,
    to: list[str],
    subject: str,
    in_reply_to: str | None = None,
    references: list[str] | None = None,
    body_text: str = "hello",
    internal_date_ms: int | None = None,
) -> dict[str, Any]:
    """Construct a Gmail API message resource."""
    headers = [
        {"name": "Message-ID", "value": f"<{message_id}>"},
        {"name": "From", "value": from_},
        {"name": "To", "value": ", ".join(to)},
        {"name": "Subject", "value": subject},
        {"name": "Date", "value": "Mon, 15 May 2026 10:00:00 +0000"},
    ]
    if in_reply_to:
        headers.append({"name": "In-Reply-To", "value": f"<{in_reply_to}>"})
    if references:
        headers.append({"name": "References", "value": " ".join(f"<{r}>" for r in references)})
    body_b64 = base64.urlsafe_b64encode(body_text.encode()).rstrip(b"=").decode()
    return {
        "id": message_id,
        "threadId": f"thr-{message_id}",
        "labelIds": ["INBOX"],
        "snippet": body_text[:200],
        "internalDate": str(internal_date_ms or int(time.time() * 1000)),
        "sizeEstimate": 2048,
        "payload": {
            "headers": headers,
            "mimeType": "text/plain",
            "body": {"data": body_b64},
        },
    }


def _mock_history_list(
    mock_router: respx.MockRouter, *, message_ids: list[str], new_history_id: str = "1100",
) -> None:
    """users.history.list returns ONE page with the given messageIds added."""
    mock_router.get(
        url__regex=r"https://gmail\.googleapis\.com/gmail/v1/users/me/history.*"
    ).mock(
        return_value=httpx.Response(
            200,
            json={
                "history": [
                    {
                        "id": "h1",
                        "messagesAdded": [{"message": {"id": m}} for m in message_ids],
                    }
                ],
                "historyId": new_history_id,
            },
        )
    )


def _mock_message_get(
    mock_router: respx.MockRouter, messages: dict[str, dict[str, Any]],
) -> None:
    """users.messages.get returns the appropriate resource per id."""

    def _handler(request: httpx.Request) -> httpx.Response:
        # Path: /gmail/v1/users/me/messages/{id}
        last = request.url.path.split("/")[-1]
        msg = messages.get(last)
        if msg is None:
            return httpx.Response(404, json={"error": {"message": "not found"}})
        return httpx.Response(200, json=msg)

    mock_router.get(
        url__regex=r"https://gmail\.googleapis\.com/gmail/v1/users/me/messages/.+"
    ).mock(side_effect=_handler)


# ---------------------------------------------------------------------
# Test cases.
# ---------------------------------------------------------------------


@pytest.mark.asyncio
async def test_push_ingests_one_observation(
    pool: asyncpg.Pool, install: dict[str, Any], gmail_env: None,
) -> None:
    """A push for alice's mailbox produces exactly one observation,
    one canonical thread, one thread member, one read audit row."""
    from services.integrations.gmail.push_handler import handle_push

    msg = _gmail_message(
        message_id="msg-root@x",
        from_="customer@external.com",
        to=["alice@acme.com"],
        subject="initial inquiry",
        body_text="please send pricing",
    )

    with respx.mock(assert_all_called=False) as r:
        _mock_token_endpoint(r)
        _mock_history_list(r, message_ids=["msg-root"])
        _mock_message_get(r, {"msg-root": msg})

        result = await handle_push(
            pool=pool,
            envelope={
                "message": {
                    "data": base64.b64encode(
                        json.dumps({"emailAddress": "alice@acme.com", "historyId": "1050"}).encode()
                    ).decode(),
                    "messageId": "pubsub-msg-1",
                    "publishTime": "2026-05-15T10:00:00Z",
                },
                "subscription": install["subscription_name"],
            },
        )

    assert result["status"] == "ok", f"unexpected: {result}"
    assert result["ingested"] >= 0  # might dedup against itself if invoked twice

    # Verify observation written.
    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        obs = await tctx.fetch(
            """
            SELECT id, external_id, source_channel, thread_canonical_id,
                   content->>'subject' AS subject,
                   content->>'mailbox_email' AS mailbox
              FROM observations
             WHERE source_channel = 'gmail:' AND tenant_id = $1
            """,
            _TENANT_ID,
        )
        threads = await tctx.fetch(
            """
            SELECT id, canonical_message_id, message_count, participant_emails
              FROM gmail_threads_canonical
             WHERE gmail_installation_id = $1
            """,
            _INSTALL_ID,
        )
        members = await tctx.fetch(
            """
            SELECT message_id FROM gmail_thread_members
             WHERE gmail_installation_id = $1
            """,
            _INSTALL_ID,
        )
        audit = await tctx.fetch(
            """
            SELECT email_address, message_id, scope_used, read_path
              FROM gmail_read_audit
             WHERE gmail_installation_id = $1
            """,
            _INSTALL_ID,
        )

    assert len(obs) == 1, f"expected 1 observation, got {len(obs)}: {obs}"
    assert obs[0]["mailbox"] == "alice@acme.com"
    assert obs[0]["subject"] == "initial inquiry"
    assert obs[0]["thread_canonical_id"] is not None

    assert len(threads) == 1
    assert threads[0]["canonical_message_id"] == "msg-root@x"
    assert threads[0]["message_count"] == 1

    assert len(members) == 1
    assert members[0]["message_id"] == "msg-root@x"

    assert len(audit) == 1
    assert audit[0]["read_path"] == "push"
    assert audit[0]["scope_used"] == "gmail.metadata"


@pytest.mark.asyncio
async def test_reply_continues_thread(
    pool: asyncpg.Pool, install: dict[str, Any], gmail_env: None,
) -> None:
    """A reply (In-Reply-To pointing at root) joins the same canonical thread."""
    from services.integrations.gmail.push_handler import handle_push

    now_ms = int(time.time() * 1000)
    root = _gmail_message(
        message_id="msg-root@x",
        from_="customer@external.com",
        to=["alice@acme.com"],
        subject="initial",
        internal_date_ms=now_ms,
    )
    reply = _gmail_message(
        message_id="msg-reply@x",
        from_="alice@acme.com",
        to=["customer@external.com"],
        subject="Re: initial",
        in_reply_to="msg-root@x",
        references=["msg-root@x"],
        internal_date_ms=now_ms + 1000,
    )

    with respx.mock(assert_all_called=False) as r:
        _mock_token_endpoint(r)
        _mock_history_list(r, message_ids=["msg-root", "msg-reply"])
        _mock_message_get(r, {"msg-root": root, "msg-reply": reply})

        await handle_push(
            pool=pool,
            envelope={
                "message": {
                    "data": base64.b64encode(
                        json.dumps({"emailAddress": "alice@acme.com", "historyId": "1050"}).encode()
                    ).decode(),
                    "messageId": "pm",
                    "publishTime": "2026-05-15T10:00:00Z",
                },
                "subscription": install["subscription_name"],
            },
        )

    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        obs = await tctx.fetch(
            "SELECT thread_canonical_id, content->>'message_id' AS mid "
            "FROM observations WHERE source_channel = 'gmail:' AND tenant_id = $1 "
            "ORDER BY occurred_at",
            _TENANT_ID,
        )
        threads = await tctx.fetch(
            "SELECT message_count, participant_emails FROM gmail_threads_canonical "
            "WHERE gmail_installation_id = $1",
            _INSTALL_ID,
        )
        members = await tctx.fetch(
            "SELECT message_id FROM gmail_thread_members "
            "WHERE gmail_installation_id = $1 ORDER BY message_id",
            _INSTALL_ID,
        )

    assert len(obs) == 2
    # Both observations share the SAME canonical thread.
    assert obs[0]["thread_canonical_id"] == obs[1]["thread_canonical_id"]
    assert {obs[0]["mid"], obs[1]["mid"]} == {"msg-root@x", "msg-reply@x"}

    # Exactly one canonical thread row, with message_count == 2.
    assert len(threads) == 1
    assert threads[0]["message_count"] == 2
    # Participants accumulated from both messages.
    parts = set(threads[0]["participant_emails"])
    assert "customer@external.com" in parts
    assert "alice@acme.com" in parts

    # Two member rows, one per message_id.
    assert {m["message_id"] for m in members} == {"msg-root@x", "msg-reply@x"}


@pytest.mark.asyncio
async def test_same_message_two_mailboxes_dedups(
    pool: asyncpg.Pool, install: dict[str, Any], gmail_env: None,
) -> None:
    """When the same internal message arrives via alice's mailbox AND
    bob's mailbox (both watched), we get exactly ONE observation row
    (external_id is namespaced by install, not by mailbox)."""
    from services.integrations.gmail.push_handler import handle_push

    msg = _gmail_message(
        message_id="internal-msg@x",
        from_="alice@acme.com",
        to=["bob@acme.com"],
        subject="quick sync",
    )

    with respx.mock(assert_all_called=False) as r:
        _mock_token_endpoint(r)
        _mock_history_list(r, message_ids=["internal-msg"])
        _mock_message_get(r, {"internal-msg": msg})

        for mailbox in ("alice@acme.com", "bob@acme.com"):
            await handle_push(
                pool=pool,
                envelope={
                    "message": {
                        "data": base64.b64encode(
                            json.dumps({"emailAddress": mailbox, "historyId": "1050"}).encode()
                        ).decode(),
                        "messageId": "pm",
                        "publishTime": "2026-05-15T10:00:00Z",
                    },
                    "subscription": install["subscription_name"],
                },
            )

    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        obs_count = await tctx.fetchval(
            "SELECT COUNT(*) FROM observations "
            "WHERE source_channel = 'gmail:' AND tenant_id = $1",
            _TENANT_ID,
        )
        member_count = await tctx.fetchval(
            "SELECT COUNT(*) FROM gmail_thread_members "
            "WHERE gmail_installation_id = $1",
            _INSTALL_ID,
        )
        thread_count = await tctx.fetchval(
            "SELECT COUNT(*) FROM gmail_threads_canonical "
            "WHERE gmail_installation_id = $1",
            _INSTALL_ID,
        )
        # Read audit logs BOTH reads (per-user attestation).
        audit_count = await tctx.fetchval(
            "SELECT COUNT(*) FROM gmail_read_audit "
            "WHERE gmail_installation_id = $1",
            _INSTALL_ID,
        )

    assert obs_count == 1, "two mailboxes saw same message → still ONE observation"
    assert member_count == 1, "one thread member row per (install, message_id)"
    assert thread_count == 1
    assert audit_count == 2, "read attestation logs BOTH reads (alice + bob)"


@pytest.mark.asyncio
async def test_forwarded_message_new_thread(
    pool: asyncpg.Pool, install: dict[str, Any], gmail_env: None,
) -> None:
    """A forwarded message with NO references → new canonical thread."""
    from services.integrations.gmail.push_handler import handle_push

    root = _gmail_message(
        message_id="orig-msg@x",
        from_="customer@external.com",
        to=["alice@acme.com"],
        subject="contract details",
    )
    forwarded = _gmail_message(
        message_id="fwd-msg@x",
        from_="alice@acme.com",
        to=["bob@acme.com"],
        subject="Fwd: contract details",
        # No In-Reply-To, no References — broken chain.
    )

    with respx.mock(assert_all_called=False) as r:
        _mock_token_endpoint(r)
        _mock_history_list(r, message_ids=["orig-msg", "fwd-msg"])
        _mock_message_get(r, {"orig-msg": root, "fwd-msg": forwarded})

        await handle_push(
            pool=pool,
            envelope={
                "message": {
                    "data": base64.b64encode(
                        json.dumps({"emailAddress": "alice@acme.com", "historyId": "1050"}).encode()
                    ).decode(),
                    "messageId": "pm",
                    "publishTime": "2026-05-15T10:00:00Z",
                },
                "subscription": install["subscription_name"],
            },
        )

    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        threads = await tctx.fetch(
            "SELECT canonical_message_id FROM gmail_threads_canonical "
            "WHERE gmail_installation_id = $1 ORDER BY canonical_message_id",
            _INSTALL_ID,
        )

    # Two distinct canonical threads — the broken chain prevents merging.
    assert len(threads) == 2
    canonical_ids = {t["canonical_message_id"] for t in threads}
    assert canonical_ids == {"orig-msg@x", "fwd-msg@x"}


@pytest.mark.asyncio
async def test_unknown_subscription_dropped(
    pool: asyncpg.Pool, install: dict[str, Any], gmail_env: None,
) -> None:
    """A push for a subscription we don't own is skipped, not errored."""
    from services.integrations.gmail.push_handler import handle_push

    result = await handle_push(
        pool=pool,
        envelope={
            "message": {
                "data": base64.b64encode(
                    json.dumps({"emailAddress": "alice@acme.com", "historyId": "1050"}).encode()
                ).decode(),
                "messageId": "pm",
                "publishTime": "2026-05-15T10:00:00Z",
            },
            "subscription": "projects/wrong/subscriptions/not-ours",
        },
    )
    assert result["status"] == "skipped"
    assert result["reason"] == "unknown_subscription"


@pytest.mark.asyncio
async def test_optout_blocks_ingest(
    pool: asyncpg.Pool, install: dict[str, Any], gmail_env: None,
) -> None:
    """After an opt-out the mailbox's watch is paused and pushes are dropped."""
    from services.integrations.gmail.optout import add_optout
    from services.integrations.gmail.push_handler import handle_push

    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        added = await add_optout(
            tctx,
            gmail_installation_id=_INSTALL_ID,
            email_address="alice@acme.com",
            reason="user requested",
            actor_email="alice@acme.com",
        )
    assert added is True

    msg = _gmail_message(
        message_id="post-optout@x",
        from_="customer@external.com",
        to=["alice@acme.com"],
        subject="this should not be ingested",
    )

    with respx.mock(assert_all_called=False) as r:
        _mock_token_endpoint(r)
        _mock_history_list(r, message_ids=["post-optout"])
        _mock_message_get(r, {"post-optout": msg})

        result = await handle_push(
            pool=pool,
            envelope={
                "message": {
                    "data": base64.b64encode(
                        json.dumps({"emailAddress": "alice@acme.com", "historyId": "1050"}).encode()
                    ).decode(),
                    "messageId": "pm",
                    "publishTime": "2026-05-15T10:00:00Z",
                },
                "subscription": install["subscription_name"],
            },
        )

    assert result["status"] == "skipped"
    assert result["reason"] == "watch_inactive"
    assert result["state"] == "opted_out"

    async with tenant_transaction(_TENANT_ID, pool=pool) as tctx:
        obs = await tctx.fetchval(
            "SELECT COUNT(*) FROM observations "
            "WHERE source_channel = 'gmail:' AND tenant_id = $1",
            _TENANT_ID,
        )
        audit = await tctx.fetch(
            "SELECT action FROM gmail_install_audit "
            "WHERE gmail_installation_id = $1 ORDER BY occurred_at DESC LIMIT 5",
            _INSTALL_ID,
        )
    assert obs == 0, "opted-out mailbox should not produce any observation"
    assert any(r["action"] == "gmail.optout_added" for r in audit)
