"""Tests for the Gmail history.list fallback poller."""

from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction
from services.ingest.integrations.gmail import fetcher as gmail_fetcher
from services.ingest.integrations.gmail.client import (
    GmailHistoryExpired,
    GmailHistoryRecoveryIncomplete,
    GoogleApiError,
)
from services.ingest.integrations.gmail import history_poller


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def _seeded_tenants(fresh_db: asyncpg.Pool):
    tenants: list[UUID] = []
    try:
        yield tenants
    finally:
        for tenant in tenants:
            async with tenant_transaction(tenant, pool=fresh_db) as tctx:
                await tctx.execute(
                    "DELETE FROM gmail_mailbox_watches WHERE tenant_id = $1",
                    tenant,
                )
                await tctx.execute(
                    "DELETE FROM gmail_installations WHERE tenant_id = $1",
                    tenant,
                )
            await fresh_db.execute("DELETE FROM tenants WHERE id = $1", tenant)


async def _seed_mailbox(
    pool: asyncpg.Pool,
    seeded_tenants: list[UUID],
    *,
    email: str,
    history_id: str = "1000",
) -> UUID:
    tenant = uuid4()
    seeded_tenants.append(tenant)
    await pool.execute(
        "INSERT INTO tenants (id, name) VALUES ($1, 'gmail-history')", tenant
    )
    install = uuid7()
    watch = uuid7()
    async with tenant_transaction(tenant, pool=pool) as tctx:
        await tctx.execute(
            """
            INSERT INTO gmail_installations (
              id, tenant_id, workspace_domain, service_account_email,
              scope, resolved_user_count, resolved_at
            )
            VALUES (
              $1, $2, 'acme.com', 'svc@acme.iam',
              'gmail.metadata', 1, now()
            )
            """,
            install,
            tenant,
        )
        await tctx.execute(
            """
            INSERT INTO gmail_mailbox_watches (
              id, tenant_id, gmail_installation_id, email_address,
              state, history_id, watch_expiration
            )
            VALUES (
              $1, $2, $3, $4, 'active', $5, now() + interval '6 days'
            )
            """,
            watch,
            tenant,
            install,
            email,
            history_id,
        )
    return watch


async def test_concurrent_replicas_lease_distinct_mailboxes(
    fresh_db: asyncpg.Pool,
    _seeded_tenants: list[UUID],
) -> None:
    watch_a = await _seed_mailbox(
        fresh_db,
        _seeded_tenants,
        email="alice@acme.com",
    )
    watch_b = await _seed_mailbox(
        fresh_db,
        _seeded_tenants,
        email="bob@acme.com",
    )
    first_ready = asyncio.Event()
    release = asyncio.Event()

    async def _lease_one() -> UUID:
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                rows = await history_poller._lease_due_mailboxes(conn, limit=1)
                assert len(rows) == 1
                first_ready.set()
                await release.wait()
                return rows[0]["id"]

    first = asyncio.create_task(_lease_one())
    try:
        await asyncio.wait_for(first_ready.wait(), timeout=5.0)
        async with fresh_db.acquire() as conn:
            async with conn.transaction():
                rows = await history_poller._lease_due_mailboxes(conn, limit=1)
                assert len(rows) == 1
                second_id = rows[0]["id"]
        release.set()
        first_id = await asyncio.wait_for(first, timeout=5.0)
    finally:
        release.set()

    assert {first_id, second_id} == {watch_a, watch_b}


async def _load_binding(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    watch_id: UUID,
) -> asyncpg.Record:
    async with tenant_transaction(tenant_id, pool=pool) as tctx:
        row = await tctx.fetchrow(
            """
            SELECT id, gmail_installation_id, email_address, history_id,
                   last_push_at, last_poll_at, last_error
              FROM gmail_mailbox_watches
             WHERE id = $1
            """,
            watch_id,
        )
    assert row is not None
    return row


class _ExpiredHistoryGmail:
    def __init__(self, *, fail_message: str | None = None) -> None:
        self.fail_message = fail_message
        self.history_calls = 0
        self.profile_calls = 0
        self.messages_list_calls = 0

    async def history_list(
        self,
        *,
        start_history_id: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        self.history_calls += 1
        if start_history_id == "1000":
            raise GmailHistoryExpired(
                "expired",
                status=404,
                start_history_id=start_history_id,
            )
        return {
            "history": [{"messagesAdded": [{"message": {"id": "catch-up-message"}}]}],
            "historyId": "5001",
        }

    async def get_profile(self, **_kwargs: Any) -> dict[str, Any]:
        self.profile_calls += 1
        return {"historyId": "5000"}

    async def messages_list(self, **_kwargs: Any) -> dict[str, Any]:
        self.messages_list_calls += 1
        return {"messages": [{"id": "snapshot-message"}]}

    async def get_message(
        self,
        *,
        message_id: str,
        **_kwargs: Any,
    ) -> dict[str, Any]:
        if message_id == self.fail_message:
            raise GoogleApiError("message unavailable", status=503)
        return {"id": message_id}


def _patch_successful_inline_dispatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _dispatch(
        _ctx: Any,
        _resource: dict[str, Any],
    ) -> dict[str, Any]:
        return {"deduped": False}

    async def _audit(**_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(
        gmail_fetcher,
        "_dispatch_gmail_resource_inline",
        _dispatch,
    )
    monkeypatch.setattr(gmail_fetcher, "_write_gmail_read_audit", _audit)


async def test_expired_history_recovery_commits_only_after_snapshot_and_catchup(
    fresh_db: asyncpg.Pool,
    _seeded_tenants: list[UUID],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watch_id = await _seed_mailbox(
        fresh_db,
        _seeded_tenants,
        email="recovery-success@acme.com",
    )
    tenant_id = _seeded_tenants[-1]
    before = await _load_binding(fresh_db, tenant_id, watch_id)
    gmail = _ExpiredHistoryGmail()
    _patch_successful_inline_dispatch(monkeypatch)

    result = await gmail_fetcher.drain_mailbox_history(
        pool=fresh_db,
        gmail=gmail,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        gmail_installation_id=before["gmail_installation_id"],
        email_address=before["email_address"],
        read_path="push",
    )

    after = await _load_binding(fresh_db, tenant_id, watch_id)
    assert result == {
        "status": "recovered",
        "ingested": 2,
        "deduped": 0,
        "messages_seen": 2,
        "history_id": "5001",
    }
    assert after["history_id"] == "5001"
    assert after["last_error"] is None
    assert after["last_push_at"] is not None
    assert gmail.history_calls == 2


async def test_failed_recovery_keeps_cursor_and_durably_cools_push_retries(
    fresh_db: asyncpg.Pool,
    _seeded_tenants: list[UUID],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    watch_id = await _seed_mailbox(
        fresh_db,
        _seeded_tenants,
        email="recovery-failure@acme.com",
    )
    tenant_id = _seeded_tenants[-1]
    before = await _load_binding(fresh_db, tenant_id, watch_id)
    gmail = _ExpiredHistoryGmail(fail_message="snapshot-message")
    _patch_successful_inline_dispatch(monkeypatch)

    with pytest.raises(GmailHistoryRecoveryIncomplete):
        await gmail_fetcher.drain_mailbox_history(
            pool=fresh_db,
            gmail=gmail,  # type: ignore[arg-type]
            tenant_id=tenant_id,
            gmail_installation_id=before["gmail_installation_id"],
            email_address=before["email_address"],
            read_path="push",
        )

    failed = await _load_binding(fresh_db, tenant_id, watch_id)
    assert failed["history_id"] == "1000"
    assert failed["last_push_at"] is None
    assert failed["last_poll_at"] is not None
    assert failed["last_error"].startswith("history_recovery:")
    calls_after_failure = gmail.history_calls

    deferred = await gmail_fetcher.drain_mailbox_history(
        pool=fresh_db,
        gmail=gmail,  # type: ignore[arg-type]
        tenant_id=tenant_id,
        gmail_installation_id=before["gmail_installation_id"],
        email_address=before["email_address"],
        read_path="push",
    )

    assert deferred["status"] == "retry_later"
    assert deferred["reason"] == "history_recovery_cooldown"
    assert "not_before" in deferred
    assert gmail.history_calls == calls_after_failure
    assert (await _load_binding(fresh_db, tenant_id, watch_id))["history_id"] == "1000"
