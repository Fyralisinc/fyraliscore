from __future__ import annotations

import json
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

import pytest

from services.ingest.integrations.gmail import uninstall as gmail_uninstall


pytestmark = pytest.mark.asyncio


class _FakeTenantContext:
    def __init__(
        self,
        tenant_id: UUID,
        *,
        install: dict[str, object] | None = None,
        watches: list[dict[str, object]] | None = None,
    ) -> None:
        self.tenant_id = tenant_id
        self.install = install
        self.watches = watches or []
        self.executed: list[tuple[str, tuple[object, ...]]] = []

    async def fetchrow(self, query: str, *args: object) -> dict[str, object] | None:
        if "FROM gmail_installations" in query:
            return self.install
        raise AssertionError(f"unexpected fetchrow query: {query}")

    async def fetch(self, query: str, *args: object) -> list[dict[str, object]]:
        if "FROM gmail_mailbox_watches" in query:
            return self.watches
        raise AssertionError(f"unexpected fetch query: {query}")

    async def execute(self, query: str, *args: object) -> str:
        self.executed.append((query, args))
        return "UPDATE 1"


def _patch_tenant_transactions(
    monkeypatch: pytest.MonkeyPatch,
    tenant_id: UUID,
    contexts: list[_FakeTenantContext],
) -> None:
    remaining = list(contexts)

    @asynccontextmanager
    async def _tenant_transaction(actual_tenant_id: UUID):
        assert actual_tenant_id == tenant_id
        if not remaining:
            raise AssertionError("tenant_transaction called too many times")
        yield remaining.pop(0)

    monkeypatch.setattr(gmail_uninstall, "tenant_transaction", _tenant_transaction)


def _patch_google_stop(
    monkeypatch: pytest.MonkeyPatch,
    calls: list[tuple[str, str]],
) -> None:
    class _FakeGoogleHttpClient:
        def __init__(self, minter: object) -> None:
            self.minter = minter

        async def __aenter__(self) -> "_FakeGoogleHttpClient":
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

    class _FakeGmailClient:
        def __init__(self, http: object) -> None:
            self.http = http

        async def stop(self, *, user_email: str, scope: str) -> None:
            calls.append((user_email, scope))

    monkeypatch.setattr(gmail_uninstall, "get_minter", lambda: object())
    monkeypatch.setattr(gmail_uninstall, "GoogleHttpClient", _FakeGoogleHttpClient)
    monkeypatch.setattr(gmail_uninstall, "GmailClient", _FakeGmailClient)


def _patch_pubsub(
    monkeypatch: pytest.MonkeyPatch,
    calls: list[UUID],
    *,
    fail: bool,
) -> None:
    class _FakePubsubAdmin:
        async def __aenter__(self) -> "_FakePubsubAdmin":
            return self

        async def __aexit__(self, *exc: object) -> None:
            return None

        async def teardown(self, tenant_id: UUID) -> None:
            calls.append(tenant_id)
            if fail:
                raise RuntimeError("delete failed")

    monkeypatch.setattr(gmail_uninstall, "PubsubAdmin", _FakePubsubAdmin)


def _executed_query(ctx: _FakeTenantContext, fragment: str) -> tuple[str, tuple[object, ...]]:
    for query, args in ctx.executed:
        if fragment in query:
            return query, args
    raise AssertionError(f"query containing {fragment!r} was not executed")


def _audit_details(ctx: _FakeTenantContext) -> dict[str, object]:
    _, args = _executed_query(ctx, "INSERT INTO gmail_install_audit")
    return json.loads(str(args[5]))


async def test_uninstall_clears_local_watch_state_and_tears_down_pubsub(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    stop_calls: list[tuple[str, str]] = []
    pubsub_calls: list[UUID] = []
    read_ctx = _FakeTenantContext(
        tenant_id,
        install={"id": installation_id, "scope": "gmail.metadata", "disabled_at": None},
        watches=[
            {"id": uuid4(), "email_address": "alice@example.test"},
            {"id": uuid4(), "email_address": "bob@example.test"},
        ],
    )
    write_ctx = _FakeTenantContext(tenant_id)
    _patch_tenant_transactions(monkeypatch, tenant_id, [read_ctx, write_ctx])
    _patch_google_stop(monkeypatch, stop_calls)
    _patch_pubsub(monkeypatch, pubsub_calls, fail=False)

    await gmail_uninstall.uninstall_install(
        tenant_id=tenant_id,
        gmail_installation_id=installation_id,
        actor_email="ops@example.test",
    )

    assert stop_calls == [
        ("alice@example.test", "gmail.metadata"),
        ("bob@example.test", "gmail.metadata"),
    ]
    assert pubsub_calls == [tenant_id]
    watch_update, args = _executed_query(write_ctx, "UPDATE gmail_mailbox_watches")
    assert args == (installation_id,)
    assert "state = 'paused'" in watch_update
    assert "history_id = NULL" in watch_update
    assert "watch_expiration = NULL" in watch_update
    assert "last_push_at = NULL" in watch_update
    assert "last_poll_at = NULL" in watch_update
    assert "consecutive_poll_failures = 0" in watch_update
    assert "last_error = NULL" in watch_update
    assert _audit_details(write_ctx) == {
        "watches_stopped": 2,
        "pubsub_teardown_succeeded": True,
    }


async def test_uninstall_still_disables_local_rows_when_pubsub_teardown_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    stop_calls: list[tuple[str, str]] = []
    pubsub_calls: list[UUID] = []
    read_ctx = _FakeTenantContext(
        tenant_id,
        install={"id": installation_id, "scope": "gmail.metadata", "disabled_at": None},
        watches=[],
    )
    write_ctx = _FakeTenantContext(tenant_id)
    _patch_tenant_transactions(monkeypatch, tenant_id, [read_ctx, write_ctx])
    _patch_google_stop(monkeypatch, stop_calls)
    _patch_pubsub(monkeypatch, pubsub_calls, fail=True)

    await gmail_uninstall.uninstall_install(
        tenant_id=tenant_id,
        gmail_installation_id=installation_id,
    )

    assert pubsub_calls == [tenant_id]
    _executed_query(write_ctx, "UPDATE gmail_mailbox_watches")
    _executed_query(write_ctx, "UPDATE gmail_pubsub_topics")
    _executed_query(write_ctx, "UPDATE gmail_installations")
    assert _audit_details(write_ctx) == {
        "watches_stopped": 0,
        "pubsub_teardown_succeeded": False,
    }


async def test_stop_mailbox_clears_local_watch_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tenant_id = uuid4()
    installation_id = uuid4()
    stop_calls: list[tuple[str, str]] = []
    read_ctx = _FakeTenantContext(
        tenant_id,
        install={"scope": "gmail.metadata"},
    )
    write_ctx = _FakeTenantContext(tenant_id)
    _patch_tenant_transactions(monkeypatch, tenant_id, [read_ctx, write_ctx])
    _patch_google_stop(monkeypatch, stop_calls)

    await gmail_uninstall.stop_mailbox(
        tenant_id=tenant_id,
        gmail_installation_id=installation_id,
        email_address="Alice@Example.Test",
    )

    assert stop_calls == [("Alice@Example.Test", "gmail.metadata")]
    watch_update, args = _executed_query(write_ctx, "UPDATE gmail_mailbox_watches")
    assert args == (installation_id, "alice@example.test")
    assert "state = 'paused'" in watch_update
    assert "history_id = NULL" in watch_update
    assert "watch_expiration = NULL" in watch_update
    assert "last_push_at = NULL" in watch_update
    assert "last_poll_at = NULL" in watch_update
    assert "consecutive_poll_failures = 0" in watch_update
    assert "last_error = NULL" in watch_update
