"""Unit tests for the Notion revocation chokepoint (IN-14 hardening).

DB-free: an inline fake asyncpg pool records the UPDATE / audit calls, so
these run without touching the dev database.
"""
from __future__ import annotations

from uuid import uuid4

import pytest

from services.ingest.integrations.notion.uninstall import (
    _disable_installation_notion,
)


pytestmark = pytest.mark.asyncio


class _FakeConn:
    def __init__(self, row_to_return):
        self._row = row_to_return
        self.fetchrow_calls: list[tuple] = []
        self.execute_calls: list[tuple] = []

    def transaction(self):
        class _Txn:
            async def __aenter__(self_):
                return None

            async def __aexit__(self_, *a):
                return False

        return _Txn()

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        return self._row

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))


class _FakePool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        conn = self._conn

        class _Acq:
            async def __aenter__(self_):
                return conn

            async def __aexit__(self_, *a):
                return False

        return _Acq()


async def test_disable_first_fire_updates_and_audits():
    conn = _FakeConn(row_to_return={"id": uuid4()})
    tenant = uuid4()
    fired = await _disable_installation_notion(
        pool=_FakePool(conn), tenant_id=tenant, workspace_id="ws-1",
    )
    assert fired is True
    # UPDATE ran with (workspace_id, tenant_id) and guards on enabled=TRUE.
    assert len(conn.fetchrow_calls) == 1
    sql, args = conn.fetchrow_calls[0]
    assert "provider_installations" in sql and "enabled = FALSE" in sql
    assert "enabled = TRUE" in sql  # idempotency guard
    assert args == ("ws-1", tenant)
    # One audit row written.
    assert len(conn.execute_calls) == 1
    assert "installation_audit_log" in conn.execute_calls[0][0]


async def test_disable_idempotent_when_already_disabled():
    # Row already disabled / not found → UPDATE ... RETURNING yields None.
    conn = _FakeConn(row_to_return=None)
    fired = await _disable_installation_notion(
        pool=_FakePool(conn), tenant_id=uuid4(), workspace_id="ws-1",
    )
    assert fired is False
    assert len(conn.fetchrow_calls) == 1
    assert conn.execute_calls == []  # no audit on a no-op fire


async def test_disable_never_raises_on_pool_error():
    class _BoomPool:
        def acquire(self):
            raise RuntimeError("db down")

    # Chokepoint must swallow its own failure (caller surfaces the 401).
    fired = await _disable_installation_notion(
        pool=_BoomPool(), tenant_id=uuid4(), workspace_id="ws-1",
    )
    assert fired is False
