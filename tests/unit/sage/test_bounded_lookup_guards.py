from __future__ import annotations

from typing import Any

import asyncpg
import pytest

from services.platform.execution import inquiry as inquiry_mod
from services.reasoning.sage import reader as reader_mod


class _FakeTransaction:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeTransaction:
        self._conn.transaction_enter_count += 1
        return self

    async def __aexit__(self, *_exc: object) -> bool:
        self._conn.transaction_exit_count += 1
        return False


class _FakeConn:
    def __init__(
        self,
        *,
        rows: list[dict[str, Any]] | None = None,
        error: BaseException | None = None,
        outer_transaction: bool = False,
        statement_timeout: str = "0",
    ) -> None:
        self.rows = rows or []
        self.error = error
        self.outer_transaction = outer_transaction
        self.statement_timeout = statement_timeout
        self.executed: list[str] = []
        self.fetchval_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[Any, ...]]] = []
        self.transaction_enter_count = 0
        self.transaction_exit_count = 0

    def is_in_transaction(self) -> bool:
        return self.outer_transaction

    def transaction(self) -> _FakeTransaction:
        return _FakeTransaction(self)

    async def execute(self, query: str) -> None:
        self.executed.append(query)

    async def fetchval(self, query: str, *args: Any) -> Any:
        self.fetchval_calls.append((query, args))
        if "current_setting('statement_timeout')" in query:
            return self.statement_timeout
        if "set_config('statement_timeout'" in query:
            self.statement_timeout = str(args[0])
            return self.statement_timeout
        return None

    async def fetch(self, query: str, *args: Any) -> list[dict[str, Any]]:
        self.fetch_calls.append((query, args))
        if self.error is not None:
            raise self.error
        return self.rows


@pytest.mark.asyncio
async def test_sage_bounded_lookup_returns_rows_under_statement_timeout() -> None:
    conn = _FakeConn(rows=[{"id": "model-a"}])

    rows = await reader_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT $1",
        "needle",
        label="unit",
    )

    assert rows == [{"id": "model-a"}]
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert conn.fetch_calls == [("SELECT $1", ("needle",))]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_sage_bounded_lookup_degrades_on_statement_timeout() -> None:
    conn = _FakeConn(error=asyncpg.QueryCanceledError("statement timeout"))

    rows = await reader_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="unit_timeout",
    )

    assert rows == []
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert len(conn.fetch_calls) == 1
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_sage_bounded_lookup_restores_outer_transaction_timeout() -> None:
    conn = _FakeConn(
        error=asyncpg.QueryCanceledError("statement timeout"),
        outer_transaction=True,
        statement_timeout="7s",
    )

    rows = await reader_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="outer_timeout",
    )

    assert rows == []
    assert conn.executed == []
    assert conn.statement_timeout == "7s"
    assert conn.fetchval_calls == [
        ("SELECT current_setting('statement_timeout')", ()),
        ("SELECT set_config('statement_timeout', $1, true)", ("1500",)),
        ("SELECT set_config('statement_timeout', $1, true)", ("7s",)),
    ]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_inquiry_bounded_lookup_returns_rows_under_statement_timeout() -> None:
    conn = _FakeConn(rows=[{"id": "model-b"}])

    rows = await inquiry_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT $1",
        "needle",
        label="unit",
    )

    assert rows == [{"id": "model-b"}]
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert conn.fetch_calls == [("SELECT $1", ("needle",))]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_inquiry_bounded_lookup_degrades_on_statement_timeout() -> None:
    conn = _FakeConn(error=asyncpg.QueryCanceledError("statement timeout"))

    rows = await inquiry_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="unit_timeout",
    )

    assert rows == []
    assert conn.executed == ["SET LOCAL statement_timeout = 1500"]
    assert len(conn.fetch_calls) == 1
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1


@pytest.mark.asyncio
async def test_inquiry_bounded_lookup_restores_outer_transaction_timeout() -> None:
    conn = _FakeConn(
        error=asyncpg.QueryCanceledError("statement timeout"),
        outer_transaction=True,
        statement_timeout="9s",
    )

    rows = await inquiry_mod._fetch_bounded_lookup_rows(
        conn,  # type: ignore[arg-type]
        "SELECT pg_sleep(10)",
        label="outer_timeout",
    )

    assert rows == []
    assert conn.executed == []
    assert conn.statement_timeout == "9s"
    assert conn.fetchval_calls == [
        ("SELECT current_setting('statement_timeout')", ()),
        ("SELECT set_config('statement_timeout', $1, true)", ("1500",)),
        ("SELECT set_config('statement_timeout', $1, true)", ("9s",)),
    ]
    assert conn.transaction_enter_count == 1
    assert conn.transaction_exit_count == 1
