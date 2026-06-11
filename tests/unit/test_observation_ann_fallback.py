from __future__ import annotations

import pytest

from services.domain.observations.repo import _exact_embedding_fallback


class _Tx:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeConn:
    def __init__(self, rows):
        self.rows = rows
        self.executed: list[str] = []
        self.fetches: list[tuple[str, tuple[object, ...]]] = []

    def transaction(self):
        return _Tx()

    async def execute(self, sql: str) -> None:
        self.executed.append(sql)

    async def fetch(self, sql: str, *params):
        self.fetches.append((sql, params))
        return self.rows


@pytest.mark.asyncio
async def test_exact_embedding_fallback_retries_with_index_scans_disabled():
    exact_rows = [{"id": "a"}, {"id": "b"}]
    conn = _FakeConn(exact_rows)

    rows = await _exact_embedding_fallback(
        conn,  # type: ignore[arg-type]
        "SELECT * FROM observations ORDER BY embedding <=> $1 LIMIT $2",
        [[0.1], 2],
        [{"id": "a"}],  # type: ignore[list-item]
    )

    assert rows == exact_rows
    assert conn.executed == [
        "SET LOCAL enable_indexscan = off",
        "SET LOCAL enable_bitmapscan = off",
    ]
    assert conn.fetches == [
        (
            "SELECT * FROM observations ORDER BY embedding <=> $1 LIMIT $2",
            ([0.1], 2),
        )
    ]


@pytest.mark.asyncio
async def test_exact_embedding_fallback_keeps_indexed_rows_when_not_improved():
    indexed_rows = [{"id": "a"}, {"id": "b"}]
    conn = _FakeConn([{"id": "a"}])

    rows = await _exact_embedding_fallback(
        conn,  # type: ignore[arg-type]
        "SELECT * FROM observations ORDER BY embedding <=> $1 LIMIT $2",
        [[0.1], 2],
        indexed_rows,  # type: ignore[arg-type]
    )

    assert rows == indexed_rows
