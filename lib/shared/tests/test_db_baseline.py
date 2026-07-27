from __future__ import annotations

from typing import Any

import pytest

from lib.shared.testing.db_baseline import (
    _test_source_catalog_rows,
    seed_test_source_catalog,
)


class _RecordingConnection:
    def __init__(self) -> None:
        self.query = ""
        self.arguments: list[tuple[Any, ...]] = []

    async def executemany(
        self,
        query: str,
        arguments: list[tuple[Any, ...]],
    ) -> None:
        self.query = query
        self.arguments = arguments


@pytest.mark.asyncio
async def test_seed_test_source_catalog_restores_all_contract_rows() -> None:
    connection = _RecordingConnection()

    await seed_test_source_catalog(connection)  # type: ignore[arg-type]

    assert "ON CONFLICT (id) DO UPDATE" in connection.query
    assert "VALUES ($1, $2, $3::text[], $4, $5)" in connection.query
    assert len(connection.arguments) == 27
    assert connection.arguments == _test_source_catalog_rows()
    assert len({row[0] for row in connection.arguments}) == 27
    whatsapp = next(row for row in connection.arguments if row[0] == "whatsapp")
    assert whatsapp[3] is False
