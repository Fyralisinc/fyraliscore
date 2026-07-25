from __future__ import annotations

from services.ingest.ingestion.source_catalog_db import (
    COMPILED_SOURCE_CATALOG_HASH,
    SourceCatalogDatabaseMismatch,
    compiled_membership,
    membership_hash,
    validate_database_catalog,
)


class _Executor:
    def __init__(self, rows):  # noqa: ANN001
        self.rows = rows

    async def fetch(self, _sql: str):  # noqa: ANN201
        return self.rows


def test_compiled_catalog_hash_covers_all_27() -> None:
    rows = compiled_membership()
    assert len(rows) == 27
    assert membership_hash(rows) == COMPILED_SOURCE_CATALOG_HASH
    whatsapp = next(row for row in rows if row["id"] == "whatsapp")
    assert whatsapp["historical_supported"] is False


async def test_database_membership_parity_returns_common_hash() -> None:
    actual = await validate_database_catalog(_Executor(compiled_membership()))
    assert actual == COMPILED_SOURCE_CATALOG_HASH


async def test_database_membership_mismatch_reports_exact_diff() -> None:
    rows = list(compiled_membership())
    rows = [row for row in rows if row["id"] != "slack"]
    rows.append(
        {
            "id": "unknown",
            "historical_supported": True,
            "data_plane": True,
        }
    )
    try:
        await validate_database_catalog(_Executor(rows))
    except SourceCatalogDatabaseMismatch as exc:
        message = str(exc)
    else:
        raise AssertionError("expected catalog mismatch")
    assert "missing=['slack']" in message
    assert "extra=['unknown']" in message


async def test_database_membership_mismatch_reports_semantic_change() -> None:
    rows = [
        {
            **row,
            "historical_supported": True,
        }
        if row["id"] == "whatsapp"
        else row
        for row in compiled_membership()
    ]
    try:
        await validate_database_catalog(_Executor(rows))
    except SourceCatalogDatabaseMismatch as exc:
        assert "changed=['whatsapp']" in str(exc)
    else:
        raise AssertionError("expected catalog mismatch")
