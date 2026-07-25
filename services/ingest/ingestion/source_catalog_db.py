"""Database parity gate for the compiled source contract.

Executable behavior lives in Python; persisted source membership lives in
``ingestion_source_catalog`` so foreign keys can protect wire-compatible IDs.
Every database-owning ingestion process validates both views before serving.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS


class SourceCatalogDatabaseMismatch(RuntimeError):
    """The migrated database and compiled source catalog disagree."""


def compiled_membership() -> tuple[dict[str, object], ...]:
    return tuple(
        {
            "id": source.source_id,
            "historical_supported": source.history is not None,
            "data_plane": True,
        }
        for source in sorted(SOURCE_DEFINITIONS, key=lambda item: item.source_id)
    )


def membership_hash(rows: Sequence[Mapping[str, Any]]) -> str:
    normalized = [
        {
            "id": str(row["id"]),
            "historical_supported": bool(row["historical_supported"]),
            "data_plane": bool(row["data_plane"]),
        }
        for row in rows
    ]
    normalized.sort(key=lambda item: item["id"])
    payload = json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


COMPILED_SOURCE_CATALOG_HASH = membership_hash(compiled_membership())

_LOAD_MEMBERSHIP_SQL = """
SELECT id, historical_supported, data_plane
  FROM ingestion_source_catalog
 ORDER BY id
"""


async def validate_database_catalog(executor: Any) -> str:
    """Return the common hash or fail startup with an actionable diff."""

    try:
        rows = await executor.fetch(_LOAD_MEMBERSHIP_SQL)
    except Exception as exc:  # noqa: BLE001 - surface missing migration cleanly
        raise SourceCatalogDatabaseMismatch(
            "could not load ingestion_source_catalog; apply migration 0193 "
            "before starting ingestion workers"
        ) from exc

    actual_rows = tuple(dict(row) for row in rows)
    actual_hash = membership_hash(actual_rows)
    if actual_hash == COMPILED_SOURCE_CATALOG_HASH:
        return actual_hash

    expected = {str(row["id"]): row for row in compiled_membership()}
    actual = {str(row["id"]): row for row in actual_rows}
    missing = sorted(set(expected) - set(actual))
    extra = sorted(set(actual) - set(expected))
    changed = sorted(
        source_id
        for source_id in set(expected) & set(actual)
        if {
            "historical_supported": bool(expected[source_id]["historical_supported"]),
            "data_plane": bool(expected[source_id]["data_plane"]),
        }
        != {
            "historical_supported": bool(actual[source_id]["historical_supported"]),
            "data_plane": bool(actual[source_id]["data_plane"]),
        }
    )
    raise SourceCatalogDatabaseMismatch(
        "compiled/database source catalog mismatch "
        f"(compiled_hash={COMPILED_SOURCE_CATALOG_HASH}, "
        f"database_hash={actual_hash}, missing={missing}, extra={extra}, "
        f"changed={changed})"
    )


__all__ = [
    "COMPILED_SOURCE_CATALOG_HASH",
    "SourceCatalogDatabaseMismatch",
    "compiled_membership",
    "membership_hash",
    "validate_database_catalog",
]
