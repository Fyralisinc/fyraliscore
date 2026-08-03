"""Generated low-level source identity index consumed by data-plane DTOs."""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

SOURCE_INDEX_SCHEMA = "sources.fyralis.io/source-index/v1"
_SOURCE_INDEX_PATH = Path(__file__).with_name("source-index.json")


@lru_cache(maxsize=1)
def source_ids() -> tuple[str, ...]:
    payload = json.loads(_SOURCE_INDEX_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("schema") != SOURCE_INDEX_SCHEMA:
        raise ValueError("source identity index has an unsupported schema")
    raw = payload.get("sources")
    if (
        not isinstance(raw, list)
        or any(not isinstance(item, str) or not item for item in raw)
        or len(raw) != len(set(raw))
    ):
        raise ValueError("source identity index contains invalid source IDs")
    return tuple(raw)


def require_source_id(value: str) -> str:
    if value not in source_ids():
        raise ValueError(f"unknown ingestion source: {value!r}")
    return value


__all__ = ["SOURCE_INDEX_SCHEMA", "require_source_id", "source_ids"]
