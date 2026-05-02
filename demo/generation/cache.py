"""Prompt-hash-based file cache for demo-generation LLM calls.

Determinism: the same `(system, user, model, schema_name)` hash returns
the same cached JSON. Iterate on one company's spec without
regenerating the others by keeping the cache directory between runs.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable

CACHE_DIR = Path(__file__).resolve().parent / ".cache"


def _hash_key(
    *,
    system: str,
    user: str,
    model: str,
    schema_name: str,
) -> str:
    h = hashlib.sha256()
    for part in (system, user, model, schema_name):
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    return h.hexdigest()


@dataclass
class FileCache:
    """File-backed cache of LLM responses keyed by prompt+model+schema."""

    root: Path = CACHE_DIR

    def __post_init__(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)

    def path_for(self, key: str) -> Path:
        return self.root / f"{key}.json"

    def get(
        self,
        *,
        system: str,
        user: str,
        model: str,
        schema_name: str,
    ) -> dict[str, Any] | None:
        p = self.path_for(_hash_key(
            system=system, user=user, model=model, schema_name=schema_name,
        ))
        if not p.exists():
            return None
        return json.loads(p.read_text(encoding="utf-8"))

    def put(
        self,
        *,
        system: str,
        user: str,
        model: str,
        schema_name: str,
        value: dict[str, Any],
    ) -> None:
        p = self.path_for(_hash_key(
            system=system, user=user, model=model, schema_name=schema_name,
        ))
        p.write_text(json.dumps(value, indent=2, sort_keys=True), encoding="utf-8")

    async def get_or_fetch(
        self,
        *,
        system: str,
        user: str,
        model: str,
        schema_name: str,
        fetch_fn: Callable[[], Awaitable[dict[str, Any]]],
    ) -> dict[str, Any]:
        hit = self.get(
            system=system, user=user, model=model, schema_name=schema_name,
        )
        if hit is not None:
            return hit
        value = await fetch_fn()
        self.put(
            system=system, user=user, model=model,
            schema_name=schema_name, value=value,
        )
        return value


__all__ = ["FileCache", "CACHE_DIR"]
