"""services/gateway/db_bootstrap.py — pool bootstrap with JSONB codec.

Wave 1-D flagged that `lib.shared.db.init_pool` does not install a JSONB
codec on new connections; without it, asyncpg returns `jsonb` columns
as `str`, which causes Pydantic hydration to fail downstream (dict
expected). Every production pool used by the Gateway / Ingestion must
install the codec at acquire time.

Two approaches were considered:

  (a) Extend `lib.shared.db.init_pool` with an `init` callback.
  (b) Provide a Gateway-local bootstrap that constructs an asyncpg.Pool
      directly with the `init=` hook.

(b) was chosen because changing the shared library touches other wave-
owned surfaces (risky during parallel Wave 2 work). Tests in this
service also call `_register_codecs` directly on connections they own
(e.g. per-test-transaction pattern), so the codec is installed in both
pool-acquired and test-owned connection paths.

Schema refs: S1.1 `observations.content` / `entities_mentioned` JSONB,
S5.1 `actors.metadata` JSONB, S6.1 `entity_aliases.resolved_entity_ref`.
"""
from __future__ import annotations

import json
import os
from typing import Any

import asyncpg

from lib.shared import db as _db_module


async def _register_codecs(conn: asyncpg.Connection) -> None:
    """Install JSONB + JSON codecs so asyncpg returns dicts, not strings.

    Safe to call on a connection that already has the codec — asyncpg
    replaces in place. Callers that own their own connection (tests
    using the per-test-transaction pattern) should invoke this manually.
    """
    await conn.set_type_codec(
        "jsonb",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "json",
        encoder=lambda v: json.dumps(v) if not isinstance(v, str) else v,
        decoder=json.loads,
        schema="pg_catalog",
    )


async def create_gateway_pool(
    dsn: str | None = None,
    *,
    min_size: int = 1,
    max_size: int = 10,
    command_timeout: float = 30.0,
) -> asyncpg.Pool:
    """Create an asyncpg pool with JSONB codec installed on every acquire.

    Stores the created pool in `lib.shared.db._pool` so downstream
    consumers of `get_pool()` (e.g. the Wave 1 repos that call
    `lib.shared.db.get_pool` from module scope) pick it up.
    """
    dsn = dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError(
            "DATABASE_URL not set — cannot create gateway pool",
        )
    pool = await asyncpg.create_pool(
        dsn,
        min_size=min_size,
        max_size=max_size,
        command_timeout=command_timeout,
        init=_register_codecs,
    )
    _db_module._pool = pool
    return pool


async def close_gateway_pool(pool: asyncpg.Pool | None = None) -> None:
    """Close the gateway pool and clear the shared `_pool` slot.

    If `pool` is None, closes whatever is currently in
    `lib.shared.db._pool`.
    """
    target = pool or _db_module._pool
    if target is None:
        return
    try:
        await target.close()
    finally:
        if _db_module._pool is target:
            _db_module._pool = None


__all__ = [
    "_register_codecs",
    "create_gateway_pool",
    "close_gateway_pool",
]
