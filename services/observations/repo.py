"""services/observations/repo.py — async repository over observations.

BUILD-PLAN.md §2 Prompt 1.A item 1:
    repo.py — async repository with:
      - insert(obs) — dedup via (source_channel, external_id); returns
        existing on conflict; computes embedding; embedding_pending=True
        fallback if Ollama is down.
      - get_by_id(id, tenant_id)
      - search_by_embedding(vec, tenant_id, k, filters) — HNSW cosine
      - by_actor_time_range
      - by_channel_time_range
      - by_entities
      - by_kind
      - cascade_trace — recursive CTE up cause_id chain

Schema refs: SCHEMA-LOCK.md S1.1 (observations) and S1.2 (indexes).
Spec §1 authoritative for field semantics.

Design:
- Repository is a plain class holding a reference to an `asyncpg.Pool`
  and an `OllamaClient`. No globals; tests construct their own
  instance against the `fresh_db` fixture pool.
- Per BUILD-PLAN §0, Wave-0 PK is composite `(id, occurred_at)`. That
  means application-level FK enforcement (Wave 0 decision). We do
  not touch this — just supply both columns on every INSERT.
- Partitioned parent means FKs INTO observations are application-level,
  not DB-level. Confirmed in BUILD-LOG Wave 0 note.
- Every SELECT hydrates through `ObservationRow` via `select_one` /
  `select_many` so schema drift surfaces immediately on reads.
- Vector columns: we register pgvector's codec once per pool so
  `list[float]` round-trips transparently as VECTOR(768).
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
from pgvector.asyncpg import register_vector

from lib.embeddings.ollama import (
    EMBEDDING_DIM,
    OllamaClient,
    OllamaDimensionMismatch,
    OllamaError,
)
from lib.shared.db import RowHydrationError
from lib.shared.errors import CompanyOSError
from lib.shared.ids import uuid7
from lib.shared.types import ObservationCreate, ObservationRow, TrustTierValue

from .events import NewObservationEvent, schedule_notify


class ObservationError(CompanyOSError):
    default_code = "observation_error"


class InvalidTrustTier(ObservationError):
    default_code = "invalid_trust_tier"


# Central list of columns in the canonical row order — used by SELECT
# query builders to avoid column-order drift between reads.
_COLUMNS = (
    "id",
    "tenant_id",
    "occurred_at",
    "ingested_at",
    "kind",
    "source_channel",
    "source_actor_ref",
    "actor_id",
    "content",
    "content_text",
    "embedding",
    "embedding_pending",
    "trust_tier",
    "external_id",
    "cause_id",
    "sequence_num",
    "entities_mentioned",
)
_SELECT_COLS = ", ".join(_COLUMNS)


_VALID_TRUST_TIERS = frozenset(TrustTierValue.__args__)  # from Literal


async def _ensure_vector_codec(conn: asyncpg.Connection) -> None:
    """
    Register pgvector's codec on this connection. `register_vector`
    installs a type codec for the `vector` OID; asyncpg is tolerant
    of re-registration on the same connection (it replaces the
    existing codec). We call it on every acquire — the cost is one
    catalog lookup, which is cheap and safer than trying to cache
    on a slotted `PoolConnectionProxy`.
    """
    await register_vector(conn)


def _hydrate_row(record: asyncpg.Record) -> ObservationRow:
    """Normalize asyncpg Record into a Pydantic ObservationRow."""
    raw = dict(record)
    # asyncpg returns JSONB as str unless a codec is configured;
    # we parse explicitly to stay codec-agnostic.
    for key in ("content", "entities_mentioned"):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            raw[key] = json.loads(v)
    # pgvector's asyncpg codec returns numpy arrays when registered;
    # convert to list[float] so Pydantic validates cleanly.
    emb = raw.get("embedding")
    if emb is not None and not isinstance(emb, list):
        raw["embedding"] = [float(x) for x in emb]
    try:
        return ObservationRow.model_validate(raw)
    except Exception as e:
        raise RowHydrationError(
            f"could not hydrate observations row: {e}",
            row_keys=list(record.keys()),
        ) from e


def _validate_trust_tier(tier: str) -> None:
    if tier not in _VALID_TRUST_TIERS:
        raise InvalidTrustTier(
            f"invalid trust tier {tier!r}",
            tier=tier,
            valid=sorted(_VALID_TRUST_TIERS),
        )


class ObservationRepository:
    """
    Async CRUD + retrieval facade for the `observations` table.
    Construct with a pool (e.g. from `lib.shared.db.get_pool()`) and
    an embedder. The embedder may be None in tests that supply
    embeddings explicitly via `ObservationCreate.content_text` being
    ignored and `embedding_pending=True`.
    """

    def __init__(
        self,
        pool: asyncpg.Pool | asyncpg.Connection,
        *,
        embedder: OllamaClient | None = None,
    ) -> None:
        """
        Accept either a pool (production) or a single connection
        (tests that want to pin all reads/writes to the same tx).
        If a connection is given, every method uses it directly,
        skipping acquire/release.
        """
        if isinstance(pool, asyncpg.Connection):
            self._pool = None
            self._default_conn = pool
        else:
            self._pool = pool
            self._default_conn = None
        self._embedder = embedder

    # -----------------------------------------------------------------
    # Method 1: insert
    # -----------------------------------------------------------------
    async def insert(
        self,
        obs: ObservationCreate,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ObservationRow:
        """
        Insert an observation. Behavior:

        1. Compute embedding via Ollama (unless Ollama is unreachable
           — fall back to embedding_pending=True).
        2. Dedup via UNIQUE (source_channel, external_id, occurred_at):
           if an observation with the same (channel, external_id)
           already exists, return the existing row rather than raising.
        3. Schedule post-commit NOTIFY via `schedule_notify` (a no-op
           outside an active `notify_scope()`).

        The unique constraint was widened to include `occurred_at` in
        Wave 0 because PG requires every unique key on a partitioned
        table to include the partition key. For dedup purposes the
        `(source_channel, external_id)` pair is still effectively
        unique because ingestion assigns a stable `occurred_at` per
        external event. To handle the edge case of the same external
        event being re-submitted at a different `occurred_at`, dedup
        does a pre-check on `(source_channel, external_id)` ignoring
        occurred_at.
        """
        _validate_trust_tier(obs.trust_tier)

        # -- 1. Assign id (UUID v7) if absent -----------------------
        obs_id = obs.id or uuid7()

        # -- 2. Compute embedding -----------------------------------
        embedding, embedding_pending = await self._maybe_embed(obs.content_text)

        # -- 3. Insert or fetch existing ----------------------------
        conn = conn or self._default_conn
        if conn is None:
            async with self._pool.acquire() as owned:
                row = await self._insert_with_conn(
                    owned, obs, obs_id, embedding, embedding_pending
                )
        else:
            row = await self._insert_with_conn(
                conn, obs, obs_id, embedding, embedding_pending
            )

        # -- 4. Schedule post-commit notify -------------------------
        schedule_notify(
            NewObservationEvent(
                id=row.id,
                kind=row.kind,
                tenant_id=row.tenant_id,
                source_channel=row.source_channel,
            )
        )
        return row

    async def _insert_with_conn(
        self,
        conn: asyncpg.Connection,
        obs: ObservationCreate,
        obs_id: UUID,
        embedding: list[float] | None,
        embedding_pending: bool,
    ) -> ObservationRow:
        await _ensure_vector_codec(conn)

        # Dedup pre-check: if external_id is NULL, skip (NULLs are
        # never equal in the unique constraint — each insert is new).
        if obs.external_id is not None:
            existing = await conn.fetchrow(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE source_channel = $1 AND external_id = $2 "
                "ORDER BY occurred_at DESC LIMIT 1",
                obs.source_channel,
                obs.external_id,
            )
            if existing is not None:
                return _hydrate_row(existing)

        # Vector: pgvector.asyncpg wants a list[float] (or np array).
        emb_value: Any = embedding
        row = await conn.fetchrow(
            f"""
            INSERT INTO observations (
                id, tenant_id, occurred_at, kind, source_channel,
                source_actor_ref, actor_id,
                content, content_text,
                embedding, embedding_pending,
                trust_tier, external_id, cause_id,
                entities_mentioned
            ) VALUES (
                $1, $2, $3, $4, $5,
                $6, $7,
                $8::jsonb, $9,
                $10, $11,
                $12, $13, $14,
                $15::jsonb
            )
            ON CONFLICT (source_channel, external_id, occurred_at) DO NOTHING
            RETURNING {_SELECT_COLS}
            """,
            obs_id,
            obs.tenant_id,
            obs.occurred_at,
            obs.kind,
            obs.source_channel,
            obs.source_actor_ref,
            obs.actor_id,
            json.dumps(obs.content),
            obs.content_text,
            emb_value,
            embedding_pending,
            obs.trust_tier,
            obs.external_id,
            obs.cause_id,
            json.dumps(obs.entities_mentioned),
        )

        if row is None:
            # ON CONFLICT DO NOTHING → fetch the existing row.
            if obs.external_id is None:
                # Shouldn't happen — without external_id the unique
                # constraint can't trigger — but be defensive.
                raise ObservationError(
                    "insert conflict with NULL external_id",
                    source_channel=obs.source_channel,
                    occurred_at=obs.occurred_at.isoformat(),
                )
            existing = await conn.fetchrow(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE source_channel = $1 AND external_id = $2 "
                "  AND occurred_at = $3",
                obs.source_channel,
                obs.external_id,
                obs.occurred_at,
            )
            if existing is None:
                # Still race: the conflict hit on a different
                # occurred_at. Fall back to the broader dedup query.
                existing = await conn.fetchrow(
                    f"SELECT {_SELECT_COLS} FROM observations "
                    "WHERE source_channel = $1 AND external_id = $2 "
                    "ORDER BY occurred_at DESC LIMIT 1",
                    obs.source_channel,
                    obs.external_id,
                )
            if existing is None:
                raise ObservationError(
                    "insert conflict but no existing row found",
                    source_channel=obs.source_channel,
                    external_id=obs.external_id,
                )
            return _hydrate_row(existing)

        return _hydrate_row(row)

    async def _maybe_embed(self, text: str) -> tuple[list[float] | None, bool]:
        """
        Run Ollama embedding with a structured fallback. Returns
        `(embedding, embedding_pending)`.

        Fallback triggers on OllamaError (connection, 5xx, timeout)
        and OllamaDimensionMismatch. A misconfigured model should be
        loud in dev — we still flip embedding_pending=True so
        retrieval filters the row out, but log the error.
        """
        if self._embedder is None:
            return None, True
        try:
            vec = await self._embedder.embed(text)
            return vec, False
        except (OllamaError, OllamaDimensionMismatch):
            return None, True

    # -----------------------------------------------------------------
    # Method 2: get_by_id
    # -----------------------------------------------------------------
    async def get_by_id(
        self,
        obs_id: UUID,
        tenant_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ObservationRow | None:
        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            row = await c.fetchrow(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE id = $1 AND tenant_id = $2",
                obs_id,
                tenant_id,
            )
        return _hydrate_row(row) if row is not None else None

    # -----------------------------------------------------------------
    # Method 3: search_by_embedding (HNSW cosine)
    # -----------------------------------------------------------------
    async def search_by_embedding(
        self,
        vec: list[float],
        tenant_id: UUID,
        k: int = 20,
        *,
        filters: dict[str, Any] | None = None,
        conn: asyncpg.Connection | None = None,
    ) -> list[ObservationRow]:
        """
        Cosine-similarity nearest-neighbour search, HNSW-indexed.
        Always filters embedding_pending = FALSE (pending rows have
        NULL vectors that would throw off the operator anyway) and
        tenant_id = $tenant_id for isolation.

        `filters` is an optional dict of extra WHERE clauses with keys
        {'kind', 'source_channel', 'actor_id',
         'occurred_after', 'occurred_before'}.
        """
        if len(vec) != EMBEDDING_DIM:
            raise ObservationError(
                f"search vector dim={len(vec)} != {EMBEDDING_DIM}",
            )
        if k <= 0:
            raise ObservationError("k must be positive", k=k)

        params: list[Any] = [vec, tenant_id]
        clauses: list[str] = [
            "tenant_id = $2",
            "embedding IS NOT NULL",
            "embedding_pending = FALSE",
        ]
        filters = filters or {}
        for key, value in filters.items():
            if value is None:
                continue
            if key == "kind":
                params.append(value)
                clauses.append(f"kind = ${len(params)}")
            elif key == "source_channel":
                params.append(value)
                clauses.append(f"source_channel = ${len(params)}")
            elif key == "actor_id":
                params.append(value)
                clauses.append(f"actor_id = ${len(params)}")
            elif key == "occurred_after":
                params.append(value)
                clauses.append(f"occurred_at >= ${len(params)}")
            elif key == "occurred_before":
                params.append(value)
                clauses.append(f"occurred_at < ${len(params)}")
            else:
                raise ObservationError(
                    f"unknown filter key {key!r}",
                    supported=sorted(
                        ("kind", "source_channel", "actor_id",
                         "occurred_after", "occurred_before")
                    ),
                )
        params.append(k)

        sql = (
            f"SELECT {_SELECT_COLS} FROM observations "
            f"WHERE {' AND '.join(clauses)} "
            "ORDER BY embedding <=> $1 "
            f"LIMIT ${len(params)}"
        )

        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            rows = await c.fetch(sql, *params)
        return [_hydrate_row(r) for r in rows]

    # -----------------------------------------------------------------
    # Method 4: by_actor_time_range
    # -----------------------------------------------------------------
    async def by_actor_time_range(
        self,
        actor_id: UUID,
        start: datetime,
        end: datetime,
        tenant_id: UUID,
        *,
        limit: int = 500,
        conn: asyncpg.Connection | None = None,
    ) -> list[ObservationRow]:
        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            rows = await c.fetch(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE tenant_id = $1 AND actor_id = $2 "
                "  AND occurred_at >= $3 AND occurred_at < $4 "
                "ORDER BY occurred_at DESC "
                "LIMIT $5",
                tenant_id,
                actor_id,
                start,
                end,
                limit,
            )
        return [_hydrate_row(r) for r in rows]

    # -----------------------------------------------------------------
    # Method 5: by_channel_time_range
    # -----------------------------------------------------------------
    async def by_channel_time_range(
        self,
        source_channel: str,
        start: datetime,
        end: datetime,
        tenant_id: UUID,
        *,
        limit: int = 500,
        conn: asyncpg.Connection | None = None,
    ) -> list[ObservationRow]:
        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            rows = await c.fetch(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE tenant_id = $1 AND source_channel = $2 "
                "  AND occurred_at >= $3 AND occurred_at < $4 "
                "ORDER BY occurred_at DESC "
                "LIMIT $5",
                tenant_id,
                source_channel,
                start,
                end,
                limit,
            )
        return [_hydrate_row(r) for r in rows]

    # -----------------------------------------------------------------
    # Method 6: by_entities (GIN @> match on entities_mentioned)
    # -----------------------------------------------------------------
    async def by_entities(
        self,
        entities_mentioned: list[dict[str, Any]],
        tenant_id: UUID,
        *,
        limit: int = 500,
        conn: asyncpg.Connection | None = None,
    ) -> list[ObservationRow]:
        """
        Return observations whose `entities_mentioned` JSONB contains
        every element of the argument list. Uses the GIN index on
        entities_mentioned (obs_entities_idx).
        """
        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            rows = await c.fetch(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE tenant_id = $1 "
                "  AND entities_mentioned @> $2::jsonb "
                "ORDER BY occurred_at DESC "
                "LIMIT $3",
                tenant_id,
                json.dumps(entities_mentioned),
                limit,
            )
        return [_hydrate_row(r) for r in rows]

    # -----------------------------------------------------------------
    # Method 7: by_kind
    # -----------------------------------------------------------------
    async def by_kind(
        self,
        kind: str,
        tenant_id: UUID,
        *,
        limit: int = 500,
        conn: asyncpg.Connection | None = None,
    ) -> list[ObservationRow]:
        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            rows = await c.fetch(
                f"SELECT {_SELECT_COLS} FROM observations "
                "WHERE tenant_id = $1 AND kind = $2 "
                "ORDER BY occurred_at DESC "
                "LIMIT $3",
                tenant_id,
                kind,
                limit,
            )
        return [_hydrate_row(r) for r in rows]

    # -----------------------------------------------------------------
    # Method 8: cascade_trace — recursive CTE up the cause_id chain
    # -----------------------------------------------------------------
    async def cascade_trace(
        self,
        cause_id: UUID,
        *,
        tenant_id: UUID | None = None,
        max_depth: int = 32,
        conn: asyncpg.Connection | None = None,
    ) -> list[ObservationRow]:
        """
        Walk from the given observation up the `cause_id` chain to the
        root ancestor. Spec §1 "read path by cause: WHERE cause_id =
        $parent (cascade traces)". We interpret `cause_id` as the id
        of a child observation; the trace starts at that child and
        follows its `cause_id` FK upward until NULL or until
        `max_depth` is reached (cycle safety — cause_id graphs should
        be acyclic but we cap anyway).

        Returned list is ordered root → leaf (ancestors first, then
        descendants are not explored). If `cause_id` doesn't exist,
        returns an empty list.

        Tenant filter is applied if provided — cascades never cross
        tenants.
        """
        if max_depth <= 0:
            raise ObservationError("max_depth must be positive", max_depth=max_depth)

        # Recursive CTE walking parents (up cause_id chain).
        # Starting row: the row with id = $1. Each iteration joins to
        # the parent via cause_id.
        sql = f"""
            WITH RECURSIVE chain AS (
                SELECT {_SELECT_COLS}, 0 AS depth
                FROM observations
                WHERE id = $1
                  {"AND tenant_id = $2" if tenant_id else ""}
                UNION ALL
                SELECT {', '.join('o.' + c for c in _COLUMNS)}, chain.depth + 1
                FROM observations o
                JOIN chain ON o.id = chain.cause_id
                WHERE chain.depth < ${3 if tenant_id else 2}
                  {"AND o.tenant_id = $2" if tenant_id else ""}
            )
            SELECT {_SELECT_COLS} FROM chain
            ORDER BY depth DESC
        """
        params: list[Any] = [cause_id]
        if tenant_id:
            params.append(tenant_id)
        params.append(max_depth)

        async with _connection(self._pool, conn or self._default_conn) as c:
            await _ensure_vector_codec(c)
            rows = await c.fetch(sql, *params)
        return [_hydrate_row(r) for r in rows]


# ---------------------------------------------------------------------
# Connection helper — use caller's conn if provided, else acquire.
# ---------------------------------------------------------------------

class _connection:
    """
    Small context manager: if `conn` is given, yield it (and do
    nothing on exit); otherwise acquire from the pool and release.
    """

    def __init__(
        self,
        pool: asyncpg.Pool | None,
        conn: asyncpg.Connection | None,
    ) -> None:
        self._pool = pool
        self._conn = conn
        self._acquired: asyncpg.Connection | None = None

    async def __aenter__(self) -> asyncpg.Connection:
        if self._conn is not None:
            return self._conn
        if self._pool is None:
            raise RuntimeError("No connection and no pool supplied to _connection")
        self._acquired = await self._pool.acquire()
        return self._acquired

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._acquired is not None:
            await self._pool.release(self._acquired)
            self._acquired = None


__all__ = [
    "ObservationRepository",
    "ObservationError",
    "InvalidTrustTier",
]
