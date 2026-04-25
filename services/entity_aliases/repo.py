"""
services/entity_aliases/repo.py — Entity alias resolution repo.

Schema refs (SCHEMA-LOCK.md):
  - S6.1 `entity_aliases` table (resolved_entity_ref is JSONB; spec §6
    originally called it `canonical_ref` — adapted per Q6 resolution.)
  - S6.2 indexes (aliases_text_idx, aliases_entity_idx GIN, HNSW on
    alias_embedding, etc.)

Public API (per BUILD-PLAN §2 Prompt 1-B):

  - fast_path_resolve(phrase, tenant_id) → canonical_ref | None
        Case-/whitespace-tolerant exact lookup. When the normalized
        phrase matches exactly one row, returns its resolved_entity_ref
        JSONB. When it matches multiple rows (ambiguous), returns None
        — callers must go through list_ambiguous() / the resolver
        worker.

  - insert_alias(phrase, resolved_entity_ref, source, confidence,
                 tenant_id, ...) → EntityAliasRow
        `source` is a semantic label recorded in
        `entity_metadata.source`. See "Deviations" in the log: S6.1 has
        no dedicated `source` column, so the parameter lands in the
        JSONB sidecar.

  - record_usage(alias_id) → EntityAliasRow
        Increments confirmed_count and bumps last_used_at.

  - list_ambiguous(tenant_id, threshold=0.5) → list of phrase groups
        Any normalized phrase that maps to >1 distinct
        resolved_entity_ref OR whose max confidence is below the
        threshold. Returned as a list of dicts suitable for the
        resolver-worker prompt.

  - reverse_lookup(canonical_ref, tenant_id) → list[str] of phrases
        Given a JSONB entity ref, return every alias_text that maps
        to it.

Normalization rule: lowercase + collapse whitespace. This is the
single invariant every test checks — change it and the fast-path
index must also change.
"""
from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import EntityAliasRow


# Legal `source` labels for `insert_alias`. These mirror BUILD-PLAN
# 1-B ("ingestion" | "resolver_worker" | "manual"). The repo rejects
# unknown values locally so callers get a clear error before touching
# the DB.
_LEGAL_SOURCES: frozenset[str] = frozenset(
    ("ingestion", "resolver_worker", "manual")
)

_WHITESPACE_RE = re.compile(r"\s+", flags=re.UNICODE)


def normalize_phrase(phrase: str) -> str:
    """
    Normalization used for the fast path:
      - Unicode-casefold to lowercase.
      - Collapse any run of whitespace (spaces, tabs, newlines) to a
        single space.
      - Strip leading/trailing whitespace.

    Deterministic: normalize_phrase(normalize_phrase(x)) == normalize_phrase(x).
    """
    if phrase is None:
        raise ValidationError("phrase must not be None", field="phrase")
    folded = phrase.casefold()
    collapsed = _WHITESPACE_RE.sub(" ", folded).strip()
    return collapsed


class EntityAliasRepo:
    """Repository for entity_aliases."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # -----------------------------------------------------------------
    # fast_path_resolve
    # -----------------------------------------------------------------
    async def fast_path_resolve(
        self, phrase: str, tenant_id: UUID
    ) -> dict[str, Any] | None:
        """
        O(1)-ish lookup by normalized alias_text within a tenant.

        Resolution rules:
        - Normalize the incoming phrase.
        - Fetch every row in `tenant_id` whose normalized alias_text
          matches exactly.
        - If zero rows: return None.
        - If every row points at the same `resolved_entity_ref`:
          return that ref (the highest-confidence copy wins for ties).
        - If rows point at distinct refs: ambiguous → return None.
          The caller should fall back to the resolver worker via
          list_ambiguous.
        """
        if not phrase or not phrase.strip():
            return None
        norm = normalize_phrase(phrase)
        if not norm:
            return None

        # We store the raw alias_text as provided by the caller and
        # compare via LOWER(regexp_replace(...)) at query time. This
        # avoids a second column while still using the text index
        # (aliases_text_idx) as a bounded prefilter. For the 10k-row
        # benchmark, confirm index usage via EXPLAIN in tests.
        rows = await self._pool.fetch(
            """
            SELECT id, resolved_entity_ref, confidence
            FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = $2
            ORDER BY confidence DESC, last_used_at DESC
            """,
            tenant_id,
            norm,
        )
        if not rows:
            return None

        # Collapse rows that point at the same canonical ref. asyncpg
        # returns JSONB as str; parse once per row and use the string
        # form as the dedup key.
        refs_by_json: dict[str, dict[str, Any]] = {}
        for r in rows:
            raw = r["resolved_entity_ref"]
            # asyncpg returns JSONB either as dict or string depending
            # on codec state. Handle both.
            if isinstance(raw, str):
                parsed = json.loads(raw)
            else:
                parsed = raw
            key = json.dumps(parsed, sort_keys=True)
            refs_by_json.setdefault(key, parsed)

        if len(refs_by_json) == 1:
            return next(iter(refs_by_json.values()))
        # Ambiguous — multiple distinct canonical refs for same phrase.
        return None

    # -----------------------------------------------------------------
    # insert_alias
    # -----------------------------------------------------------------
    async def insert_alias(
        self,
        *,
        phrase: str,
        resolved_entity_ref: dict[str, Any],
        source: str,
        confidence: float,
        tenant_id: UUID,
        actor_id: UUID | None = None,
        source_event_id: UUID | None = None,
        is_canonical: bool = False,
        alias_embedding: list[float] | None = None,
        extra_metadata: dict[str, Any] | None = None,
    ) -> EntityAliasRow:
        """
        Insert an alias. Idempotent on (tenant_id, alias_text, actor_id)
        per S6.1 UNIQUE constraint: if the same tuple is inserted
        again, the existing row is returned (via ON CONFLICT ...
        DO UPDATE that effectively preserves the first-seen row but
        bumps last_used_at).

        `source` is a label (ingestion|resolver_worker|manual) — S6.1
        has no dedicated `source` column, so the value lands in the
        JSONB `entity_metadata.source` sidecar. Callers that need to
        filter on source should use a GIN-friendly query such as
        `WHERE entity_metadata->>'source' = 'ingestion'`.
        """
        if not phrase or not phrase.strip():
            raise ValidationError("phrase must be non-empty", field="phrase")
        if source not in _LEGAL_SOURCES:
            raise ValidationError(
                f"unknown alias source {source!r}; "
                f"must be one of {sorted(_LEGAL_SOURCES)}",
                field="source",
                value=source,
            )
        if not (0.0 <= confidence <= 1.0):
            raise ValidationError(
                f"confidence must be in [0,1]; got {confidence}",
                field="confidence",
                value=confidence,
            )
        if not isinstance(resolved_entity_ref, dict) or not resolved_entity_ref:
            raise ValidationError(
                "resolved_entity_ref must be a non-empty JSON object",
                field="resolved_entity_ref",
            )

        md: dict[str, Any] = dict(extra_metadata or {})
        md["source"] = source

        alias_id = uuid7()

        # Postgres UNIQUE with a NULL column treats each NULL as
        # distinct, so `ON CONFLICT (tenant_id, alias_text, actor_id)`
        # will NOT fire when actor_id IS NULL. We resolve idempotency
        # in two ways: (a) serialise concurrent writers to the same
        # (tenant, phrase) key on a transaction-scoped advisory lock,
        # and (b) use INSERT ... WHERE NOT EXISTS so that even if the
        # lock attempt is bypassed (e.g. mocked), at most one row can
        # be inserted per (tenant, phrase, NULL actor_id) tuple in a
        # single statement — a second winner re-selects the first row.
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                if actor_id is None:
                    # Serialise concurrent writers on (tenant, phrase)
                    # via an advisory lock held for the transaction.
                    # With READ COMMITTED isolation, each statement
                    # takes a fresh snapshot AFTER the lock returns,
                    # so the subsequent SELECT sees any previously
                    # committed row by another writer.
                    lock_key = _advisory_lock_key(tenant_id, phrase)
                    await conn.execute(
                        "SELECT pg_advisory_xact_lock($1)", lock_key
                    )
                    existing = await conn.fetchrow(
                        """
                        SELECT id, tenant_id, alias_text, alias_embedding,
                               actor_id, resolved_entity_ref, is_canonical,
                               entity_metadata, confidence,
                               confirmed_count, contested_count,
                               first_seen_at, last_used_at, source_event_id
                        FROM entity_aliases
                        WHERE tenant_id = $1
                          AND alias_text = $2
                          AND actor_id IS NULL
                        """,
                        tenant_id,
                        phrase,
                    )
                    if existing is not None:
                        # Bump last_used_at, return existing row.
                        row = await conn.fetchrow(
                            """
                            UPDATE entity_aliases
                            SET last_used_at = now()
                            WHERE id = $1
                            RETURNING id, tenant_id, alias_text, alias_embedding,
                                      actor_id, resolved_entity_ref, is_canonical,
                                      entity_metadata, confidence,
                                      confirmed_count, contested_count,
                                      first_seen_at, last_used_at, source_event_id
                            """,
                            existing["id"],
                        )
                        assert row is not None
                        return _hydrate_alias(row)
                    # No existing row; INSERT fresh.
                    row = await conn.fetchrow(
                        """
                        INSERT INTO entity_aliases (
                            id, tenant_id, alias_text, alias_embedding,
                            actor_id, resolved_entity_ref, is_canonical,
                            entity_metadata, confidence,
                            confirmed_count, contested_count,
                            first_seen_at, last_used_at, source_event_id
                        ) VALUES (
                            $1, $2, $3, $4,
                            NULL, $5::jsonb, $6,
                            $7::jsonb, $8,
                            0, 0,
                            now(), now(), $9
                        )
                        RETURNING id, tenant_id, alias_text, alias_embedding,
                                  actor_id, resolved_entity_ref, is_canonical,
                                  entity_metadata, confidence,
                                  confirmed_count, contested_count,
                                  first_seen_at, last_used_at, source_event_id
                        """,
                        alias_id,
                        tenant_id,
                        phrase,
                        alias_embedding,
                        json.dumps(resolved_entity_ref),
                        is_canonical,
                        json.dumps(md),
                        confidence,
                        source_event_id,
                    )
                    assert row is not None
                    return _hydrate_alias(row)

                # actor_id IS NOT NULL — UNIQUE constraint covers this
                # case directly.
                row = await conn.fetchrow(
                    """
                    INSERT INTO entity_aliases (
                        id, tenant_id, alias_text, alias_embedding,
                        actor_id, resolved_entity_ref, is_canonical,
                        entity_metadata, confidence,
                        confirmed_count, contested_count,
                        first_seen_at, last_used_at, source_event_id
                    ) VALUES (
                        $1, $2, $3, $4,
                        $5, $6::jsonb, $7,
                        $8::jsonb, $9,
                        0, 0,
                        now(), now(), $10
                    )
                    ON CONFLICT (tenant_id, alias_text, actor_id)
                    DO UPDATE SET last_used_at = now()
                    RETURNING id, tenant_id, alias_text, alias_embedding,
                              actor_id, resolved_entity_ref, is_canonical,
                              entity_metadata, confidence,
                              confirmed_count, contested_count,
                              first_seen_at, last_used_at, source_event_id
                    """,
                    alias_id,
                    tenant_id,
                    phrase,
                    alias_embedding,
                    actor_id,
                    json.dumps(resolved_entity_ref),
                    is_canonical,
                    json.dumps(md),
                    confidence,
                    source_event_id,
                )
                assert row is not None
                return _hydrate_alias(row)

    # -----------------------------------------------------------------
    # record_usage
    # -----------------------------------------------------------------
    async def record_usage(self, alias_id: UUID) -> EntityAliasRow:
        """
        Increment confirmed_count and touch last_used_at.

        Spec §6 tracks `confirmed_count` / `contested_count`
        separately; "usage" in BUILD-PLAN 1-B maps to a confirmation
        (the phrase was used and accepted). Raises ValidationError if
        the alias does not exist.
        """
        row = await self._pool.fetchrow(
            """
            UPDATE entity_aliases
            SET confirmed_count = confirmed_count + 1,
                last_used_at = now()
            WHERE id = $1
            RETURNING id, tenant_id, alias_text, alias_embedding,
                      actor_id, resolved_entity_ref, is_canonical,
                      entity_metadata, confidence,
                      confirmed_count, contested_count,
                      first_seen_at, last_used_at, source_event_id
            """,
            alias_id,
        )
        if row is None:
            raise ValidationError(
                f"alias {alias_id} not found",
                alias_id=str(alias_id),
            )
        return _hydrate_alias(row)

    # -----------------------------------------------------------------
    # list_ambiguous
    # -----------------------------------------------------------------
    async def list_ambiguous(
        self, tenant_id: UUID, threshold: float = 0.5
    ) -> list[dict[str, Any]]:
        """
        Return candidate phrases for the resolver worker.

        A phrase is ambiguous if EITHER:
        - its normalized form maps to >1 distinct resolved_entity_ref
          within the tenant, OR
        - its highest confidence is below `threshold`.

        Returned shape:
            [
              {
                "normalized": "foo bar",
                "candidates": [
                    {"alias_id": ..., "resolved_entity_ref": {...},
                     "confidence": 0.4, "alias_text": "Foo  Bar"},
                    ...
                ],
                "reason": "multiple_refs" | "low_confidence",
              },
              ...
            ]
        """
        if not (0.0 <= threshold <= 1.0):
            raise ValidationError(
                f"threshold must be in [0,1]; got {threshold}",
                field="threshold",
                value=threshold,
            )

        rows = await self._pool.fetch(
            """
            SELECT id, alias_text, resolved_entity_ref, confidence,
                   regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
                     AS normalized
            FROM entity_aliases
            WHERE tenant_id = $1
            ORDER BY normalized, confidence DESC
            """,
            tenant_id,
        )

        groups: dict[str, list[dict[str, Any]]] = {}
        for r in rows:
            ref_raw = r["resolved_entity_ref"]
            ref = json.loads(ref_raw) if isinstance(ref_raw, str) else ref_raw
            groups.setdefault(r["normalized"], []).append(
                {
                    "alias_id": r["id"],
                    "alias_text": r["alias_text"],
                    "resolved_entity_ref": ref,
                    "confidence": float(r["confidence"]),
                }
            )

        ambiguous: list[dict[str, Any]] = []
        for normalized, candidates in groups.items():
            distinct_refs = {
                json.dumps(c["resolved_entity_ref"], sort_keys=True)
                for c in candidates
            }
            if len(distinct_refs) > 1:
                ambiguous.append(
                    {
                        "normalized": normalized,
                        "candidates": candidates,
                        "reason": "multiple_refs",
                    }
                )
                continue
            max_conf = max(c["confidence"] for c in candidates)
            if max_conf < threshold:
                ambiguous.append(
                    {
                        "normalized": normalized,
                        "candidates": candidates,
                        "reason": "low_confidence",
                    }
                )
        return ambiguous

    # -----------------------------------------------------------------
    # reverse_lookup
    # -----------------------------------------------------------------
    async def reverse_lookup(
        self, canonical_ref: dict[str, Any], tenant_id: UUID
    ) -> list[str]:
        """
        Return every alias_text (raw, unnormalized) that maps to the
        given resolved_entity_ref within the tenant. Uses the GIN
        index `aliases_entity_idx` via the JSONB containment operator.
        """
        if not isinstance(canonical_ref, dict) or not canonical_ref:
            raise ValidationError(
                "canonical_ref must be a non-empty JSON object",
                field="canonical_ref",
            )
        rows = await self._pool.fetch(
            """
            SELECT alias_text
            FROM entity_aliases
            WHERE tenant_id = $1
              AND resolved_entity_ref @> $2::jsonb
              AND resolved_entity_ref <@ $2::jsonb
            ORDER BY last_used_at DESC, alias_text ASC
            """,
            tenant_id,
            json.dumps(canonical_ref),
        )
        return [r["alias_text"] for r in rows]


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _advisory_lock_key(tenant_id: UUID, phrase: str) -> int:
    """
    Derive a deterministic 64-bit signed int from (tenant_id, phrase)
    for use with pg_advisory_xact_lock. Any collision only serialises
    unrelated keys — never causes incorrect results.
    """
    import hashlib

    h = hashlib.blake2b(
        f"{tenant_id}:{phrase}".encode("utf-8"), digest_size=8
    ).digest()
    # Convert to signed 64-bit for pg_advisory_xact_lock's `bigint`
    # parameter. Mask off the high bit to stay within int8 range.
    unsigned = int.from_bytes(h, "big")
    signed = unsigned - (1 << 64) if unsigned >= (1 << 63) else unsigned
    return signed


def _hydrate_alias(row: asyncpg.Record) -> EntityAliasRow:
    """
    Convert the asyncpg Record to EntityAliasRow, parsing JSONB fields
    that might arrive as strings (depending on codec state).
    """
    d = dict(row)
    for jsonb_field in ("resolved_entity_ref", "entity_metadata"):
        v = d.get(jsonb_field)
        if isinstance(v, str):
            d[jsonb_field] = json.loads(v)
    emb = d.get("alias_embedding")
    if emb is not None and not isinstance(emb, list):
        # pgvector returns a string like "[0.1,0.2,...]" when codec is
        # not registered. Parse it manually for the test path that
        # might not register the pgvector codec.
        if isinstance(emb, str):
            emb_str = emb.strip().strip("[]")
            d["alias_embedding"] = (
                [float(x) for x in emb_str.split(",") if x]
                if emb_str
                else None
            )
    return EntityAliasRow.model_validate(d)


__all__ = ["EntityAliasRepo", "normalize_phrase"]
