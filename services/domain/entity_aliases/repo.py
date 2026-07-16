"""
services/domain/entity_aliases/repo.py — Entity alias resolution repo.

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

from lib.contracts.kernel import canonical_sha256
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

# Actors and resources are the canonical entity classes whose lifecycle is
# represented in the current schema. Other ref types are source-native or
# legacy references and remain eligible for the generic alias fast path.
_ACTIVE_CANONICAL_TARGET_SQL = r"""
(
  COALESCE(resolved_entity_ref ->> 'type', '')
    NOT IN ('actor', 'resource', 'customer')
  OR COALESCE(resolved_entity_ref ->> 'id', '')
    !~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
  OR (
    COALESCE(resolved_entity_ref ->> 'version', '1') = '1'
    AND (
      (
        resolved_entity_ref ->> 'type' = 'actor'
        AND EXISTS (
          SELECT 1
          FROM actors target
          WHERE target.tenant_id = entity_aliases.tenant_id
            AND target.id::text = resolved_entity_ref ->> 'id'
            AND target.status = 'active'
        )
      )
      OR (
        resolved_entity_ref ->> 'type' IN ('resource', 'customer')
        AND EXISTS (
          SELECT 1
          FROM resources target
          WHERE target.tenant_id = entity_aliases.tenant_id
            AND target.id::text = resolved_entity_ref ->> 'id'
            AND target.archived_at IS NULL
            AND (
              resolved_entity_ref ->> 'type' <> 'customer'
              OR target.metadata ->> 'semantic_kind' = 'customer'
            )
        )
      )
    )
  )
)
"""


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


def _parse_jsonb_obj(raw: Any) -> dict[str, Any]:
    """asyncpg may return JSONB as a dict or encoded string."""
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


async def validate_governed_alias_replay(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    alias_id: UUID,
    phrase: str,
    canonical_ref: dict[str, Any],
    identity_basis_ref: str,
    adjudication_answer_digest: str,
) -> bool:
    """Revalidate exact tenant-global alias authority inside the write transaction."""

    await conn.execute(
        "SELECT pg_advisory_xact_lock($1)",
        _advisory_lock_key(tenant_id, phrase),
    )
    rows = await conn.fetch(
        """
        SELECT id, actor_id, resolved_entity_ref, entity_metadata,
               source_event_id
        FROM entity_aliases
        WHERE tenant_id=$1
          AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
              = regexp_replace(lower($2::text), '\\s+', ' ', 'g')
        FOR SHARE
        """,
        tenant_id,
        phrase,
    )
    if not rows or any(row["actor_id"] is not None for row in rows):
        return False
    refs = {
        json.dumps(_parse_jsonb_obj(row["resolved_entity_ref"]), sort_keys=True)
        for row in rows
    }
    if len(refs) != 1:
        return False
    selected = next((row for row in rows if row["id"] == alias_id), None)
    if selected is None:
        return False
    selected_ref = _parse_jsonb_obj(selected["resolved_entity_ref"])
    expected_ref = {
        "type": str(canonical_ref.get("type") or ""),
        "id": str(canonical_ref.get("id") or ""),
        "version": int(canonical_ref.get("version", 1)),
    }
    materialized_ref = {
        "type": str(selected_ref.get("type") or ""),
        "id": str(selected_ref.get("id") or ""),
        "version": int(selected_ref.get("version", 1)),
    }
    if materialized_ref != expected_ref:
        return False
    metadata = _parse_jsonb_obj(selected["entity_metadata"]) or {}
    if (
        metadata.get("source") != "manual"
        or metadata.get("identity_basis_class") != "independently_adjudicated"
        or metadata.get("identity_basis_ref") != identity_basis_ref
        or metadata.get("adjudication_state") != "active"
        or metadata.get("resolution_scope") != "tenant_global_exact"
        or metadata.get("autonomous_replay_eligible") is not True
        or metadata.get("adjudication_answer_digest")
        != adjudication_answer_digest
        or metadata.get("replay_policy_version")
        != "governed-exact-alias-replay-v1"
    ):
        return False
    clarification_id = metadata.get("clarification_request_id")
    lineage = metadata.get("grounding_feedback_lineage")
    if not isinstance(lineage, dict) or not clarification_id:
        return False
    clarification = await conn.fetchrow(
        """
        SELECT id, source_observation_id, payload, answer, answered_by
        FROM clarification_requests
        WHERE tenant_id=$1 AND id::text=$2 AND status='answered'
        FOR SHARE
        """,
        tenant_id,
        str(clarification_id),
    )
    if clarification is None or clarification["source_observation_id"] != selected[
        "source_event_id"
    ]:
        return False
    payload = _parse_jsonb_obj(clarification["payload"]) or {}
    payload_lineage = payload.get("feedback_lineage")
    if (
        payload.get("phrase") != phrase
        or not isinstance(payload_lineage, dict)
        or payload_lineage != lineage
    ):
        return False
    answer = _parse_jsonb_obj(clarification["answer"]) or {}
    if isinstance(answer.get("value"), dict):
        answer = {**answer["value"], **{k: v for k, v in answer.items() if k != "value"}}
    if (
        answer.get("action") not in {"accept_candidate", "create_new_entity"}
        or answer.get("resolution_scope") != "tenant_global_exact"
        or answer.get("confirm_tenant_global_reuse") is not True
    ):
        return False
    answer_ref = answer.get("canonical_ref")
    if isinstance(answer_ref, dict):
        normalized_answer_ref = {
            "type": str(answer_ref.get("type") or ""),
            "id": str(answer_ref.get("id") or ""),
            "version": int(answer_ref.get("version", 1)),
        }
        if normalized_answer_ref != expected_ref:
            return False
    answered_by = clarification["answered_by"]
    if answered_by is None or metadata.get("adjudicated_by") != str(answered_by):
        return False
    digest = canonical_sha256(
        {
            "tenant_id": str(tenant_id),
            "clarification_request_id": str(clarification["id"]),
            "phrase": phrase,
            "canonical_ref": canonical_ref,
            "resolution_scope": "tenant_global_exact",
            "answered_by": str(answered_by),
            "feedback_lineage": payload_lineage,
        }
    )
    if digest != adjudication_answer_digest:
        return False
    authorized = await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM actors actor
          JOIN actor_roles role
            ON role.tenant_id=actor.tenant_id
           AND role.actor_id=actor.id
           AND role.entity_type='tenant'
           AND role.entity_id IS NULL
           AND role.role IN ('admin', 'leadership')
           AND role.revoked_at IS NULL
          WHERE actor.tenant_id=$1
            AND actor.id=$2
            AND actor.status='active'
        )
        """,
        tenant_id,
        answered_by,
    )
    if not authorized:
        return False
    predecessor_trace_id = lineage.get("grounding_trace_id")
    successor_valid = await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM grounding_traces predecessor
          JOIN grounding_traces successor
            ON successor.tenant_id=predecessor.tenant_id
           AND successor.trace ->> 'supersedes_grounding_trace_id'
               = predecessor.id::text
          WHERE predecessor.tenant_id=$1
            AND predecessor.id::text=$2
            AND successor.trace ->> 'adjudication_ref'=$3
            AND successor.current_fate='resolved_for_consumer'
            AND successor.selected_referent=$4::jsonb
        )
        """,
        tenant_id,
        str(predecessor_trace_id or ""),
        identity_basis_ref,
        json.dumps(expected_ref),
    )
    if not successor_valid:
        return False
    try:
        target_id = UUID(expected_ref["id"])
    except (TypeError, ValueError):
        return False
    if expected_ref["version"] != 1:
        return False
    if expected_ref["type"] == "actor":
        return bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                  SELECT 1 FROM actors
                  WHERE tenant_id=$1 AND id=$2 AND status='active'
                )
                """,
                tenant_id,
                target_id,
            )
        )
    if expected_ref["type"] in {"resource", "customer"}:
        return bool(
            await conn.fetchval(
                """
                SELECT EXISTS (
                  SELECT 1 FROM resources
                  WHERE tenant_id=$1 AND id=$2 AND archived_at IS NULL
                    AND (
                      $3 <> 'customer'
                      OR metadata ->> 'semantic_kind' = 'customer'
                    )
                )
                """,
                tenant_id,
                target_id,
                expected_ref["type"],
            )
        )
    return False


async def insert_alias_with_connection(
    conn: asyncpg.Connection,
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
    adjudicated: bool = False,
) -> EntityAliasRow:
    """Insert one alias on a caller-owned connection and transaction."""

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

    metadata: dict[str, Any] = dict(extra_metadata or {})
    metadata["source"] = source
    alias_id = uuid7()

    if actor_id is None:
        lock_key = _advisory_lock_key(tenant_id, phrase)
        await conn.execute("SELECT pg_advisory_xact_lock($1)", lock_key)
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
            if adjudicated:
                row = await conn.fetchrow(
                    """
                    UPDATE entity_aliases
                    SET resolved_entity_ref = $2::jsonb,
                        entity_metadata = COALESCE(entity_metadata, '{}'::jsonb)
                                          || $3::jsonb,
                        confidence = GREATEST(confidence, $4),
                        confirmed_count = confirmed_count + 1,
                        contested_count = contested_count + CASE
                          WHEN resolved_entity_ref = $2::jsonb THEN 0 ELSE 1
                        END,
                        source_event_id = COALESCE($5, source_event_id),
                        last_used_at = now()
                    WHERE id = $1
                    RETURNING id, tenant_id, alias_text, alias_embedding,
                              actor_id, resolved_entity_ref, is_canonical,
                              entity_metadata, confidence,
                              confirmed_count, contested_count,
                              first_seen_at, last_used_at, source_event_id
                    """,
                    existing["id"],
                    json.dumps(resolved_entity_ref),
                    json.dumps(metadata),
                    confidence,
                    source_event_id,
                )
                assert row is not None
                return _hydrate_alias(row)
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
            json.dumps(metadata),
            confidence,
            source_event_id,
        )
        assert row is not None
        return _hydrate_alias(row)

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
        json.dumps(metadata),
        confidence,
        source_event_id,
    )
    assert row is not None
    return _hydrate_alias(row)


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
        - Fetch every ingest-eligible row in `tenant_id` whose normalized
          alias_text matches exactly. Context-local adjudications remain
          resolver context and never become source-native entity hints.
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

        # We only need to know whether the normalized phrase maps to
        # zero, one, or multiple canonical refs. Fetching at most two
        # distinct refs avoids sorting every duplicate alias row on the
        # hot ingestion path.
        rows = await self._pool.fetch(
            f"""
            SELECT DISTINCT resolved_entity_ref
            FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = $2
              AND NOT (
                COALESCE(
                  entity_metadata ->> 'identity_basis_class'
                    = 'independently_adjudicated',
                  FALSE
                )
                AND (
                  entity_metadata ->> 'resolution_scope'
                    = 'source_context_only'
                  OR (
                    entity_metadata ? 'autonomous_replay_eligible'
                    AND COALESCE(
                      (entity_metadata
                        ->> 'autonomous_replay_eligible')::boolean,
                      FALSE
                    ) = FALSE
                  )
                )
              )
              AND {_ACTIVE_CANONICAL_TARGET_SQL}
            LIMIT 2
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
            parsed = _parse_jsonb_obj(r["resolved_entity_ref"])
            key = json.dumps(parsed, sort_keys=True)
            refs_by_json.setdefault(key, parsed)

        if len(refs_by_json) == 1:
            return next(iter(refs_by_json.values()))
        # Ambiguous — multiple distinct canonical refs for same phrase.
        return None

    async def fast_path_resolve_many(
        self, phrases: list[str], tenant_id: UUID
    ) -> dict[str, dict[str, Any]]:
        """
        Bulk version of :meth:`fast_path_resolve`.

        The return keys are normalized phrases. Only unambiguous aliases
        are returned; phrases that have no row or multiple distinct refs
        are omitted so callers can use the same fallback path as the
        single-phrase resolver. This collapses the ingestion hot path from
        up to 50 alias queries per observation to one indexed lookup.
        """
        norms: list[str] = []
        seen: set[str] = set()
        for phrase in phrases:
            if not phrase or not phrase.strip():
                continue
            norm = normalize_phrase(phrase)
            if not norm or norm in seen:
                continue
            seen.add(norm)
            norms.append(norm)
        if not norms:
            return {}

        rows = await self._pool.fetch(
            f"""
            SELECT
                regexp_replace(lower(alias_text), '\\s+', ' ', 'g') AS normalized,
                resolved_entity_ref
            FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = ANY($2::text[])
              AND NOT (
                COALESCE(
                  entity_metadata ->> 'identity_basis_class'
                    = 'independently_adjudicated',
                  FALSE
                )
                AND (
                  entity_metadata ->> 'resolution_scope'
                    = 'source_context_only'
                  OR (
                    entity_metadata ? 'autonomous_replay_eligible'
                    AND COALESCE(
                      (entity_metadata
                        ->> 'autonomous_replay_eligible')::boolean,
                      FALSE
                    ) = FALSE
                  )
                )
              )
              AND {_ACTIVE_CANONICAL_TARGET_SQL}
            """,
            tenant_id,
            norms,
        )

        refs_by_norm: dict[str, dict[str, dict[str, Any]]] = {}
        for row in rows:
            ref = _parse_jsonb_obj(row["resolved_entity_ref"])
            key = json.dumps(ref, sort_keys=True)
            refs_by_norm.setdefault(row["normalized"], {}).setdefault(key, ref)

        resolved: dict[str, dict[str, Any]] = {}
        for norm, refs_by_json in refs_by_norm.items():
            if len(refs_by_json) == 1:
                resolved[norm] = next(iter(refs_by_json.values()))
        return resolved

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
        adjudicated: bool = False,
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
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                return await insert_alias_with_connection(
                    conn,
                    phrase=phrase,
                    resolved_entity_ref=resolved_entity_ref,
                    source=source,
                    confidence=confidence,
                    tenant_id=tenant_id,
                    actor_id=actor_id,
                    source_event_id=source_event_id,
                    is_canonical=is_canonical,
                    alias_embedding=alias_embedding,
                    extra_metadata=extra_metadata,
                    adjudicated=adjudicated,
                )

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


__all__ = [
    "EntityAliasRepo",
    "insert_alias_with_connection",
    "normalize_phrase",
    "validate_governed_alias_replay",
]
