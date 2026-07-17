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
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7
from lib.shared.types import EntityAliasRow
from services.domain.entity_aliases.authority import governed_identity_writer
from services.domain.entity_aliases.authority import identity_command_authority


# Resolver output is assessment/candidate state, never canonical identity.
# Canonical aliases may be seeded by ingestion, written by an independently
# adjudicated manual transition, or maintained by resource lifecycle code.
_LEGAL_SOURCES: frozenset[str] = frozenset(
    ("ingestion", "manual", "resource_lifecycle")
)

_WHITESPACE_RE = re.compile(r"\s+", flags=re.UNICODE)

# Actors and resources are the canonical entity classes whose lifecycle is
# represented in the current schema. Customer resources have valid-time
# archival semantics; actor history is intentionally out of scope here.
# Other ref types are source-native or legacy references and remain eligible
# for the generic alias fast path.
#
# `$3` is the caller's event time. NULL means current-time resolution.
_VALID_CANONICAL_TARGET_SQL = r"""
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
            AND (
              target.archived_at IS NULL
              OR target.archived_at > COALESCE($3::timestamptz, now())
            )
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


def _requires_identity_write_authority(
    resolved_entity_ref: dict[str, Any],
) -> bool:
    if resolved_entity_ref.get("type") not in {
        "actor",
        "resource",
        "customer",
    }:
        return False
    try:
        UUID(str(resolved_entity_ref.get("id") or ""))
    except ValueError:
        return False
    return True


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
          AND valid_from <= now()
          AND valid_until IS NULL
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


@governed_identity_writer
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
    valid_from: datetime | None = None,
    valid_until: datetime | None = None,
    validity_event_id: UUID | None = None,
    validity_reason: str | None = None,
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
    if valid_from is not None and valid_from.tzinfo is None:
        raise ValidationError(
            "valid_from must be timezone-aware",
            field="valid_from",
        )
    if valid_until is not None and valid_until.tzinfo is None:
        raise ValidationError(
            "valid_until must be timezone-aware",
            field="valid_until",
        )
    if (
        valid_from is not None
        and valid_until is not None
        and valid_until < valid_from
    ):
        raise ValidationError(
            "valid_until must not be earlier than valid_from",
            field="valid_until",
        )

    metadata: dict[str, Any] = dict(extra_metadata or {})
    metadata["source"] = source
    if (
        source == "manual"
        and _requires_identity_write_authority(resolved_entity_ref)
        and not adjudicated
    ):
        raise ValidationError(
            "UUID-backed canonical aliases require an authorized "
            "adjudication trace",
            field="adjudicated",
        )
    if adjudicated:
        await _validate_adjudicated_alias_authority(
            conn,
            tenant_id=tenant_id,
            phrase=phrase,
            resolved_entity_ref=resolved_entity_ref,
            source_event_id=source_event_id,
            metadata=metadata,
        )
    alias_id = uuid7()

    if actor_id is None:
        # Match PostgreSQL's eventual INSERT lock order before taking the
        # per-name advisory lock. Customer lifecycle writes take a stronger
        # table lock, so this ordering avoids table/advisory deadlocks.
        await conn.execute("LOCK TABLE entity_aliases IN ROW EXCLUSIVE MODE")
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
              AND valid_until IS NULL
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
                first_seen_at, last_used_at, source_event_id,
                valid_from, valid_until, validity_event_id, validity_reason
            ) VALUES (
                $1, $2, $3, $4,
                NULL, $5::jsonb, $6,
                $7::jsonb, $8,
                0, 0,
                COALESCE($9::timestamptz, now()), now(), $10,
                COALESCE($9::timestamptz, now()), $11, $12, $13
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
            valid_from,
            source_event_id,
            valid_until,
            validity_event_id,
            validity_reason,
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
            first_seen_at, last_used_at, source_event_id,
            valid_from, valid_until, validity_event_id, validity_reason
        ) VALUES (
            $1, $2, $3, $4,
            $5, $6::jsonb, $7,
            $8::jsonb, $9,
            0, 0,
            COALESCE($10::timestamptz, now()), now(), $11,
            COALESCE($10::timestamptz, now()), $12, $13, $14
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
        valid_from,
        source_event_id,
        valid_until,
        validity_event_id,
        validity_reason,
    )
    assert row is not None
    return _hydrate_alias(row)


@governed_identity_writer
async def close_aliases_for_entity_with_connection(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    resolved_entity_ref: dict[str, Any],
    valid_until: datetime,
    validity_event_id: UUID | None,
    validity_reason: str,
    phrases: list[str] | None = None,
) -> int:
    """Close every current object alias for one canonical entity.

    Lifecycle mutations are rare and correctness-sensitive. A table-level
    writer lock prevents an alias insert from racing between the close and the
    successor-name insert in the same customer transaction.
    """
    if valid_until.tzinfo is None:
        raise ValidationError(
            "valid_until must be timezone-aware",
            field="valid_until",
        )
    if not resolved_entity_ref:
        raise ValidationError(
            "resolved_entity_ref must be a non-empty JSON object",
            field="resolved_entity_ref",
        )
    if not validity_reason.strip():
        raise ValidationError(
            "validity_reason must be non-empty",
            field="validity_reason",
        )

    normalized_phrases: list[str] | None = None
    if phrases is not None:
        normalized_phrases = sorted(
            {
                normalized
                for phrase in phrases
                if phrase
                and (normalized := normalize_phrase(phrase))
            }
        )
    await conn.execute("LOCK TABLE entity_aliases IN SHARE ROW EXCLUSIVE MODE")
    result = await conn.execute(
        """
        UPDATE entity_aliases
        SET valid_until = $3,
            validity_event_id = $4,
            validity_reason = $5,
            entity_metadata = COALESCE(entity_metadata, '{}'::jsonb)
              || jsonb_build_object(
                   'validity_reason', $5::text,
                   'validity_event_id', CASE
                     WHEN $4::uuid IS NULL THEN NULL
                     ELSE $4::uuid::text
                   END
                 )
        WHERE tenant_id = $1
          AND actor_id IS NULL
          AND resolved_entity_ref ->> 'type' = $2::jsonb ->> 'type'
          AND resolved_entity_ref ->> 'id' = $2::jsonb ->> 'id'
          AND COALESCE(resolved_entity_ref ->> 'version', '1')
              = COALESCE($2::jsonb ->> 'version', '1')
          AND valid_from <= $3
          AND valid_until IS NULL
          AND (
            $6::text[] IS NULL
            OR regexp_replace(lower(alias_text), '\\s+', ' ', 'g')
               = ANY($6::text[])
          )
        """,
        tenant_id,
        json.dumps(resolved_entity_ref),
        valid_until,
        validity_event_id,
        validity_reason,
        normalized_phrases,
    )
    return int(result.rsplit(" ", 1)[-1])


@governed_identity_writer
async def delete_stale_aliases_with_connection(
    conn: asyncpg.Connection, *, stale_days: int
) -> int:
    """Governed maintenance transition for unused, stale aliases."""
    tag = await conn.execute(
        """
        DELETE FROM entity_aliases
        WHERE confirmed_count = 0
          AND contested_count = 0
          AND last_used_at < (now() - ($1 || ' days')::interval)
        """,
        str(int(stale_days)),
    )
    return int(tag.rsplit(" ", 1)[-1])


async def _validate_adjudicated_alias_authority(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    phrase: str,
    resolved_entity_ref: dict[str, Any],
    source_event_id: UUID | None,
    metadata: dict[str, Any],
) -> None:
    """Require an answered clarification and exact grounding successor."""

    if metadata.get("source") != "manual":
        raise ValidationError(
            "adjudicated alias writes require the manual authority lane",
            field="source",
        )
    clarification_id = str(
        metadata.get("clarification_request_id") or ""
    ).strip()
    identity_basis_ref = str(
        metadata.get("identity_basis_ref") or ""
    ).strip()
    successor_trace_id = str(
        metadata.get("grounding_successor_trace_id") or ""
    ).strip()
    lineage = metadata.get("grounding_feedback_lineage")
    if (
        metadata.get("identity_basis_class") != "independently_adjudicated"
        or not clarification_id
        or identity_basis_ref
        != f"clarification-request:{clarification_id}"
        or not successor_trace_id
        or not isinstance(lineage, dict)
        or not lineage.get("grounding_trace_id")
        or source_event_id is None
    ):
        raise ValidationError(
            "adjudicated alias requires an authorized promotion trace and "
            "grounding lineage",
            field="extra_metadata",
        )
    authorized = await conn.fetchval(
        """
        SELECT EXISTS (
          SELECT 1
          FROM clarification_requests clarification
          JOIN grounding_traces predecessor
            ON predecessor.tenant_id=clarification.tenant_id
           AND predecessor.id::text=$6
          JOIN grounding_traces successor
            ON successor.tenant_id=predecessor.tenant_id
           AND successor.id::text=$7
           AND successor.trace ->> 'supersedes_grounding_trace_id'
               = predecessor.id::text
          WHERE clarification.tenant_id=$1
            AND clarification.id::text=$2
            AND clarification.kind='entity_resolution'
            AND clarification.status='answered'
            AND clarification.source_observation_id=$3
            AND clarification.payload -> 'feedback_lineage'=$4::jsonb
            AND successor.trace ->> 'adjudication_ref'=$5
            AND successor.current_fate='resolved_for_consumer'
            AND successor.source_observation_id=$3
            AND successor.phrase=$8
            AND successor.selected_referent=$9::jsonb
        )
        """,
        tenant_id,
        clarification_id,
        source_event_id,
        json.dumps(lineage, sort_keys=True, default=str),
        identity_basis_ref,
        str(lineage["grounding_trace_id"]),
        successor_trace_id,
        phrase,
        json.dumps(resolved_entity_ref, sort_keys=True, default=str),
    )
    if not authorized:
        raise ValidationError(
            "adjudicated alias authority does not match grounding lineage",
            field="extra_metadata",
        )


class EntityAliasRepo:
    """Repository for entity_aliases."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # -----------------------------------------------------------------
    # fast_path_resolve
    # -----------------------------------------------------------------
    async def fast_path_resolve(
        self,
        phrase: str,
        tenant_id: UUID,
        *,
        as_of: datetime | None = None,
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
        if as_of is not None and as_of.tzinfo is None:
            raise ValidationError("as_of must be timezone-aware", field="as_of")

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
              AND valid_from <= COALESCE($3::timestamptz, now())
              AND (
                valid_until IS NULL
                OR valid_until > COALESCE($3::timestamptz, now())
              )
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
              AND {_VALID_CANONICAL_TARGET_SQL}
            LIMIT 2
            """,
            tenant_id,
            norm,
            as_of,
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
        self,
        phrases: list[str],
        tenant_id: UUID,
        *,
        as_of: datetime | None = None,
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
        if as_of is not None and as_of.tzinfo is None:
            raise ValidationError("as_of must be timezone-aware", field="as_of")

        rows = await self._pool.fetch(
            f"""
            SELECT
                regexp_replace(lower(alias_text), '\\s+', ' ', 'g') AS normalized,
                resolved_entity_ref
            FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = ANY($2::text[])
              AND valid_from <= COALESCE($3::timestamptz, now())
              AND (
                valid_until IS NULL
                OR valid_until > COALESCE($3::timestamptz, now())
              )
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
              AND {_VALID_CANONICAL_TARGET_SQL}
            """,
            tenant_id,
            norms,
            as_of,
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
        valid_from: datetime | None = None,
        valid_until: datetime | None = None,
        validity_event_id: UUID | None = None,
        validity_reason: str | None = None,
    ) -> EntityAliasRow:
        """
        Insert an alias. Idempotent on (tenant_id, alias_text, actor_id)
        per S6.1 UNIQUE constraint: if the same tuple is inserted
        again, the existing row is returned (via ON CONFLICT ...
        DO UPDATE that effectively preserves the first-seen row but
        bumps last_used_at).

        `source` is a label (ingestion|manual|resource_lifecycle) — S6.1
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
                    valid_from=valid_from,
                    valid_until=valid_until,
                    validity_event_id=validity_event_id,
                    validity_reason=validity_reason,
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
        async with self._pool.acquire() as conn, conn.transaction():
            async with identity_command_authority(conn):
                row = await conn.fetchrow(
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
              AND valid_from <= now()
              AND (valid_until IS NULL OR valid_until > now())
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

    async def list_history(
        self,
        phrase: str,
        tenant_id: UUID,
    ) -> list[dict[str, Any]]:
        """Return the complete valid-time history for one normalized name."""
        if not phrase or not phrase.strip():
            return []
        norm = normalize_phrase(phrase)
        rows = await self._pool.fetch(
            """
            SELECT id, alias_text, resolved_entity_ref, is_canonical,
                   confidence, first_seen_at, last_used_at,
                   source_event_id, valid_from, valid_until,
                   validity_event_id, validity_reason
            FROM entity_aliases
            WHERE tenant_id = $1
              AND regexp_replace(lower(alias_text), '\\s+', ' ', 'g') = $2
            ORDER BY valid_from ASC, first_seen_at ASC, id ASC
            """,
            tenant_id,
            norm,
        )
        history: list[dict[str, Any]] = []
        for row in rows:
            item = dict(row)
            item["resolved_entity_ref"] = _parse_jsonb_obj(
                item["resolved_entity_ref"]
            )
            history.append(item)
        return history


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
    "close_aliases_for_entity_with_connection",
    "insert_alias_with_connection",
    "delete_stale_aliases_with_connection",
    "normalize_phrase",
    "validate_governed_alias_replay",
]
