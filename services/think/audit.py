"""services/think/audit.py — per-Model audit chain.

PR 1 (Q5) implementation. See services/think/SUBSTRATE_SEMANTICS.md
"Q5 — Audit chain" for the canonical decision.

Public API:

  * `emit_audit_event(conn, ...)` — emit a single audit_events row inside
     the caller's transaction. Returns the new event_id.
  * `emit_reconciliation_merge_audit(conn, ...)` — convenience wrapper
     that emits with cause_type='reconciliation_merge' and
     source_model_ids populated.
  * `get_audit_chain(conn, model_id)` — return the full chain for a
     Model, ordered by occurred_at. Walks reconciliation_merge events'
     source_model_ids transitively to return the union of source chains.
  * `find_re_assertable_event(conn, model_id, new_state)` — find the
     earliest prior event on the same Model whose new_state matches.
     Used to populate re_asserts_event_id on reversal-of-reversal.
  * `model_state_snapshot(row)` — extract the audit-relevant subset
     of a ModelRow as a JSON-serialisable dict. Excludes embedding
     (768 floats per row would balloon audit storage).

Cause-type vocabulary (matches the CHECK in migration 0030):

  CAUSE_CREATE              — Model insert.
  CAUSE_ARCHIVE             — status='archived' transition.
  CAUSE_FIELD_UPDATE        — generic column update (signal_readings,
                              evidential_weight, etc.).
  CAUSE_CONFIDENCE_UPDATE   — bulk_confidence_update path.
  CAUSE_RECONCILIATION_MERGE — auto_merge / second_pass_merge that
                              converted an insert into an update on an
                              existing Model.

Co-existence with `reconciliation_events` and observations:
  * `reconciliation_events` is decision history for the reconciler
    (auto_merge/human_review/no_match), keyed by candidate pair.
  * Observations(kind='state_change') is the signal-shaped event log
    for cascade and NOTIFY subscribers.
  * `audit_events` is per-Model state history, keyed by model_id.
  All three are emitted in the same transaction; none replaces another.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Sequence
from uuid import UUID

import asyncpg


# =====================================================================
# Cause-type vocabulary — keep in sync with migration 0030's CHECK.
# =====================================================================

CAUSE_CREATE = "create"
CAUSE_ARCHIVE = "archive"
CAUSE_FIELD_UPDATE = "field_update"
CAUSE_CONFIDENCE_UPDATE = "confidence_update"
CAUSE_RECONCILIATION_MERGE = "reconciliation_merge"

_CAUSE_TYPES: frozenset[str] = frozenset({
    CAUSE_CREATE,
    CAUSE_ARCHIVE,
    CAUSE_FIELD_UPDATE,
    CAUSE_CONFIDENCE_UPDATE,
    CAUSE_RECONCILIATION_MERGE,
})


# =====================================================================
# AuditEvent dataclass — return type of get_audit_chain().
# =====================================================================


@dataclass
class AuditEvent:
    event_id: int
    model_id: UUID
    tenant_id: UUID
    occurred_at: datetime
    cause_type: str
    new_state: dict[str, Any]
    previous_state: dict[str, Any] | None = None
    cause_id: UUID | None = None
    changed_fields: list[str] = field(default_factory=list)
    re_asserts_event_id: int | None = None
    source_model_ids: list[UUID] = field(default_factory=list)


# =====================================================================
# Snapshot helper — what goes into previous_state / new_state.
# =====================================================================

# Audit-relevant columns. Embedding is omitted (768 floats per snapshot
# would balloon storage); creation timestamps are derivable from the
# audit chain itself; large JSONB fields (proposition, scope_*) are
# kept because the diff between events is meaningful for them.
_SNAPSHOT_FIELDS: tuple[str, ...] = (
    "id",
    "tenant_id",
    "born_from_event_id",
    "proposition",
    "natural",
    "scope_actors",
    "scope_entities",
    "scope_temporal",
    "confidence",
    "activation",
    "falsifier",
    "signal_readings",
    "reading_contestable",
    "supporting_event_ids",
    "supporting_model_ids",
    "evidential_weight",
    "status",
    "archive_reason",
    "evaluate_at",
    "resolution_criteria",
    "contributing_models",
    "visible_to_subjects",
    "proposition_kind",
    "confirmed_count",
    "contested_count",
    "last_confirmed_at",
    "confidence_at_assertion",
    "resolved_at",
    "resolution_outcome",
    "activation_coefficient",
)


def model_state_snapshot(row: Any) -> dict[str, Any]:
    """
    Extract the audit-relevant subset of a Model row (ModelRow or
    asyncpg Record) into a JSON-serialisable dict.

    UUIDs become strings, datetimes become ISO strings — exactly what
    JSONB needs. Embedding is excluded.

    Tolerant of pydantic models, dataclasses, and asyncpg Records by
    using `getattr` / item access fallbacks.
    """
    snap: dict[str, Any] = {}
    for col in _SNAPSHOT_FIELDS:
        v = _get_field(row, col)
        if v is None:
            continue
        snap[col] = _to_json_safe(v)
    return snap


def _get_field(row: Any, name: str) -> Any:
    if hasattr(row, name):
        return getattr(row, name)
    try:
        return row[name]
    except (KeyError, TypeError):
        return None


def _to_json_safe(v: Any) -> Any:
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, UUID):
        return str(v)
    if isinstance(v, datetime):
        return v.isoformat()
    if isinstance(v, (list, tuple)):
        return [_to_json_safe(x) for x in v]
    if isinstance(v, dict):
        return {k: _to_json_safe(x) for k, x in v.items()}
    # Fall back to str() for anything else; better than failing the
    # audit emission.
    return str(v)


# =====================================================================
# emit_audit_event — single-row insert.
# =====================================================================


async def emit_audit_event(
    conn: asyncpg.Connection,
    *,
    model_id: UUID,
    tenant_id: UUID,
    cause_type: str,
    new_state: dict[str, Any],
    previous_state: dict[str, Any] | None = None,
    cause_id: UUID | None = None,
    changed_fields: Sequence[str] | None = None,
    re_asserts_event_id: int | None = None,
    source_model_ids: Sequence[UUID] | None = None,
    detect_re_assert: bool = True,
) -> int:
    """
    Insert one audit_events row inside `conn`'s transaction and return
    the new event_id.

    `cause_type` MUST be one of the constants in this module. Unknown
    values raise ValueError loudly — drift is a bug.

    `detect_re_assert`: if True (default) and `re_asserts_event_id` is
    not provided, look up the earliest prior event on `model_id` whose
    new_state matches the new `new_state`, and set re_asserts_event_id
    to that event's id. Disabling is for explicit-ID call sites or for
    cases where reversal detection would be a false positive (e.g.
    creates).
    """
    if cause_type not in _CAUSE_TYPES:
        raise ValueError(
            f"unknown cause_type {cause_type!r}; must be one of "
            f"{sorted(_CAUSE_TYPES)!r}"
        )

    if (
        re_asserts_event_id is None
        and detect_re_assert
        and cause_type != CAUSE_CREATE
    ):
        re_asserts_event_id = await find_re_assertable_event(
            conn, model_id, new_state
        )

    fields_arr = list(changed_fields or [])
    sources_arr = [str(s) for s in (source_model_ids or [])]

    row = await conn.fetchrow(
        """
        INSERT INTO audit_events (
            model_id, tenant_id, cause_id, cause_type,
            previous_state, new_state, changed_fields,
            re_asserts_event_id, source_model_ids
        ) VALUES (
            $1, $2, $3, $4,
            $5::jsonb, $6::jsonb, $7,
            $8, $9::uuid[]
        )
        RETURNING event_id
        """,
        model_id,
        tenant_id,
        cause_id,
        cause_type,
        json.dumps(previous_state, default=str) if previous_state is not None else None,
        json.dumps(new_state, default=str),
        fields_arr,
        re_asserts_event_id,
        sources_arr,
    )
    assert row is not None
    return int(row["event_id"])


# =====================================================================
# emit_reconciliation_merge_audit — convenience wrapper.
# =====================================================================


async def emit_reconciliation_merge_audit(
    conn: asyncpg.Connection,
    *,
    merged_model_id: UUID,
    source_model_ids: Sequence[UUID],
    tenant_id: UUID,
    new_state: dict[str, Any],
    previous_state: dict[str, Any] | None = None,
    cause_id: UUID | None = None,
    changed_fields: Sequence[str] | None = None,
) -> int:
    """
    Emit a `reconciliation_merge` audit event on `merged_model_id`.

    `source_model_ids` is the set of Models that were folded INTO
    `merged_model_id`. For PR 1 single-pass auto_merge (where no source
    Model is ever created), pass an empty list — the merge event still
    records the rule but `get_audit_chain` will not walk into other
    chains. For PR 4 second-pass merges where two Models existed and
    one is being absorbed, populate with the absorbed Model IDs;
    `get_audit_chain` will then return the union of all source chains.
    """
    return await emit_audit_event(
        conn,
        model_id=merged_model_id,
        tenant_id=tenant_id,
        cause_type=CAUSE_RECONCILIATION_MERGE,
        new_state=new_state,
        previous_state=previous_state,
        cause_id=cause_id,
        changed_fields=changed_fields,
        source_model_ids=source_model_ids,
    )


# =====================================================================
# find_re_assertable_event — reversal-of-reversal detection.
# =====================================================================


async def find_re_assertable_event(
    conn: asyncpg.Connection,
    model_id: UUID,
    new_state: dict[str, Any],
) -> int | None:
    """
    Find the earliest prior audit event on `model_id` whose `new_state`
    contains all the key/value pairs of the given `new_state`. Returns
    the event_id of the first match, or None if no prior event matches.

    Comparison uses JSONB containment (`@>`). The caller's `new_state`
    is typically a partial snapshot (just the changed fields); we look
    for prior events whose stored snapshot — which may be a full
    snapshot for a `create` event or a partial for an update — contains
    every key/value being asserted now. This handles both A → B → A
    oscillation across full and partial snapshots.

    Used by `emit_audit_event` to populate `re_asserts_event_id`. If
    the new event re-asserts values that already appeared, the linkage
    points back to the earliest such occurrence — preserving the
    "we're back where we started" rhetorical shape per
    SUBSTRATE_SEMANTICS.md Q5.
    """
    row = await conn.fetchrow(
        """
        SELECT event_id
        FROM audit_events
        WHERE model_id = $1 AND new_state @> $2::jsonb
        ORDER BY occurred_at ASC, event_id ASC
        LIMIT 1
        """,
        model_id,
        json.dumps(new_state, default=str),
    )
    if row is None:
        return None
    return int(row["event_id"])


# =====================================================================
# get_audit_chain — full ordered history.
# =====================================================================


async def get_audit_chain(
    conn: asyncpg.Connection,
    model_id: UUID,
    *,
    include_merged_sources: bool = True,
) -> list[AuditEvent]:
    """
    Return the full audit chain for `model_id`, ordered by occurred_at
    ascending.

    If `include_merged_sources` is True (default), any
    cause_type='reconciliation_merge' event in the chain causes the
    chains of its source_model_ids to be unioned in (transitively, so
    A merged into B and B merged into C surfaces all three chains
    when querying C). The merge event itself appears once in the
    unified chain.

    If False, only the rows whose model_id matches exactly are
    returned.

    Returns an empty list if the Model has no audit events.

    Per SUBSTRATE_SEMANTICS.md OQ9 resolution: unbounded — paging is a
    gateway-layer concern, not substrate.
    """
    if include_merged_sources:
        rows = await conn.fetch(
            """
            WITH RECURSIVE chain_models(model_id) AS (
              SELECT $1::uuid
              UNION
              SELECT unnest(ae.source_model_ids)
                FROM audit_events ae
                JOIN chain_models cm ON ae.model_id = cm.model_id
                WHERE ae.cause_type = 'reconciliation_merge'
                  AND cardinality(ae.source_model_ids) > 0
            )
            SELECT
              event_id, model_id, tenant_id, occurred_at,
              cause_id, cause_type,
              previous_state, new_state,
              changed_fields, re_asserts_event_id, source_model_ids
            FROM audit_events
            WHERE model_id IN (SELECT model_id FROM chain_models)
            ORDER BY occurred_at ASC, event_id ASC
            """,
            model_id,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT
              event_id, model_id, tenant_id, occurred_at,
              cause_id, cause_type,
              previous_state, new_state,
              changed_fields, re_asserts_event_id, source_model_ids
            FROM audit_events
            WHERE model_id = $1
            ORDER BY occurred_at ASC, event_id ASC
            """,
            model_id,
        )

    return [_hydrate_event(r) for r in rows]


def _hydrate_event(row: asyncpg.Record) -> AuditEvent:
    raw = dict(row)

    # JSONB columns may arrive as str/bytes depending on codec settings;
    # decode to dict.
    for key in ("previous_state", "new_state"):
        v = raw.get(key)
        if isinstance(v, (bytes, bytearray)):
            v = v.decode()
        if isinstance(v, str):
            try:
                raw[key] = json.loads(v)
            except json.JSONDecodeError:
                pass

    return AuditEvent(
        event_id=int(raw["event_id"]),
        model_id=raw["model_id"],
        tenant_id=raw["tenant_id"],
        occurred_at=raw["occurred_at"],
        cause_type=raw["cause_type"],
        new_state=raw["new_state"] or {},
        previous_state=raw.get("previous_state"),
        cause_id=raw.get("cause_id"),
        changed_fields=list(raw.get("changed_fields") or []),
        re_asserts_event_id=(
            int(raw["re_asserts_event_id"])
            if raw.get("re_asserts_event_id") is not None
            else None
        ),
        source_model_ids=list(raw.get("source_model_ids") or []),
    )


# =====================================================================
# changed_fields helper — diff two snapshots.
# =====================================================================


def diff_changed_fields(
    previous_state: dict[str, Any] | None,
    new_state: dict[str, Any],
) -> list[str]:
    """
    Return the sorted list of keys whose value differs between
    previous_state and new_state. If previous_state is None (create),
    every key in new_state is reported.

    Used at audit emission time to populate the changed_fields column.
    """
    if previous_state is None:
        return sorted(new_state.keys())
    keys = set(previous_state.keys()) | set(new_state.keys())
    diff: list[str] = []
    for k in keys:
        if previous_state.get(k) != new_state.get(k):
            diff.append(k)
    return sorted(diff)


__all__ = [
    "AuditEvent",
    "CAUSE_CREATE",
    "CAUSE_ARCHIVE",
    "CAUSE_FIELD_UPDATE",
    "CAUSE_CONFIDENCE_UPDATE",
    "CAUSE_RECONCILIATION_MERGE",
    "emit_audit_event",
    "emit_reconciliation_merge_audit",
    "find_re_assertable_event",
    "get_audit_chain",
    "model_state_snapshot",
    "diff_changed_fields",
]
