"""services/reasoning/think/region_locks.py — Postgres advisory-lock region
serialization per Wave 2→3 amendment W3.Q4.

Mechanism: `pg_advisory_xact_lock(tenant_hash, entity_hash)` inside
Think's apply transaction. The `_xact_` variant auto-releases at
COMMIT or ROLLBACK, including on connection drop / worker crash.

Confusion with the session-scoped `pg_advisory_lock` variant is the
#1 source of "why is Think hung" bugs — this module exports ONLY the
transaction-scoped API.

Region key computation is verbatim from SCHEMA-LOCK.md W3.Q4 so it is
stable across languages and implementations.
"""
from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


# ---------------------------------------------------------------------
# TK-4 — deterministic `primary_entity_id` selection.
#
# THINK-DESIGN-AUDIT §7 argument 1 flagged that the T1 region key
# `(tenant_id, actor_id, primary_entity_id)` had no definition for
# "primary" — two equivalent triggers could hash to different keys if
# the LLM emitted `entities_mentioned` in a different order. Fix:
# define precedence deterministically so order doesn't matter.
#
# Precedence: commitment > goal > decision > resource/customer > actor.
# Ties broken by `id` ascending (string compare — UUIDs are already
# sortable as hex). Returns (type, id) as a two-tuple, or None if the
# input is empty.
#
# Callers (`compute_region_key_t1`, below) wire this into the advisory
# lock so two triggers whose `entities_mentioned` carry the same
# entities in different orders contend on the same lock key.
# ---------------------------------------------------------------------

ENTITY_TYPE_PRECEDENCE: dict[str, int] = {
    "commitment": 0,
    "goal": 1,
    "decision": 2,
    "resource": 3,
    "customer": 3,  # treated as same tier as resource
    "customer_resource": 3,
    "actor": 4,
}

_MODEL_REF_SCALAR_KEYS = {
    "model_id",
    "source_model_id",
    "target_model_id",
    "matched_model_id",
    "reconcile_matched_model_id",
    "canonical_model_id",
    "merged_into_model_id",
}

_MODEL_REF_LIST_KEYS = {
    "model_ids",
    "member_model_ids",
    "supporting_model_ids",
    "contributing_models",
    "evidence_model_ids",
    "counterevidence_model_ids",
    "source_model_ids",
    "target_model_ids",
    "candidate_model_ids",
    "seed_model_ids",
    "selected_model_ids",
    "dropped_model_ids",
    "graph_selected_model_ids",
    "applied_model_ids",
    "subject_model_ids",
}

_SOURCE_REF_KEYS = {
    "source_ref",
    "raw_content_ref",
    "source_ref_id",
}


def compute_primary_entity(
    entities_mentioned: list[dict] | None,
) -> tuple[str, str] | None:
    """
    Pick the deterministic "primary" entity from a list of
    `{"type": ..., "id": ...}` dicts.

    Sort key: (type precedence, id-as-str ascending).

    Unknown types fall to precedence 99 (sorted last). Entities missing
    either `type` or `id` are skipped. Returns None if the effective
    list is empty.
    """
    if not entities_mentioned:
        return None
    filtered = [
        e for e in entities_mentioned
        if isinstance(e, dict) and e.get("type") and e.get("id") is not None
    ]
    if not filtered:
        return None
    sorted_entities = sorted(
        filtered,
        key=lambda e: (
            ENTITY_TYPE_PRECEDENCE.get(str(e["type"]), 99),
            str(e["id"]),
        ),
    )
    top = sorted_entities[0]
    return (str(top["type"]), str(top["id"]))


def compute_region_key_t1(trigger: Any) -> tuple[Any, ...]:
    """
    T1 region key tuple, derived from a TriggerContext-like object.

    Returns `(tenant_id, actor_id, primary_type, primary_id)` when a
    primary entity is present, or `(tenant_id, actor_id, 'no_entity')`
    as a stable fallback.

    "actor_id" defaults to `trigger.scope_actors[0]` if the object
    doesn't expose an explicit `actor_id` attribute. Used for
    advisory-lock key parity across workers — two triggers whose
    entities_mentioned lists differ only in order MUST yield the same
    tuple.
    """
    tenant_id = getattr(trigger, "tenant_id", None)
    actor_id = getattr(trigger, "actor_id", None)
    if actor_id is None:
        scope_actors = getattr(trigger, "scope_actors", None) or []
        if scope_actors:
            actor_id = scope_actors[0]
    entities = getattr(trigger, "entities_mentioned", None)
    if entities is None:
        entities = getattr(trigger, "seed_entity_ids", None)
    primary = compute_primary_entity(entities)
    if primary is None:
        return (tenant_id, actor_id, "no_entity")
    return (tenant_id, actor_id, primary[0], primary[1])


# ---------------------------------------------------------------------
# Region key computation — pure function, verbatim from SCHEMA-LOCK W3.Q4
# ---------------------------------------------------------------------


def _hash_int32(s: str) -> int:
    """
    Stable signed 32-bit int from SHA-256 first 4 bytes (big-endian).

    Signed because `pg_advisory_xact_lock(int, int)` takes two INT4s
    (signed). Using an unsigned top-bit would either wrap silently or
    overflow the asyncpg codec.
    """
    digest = hashlib.sha256(s.encode()).digest()[:4]
    return int.from_bytes(digest, "big", signed=True)


def region_lock_key(
    tenant_id: UUID | str,
    entity_ids: list[tuple[str, UUID | str]],
) -> tuple[int, int]:
    """
    Return `(tenant_hash, entity_hash)` for `pg_advisory_xact_lock`.

    Per SCHEMA-LOCK W3.Q4:
      sorted_entities = sorted(entity_ids)
      entity_hash  = hash_int32(canonical_json(sorted_entities))
      tenant_hash  = hash_int32(str(tenant_id))

    `entity_ids` is a list of `(type_str, id)` tuples. We stringify the
    UUIDs so canonical_json is deterministic and type-agnostic. The
    sort is lexicographic on the stringified tuples.

    IMPORTANT: This function MUST stay pure + deterministic. Two calls
    with the same input must produce identical output — we rely on
    that for cross-worker serialization.
    """
    stringified = sorted(
        [(t, str(i)) for (t, i) in entity_ids]
    )
    entity_hash = _hash_int32(
        json.dumps(stringified, separators=(",", ":"))
    )
    tenant_hash = _hash_int32(str(tenant_id))
    return (tenant_hash, entity_hash)


# ---------------------------------------------------------------------
# Advisory-lock acquisition (for use inside Think's apply transaction)
# ---------------------------------------------------------------------


@dataclass
class RegionLockAcquisition:
    """
    Observability payload produced by `acquire_region_lock`. The caller
    keeps one of these around and hands it to `observability.py`'s
    post-commit log writer.
    """

    tenant_hash: int
    entity_hash: int
    entity_ids: list[tuple[str, str]]
    requested_at: float
    acquired_at: float

    @property
    def wait_duration_ms(self) -> int:
        return max(0, int((self.acquired_at - self.requested_at) * 1000))


async def acquire_region_lock(
    conn: asyncpg.Connection,
    tenant_id: UUID | str,
    entity_ids: list[tuple[str, UUID | str]],
) -> RegionLockAcquisition:
    """
    Acquire a transaction-scoped advisory lock on (tenant, entities).

    MUST be called inside an open transaction on `conn`. The lock
    releases automatically when the transaction COMMITs or ROLLBACKs —
    do not call any "release" function.

    Returns a RegionLockAcquisition with timing data. On contention
    the call blocks; the caller's outer transaction has whatever
    timeout it set (we don't add our own here — Think's worker loop
    is responsible for overall LockTimeout policy).
    """
    th, eh = region_lock_key(tenant_id, entity_ids)
    requested_at = time.monotonic()
    await conn.execute(
        "SELECT pg_advisory_xact_lock($1::int, $2::int)",
        th,
        eh,
    )
    acquired_at = time.monotonic()
    return RegionLockAcquisition(
        tenant_hash=th,
        entity_hash=eh,
        entity_ids=[(t, str(i)) for (t, i) in entity_ids],
        requested_at=requested_at,
        acquired_at=acquired_at,
    )


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def touched_entity_ids_from_diff(
    diff: Any,
) -> list[tuple[str, str]]:
    """
    Extract (type, id) entity pairs from a ValidatedDiff for region-lock
    keying. Best-effort: a Model being inserted has no id yet, so we
    derive its region from declared scope (scope_actors + scope_entities)
    instead. Two diffs with identical scope contend on the same advisory
    lock and serialize.

    Used by `apply_diff` to acquire its own region lock — direct callers
    (harness, scripts, future entry points) get serialization without
    having to remember to lock first. The reason.py path also acquires
    its own (broader) retrieval-region lock; the two locks coexist
    because pg_advisory_xact_lock is keyed by tuple, and both release at
    transaction commit.
    """
    entities: set[tuple[str, str]] = set()

    for op in (getattr(diff, "claim_ops", []) or []):
        if getattr(op, "op", None) == "insert":
            entry = getattr(op, "entry", None) or {}
            for a in (entry.get("scope_actors") or []):
                entities.add(("actor", str(a)))
            for e in (entry.get("scope_entities") or []):
                if isinstance(e, dict):
                    et = e.get("type")
                    eid = e.get("id")
                    if et and eid:
                        entities.add((str(et), str(eid)))
        else:
            mid = getattr(op, "model_id", None)
            if mid is not None:
                entities.add(("model", str(mid)))

    for op in (getattr(diff, "edge_ops", []) or []):
        source_id = getattr(op, "source_model_id", None)
        target_id = getattr(op, "target_model_id", None)
        if source_id is not None:
            entities.add(("model", str(source_id)))
        if target_id is not None:
            entities.add(("model", str(target_id)))

    for op in (getattr(diff, "act_ops", []) or []):
        ent = getattr(op, "entity", None) or {}
        op_kind = getattr(op, "op", "") or ""
        for key, kind in (
            ("commitment_id", "commitment"),
            ("goal_id", "goal"),
            ("decision_id", "decision"),
        ):
            v = ent.get(key)
            if v is not None:
                entities.add((kind, str(v)))
        v = ent.get("id")
        if v is not None:
            if "commitment" in op_kind:
                kind = "commitment"
            elif "goal" in op_kind:
                kind = "goal"
            elif "decision" in op_kind:
                kind = "decision"
            else:
                kind = "act"
            entities.add((kind, str(v)))

    for op in (getattr(diff, "resource_ops", []) or []):
        rid = getattr(op, "resource_id", None)
        if rid is not None:
            entities.add(("resource", str(rid)))
        cid = getattr(op, "commitment_id", None)
        if cid is not None:
            entities.add(("commitment", str(cid)))

    return sorted(entities)


def touched_entity_ids(
    retrieval_like: Any,
) -> list[tuple[str, str]]:
    """
    Extract the touched entity set from a RetrievalResult (or any
    object with .models / .acts / .resources attrs).

    The region is computed pre-LLM from the retrieval output per W3.Q4.
    Validation later rejects diffs that touch entities outside this
    set (see validator.py `_reject_out_of_region`).
    """
    entities: set[tuple[str, str]] = set()

    # Models contribute (model, id) AND their scoped entities.
    models = getattr(retrieval_like, "models", []) or []
    for m in models:
        mid = getattr(m, "id", None)
        if mid is not None:
            entities.add(("model", str(mid)))
        for attr in (
            "supporting_model_ids",
            "contributing_models",
            "member_model_ids",
        ):
            _add_model_refs(entities, getattr(m, attr, None))
        for attr in (
            "proposition",
            "falsifier",
            "signal_readings",
            "resolution_criteria",
        ):
            _add_structured_model_refs(entities, getattr(m, attr, None))
        # Include scope_entities verbatim so the lock covers the
        # entities the Model speaks about.
        for e in (getattr(m, "scope_entities", []) or []):
            if isinstance(e, dict):
                _add_entity_ref(entities, e.get("type"), e.get("id"))

    # Selected observations are visible context too. When the LLM inserts
    # a new claim scoped to an entity mentioned only in the evidence row,
    # region containment should allow normal validation to decide whether
    # the operation is good instead of forcing a full Think retry.
    for obs in getattr(retrieval_like, "observations", []) or []:
        _add_entity_refs(entities, getattr(obs, "entities_mentioned", None))

    acts = getattr(retrieval_like, "acts", None) or {}
    for g in acts.get("goals", []) or []:
        gid = getattr(g, "id", None)
        if gid is not None:
            entities.add(("goal", str(gid)))
    for c in acts.get("commitments", []) or []:
        cid = getattr(c, "id", None)
        if cid is not None:
            entities.add(("commitment", str(cid)))
    for d in acts.get("decisions", []) or []:
        did = getattr(d, "id", None)
        if did is not None:
            entities.add(("decision", str(did)))

    for r in getattr(retrieval_like, "resources", []) or []:
        rid = getattr(r, "id", None)
        if rid is not None:
            _add_entity_ref(entities, "resource", rid)
            if str(getattr(r, "kind", "")) == "relational":
                _add_entity_ref(entities, "customer_resource", rid)

    # Trigger seed entities also belong in the region (the trigger
    # may point at an entity not yet retrieved via pathway A).
    trigger = getattr(retrieval_like, "trigger", None)
    if trigger is not None:
        for e in (getattr(trigger, "seed_entity_ids", []) or []):
            if isinstance(e, dict):
                _add_entity_ref(entities, e.get("type"), e.get("id"))
        _add_entity_refs(entities, getattr(trigger, "entities_mentioned", None))
        mid = getattr(trigger, "model_id", None)
        if mid is not None:
            entities.add(("model", str(mid)))
        oid = getattr(trigger, "observation_id", None)
        if oid is not None:
            entities.add(("observation", str(oid)))

    _add_inquiry_packet_refs(entities, getattr(retrieval_like, "notes", None))

    return sorted(entities)


def _add_entity_refs(
    entities: set[tuple[str, str]],
    refs: Any,
) -> None:
    if not isinstance(refs, list):
        return
    for raw in refs:
        if not isinstance(raw, dict):
            continue
        _add_entity_ref(entities, raw.get("type"), raw.get("id"))


def _add_entity_ref(
    entities: set[tuple[str, str]],
    entity_type: Any,
    entity_id: Any,
) -> None:
    if not entity_type or not entity_id:
        return
    kind = str(entity_type)
    ident = str(entity_id)
    entities.add((kind, ident))
    # Customer resources are relational Resource rows, but the codebase
    # uses all three labels in different layers. Treat them as aliases for
    # region containment so visible customer evidence does not cause a
    # wasteful out-of-region retry.
    if kind in {"customer", "customer_resource"}:
        entities.add(("customer", ident))
        entities.add(("customer_resource", ident))
        entities.add(("resource", ident))


def _add_source_ref(
    entities: set[tuple[str, str]],
    value: Any,
    *,
    source_type: Any = None,
) -> None:
    if not value:
        return
    text = str(value)
    if ":" in text:
        kind, ident = text.split(":", 1)
        if kind and ident:
            _add_entity_ref(entities, kind, ident)
            if kind == "resource":
                _add_entity_ref(entities, "customer_resource", ident)
        return
    if source_type:
        _add_entity_ref(entities, source_type, text)
        if str(source_type) == "resource":
            _add_entity_ref(entities, "customer_resource", text)


def _add_model_refs(
    entities: set[tuple[str, str]],
    refs: Any,
) -> None:
    if not isinstance(refs, (list, tuple, set)):
        _add_model_ref(entities, refs)
        return
    for value in refs:
        _add_model_ref(entities, value)


def _add_model_ref(
    entities: set[tuple[str, str]],
    value: Any,
) -> None:
    if value is None:
        return
    if isinstance(value, UUID):
        entities.add(("model", str(value)))
        return
    text = str(value).strip()
    if not text:
        return
    if text.startswith("model:"):
        text = text.split(":", 1)[1]
    try:
        UUID(text)
    except (TypeError, ValueError):
        return
    entities.add(("model", text))


def _add_structured_model_refs(
    entities: set[tuple[str, str]],
    value: Any,
) -> None:
    if isinstance(value, dict):
        source_type = value.get("source_type")
        for key, raw in value.items():
            key_text = str(key)
            if (
                key_text in _MODEL_REF_SCALAR_KEYS
                or key_text.endswith("_model_id")
            ):
                _add_model_ref(entities, raw)
            elif (
                key_text in _MODEL_REF_LIST_KEYS
                or key_text.endswith("_model_ids")
            ):
                _add_model_refs(entities, raw)
            elif key_text in _SOURCE_REF_KEYS:
                if isinstance(raw, list):
                    for item in raw:
                        _add_source_ref(
                            entities,
                            item,
                            source_type=source_type,
                        )
                else:
                    _add_source_ref(entities, raw, source_type=source_type)
            if isinstance(raw, (dict, list)):
                _add_structured_model_refs(entities, raw)
        return
    if isinstance(value, list):
        for item in value:
            _add_structured_model_refs(entities, item)


def _add_inquiry_packet_refs(
    entities: set[tuple[str, str]],
    notes: Any,
) -> None:
    if not isinstance(notes, dict):
        return

    _add_structured_model_refs(entities, notes)

    packets: list[dict[str, Any]] = []
    packet = notes.get("inquiry_context_packet")
    if isinstance(packet, dict):
        packets.append(packet)
    inquiry = notes.get("inquiry")
    if isinstance(inquiry, dict) and isinstance(inquiry.get("context_packet"), dict):
        packets.append(inquiry["context_packet"])

    for packet in packets:
        _add_entity_refs(entities, packet.get("resolved_entities"))

        metadata = packet.get("source_metadata")
        if isinstance(metadata, dict):
            _add_source_ref(
                entities,
                metadata.get("model_id"),
                source_type="model",
            )
            _add_source_ref(
                entities,
                metadata.get("observation_id"),
                source_type="observation",
            )

        tiers = packet.get("tiers")
        if not isinstance(tiers, dict):
            continue

        for item in tiers.get("decisive_evidence") or []:
            if not isinstance(item, dict):
                continue
            source_type = item.get("source_type")
            for key in ("source_ref", "raw_content_ref", "source_ref_id"):
                _add_source_ref(entities, item.get(key), source_type=source_type)

        for group in tiers.get("supporting_evidence_groups") or []:
            if not isinstance(group, dict):
                continue
            for ref in group.get("source_refs") or []:
                _add_source_ref(entities, ref)

        for item in tiers.get("omission_ledger") or []:
            if isinstance(item, dict):
                _add_source_ref(entities, item.get("source_ref"))


__all__ = [
    "region_lock_key",
    "acquire_region_lock",
    "touched_entity_ids",
    "touched_entity_ids_from_diff",
    "RegionLockAcquisition",
    "compute_primary_entity",
    "compute_region_key_t1",
    "ENTITY_TYPE_PRECEDENCE",
]
