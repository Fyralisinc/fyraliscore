"""Deterministic mutation compiler for Think diffs.

Think should interpret company signals. This module keeps the operational
commit decision deterministic: resolve canonical Model identity, bind legal
operation prerequisites, and downgrade graph writes that cannot be safely
projected before the validator/applier pay for failed mutations.
"""
from __future__ import annotations

import os
import re
from dataclasses import asdict, dataclass, field
from typing import Any, Mapping
from uuid import UUID

import asyncpg

from services.domain.models.address import build_belief_address
from services.reasoning.relationships.ontology_runtime import resolve_edge_kind_spec

from .diff_schema import (
    ActOp,
    ClaimOp,
    EdgeOp,
    MemoryLifecycleOp,
    RawDiff,
    RelationClaimOp,
)


_WS_RE = re.compile(r"\s+")
_BASIS_EXEMPT_ACT_OPS = frozenset({"update_goal_health", "update_goal", "create_goal"})
_MIN_DUPLICATE_NATURAL_LEN = 12
_CYCLE_GUARD = "mutation_compiler_cycle_guard"


@dataclass(frozen=True)
class MutationCompileSummary:
    duplicate_inserts_rewritten: int = 0
    model_refs_remapped: int = 0
    act_ops_bound_confidence_basis: int = 0
    edge_ops_dropped_self_edge: int = 0
    edge_ops_downgraded_for_cycle: int = 0
    relation_claims_dropped_self_edge: int = 0
    relation_claims_downgraded_for_cycle: int = 0
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def changed(self) -> bool:
        return any(
            (
                self.duplicate_inserts_rewritten,
                self.model_refs_remapped,
                self.act_ops_bound_confidence_basis,
                self.edge_ops_dropped_self_edge,
                self.edge_ops_downgraded_for_cycle,
                self.relation_claims_dropped_self_edge,
                self.relation_claims_downgraded_for_cycle,
            )
        )

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class _MutableSummary:
    duplicate_inserts_rewritten: int = 0
    model_refs_remapped: int = 0
    act_ops_bound_confidence_basis: int = 0
    edge_ops_dropped_self_edge: int = 0
    edge_ops_downgraded_for_cycle: int = 0
    relation_claims_dropped_self_edge: int = 0
    relation_claims_downgraded_for_cycle: int = 0
    notes: list[str] = field(default_factory=list)

    def freeze(self) -> MutationCompileSummary:
        return MutationCompileSummary(
            duplicate_inserts_rewritten=self.duplicate_inserts_rewritten,
            model_refs_remapped=self.model_refs_remapped,
            act_ops_bound_confidence_basis=self.act_ops_bound_confidence_basis,
            edge_ops_dropped_self_edge=self.edge_ops_dropped_self_edge,
            edge_ops_downgraded_for_cycle=self.edge_ops_downgraded_for_cycle,
            relation_claims_dropped_self_edge=self.relation_claims_dropped_self_edge,
            relation_claims_downgraded_for_cycle=(
                self.relation_claims_downgraded_for_cycle
            ),
            notes=tuple(self.notes[:20]),
        )


def mutation_compiler_enabled() -> bool:
    return os.environ.get("THINK_MUTATION_COMPILER", "1").strip().lower() in {
        "1",
        "on",
        "true",
        "yes",
    }


async def compile_raw_diff_mutations(
    diff: RawDiff,
    *,
    conn: asyncpg.Connection,
    retrieval_result: Any,
    bundle: Any,
) -> tuple[RawDiff, MutationCompileSummary]:
    """Compile a raw LLM/deterministic diff into canonical mutation intent.

    The compiler is deliberately conservative. It rewrites only when an existing
    active Model can be proven by belief fingerprint or exact normalized natural
    text, and it downgrades graph projections only when the same deterministic
    graph legality rule used by the validator says the edge cannot be accepted.
    """

    if not mutation_compiler_enabled():
        return diff, MutationCompileSummary()

    summary = _MutableSummary()
    claim_ops, lifecycle_ops, placeholder_map = await _compile_claim_ops(
        diff,
        conn=conn,
        summary=summary,
    )
    if placeholder_map:
        diff = _remap_model_references(diff, placeholder_map, summary=summary)

    pending_basis_ids = _pending_claim_placeholder_ids_from_ops(claim_ops)
    act_ops = await _bind_act_confidence_basis(
        diff.act_ops,
        conn=conn,
        tenant_id=diff.tenant_id,
        retrieval_result=retrieval_result,
        bundle=bundle,
        pending_basis_ids=pending_basis_ids,
        preferred_model_ids=[
            op.model_id for op in lifecycle_ops if op.action in {"confirm", "revise"}
        ],
        summary=summary,
    )
    edge_ops, relation_ops_from_edges = await _compile_edge_ops(
        diff.edge_ops,
        conn=conn,
        tenant_id=diff.tenant_id,
        summary=summary,
    )
    relation_claim_ops = await _compile_relation_claim_ops(
        [*diff.relation_claim_ops, *relation_ops_from_edges],
        conn=conn,
        tenant_id=diff.tenant_id,
        summary=summary,
    )

    frozen = summary.freeze()
    compiled = diff.model_copy(
        update={
            "claim_ops": claim_ops,
            "memory_lifecycle_ops": [*diff.memory_lifecycle_ops, *lifecycle_ops],
            "relation_claim_ops": relation_claim_ops,
            "edge_ops": edge_ops,
            "act_ops": act_ops,
            "reasoning_trace": _append_compile_trace(diff.reasoning_trace, frozen),
        }
    )
    return compiled, frozen


async def _compile_claim_ops(
    diff: RawDiff,
    *,
    conn: asyncpg.Connection,
    summary: _MutableSummary,
) -> tuple[list[ClaimOp], list[MemoryLifecycleOp], dict[UUID, UUID]]:
    kept: list[ClaimOp] = []
    lifecycle_ops: list[MemoryLifecycleOp] = []
    placeholder_map: dict[UUID, UUID] = {}
    belief_table_exists = await _table_exists(conn, "model_belief_addresses")

    for op in diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            kept.append(op)
            continue
        canonical = await _find_canonical_model_for_insert(
            conn,
            tenant_id=diff.tenant_id,
            entry=op.entry,
            belief_table_exists=belief_table_exists,
        )
        if canonical is None:
            kept.append(op)
            continue
        evidence_event_ids = _event_ids_from_entry(op.entry)
        evidence_model_ids = _model_ids_from_entry(op.entry)
        if not evidence_event_ids and not evidence_model_ids:
            kept.append(op)
            continue

        canonical_id = canonical["id"]
        lifecycle_ops.append(
            MemoryLifecycleOp(
                model_id=canonical_id,
                action="confirm",
                evidence_event_ids=evidence_event_ids,
                claim_local_evidence_event_ids=evidence_event_ids,
                evidence_model_ids=evidence_model_ids,
                rationale=(
                    "Canonical mutation compiler rewrote a duplicate Model "
                    "insert into confirmation of the existing belief."
                ),
                metadata={
                    "source": "mutation_compiler",
                    "reason": "duplicate_model_insert",
                    "canonical_model_id": str(canonical_id),
                    "duplicate_match": canonical.get("match"),
                    "duplicate_fingerprint": canonical.get("fingerprint"),
                    "duplicate_natural": str(op.entry.get("natural") or "")[:500],
                },
            )
        )
        for placeholder_id in _placeholder_ids_for_insert(op.entry):
            placeholder_map[placeholder_id] = canonical_id
        summary.duplicate_inserts_rewritten += 1
        summary.notes.append(f"duplicate_insert->{canonical_id}")

    return kept, lifecycle_ops, placeholder_map


async def _find_canonical_model_for_insert(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    entry: Mapping[str, Any],
    belief_table_exists: bool,
) -> dict[str, Any] | None:
    fingerprint = _safe_belief_fingerprint(entry)
    if fingerprint and belief_table_exists:
        row = await conn.fetchrow(
            """
            SELECT m.id, m.confidence
            FROM model_belief_addresses mba
            JOIN models m ON m.id = mba.model_id
            WHERE mba.tenant_id = $1
              AND mba.fingerprint = $2
              AND mba.status = 'active'
              AND m.status = 'active'
            ORDER BY m.created_at ASC, m.id ASC
            LIMIT 1
            """,
            tenant_id,
            fingerprint,
        )
        if row is not None:
            return {
                "id": row["id"],
                "confidence": row["confidence"],
                "match": "belief_fingerprint",
                "fingerprint": fingerprint,
            }

    normalized_natural = _normalize_natural(entry.get("natural"))
    if len(normalized_natural) < _MIN_DUPLICATE_NATURAL_LEN:
        return None
    row = await conn.fetchrow(
        """
        SELECT id, confidence
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND lower(regexp_replace(trim("natural"), '\\s+', ' ', 'g')) = $2
        ORDER BY created_at ASC, id ASC
        LIMIT 1
        """,
        tenant_id,
        normalized_natural,
    )
    if row is None:
        return None
    return {
        "id": row["id"],
        "confidence": row["confidence"],
        "match": "exact_natural",
        "fingerprint": fingerprint,
    }


def _safe_belief_fingerprint(entry: Mapping[str, Any]) -> str | None:
    prop = entry.get("proposition")
    if not isinstance(prop, Mapping):
        return None
    for key in ("belief_address", "semantic_address"):
        address = prop.get(key)
        if isinstance(address, Mapping) and _address_has_identity(address):
            fingerprint = str(address.get("fingerprint") or "").strip()
            if fingerprint:
                return fingerprint
    try:
        address = build_belief_address(
            prop,
            natural=str(entry.get("natural") or ""),
            scope_entities=_mapping_sequence(entry.get("scope_entities")),
        )
    except Exception:  # noqa: BLE001 - malformed claims stay validator-owned
        return None
    if not _address_has_identity(address):
        return None
    fingerprint = str(address.get("fingerprint") or "").strip()
    return fingerprint or None


def _address_has_identity(address: Mapping[str, Any]) -> bool:
    identity_text = " ".join(
        str(address.get(key) or "") for key in ("subject", "object", "qualifier")
    )
    return len(_normalize_natural(identity_text)) >= _MIN_DUPLICATE_NATURAL_LEN


def _mapping_sequence(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, list):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _normalize_natural(value: Any) -> str:
    return _WS_RE.sub(" ", str(value or "").strip().casefold())


def _event_ids_from_entry(entry: Mapping[str, Any]) -> list[UUID]:
    values: list[Any] = [entry.get("born_from_event_id")]
    values.extend(entry.get("supporting_event_ids") or [])
    return _dedupe_uuids(values)


def _model_ids_from_entry(entry: Mapping[str, Any]) -> list[UUID]:
    values: list[Any] = []
    values.extend(entry.get("supporting_model_ids") or [])
    values.extend(entry.get("contributing_models") or [])
    return _dedupe_uuids(values)


def _placeholder_ids_for_insert(entry: Mapping[str, Any]) -> list[UUID]:
    return _dedupe_uuids(entry.get(key) for key in ("id", "model_id", "born_from_event_id"))


def _pending_claim_placeholder_ids_from_ops(claim_ops: list[ClaimOp]) -> set[UUID]:
    ids: set[UUID] = set()
    for op in claim_ops:
        if op.op == "insert" and isinstance(op.entry, dict):
            ids.update(_placeholder_ids_for_insert(op.entry))
    return ids


def _remap_model_references(
    diff: RawDiff,
    placeholder_map: Mapping[UUID, UUID],
    *,
    summary: _MutableSummary,
) -> RawDiff:
    relation_claim_ops: list[RelationClaimOp] = []
    edge_ops: list[EdgeOp] = []
    act_ops: list[ActOp] = []
    remapped = 0

    for op in diff.relation_claim_ops:
        update: dict[str, Any] = {}
        for key in ("source_model_id", "target_model_id"):
            current = getattr(op, key)
            mapped = _remap_uuid(current, placeholder_map)
            if mapped != current:
                update[key] = mapped
                remapped += 1
        for key in ("subject_ref", "object_ref"):
            mapped_ref, changed = _remap_model_ref_dict(getattr(op, key), placeholder_map)
            if changed:
                update[key] = mapped_ref
                remapped += 1
        mapped_evidence = _remap_uuid_list(op.evidence_model_ids, placeholder_map)
        if mapped_evidence != op.evidence_model_ids:
            update["evidence_model_ids"] = mapped_evidence
            remapped += 1
        relation_claim_ops.append(op.model_copy(update=update) if update else op)

    for op in diff.edge_ops:
        update = {}
        for key in ("source_model_id", "target_model_id"):
            current = getattr(op, key)
            mapped = _remap_uuid(current, placeholder_map)
            if mapped != current:
                update[key] = mapped
                remapped += 1
        mapped_evidence = _remap_uuid_list(op.evidence_model_ids, placeholder_map)
        if mapped_evidence != op.evidence_model_ids:
            update["evidence_model_ids"] = mapped_evidence
            remapped += 1
        edge_ops.append(op.model_copy(update=update) if update else op)

    for op in diff.act_ops:
        mapped = _remap_uuid(op.confidence_basis, placeholder_map)
        if mapped != op.confidence_basis:
            act_ops.append(op.model_copy(update={"confidence_basis": mapped}))
            remapped += 1
        else:
            act_ops.append(op)

    if remapped:
        summary.model_refs_remapped += remapped
    return diff.model_copy(
        update={
            "relation_claim_ops": relation_claim_ops,
            "edge_ops": edge_ops,
            "act_ops": act_ops,
        }
    )


async def _bind_act_confidence_basis(
    act_ops: list[ActOp],
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    retrieval_result: Any,
    bundle: Any,
    pending_basis_ids: set[UUID],
    preferred_model_ids: list[UUID | None],
    summary: _MutableSummary,
) -> list[ActOp]:
    if not act_ops:
        return []
    candidate_ids = _basis_candidate_ids(
        retrieval_result=retrieval_result,
        bundle=bundle,
        preferred_model_ids=preferred_model_ids,
    )
    existing_candidates = await _existing_active_model_ids(conn, tenant_id, candidate_ids)
    fallback_basis = next((mid for mid in candidate_ids if mid in existing_candidates), None)

    current_basis_ids = [op.confidence_basis for op in act_ops if op.confidence_basis]
    existing_current = await _existing_active_model_ids(conn, tenant_id, current_basis_ids)

    compiled: list[ActOp] = []
    for op in act_ops:
        if op.op in _BASIS_EXEMPT_ACT_OPS:
            compiled.append(op)
            continue
        basis = op.confidence_basis
        if basis in pending_basis_ids or basis in existing_current:
            compiled.append(op)
            continue
        if fallback_basis is None:
            compiled.append(op)
            continue
        compiled.append(op.model_copy(update={"confidence_basis": fallback_basis}))
        summary.act_ops_bound_confidence_basis += 1
        summary.notes.append(f"bound_{op.op}_basis->{fallback_basis}")
    return compiled


def _basis_candidate_ids(
    *,
    retrieval_result: Any,
    bundle: Any,
    preferred_model_ids: list[UUID | None],
) -> list[UUID]:
    raw: list[Any] = [mid for mid in preferred_model_ids if mid is not None]
    trigger = getattr(retrieval_result, "trigger", None)
    if trigger is not None:
        raw.append(getattr(trigger, "model_id", None))
        raw.extend(getattr(trigger, "member_model_ids", []) or [])
    raw.extend(getattr(model, "id", None) for model in getattr(bundle, "models", []) or [])
    raw.extend(
        getattr(model, "id", None)
        for model in getattr(retrieval_result, "models", []) or []
    )
    return _dedupe_uuids(raw)


async def _compile_edge_ops(
    edge_ops: list[EdgeOp],
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    summary: _MutableSummary,
) -> tuple[list[EdgeOp], list[RelationClaimOp]]:
    kept: list[EdgeOp] = []
    downgraded: list[RelationClaimOp] = []
    pending_kept: list[EdgeOp] = []
    for op in edge_ops:
        if op.source_model_id == op.target_model_id:
            summary.edge_ops_dropped_self_edge += 1
            summary.notes.append(f"dropped_self_edge:{op.edge_kind}")
            continue
        if op.op == "add" and await _edge_would_create_cycle(
            op.source_model_id,
            op.target_model_id,
            op.edge_kind,
            conn=conn,
            tenant_id=tenant_id,
            pending_edge_ops=pending_kept,
        ):
            downgraded.append(_relation_claim_from_cyclic_edge(op))
            summary.edge_ops_downgraded_for_cycle += 1
            summary.notes.append(f"edge_cycle_review:{op.edge_kind}")
            continue
        kept.append(op)
        pending_kept.append(op)
    return kept, downgraded


def _relation_claim_from_cyclic_edge(op: EdgeOp) -> RelationClaimOp:
    return RelationClaimOp(
        source_model_id=op.source_model_id,
        target_model_id=op.target_model_id,
        subject_ref={"kind": "model", "model_id": str(op.source_model_id)},
        object_ref={"kind": "model", "model_id": str(op.target_model_id)},
        predicate=op.edge_kind,
        edge_kind=op.edge_kind,
        direction="source_to_target",
        endpoint_binding_status="bound",
        write_policy="needs_review",
        status="needs_review",
        confidence=min(float(op.confidence), 0.95),
        weight=op.weight,
        binding_confidence=0.95,
        evidence_event_ids=list(op.evidence_event_ids),
        evidence_model_ids=list(op.evidence_model_ids),
        explanation=op.explanation or f"{op.edge_kind} relation needs review.",
        metadata={
            **dict(op.metadata or {}),
            "source": "mutation_compiler",
            "review_status_downgraded_by": _CYCLE_GUARD,
            "mutation_compiler_cycle_guard": True,
        },
    )


async def _compile_relation_claim_ops(
    relation_claim_ops: list[RelationClaimOp],
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    summary: _MutableSummary,
) -> list[RelationClaimOp]:
    kept: list[RelationClaimOp] = []
    for op in relation_claim_ops:
        source_model_id = op.source_model_id or _relation_ref_model_id(op.subject_ref)
        target_model_id = op.target_model_id or _relation_ref_model_id(op.object_ref)
        if (
            source_model_id is not None
            and target_model_id is not None
            and source_model_id == target_model_id
        ):
            summary.relation_claims_dropped_self_edge += 1
            summary.notes.append(f"dropped_self_relation:{op.edge_kind}")
            continue
        if (
            source_model_id is not None
            and target_model_id is not None
            and op.write_policy == "accepted_edge"
            and await _edge_would_create_cycle(
                source_model_id,
                target_model_id,
                op.edge_kind,
                conn=conn,
                tenant_id=tenant_id,
                pending_edge_ops=[],
            )
        ):
            kept.append(_downgrade_relation_claim_for_cycle(op))
            summary.relation_claims_downgraded_for_cycle += 1
            summary.notes.append(f"relation_cycle_review:{op.edge_kind}")
            continue
        kept.append(op)
    return kept


def _downgrade_relation_claim_for_cycle(op: RelationClaimOp) -> RelationClaimOp:
    metadata = {
        **dict(op.metadata or {}),
        "source": "mutation_compiler",
        "review_status_downgraded_by": _CYCLE_GUARD,
        "mutation_compiler_cycle_guard": True,
    }
    return op.model_copy(
        update={
            "write_policy": "needs_review",
            "status": "needs_review",
            "metadata": metadata,
        }
    )


async def _edge_would_create_cycle(
    source_model_id: UUID,
    target_model_id: UUID,
    edge_kind: str,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    pending_edge_ops: list[EdgeOp],
) -> bool:
    try:
        spec = await resolve_edge_kind_spec(conn, tenant_id=tenant_id, kind=edge_kind)
    except Exception:  # noqa: BLE001 - invalid edge kinds stay validator-owned
        return False
    scope = spec.cycle_scope
    if scope is None:
        return False

    pending_adjacency: dict[UUID, set[UUID]] = {}
    for pending in pending_edge_ops:
        if pending.op != "add" or pending.edge_kind not in scope:
            continue
        pending_adjacency.setdefault(pending.source_model_id, set()).add(
            pending.target_model_id
        )

    seen: set[UUID] = set()
    frontier: set[UUID] = {target_model_id}
    scope_list = list(scope)
    for _ in range(512):
        frontier -= seen
        if not frontier:
            return False
        if source_model_id in frontier:
            return True
        seen.update(frontier)
        rows = await conn.fetch(
            """
            SELECT target_model_id
            FROM model_edges
            WHERE tenant_id = $1
              AND source_model_id = ANY($2::uuid[])
              AND edge_kind = ANY($3::text[])
              AND status = 'active'
            """,
            tenant_id,
            list(frontier),
            scope_list,
        )
        next_frontier = {row["target_model_id"] for row in rows}
        for node in frontier:
            next_frontier.update(pending_adjacency.get(node, ()))
        frontier = next_frontier
    return True


def _relation_ref_model_id(ref: Any) -> UUID | None:
    if not isinstance(ref, Mapping) or ref.get("kind") != "model":
        return None
    return _coerce_uuid(ref.get("model_id"))


def _remap_uuid(value: Any, placeholder_map: Mapping[UUID, UUID]) -> UUID | None:
    uid = _coerce_uuid(value)
    if uid is None:
        return uid
    return placeholder_map.get(uid, uid)


def _remap_uuid_list(
    values: list[UUID],
    placeholder_map: Mapping[UUID, UUID],
) -> list[UUID]:
    return _dedupe_uuids(placeholder_map.get(value, value) for value in values)


def _remap_model_ref_dict(
    value: Any,
    placeholder_map: Mapping[UUID, UUID],
) -> tuple[dict[str, Any], bool]:
    if not isinstance(value, Mapping) or value.get("kind") != "model":
        return dict(value or {}), False
    current = _coerce_uuid(value.get("model_id"))
    if current is None or current not in placeholder_map:
        return dict(value), False
    out = dict(value)
    out["model_id"] = str(placeholder_map[current])
    return out, True


async def _existing_active_model_ids(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    ids: list[UUID | None] | list[UUID],
) -> set[UUID]:
    clean = _dedupe_uuids(ids)
    if not clean:
        return set()
    rows = await conn.fetch(
        """
        SELECT id
        FROM models
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
          AND status = 'active'
        """,
        tenant_id,
        clean,
    )
    return {row["id"] for row in rows}


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1)", f"public.{table_name}"))


def _dedupe_uuids(values: Any) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values or []:
        uid = _coerce_uuid(value)
        if uid is None or uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _append_compile_trace(
    trace: str | None,
    summary: MutationCompileSummary,
) -> str | None:
    if not summary.changed:
        return trace
    note = "mutation_compiler=" + ",".join(
        f"{key}:{value}"
        for key, value in summary.to_dict().items()
        if key != "notes" and value
    )
    if summary.notes:
        note = f"{note}; notes={';'.join(summary.notes[:6])}"
    return f"{trace}\n{note}".strip() if trace else note


__all__ = [
    "MutationCompileSummary",
    "compile_raw_diff_mutations",
    "mutation_compiler_enabled",
]
