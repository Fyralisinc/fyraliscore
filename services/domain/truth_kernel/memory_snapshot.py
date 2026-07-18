"""Build and revalidate exact accepted-memory snapshots.

The adapter intentionally reads the accepted-current views, rather than raw
heads, so a head that has lost admission/evidence eligibility cannot enter a
learning transaction.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable
from uuid import UUID, uuid5

from lib.contracts.company_learning import (
    AcceptedHeadRef,
    AcceptedMemorySnapshot,
    AcceptedRelationHeadRef,
)
from lib.contracts.truth_admission import ModelTruthLifecycle
from lib.shared.errors import InvariantViolation


_SNAPSHOT_NAMESPACE = UUID("71f1b7ec-c539-5ac1-a29d-edffb8542cc5")

_MODEL_HEADS_SQL = """
/* accepted-memory:model-heads */
SELECT a.id AS model_id, a.tenant_id, a.truth_version_id AS version_id,
       a.truth_version AS version, a.truth_semantic_digest AS semantic_digest,
       a.truth_lifecycle AS lifecycle, a.created_at,
       a.truth_advanced_at AS advanced_at,
       array_agg(DISTINCT s.canonical_ref ORDER BY s.canonical_ref)
         FILTER (WHERE s.canonical_ref IS NOT NULL) AS canonical_scope_refs,
       bool_and(s.canonical_ref IS NOT NULL) AS all_scope_refs_canonical
FROM accepted_current_models a
LEFT JOIN model_truth_scope_bindings s
  ON s.tenant_id = a.tenant_id AND s.model_version_id = a.truth_version_id
WHERE a.tenant_id=$1 AND a.id = ANY($2::uuid[])
GROUP BY a.id, a.tenant_id, a.truth_version_id, a.truth_version,
         a.truth_semantic_digest, a.truth_lifecycle, a.created_at,
         a.truth_advanced_at
ORDER BY a.id
"""

_RELATION_HEADS_SQL = """
/* accepted-memory:relation-heads */
SELECT a.id AS relation_id, a.tenant_id,
       a.truth_relation_version_id AS relation_version_id,
       a.truth_version AS version, a.truth_semantic_digest AS semantic_digest,
       a.truth_lifecycle AS lifecycle, v.created_at,
       a.truth_advanced_at AS advanced_at
FROM accepted_current_relations a
JOIN relation_truth_versions v
  ON v.tenant_id = a.tenant_id
 AND v.relation_version_id = a.truth_relation_version_id
WHERE a.tenant_id=$1 AND a.id = ANY($2::uuid[])
ORDER BY a.id
"""


def _ordered_ids(values: Iterable[UUID]) -> tuple[UUID, ...]:
    return tuple(sorted(set(values), key=str))


def _fail(invariant: str, message: str, **context: Any) -> None:
    raise InvariantViolation(invariant, message, **context)


def _check_cutoff(row: Any, *, cutoff_at: datetime, kind: str, object_id: UUID) -> None:
    if row["created_at"] > cutoff_at or row["advanced_at"] > cutoff_at:
        _fail(
            "accepted_memory_head_after_cutoff",
            f"{kind} head was not current by the snapshot cutoff",
            object_kind=kind,
            object_id=str(object_id),
            cutoff_at=cutoff_at.isoformat(),
        )


async def _load_heads(
    tx: Any,
    *,
    tenant_id: UUID,
    cutoff_at: datetime,
    model_ids: tuple[UUID, ...],
    relation_ids: tuple[UUID, ...],
) -> tuple[tuple[AcceptedHeadRef, ...], tuple[AcceptedRelationHeadRef, ...]]:
    model_rows = await tx.fetch(_MODEL_HEADS_SQL, tenant_id, list(model_ids)) if model_ids else ()
    relation_rows = (
        await tx.fetch(_RELATION_HEADS_SQL, tenant_id, list(relation_ids))
        if relation_ids else ()
    )

    found_models = {row["model_id"] for row in model_rows}
    missing_models = tuple(str(value) for value in model_ids if value not in found_models)
    if missing_models:
        _fail(
            "accepted_memory_model_head_missing",
            "requested Models do not have accepted current heads",
            tenant_id=str(tenant_id),
            model_ids=missing_models,
        )
    found_relations = {row["relation_id"] for row in relation_rows}
    missing_relations = tuple(str(value) for value in relation_ids if value not in found_relations)
    if missing_relations:
        _fail(
            "accepted_memory_relation_head_missing",
            "requested relations do not have accepted current heads",
            tenant_id=str(tenant_id),
            relation_ids=missing_relations,
        )

    models: list[AcceptedHeadRef] = []
    for row in sorted(model_rows, key=lambda item: str(item["model_id"])):
        _check_cutoff(row, cutoff_at=cutoff_at, kind="model", object_id=row["model_id"])
        refs = tuple(row["canonical_scope_refs"] or ())
        if not refs or not row["all_scope_refs_canonical"]:
            _fail(
                "accepted_memory_scope_not_canonical",
                "accepted Model head lacks complete canonical scope coordinates",
                model_id=str(row["model_id"]),
                version_id=str(row["version_id"]),
            )
        models.append(AcceptedHeadRef(
            tenant_id=row["tenant_id"], model_id=row["model_id"],
            version_id=row["version_id"], version=row["version"],
            semantic_digest=row["semantic_digest"],
            lifecycle=ModelTruthLifecycle(row["lifecycle"]),
            canonical_scope_refs=refs,
        ))

    relations: list[AcceptedRelationHeadRef] = []
    for row in sorted(relation_rows, key=lambda item: str(item["relation_id"])):
        _check_cutoff(row, cutoff_at=cutoff_at, kind="relation", object_id=row["relation_id"])
        relations.append(AcceptedRelationHeadRef(
            tenant_id=row["tenant_id"], relation_id=row["relation_id"],
            relation_version_id=row["relation_version_id"], version=row["version"],
            semantic_digest=row["semantic_digest"], lifecycle=row["lifecycle"],
        ))
    return tuple(models), tuple(relations)


async def build_accepted_memory_snapshot(
    tx: Any,
    *,
    tenant_id: UUID,
    cutoff_at: datetime,
    model_ids: Iterable[UUID] = (),
    relation_ids: Iterable[UUID] = (),
    retrieval_receipt_ids: Iterable[UUID] = (),
) -> AcceptedMemorySnapshot:
    """Capture exact accepted heads visible at ``cutoff_at``.

    This is a current-head snapshot with a cutoff guard, not a historical
    reconstruction API.
    """
    if cutoff_at.tzinfo is None or cutoff_at.utcoffset() is None:
        _fail("accepted_memory_naive_cutoff", "snapshot cutoff must be timezone-aware")
    models_requested = _ordered_ids(model_ids)
    relations_requested = _ordered_ids(relation_ids)
    receipts = _ordered_ids(retrieval_receipt_ids)
    models, relations = await _load_heads(
        tx, tenant_id=tenant_id, cutoff_at=cutoff_at,
        model_ids=models_requested, relation_ids=relations_requested,
    )
    identity = "|".join((
        str(tenant_id), cutoff_at.astimezone(timezone.utc).isoformat(),
        *(f"m:{item.model_id}:{item.version_id}:{item.semantic_digest}" for item in models),
        *(
            f"r:{item.relation_id}:{item.relation_version_id}:{item.semantic_digest}"
            for item in relations
        ),
        *(f"receipt:{value}" for value in receipts),
    ))
    return AcceptedMemorySnapshot(
        snapshot_id=uuid5(_SNAPSHOT_NAMESPACE, identity), tenant_id=tenant_id,
        cutoff_at=cutoff_at, model_heads=models, relation_heads=relations,
        retrieval_receipt_ids=receipts,
    )


async def validate_accepted_memory_snapshot(tx: Any, snapshot: AcceptedMemorySnapshot) -> None:
    """Reject a snapshot whose exact heads are no longer accepted/current."""
    current_models, current_relations = await _load_heads(
        tx, tenant_id=snapshot.tenant_id, cutoff_at=snapshot.cutoff_at,
        model_ids=tuple(item.model_id for item in snapshot.model_heads),
        relation_ids=tuple(item.relation_id for item in snapshot.relation_heads),
    )
    if current_models != snapshot.model_heads or current_relations != snapshot.relation_heads:
        _fail(
            "accepted_memory_snapshot_stale",
            "accepted memory snapshot no longer matches exact current heads",
            snapshot_id=str(snapshot.snapshot_id),
            snapshot_digest=snapshot.snapshot_digest,
        )


__all__ = ["build_accepted_memory_snapshot", "validate_accepted_memory_snapshot"]
