"""Adjudication helpers for pre-truth relationship candidates.

The topology layer proposes candidates; Think decides whether those
signals become durable memory. This module closes that loop by marking
the originating `relationship_candidates` row based on the validated
diff that actually applied.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from services.retrieval.primary import TriggerContext
from services.think.diff_schema import ValidatedDiff

from .repo import RelationshipCandidatesRepo


@dataclass(frozen=True)
class CandidateAdjudication:
    candidate_id: UUID
    review_status: str
    reason: str
    accepted_model_id: UUID | None = None
    accepted_edge_ids: tuple[UUID, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


async def load_candidate_for_trigger(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    *,
    repo: RelationshipCandidatesRepo | None = None,
) -> dict[str, Any] | None:
    """Load and attach a T4 latent relationship candidate to a trigger.

    Mutates `trigger.seed_signature` and `trigger.member_model_ids` so
    retrieval and prompt rendering see the real candidate record, not
    only the compact queue payload.
    """
    candidate_id = candidate_id_from_trigger(trigger)
    if candidate_id is None:
        return None
    repo = repo or RelationshipCandidatesRepo()
    row = await repo.get(
        conn,
        candidate_id=candidate_id,
        tenant_id=trigger.tenant_id,
    )
    if row is None:
        return None
    signature = (
        dict(trigger.seed_signature)
        if isinstance(trigger.seed_signature, dict)
        else {}
    )
    signature["relationship_candidate"] = _candidate_prompt_shape(row)
    signature["relationship_candidate_id"] = str(candidate_id)
    trigger.seed_signature = signature

    members = [_coerce_uuid(v) for v in row.get("member_model_ids") or []]
    members = [m for m in members if m is not None]
    if members:
        merged = list(dict.fromkeys([*trigger.member_model_ids, *members]))
        trigger.member_model_ids = merged
    return row


async def adjudicate_candidate_for_trigger(
    conn: asyncpg.Connection,
    *,
    trigger: TriggerContext,
    diff: ValidatedDiff,
    applied: dict[str, Any],
    repo: RelationshipCandidatesRepo | None = None,
) -> CandidateAdjudication | None:
    candidate_id = candidate_id_from_trigger(trigger)
    if candidate_id is None:
        return None
    repo = repo or RelationshipCandidatesRepo()
    candidate = await repo.get(
        conn,
        candidate_id=candidate_id,
        tenant_id=trigger.tenant_id,
    )
    if candidate is None:
        return None

    adjudication = _adjudicate(candidate, diff, applied)
    await repo.mark_decided(
        conn,
        candidate_id=candidate_id,
        tenant_id=trigger.tenant_id,
        review_status=adjudication.review_status,
        accepted_model_id=adjudication.accepted_model_id,
        accepted_edge_ids=list(adjudication.accepted_edge_ids),
        decision_metadata={
            "reason": adjudication.reason,
            "accepted_edge_ids": [
                str(edge_id) for edge_id in adjudication.accepted_edge_ids
            ],
            "accepted_model_id": (
                str(adjudication.accepted_model_id)
                if adjudication.accepted_model_id else None
            ),
            **adjudication.metadata,
        },
    )
    return adjudication


def candidate_id_from_trigger(trigger: TriggerContext) -> UUID | None:
    if trigger.kind != "T4" or trigger.subkind != "latent_relationship_candidate":
        return None
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    raw = signature.get("relationship_candidate_id")
    if raw is None:
        nested = signature.get("seed_signature")
        if isinstance(nested, dict):
            raw = nested.get("relationship_candidate_id")
    return _coerce_uuid(raw)


def _adjudicate(
    candidate: dict[str, Any],
    diff: ValidatedDiff,
    applied: dict[str, Any],
) -> CandidateAdjudication:
    candidate_id = candidate["id"]
    accepted_edge_ids = _accepted_edge_ids(candidate, applied)
    accepted_model_id = _accepted_situation_model_id(candidate, diff, applied)
    if accepted_edge_ids or accepted_model_id is not None:
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="accepted",
            reason="think_promoted_candidate_to_durable_memory",
            accepted_model_id=accepted_model_id,
            accepted_edge_ids=tuple(accepted_edge_ids),
        )

    if _has_needs_review_edge(candidate, applied) or diff.dropped_op_count:
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="needs_review",
            reason=(
                "think_found_partial_or_uncertain_candidate_evidence"
                if not diff.dropped_op_count
                else "think_validation_dropped_candidate_ops"
            ),
            metadata={
                "dropped_op_count": diff.dropped_op_count,
                "dropped_op_errors": list(diff.dropped_op_errors[:5]),
            },
        )

    op_count = (
        len(applied.get("claim_ops") or [])
        + len(applied.get("edge_ops") or [])
        + len(applied.get("act_ops") or [])
        + len(applied.get("resource_ops") or [])
    )
    if op_count == 0:
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="rejected",
            reason="think_interpreted_candidate_as_noise_or_non_durable",
        )

    return CandidateAdjudication(
        candidate_id=candidate_id,
        review_status="needs_review",
        reason="think_applied_other_ops_without_directly_promoting_candidate",
    )


def _accepted_edge_ids(
    candidate: dict[str, Any],
    applied: dict[str, Any],
) -> list[UUID]:
    out: list[UUID] = []
    candidate_members = _candidate_member_set(candidate)
    candidate_kind = candidate.get("edge_kind")

    for summary in applied.get("edge_ops") or []:
        if summary.get("op") != "add":
            continue
        if summary.get("review_status") not in {None, "accepted"}:
            continue
        edge_kind = summary.get("edge_kind")
        if candidate_kind and edge_kind != candidate_kind:
            continue
        left = _coerce_uuid(summary.get("source_model_id"))
        right = _coerce_uuid(summary.get("target_model_id"))
        if left is None or right is None:
            continue
        if not {left, right}.issubset(candidate_members):
            continue
        for edge_id in summary.get("edge_ids") or []:
            eid = _coerce_uuid(edge_id)
            if eid is not None:
                out.append(eid)
    return out


def _accepted_situation_model_id(
    candidate: dict[str, Any],
    diff: ValidatedDiff,
    applied: dict[str, Any],
) -> UUID | None:
    candidate_members = {
        _coerce_uuid(v) for v in candidate.get("member_model_ids") or []
    }
    candidate_members.discard(None)
    if len(candidate_members) < 2:
        return None

    summaries = applied.get("claim_ops") or []
    for index, op in enumerate(diff.claim_ops):
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        prop = op.entry.get("proposition") or {}
        if not isinstance(prop, dict) or prop.get("kind") != "situation":
            continue
        members = {
            _coerce_uuid(v)
            for v in (prop.get("member_model_ids") or op.entry.get("member_model_ids") or [])
        }
        members.discard(None)
        if not candidate_members.issubset(members):
            continue
        if index >= len(summaries):
            continue
        return _coerce_uuid(summaries[index].get("model_id"))
    return None


def _has_needs_review_edge(
    candidate: dict[str, Any],
    applied: dict[str, Any],
) -> bool:
    candidate_members = _candidate_member_set(candidate)
    for summary in applied.get("edge_ops") or []:
        if summary.get("op") != "add":
            continue
        if summary.get("review_status") not in {"candidate", "needs_review"}:
            continue
        left = _coerce_uuid(summary.get("source_model_id"))
        right = _coerce_uuid(summary.get("target_model_id"))
        if left is not None and right is not None and {left, right}.issubset(
            candidate_members
        ):
            return True
    return False


def _candidate_member_set(candidate: dict[str, Any]) -> set[UUID]:
    members = {
        coerced
        for coerced in (
            _coerce_uuid(v) for v in candidate.get("member_model_ids") or []
        )
        if coerced is not None
    }
    source = _coerce_uuid(candidate.get("source_model_id"))
    target = _coerce_uuid(candidate.get("target_model_id"))
    if source is not None:
        members.add(source)
    if target is not None:
        members.add(target)
    return members


def _candidate_prompt_shape(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "id": str(row["id"]),
        "candidate_kind": row.get("candidate_kind"),
        "basis": row.get("basis"),
        "source_model_id": (
            str(row["source_model_id"]) if row.get("source_model_id") else None
        ),
        "target_model_id": (
            str(row["target_model_id"]) if row.get("target_model_id") else None
        ),
        "edge_kind": row.get("edge_kind"),
        "member_model_ids": [str(v) for v in row.get("member_model_ids") or []],
        "evidence_model_ids": [
            str(v) for v in row.get("evidence_model_ids") or []
        ],
        "explanation": row.get("explanation"),
        "judgment_leverage_score": row.get("judgment_leverage_score"),
        "proposed_proposition": row.get("proposed_proposition"),
        "metadata": row.get("metadata") or {},
    }


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


__all__ = [
    "CandidateAdjudication",
    "adjudicate_candidate_for_trigger",
    "candidate_id_from_trigger",
    "load_candidate_for_trigger",
]
