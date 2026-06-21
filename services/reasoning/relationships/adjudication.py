"""Adjudication helpers for pre-truth relationship candidates.

The topology layer proposes candidates; Think decides whether those
signals become durable memory. This module closes that loop by marking
the originating `relationship_candidates` row based on the validated
diff that actually applied.

Adjudication is structural: even if Think applies an edge of the
requested kind, the edge is only accepted when kind-specific structural
justification is present in the candidate metadata. Otherwise the row is
marked `needs_review`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal
from uuid import UUID

import asyncpg

from lib.shared.memory_grammar import derive_memory_grammar
from services.reasoning.retrieval.primary import TriggerContext
from services.reasoning.think.diff_schema import ValidatedDiff

from .repo import RelationshipCandidatesRepo


DecisionReason = Literal[
    "accepted_with_justification",
    "accepted_low_confidence",
    "needs_review_missing_mechanism",
    "needs_review_partial_evidence",
    "needs_review_dropped_ops",
    "needs_review_unrelated_ops",
    "needs_review_situation_missing_fields",
    "rejected_no_match",
]


# Per-kind structural requirements. An accepted edge must satisfy the
# corresponding predicate against the candidate metadata. Pure pressure
# overlap is not enough — the metadata has to contain the structural
# justification the rule promised.
def _has_mechanism(metadata: dict[str, Any]) -> bool:
    if not isinstance(metadata, dict):
        return False
    if isinstance(metadata.get("mechanism"), str) and metadata["mechanism"].strip():
        return True
    causal = metadata.get("causal") or {}
    if isinstance(causal, dict):
        ms = causal.get("mechanism_summary")
        if isinstance(ms, str) and ms.strip():
            return True
    return False


def _has_dependency_basis(metadata: dict[str, Any]) -> bool:
    if not isinstance(metadata, dict):
        return False
    if metadata.get("dependency_basis"):
        return True
    rule = metadata.get("rule") or {}
    if isinstance(rule, dict) and rule.get("dependency_basis"):
        return True
    return False


def _has_lead_time_evidence(metadata: dict[str, Any]) -> bool:
    if not isinstance(metadata, dict):
        return False
    if metadata.get("lead_time_evidence") or metadata.get("historical_basis"):
        return True
    rule = metadata.get("rule") or {}
    if isinstance(rule, dict) and (
        rule.get("lead_time_evidence") or rule.get("historical_basis")
    ):
        return True
    return False


# Edge kind → predicate returning (ok, missing_field_name).
_STRUCTURAL_REQUIREMENTS: dict[str, list[tuple[str, Any]]] = {
    "blocks": [
        ("mechanism_or_dependency_basis", lambda md: _has_mechanism(md) or _has_dependency_basis(md)),
    ],
    "early_warning_for": [
        ("lead_time_evidence_or_historical_basis", lambda md: _has_lead_time_evidence(md)),
    ],
    "explains": [
        ("mechanism", lambda md: _has_mechanism(md)),
    ],
    "enables": [
        ("mechanism", lambda md: _has_mechanism(md)),
    ],
}

_SITUATION_PRESSURE_TYPES = {
    "capacity",
    "trust",
    "revenue",
    "compliance",
    "decision",
    "execution",
    "market",
    "resource",
}


@dataclass(frozen=True)
class CandidateAdjudication:
    candidate_id: UUID
    review_status: str
    reason: str
    decision_reason: DecisionReason
    accepted_model_id: UUID | None = None
    accepted_edge_ids: tuple[UUID, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


async def load_candidate_for_trigger(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    *,
    repo: RelationshipCandidatesRepo | None = None,
) -> dict[str, Any] | None:
    """Load and attach T4 latent relationship candidates to a trigger.

    Mutates `trigger.seed_signature` and `trigger.member_model_ids` so
    retrieval and prompt rendering see the real candidate records, not
    only the compact queue payload. Scalar triggers attach one candidate;
    batched triggers attach `relationship_candidates` while preserving
    the first candidate under the legacy singular key.
    """
    candidate_ids = candidate_ids_from_trigger(trigger)
    if not candidate_ids:
        return None
    repo = repo or RelationshipCandidatesRepo()
    rows: list[dict[str, Any]] = []
    for candidate_id in candidate_ids:
        row = await repo.get(
            conn,
            candidate_id=candidate_id,
            tenant_id=trigger.tenant_id,
        )
        if row is not None:
            rows.append(row)
    if not rows:
        return None
    signature = (
        dict(trigger.seed_signature)
        if isinstance(trigger.seed_signature, dict)
        else {}
    )
    prompt_shapes = [_candidate_prompt_shape(row) for row in rows]
    signature["relationship_candidate"] = prompt_shapes[0]
    signature["relationship_candidate_id"] = str(rows[0]["id"])
    if len(prompt_shapes) > 1:
        signature["relationship_candidates"] = prompt_shapes
        signature["relationship_candidate_ids"] = [
            str(row["id"]) for row in rows
        ]
    trigger.seed_signature = signature

    members: list[UUID] = []
    for row in rows:
        for raw in row.get("member_model_ids") or []:
            member = _coerce_uuid(raw)
            if member is not None:
                members.append(member)
    if members:
        merged = list(dict.fromkeys([*trigger.member_model_ids, *members]))
        trigger.member_model_ids = merged
    return rows[0]


async def adjudicate_candidate_for_trigger(
    conn: asyncpg.Connection,
    *,
    trigger: TriggerContext,
    diff: ValidatedDiff,
    applied: dict[str, Any],
    repo: RelationshipCandidatesRepo | None = None,
) -> CandidateAdjudication | None:
    results = await adjudicate_candidates_for_trigger(
        conn,
        trigger=trigger,
        diff=diff,
        applied=applied,
        repo=repo,
    )
    return results[0] if results else None


async def adjudicate_candidates_for_trigger(
    conn: asyncpg.Connection,
    *,
    trigger: TriggerContext,
    diff: ValidatedDiff,
    applied: dict[str, Any],
    repo: RelationshipCandidatesRepo | None = None,
) -> list[CandidateAdjudication]:
    candidate_ids = candidate_ids_from_trigger(trigger)
    if not candidate_ids:
        return []
    repo = repo or RelationshipCandidatesRepo()
    out: list[CandidateAdjudication] = []
    for candidate_id in candidate_ids:
        candidate = await repo.get(
            conn,
            candidate_id=candidate_id,
            tenant_id=trigger.tenant_id,
        )
        if candidate is None:
            continue
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
                "decision_reason": adjudication.decision_reason,
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
        out.append(adjudication)
    return out


def candidate_id_from_trigger(trigger: TriggerContext) -> UUID | None:
    candidate_ids = candidate_ids_from_trigger(trigger)
    return candidate_ids[0] if candidate_ids else None


def candidate_ids_from_trigger(trigger: TriggerContext) -> tuple[UUID, ...]:
    if trigger.kind != "T4" or trigger.subkind != "latent_relationship_candidate":
        return ()
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    raw_values: list[Any] = []
    for key in ("relationship_candidate_ids", "batch_relationship_candidate_ids"):
        value = signature.get(key)
        if isinstance(value, list):
            raw_values.extend(value)
    singular = signature.get("relationship_candidate_id")
    if singular is not None:
        raw_values.append(singular)
    nested = signature.get("seed_signature")
    if isinstance(nested, dict):
        for key in (
            "relationship_candidate_ids",
            "batch_relationship_candidate_ids",
        ):
            value = nested.get(key)
            if isinstance(value, list):
                raw_values.extend(value)
        singular = nested.get("relationship_candidate_id")
        if singular is not None:
            raw_values.append(singular)
    out: list[UUID] = []
    seen: set[UUID] = set()
    for raw in raw_values:
        candidate_id = _coerce_uuid(raw)
        if candidate_id is None or candidate_id in seen:
            continue
        seen.add(candidate_id)
        out.append(candidate_id)
    return tuple(out)


def _adjudicate(
    candidate: dict[str, Any],
    diff: ValidatedDiff,
    applied: dict[str, Any],
) -> CandidateAdjudication:
    candidate_id = candidate["id"]
    candidate_metadata = candidate.get("metadata") or {}
    edge_kind = candidate.get("edge_kind")
    if candidate.get("candidate_kind") == "edge_type":
        proposal = candidate.get("proposed_proposition") or {}
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="needs_review",
            reason="edge_type_candidate_requires_ontology_promotion",
            decision_reason="needs_review_unrelated_ops",
            metadata={
                "proposed_edge_kind": (
                    proposal.get("proposed_edge_kind")
                    if isinstance(proposal, dict)
                    else None
                ),
                "promotion_surface": "ontology_gap",
            },
        )

    accepted_edge_ids = _accepted_edge_ids(candidate, applied)
    accepted_model_id, situation_validation = _accepted_situation(
        candidate, diff, applied
    )

    if accepted_edge_ids:
        missing = _structural_missing_fields(edge_kind, candidate_metadata)
        if missing:
            return CandidateAdjudication(
                candidate_id=candidate_id,
                review_status="needs_review",
                reason="think_applied_edge_without_structural_justification",
                decision_reason="needs_review_missing_mechanism",
                accepted_edge_ids=tuple(accepted_edge_ids),
                metadata={
                    "edge_kind": edge_kind,
                    "missing_fields": missing,
                },
            )
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="accepted",
            reason="think_promoted_candidate_to_durable_memory",
            decision_reason="accepted_with_justification",
            accepted_edge_ids=tuple(accepted_edge_ids),
            metadata={"edge_kind": edge_kind},
        )

    if accepted_model_id is not None:
        if situation_validation and situation_validation.get("missing"):
            return CandidateAdjudication(
                candidate_id=candidate_id,
                review_status="needs_review",
                reason="situation_model_missing_required_fields",
                decision_reason="needs_review_situation_missing_fields",
                accepted_model_id=accepted_model_id,
                metadata={"missing_fields": situation_validation["missing"]},
            )
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="accepted",
            reason="think_promoted_candidate_to_durable_memory",
            decision_reason="accepted_with_justification",
            accepted_model_id=accepted_model_id,
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
            decision_reason=(
                "needs_review_partial_evidence"
                if not diff.dropped_op_count
                else "needs_review_dropped_ops"
            ),
            metadata={
                "dropped_op_count": diff.dropped_op_count,
                "dropped_op_errors": list(diff.dropped_op_errors[:5]),
            },
        )

    op_count = (
        len(applied.get("claim_ops") or [])
        + len(applied.get("memory_lifecycle_ops") or [])
        + len(applied.get("relation_claim_ops") or [])
        + len(applied.get("relation_frame_ops") or [])
        + len(applied.get("edge_ops") or [])
        + len(applied.get("ontology_gap_ops") or [])
        + len(applied.get("act_ops") or [])
        + len(applied.get("resource_ops") or [])
    )
    if op_count == 0:
        return CandidateAdjudication(
            candidate_id=candidate_id,
            review_status="rejected",
            reason="think_interpreted_candidate_as_noise_or_non_durable",
            decision_reason="rejected_no_match",
        )

    return CandidateAdjudication(
        candidate_id=candidate_id,
        review_status="needs_review",
        reason="think_applied_other_ops_without_directly_promoting_candidate",
        decision_reason="needs_review_unrelated_ops",
    )


def _structural_missing_fields(
    edge_kind: str | None,
    metadata: dict[str, Any],
) -> list[str]:
    if not edge_kind:
        return []
    requirements = _STRUCTURAL_REQUIREMENTS.get(edge_kind)
    if not requirements:
        return []
    missing: list[str] = []
    for name, predicate in requirements:
        try:
            if not predicate(metadata):
                missing.append(name)
        except Exception:  # noqa: BLE001
            missing.append(name)
    return missing


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


def _accepted_situation(
    candidate: dict[str, Any],
    diff: ValidatedDiff,
    applied: dict[str, Any],
) -> tuple[UUID | None, dict[str, Any] | None]:
    candidate_members = {
        _coerce_uuid(v) for v in candidate.get("member_model_ids") or []
    }
    candidate_members.discard(None)
    if len(candidate_members) < 2:
        return None, None

    summaries = applied.get("claim_ops") or []
    for index, op in enumerate(diff.claim_ops):
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        prop = op.entry.get("proposition") or {}
        if (
            not isinstance(prop, dict)
            or derive_memory_grammar(prop).claim_role != "situation"
        ):
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
        validation = _validate_situation_fields(prop)
        return _coerce_uuid(summaries[index].get("model_id")), validation
    return None, None


def _validate_situation_fields(proposition: dict[str, Any]) -> dict[str, Any]:
    """Validate situation Model has the required structural fields."""
    pressure_type = proposition.get("pressure_type")
    shared_mechanism = proposition.get("shared_mechanism")
    missing: list[str] = []
    if pressure_type not in _SITUATION_PRESSURE_TYPES:
        missing.append("pressure_type")
    if not isinstance(shared_mechanism, str) or not shared_mechanism.strip():
        missing.append("shared_mechanism")
    return {"missing": missing}


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
    for summary in applied.get("relation_claim_ops") or []:
        if summary.get("write_policy") not in {"candidate", "needs_review"}:
            continue
        if summary.get("status") not in {None, "candidate", "needs_review"}:
            continue
        edge_kind = summary.get("edge_kind")
        candidate_kind = candidate.get("edge_kind")
        if candidate_kind and edge_kind != candidate_kind:
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
    "DecisionReason",
    "adjudicate_candidate_for_trigger",
    "adjudicate_candidates_for_trigger",
    "candidate_id_from_trigger",
    "candidate_ids_from_trigger",
    "load_candidate_for_trigger",
]
