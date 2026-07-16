"""Promotion and clarification plans for provisional substrate candidates.

Substrate candidates let Think bind memory to provisional actors, customers,
systems, vendors, workstreams, commitments, and patterns before canonical rows
exist. This module is the deterministic bridge from "candidate exists" to
"we know what should happen next": promote, ask the user, merge, or reject.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field, replace
from typing import Any, Mapping
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7

from .acts import commitments as commitments_svc
from .resources import customer_commitments as customer_commitments_svc
from .clarifications import open_clarification_request
from .resources import repo as resources_repo
from .substrate_candidates import SubstrateCandidate
from .triggers import enqueue_trigger


_AMBIGUITY_KEYS = frozenset(
    {
        "ambiguous",
        "ambiguous_aliases",
        "candidate_conflict",
        "same_label_candidate_ids",
        "possible_matches",
        "merge_candidates",
    }
)
_ACTOR_TYPES = frozenset({"human_internal", "human_external", "ai_agent"})
_RESOURCE_KIND_BY_CANDIDATE_KIND = {
    "customer": "relational",
    "vendor": "relational",
    "system": "infrastructure",
    "workstream": "capacity",
}


@dataclass(frozen=True, slots=True)
class CandidatePromotionPlan:
    candidate_id: UUID
    candidate_kind: str
    action: str
    confidence: float
    reason: str
    needs_user: bool = False
    canonical_kind: str | None = None
    canonical_ref: dict[str, Any] | None = None
    actor: dict[str, Any] | None = None
    resource: dict[str, Any] | None = None
    alias_mappings: list[dict[str, Any]] = field(default_factory=list)
    clarification: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "candidate_id": str(self.candidate_id),
            "candidate_kind": self.candidate_kind,
            "action": self.action,
            "confidence": self.confidence,
            "reason": self.reason,
            "needs_user": self.needs_user,
            "canonical_kind": self.canonical_kind,
            "canonical_ref": self.canonical_ref,
            "actor": self.actor,
            "resource": self.resource,
            "alias_mappings": self.alias_mappings,
            "clarification": self.clarification,
        }


def plan_candidate_promotion(
    candidate: SubstrateCandidate,
    *,
    answer: Mapping[str, Any] | None = None,
    confidence_floor: float = 0.72,
) -> CandidatePromotionPlan:
    """Return the next deterministic action for a provisional candidate."""

    if candidate.status in {"promoted", "merged", "rejected"} and answer is None:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="noop",
            confidence=candidate.confidence,
            reason=f"candidate_already_{candidate.status}",
            canonical_ref=candidate.promotion_ref or candidate.proposed_canonical_ref,
        )

    if answer is not None:
        return _plan_from_answer(candidate, answer)

    if _needs_clarification(candidate, confidence_floor=confidence_floor):
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="ask_user",
            confidence=candidate.confidence,
            reason=_clarification_reason(candidate, confidence_floor),
            needs_user=True,
            clarification=clarification_payload_for_candidate(candidate),
        )

    if candidate.kind == "actor":
        actor = _actor_payload_for_candidate(candidate)
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="promote_actor",
            confidence=candidate.confidence,
            reason="high_confidence_actor_candidate",
            canonical_kind="actor",
            actor=actor,
            alias_mappings=_actor_alias_mappings(candidate),
        )

    if candidate.kind in _RESOURCE_KIND_BY_CANDIDATE_KIND:
        resource = _resource_payload_for_candidate(candidate)
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="promote_resource",
            confidence=candidate.confidence,
            reason=f"high_confidence_{candidate.kind}_candidate",
            canonical_kind="resource",
            resource=resource,
            alias_mappings=_entity_alias_mappings(candidate, resource),
        )

    if candidate.kind == "commitment":
        if candidate.confidence >= 0.78:
            return CandidatePromotionPlan(
                candidate_id=candidate.id,
                candidate_kind=candidate.kind,
                action="promote_commitment",
                confidence=candidate.confidence,
                reason="high_confidence_commitment_candidate",
                canonical_kind="commitment",
            )
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="keep_provisional",
            confidence=candidate.confidence,
            reason="commitment_confidence_below_promotion_floor",
            canonical_kind=f"candidate_{candidate.kind}",
            canonical_ref=candidate.scope_ref,
        )

    if candidate.kind == "pattern":
        if candidate.confidence >= 0.58:
            return CandidatePromotionPlan(
                candidate_id=candidate.id,
                candidate_kind=candidate.kind,
                action="promote_pattern_candidate",
                confidence=candidate.confidence,
                reason="recurring_pattern_candidate_ready_for_review",
                canonical_kind="pattern_candidate",
            )
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="keep_provisional",
            confidence=candidate.confidence,
            reason="pattern_confidence_below_review_floor",
            canonical_kind="candidate_pattern",
            canonical_ref=candidate.scope_ref,
        )

    if candidate.kind == "actor_alias":
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="ask_user",
            confidence=candidate.confidence,
            reason="alias_candidates_must_resolve_to_actor",
            needs_user=True,
            clarification=clarification_payload_for_candidate(candidate),
        )

    return CandidatePromotionPlan(
        candidate_id=candidate.id,
        candidate_kind=candidate.kind,
        action="keep_provisional",
        confidence=candidate.confidence,
        reason=f"{candidate.kind}_promotion_requires_domain_workflow",
        canonical_kind=f"candidate_{candidate.kind}",
        canonical_ref=candidate.scope_ref,
    )


def clarification_payload_for_candidate(
    candidate: SubstrateCandidate,
) -> dict[str, Any]:
    """Build a bounded user question for an ambiguous candidate."""

    label = candidate.label.strip()
    options: list[dict[str, Any]] = []
    if candidate.kind in {"actor", "actor_alias"}:
        options.extend(
            [
                {
                    "action": "promote_actor",
                    "label": f"Create canonical actor for {label}",
                    "actor_type": _infer_actor_type(candidate),
                },
                {
                    "action": "merge",
                    "label": "Merge into an existing actor/candidate",
                    "requires": ["canonical_ref or merge_target_id"],
                },
            ]
        )
    elif candidate.kind in _RESOURCE_KIND_BY_CANDIDATE_KIND:
        resource = _resource_payload_for_candidate(candidate)
        options.extend(
            [
                {
                    "action": "promote_resource",
                    "label": f"Create canonical {candidate.kind} resource",
                    "resource_kind": resource["kind"],
                },
                {
                    "action": "merge",
                    "label": "Merge into an existing resource/candidate",
                    "requires": ["canonical_ref or merge_target_id"],
                },
            ]
        )
    elif candidate.kind == "commitment":
        options.extend(
            [
                {
                    "action": "promote_commitment",
                    "label": f"Create proposed commitment for {label}",
                },
                {
                    "action": "merge",
                    "label": "Merge into an existing commitment/candidate",
                    "requires": ["canonical_ref or merge_target_id"],
                },
                {
                    "action": "keep_provisional",
                    "label": "Keep as provisional commitment",
                },
            ]
        )
    elif candidate.kind == "pattern":
        options.extend(
            [
                {
                    "action": "promote_pattern_candidate",
                    "label": f"Create pattern review candidate for {label}",
                },
                {
                    "action": "keep_provisional",
                    "label": "Keep as provisional pattern",
                },
            ]
        )
    else:
        options.append(
            {
                "action": "keep_provisional",
                "label": f"Keep as provisional {candidate.kind}",
            }
        )
    options.append({"action": "reject", "label": "Reject this candidate"})

    return {
        "kind": "substrate_candidate_resolution",
        "question": _candidate_question(candidate),
        "explanation": (
            "This candidate was inferred from source evidence but is not safe "
            "to silently canonicalize."
        ),
        "object_kind": "substrate_candidate",
        "object_id": str(candidate.id),
        "object_key": f"{candidate.kind}:{candidate.fingerprint}",
        "priority": _clarification_priority(candidate),
        "options": options,
        "payload": {
            "candidate": candidate.to_dict(),
            "aliases": candidate.aliases,
            "evidence_observation_ids": [
                str(value) for value in candidate.evidence_observation_ids
            ],
            "proposed_canonical_ref": candidate.proposed_canonical_ref,
            "ambiguity": {
                key: candidate.metadata.get(key)
                for key in sorted(_AMBIGUITY_KEYS)
                if key in candidate.metadata
            },
        },
    }


async def open_candidate_clarification(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
) -> UUID:
    """Open a user clarification and mark the candidate as waiting on it."""

    payload = clarification_payload_for_candidate(candidate)
    request_id = await open_clarification_request(
        conn,
        tenant_id=candidate.tenant_id,
        kind=payload["kind"],
        question=payload["question"],
        object_kind=payload["object_kind"],
        object_id=candidate.id,
        object_key=payload["object_key"],
        priority=payload["priority"],
        explanation=payload["explanation"],
        source_observation_id=(
            candidate.evidence_observation_ids[0]
            if candidate.evidence_observation_ids
            else None
        ),
        options=payload["options"],
        payload=payload["payload"],
    )
    await mark_candidate_resolution(
        conn,
        candidate=candidate,
        status="needs_clarification",
        metadata_patch={"clarification_request_id": str(request_id)},
    )
    return request_id


async def apply_candidate_resolution_answer(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    answer: Mapping[str, Any],
) -> CandidatePromotionPlan:
    """Apply a user/system resolution answer to the candidate row.

    The function updates the candidate lifecycle and executes durable
    promotion/backfill when the answer asks for canonical substrate.
    """

    plan = plan_candidate_promotion(candidate, answer=answer)
    if plan.action == "reject":
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="rejected",
            metadata_patch={"resolution_answer": dict(answer), "resolution": "reject"},
        )
        return plan
    if plan.action == "merge":
        merge_target_id = _uuid_or_none(answer.get("merge_target_id"))
        if merge_target_id is None and not plan.canonical_ref:
            raise ValidationError(
                "merge resolution requires merge_target_id or canonical_ref",
                field="answer.merge_target_id",
            )
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="merged",
            promotion_ref=plan.canonical_ref,
            merge_target_id=merge_target_id,
            metadata_patch={"resolution_answer": dict(answer), "resolution": "merge"},
        )
        await _backfill_canonical_ref_for_candidate(
            conn,
            candidate=candidate,
            canonical_ref=plan.canonical_ref,
        )
        return plan
    if plan.action == "promote_actor":
        result = await promote_actor_candidate(conn, candidate=candidate, plan=plan)
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="promoted",
            promotion_ref=result["canonical_ref"],
            proposed_canonical_ref=result["canonical_ref"],
            metadata_patch={
                "resolution_answer": dict(answer),
                "resolution": plan.action,
                "promotion_plan": replace(
                    plan,
                    canonical_ref=result["canonical_ref"],
                ).to_dict(),
            },
        )
        return replace(plan, canonical_ref=result["canonical_ref"])
    if plan.action == "promote_resource":
        result = await promote_resource_candidate(conn, candidate=candidate, plan=plan)
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="promoted",
            promotion_ref=result["canonical_ref"],
            proposed_canonical_ref=result["canonical_ref"],
            metadata_patch={
                "resolution_answer": dict(answer),
                "resolution": plan.action,
                "promotion_plan": replace(
                    plan,
                    canonical_ref=result["canonical_ref"],
                ).to_dict(),
            },
        )
        return replace(plan, canonical_ref=result["canonical_ref"])
    if plan.action == "promote_commitment":
        result = await promote_commitment_candidate(
            conn,
            candidate=candidate,
            confidence_floor=0.0,
        )
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="promoted",
            promotion_ref=result["canonical_ref"],
            proposed_canonical_ref=result["canonical_ref"],
            metadata_patch={
                "resolution_answer": dict(answer),
                "resolution": plan.action,
                "promotion_plan": replace(
                    plan,
                    canonical_ref=result["canonical_ref"],
                ).to_dict(),
            },
        )
        return replace(plan, canonical_ref=result["canonical_ref"])
    if plan.action == "promote_pattern_candidate":
        result = await promote_pattern_substrate_candidate(
            conn,
            candidate=candidate,
            require_constituents=True,
        )
        if result is None:
            raise ValidationError(
                "pattern candidate promotion requires at least three active model constituents",
                field="candidate.evidence_model_ids",
            )
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="promoted",
            promotion_ref=result["canonical_ref"],
            proposed_canonical_ref=result["canonical_ref"],
            metadata_patch={
                "resolution_answer": dict(answer),
                "resolution": plan.action,
                "promotion_plan": replace(
                    plan,
                    canonical_ref=result["canonical_ref"],
                ).to_dict(),
            },
        )
        return replace(plan, canonical_ref=result["canonical_ref"])
    if plan.action == "link_existing":
        if plan.action == "link_existing" and not plan.canonical_ref:
            raise ValidationError(
                "link_existing resolution requires canonical_ref",
                field="answer.canonical_ref",
            )
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="promoted",
            promotion_ref=plan.canonical_ref,
            proposed_canonical_ref=plan.canonical_ref,
            metadata_patch={
                "resolution_answer": dict(answer),
                "resolution": plan.action,
                "promotion_plan": plan.to_dict(),
            },
        )
        await _backfill_canonical_ref_for_candidate(
            conn,
            candidate=candidate,
            canonical_ref=plan.canonical_ref,
        )
        await link_related_customer_commitments_for_candidate(
            conn,
            candidate=candidate,
            canonical_ref=plan.canonical_ref,
        )
        return plan
    if plan.action == "ask_user":
        await mark_candidate_resolution(
            conn,
            candidate=candidate,
            status="needs_clarification",
            metadata_patch={"resolution_answer": dict(answer), "resolution": "ask_user"},
        )
        return plan
    return plan


async def mark_candidate_resolution(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    status: str,
    promotion_ref: Mapping[str, Any] | None = None,
    proposed_canonical_ref: Mapping[str, Any] | None = None,
    merge_target_id: UUID | None = None,
    metadata_patch: Mapping[str, Any] | None = None,
) -> None:
    """Update lifecycle fields on ``substrate_candidates``."""

    if status not in {
        "proposed",
        "needs_clarification",
        "promoted",
        "rejected",
        "merged",
        "stale",
    }:
        raise ValidationError(
            f"invalid substrate candidate status {status!r}",
            field="status",
            value=status,
        )
    await conn.execute(
        """
        UPDATE substrate_candidates
        SET status = $3,
            promotion_ref = COALESCE($4::jsonb, promotion_ref),
            proposed_canonical_ref = COALESCE($5::jsonb, proposed_canonical_ref),
            merge_target_id = COALESCE($6, merge_target_id),
            metadata = COALESCE(metadata, '{}'::jsonb) || $7::jsonb,
            updated_at = now()
        WHERE tenant_id = $1 AND id = $2
        """,
        candidate.tenant_id,
        candidate.id,
        status,
        _jsonb_or_none(promotion_ref),
        _jsonb_or_none(proposed_canonical_ref),
        merge_target_id,
        json.dumps(dict(metadata_patch or {}), sort_keys=True, default=str),
    )


async def promote_actor_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    plan: CandidatePromotionPlan | None = None,
) -> dict[str, Any]:
    """Create a canonical actor plus source-actor mappings for a candidate."""

    plan = plan or plan_candidate_promotion(candidate)
    if plan.action != "promote_actor" or not plan.actor:
        raise ValidationError(
            "candidate is not promotable as an actor",
            field="candidate.kind",
            value=candidate.kind,
        )
    actor_id = uuid7()
    actor = dict(plan.actor)
    await conn.fetchrow(
        """
        INSERT INTO actors (
            id, tenant_id, type, display_name, email,
            status, metadata, specification_id,
            created_at, last_seen_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            'active', $6::jsonb, NULL,
            now(), NULL
        )
        RETURNING id
        """,
        actor_id,
        candidate.tenant_id,
        actor["type"],
        actor["display_name"],
        actor.get("email"),
        json.dumps(actor.get("metadata") or {}, sort_keys=True, default=str),
    )
    mapping_count = 0
    for mapping in plan.alias_mappings:
        if not mapping.get("source_channel") or not mapping.get("source_actor_ref"):
            continue
        await conn.execute(
            """
            INSERT INTO actor_identity_mappings (
                actor_id, source_channel, source_actor_ref,
                confidence, created_at
            ) VALUES ($1, $2, $3, $4, now())
            ON CONFLICT (source_channel, source_actor_ref) DO UPDATE SET
                actor_id = EXCLUDED.actor_id,
                confidence = greatest(
                    actor_identity_mappings.confidence,
                    EXCLUDED.confidence
                )
            """,
            actor_id,
            str(mapping["source_channel"]),
            str(mapping["source_actor_ref"]),
            float(mapping.get("confidence", candidate.confidence)),
        )
        mapping_count += 1
    canonical_ref = {"type": "actor", "id": str(actor_id)}
    await mark_candidate_resolution(
        conn,
        candidate=candidate,
        status="promoted",
        promotion_ref=canonical_ref,
        proposed_canonical_ref=canonical_ref,
        metadata_patch={
            "promotion": "actor",
            "actor_alias_mapping_count": mapping_count,
        },
    )
    backfilled_models = await backfill_promoted_candidate_scopes(
        conn,
        candidate=candidate,
        actor_id=actor_id,
    )
    return {
        "canonical_ref": canonical_ref,
        "actor_id": actor_id,
        "alias_mapping_count": mapping_count,
        "backfilled_models": backfilled_models,
    }


async def promote_resource_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    plan: CandidatePromotionPlan | None = None,
) -> dict[str, Any]:
    """Create a canonical resource for a customer/vendor/system/workstream candidate."""

    plan = plan or plan_candidate_promotion(candidate)
    if plan.action != "promote_resource" or not plan.resource:
        raise ValidationError(
            "candidate is not promotable as a resource",
            field="candidate.kind",
            value=candidate.kind,
        )
    cause_event_id = _candidate_cause_event_id(candidate)
    existing_id = await conn.fetchval(
        """
        SELECT id
        FROM resources
        WHERE tenant_id = $1
          AND archived_at IS NULL
          AND metadata->>'promoted_from_candidate_id' = $2
        LIMIT 1
        """,
        candidate.tenant_id,
        str(candidate.id),
    )
    if existing_id is not None:
        resource_id = existing_id if isinstance(existing_id, UUID) else UUID(str(existing_id))
    else:
        resource = await resources_repo.create(
            kind=plan.resource["kind"],
            identity=plan.resource["identity"],
            description=plan.resource.get("description"),
            current_value=dict(plan.resource.get("current_value") or {}),
            valuation_confidence=float(
                plan.resource.get("valuation_confidence", candidate.confidence)
            ),
            metadata=dict(plan.resource.get("metadata") or {}),
            created_by_event_id=cause_event_id,
            tenant_id=candidate.tenant_id,
            conn=conn,
        )
        resource_id = resource.id

    canonical_ref = _resource_canonical_ref(candidate, resource_id)
    await mark_candidate_resolution(
        conn,
        candidate=candidate,
        status="promoted",
        promotion_ref=canonical_ref,
        proposed_canonical_ref=canonical_ref,
        metadata_patch={
            "promotion": "resource",
            "resource_id": str(resource_id),
            "resource_alias_candidate_count": len(plan.alias_mappings),
            "canonical_alias_write": (
                "withheld_pending_grounded_adjudication"
            ),
        },
    )
    backfilled_models = await backfill_promoted_candidate_scopes(
        conn,
        candidate=candidate,
        canonical_refs=_scope_refs_for_resource_candidate(candidate, resource_id),
    )
    linked_customer_commitments = await link_related_customer_commitments_for_candidate(
        conn,
        candidate=candidate,
        canonical_ref=canonical_ref,
    )
    return {
        "canonical_ref": canonical_ref,
        "resource_id": resource_id,
        "alias_mapping_count": 0,
        "alias_candidate_count": len(plan.alias_mappings),
        "backfilled_models": backfilled_models,
        "linked_customer_commitments": linked_customer_commitments,
    }


async def promote_commitment_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    confidence_floor: float = 0.78,
) -> dict[str, Any]:
    """Create a conservative proposed commitment from a commitment candidate."""

    if candidate.kind != "commitment":
        raise ValidationError(
            "candidate is not promotable as a commitment",
            field="candidate.kind",
            value=candidate.kind,
        )
    if candidate.confidence < confidence_floor:
        raise ValidationError(
            "commitment candidate confidence is below promotion floor",
            field="candidate.confidence",
            value=candidate.confidence,
        )
    cause_event_id = _candidate_cause_event_id(candidate)
    existing_id = await conn.fetchval(
        """
        SELECT id
        FROM commitments
        WHERE tenant_id = $1
          AND estimated_capacity->>'promoted_from_candidate_id' = $2
        LIMIT 1
        """,
        candidate.tenant_id,
        str(candidate.id),
    )
    if existing_id is not None:
        commitment_id = (
            existing_id if isinstance(existing_id, UUID) else UUID(str(existing_id))
        )
    else:
        commitment = await commitments_svc.create(
            title=candidate.label,
            description="Proposed commitment inferred from company signals.",
            initial_state="proposed",
            ambition_level="base",
            priority=_commitment_priority(candidate),
            success_criteria={
                "source": "substrate_candidate",
                "candidate_kind": candidate.kind,
                "candidate_confidence": candidate.confidence,
                "evidence_observation_ids": [
                    str(value) for value in candidate.evidence_observation_ids
                ],
                "aliases": candidate.aliases,
            },
            estimated_capacity={
                "maintenance": True,
                "promoted_from_candidate_id": str(candidate.id),
                "candidate_fingerprint": candidate.fingerprint,
                "candidate_scope_ref": candidate.scope_ref,
            },
            is_maintenance=True,
            created_by_event_id=cause_event_id,
            tenant_id=candidate.tenant_id,
            conn=conn,
        )
        commitment_id = commitment.id

    canonical_ref = {"type": "commitment", "id": str(commitment_id)}
    await mark_candidate_resolution(
        conn,
        candidate=candidate,
        status="promoted",
        promotion_ref=canonical_ref,
        proposed_canonical_ref=canonical_ref,
        metadata_patch={
            "promotion": "commitment",
            "commitment_id": str(commitment_id),
        },
    )
    backfilled_models = await backfill_promoted_candidate_scopes(
        conn,
        candidate=candidate,
        canonical_refs=[canonical_ref],
    )
    linked_customer_commitments = await link_related_customer_commitments_for_candidate(
        conn,
        candidate=candidate,
        canonical_ref=canonical_ref,
    )
    return {
        "canonical_ref": canonical_ref,
        "commitment_id": commitment_id,
        "backfilled_models": backfilled_models,
        "linked_customer_commitments": linked_customer_commitments,
    }


async def promote_pattern_substrate_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    min_constituents: int = 3,
    require_constituents: bool = True,
) -> dict[str, Any] | None:
    """Create a durable pattern review candidate from a provisional pattern.

    Observation-level recurrence is useful, but the existing pattern promotion
    pipeline expects model constituents. We therefore only bridge when at least
    ``min_constituents`` active models explicitly carry the provisional pattern
    scope or were attached as candidate evidence.
    """

    if candidate.kind != "pattern":
        raise ValidationError(
            "candidate is not promotable as a pattern candidate",
            field="candidate.kind",
            value=candidate.kind,
        )
    constituent_ids = await _pattern_constituent_model_ids(conn, candidate=candidate)
    if len(constituent_ids) < min_constituents:
        if not require_constituents:
            return None
        raise ValidationError(
            "pattern candidate promotion requires at least three active model constituents",
            field="candidate.evidence_model_ids",
            value=[str(value) for value in constituent_ids],
        )

    existing_id = await conn.fetchval(
        """
        SELECT id
        FROM pattern_candidates
        WHERE tenant_id = $1
          AND proposed_signature->>'substrate_candidate_id' = $2
        LIMIT 1
        """,
        candidate.tenant_id,
        str(candidate.id),
    )
    if existing_id is not None:
        pattern_candidate_id = _uuid_value(existing_id)
        inserted = False
    else:
        pattern_candidate_id = uuid7()
        density = max(0.5, min(1.0, float(candidate.confidence)))
        await conn.execute(
            """
            INSERT INTO pattern_candidates (
                id, tenant_id, proposed_signature, observed_tendency,
                constituent_model_ids, cluster_size, density
            ) VALUES (
                $1, $2, $3::jsonb, $4::jsonb, $5::uuid[], $6, $7
            )
            """,
            pattern_candidate_id,
            candidate.tenant_id,
            json.dumps(
                _pattern_signature(candidate),
                sort_keys=True,
                default=str,
            ),
            json.dumps(
                _pattern_observed_tendency(
                    candidate,
                    constituent_count=len(constituent_ids),
                    density=density,
                ),
                sort_keys=True,
                default=str,
            ),
            constituent_ids,
            len(constituent_ids),
            density,
        )
        inserted = True

    canonical_ref = {"type": "pattern_candidate", "id": str(pattern_candidate_id)}
    await mark_candidate_resolution(
        conn,
        candidate=candidate,
        status="promoted",
        promotion_ref=canonical_ref,
        proposed_canonical_ref=canonical_ref,
        metadata_patch={
            "promotion": "pattern_candidate",
            "pattern_candidate_id": str(pattern_candidate_id),
            "pattern_constituent_model_count": len(constituent_ids),
        },
    )
    trigger_id = await _enqueue_pattern_review_if_possible(
        conn,
        tenant_id=candidate.tenant_id,
        pattern_candidate_id=pattern_candidate_id,
        observation_id=(
            candidate.evidence_observation_ids[0]
            if candidate.evidence_observation_ids
            else None
        ),
    )
    return {
        "canonical_ref": canonical_ref,
        "pattern_candidate_id": pattern_candidate_id,
        "constituent_model_count": len(constituent_ids),
        "inserted": inserted,
        "trigger_id": trigger_id,
    }


async def link_related_customer_commitments_for_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    canonical_ref: Mapping[str, Any] | None,
) -> int:
    """Link promoted customer and commitment candidates with shared evidence."""

    if not canonical_ref or not candidate.related_candidate_ids:
        return 0
    customer_resource_id = _customer_resource_id_from_ref(
        canonical_ref,
        candidate_kind=candidate.kind,
    )
    commitment_id = _commitment_id_from_ref(canonical_ref)
    if customer_resource_id is None and commitment_id is None:
        return 0

    rows = await conn.fetch(
        """
        SELECT id, kind, label, promotion_ref, proposed_canonical_ref
        FROM substrate_candidates
        WHERE tenant_id = $1
          AND id = ANY($2::uuid[])
          AND status IN ('promoted', 'merged')
        """,
        candidate.tenant_id,
        candidate.related_candidate_ids,
    )
    linked = 0
    seen: set[tuple[UUID, UUID]] = set()
    for row in rows:
        related_ref = _json_obj_or_none(_row_get(row, "promotion_ref"))
        if related_ref is None:
            related_ref = _json_obj_or_none(_row_get(row, "proposed_canonical_ref"))
        if related_ref is None:
            continue
        related_kind = str(_row_get(row, "kind") or "")
        if customer_resource_id is not None and related_kind == "commitment":
            related_commitment_id = _commitment_id_from_ref(related_ref)
            if related_commitment_id is None:
                continue
            pair = (customer_resource_id, related_commitment_id)
            if pair in seen:
                continue
            seen.add(pair)
            await customer_commitments_svc.link_commitment(
                customer_resource_id,
                related_commitment_id,
                tenant_id=candidate.tenant_id,
                relationship_kind="delivers",
                criticality="medium",
                served_description=_customer_commitment_description(
                    candidate,
                    related_label=str(_row_get(row, "label") or ""),
                ),
                conn=conn,
            )
            linked += 1
            continue
        if commitment_id is not None and related_kind == "customer":
            related_customer_id = _customer_resource_id_from_ref(
                related_ref,
                candidate_kind=related_kind,
            )
            if related_customer_id is None:
                continue
            pair = (related_customer_id, commitment_id)
            if pair in seen:
                continue
            seen.add(pair)
            await customer_commitments_svc.link_commitment(
                related_customer_id,
                commitment_id,
                tenant_id=candidate.tenant_id,
                relationship_kind="delivers",
                criticality="medium",
                served_description=_customer_commitment_description(
                    candidate,
                    related_label=str(_row_get(row, "label") or ""),
                ),
                conn=conn,
            )
            linked += 1
    return linked


async def _pattern_constituent_model_ids(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
) -> list[UUID]:
    rows = await conn.fetch(
        """
        SELECT id
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND (
            scope_entities @> $2::jsonb
            OR id = ANY($3::uuid[])
          )
        ORDER BY created_at ASC, id ASC
        """,
        candidate.tenant_id,
        json.dumps([candidate.scope_ref], sort_keys=True, default=str),
        candidate.evidence_model_ids,
    )
    out: list[UUID] = []
    seen: set[UUID] = set()
    for row in rows:
        try:
            model_id = _uuid_value(_row_get(row, "id"))
        except (TypeError, ValueError):
            continue
        if model_id in seen:
            continue
        seen.add(model_id)
        out.append(model_id)
    return out


def _pattern_signature(candidate: SubstrateCandidate) -> dict[str, Any]:
    return {
        "kind": "substrate_recurrence",
        "substrate_candidate_id": str(candidate.id),
        "fingerprint": candidate.fingerprint,
        "label": candidate.label,
        "signature": candidate.metadata.get("signature"),
        "basis": candidate.metadata.get("basis") or "substrate_candidate",
    }


def _pattern_observed_tendency(
    candidate: SubstrateCandidate,
    *,
    constituent_count: int,
    density: float,
) -> dict[str, Any]:
    return {
        "summary": candidate.label,
        "exemplars": [candidate.label],
        "candidate_confidence": candidate.confidence,
        "cluster_size": constituent_count,
        "cluster_density": density,
        "count_in_context": candidate.metadata.get("count_in_context"),
        "actor_fingerprints": candidate.metadata.get("actor_fingerprints") or [],
        "evidence_observation_ids": [
            str(value) for value in candidate.evidence_observation_ids
        ],
    }


async def _enqueue_pattern_review_if_possible(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    pattern_candidate_id: UUID,
    observation_id: UUID | None,
) -> UUID | None:
    if not await _table_exists(conn, "think_trigger_queue"):
        return None
    row = await conn.fetchrow(
        """
        SELECT proposed_signature, observed_tendency,
               constituent_model_ids, cluster_size, density
        FROM pattern_candidates
        WHERE tenant_id = $1
          AND id = $2
        """,
        tenant_id,
        pattern_candidate_id,
    )
    payload: dict[str, Any] = {
        "pattern_candidate_id": str(pattern_candidate_id),
        "source": "substrate_promotion",
        "review_mode": "semantic_required",
    }
    if row is not None:
        payload.update(
            {
                "proposed_signature": _json_obj_or_none(
                    _row_get(row, "proposed_signature")
                )
                or {},
                "observed_tendency": _json_obj_or_none(
                    _row_get(row, "observed_tendency")
                )
                or {},
                "constituent_model_ids": [
                    str(model_id)
                    for model_id in _uuid_list_from_any(
                        _row_get(row, "constituent_model_ids")
                    )
                ],
                "cluster_size": int(_row_get(row, "cluster_size") or 0),
                "density": float(_row_get(row, "density") or 0.0),
            }
        )
    return await enqueue_trigger(
        conn,
        tenant_id=tenant_id,
        trigger_kind="T4",
        trigger_subkind="pattern_review",
        observation_id=observation_id,
        payload=payload,
    )


async def _table_exists(conn: asyncpg.Connection, table_name: str) -> bool:
    exists = await conn.fetchval(
        """
        SELECT EXISTS (
            SELECT 1 FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = 'public'
              AND c.relname = $1
              AND c.relkind IN ('r', 'p')
        )
        """,
        table_name,
    )
    return bool(exists)


def _customer_resource_id_from_ref(
    ref: Mapping[str, Any],
    *,
    candidate_kind: str,
) -> UUID | None:
    ref_type = str(ref.get("type") or "")
    if ref_type == "customer" or (ref_type == "resource" and candidate_kind == "customer"):
        return _uuid_from_ref(ref)
    return None


def _commitment_id_from_ref(ref: Mapping[str, Any]) -> UUID | None:
    if str(ref.get("type") or "") != "commitment":
        return None
    return _uuid_from_ref(ref)


def _customer_commitment_description(
    candidate: SubstrateCandidate,
    *,
    related_label: str,
) -> str:
    labels = [candidate.label]
    if related_label and related_label not in labels:
        labels.append(related_label)
    return "Linked from shared substrate evidence: " + " / ".join(labels)


async def auto_promote_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    confidence_floor: float = 0.72,
    commitment_confidence_floor: float = 0.78,
) -> dict[str, Any] | None:
    """Promote one safe candidate into its durable substrate, if possible."""

    if candidate.status != "proposed":
        return None
    if not candidate.evidence_observation_ids:
        return None
    plan = plan_candidate_promotion(
        candidate,
        confidence_floor=_auto_promotion_floor(candidate, default=confidence_floor),
    )
    if plan.action == "promote_actor":
        return await promote_actor_candidate(conn, candidate=candidate, plan=plan)
    if plan.action == "promote_resource":
        return await promote_resource_candidate(conn, candidate=candidate, plan=plan)
    if plan.action == "promote_commitment":
        return await promote_commitment_candidate(
            conn,
            candidate=candidate,
            confidence_floor=commitment_confidence_floor,
        )
    if plan.action == "promote_pattern_candidate":
        return await promote_pattern_substrate_candidate(
            conn,
            candidate=candidate,
            require_constituents=False,
        )
    return None


def _auto_promotion_floor(
    candidate: SubstrateCandidate,
    *,
    default: float,
) -> float:
    """Return the deterministic promotion floor for a discovered substrate.

    The global floor is intentionally conservative for ambiguous people and
    customer names. Some Alpen-scale substrate is much more concrete, though:
    source systems, vendor integrations, repository handles, Jira work items,
    and PR-backed commitments are deterministic enough to promote without a
    clarification round-trip.
    """

    metadata = candidate.metadata or {}
    basis = str(metadata.get("basis") or "")
    kind = candidate.kind
    if kind == "system" and basis in {
        "source_channel",
        "machine_source_actor_ref",
        "repo_text",
        "known_system_phrase",
    }:
        return 0.60
    if kind == "vendor" and basis == "vendor_source":
        return 0.68
    if kind == "workstream" and basis in {"jira_issue_key", "entities_mentioned"}:
        return 0.70
    if kind == "customer" and basis in {"entities_mentioned", "external_email_domain"}:
        return 0.60
    return default


async def backfill_promoted_candidate_scopes(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    canonical_refs: list[Mapping[str, Any]] | None = None,
    actor_id: UUID | None = None,
) -> int:
    """Add durable scope refs to active models already scoped to a candidate."""

    canonical_refs = [dict(ref) for ref in (canonical_refs or []) if ref]
    if actor_id is None and not canonical_refs:
        return 0
    rows = await conn.fetch(
        """
        SELECT id, scope_actors, scope_entities
        FROM models
        WHERE tenant_id = $1
          AND status = 'active'
          AND scope_entities @> $2::jsonb
        """,
        candidate.tenant_id,
        json.dumps([candidate.scope_ref], sort_keys=True, default=str),
    )
    updated = 0
    for row in rows:
        model_id = _uuid_value(row["id"])
        scope_actors = _uuid_list_from_any(_row_get(row, "scope_actors"))
        if actor_id is not None and actor_id not in scope_actors:
            scope_actors.append(actor_id)
        scope_entities = _json_list(_row_get(row, "scope_entities"))
        for ref in canonical_refs:
            _append_unique_scope_ref(scope_entities, ref)
        await conn.execute(
            """
            UPDATE models
            SET scope_actors = $3::uuid[],
                scope_entities = $4::jsonb
            WHERE tenant_id = $1 AND id = $2
            """,
            candidate.tenant_id,
            model_id,
            scope_actors,
            json.dumps(scope_entities, sort_keys=True, default=str),
        )
        if actor_id is not None:
            await conn.execute(
                """
                INSERT INTO model_scope_actors
                  (model_id, tenant_id, actor_id, source, confidence)
                VALUES ($1, $2, $3, 'substrate_promotion', $4)
                ON CONFLICT (model_id, actor_id) DO UPDATE
                  SET source = EXCLUDED.source,
                      confidence = greatest(
                        model_scope_actors.confidence,
                        EXCLUDED.confidence
                      )
                """,
                model_id,
                candidate.tenant_id,
                actor_id,
                float(candidate.confidence),
            )
        for ref in canonical_refs:
            entity_id = _uuid_from_ref(ref)
            entity_type = str(ref.get("type") or "")
            if not entity_type or entity_id is None:
                continue
            await conn.execute(
                """
                INSERT INTO model_scope_entities
                  (model_id, tenant_id, entity_type, entity_id, source, confidence)
                VALUES ($1, $2, $3, $4, 'substrate_promotion', $5)
                ON CONFLICT (model_id, entity_type, entity_id) DO UPDATE
                  SET source = EXCLUDED.source,
                      confidence = greatest(
                        model_scope_entities.confidence,
                        EXCLUDED.confidence
                      )
                """,
                model_id,
                candidate.tenant_id,
                entity_type,
                entity_id,
                float(candidate.confidence),
            )
        updated += 1
    return updated


async def _backfill_canonical_ref_for_candidate(
    conn: asyncpg.Connection,
    *,
    candidate: SubstrateCandidate,
    canonical_ref: Mapping[str, Any] | None,
) -> int:
    if not canonical_ref:
        return 0
    ref_type = str(canonical_ref.get("type") or "")
    entity_id = _uuid_from_ref(canonical_ref)
    if ref_type == "actor" and entity_id is not None:
        return await backfill_promoted_candidate_scopes(
            conn,
            candidate=candidate,
            actor_id=entity_id,
        )
    if ref_type in {"customer", "resource", "commitment", "goal", "decision"}:
        return await backfill_promoted_candidate_scopes(
            conn,
            candidate=candidate,
            canonical_refs=[canonical_ref],
        )
    return 0


def _candidate_cause_event_id(candidate: SubstrateCandidate) -> UUID:
    if not candidate.evidence_observation_ids:
        raise ValidationError(
            "candidate promotion requires at least one evidence observation",
            field="candidate.evidence_observation_ids",
        )
    return candidate.evidence_observation_ids[0]


def _resource_canonical_ref(
    candidate: SubstrateCandidate,
    resource_id: UUID,
) -> dict[str, Any]:
    if candidate.kind == "customer":
        return {
            "type": "customer",
            "id": str(resource_id),
            "resource_id": str(resource_id),
        }
    return {
        "type": "resource",
        "id": str(resource_id),
        "semantic_kind": candidate.kind,
    }


def _scope_refs_for_resource_candidate(
    candidate: SubstrateCandidate,
    resource_id: UUID,
) -> list[dict[str, Any]]:
    refs = [{"type": "resource", "id": str(resource_id)}]
    if candidate.kind == "customer":
        refs.append({"type": "customer", "id": str(resource_id)})
    return refs


def _commitment_priority(candidate: SubstrateCandidate) -> int:
    if candidate.confidence >= 0.9:
        return 3
    if candidate.confidence >= 0.82:
        return 4
    return 5


def _row_get(row: Any, key: str, default: Any = None) -> Any:
    try:
        return row[key]
    except (KeyError, TypeError):
        if hasattr(row, "get"):
            return row.get(key, default)
        return default


def _uuid_value(value: Any) -> UUID:
    if isinstance(value, UUID):
        return value
    return UUID(str(value))


def _uuid_from_ref(ref: Mapping[str, Any]) -> UUID | None:
    raw = ref.get("id")
    if raw is None:
        raw = ref.get("resource_id")
    if raw is None:
        return None
    try:
        return _uuid_value(raw)
    except (TypeError, ValueError):
        return None


def _uuid_list_from_any(value: Any) -> list[UUID]:
    if value is None:
        return []
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    out: list[UUID] = []
    seen: set[UUID] = set()
    if not isinstance(value, (list, tuple)):
        return out
    for item in value:
        try:
            uid = _uuid_value(item)
        except (TypeError, ValueError):
            continue
        if uid in seen:
            continue
        seen.add(uid)
        out.append(uid)
    return out


def _json_list(value: Any) -> list[dict[str, Any]]:
    if value is None:
        return []
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except json.JSONDecodeError:
            return []
    if not isinstance(value, (list, tuple)):
        return []
    return [dict(item) for item in value if isinstance(item, Mapping)]


def _json_obj_or_none(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            decoded = json.loads(value)
        except json.JSONDecodeError:
            return None
        return dict(decoded) if isinstance(decoded, Mapping) else None
    return None


def _append_unique_scope_ref(
    scope_entities: list[dict[str, Any]],
    ref: Mapping[str, Any],
) -> None:
    ref_type = str(ref.get("type") or "")
    ref_id = ref.get("id")
    if not ref_type or ref_id is None:
        return
    key = (ref_type, str(ref_id))
    for item in scope_entities:
        if (str(item.get("type") or ""), str(item.get("id"))) == key:
            return
    scope_entities.append(dict(ref))


def _plan_from_answer(
    candidate: SubstrateCandidate,
    answer: Mapping[str, Any],
) -> CandidatePromotionPlan:
    action = str(answer.get("action") or "").strip().lower()
    if action in {"reject", "rejected"}:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="reject",
            confidence=1.0,
            reason="answer_rejected_candidate",
        )
    canonical_ref = _canonical_ref_from_answer(answer)
    if action in {"merge", "merged"}:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="merge",
            confidence=1.0,
            reason="answer_merged_candidate",
            canonical_kind=_canonical_kind(canonical_ref),
            canonical_ref=canonical_ref,
        )
    if action in {"link_existing", "link", "existing"}:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="link_existing",
            confidence=1.0,
            reason="answer_linked_existing_canonical",
            canonical_kind=_canonical_kind(canonical_ref),
            canonical_ref=canonical_ref,
        )
    if action in {"promote_actor", "create_actor"}:
        actor = _actor_payload_for_candidate(candidate, answer=answer)
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="promote_actor",
            confidence=1.0,
            reason="answer_promoted_actor",
            canonical_kind="actor",
            canonical_ref=canonical_ref,
            actor=actor,
            alias_mappings=_actor_alias_mappings(candidate),
        )
    if action in {"promote_resource", "create_resource"}:
        resource = _resource_payload_for_candidate(candidate, answer=answer)
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="promote_resource",
            confidence=1.0,
            reason="answer_promoted_resource",
            canonical_kind="resource",
            canonical_ref=canonical_ref,
            resource=resource,
            alias_mappings=_entity_alias_mappings(candidate, resource),
        )
    if action in {"promote_commitment", "create_commitment"}:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="promote_commitment",
            confidence=1.0,
            reason="answer_promoted_commitment",
            canonical_kind="commitment",
            canonical_ref=canonical_ref,
        )
    if action in {"promote_pattern_candidate", "create_pattern_candidate"}:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="promote_pattern_candidate",
            confidence=1.0,
            reason="answer_promoted_pattern_candidate",
            canonical_kind="pattern_candidate",
            canonical_ref=canonical_ref,
        )
    if action in {"ask_user", "needs_clarification"}:
        return CandidatePromotionPlan(
            candidate_id=candidate.id,
            candidate_kind=candidate.kind,
            action="ask_user",
            confidence=candidate.confidence,
            reason="answer_kept_ambiguous",
            needs_user=True,
            clarification=clarification_payload_for_candidate(candidate),
        )
    raise ValidationError(
        "candidate resolution answer action is invalid",
        field="answer.action",
        value=action,
    )


def _needs_clarification(
    candidate: SubstrateCandidate,
    *,
    confidence_floor: float,
) -> bool:
    if candidate.confidence < confidence_floor:
        return True
    metadata = candidate.metadata or {}
    if any(metadata.get(key) for key in _AMBIGUITY_KEYS):
        return True
    if candidate.kind in {"actor", "actor_alias"} and not _actor_alias_mappings(candidate):
        return True
    if candidate.proposed_canonical_ref and candidate.confidence < 0.9:
        return True
    return False


def _clarification_reason(candidate: SubstrateCandidate, confidence_floor: float) -> str:
    if candidate.confidence < confidence_floor:
        return "candidate_confidence_below_promotion_floor"
    for key in sorted(_AMBIGUITY_KEYS):
        if candidate.metadata.get(key):
            return f"candidate_metadata_{key}"
    if candidate.kind in {"actor", "actor_alias"}:
        return "actor_candidate_without_source_alias"
    return "candidate_needs_user_resolution"


def _candidate_question(candidate: SubstrateCandidate) -> str:
    if candidate.kind in {"actor", "actor_alias"}:
        return f"Who is the canonical actor for '{candidate.label}'?"
    if candidate.kind == "customer":
        return f"Is '{candidate.label}' a customer we should model canonically?"
    if candidate.kind == "vendor":
        return f"Is '{candidate.label}' a vendor we should model canonically?"
    if candidate.kind == "system":
        return f"Is '{candidate.label}' a system we should model canonically?"
    return f"How should the provisional {candidate.kind} '{candidate.label}' resolve?"


def _clarification_priority(candidate: SubstrateCandidate) -> str:
    evidence_count = len(candidate.evidence_observation_ids) + len(candidate.evidence_model_ids)
    if evidence_count >= 20 or candidate.confidence >= 0.85:
        return "high"
    if candidate.confidence < 0.45:
        return "low"
    return "normal"


def _actor_payload_for_candidate(
    candidate: SubstrateCandidate,
    *,
    answer: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    answer = answer or {}
    email = _first_non_empty(
        answer.get("email"),
        candidate.metadata.get("email"),
        *[alias.get("email") for alias in candidate.aliases],
    )
    actor_type = str(answer.get("actor_type") or "").strip()
    if actor_type not in _ACTOR_TYPES:
        actor_type = _infer_actor_type(candidate, email=email)
    return {
        "display_name": str(answer.get("display_name") or candidate.label).strip(),
        "email": email,
        "type": actor_type,
        "metadata": {
            "promoted_from_candidate_id": str(candidate.id),
            "candidate_kind": candidate.kind,
            "candidate_fingerprint": candidate.fingerprint,
            "candidate_confidence": candidate.confidence,
            "candidate_aliases": candidate.aliases,
        },
    }


def _resource_payload_for_candidate(
    candidate: SubstrateCandidate,
    *,
    answer: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    answer = answer or {}
    resource_kind = str(
        answer.get("resource_kind")
        or candidate.metadata.get("resource_kind")
        or _RESOURCE_KIND_BY_CANDIDATE_KIND.get(candidate.kind)
        or "relational"
    )
    if resource_kind not in {
        "financial",
        "ip",
        "relational",
        "capacity",
        "infrastructure",
        "regulatory",
    }:
        raise ValidationError(
            "candidate resource kind is invalid",
            field="resource_kind",
            value=resource_kind,
        )
    return {
        "kind": resource_kind,
        "identity": str(answer.get("identity") or candidate.label).strip(),
        "description": str(
            answer.get("description")
            or f"Canonical {candidate.kind} inferred from company signals."
        ),
        "current_value": {
            "semantic_kind": candidate.kind,
            "label": candidate.label,
            "candidate_confidence": candidate.confidence,
            "aliases": candidate.aliases,
        },
        "metadata": {
            "promoted_from_candidate_id": str(candidate.id),
            "candidate_kind": candidate.kind,
            "candidate_fingerprint": candidate.fingerprint,
            "candidate_scope_ref": candidate.scope_ref,
        },
        "valuation_confidence": min(1.0, max(0.05, candidate.confidence)),
    }


def _infer_actor_type(
    candidate: SubstrateCandidate,
    *,
    email: str | None = None,
) -> str:
    explicit = str(candidate.metadata.get("actor_type") or "").strip()
    if explicit in _ACTOR_TYPES:
        return explicit
    label = candidate.label.casefold()
    if "bot" in label or "agent" in label or candidate.metadata.get("is_ai"):
        return "ai_agent"
    external_markers = [
        candidate.metadata.get("external"),
        candidate.metadata.get("customer"),
        candidate.metadata.get("vendor"),
        "external" in str(candidate.metadata.get("source_context") or "").casefold(),
    ]
    email_value = email or str(candidate.metadata.get("email") or "")
    company_domains = {
        str(value).casefold().lstrip("@")
        for value in candidate.metadata.get("company_domains", []) or []
    }
    if email_value and "@" in email_value and company_domains:
        domain = email_value.rsplit("@", 1)[-1].casefold()
        if domain not in company_domains:
            external_markers.append(True)
    return "human_external" if any(external_markers) else "human_internal"


def _actor_alias_mappings(candidate: SubstrateCandidate) -> list[dict[str, Any]]:
    mappings: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for alias in candidate.aliases:
        source_channel = str(alias.get("source_channel") or "").strip()
        source_actor_ref = str(alias.get("source_actor_ref") or "").strip()
        if not source_channel or not source_actor_ref:
            continue
        key = (source_channel, source_actor_ref)
        if key in seen:
            continue
        seen.add(key)
        mappings.append(
            {
                "source_channel": source_channel,
                "source_actor_ref": source_actor_ref,
                "confidence": float(alias.get("confidence", candidate.confidence)),
            }
        )
    return mappings


def _entity_alias_mappings(
    candidate: SubstrateCandidate,
    resource: Mapping[str, Any],
) -> list[dict[str, Any]]:
    canonical_ref = {"type": "resource", "identity": resource.get("identity")}
    aliases: list[dict[str, Any]] = [
        {
            "phrase": candidate.label,
            "resolved_entity_ref": canonical_ref,
            "source": "candidate_proposal",
            "confidence": candidate.confidence,
        }
    ]
    for alias in candidate.aliases:
        phrase = alias.get("alias_text") or alias.get("phrase") or alias.get("name")
        if not phrase:
            continue
        aliases.append(
            {
                "phrase": str(phrase),
                "resolved_entity_ref": canonical_ref,
                "source": "candidate_proposal",
                "confidence": float(alias.get("confidence", candidate.confidence)),
            }
        )
    return aliases


def _canonical_ref_from_answer(answer: Mapping[str, Any]) -> dict[str, Any] | None:
    raw = answer.get("canonical_ref") or answer.get("promotion_ref")
    if raw is None:
        return None
    if not isinstance(raw, Mapping):
        raise ValidationError(
            "canonical_ref must be an object",
            field="answer.canonical_ref",
        )
    value = dict(raw)
    if not value.get("type"):
        raise ValidationError(
            "canonical_ref.type is required",
            field="answer.canonical_ref.type",
        )
    return value


def _canonical_kind(canonical_ref: Mapping[str, Any] | None) -> str | None:
    if not canonical_ref:
        return None
    return str(canonical_ref.get("type") or "") or None


def _first_non_empty(*values: Any) -> str | None:
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return None


def _uuid_or_none(value: Any) -> UUID | None:
    if value is None or value == "":
        return None
    try:
        return value if isinstance(value, UUID) else UUID(str(value))
    except (TypeError, ValueError):
        raise ValidationError(
            "value must be a UUID",
            field="uuid",
            value=value,
        ) from None


def _jsonb_or_none(value: Mapping[str, Any] | None) -> str | None:
    if value is None:
        return None
    return json.dumps(dict(value), sort_keys=True, default=str)


__all__ = [
    "CandidatePromotionPlan",
    "apply_candidate_resolution_answer",
    "auto_promote_candidate",
    "backfill_promoted_candidate_scopes",
    "clarification_payload_for_candidate",
    "mark_candidate_resolution",
    "open_candidate_clarification",
    "plan_candidate_promotion",
    "promote_actor_candidate",
    "promote_commitment_candidate",
    "promote_pattern_substrate_candidate",
    "promote_resource_candidate",
    "link_related_customer_commitments_for_candidate",
]
