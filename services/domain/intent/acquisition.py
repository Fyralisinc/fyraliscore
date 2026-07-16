"""Construct governed intent proposals from untrusted interpreted direction."""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

from lib.contracts.agency import (
    IntentMutation,
    IntentObjectKind,
    IntentOperation,
    InterpretedIntentProposal,
)
from lib.contracts.kernel import ProcessingAuthorityContext, RestrictionSet
from lib.shared.errors import ValidationError
from lib.shared.ids import uuid7


_ACT_MAPPING: dict[str, tuple[IntentObjectKind, IntentOperation, str | None]] = {
    "create_goal": (IntentObjectKind.GOAL, IntentOperation.CREATE, None),
    "update_goal": (IntentObjectKind.GOAL, IntentOperation.UPDATE, "id"),
    "transition_goal": (IntentObjectKind.GOAL, IntentOperation.TRANSITION, "id"),
    "create_commitment": (IntentObjectKind.COMMITMENT, IntentOperation.CREATE, None),
    "transition_commitment": (
        IntentObjectKind.COMMITMENT,
        IntentOperation.TRANSITION,
        "id",
    ),
    "create_decision": (IntentObjectKind.DECISION, IntentOperation.CREATE, None),
    "transition_decision": (IntentObjectKind.DECISION, IntentOperation.TRANSITION, "id"),
    "add_edge_contributes_to": (
        IntentObjectKind.COMMITMENT,
        IntentOperation.UPDATE,
        "commitment_id",
    ),
    "add_edge_depends_on": (
        IntentObjectKind.COMMITMENT,
        IntentOperation.UPDATE,
        "dependent_commitment_id",
    ),
    "add_edge_constrained_by": (
        IntentObjectKind.COMMITMENT,
        IntentOperation.UPDATE,
        "commitment_id",
    ),
}

_PRODUCT_OPERATION_MAPPING: dict[
    tuple[str, str], tuple[IntentObjectKind, IntentOperation]
] = {
    ("goal", "create"): (IntentObjectKind.GOAL, IntentOperation.CREATE),
    ("goal", "update"): (IntentObjectKind.GOAL, IntentOperation.UPDATE),
    ("goal", "transition"): (IntentObjectKind.GOAL, IntentOperation.TRANSITION),
    ("commitment", "create"): (
        IntentObjectKind.COMMITMENT,
        IntentOperation.CREATE,
    ),
    ("commitment", "transition"): (
        IntentObjectKind.COMMITMENT,
        IntentOperation.TRANSITION,
    ),
    ("decision", "transition"): (
        IntentObjectKind.DECISION,
        IntentOperation.TRANSITION,
    ),
    ("decision", "archive"): (IntentObjectKind.DECISION, IntentOperation.RETIRE),
}


def is_intent_bearing_act_op(operation: str) -> bool:
    return operation in _ACT_MAPPING


def proposal_semantic_key(
    *, trigger_id: UUID, think_run_id: UUID | None, operation_index: int
) -> str:
    owner = f"think-run:{think_run_id}" if think_run_id else f"trigger:{trigger_id}"
    return f"{owner}:intent-act-op:{operation_index}"


def build_reasoning_intent_proposal(
    *,
    tenant_id: UUID,
    trigger_id: UUID,
    think_run_id: UUID | None,
    operation_index: int,
    act_operation: str,
    entity_payload: dict[str, Any],
    confidence: float | None,
    confidence_basis_ref: str | None,
    source_observation_id: UUID | None,
    interpreted_at: datetime,
    expected_target_version: int | None = None,
) -> InterpretedIntentProposal:
    """Normalize a reasoning act op without conferring intent authority."""

    if interpreted_at.tzinfo is None or interpreted_at.utcoffset() is None:
        raise ValidationError("intent interpretation time must be timezone-aware")
    try:
        object_kind, operation, target_field = _ACT_MAPPING[act_operation]
    except KeyError as exc:
        raise ValidationError(
            "reasoning operation is not a registered intent-bearing act",
            operation=act_operation,
        ) from exc

    target_id = None
    if target_field is not None:
        raw_target = entity_payload.get(target_field)
        try:
            target_id = UUID(str(raw_target))
        except (TypeError, ValueError) as exc:
            raise ValidationError(
                "intent-bearing act is missing its exact target",
                operation=act_operation,
                target_field=target_field,
            ) from exc

    mutation_payload = dict(entity_payload)
    grounding_dependency_refs = tuple(
        sorted(str(item) for item in mutation_payload.pop("_grounding_dependency_refs", ()))
    )
    mutation_payload["requested_act_operation"] = act_operation
    mutation = IntentMutation(
        object_kind=object_kind,
        operation=operation,
        target_aggregate_id=target_id,
        expected_target_version=expected_target_version,
        payload=mutation_payload,
        schema_version="think-act-intent-proposal-v1",
        effective_at=interpreted_at,
    )
    source_ref = (
        f"observation:{source_observation_id}"
        if source_observation_id
        else f"trigger:{trigger_id}"
    )
    basis_refs = {f"trigger:{trigger_id}"}
    if think_run_id:
        basis_refs.add(f"think-run:{think_run_id}")
    if confidence_basis_ref:
        basis_refs.add(f"belief:{confidence_basis_ref}")
    processing_authority = ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="service:think",
        purpose="interpret_intent_proposal",
        operation="propose",
        object_types=RestrictionSet.only(object_kind.value),
        object_ids=(
            RestrictionSet.only(str(target_id))
            if target_id is not None
            else RestrictionSet.unrestricted()
        ),
        fields=RestrictionSet.only(*sorted(mutation_payload)),
        source_labels=RestrictionSet.only("think:act_op"),
        authority_basis_refs=frozenset(basis_refs),
        policy_version="think-intent-proposal-authority-v1",
        authority_epoch=1,
        decision_time=interpreted_at,
        expires_at=interpreted_at + timedelta(days=1),
    )
    return InterpretedIntentProposal(
        proposal_id=uuid7(timestamp_ms=int(interpreted_at.timestamp() * 1000)),
        tenant_id=tenant_id,
        proposal_version=1,
        normalized_mutation=mutation,
        normalized_payload_digest=mutation.payload_digest,
        source_assertion_refs=(source_ref,),
        semantic_frame_refs=(
            f"reasoning-act-op:{act_operation}:{operation_index}",
        ),
        grounding_dependency_refs=grounding_dependency_refs,
        uncertainty_reasons=(
            "reasoning_output_is_interpreted_direction_not_company_intent",
            "exact_capable_principal_acceptance_required",
        ),
        confidence=confidence,
        processing_authority=processing_authority,
        processing_authority_fingerprint=processing_authority.fingerprint,
        created_at=interpreted_at,
        review_due_at=interpreted_at + timedelta(days=7),
    )


def build_product_recommendation_intent_proposal(
    *,
    tenant_id: UUID,
    recommendation_id: UUID,
    source_observation_id: UUID,
    target_type: str,
    target_id: UUID | None,
    proposed_operation: str,
    proposed_payload: dict[str, Any],
    expected_target_version: int | None,
    recommendation_confidence: float | None,
    interpreted_at: datetime,
) -> InterpretedIntentProposal:
    """Normalize one displayed recommendation before the user's acceptance."""

    try:
        object_kind, operation = _PRODUCT_OPERATION_MAPPING[
            (target_type, proposed_operation)
        ]
    except KeyError as exc:
        raise ValidationError(
            "recommendation operation is not registered as an intent mutation",
            target_type=target_type,
            operation=proposed_operation,
        ) from exc
    if operation is IntentOperation.CREATE:
        mutation_target = None
        expected_target_version = None
    else:
        if target_id is None:
            raise ValidationError("non-create recommendation requires an exact target")
        mutation_target = target_id
    payload = dict(proposed_payload)
    payload["requested_product_operation"] = proposed_operation
    mutation = IntentMutation(
        object_kind=object_kind,
        operation=operation,
        target_aggregate_id=mutation_target,
        expected_target_version=expected_target_version,
        payload=payload,
        schema_version="product-recommendation-intent-proposal-v1",
        effective_at=interpreted_at,
    )
    processing_authority = ProcessingAuthorityContext(
        tenant_id=tenant_id,
        principal_or_service_id="service:recommendation_interpreter",
        purpose="interpret_intent_proposal",
        operation="propose",
        object_types=RestrictionSet.only(object_kind.value),
        object_ids=(
            RestrictionSet.only(str(mutation_target))
            if mutation_target is not None
            else RestrictionSet.unrestricted()
        ),
        fields=RestrictionSet.only(*sorted(payload)),
        source_labels=RestrictionSet.only("product:recommendation"),
        authority_basis_refs=frozenset(
            {
                f"recommendation:{recommendation_id}",
                f"observation:{source_observation_id}",
            }
        ),
        policy_version="product-recommendation-proposal-authority-v1",
        authority_epoch=1,
        decision_time=interpreted_at,
        expires_at=interpreted_at + timedelta(days=90),
    )
    return InterpretedIntentProposal(
        proposal_id=uuid7(timestamp_ms=int(interpreted_at.timestamp() * 1000)),
        tenant_id=tenant_id,
        proposal_version=1,
        normalized_mutation=mutation,
        normalized_payload_digest=mutation.payload_digest,
        source_assertion_refs=(f"observation:{source_observation_id}",),
        semantic_frame_refs=(f"recommendation-model:{recommendation_id}",),
        uncertainty_reasons=(
            "recommendation_is_not_company_intent_until_exact_acceptance",
        ),
        confidence=recommendation_confidence,
        processing_authority=processing_authority,
        processing_authority_fingerprint=processing_authority.fingerprint,
        created_at=interpreted_at,
        review_due_at=interpreted_at + timedelta(days=90),
    )


__all__ = [
    "build_product_recommendation_intent_proposal",
    "build_reasoning_intent_proposal",
    "is_intent_bearing_act_op",
    "proposal_semantic_key",
]
