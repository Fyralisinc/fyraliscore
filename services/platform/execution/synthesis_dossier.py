"""Closed, scope-local reasoning dossiers for bounded synthesis decisions.

The assembler is intentionally provider- and evaluator-blind.  Callers supply
governed episodes, exact current Model heads, and durable semantic annotations;
this module validates and deterministically packages those runtime facts.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any, Literal, Mapping, Sequence
from uuid import UUID

from lib.contracts.kernel import canonical_sha256
from services.platform.execution.governed_learning_episode import (
    GovernedLearningEpisode,
    GovernedObservationAssertion,
)

EvidenceRole = Literal["direct", "transitive", "contradictory", "auxiliary"]
MechanismRole = Literal["cause", "condition", "outcome"]
MechanismOpportunity = Literal["mature", "immature", "none"]


class DossierContractError(ValueError):
    """Raised when runtime material cannot form one closed dossier."""


@dataclass(frozen=True, slots=True)
class ModelHeadInput:
    model_id: UUID
    truth_version_id: UUID
    natural_text: str
    proposition: Mapping[str, Any]
    canonical_scope_ref: str
    accepted_current: bool
    valid_as_of: datetime | None = None
    truth_advanced_at: datetime | None = None
    member_model_ids: tuple[UUID, ...] = ()
    evidence_observation_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class EvidenceAnnotation:
    object_kind: Literal["observation", "model"]
    object_id: UUID
    role: EvidenceRole
    mechanism_roles: tuple[MechanismRole, ...] = ()
    reason: str = ""


@dataclass(frozen=True, slots=True)
class DurableExplanationInput:
    provenance_id: str
    text: str
    status: Literal["open", "weakened", "rejected", "previously_abstained"]
    supporting_object_ids: tuple[UUID, ...] = ()
    counterevidence_object_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class DurableUncertaintyInput:
    provenance_id: str
    question: str
    status: Literal["missing", "inconclusive"] = "missing"
    discriminates_between: tuple[str, ...] = ()
    retrieval_target: str | None = None


@dataclass(frozen=True, slots=True)
class DossierObject:
    handle: str
    object_kind: Literal["observation", "accepted_model_head", "explanation", "uncertainty"]
    semantic_content: Mapping[str, Any]
    canonical_id: str
    canonical_scope_ref: str | None = None
    exact_version_id: str | None = None
    evidence_role: EvidenceRole | None = None
    authority_tier: str | None = None
    independence_group: str | None = None


@dataclass(frozen=True, slots=True)
class EvidenceReference:
    object_handle: str
    role: EvidenceRole
    reason: str = ""


@dataclass(frozen=True, slots=True)
class AssemblyReceipt:
    input_episode_ids: tuple[str, ...]
    input_object_count: int
    included_object_count: int
    excluded_object_count: int
    exclusion_reasons: Mapping[str, int]
    handle_binding_digest: str
    content_digest: str
    canonical_scope_closed: bool
    chronology_valid: bool
    evidence_closure_valid: bool
    mechanism_opportunity: MechanismOpportunity
    readiness_reasons: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class SynthesisDossier:
    schema_version: Literal["synthesis-dossier-v1"]
    dossier_id: str
    tenant_id: UUID
    scope: Mapping[str, str]
    window: Mapping[str, str]
    handles: tuple[DossierObject, ...]
    event_order: tuple[str, ...]
    accepted_model_heads: tuple[str, ...]
    direct_observations: tuple[str, ...]
    supporting_evidence: tuple[EvidenceReference, ...]
    contradictory_evidence: tuple[EvidenceReference, ...]
    auxiliary_evidence: tuple[EvidenceReference, ...]
    open_uncertainty: tuple[str, ...]
    candidate_mechanism_slots: Mapping[str, tuple[str, ...]]
    considered_explanations: tuple[str, ...]
    discriminating_missing_evidence: tuple[str, ...]
    assembly_receipt: AssemblyReceipt

    def provider_payload(self) -> dict[str, Any]:
        """Return semantic content and local handles without canonical IDs."""
        objects = []
        for item in self.handles:
            semantic_content = _provider_safe(dict(item.semantic_content))
            # Exact evidence addresses remain compiler-owned bindings because
            # observation addresses contain canonical UUIDs.
            semantic_content.pop("evidence_address", None)
            semantic_content.pop("member_model_ids", None)
            semantic_content.pop("evidence_observation_ids", None)
            objects.append({
                "handle": item.handle,
                "object_kind": item.object_kind,
                "semantic_content": semantic_content,
                "evidence_role": item.evidence_role,
                "authority_tier": item.authority_tier,
                "independence_group": item.independence_group,
            })
        return {
            "schema_version": self.schema_version,
            "dossier_id": self.dossier_id,
            "scope": {"display_label": self.scope["display_label"]},
            "window": dict(self.window),
            "handles": objects,
            "event_order": list(self.event_order),
            "accepted_model_heads": list(self.accepted_model_heads),
            "direct_observations": list(self.direct_observations),
            "supporting_evidence": [asdict(item) for item in self.supporting_evidence],
            "contradictory_evidence": [asdict(item) for item in self.contradictory_evidence],
            "auxiliary_evidence": [asdict(item) for item in self.auxiliary_evidence],
            "open_uncertainty": list(self.open_uncertainty),
            "candidate_mechanism_slots": {
                key: list(value) for key, value in self.candidate_mechanism_slots.items()
            },
            "considered_explanations": list(self.considered_explanations),
            "discriminating_missing_evidence": list(self.discriminating_missing_evidence),
        }


def assemble_synthesis_dossier(
    *,
    tenant_id: UUID,
    episode: GovernedLearningEpisode,
    as_of_at: datetime,
    model_heads: Sequence[ModelHeadInput] = (),
    evidence_annotations: Sequence[EvidenceAnnotation] = (),
    source_identities: Mapping[UUID, str] | None = None,
    explanations: Sequence[DurableExplanationInput] = (),
    uncertainties: Sequence[DurableUncertaintyInput] = (),
) -> SynthesisDossier:
    """Validate and assemble one deterministic, exact-scope dossier."""
    model_heads = tuple(model_heads)
    evidence_annotations = tuple(evidence_annotations)
    explanations = tuple(explanations)
    uncertainties = tuple(uncertainties)
    if episode.tenant_id != tenant_id:
        raise DossierContractError("episode tenant does not match dossier tenant")
    scope_ref = _resolved_scope(episode)
    assertions, excluded = _eligible_assertions(episode, tenant_id, scope_ref, as_of_at)
    if not assertions:
        raise DossierContractError("dossier requires same-scope resolved observations")
    display_label = _display_label(assertions)
    models, model_exclusions = _eligible_models(model_heads, scope_ref, as_of_at)
    for reason, count in model_exclusions.items():
        excluded[reason] = excluded.get(reason, 0) + count

    annotations = _annotation_index(evidence_annotations)
    objects: list[DossierObject] = []
    handle_by_id: dict[UUID, str] = {}
    mechanism: dict[str, list[str]] = {"causes": [], "conditions": [], "outcomes": []}
    evidence_refs: dict[EvidenceRole, list[EvidenceReference]] = {
        "direct": [], "transitive": [], "contradictory": [], "auxiliary": [],
    }
    identities = source_identities or {}

    for index, assertion in enumerate(assertions, 1):
        handle = f"O{index}"
        handle_by_id[assertion.observation_id] = handle
        annotation = annotations.get(("observation", assertion.observation_id))
        role: EvidenceRole = annotation.role if annotation else "direct"
        source_identity = str(identities.get(assertion.observation_id) or "").strip()
        independence = (
            f"source:{source_identity}" if source_identity
            else f"unknown:{assertion.source_channel or 'unspecified'}"
        )
        objects.append(DossierObject(
            handle=handle,
            object_kind="observation",
            canonical_id=str(assertion.observation_id),
            canonical_scope_ref=scope_ref,
            evidence_role=role,
            authority_tier=assertion.trust_tier,
            independence_group=independence,
            semantic_content={
                "occurred_at": assertion.occurred_at.isoformat(),
                "assertion_text": assertion.assertion_text,
                "evidence_address": assertion.evidence_address,
                "evidence_field_path": assertion.evidence_field_path,
                "evidence_span_start": assertion.evidence_span_start,
                "evidence_span_end": assertion.evidence_span_end,
                "source_channel": assertion.source_channel,
                "source_identity": source_identity or None,
            },
        ))
        evidence_refs[role].append(EvidenceReference(
            object_handle=handle, role=role, reason=annotation.reason if annotation else "",
        ))
        _add_mechanism_handles(mechanism, handle, annotation)

    for index, model in enumerate(models, 1):
        handle = f"M{index}"
        handle_by_id[model.model_id] = handle
        annotation = annotations.get(("model", model.model_id))
        role: EvidenceRole = annotation.role if annotation else "transitive"
        objects.append(DossierObject(
            handle=handle,
            object_kind="accepted_model_head",
            canonical_id=str(model.model_id),
            exact_version_id=str(model.truth_version_id),
            canonical_scope_ref=scope_ref,
            evidence_role=role,
            semantic_content={
                "natural_text": model.natural_text,
                "proposition": dict(model.proposition),
                "valid_as_of": model.valid_as_of.isoformat() if model.valid_as_of else None,
                "member_model_ids": [str(value) for value in model.member_model_ids],
                "evidence_observation_ids": [
                    str(value) for value in model.evidence_observation_ids
                ],
            },
        ))
        evidence_refs[role].append(EvidenceReference(
            object_handle=handle, role=role, reason=annotation.reason if annotation else "",
        ))
        _add_mechanism_handles(mechanism, handle, annotation)

    explanation_handles: list[str] = []
    explanation_handle_by_provenance: dict[str, str] = {}
    for index, item in enumerate(sorted(explanations, key=lambda row: row.provenance_id), 1):
        if not item.provenance_id.strip():
            raise DossierContractError("explanation requires durable provenance")
        handle = f"X{index}"
        explanation_handles.append(handle)
        explanation_handle_by_provenance[item.provenance_id] = handle
        unknown_references = (
            set(item.supporting_object_ids) | set(item.counterevidence_object_ids)
        ) - set(handle_by_id)
        if unknown_references:
            raise DossierContractError(
                "explanation references excluded or unknown evidence"
            )
        objects.append(DossierObject(
            handle=handle, object_kind="explanation", canonical_id=item.provenance_id,
            semantic_content={
                "text": item.text,
                "status": item.status,
                "supporting_handles": [
                    handle_by_id[value] for value in item.supporting_object_ids
                ],
                "counterevidence_handles": [
                    handle_by_id[value] for value in item.counterevidence_object_ids
                ],
            },
        ))

    uncertainty_handles: list[str] = []
    for index, item in enumerate(sorted(uncertainties, key=lambda row: row.provenance_id), 1):
        if not item.provenance_id.strip():
            raise DossierContractError("uncertainty requires durable provenance")
        unknown_explanations = set(item.discriminates_between) - set(
            explanation_handle_by_provenance
        )
        if unknown_explanations:
            raise DossierContractError("uncertainty references unknown explanation provenance")
        handle = f"U{index}"
        uncertainty_handles.append(handle)
        objects.append(DossierObject(
            handle=handle, object_kind="uncertainty", canonical_id=item.provenance_id,
            semantic_content={
                "question": item.question, "status": item.status,
                "discriminates_between": [
                    explanation_handle_by_provenance[value]
                    for value in item.discriminates_between
                ],
                "retrieval_target": item.retrieval_target,
            },
        ))

    _validate_annotation_closure(evidence_annotations, handle_by_id)
    _validate_handle_registry(objects)
    event_order = tuple(f"O{index}" for index in range(1, len(assertions) + 1))
    model_handles = tuple(f"M{index}" for index in range(1, len(models) + 1))
    direct_handles = tuple(ref.object_handle for ref in evidence_refs["direct"] if ref.object_handle.startswith("O"))
    opportunity, reasons = _mechanism_opportunity(models, mechanism, direct_handles)
    window = {
        "start_at": assertions[0].occurred_at.isoformat(),
        "end_at": assertions[-1].occurred_at.isoformat(),
        "as_of_at": as_of_at.isoformat(),
        "ordering": "occurred_at_observation_id",
    }
    scope = {
        "canonical_ref": scope_ref,
        "display_label": display_label,
        "coordinate_authority": "resolved",
        "episode_id": episode.episode_id,
    }
    bindings = [
        {
            "handle": item.handle, "kind": item.object_kind,
            "canonical_id": item.canonical_id, "version": item.exact_version_id,
            "scope": item.canonical_scope_ref,
        }
        for item in objects
    ]
    body = {
        "schema_version": "synthesis-dossier-v1",
        "tenant_id": str(tenant_id), "scope": scope, "window": window,
        "bindings": bindings,
        "objects": [asdict(item) for item in objects],
        "evidence": {
            role: [asdict(item) for item in values]
            for role, values in evidence_refs.items()
        },
        "mechanism": mechanism,
        "open_uncertainty": uncertainty_handles,
        "considered_explanations": explanation_handles,
    }
    binding_digest = canonical_sha256(bindings)
    content_digest = canonical_sha256(body)
    dossier_id = f"DOS_{content_digest[:24]}"
    receipt = AssemblyReceipt(
        input_episode_ids=(episode.episode_id,),
        input_object_count=len(episode.assertions) + len(model_heads),
        included_object_count=len(objects),
        excluded_object_count=sum(excluded.values()),
        exclusion_reasons=dict(sorted(excluded.items())),
        handle_binding_digest=binding_digest,
        content_digest=content_digest,
        canonical_scope_closed=True,
        chronology_valid=True,
        evidence_closure_valid=True,
        mechanism_opportunity=opportunity,
        readiness_reasons=reasons,
    )
    return SynthesisDossier(
        schema_version="synthesis-dossier-v1", dossier_id=dossier_id,
        tenant_id=tenant_id, scope=scope, window=window, handles=tuple(objects),
        event_order=event_order, accepted_model_heads=model_handles,
        direct_observations=direct_handles,
        supporting_evidence=tuple(evidence_refs["direct"] + evidence_refs["transitive"]),
        contradictory_evidence=tuple(evidence_refs["contradictory"]),
        auxiliary_evidence=tuple(evidence_refs["auxiliary"]),
        open_uncertainty=tuple(uncertainty_handles),
        candidate_mechanism_slots={key: tuple(value) for key, value in mechanism.items()},
        considered_explanations=tuple(explanation_handles),
        discriminating_missing_evidence=tuple(uncertainty_handles),
        assembly_receipt=receipt,
    )


def _resolved_scope(episode: GovernedLearningEpisode) -> str:
    scope = str(episode.canonical_ref or "").strip()
    if not scope or scope.startswith("mention:") or ":" not in scope:
        raise DossierContractError("dossier requires one resolved canonical scope")
    refs = {str(item.canonical_ref or "") for item in episode.assertions}
    if refs != {scope}:
        raise DossierContractError("episode contains ambiguous or cross-scope assertions")
    if any(item.coordinate_authority != "resolved" for item in episode.assertions):
        raise DossierContractError("provisional or unresolved coordinates are forbidden")
    return scope


def _eligible_assertions(
    episode: GovernedLearningEpisode,
    tenant_id: UUID,
    scope_ref: str,
    as_of_at: datetime,
) -> tuple[list[GovernedObservationAssertion], dict[str, int]]:
    included: list[GovernedObservationAssertion] = []
    excluded: dict[str, int] = {}
    seen: set[UUID] = set()
    for item in episode.assertions:
        reason = None
        if item.tenant_id != tenant_id:
            reason = "cross_tenant"
        elif item.canonical_ref != scope_ref:
            reason = "cross_scope"
        elif item.occurred_at > as_of_at:
            reason = "future_observation"
        elif not item.assertion_text.strip() or not item.evidence_address.strip():
            reason = "malformed_observation"
        elif item.observation_id in seen:
            reason = "duplicate_observation"
        if reason:
            excluded[reason] = excluded.get(reason, 0) + 1
            continue
        seen.add(item.observation_id)
        included.append(item)
    included.sort(key=lambda row: (row.occurred_at, str(row.observation_id)))
    return included, excluded


def _eligible_models(
    models: Sequence[ModelHeadInput], scope_ref: str, as_of_at: datetime,
) -> tuple[list[ModelHeadInput], dict[str, int]]:
    included: list[ModelHeadInput] = []
    excluded: dict[str, int] = {}
    seen: set[tuple[UUID, UUID]] = set()
    for item in models:
        reason = None
        key = (item.model_id, item.truth_version_id)
        effective = item.valid_as_of or item.truth_advanced_at
        if not item.accepted_current:
            reason = "stale_model"
        elif item.canonical_scope_ref != scope_ref:
            reason = "cross_scope_model"
        elif effective and effective > as_of_at:
            reason = "future_model"
        elif not item.natural_text.strip() or not isinstance(item.proposition, Mapping):
            reason = "malformed_model"
        elif key in seen:
            reason = "duplicate_model_version"
        if reason:
            excluded[reason] = excluded.get(reason, 0) + 1
            continue
        seen.add(key)
        included.append(item)
    included.sort(key=lambda row: (
        row.valid_as_of or row.truth_advanced_at or datetime.min.replace(tzinfo=as_of_at.tzinfo),
        str(row.model_id), str(row.truth_version_id),
    ))
    return included, excluded


def _annotation_index(
    annotations: Sequence[EvidenceAnnotation],
) -> dict[tuple[str, UUID], EvidenceAnnotation]:
    out: dict[tuple[str, UUID], EvidenceAnnotation] = {}
    for item in annotations:
        key = (item.object_kind, item.object_id)
        if key in out:
            raise DossierContractError("duplicate evidence annotation")
        out[key] = item
    return out


def _add_mechanism_handles(
    mechanism: dict[str, list[str]], handle: str, annotation: EvidenceAnnotation | None,
) -> None:
    if annotation is None:
        return
    keys = {"cause": "causes", "condition": "conditions", "outcome": "outcomes"}
    for role in annotation.mechanism_roles:
        mechanism[keys[role]].append(handle)


def _validate_annotation_closure(
    annotations: Sequence[EvidenceAnnotation], handles: Mapping[UUID, str],
) -> None:
    unknown = [item for item in annotations if item.object_id not in handles]
    if unknown:
        raise DossierContractError("evidence annotation references excluded or unknown object")


def _validate_handle_registry(objects: Sequence[DossierObject]) -> None:
    handles = [item.handle for item in objects]
    identities = [(item.object_kind, item.canonical_id, item.exact_version_id) for item in objects]
    if len(handles) != len(set(handles)) or len(identities) != len(set(identities)):
        raise DossierContractError("dossier handle registry is not one-to-one")


def _mechanism_opportunity(
    models: Sequence[ModelHeadInput],
    mechanism: Mapping[str, Sequence[str]],
    direct_handles: Sequence[str],
) -> tuple[MechanismOpportunity, tuple[str, ...]]:
    has_prior = len(models) >= 2
    has_current = bool(direct_handles)
    has_mechanism = bool(mechanism["causes"] or mechanism["conditions"])
    has_outcome = bool(mechanism["outcomes"])
    if has_prior and has_current and has_mechanism and has_outcome:
        return "mature", (
            "two_or_more_exact_current_heads", "current_direct_evidence",
            "typed_cause_or_condition", "typed_outcome", "closed_scope_and_handles",
        )
    if has_current and (has_prior or has_mechanism or has_outcome):
        missing = []
        if not has_prior:
            missing.append("needs_two_exact_current_heads")
        if not has_mechanism:
            missing.append("needs_typed_cause_or_condition")
        if not has_outcome:
            missing.append("needs_typed_outcome")
        return "immature", tuple(missing)
    return "none", ("no_structural_mechanism_opportunity",)


def _display_label(assertions: Sequence[GovernedObservationAssertion]) -> str:
    counts: dict[str, int] = {}
    labels: dict[str, str] = {}
    for item in assertions:
        label = " ".join(str(item.governed_surface or "").split())
        if not label:
            continue
        key = label.casefold()
        counts[key] = counts.get(key, 0) + 1
        labels.setdefault(key, label)
    if not counts:
        raise DossierContractError("resolved scope requires a governed display label")
    key = min(counts, key=lambda value: (-counts[value], value))
    return labels[key]


def _provider_safe(value: Any) -> Any:
    """Remove compiler-owned identity fields from provider-visible semantics."""
    if isinstance(value, Mapping):
        return {
            str(key): _provider_safe(item)
            for key, item in value.items()
            if str(key) not in {
                "evidence_address", "member_model_ids", "evidence_observation_ids",
                "canonical_ref", "scope_ref", "model_id", "truth_version_id",
            }
            and not str(key).endswith("_uuid")
        }
    if isinstance(value, (list, tuple)):
        return [_provider_safe(item) for item in value]
    if isinstance(value, UUID):
        raise DossierContractError("provider semantic content contains a canonical UUID")
    if isinstance(value, str):
        try:
            UUID(value)
        except ValueError:
            return value
        raise DossierContractError("provider semantic content contains a canonical UUID")
    return value


__all__ = [
    "AssemblyReceipt", "DossierContractError", "DossierObject",
    "DurableExplanationInput", "DurableUncertaintyInput", "EvidenceAnnotation",
    "EvidenceReference", "ModelHeadInput", "SynthesisDossier",
    "assemble_synthesis_dossier",
]
