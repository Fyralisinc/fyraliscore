"""Frozen TI2 synthesis/abstention contract and deterministic compiler.

Provider-facing decisions use dossier-local handles only.  Canonical identity,
exact head versions, evidence closure, and mutation commands remain compiler
owned.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from lib.shared.edge_registry import EDGE_REGISTRY
from lib.shared.ids import uuid7

from .diff_schema import ClaimOp, RawDiff, RelationClaimOp


CONTRACT_DIGEST = "8f90d9ecc723d61253f3e678fece1122967d280dcf8d8c23f1997d04d36c7a8f"
LocalHandle = Annotated[str, Field(pattern=r"^(?:M|O)[1-9][0-9]{0,2}$")]
SemanticRole = Literal[
    "cause", "effect", "support", "counterevidence", "novelty_reference",
]
SynthesisRelationKind = Literal[
    "blocks", "depends_on", "causes", "influences", "predicts",
]


class EvidenceAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    handle: LocalHandle
    bearing: Literal["weakens", "contradicts"]
    explanation: str = Field(min_length=1, max_length=500)


class AlternativeAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    thesis: str = Field(min_length=1, max_length=500)
    mechanism: str = Field(min_length=1, max_length=700)
    supporting_handles: list[LocalHandle] = Field(default_factory=list, max_length=8)
    why_weaker: str = Field(min_length=1, max_length=500)


class NoveltyAssessment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    classification: Literal["novel", "extends", "confirms", "duplicates"]
    relative_to_model_handles: list[LocalHandle] = Field(default_factory=list, max_length=8)
    explanation: str = Field(min_length=1, max_length=500)

    @model_validator(mode="after")
    def require_exact_prior_for_comparison(self) -> "NoveltyAssessment":
        if self.classification != "novel" and not self.relative_to_model_handles:
            raise ValueError("non-novel decisions require a relative Model handle")
        if any(not handle.startswith("M") for handle in self.relative_to_model_handles):
            raise ValueError("novelty references must be Model handles")
        return self


class SemanticRelationProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    relation_kind: SynthesisRelationKind
    source_handles: list[LocalHandle] = Field(
        min_length=1,
        max_length=8,
        description=(
            "Subset of cause_condition_handles containing at least one accepted "
            "Model handle and no effect handle."
        ),
    )
    target: Literal["synthesis_output"]
    direction: Literal["source_to_target"]
    explanation: str = Field(min_length=1, max_length=500)


class SynthesisProposal(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["synthesis"]
    thesis: str = Field(min_length=1, max_length=500)
    mechanism: str = Field(min_length=1, max_length=1000)
    cause_condition_handles: list[LocalHandle] = Field(min_length=1, max_length=8)
    effect_handles: list[LocalHandle] = Field(min_length=1, max_length=8)
    supporting_evidence_handles: list[LocalHandle] = Field(
        min_length=1,
        max_length=16,
        description="Closed support handles including at least one direct O handle.",
    )
    counterevidence: list[EvidenceAssessment] = Field(default_factory=list, max_length=8)
    strongest_alternative: AlternativeAssessment
    novelty: NoveltyAssessment
    confidence: float = Field(ge=0.0, le=1.0)
    falsifying_evidence: list[str] = Field(min_length=1, max_length=8)
    relation: SemanticRelationProposal

    @model_validator(mode="after")
    def require_closed_relation_and_direct_support(self) -> "SynthesisProposal":
        causes = set(self.cause_condition_handles)
        effects = set(self.effect_handles)
        sources = set(self.relation.source_handles)
        if not sources <= causes:
            raise ValueError("relation sources must be a subset of cause_condition handles")
        if sources & effects:
            raise ValueError("relation sources cannot contain effect handles")
        if not any(handle.startswith("M") for handle in sources):
            raise ValueError("relation requires at least one accepted Model source")
        if not any(handle.startswith("O") for handle in self.supporting_evidence_handles):
            raise ValueError("supporting evidence requires at least one direct observation")
        return self


class AbstentionDecision(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["abstain"]
    reason_code: Literal[
        "insufficient_evidence", "conflicting_evidence", "no_coherent_mechanism",
        "not_novel", "out_of_scope",
    ]
    explanation: str = Field(min_length=1, max_length=700)
    missing_evidence: list[str] = Field(min_length=1, max_length=8)
    relevant_handles: list[LocalHandle] = Field(default_factory=list, max_length=16)
    strongest_alternative: AlternativeAssessment | None = None
    confidence: float = Field(ge=0.0, le=1.0)


Decision = Annotated[SynthesisProposal | AbstentionDecision, Field(discriminator="kind")]


class SynthesisProviderDecision(BaseModel):
    """Identity-free semantic decision accepted directly from the provider."""

    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["think-synthesis-provider-decision-v2"]
    decision: Decision


class SynthesisDecisionEnvelope(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["think-synthesis-decision-v1"]
    dossier_id: str = Field(min_length=1, max_length=120)
    dossier_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    decision: Decision


def bind_synthesis_provider_decision(
    provider_decision: SynthesisProviderDecision,
    *,
    dossier_id: str,
    dossier_digest: str,
) -> SynthesisDecisionEnvelope:
    """Bind trusted closed-dossier identity after provider schema validation."""
    return SynthesisDecisionEnvelope(
        schema_version="think-synthesis-decision-v1",
        dossier_id=dossier_id,
        dossier_digest=dossier_digest,
        decision=provider_decision.decision,
    )


@dataclass(frozen=True)
class HandleBinding:
    handle: str
    object_kind: Literal["accepted_model_head", "observation"]
    canonical_id: UUID
    exact_version_id: UUID | None
    tenant_id: UUID
    canonical_scope_ref: str
    authority: str
    allowed_roles: frozenset[SemanticRole]
    current_accepted: bool = True


@dataclass(frozen=True)
class SynthesisCompileContext:
    dossier_id: str
    dossier_digest: str
    tenant_id: UUID
    canonical_scope_ref: str
    trigger_ref: UUID
    trigger_observation_ids: frozenset[UUID]
    bindings: tuple[HandleBinding, ...]


class SynthesisContractError(ValueError):
    """The semantic proposal cannot cross the pre-mutation boundary."""


def compile_synthesis_decision(
    envelope: SynthesisDecisionEnvelope,
    *,
    context: SynthesisCompileContext,
) -> RawDiff:
    """Compile one closed decision into zero ops or one atomic claim/relation pair."""
    if envelope.dossier_id != context.dossier_id or envelope.dossier_digest != context.dossier_digest:
        raise SynthesisContractError("dossier identity or digest mismatch")
    by_handle = _validate_binding_table(context)
    decision = envelope.decision
    if isinstance(decision, AbstentionDecision):
        _resolve_handles(decision.relevant_handles, by_handle, role=None)
        if decision.strongest_alternative:
            _resolve_handles(decision.strongest_alternative.supporting_handles, by_handle, role="support")
        return RawDiff(trigger_ref=context.trigger_ref, tenant_id=context.tenant_id,
                       reasoning_trace="ti2_synthesis_decision=abstain")

    groups = {
        "cause": decision.cause_condition_handles,
        "effect": decision.effect_handles,
        "support": decision.supporting_evidence_handles,
        "counterevidence": [row.handle for row in decision.counterevidence],
        "novelty_reference": decision.novelty.relative_to_model_handles,
    }
    for role, handles in groups.items():
        _require_unique(handles, f"repeated {role} handle")
        _resolve_handles(handles, by_handle, role=role)  # type: ignore[arg-type]
    _resolve_handles(decision.strongest_alternative.supporting_handles, by_handle, role="support")
    if set(decision.cause_condition_handles) & set(decision.effect_handles):
        raise SynthesisContractError("source/effect overlap")
    _require_unique(decision.relation.source_handles, "duplicate relation source")
    if not set(decision.relation.source_handles) <= set(decision.cause_condition_handles):
        raise SynthesisContractError("relation sources are outside causes")
    if decision.relation.relation_kind not in EDGE_REGISTRY:
        raise SynthesisContractError("unsupported governed relation kind")
    source_bindings = _resolve_handles(decision.relation.source_handles, by_handle, role="cause")
    canonical_sources = [
        row for row in source_bindings if row.object_kind == "accepted_model_head"
    ]
    if not canonical_sources:
        raise SynthesisContractError("canonical relation sources must be accepted Model heads")
    support = _resolve_handles(decision.supporting_evidence_handles, by_handle, role="support")
    direct_observations = [row for row in support if row.object_kind == "observation"]
    if not direct_observations:
        raise SynthesisContractError("synthesis requires direct observation support")

    members = _ordered_bindings(
        [*decision.cause_condition_handles, *decision.effect_handles,
         *decision.supporting_evidence_handles,
         *decision.novelty.relative_to_model_handles], by_handle,
        object_kind="accepted_model_head",
    )
    if len(members) < 2:
        raise SynthesisContractError("composite synthesis requires two exact accepted Model members")
    placeholder = uuid7()
    evidence_ids = [row.canonical_id for row in direct_observations]
    source = canonical_sources[0]
    closure_target = next(
        (row for row in members if row.canonical_id != source.canonical_id), None,
    )
    if closure_target is None:
        raise SynthesisContractError("synthesis relation closure requires distinct Model members")
    effect_observation_ids = {
        row.canonical_id for row in _resolve_handles(
            decision.effect_handles, by_handle, role="effect",
        ) if row.object_kind == "observation"
    }
    opener = next(
        (row for row in direct_observations if row.canonical_id in effect_observation_ids),
        direct_observations[0],
    )
    canonical_relation_kind = {
        "blocks": "dependency_constraint",
        "depends_on": "dependency_constraint",
        "causes": "causal_influence",
        "influences": "causal_influence",
        "predicts": "predictive_indicator",
    }.get(decision.relation.relation_kind, decision.relation.relation_kind)
    proposition = {
        "kind": "situation", "claim_role": "situation", "abstraction_level": "composite",
        "situation": decision.thesis, "summary": decision.thesis,
        "shared_mechanism": decision.mechanism,
        "member_model_ids": [str(row.canonical_id) for row in members],
        "member_model_version_ids": [str(row.exact_version_id) for row in members],
        "evidence_event_ids": [str(value) for value in evidence_ids],
        "counterevidence": [row.model_dump() for row in decision.counterevidence],
        "strongest_alternative": decision.strongest_alternative.model_dump(),
        "novelty": decision.novelty.model_dump(),
        "falsifying_evidence": decision.falsifying_evidence,
        # Admission verifies exact member heads through this embedded closure
        # certificate.  It is not a second persisted relation command.
        "supported_relation": {"kind": canonical_relation_kind,
            "mechanism": decision.mechanism,
            "source_model_id": str(source.canonical_id),
            "target_model_id": str(closure_target.canonical_id),
            "source_model_version_id": str(source.exact_version_id),
            "target_model_version_id": str(closure_target.exact_version_id)},
        "synthesis_contract": True, "synthesis_contract_version": envelope.schema_version,
        "contract_digest": CONTRACT_DIGEST, "dossier_id": envelope.dossier_id,
        "dossier_digest": envelope.dossier_digest,
    }
    claim = ClaimOp(op="insert", entry={
        "tenant_id": str(context.tenant_id), "born_from_event_id": str(placeholder),
        # Truth admission accepts one conclusion opener; the proposition keeps
        # the complete closed support set without discarding provider semantics.
        "supporting_event_ids": [str(opener.canonical_id)],
        "proposition": proposition, "natural": decision.thesis,
        "confidence": decision.confidence, "confidence_at_assertion": decision.confidence,
        "scope_entities": [{"type": context.canonical_scope_ref.split(":", 1)[0],
                            "id": context.canonical_scope_ref,
                            "canonical_ref": context.canonical_scope_ref}],
        "scope_actors": [], "scope_temporal": {},
        "falsifier": {"kind": "evidence_condition",
                      "pattern": decision.falsifying_evidence[0]},
    })
    relation = RelationClaimOp(
        source_model_id=source.canonical_id, target_model_id=placeholder,
        source_model_version_id=source.exact_version_id,
        predicate=canonical_relation_kind, edge_kind=decision.relation.relation_kind,
        direction="source_to_target", endpoint_binding_status="bound",
        write_policy="accepted_edge", status="accepted", confidence=decision.confidence,
        binding_confidence=1.0, evidence_event_ids=evidence_ids,
        evidence_model_ids=[row.canonical_id for row in members],
        evidence_text=decision.mechanism, explanation=decision.relation.explanation,
        semantic_scope=[context.canonical_scope_ref], metadata={
            "relation_claim_origin": "ti2_synthesis_contract",
            "synthesis_contract": True, "atomic_with_synthesis": True,
            "target_claim_placeholder": str(placeholder), "contract_digest": CONTRACT_DIGEST,
            "dossier_digest": envelope.dossier_digest,
        },
    )
    return RawDiff(trigger_ref=context.trigger_ref, tenant_id=context.tenant_id,
                   claim_ops=[claim], relation_claim_ops=[relation],
                   reasoning_trace="ti2_synthesis_decision=synthesis")


def _validate_binding_table(context: SynthesisCompileContext) -> dict[str, HandleBinding]:
    by_handle: dict[str, HandleBinding] = {}
    canonical: set[tuple[str, UUID]] = set()
    for row in context.bindings:
        if not re.fullmatch(r"(?:M|O)[1-9][0-9]{0,2}", row.handle):
            raise SynthesisContractError("invalid local handle")
        if row.handle in by_handle or (row.object_kind, row.canonical_id) in canonical:
            raise SynthesisContractError("duplicate handle binding")
        if row.tenant_id != context.tenant_id or row.canonical_scope_ref != context.canonical_scope_ref:
            raise SynthesisContractError("binding tenant or scope mismatch")
        if row.object_kind == "accepted_model_head":
            if not row.handle.startswith("M") or row.exact_version_id is None or not row.current_accepted:
                raise SynthesisContractError("stale or malformed Model binding")
        elif not row.handle.startswith("O") or row.exact_version_id is not None:
            raise SynthesisContractError("malformed observation binding")
        elif row.canonical_id not in context.trigger_observation_ids:
            raise SynthesisContractError("observation outside trigger closure")
        by_handle[row.handle] = row
        canonical.add((row.object_kind, row.canonical_id))
    return by_handle


def _resolve_handles(handles: list[str], bindings: dict[str, HandleBinding],
                     *, role: SemanticRole | None) -> list[HandleBinding]:
    rows: list[HandleBinding] = []
    for handle in handles:
        row = bindings.get(handle)
        if row is None:
            raise SynthesisContractError(f"unknown handle {handle}")
        if role is not None and role not in row.allowed_roles:
            raise SynthesisContractError(f"unauthorized {role} handle {handle}")
        rows.append(row)
    return rows


def _require_unique(handles: list[str], message: str) -> None:
    if len(handles) != len(set(handles)):
        raise SynthesisContractError(message)


def _ordered_bindings(handles: list[str], bindings: dict[str, HandleBinding],
                      *, object_kind: str) -> list[HandleBinding]:
    seen: set[str] = set()
    return [bindings[handle] for handle in handles
            if not (handle in seen or seen.add(handle))
            and bindings[handle].object_kind == object_kind]


__all__ = [
    "AbstentionDecision", "AlternativeAssessment", "CONTRACT_DIGEST",
    "EvidenceAssessment", "HandleBinding", "NoveltyAssessment",
    "SemanticRelationProposal", "SynthesisCompileContext", "SynthesisContractError",
    "SynthesisDecisionEnvelope", "SynthesisProposal", "SynthesisProviderDecision",
    "bind_synthesis_provider_decision", "compile_synthesis_decision",
]
