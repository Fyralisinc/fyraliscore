"""
services/domain/models/propositions.py — four-stance proposition validation.

`proposition.kind` is intentionally small: observation, belief,
prediction, norm. Semantic shape lives in memory-grammar fields such as
`claim_role`, `domain_tags`, `abstraction_level`, and `legacy_kind`.

Legacy twelve-kind payloads are accepted at the boundary and normalized
into the four-stance grammar so older rows, fixtures, and callers remain
readable while new writes converge on the smaller ontology.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError
from pydantic import TypeAdapter, model_validator

from lib.shared.claim_role_registry import validate_claim_role_contract
from lib.shared.errors import ValidationError
from lib.shared.types import PropositionKind


CanonicalPropositionKind = Literal["observation", "belief", "prediction", "norm"]
LegacyPropositionKind = Literal[
    "state",
    "relation",
    "pattern",
    "pattern_instance",
    "capability_assessment",
    "hypothesis",
    "concern",
    "market_assessment",
    "environmental_trend",
    "situation",
    "recommendation",
]

SituationPressureType = Literal[
    "capacity",
    "trust",
    "revenue",
    "compliance",
    "decision",
    "execution",
    "market",
    "resource",
]

_CANONICAL_KINDS = frozenset({"observation", "belief", "prediction", "norm"})
_LEGACY_TO_STANCE: dict[str, str] = {
    "state": "belief",
    "relation": "belief",
    "pattern": "belief",
    "pattern_instance": "belief",
    "capability_assessment": "belief",
    "hypothesis": "belief",
    "concern": "belief",
    "market_assessment": "belief",
    "environmental_trend": "belief",
    "situation": "belief",
    "recommendation": "norm",
    "prediction": "prediction",
}
_LEGACY_GRAMMAR: dict[str, dict[str, Any]] = {
    "state": {
        "claim_role": "fact",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "observed",
        "polarity": "neutral",
    },
    "relation": {
        "claim_role": "relation",
        "abstraction_level": "relationship",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "neutral",
    },
    "pattern": {
        "claim_role": "pattern",
        "abstraction_level": "pattern",
        "time_mode": "recurring",
        "modality": "inferred",
        "polarity": "neutral",
    },
    "pattern_instance": {
        "claim_role": "pattern",
        "abstraction_level": "atomic",
        "time_mode": "past",
        "modality": "observed",
        "polarity": "neutral",
    },
    "capability_assessment": {
        "claim_role": "capability",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "neutral",
    },
    "hypothesis": {
        "claim_role": "hypothesis",
        "abstraction_level": "atomic",
        "time_mode": "unspecified",
        "modality": "inferred",
        "polarity": "neutral",
    },
    "concern": {
        "claim_role": "concern",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "negative",
    },
    "market_assessment": {
        "claim_role": "fact",
        "abstraction_level": "atomic",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "neutral",
        "domain_tags": ["market"],
    },
    "environmental_trend": {
        "claim_role": "pattern",
        "abstraction_level": "pattern",
        "time_mode": "recurring",
        "modality": "inferred",
        "polarity": "neutral",
    },
    "situation": {
        "claim_role": "situation",
        "abstraction_level": "composite",
        "time_mode": "current",
        "modality": "inferred",
        "polarity": "mixed",
    },
    "recommendation": {
        "claim_role": "recommendation",
        "abstraction_level": "atomic",
        "time_mode": "future",
        "modality": "normative",
        "polarity": "mixed",
    },
    "prediction": {
        "claim_role": "prediction",
        "abstraction_level": "atomic",
        "time_mode": "future",
        "modality": "expected",
        "polarity": "neutral",
    },
}

_LEGAL_ACT_REF_TYPES = frozenset({"goal", "commitment", "decision", "resource"})
_LEGAL_PROPOSED_OPS = frozenset({"create", "update", "archive", "transition"})
_SITUATION_PRESSURE_TYPES = frozenset(SituationPressureType.__args__)  # type: ignore[attr-defined]
_DEFAULT_SITUATION_PRESSURE_TYPE = "execution"
_DEFAULT_SHARED_MECHANISM = "Situation members share an operational mechanism."


class _PropositionBase(BaseModel):
    model_config = ConfigDict(extra="allow", str_strip_whitespace=False)

    legacy_kind: str | None = None
    claim_role: str | None = None
    abstraction_level: str | None = None
    time_mode: str | None = None
    modality: str | None = None
    polarity: str | None = None
    domain_tags: list[str] | None = None

    @model_validator(mode="after")
    def _check_common_shape(self) -> "_PropositionBase":
        if self.domain_tags is not None:
            if any((not isinstance(tag, str) or not tag.strip()) for tag in self.domain_tags):
                raise ValueError("domain_tags entries must be non-empty strings")
        return self


class ObservationProposition(_PropositionBase):
    kind: Literal["observation"] = "observation"
    subject: str | dict[str, Any] | None = None
    event: str | dict[str, Any] | None = None
    assertion: str | None = None
    summary: str | None = None

    @model_validator(mode="after")
    def _check_observation_content(self) -> "ObservationProposition":
        _require_some_content(self, ("event", "assertion", "summary"))
        return self


class BeliefProposition(_PropositionBase):
    kind: Literal["belief"] = "belief"
    subject: str | dict[str, Any] | None = None
    assertion: str | None = None
    summary: str | None = None
    claim: str | None = None
    situation: str | None = None
    member_model_ids: list[str] | None = None
    relationship_summary: str | None = None
    status: str | None = None
    pressure_type: SituationPressureType | None = None
    shared_mechanism: str | None = None
    judgment_change: str | None = None
    affected_decisions: list[str] | None = None
    affected_customers: list[str] | None = None
    affected_teams: list[str] | None = None
    evidence_event_ids: list[str] | None = None
    open_falsifier: str | None = None

    @model_validator(mode="after")
    def _check_belief_content(self) -> "BeliefProposition":
        _require_some_content(
            self,
            (
                "assertion",
                "summary",
                "claim",
                "relation",
                "object",
                "signature",
                "observed_tendency",
                "matched_context",
                "assessment",
                "hypothesis_text",
                "nature",
                "situation",
            ),
        )
        _check_situation_shape(self)
        return self


class PredictionProposition(_PropositionBase):
    kind: Literal["prediction"] = "prediction"
    expected: str | dict[str, Any]
    resolution: str | dict[str, Any]


class NormProposition(_PropositionBase):
    kind: Literal["norm"] = "norm"
    subject: str | dict[str, Any] | None = None
    desired_state: str | dict[str, Any] | None = None
    goal: str | dict[str, Any] | None = None
    policy: str | dict[str, Any] | None = None
    rationale: str | None = None

    target_act_ref: dict[str, Any] | None = None
    proposed_change: dict[str, Any] | None = None
    expected_impact: float | None = None
    qualitative_impact: str | None = None
    target_actor_id: str | None = Field(default=None)

    @model_validator(mode="after")
    def _check_norm_content(self) -> "NormProposition":
        is_recommendation = (
            self.legacy_kind == "recommendation"
            or self.claim_role == "recommendation"
            or self.proposed_change is not None
        )
        if is_recommendation:
            _check_recommendation_shape(self)
        else:
            _require_some_content(
                self,
                ("desired_state", "goal", "policy", "rationale", "summary", "assertion"),
            )
        return self


def _require_some_content(model: BaseModel, names: tuple[str, ...]) -> None:
    extras = getattr(model, "__pydantic_extra__", None) or {}
    for name in names:
        value = getattr(model, name, None)
        if value is None:
            value = extras.get(name)
        if isinstance(value, str) and value.strip():
            return
        if isinstance(value, (dict, list)) and value:
            return
    raise ValueError(f"one of {', '.join(names)} must be supplied")


def _check_situation_shape(model: BaseModel) -> None:
    extras = getattr(model, "__pydantic_extra__", None) or {}
    if getattr(model, "legacy_kind", None) != "situation" and getattr(model, "claim_role", None) != "situation":
        return
    situation = getattr(model, "situation", None) or extras.get("situation")
    summary = getattr(model, "summary", None) or extras.get("summary")
    relationship_summary = getattr(model, "relationship_summary", None) or extras.get("relationship_summary")
    if isinstance(situation, str) and not situation.strip():
        raise ValueError("situation must be non-empty")
    if isinstance(summary, str) and not summary.strip():
        raise ValueError("summary must be non-empty")
    if isinstance(relationship_summary, str) and not relationship_summary.strip():
        raise ValueError("relationship_summary must be non-empty")
    member_ids = getattr(model, "member_model_ids", None) or extras.get("member_model_ids")
    if isinstance(member_ids, list) and len(set(map(str, member_ids))) != len(member_ids):
        raise ValueError("member_model_ids must not contain duplicates")
    pressure_type = getattr(model, "pressure_type", None) or extras.get("pressure_type")
    if pressure_type is not None and pressure_type not in _SITUATION_PRESSURE_TYPES:
        raise ValueError("pressure_type is not a legal situation pressure")
    for field_name in ("shared_mechanism", "judgment_change", "open_falsifier"):
        value = getattr(model, field_name, None) or extras.get(field_name)
        if value is not None and (not isinstance(value, str) or not value.strip()):
            raise ValueError(f"{field_name} must be non-empty when provided")


def _check_recommendation_shape(model: NormProposition) -> None:
    if "target_actor_id" not in model.model_fields_set:
        raise ValueError("recommendation.target_actor_id field is required")
    if model.proposed_change is None:
        raise ValueError("proposed_change is required for recommendation norms")
    op = model.proposed_change.get("operation")
    if op not in _LEGAL_PROPOSED_OPS:
        raise ValueError(
            f"proposed_change.operation must be one of {sorted(_LEGAL_PROPOSED_OPS)}; got {op!r}"
        )
    if not isinstance(model.proposed_change.get("payload"), dict):
        raise ValueError("proposed_change.payload must be a dict")

    if model.target_act_ref is not None:
        ref_type = model.target_act_ref.get("type")
        ref_id = model.target_act_ref.get("id")
        if ref_type not in _LEGAL_ACT_REF_TYPES:
            raise ValueError(
                f"target_act_ref.type must be one of {sorted(_LEGAL_ACT_REF_TYPES)}; got {ref_type!r}"
            )
        if ref_id is None:
            if op != "create":
                raise ValueError(
                    "target_act_ref.id may be null only for proposed_change.operation='create'"
                )
        elif not isinstance(ref_id, str) or not ref_id:
            raise ValueError("target_act_ref.id must be a non-empty UUID string")

    if model.expected_impact is None and not (
        model.qualitative_impact and model.qualitative_impact.strip()
    ):
        raise ValueError(
            "either expected_impact or qualitative_impact must be supplied"
        )
    if model.target_actor_id is not None and (
        not isinstance(model.target_actor_id, str) or not model.target_actor_id
    ):
        raise ValueError("target_actor_id must be a non-empty UUID string")


PropositionModel = Annotated[
    Union[
        ObservationProposition,
        BeliefProposition,
        PredictionProposition,
        NormProposition,
    ],
    Field(discriminator="kind"),
]

_ADAPTER: TypeAdapter[Any] = TypeAdapter(PropositionModel)
LEGAL_KINDS: frozenset[str] = _CANONICAL_KINDS
LEGACY_KINDS: frozenset[str] = frozenset(_LEGACY_TO_STANCE.keys()) - _CANONICAL_KINDS


def canonicalize_proposition(raw: dict[str, Any]) -> dict[str, Any]:
    """Return a new proposition using the four canonical stance kinds."""
    if not isinstance(raw, dict):
        raise ValidationError(
            f"proposition must be a dict; got {type(raw).__name__}",
            field="proposition",
        )
    kind = raw.get("kind")
    if not isinstance(kind, str) or not kind:
        raise ValidationError(
            "proposition missing 'kind' discriminator",
            field="proposition.kind",
        )
    if kind in _CANONICAL_KINDS:
        canonical = dict(raw)
        _apply_stance_defaults(canonical)
        return canonical
    stance = _LEGACY_TO_STANCE.get(kind)
    if stance is None:
        raise ValidationError(
            f"unknown proposition kind {kind!r}; must be one of {sorted(LEGAL_KINDS)}",
            field="proposition.kind",
            value=kind,
        )
    canonical = dict(raw)
    canonical["kind"] = stance
    canonical.setdefault("legacy_kind", kind)
    for key, value in _LEGACY_GRAMMAR.get(kind, {}).items():
        if key == "domain_tags":
            existing = canonical.get("domain_tags")
            tags = [str(tag) for tag in existing] if isinstance(existing, list) else []
            for tag in value:
                if tag not in tags:
                    tags.append(tag)
            canonical["domain_tags"] = tags
        else:
            canonical.setdefault(key, value)
    _apply_stance_defaults(canonical)
    return canonical


def ensure_situation_compositional_defaults(entry: dict[str, Any]) -> None:
    """Mutate a ModelCreate-like entry so situation inserts are complete.

    The DB requires `pressure_type` and `shared_mechanism` for situation
    Models. We also backfill `judgment_change` and `open_falsifier` so
    synthesized situations are useful retrieval/action anchors even when
    the provider emits only the legacy five-field shape.
    """
    prop = entry.get("proposition")
    if not _is_situation_payload(prop):
        return

    pressure = prop.get("pressure_type")
    if not isinstance(pressure, str) or pressure not in _SITUATION_PRESSURE_TYPES:
        prop["pressure_type"] = _DEFAULT_SITUATION_PRESSURE_TYPE

    mechanism = prop.get("shared_mechanism")
    if not isinstance(mechanism, str) or not mechanism.strip():
        fallback = (
            prop.get("relationship_summary")
            or prop.get("summary")
            or entry.get("natural")
            or prop.get("situation")
            or _DEFAULT_SHARED_MECHANISM
        )
        prop["shared_mechanism"] = str(fallback).strip() or _DEFAULT_SHARED_MECHANISM

    judgment_change = prop.get("judgment_change")
    if not isinstance(judgment_change, str) or not judgment_change.strip():
        prop["judgment_change"] = (
            "Together, the member models should be treated as one "
            f"operational situation: {prop['shared_mechanism']}"
        )

    open_falsifier = prop.get("open_falsifier")
    if not isinstance(open_falsifier, str) or not open_falsifier.strip():
        prop["open_falsifier"] = (
            _falsifier_text(entry.get("falsifier"))
            or "The shared mechanism no longer holds, or the member models "
            "stop being jointly true."
        )


def _is_situation_payload(prop: Any) -> bool:
    if not isinstance(prop, dict):
        return False
    return (
        prop.get("claim_role") == "situation"
        or prop.get("legacy_kind") == "situation"
        or prop.get("kind") == "situation"
    )


def _falsifier_text(value: Any) -> str:
    if not isinstance(value, dict):
        return ""
    for key in ("description", "condition", "natural", "pattern"):
        raw = value.get(key)
        if isinstance(raw, str) and raw.strip():
            return raw.strip()
    return ""


def _apply_stance_defaults(prop: dict[str, Any]) -> None:
    kind = prop.get("kind")
    if kind == "observation":
        prop.setdefault("claim_role", "fact")
        prop.setdefault("abstraction_level", "atomic")
        prop.setdefault("time_mode", "past")
        prop.setdefault("modality", "observed")
        prop.setdefault("polarity", "neutral")
    elif kind == "belief":
        prop.setdefault("claim_role", "fact")
        prop.setdefault("abstraction_level", "atomic")
        prop.setdefault("time_mode", "current")
        prop.setdefault("modality", "inferred")
        prop.setdefault("polarity", "neutral")
    elif kind == "prediction":
        for key, value in _LEGACY_GRAMMAR["prediction"].items():
            prop.setdefault(key, value)
    elif kind == "norm":
        for key, value in _LEGACY_GRAMMAR["recommendation"].items():
            prop.setdefault(key, value)


def validate_proposition(raw: dict[str, Any]) -> _PropositionBase:
    """Validate a raw proposition dict and return the typed stance model."""
    canonical = canonicalize_proposition(raw)
    kind = canonical.get("kind")
    try:
        parsed = _ADAPTER.validate_python(canonical)
    except PydanticValidationError as e:
        raise ValidationError(
            f"proposition kind={kind!r} failed schema validation: {e}",
            field="proposition",
            kind=kind,
            errors=[
                {"loc": err["loc"], "msg": err["msg"], "type": err["type"]}
                for err in e.errors()
            ],
        ) from e
    validate_claim_role_contract(canonical)
    return parsed


def proposition_kind(raw: dict[str, Any]) -> PropositionKind:
    """Return the canonical stance discriminator value after validation."""
    model = validate_proposition(raw)
    return model.kind  # type: ignore[return-value]


# Compatibility aliases for call sites/tests that import the old names.
StateProposition = BeliefProposition
RelationProposition = BeliefProposition
PatternProposition = BeliefProposition
PatternInstanceProposition = BeliefProposition
CapabilityAssessmentProposition = BeliefProposition
HypothesisProposition = BeliefProposition
ConcernProposition = BeliefProposition
MarketAssessmentProposition = BeliefProposition
EnvironmentalTrendProposition = BeliefProposition
SituationProposition = BeliefProposition
RecommendationProposition = NormProposition


__all__ = [
    "CanonicalPropositionKind",
    "LegacyPropositionKind",
    "PropositionModel",
    "ObservationProposition",
    "BeliefProposition",
    "PredictionProposition",
    "NormProposition",
    "StateProposition",
    "RelationProposition",
    "PatternProposition",
    "PatternInstanceProposition",
    "CapabilityAssessmentProposition",
    "HypothesisProposition",
    "ConcernProposition",
    "MarketAssessmentProposition",
    "EnvironmentalTrendProposition",
    "SituationProposition",
    "SituationPressureType",
    "RecommendationProposition",
    "canonicalize_proposition",
    "ensure_situation_compositional_defaults",
    "validate_proposition",
    "proposition_kind",
    "LEGAL_KINDS",
    "LEGACY_KINDS",
]
