"""
services/models/propositions.py — Pydantic v2 discriminated union over
the 10 Model proposition kinds per spec §2.

The `kind` field on every proposition is the discriminator. Each
proposition kind has a fixed, known set of required fields:

    state                 — {kind, subject, assertion}
    relation              — {kind, subject, relation, object}
    prediction            — {kind, expected, resolution}
    pattern               — {kind, signature, observed_tendency,
                              trigger_conditions}
    pattern_instance      — {kind, pattern_id, matched_context}
    capability_assessment — {kind, capability_id, assessment}
    hypothesis            — {kind, hypothesis_text, test_conditions}
    concern               — {kind, about, nature, raised_by}
    market_assessment     — {kind, subject_external, assessment}
    environmental_trend   — {kind, signature, direction, strength}

Use `validate_proposition(raw: dict) -> PropositionModel` to parse any
raw JSONB payload into its typed counterpart. A missing or unknown
kind raises `ValidationError` (our own, not Pydantic's).

The discriminated union is what gives round-trip typing through
`ModelCreate.proposition: dict[str, Any]` — callers that care can
validate first and dump back to a dict before storage.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, ValidationError as PydanticValidationError
from pydantic import TypeAdapter

from lib.shared.errors import ValidationError
from lib.shared.types import PropositionKind


# ---------------------------------------------------------------------
# Base — every proposition has a `kind` discriminator.
# ---------------------------------------------------------------------

class _PropositionBase(BaseModel):
    # `extra="allow"` so future, spec-defined auxiliary fields don't
    # break validation; but the discriminating + required fields are
    # strict per-kind below.
    model_config = ConfigDict(extra="allow", str_strip_whitespace=False)


# ---------------------------------------------------------------------
# Ten kinds
# ---------------------------------------------------------------------

class StateProposition(_PropositionBase):
    kind: Literal["state"] = "state"
    subject: str | dict[str, Any]
    assertion: str


class RelationProposition(_PropositionBase):
    kind: Literal["relation"] = "relation"
    subject: str | dict[str, Any]
    relation: str
    object: str | dict[str, Any] = Field(alias="object")


class PredictionProposition(_PropositionBase):
    kind: Literal["prediction"] = "prediction"
    expected: str | dict[str, Any]
    resolution: str | dict[str, Any]


class PatternProposition(_PropositionBase):
    kind: Literal["pattern"] = "pattern"
    signature: str | dict[str, Any]
    observed_tendency: str
    trigger_conditions: str | list[str] | dict[str, Any]


class PatternInstanceProposition(_PropositionBase):
    kind: Literal["pattern_instance"] = "pattern_instance"
    pattern_id: str  # UUID string OR external id; we keep it permissive
    matched_context: str | dict[str, Any]


class CapabilityAssessmentProposition(_PropositionBase):
    kind: Literal["capability_assessment"] = "capability_assessment"
    capability_id: str
    assessment: str | dict[str, Any]


class HypothesisProposition(_PropositionBase):
    kind: Literal["hypothesis"] = "hypothesis"
    hypothesis_text: str
    test_conditions: str | list[str] | dict[str, Any]


class ConcernProposition(_PropositionBase):
    kind: Literal["concern"] = "concern"
    about: str | dict[str, Any]
    nature: str
    raised_by: str | dict[str, Any]


class MarketAssessmentProposition(_PropositionBase):
    kind: Literal["market_assessment"] = "market_assessment"
    subject_external: str | dict[str, Any]
    assessment: str | dict[str, Any]


class EnvironmentalTrendProposition(_PropositionBase):
    kind: Literal["environmental_trend"] = "environmental_trend"
    signature: str | dict[str, Any]
    direction: str
    strength: str | float


# ---------------------------------------------------------------------
# Discriminated union
# ---------------------------------------------------------------------

PropositionModel = Annotated[
    Union[
        StateProposition,
        RelationProposition,
        PredictionProposition,
        PatternProposition,
        PatternInstanceProposition,
        CapabilityAssessmentProposition,
        HypothesisProposition,
        ConcernProposition,
        MarketAssessmentProposition,
        EnvironmentalTrendProposition,
    ],
    Field(discriminator="kind"),
]


_ADAPTER: TypeAdapter[Any] = TypeAdapter(PropositionModel)


# ---------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------

_KIND_TO_CLASS: dict[str, type[_PropositionBase]] = {
    "state": StateProposition,
    "relation": RelationProposition,
    "prediction": PredictionProposition,
    "pattern": PatternProposition,
    "pattern_instance": PatternInstanceProposition,
    "capability_assessment": CapabilityAssessmentProposition,
    "hypothesis": HypothesisProposition,
    "concern": ConcernProposition,
    "market_assessment": MarketAssessmentProposition,
    "environmental_trend": EnvironmentalTrendProposition,
}

LEGAL_KINDS: frozenset[str] = frozenset(_KIND_TO_CLASS.keys())


def validate_proposition(raw: dict[str, Any]) -> _PropositionBase:
    """
    Validate a raw proposition dict and return the typed model.

    Raises lib.shared.errors.ValidationError on:
      - missing `kind`
      - unknown kind
      - any kind-specific field error
    """
    if not isinstance(raw, dict):
        raise ValidationError(
            f"proposition must be a dict; got {type(raw).__name__}",
            field="proposition",
        )
    kind = raw.get("kind")
    if not kind:
        raise ValidationError(
            "proposition missing 'kind' discriminator",
            field="proposition.kind",
        )
    if kind not in _KIND_TO_CLASS:
        raise ValidationError(
            f"unknown proposition kind {kind!r}; must be one of "
            f"{sorted(LEGAL_KINDS)}",
            field="proposition.kind",
            value=kind,
        )
    try:
        return _ADAPTER.validate_python(raw)
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


def proposition_kind(raw: dict[str, Any]) -> PropositionKind:
    """Return the discriminator value after validation."""
    model = validate_proposition(raw)
    return model.kind  # type: ignore[return-value]


__all__ = [
    "PropositionModel",
    "StateProposition",
    "RelationProposition",
    "PredictionProposition",
    "PatternProposition",
    "PatternInstanceProposition",
    "CapabilityAssessmentProposition",
    "HypothesisProposition",
    "ConcernProposition",
    "MarketAssessmentProposition",
    "EnvironmentalTrendProposition",
    "validate_proposition",
    "proposition_kind",
    "LEGAL_KINDS",
]
