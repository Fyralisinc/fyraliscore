"""Shared Model memory grammar.

The grammar separates epistemic stance from subject semantics. The base
`proposition.kind` should stay small and stable (observation, belief,
prediction, norm); these axes describe what kind of memory object a Model
is in structural terms.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Literal


ClaimRole = Literal[
    "fact",
    "concern",
    "hypothesis",
    "prediction",
    "pattern",
    "situation",
    "capability",
    "relation",
    "recommendation",
]

AbstractionLevel = Literal["atomic", "relationship", "composite", "pattern"]
TimeMode = Literal["past", "current", "future", "recurring", "unspecified"]
Modality = Literal["observed", "inferred", "expected", "normative"]
Polarity = Literal["positive", "negative", "mixed", "neutral"]


@dataclass(frozen=True)
class MemoryGrammar:
    claim_role: ClaimRole
    abstraction_level: AbstractionLevel
    time_mode: TimeMode
    modality: Modality
    polarity: Polarity
    domain_tags: tuple[str, ...] = field(default_factory=tuple)


_VALID_CLAIM_ROLES = set(ClaimRole.__args__)  # type: ignore[attr-defined]
_VALID_ABSTRACTION_LEVELS = set(AbstractionLevel.__args__)  # type: ignore[attr-defined]
_VALID_TIME_MODES = set(TimeMode.__args__)  # type: ignore[attr-defined]
_VALID_MODALITIES = set(Modality.__args__)  # type: ignore[attr-defined]
_VALID_POLARITIES = set(Polarity.__args__)  # type: ignore[attr-defined]

_STANCE_GRAMMAR: dict[str, tuple[ClaimRole, AbstractionLevel, TimeMode, Modality, Polarity]] = {
    "observation": ("fact", "atomic", "past", "observed", "neutral"),
    "belief": ("fact", "atomic", "current", "inferred", "neutral"),
    "prediction": ("prediction", "atomic", "future", "expected", "neutral"),
    "norm": ("recommendation", "atomic", "future", "normative", "mixed"),
}

_LEGACY_KIND_GRAMMAR: dict[str, tuple[ClaimRole, AbstractionLevel, TimeMode, Modality, Polarity]] = {
    "state": ("fact", "atomic", "current", "observed", "neutral"),
    "relation": ("relation", "relationship", "current", "inferred", "neutral"),
    "pattern": ("pattern", "pattern", "recurring", "inferred", "neutral"),
    "pattern_instance": ("pattern", "atomic", "past", "observed", "neutral"),
    "capability_assessment": ("capability", "atomic", "current", "inferred", "neutral"),
    "hypothesis": ("hypothesis", "atomic", "unspecified", "inferred", "neutral"),
    "concern": ("concern", "atomic", "current", "inferred", "negative"),
    "market_assessment": ("fact", "atomic", "current", "inferred", "neutral"),
    "environmental_trend": ("pattern", "pattern", "recurring", "inferred", "neutral"),
    "situation": ("situation", "composite", "current", "inferred", "mixed"),
    "recommendation": _STANCE_GRAMMAR["norm"],
}

_ENTITY_DOMAIN_TAGS: dict[str, str] = {
    "actor": "people",
    "customer": "customers",
    "customer_resource": "customers",
    "commitment": "execution",
    "goal": "goals",
    "decision": "decisions",
    "resource": "resources",
}

_DOMAIN_KEYWORDS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("customers", re.compile(r"\b(customer|renewal|churn|account|arr|expansion)\b", re.I)),
    ("finance", re.compile(r"\b(runway|budget|burn|capital|funding|invoice|revenue)\b", re.I)),
    ("people", re.compile(r"\b(owner|team|reports to|manager|hiring|capacity)\b", re.I)),
    ("systems", re.compile(r"\b(platform|system|infrastructure|api|pipeline|deployment)\b", re.I)),
    ("execution", re.compile(r"\b(commitment|delivery|deadline|blocked|ship|launch|milestone)\b", re.I)),
    ("risk", re.compile(r"\b(risk|concern|blocked|constraint|escalation|compliance)\b", re.I)),
)


def derive_memory_grammar(
    proposition: dict[str, Any] | None,
    *,
    natural: str = "",
    scope_entities: list[dict[str, Any]] | tuple[dict[str, Any], ...] = (),
) -> MemoryGrammar:
    """Derive structural grammar axes from a Model payload.

    The mapping is intentionally conservative and deterministic. It gives
    downstream code a stable substrate grammar while leaving room for
    later LLM-assisted refinement through explicit updates.
    """
    kind = ""
    legacy_kind = ""
    if isinstance(proposition, dict):
        raw_kind = proposition.get("kind")
        if isinstance(raw_kind, str):
            kind = raw_kind
        raw_legacy = proposition.get("legacy_kind")
        if isinstance(raw_legacy, str):
            legacy_kind = raw_legacy
    role, abstraction, time_mode, modality, polarity = (
        _LEGACY_KIND_GRAMMAR.get(legacy_kind)
        or _STANCE_GRAMMAR.get(kind)
        or _LEGACY_KIND_GRAMMAR.get(kind)
        or ("fact", "atomic", "unspecified", "inferred", "neutral")
    )
    if isinstance(proposition, dict):
        role = _explicit_axis(
            proposition.get("claim_role"),
            _VALID_CLAIM_ROLES,
            role,
        )
        abstraction = _explicit_axis(
            proposition.get("abstraction_level"),
            _VALID_ABSTRACTION_LEVELS,
            abstraction,
        )
        time_mode = _explicit_axis(
            proposition.get("time_mode"),
            _VALID_TIME_MODES,
            time_mode,
        )
        modality = _explicit_axis(
            proposition.get("modality"),
            _VALID_MODALITIES,
            modality,
        )
        polarity = _explicit_axis(
            proposition.get("polarity"),
            _VALID_POLARITIES,
            polarity,
        )
    return MemoryGrammar(
        claim_role=role,
        abstraction_level=abstraction,
        time_mode=time_mode,
        modality=modality,
        polarity=polarity,
        domain_tags=tuple(_derive_domain_tags(scope_entities, natural, proposition)),
    )


def _explicit_axis(value: Any, allowed: set[str], fallback: Any) -> Any:
    if isinstance(value, str) and value in allowed:
        return value
    return fallback


def _derive_domain_tags(
    scope_entities: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    natural: str,
    proposition: dict[str, Any] | None = None,
) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def add(tag: str) -> None:
        if tag not in seen:
            seen.add(tag)
            tags.append(tag)

    if isinstance(proposition, dict):
        raw_tags = proposition.get("domain_tags")
        if isinstance(raw_tags, (list, tuple)):
            for raw_tag in raw_tags:
                if isinstance(raw_tag, str):
                    tag = raw_tag.strip().lower().replace(" ", "_")
                    if tag:
                        add(tag)

    for raw in scope_entities or ():
        if not isinstance(raw, dict):
            continue
        entity_type = raw.get("type")
        if entity_type is None:
            continue
        tag = _ENTITY_DOMAIN_TAGS.get(str(entity_type))
        if tag:
            add(tag)

    text = natural or ""
    for tag, pattern in _DOMAIN_KEYWORDS:
        if pattern.search(text):
            add(tag)

    return tags


__all__ = [
    "AbstractionLevel",
    "ClaimRole",
    "MemoryGrammar",
    "Modality",
    "Polarity",
    "TimeMode",
    "derive_memory_grammar",
]
