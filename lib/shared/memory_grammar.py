"""Shared Model memory grammar.

The grammar separates substrate organization from product projections.
`proposition_kind` remains the compatibility discriminator, but these
axes describe what kind of memory object a Model is in structural terms.
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


_KIND_GRAMMAR: dict[str, tuple[ClaimRole, AbstractionLevel, TimeMode, Modality, Polarity]] = {
    "state": ("fact", "atomic", "current", "observed", "neutral"),
    "relation": ("relation", "relationship", "current", "inferred", "neutral"),
    "prediction": ("prediction", "atomic", "future", "expected", "neutral"),
    "pattern": ("pattern", "pattern", "recurring", "inferred", "neutral"),
    "pattern_instance": ("pattern", "atomic", "past", "observed", "neutral"),
    "capability_assessment": ("capability", "atomic", "current", "inferred", "neutral"),
    "hypothesis": ("hypothesis", "atomic", "unspecified", "inferred", "neutral"),
    "concern": ("concern", "atomic", "current", "inferred", "negative"),
    "market_assessment": ("fact", "atomic", "current", "inferred", "neutral"),
    "environmental_trend": ("pattern", "pattern", "recurring", "inferred", "neutral"),
    "situation": ("situation", "composite", "current", "inferred", "mixed"),
    "recommendation": ("recommendation", "atomic", "future", "normative", "mixed"),
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
    if isinstance(proposition, dict):
        raw_kind = proposition.get("kind")
        if isinstance(raw_kind, str):
            kind = raw_kind
    role, abstraction, time_mode, modality, polarity = _KIND_GRAMMAR.get(
        kind,
        ("fact", "atomic", "unspecified", "inferred", "neutral"),
    )
    return MemoryGrammar(
        claim_role=role,
        abstraction_level=abstraction,
        time_mode=time_mode,
        modality=modality,
        polarity=polarity,
        domain_tags=tuple(_derive_domain_tags(scope_entities, natural)),
    )


def _derive_domain_tags(
    scope_entities: list[dict[str, Any]] | tuple[dict[str, Any], ...],
    natural: str,
) -> list[str]:
    tags: list[str] = []
    seen: set[str] = set()

    def add(tag: str) -> None:
        if tag not in seen:
            seen.add(tag)
            tags.append(tag)

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
