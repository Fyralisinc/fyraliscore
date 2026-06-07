"""Model construction boundary.

This module is part of the Model layer, not a second memory ontology.
Its job is to turn a permissive ``ModelCreate`` input into a canonical
Model draft with explicit internal responsibilities:

* core: the single belief center that the Model claims
* evidence: support, falsifier, and signal readings attached to it
* projection: search/retrieval material derived from the core
* runtime: counters and heat signals that must not define truth

The database schema still stores one ``models`` row. The constructor is
the write-time guard that keeps that row from becoming a mix of belief,
evidence, retrieval utility, and composite situation payload.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import UUID

from lib.shared.errors import ValidationError
from lib.shared.memory_grammar import MemoryGrammar, derive_memory_grammar
from lib.shared.types import ModelCreate

from services.models.address import build_belief_address
from services.models.propositions import (
    canonicalize_proposition,
    ensure_situation_compositional_defaults,
    validate_proposition,
)
from services.synthesis.operational_facets import (
    enrich_operational_model_proposition,
)


MODEL_CONTRACT_VERSION = "model_normal_form_v1"


@dataclass(frozen=True, slots=True)
class ModelCore:
    """Canonical belief identity for a Model."""

    proposition: dict[str, Any]
    grammar: MemoryGrammar
    semantic_address: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ModelEvidence:
    """Evidence/support material attached to the core belief."""

    born_from_event_id: UUID
    supporting_event_ids: tuple[UUID, ...] = ()
    supporting_model_ids: tuple[UUID, ...] = ()
    signal_readings: tuple[dict[str, Any], ...] = ()
    falsifier: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class ModelProjection:
    """Read-side material derived from the core belief."""

    natural: str
    embedding_text: str
    scope_actors: tuple[UUID, ...] = ()
    scope_entities: tuple[dict[str, Any], ...] = ()
    domain_tags: tuple[str, ...] = ()
    operational_roles: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ModelRuntime:
    """Runtime defaults that are not epistemic truth."""

    activation_coefficient: float = 1.0


@dataclass(frozen=True, slots=True)
class ConstructedModel:
    """A normalized single-row Model draft plus its internal parts."""

    proposed: ModelCreate
    core: ModelCore
    evidence: ModelEvidence
    projection: ModelProjection
    runtime: ModelRuntime
    notes: tuple[str, ...] = field(default_factory=tuple)


_COMPOSITE_ONLY_FIELDS = frozenset({
    "member_model_ids",
    "relationship_summary",
    "pressure_type",
    "shared_mechanism",
    "judgment_change",
})

_RUNTIME_PROPOSITION_FIELDS = frozenset({
    "activation",
    "activation_coefficient",
    "retrieval_count",
    "last_retrieved_at",
    "confidence",
    "confidence_at_assertion",
    "embedding",
})


def construct_model(proposed: ModelCreate) -> ConstructedModel:
    """Normalize and validate one ModelCreate as a canonical Model draft.

    This function intentionally preserves the public ``ModelsRepo.insert``
    contract: one input creates one row. Inputs that need multiple Models
    should be split before insert by Think, or by a future batch-oriented
    constructor API. The important invariant here is that a single row
    cannot masquerade as both an atomic belief and a composite situation.
    """

    canonical_prop = canonicalize_proposition(proposed.proposition)
    canonical_prop = enrich_operational_model_proposition(
        canonical_prop,
        natural=proposed.natural,
    )
    defaults_entry = {
        "proposition": canonical_prop,
        "natural": proposed.natural,
        "falsifier": proposed.falsifier,
    }
    ensure_situation_compositional_defaults(defaults_entry)
    canonical_prop = dict(defaults_entry["proposition"])
    validate_proposition(canonical_prop)

    grammar = derive_memory_grammar(
        canonical_prop,
        natural=proposed.natural,
        scope_entities=proposed.scope_entities,
    )
    _enforce_normal_form(canonical_prop, grammar)

    semantic_address = build_belief_address(
        canonical_prop,
        grammar=grammar,
        natural=proposed.natural,
        scope_entities=proposed.scope_entities,
    )
    canonical_prop.setdefault("model_contract_version", MODEL_CONTRACT_VERSION)
    canonical_prop.setdefault("semantic_address", semantic_address)
    canonical_prop.setdefault("belief_address", semantic_address)

    domain_tags = _dedupe_text([
        *list(proposed.domain_tags or ()),
        *list(grammar.domain_tags or ()),
        *[
            str(tag)
            for tag in canonical_prop.get("domain_tags", [])
            if isinstance(tag, str)
        ],
    ])
    if domain_tags:
        canonical_prop["domain_tags"] = domain_tags

    normalized = proposed.model_copy(update={
        "proposition": canonical_prop,
        "domain_tags": domain_tags,
    })

    projection = ModelProjection(
        natural=normalized.natural,
        embedding_text=_embedding_text(normalized.natural, semantic_address),
        scope_actors=tuple(normalized.scope_actors or ()),
        scope_entities=tuple(
            dict(item)
            for item in (normalized.scope_entities or ())
            if isinstance(item, Mapping)
        ),
        domain_tags=tuple(domain_tags),
        operational_roles=tuple(
            str(role)
            for role in canonical_prop.get("operational_roles", [])
            if isinstance(role, str)
        ),
    )

    return ConstructedModel(
        proposed=normalized,
        core=ModelCore(
            proposition=canonical_prop,
            grammar=grammar,
            semantic_address=semantic_address,
        ),
        evidence=ModelEvidence(
            born_from_event_id=normalized.born_from_event_id,
            supporting_event_ids=tuple(normalized.supporting_event_ids or ()),
            supporting_model_ids=tuple(normalized.supporting_model_ids or ()),
            signal_readings=tuple(
                dict(reading)
                for reading in (normalized.signal_readings or ())
                if isinstance(reading, Mapping)
            ),
            falsifier=normalized.falsifier,
        ),
        projection=projection,
        runtime=ModelRuntime(
            activation_coefficient=normalized.activation_coefficient,
        ),
    )


def _enforce_normal_form(
    proposition: dict[str, Any],
    grammar: MemoryGrammar,
) -> None:
    role = grammar.claim_role
    abstraction = grammar.abstraction_level

    leaked_runtime = [
        field for field in sorted(_RUNTIME_PROPOSITION_FIELDS)
        if _present(proposition.get(field))
    ]
    if leaked_runtime:
        raise ValidationError(
            "runtime fields must not live inside proposition",
            field="proposition",
            fields=leaked_runtime,
        )

    has_composite_fields = [
        field for field in sorted(_COMPOSITE_ONLY_FIELDS)
        if _present(proposition.get(field))
    ]
    if role != "situation" and has_composite_fields:
        raise ValidationError(
            "atomic/relationship Models cannot carry situation composition fields",
            field="proposition",
            claim_role=role,
            fields=has_composite_fields,
        )

    if role == "situation":
        if abstraction != "composite":
            raise ValidationError(
                "situation Models must use abstraction_level='composite'",
                field="proposition.abstraction_level",
                claim_role=role,
                abstraction_level=abstraction,
            )
        leaked_facets = [
            field for field in ("operational_facets", "operational_roles")
            if _present(proposition.get(field))
        ]
        if leaked_facets:
            raise ValidationError(
                "situation Models cannot carry atomic operational facet indexes",
                field="proposition",
                claim_role=role,
                fields=leaked_facets,
            )


def _embedding_text(natural: str, semantic_address: Mapping[str, Any]) -> str:
    parts = [
        str(natural or "").strip(),
        str(semantic_address.get("subject") or "").strip(),
        str(semantic_address.get("predicate") or "").strip(),
        str(semantic_address.get("object") or "").strip(),
    ]
    return " ".join(part for part in parts if part)


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _dedupe_text(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip().lower().replace(" ", "_")
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return out


__all__ = [
    "ConstructedModel",
    "MODEL_CONTRACT_VERSION",
    "ModelCore",
    "ModelEvidence",
    "ModelProjection",
    "ModelRuntime",
    "construct_model",
]
