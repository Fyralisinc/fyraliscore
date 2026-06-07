"""Belief-address helpers for queryable Model semantics.

A Model is the smallest unit of belief in Fyralis, but not every useful
question should have to rediscover that belief through prose. This module
derives a compact, deterministic address from the Model's proposition and
memory grammar: what the belief is about, what it asserts, what kind of
question it can answer, and a stable fingerprint for duplicate detection.
"""
from __future__ import annotations

import hashlib
import json
import re
from typing import Any, Mapping, Sequence

from lib.shared.memory_grammar import MemoryGrammar, derive_memory_grammar


BELIEF_ADDRESS_VERSION = "belief_address_v1"


def build_belief_address(
    proposition: Mapping[str, Any] | None,
    *,
    natural: str = "",
    scope_entities: Sequence[Mapping[str, Any]] = (),
    grammar: MemoryGrammar | None = None,
) -> dict[str, Any]:
    """Return the queryable belief address for one Model."""
    prop = dict(proposition or {})
    grammar = grammar or derive_memory_grammar(
        prop,
        natural=natural,
        scope_entities=list(scope_entities),
    )
    subject = _stable_text(_first_present(
        prop,
        "subject",
        "about",
        "subject_external",
        "capability_id",
        "situation",
    ))
    predicate = _stable_text(_predicate_for(prop, grammar))
    object_value = _stable_text(_first_present(
        prop,
        "object",
        "value",
        "assertion",
        "claim",
        "assessment",
        "expected",
        "desired_state",
        "goal",
        "policy",
        "nature",
        "summary",
        "event",
        "observed_tendency",
    ))
    qualifier = _stable_text(_first_present(
        prop,
        "qualifier",
        "status",
        "relationship_summary",
        "shared_mechanism",
        "open_falsifier",
    ))
    operational_roles = tuple(
        _norm_token(role)
        for role in prop.get("operational_roles", [])
        if isinstance(role, str) and _norm_token(role)
    )
    address_core = {
        "version": BELIEF_ADDRESS_VERSION,
        "claim_role": grammar.claim_role,
        "abstraction_level": grammar.abstraction_level,
        "time_mode": grammar.time_mode,
        "modality": grammar.modality,
        "polarity": grammar.polarity,
        "subject": subject,
        "predicate": predicate,
        "object": object_value,
        "qualifier": qualifier,
    }
    fingerprint = _fingerprint(address_core)
    obligation_keys = _obligation_keys(
        address_core,
        operational_roles=operational_roles,
    )
    primitives = _answerable_primitives(
        address_core,
        operational_roles=operational_roles,
        domain_tags=tuple(
            _norm_token(tag)
            for tag in prop.get("domain_tags", [])
            if isinstance(tag, str) and _norm_token(tag)
        ),
    )
    return {
        **address_core,
        "fingerprint": fingerprint,
        "obligation_keys": obligation_keys,
        "answerable_primitives": primitives,
    }


def belief_address_from_model_like(model: Any) -> dict[str, Any]:
    """Read or derive the belief address from a ModelRow-shaped object."""
    prop = getattr(model, "proposition", {}) or {}
    if isinstance(prop, Mapping):
        existing = prop.get("belief_address")
        if isinstance(existing, Mapping):
            return _normalize_existing_address(existing)
        semantic = prop.get("semantic_address")
        if isinstance(semantic, Mapping):
            base = _normalize_existing_address(semantic)
            if not base.get("obligation_keys") or not base.get("answerable_primitives"):
                derived = build_belief_address(
                    prop,
                    natural=str(getattr(model, "natural", "") or ""),
                    scope_entities=getattr(model, "scope_entities", []) or (),
                )
                base.setdefault("qualifier", derived.get("qualifier", ""))
                base["obligation_keys"] = base.get("obligation_keys") or derived["obligation_keys"]
                base["answerable_primitives"] = (
                    base.get("answerable_primitives") or derived["answerable_primitives"]
                )
            return base
    return build_belief_address(
        prop if isinstance(prop, Mapping) else {},
        natural=str(getattr(model, "natural", "") or ""),
        scope_entities=getattr(model, "scope_entities", []) or (),
    )


def _normalize_existing_address(raw: Mapping[str, Any]) -> dict[str, Any]:
    out = {
        "version": str(raw.get("version") or BELIEF_ADDRESS_VERSION),
        "claim_role": _norm_token(raw.get("claim_role")),
        "abstraction_level": _norm_token(raw.get("abstraction_level")),
        "time_mode": _norm_token(raw.get("time_mode")),
        "modality": _norm_token(raw.get("modality")),
        "polarity": _norm_token(raw.get("polarity")),
        "subject": _stable_text(raw.get("subject")),
        "predicate": _stable_text(raw.get("predicate")),
        "object": _stable_text(raw.get("object")),
        "qualifier": _stable_text(raw.get("qualifier")),
        "fingerprint": _norm_token(raw.get("fingerprint")),
        "obligation_keys": _clean_list(raw.get("obligation_keys")),
        "answerable_primitives": tuple(
            item.upper()
            for item in _clean_list(raw.get("answerable_primitives"))
        ),
    }
    if not out["fingerprint"]:
        out["fingerprint"] = _fingerprint(out)
    return out


def _predicate_for(proposition: Mapping[str, Any], grammar: MemoryGrammar) -> Any:
    if grammar.claim_role == "relation":
        return proposition.get("relation") or "relates_to"
    if grammar.claim_role == "recommendation":
        change = proposition.get("proposed_change")
        if isinstance(change, Mapping):
            return change.get("operation") or "proposed_change"
        return "proposed_change"
    if grammar.claim_role == "prediction":
        return "expected"
    if grammar.claim_role == "situation":
        return "shared_mechanism"
    if grammar.claim_role == "concern":
        return "risk"
    if grammar.claim_role == "hypothesis":
        return "hypothesis"
    if grammar.claim_role == "pattern":
        return "pattern"
    if grammar.claim_role == "capability":
        return "capability"
    return "asserts"


def _obligation_keys(
    address: Mapping[str, Any],
    *,
    operational_roles: Sequence[str],
) -> tuple[str, ...]:
    keys: list[str] = []

    def add(prefix: str, value: Any) -> None:
        token = _norm_token(value)
        if token:
            keys.append(f"{prefix}:{token}")

    add("role", address.get("claim_role"))
    add("level", address.get("abstraction_level"))
    add("time", address.get("time_mode"))
    add("polarity", address.get("polarity"))
    add("predicate", address.get("predicate"))
    for role in operational_roles[:8]:
        add("operational", role)

    subject = _norm_token(address.get("subject"))
    predicate = _norm_token(address.get("predicate"))
    obj = _norm_token(address.get("object"))
    qualifier = _norm_token(address.get("qualifier"))
    if subject:
        keys.append(f"subject:{subject}")
    if subject and predicate:
        keys.append(f"subject_predicate:{subject}|{predicate}")
    if subject and predicate and obj:
        keys.append(f"spo:{subject}|{predicate}|{obj[:96]}")
    if qualifier:
        keys.append(f"qualifier:{qualifier[:96]}")
    return _dedupe(keys)


def _answerable_primitives(
    address: Mapping[str, Any],
    *,
    operational_roles: Sequence[str],
    domain_tags: Sequence[str],
) -> tuple[str, ...]:
    role = str(address.get("claim_role") or "")
    predicate = str(address.get("predicate") or "")
    text = " ".join(
        str(address.get(key) or "")
        for key in ("subject", "predicate", "object", "qualifier")
    ).casefold()
    tags = set(domain_tags)
    primitives: list[str] = []

    def add(value: str) -> None:
        if value not in primitives:
            primitives.append(value)

    if role in {"relation", "situation", "capability"}:
        add("DEPENDENCY")
    if role == "pattern":
        add("RECURRENCE")
    if role in {"concern", "hypothesis", "prediction", "situation"}:
        add("COUNTEREVIDENCE")
    if role == "concern" or _contains_any(text, ("risk", "block", "constraint", "scarce", "quota")):
        add("CONSTRAINT")
    if _contains_any(text, ("owner", "owned", "owns", "assigned", "responsible")):
        add("OWNERSHIP")
    if tags & {"customers", "goals", "resources", "finance", "revenue"} or _contains_any(
        text,
        ("customer", "revenue", "resource", "goal", "arr", "renewal"),
    ):
        add("GOAL_IMPACT")
    if role == "recommendation":
        add("COMMITMENT")
    if set(operational_roles) & {"action", "sequence", "state", "delta", "count", "invariant"}:
        add("DEPENDENCY")
    if not primitives:
        add("DEPENDENCY")
    return tuple(primitives)


def _first_present(proposition: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = proposition.get(key)
        if _present(value):
            return value
    return None


def _present(value: Any) -> bool:
    if value is None:
        return False
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, (list, tuple, set, dict)):
        return bool(value)
    return True


def _stable_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return _clip(" ".join(value.split()), 240)
    if isinstance(value, Mapping):
        return _clip(json.dumps(value, sort_keys=True, default=str), 240)
    return _clip(str(value), 240)


def _norm_token(value: Any) -> str:
    text = str(value or "").strip().casefold()
    text = re.sub(r"\s+", " ", text)
    return _clip(text, 160)


def _clean_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)):
        return ()
    return _dedupe(_norm_token(item) for item in value)


def _dedupe(values: Sequence[str] | Any) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = str(value or "").strip()
        if clean and clean not in seen:
            seen.add(clean)
            out.append(clean)
    return tuple(out)


def _contains_any(text: str, needles: Sequence[str]) -> bool:
    return any(needle in text for needle in needles)


def _fingerprint(address: Mapping[str, Any]) -> str:
    stable = {
        key: address.get(key) or ""
        for key in (
            "claim_role",
            "abstraction_level",
            "time_mode",
            "modality",
            "polarity",
            "subject",
            "predicate",
            "object",
            "qualifier",
        )
    }
    raw = json.dumps(stable, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:24]


def _clip(value: str, limit: int) -> str:
    text = str(value or "").strip()
    if len(text) <= limit:
        return text
    return text[: limit - 1].rstrip() + "..."


__all__ = [
    "BELIEF_ADDRESS_VERSION",
    "belief_address_from_model_like",
    "build_belief_address",
]
