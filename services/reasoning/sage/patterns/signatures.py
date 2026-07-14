"""Structural signatures for SAGE latent pattern discovery."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, is_dataclass
from typing import Any, Iterable

from services.reasoning.sage.patterns.types import StructuralSignature


_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_-]{1,}")

_COORDINATION_TERMS = {
    "approval": "approval_loop",
    "approve": "approval_loop",
    "blocked": "blocked_flow",
    "blocker": "blocked_flow",
    "commitment": "commitment_loop",
    "commit": "commitment_loop",
    "dependency": "dependency_chain",
    "depends": "dependency_chain",
    "escalation": "escalation_loop",
    "handoff": "handoff",
    "owner": "ownership",
    "ownership": "ownership",
    "review": "review_loop",
    "ritual": "ritual",
}

_PRESSURE_TERMS = {
    "capacity": "capacity_pressure",
    "customer": "customer_pressure",
    "deadline": "deadline_pressure",
    "incident": "incident_pressure",
    "renewal": "revenue_pressure",
    "resource": "resource_pressure",
    "revenue": "revenue_pressure",
    "risk": "risk_pressure",
    "security": "security_pressure",
}

_AUTHORITY_TERMS = {
    "access": "access_authority",
    "approval": "approval_authority",
    "approve": "approval_authority",
    "blocked": "blocked_authority",
    "decision": "decision_authority",
    "permission": "access_authority",
    "review": "review_authority",
    "owner": "owner_authority",
    "ownership": "owner_authority",
}

_TEMPORAL_TERMS = {
    "again": "recurring",
    "always": "recurring",
    "cadence": "cadence",
    "deadline": "deadline",
    "drift": "drift",
    "every": "recurring",
    "later": "follow_on",
    "recurring": "recurring",
    "repeat": "recurring",
    "stale": "stale",
}

_GAP_TERMS = {
    "counterevidence": "counterevidence_unattached",
    "dropped": "validation_dropped_value",
    "falsifier": "falsifier_needed",
    "gap": "missing_structure",
    "missing": "missing_evidence",
    "question": "question_needed",
    "unanchored": "relation_unanchored",
    "unmodeled": "valuable_unmodeled",
}


def structural_signature_from_model(model: Any) -> StructuralSignature:
    """Build a structural signature from a Model-like row/object."""

    return build_structural_signature(model, source_kind="model")


def structural_signature_from_outcome_event(event: Any) -> StructuralSignature:
    """Build a structural signature from an inquiry outcome event-like row."""

    return build_structural_signature(event, source_kind="outcome_event")


def build_structural_signature(
    source: Any,
    *,
    source_kind: str,
    extra_facets: dict[str, Iterable[str]] | None = None,
    support_weight: float = 1.0,
) -> StructuralSignature:
    """Extract a surface-independent SAGE signature from a loose source object."""

    data = _mapping(source)
    payload = _mapping(_pick(data, source, "payload"))
    proposition = _mapping(_pick(data, source, "proposition"))
    metadata = _mapping(_pick(data, source, "metadata"))
    signature_payload = _mapping(payload.get("signature"))
    event_type = _text(_pick(data, source, "event_type"))

    text = _source_text(data, payload, proposition, metadata, event_type)
    tokens = set(_tokens(text))

    role_facets = _dedupe(
        [
            _facet("claim", _pick(data, source, "claim_role") or proposition.get("claim_role")),
            _facet(
                "abstraction",
                _pick(data, source, "abstraction_level")
                or proposition.get("abstraction_level"),
            ),
            _facet("source", source_kind),
            _facet(
                "question",
                payload.get("question_primitive")
                or signature_payload.get("question_primitive"),
            ),
            _facet("act", _nested(proposition, "target_act_ref", "type")),
            *_prefixed("coverage", _listish(proposition.get("coverage_roles"))),
        ]
    )
    pressure_facets = _dedupe(
        [
            _facet("pressure", proposition.get("pressure_type")),
            _facet("residual", _pick(data, source, "residual_kind")),
            *_text_facets(tokens, _PRESSURE_TERMS),
            *_prefixed("failure", _listish(payload.get("failure_modes"))),
        ]
    )
    outcome_facets = _dedupe(
        [
            _facet("event", event_type),
            _facet("polarity", _pick(data, source, "polarity") or proposition.get("polarity")),
            _facet("time", _pick(data, source, "time_mode") or proposition.get("time_mode")),
            _facet("operation", _nested(proposition, "proposed_change", "operation")),
            _facet("status", _pick(data, source, "status") or proposition.get("status")),
            _facet("bottleneck", payload.get("primary_bottleneck")),
            _facet("outcome", payload.get("outcome")),
        ]
    )
    authority_facets = _dedupe(
        [
            *_text_facets(tokens, _AUTHORITY_TERMS),
            _facet("authority", metadata.get("authority_reason")),
            _facet("access", payload.get("access_result")),
        ]
    )
    temporal_facets = _dedupe(
        [
            *_text_facets(tokens, _TEMPORAL_TERMS),
            _facet("time", _pick(data, source, "time_mode") or proposition.get("time_mode")),
        ]
    )
    coordination_facets = _dedupe(
        [
            *_text_facets(tokens, _COORDINATION_TERMS),
            _facet("path", payload.get("path") or payload.get("route")),
            _facet("edge", payload.get("edge_kind")),
        ]
    )
    evidence_gap_facets = _dedupe(
        [
            *_text_facets(tokens, _GAP_TERMS),
            _facet("residual", _pick(data, source, "residual_kind")),
            _facet("gap", _pick(data, source, "gap_kind")),
        ]
    )
    if "missing evidence" in text or "insufficient evidence" in text:
        evidence_gap_facets = _dedupe([*evidence_gap_facets, "missing_evidence"])
    if event_type.startswith("validation_failed"):
        evidence_gap_facets = _dedupe([*evidence_gap_facets, "validation_failed"])

    domain_facets = _dedupe(
        [
            *_listish(_pick(data, source, "domain_tags")),
            *_listish(proposition.get("retrieval_tags")),
            *_listish(signature_payload.get("domains")),
            *_listish(metadata.get("domains")),
        ]
    )
    actor_refs = _dedupe(
        [
            *_string_refs(_pick(data, source, "scope_actors")),
            *_string_refs(payload.get("actors")),
            *_string_refs(metadata.get("actors")),
        ]
    )
    entity_refs = _dedupe(
        [
            *_string_refs(_pick(data, source, "seed_entity_ids")),
            *_string_refs(payload.get("entities")),
            *_string_refs(signature_payload.get("entities")),
            *_string_refs(metadata.get("entities")),
        ]
    )
    surface_terms = _surface_terms(text, domain_facets)
    expected_outcome = _optional_text(
        metadata.get("expected_outcome")
        or payload.get("expected_outcome")
        or proposition.get("expected_outcome")
    )
    observed_outcome = _optional_text(
        metadata.get("observed_outcome")
        or payload.get("observed_outcome")
        or payload.get("outcome")
        or proposition.get("observed_outcome")
    )

    if extra_facets:
        role_facets = _dedupe([*role_facets, *_prefixed("extra", extra_facets.get("role", ()))])
        pressure_facets = _dedupe(
            [*pressure_facets, *_prefixed("extra", extra_facets.get("pressure", ()))]
        )
        outcome_facets = _dedupe(
            [*outcome_facets, *_prefixed("extra", extra_facets.get("outcome", ()))]
        )
        authority_facets = _dedupe(
            [*authority_facets, *_prefixed("extra", extra_facets.get("authority", ()))]
        )
        temporal_facets = _dedupe(
            [*temporal_facets, *_prefixed("extra", extra_facets.get("temporal", ()))]
        )
        coordination_facets = _dedupe(
            [
                *coordination_facets,
                *_prefixed("extra", extra_facets.get("coordination", ())),
            ]
        )
        evidence_gap_facets = _dedupe(
            [*evidence_gap_facets, *_prefixed("extra", extra_facets.get("evidence_gap", ()))]
        )

    structural_payload = {
        "role": role_facets,
        "pressure": pressure_facets,
        "outcome": outcome_facets,
        "authority": authority_facets,
        "temporal": temporal_facets,
        "coordination": coordination_facets,
        "evidence_gap": evidence_gap_facets,
        "expected_outcome": expected_outcome,
    }
    return StructuralSignature(
        signature_hash=_stable_hash(structural_payload),
        source_kind=_normalize(source_kind) or "unknown",
        role_facets=role_facets,
        pressure_facets=pressure_facets,
        outcome_facets=outcome_facets,
        authority_facets=authority_facets,
        temporal_facets=temporal_facets,
        coordination_facets=coordination_facets,
        evidence_gap_facets=evidence_gap_facets,
        domain_facets=domain_facets,
        actor_refs=actor_refs,
        entity_refs=entity_refs,
        surface_terms=surface_terms,
        expected_outcome=expected_outcome,
        observed_outcome=observed_outcome,
        source_ref=_source_ref(data, source),
        support_weight=max(0.0, float(support_weight or 0.0)),
        metadata={
            "event_type": event_type or None,
            "has_surface_text": bool(text),
            "structural_facet_count": sum(len(v) for v in structural_payload.values() if isinstance(v, tuple)),
        },
    )


def structural_neighborhood_keys(
    signature: StructuralSignature,
    *,
    max_keys: int = 8,
) -> tuple[tuple[str, ...], ...]:
    """Return bounded surface-independent keys for global scout indexing."""

    buckets = {
        "role": signature.role_facets,
        "pressure": signature.pressure_facets,
        "outcome": signature.outcome_facets,
        "authority": signature.authority_facets,
        "temporal": signature.temporal_facets,
        "coordination": signature.coordination_facets,
        "evidence_gap": signature.evidence_gap_facets,
    }
    candidates = [
        _key("coordination", buckets["coordination"], "pressure", buckets["pressure"]),
        _key("authority", buckets["authority"], "outcome", buckets["outcome"]),
        _key("pressure", buckets["pressure"], "outcome", buckets["outcome"]),
        _key("evidence_gap", buckets["evidence_gap"], "outcome", buckets["outcome"]),
        _key("role", buckets["role"], "coordination", buckets["coordination"]),
        _key("temporal", buckets["temporal"], "coordination", buckets["coordination"]),
        signature.shape_facets,
    ]
    out: list[tuple[str, ...]] = []
    for key in candidates:
        compact = tuple(sorted(dict.fromkeys(item for item in key if item)))
        if len(compact) < 2 or compact in out:
            continue
        out.append(compact)
        if len(out) >= max(1, int(max_keys)):
            break
    return tuple(out)


def _key(
    left_name: str,
    left: tuple[str, ...],
    right_name: str,
    right: tuple[str, ...],
) -> tuple[str, ...]:
    return tuple(
        [
            *(f"{left_name}:{item}" for item in left[:4]),
            *(f"{right_name}:{item}" for item in right[:4]),
        ]
    )


def _mapping(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if is_dataclass(value):
        return asdict(value)
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _pick(data: dict[str, Any], source: Any, key: str) -> Any:
    if key in data:
        return data.get(key)
    return getattr(source, key, None)


def _nested(data: dict[str, Any], key: str, nested_key: str) -> Any:
    value = data.get(key)
    if isinstance(value, dict):
        return value.get(nested_key)
    return None


def _source_text(
    data: dict[str, Any],
    payload: dict[str, Any],
    proposition: dict[str, Any],
    metadata: dict[str, Any],
    event_type: str,
) -> str:
    values = [
        event_type,
        data.get("natural_key"),
        data.get("natural_text"),
        data.get("compact_summary"),
        data.get("reason"),
        data.get("hypothesis_text"),
        proposition.get("statement"),
        proposition.get("observed_tendency"),
        proposition.get("trigger_conditions"),
        payload.get("reason"),
        payload.get("primary_bottleneck"),
        metadata.get("reason"),
    ]
    return " ".join(str(value) for value in values if value).casefold()


def _source_ref(data: dict[str, Any], source: Any) -> str | None:
    for key in (
        "id",
        "model_id",
        "source_observation_id",
        "observation_id",
        "inquiry_session_id",
        "think_run_id",
    ):
        value = _pick(data, source, key)
        if value is not None:
            return f"{key}:{value}"
    return None


def _surface_terms(text: str, domain_facets: tuple[str, ...]) -> tuple[str, ...]:
    stop = {
        "about",
        "after",
        "and",
        "from",
        "into",
        "that",
        "the",
        "this",
        "with",
    }
    values = [
        token
        for token in _tokens(text)
        if len(token) >= 4 and token not in stop and token not in domain_facets
    ]
    return tuple(dict.fromkeys(values[:16]))


def _tokens(text: str) -> list[str]:
    return [match.group(0).casefold() for match in _TOKEN_RE.finditer(text or "")]


def _text(value: Any) -> str:
    return str(value or "").strip().casefold()


def _optional_text(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _normalize(value: Any) -> str:
    return re.sub(r"[^a-z0-9_:-]+", "_", _text(value)).strip("_")


def _facet(prefix: str, value: Any) -> str | None:
    normalized = _normalize(value)
    if not normalized:
        return None
    return f"{prefix}:{normalized}"


def _prefixed(prefix: str, values: Iterable[Any] | None) -> tuple[str, ...]:
    return _dedupe(_facet(prefix, value) for value in values or ())


def _listish(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return (_normalize(value),) if _normalize(value) else ()
    if isinstance(value, dict):
        return tuple(
            _normalize(item)
            for item in value.values()
            if _normalize(item)
        )
    try:
        return tuple(_normalize(item) for item in value if _normalize(item))
    except TypeError:
        normalized = _normalize(value)
        return (normalized,) if normalized else ()


def _string_refs(value: Any) -> tuple[str, ...]:
    refs: list[str] = []
    if isinstance(value, dict):
        value = value.values()
    if value is None or isinstance(value, str):
        iterable = [value] if value else []
    else:
        try:
            iterable = list(value)
        except TypeError:
            iterable = [value]
    for item in iterable:
        if isinstance(item, dict):
            raw = item.get("id") or item.get("name") or item.get("label")
        else:
            raw = item
        text = _normalize(raw)
        if text:
            refs.append(text)
    return _dedupe(refs)


def _text_facets(tokens: set[str], mapping: dict[str, str]) -> tuple[str, ...]:
    return _dedupe(mapping[token] for token in sorted(tokens & set(mapping)))


def _dedupe(values: Iterable[Any]) -> tuple[str, ...]:
    out: list[str] = []
    for value in values:
        if value is None:
            continue
        text = _normalize(value)
        if not text or text in out:
            continue
        out.append(text)
    return tuple(out)


def _stable_hash(payload: dict[str, Any]) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:24]


__all__ = [
    "build_structural_signature",
    "structural_neighborhood_keys",
    "structural_signature_from_model",
    "structural_signature_from_outcome_event",
]
