"""Provider-blind structural checks for claims entering canonical truth."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any


_BUSINESS_KEYS = {
    "actor", "actor_id", "customer", "customer_id", "entity", "entity_id",
    "episode", "episode_id", "project", "project_id", "workstream",
    "workstream_id", "commitment", "commitment_id", "resource_id",
}
_SOURCE_TAGS = {"source", "source_digest", "source_pattern", "coverage_source"}
_GENERIC_TAGS = {"curiosity", "open_question", "generic_curiosity"}


def claim_cohesion_reasons(
    *, proposition: Mapping[str, Any], natural: str,
    scope_coordinates: Iterable[tuple[str, str]] = (),
) -> tuple[str, ...]:
    """Return stable rejection codes for semantically ungrounded claims."""

    if _is_typed_composite(proposition):
        return ()

    tags = {
        str(value).strip().casefold()
        for key in ("domain_tags", "retrieval_tags", "coverage_roles")
        for value in proposition.get(key) or ()
    }
    frame = proposition.get("contextual_frame") or {}
    frame = frame if isinstance(frame, Mapping) else {}
    coordinates = {
        (str(kind).casefold(), str(identifier))
        for kind, identifier in scope_coordinates
        if str(identifier).strip()
    }
    coordinates.update(
        (str(key).casefold(), str(value))
        for key, value in frame.items()
        if key in _BUSINESS_KEYS and _coordinate_value(value)
    )
    about = str(proposition.get("about") or "").strip().casefold()
    role = str(proposition.get("claim_role") or "").strip().casefold()
    abstraction = str(proposition.get("abstraction_level") or "").casefold()
    text = natural.casefold()
    reasons: list[str] = []

    if about in {"batch", "event_batch", "signal_batch"}:
        reasons.append("claim_scope_batch_wrapper")
    if (
        (role == "hypothesis" or tags & _GENERIC_TAGS)
        and not coordinates
        and not any(key in frame for key in ("action", "outcome", "constraint"))
    ):
        reasons.append("claim_scope_generic_curiosity")
    source_marked = bool(tags & _SOURCE_TAGS) or " source" in f" {text}"
    if source_marked and not coordinates:
        reasons.append("claim_scope_source_only")
    if abstraction in {"atomic", "pattern", "hypothesis"} and len(coordinates) > 4:
        reasons.append("claim_scope_high_entropy")
    return tuple(dict.fromkeys(reasons))


def _coordinate_value(value: Any) -> bool:
    if isinstance(value, (str, int)):
        return bool(str(value).strip())
    if isinstance(value, Mapping):
        return bool(value.get("id") or value.get("ref"))
    return False


def _is_typed_composite(proposition: Mapping[str, Any]) -> bool:
    kind = str(proposition.get("kind") or "").casefold()
    abstraction = str(proposition.get("abstraction_level") or "").casefold()
    members = proposition.get("member_model_ids") or ()
    mechanism = proposition.get("mechanism") or proposition.get("causal_mechanism")
    return (
        (kind in {"situation", "mechanism"} or abstraction == "composite")
        and (
            isinstance(members, (list, tuple)) and len(members) >= 2
            or isinstance(mechanism, Mapping)
            and bool(mechanism.get("action") and mechanism.get("outcome"))
        )
    )


__all__ = ["claim_cohesion_reasons"]
