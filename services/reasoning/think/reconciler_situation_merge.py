"""Situation merge payload helpers for Think reconciliation."""

from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.shared.memory_grammar import derive_memory_grammar


def build_situation_merge_payload(
    *,
    entry: dict[str, Any],
    best_row: dict[str, Any],
    source_event_id: UUID | None,
) -> dict[str, Any] | None:
    """Build the private applier payload for evolving an existing situation."""
    candidate_prop = _normalize_jsonish(entry.get("proposition"))
    existing_prop = _normalize_jsonish(best_row.get("proposition"))
    if not isinstance(candidate_prop, dict) or not isinstance(existing_prop, dict):
        return None
    candidate_grammar = derive_memory_grammar(candidate_prop)
    existing_grammar = derive_memory_grammar(existing_prop)
    if (
        candidate_grammar.claim_role != "situation"
        or existing_grammar.claim_role != "situation"
    ):
        return None

    merged = dict(existing_prop)
    old_members = _member_model_ids(existing_prop)
    candidate_members = _member_model_ids(candidate_prop)
    if not candidate_members:
        return None

    member_ids = _merge_uuid_lists(
        existing_prop.get("member_model_ids"),
        candidate_prop.get("member_model_ids"),
    )
    if len(member_ids) < 2:
        return None
    merged["member_model_ids"] = member_ids

    event_ids = _merge_uuid_lists(
        existing_prop.get("evidence_event_ids"),
        candidate_prop.get("evidence_event_ids"),
        [str(source_event_id)] if source_event_id is not None else [],
    )
    if event_ids:
        merged["evidence_event_ids"] = event_ids

    for key in ("affected_decisions", "affected_customers", "affected_teams"):
        merged_values = _merge_string_lists(
            existing_prop.get(key),
            candidate_prop.get(key),
        )
        if merged_values:
            merged[key] = merged_values

    for key in (
        "summary",
        "relationship_summary",
        "shared_mechanism",
        "judgment_change",
        "open_falsifier",
    ):
        candidate_value = candidate_prop.get(key)
        existing_value = merged.get(key)
        if (
            isinstance(candidate_value, str)
            and candidate_value.strip()
            and (
                not isinstance(existing_value, str)
                or len(candidate_value) > len(existing_value)
            )
        ):
            merged[key] = candidate_value

    candidate_status = candidate_prop.get("status")
    if candidate_status in {"forming", "active", "contested", "resolved"}:
        existing_status = merged.get("status")
        if existing_status in {None, "", "forming"} or candidate_status in {
            "active",
            "contested",
            "resolved",
        }:
            merged["status"] = candidate_status

    candidate_tags = set(candidate_grammar.domain_tags)
    candidate_tags.update(str(tag) for tag in (entry.get("domain_tags") or []))
    candidate_tags.update(
        str(tag)
        for tag in candidate_prop.get("domain_tags", [])
        if isinstance(tag, str)
    )
    return {
        "proposition": merged,
        "added_member_model_ids": [
            str(uid) for uid in (candidate_members - old_members)
        ],
        "candidate_domain_tags": sorted(tag for tag in candidate_tags if tag),
        "candidate_natural": str(entry.get("natural") or "")[:1000],
    }


def _normalize_jsonish(value: Any) -> Any:
    if isinstance(value, str):
        try:
            import json
            return json.loads(value)
        except ValueError:
            return value
    return value


def _member_model_ids(payload: Any) -> set[UUID]:
    payload = _normalize_jsonish(payload)
    if not isinstance(payload, dict):
        return set()
    raw = payload.get("member_model_ids")
    if not isinstance(raw, (list, tuple)):
        return set()
    ids: set[UUID] = set()
    for value in raw:
        uid = _coerce_uuid(value)
        if uid is not None:
            ids.add(uid)
    return ids


def _coerce_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (TypeError, ValueError, AttributeError):
        return None


def _merge_uuid_lists(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[UUID] = set()
    for value in values:
        if not isinstance(value, (list, tuple)):
            continue
        for raw in value:
            uid = _coerce_uuid(raw)
            if uid is None or uid in seen:
                continue
            seen.add(uid)
            out.append(str(uid))
    return out


def _merge_string_lists(*values: Any) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        if not isinstance(value, (list, tuple)):
            continue
        for raw in value:
            text = str(raw).strip()
            if not text:
                continue
            key = text.casefold()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
    return out
