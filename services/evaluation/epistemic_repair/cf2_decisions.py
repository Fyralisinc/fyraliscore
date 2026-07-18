"""Request-only deterministic decisions for the CF2 compiled Think surface.

This policy intentionally knows nothing about evaluator fixtures or expected
stories.  It can act only on coordinates and evidence printed in the compiled
runtime request and otherwise fails closed.
"""

from __future__ import annotations

import json
import re
from typing import Any, Mapping
from uuid import UUID

from services.evaluation.epistemic_repair.cf2_provider import CF2StructuredRequest


_CANDIDATE = re.compile(r"  <candidate>\n(?P<body>.*?)\n  </candidate>", re.DOTALL)
_MECHANISM = re.compile(
    r"\b(block(?:s|ed|ing)?|because|caus(?:e|es|ed|al)|depend(?:s|ed|ency)?|"
    r"prerequisite|critical path|prevent(?:s|ed)?|enable(?:s|d)?|leads? to|"
    r"waiting on|drives?|results? in)\b",
    re.IGNORECASE,
)
_AUTHORITY = re.compile(
    r"\b(authoritative|higher[- ]authority|system of record|verified by|"
    r"official record|source of truth)\b",
    re.IGNORECASE,
)
_CONTRADICTION = re.compile(
    r"\b(contradict(?:s|ed|ion)?|disprov(?:e|es|ed)|incorrect|replaced by|"
    r"supersed(?:e|es|ed)|no longer true)\b",
    re.IGNORECASE,
)


def _value(raw: str) -> Any:
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw.strip()


def _candidates(user: str) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    for match in _CANDIDATE.finditer(user):
        row: dict[str, Any] = {}
        cards: list[dict[str, Any]] = []
        in_cards = False
        for line in match.group("body").splitlines():
            stripped = line.strip()
            if stripped == "endpoint_model_cards:":
                in_cards = True
                continue
            if in_cards and stripped.startswith("- "):
                value = _value(stripped[2:])
                if isinstance(value, dict):
                    cards.append(value)
                continue
            in_cards = False
            if ": " not in stripped:
                continue
            key, raw = stripped.split(": ", 1)
            row[key] = _value(raw)
        if cards:
            row["endpoint_model_cards"] = cards
        if row.get("candidate_id"):
            candidates.append(row)
    return candidates


def _uuid_strings(value: Any) -> list[str]:
    values = value if isinstance(value, list) else [value]
    result: list[str] = []
    for item in values:
        try:
            normalized = str(UUID(str(item)))
        except (TypeError, ValueError, AttributeError):
            continue
        if normalized not in result:
            result.append(normalized)
    return result


def _event_ids(candidate: Mapping[str, Any]) -> list[str]:
    values: list[Any] = []
    for key in (
        "member_observation_ids",
        "source_observation_ids",
        "relation_evidence_observation_ids",
    ):
        value = candidate.get(key)
        values.extend(value if isinstance(value, list) else ([value] if value else []))
    return _uuid_strings(values)


def _reject(candidate_id: str, reason: str) -> dict[str, Any]:
    return {
        "candidate_id": candidate_id,
        "decision": "reject",
        "operation": "no_op",
        "confidence": 0.99,
        "reason": reason,
    }


def _exact_head_members(candidate: Mapping[str, Any]) -> list[str]:
    members = _uuid_strings(candidate.get("evidence_model_ids"))
    cards = candidate.get("endpoint_model_cards")
    if len(members) < 2 or not isinstance(cards, list):
        return []
    by_id: dict[str, Mapping[str, Any]] = {}
    for card in cards:
        if not isinstance(card, dict):
            continue
        card_ids = _uuid_strings(card.get("id"))
        versions = _uuid_strings(card.get("version_id"))
        if card_ids and versions:
            by_id[card_ids[0]] = card
    return members if set(members) == set(by_id) else []


def _synthesis_endpoints(
    candidate: Mapping[str, Any], members: list[str],
) -> tuple[str, str] | None:
    """Bind blocker -> affected-work endpoints from exact runtime Model cards."""

    cards = candidate.get("endpoint_model_cards")
    if not isinstance(cards, list):
        return None
    source_markers = (
        "open", "incomplete", "missing", "unowned", "ownership",
        "certificate", "renewal", "prerequisite", "waiting",
    )
    target_markers = (
        "delay", "delayed", "slip", "moved", "rollout", "launch window",
        "gate", "blocked", "cannot proceed", "impact",
    )
    scored: list[tuple[str, int, int]] = []
    for card in cards:
        if not isinstance(card, Mapping):
            continue
        card_ids = _uuid_strings(card.get("id"))
        if not card_ids or card_ids[0] not in members:
            continue
        text = " ".join(
            str(card.get(key) or "")
            for key in ("natural", "proposition")
        ).casefold()
        source_score = sum(marker in text for marker in source_markers)
        target_score = sum(marker in text for marker in target_markers)
        scored.append((card_ids[0], source_score, target_score))
    source_rows = sorted(
        (row for row in scored if row[1] > 0),
        key=lambda row: (-(row[1] - row[2]), -row[1], row[0]),
    )
    target_rows = sorted(
        (row for row in scored if row[2] > 0),
        key=lambda row: (-(row[2] - row[1]), -row[2], row[0]),
    )
    for source_id, _source_score, _source_target_score in source_rows:
        for target_id, _target_source_score, _target_score in target_rows:
            if source_id != target_id:
                return source_id, target_id
    return None


def _atomic_decision(candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    text = str(candidate.get("entailed_claim_text") or "").strip()
    scope = str(candidate.get("canonical_scope_ref") or "").strip()
    events = _event_ids(candidate)
    if not text or not scope or ":" not in scope or not events:
        return None
    candidate_id = str(candidate["candidate_id"])
    allowed = set(candidate.get("allowed_operations") or [])
    targets = _uuid_strings(candidate.get("target_model_ids"))
    if "memory_lifecycle" in allowed and len(targets) == 1:
        return {
            "candidate_id": candidate_id,
            "decision": "accept",
            "operation": "memory_lifecycle",
            "confidence": 0.99,
            "model_id": targets[0],
            "lifecycle_action": "confirm",
            "claim_local_evidence_event_ids": events,
            "reason": "Exact closed atomic confirms its single resolved accepted target.",
        }
    if "claim" not in allowed:
        return None
    return {
        "candidate_id": candidate_id,
        "decision": "accept",
        "operation": "claim",
        "confidence": 0.99,
        "claim_role": "fact",
        "claim_text": text,
        "claim_local_evidence_event_ids": events,
        "reason": "Compiler-owned closed atomic has exact scope and local evidence.",
    }


def _authority_contradiction(candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    allowed = set(candidate.get("allowed_operations") or [])
    targets = _uuid_strings(candidate.get("target_model_ids"))
    events = _event_ids(candidate)
    text = " ".join(
        str(candidate.get(key) or "")
        for key in ("proposed_text", "answer_summary", "reason", "observation_evidence")
    )
    if (
        "memory_lifecycle" not in allowed
        or len(targets) != 1
        or not events
        or not candidate.get("counterevidence_ids")
        or not _AUTHORITY.search(text)
        or not _CONTRADICTION.search(text)
    ):
        return None
    return {
        "candidate_id": str(candidate["candidate_id"]),
        "decision": "accept",
        "operation": "memory_lifecycle",
        "confidence": 0.95,
        "model_id": targets[0],
        "lifecycle_action": "supersede",
        "claim_local_evidence_event_ids": events,
        "reason": "Explicit higher-authority counterevidence supersedes the bound head.",
    }


def _synthesis(candidate: Mapping[str, Any]) -> dict[str, Any] | None:
    if candidate.get("candidate_kind") != "synthesis":
        return None
    text = str(candidate.get("proposed_text") or "").strip()
    scope = str(candidate.get("canonical_scope_ref") or "").strip()
    members = _exact_head_members(candidate)
    endpoints = _synthesis_endpoints(candidate, members)
    events = _event_ids(candidate)
    allowed = set(candidate.get("allowed_operations") or [])
    if (
        not text
        or not _MECHANISM.search(text)
        or not scope
        or ":" not in scope
        or len(members) < 2
        or not events
        or "situation_and_edge" not in allowed
        or endpoints is None
    ):
        return None
    source_model_id, target_model_id = endpoints
    return {
        "candidate_id": str(candidate["candidate_id"]),
        "decision": "accept",
        "operation": "situation_and_edge",
        "confidence": min(0.95, max(0.52, float(candidate.get("confidence") or 0.7))),
        "claim_role": "situation",
        "claim_text": text,
        "situation_member_model_ids": members,
        "edge_kind": "blocks",
        "source_model_id": source_model_id,
        "target_model_id": target_model_id,
        "reason": "Exact accepted heads and local evidence support an explicit mechanism.",
    }


def compiled_batch_memory_decisions(
    request: CF2StructuredRequest,
) -> Mapping[str, Any]:
    """Return deterministic, request-only ``BatchMemoryDecisionSet`` data."""

    if request.schema_name != "BatchMemoryDecisionSet":
        raise ValueError("CF2 compiled-memory handler received the wrong schema")
    decisions: list[dict[str, Any]] = []
    synthesis_admitted = False
    for candidate in _candidates(request.user):
        candidate_id = str(candidate["candidate_id"])
        decision = _atomic_decision(candidate) or _authority_contradiction(candidate)
        if decision is None and not synthesis_admitted:
            decision = _synthesis(candidate)
            synthesis_admitted = decision is not None
        decisions.append(decision or _reject(
            candidate_id,
            "Runtime request does not prove a closed mutation invariant.",
        ))
    return {
        "decisions": decisions,
        "reasoning_trace": "CF2 request-only closed-world deterministic adjudication.",
    }


__all__ = ["compiled_batch_memory_decisions"]
