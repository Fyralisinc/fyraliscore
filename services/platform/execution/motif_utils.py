"""Pure retrieval motif helpers for inquiry execution."""

from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

from services.reasoning.retrieval.primary import TriggerContext

from .routing import signal_class_for_trigger, trigger_text
from .types import RetrievalAction

_MOTIF_DOMAIN_KEYWORDS = frozenset(
    {
        "arr",
        "audit",
        "blocker",
        "capacity",
        "churn",
        "commitment",
        "compliance",
        "customer",
        "data",
        "dependency",
        "evidence",
        "export",
        "freshness",
        "incident",
        "liability",
        "mapping",
        "onboarding",
        "permission",
        "policy",
        "procurement",
        "renewal",
        "replay",
        "risk",
        "saml",
        "security",
        "soc2",
        "terms",
        "trail",
    }
)


def json_obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, (bytes, bytearray)):
        value = value.decode()
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def motif_signature_for(
    trigger: TriggerContext,
    question_primitive: str,
) -> dict[str, Any]:
    return {
        "signal_type": trigger.kind,
        "signal_class": signal_class_for_trigger(trigger),
        "question_primitive": question_primitive,
        "entity_types": sorted(
            {
                str(entity.get("type") or "").casefold()
                for entity in trigger.seed_entity_ids
                if isinstance(entity, dict) and entity.get("type")
            }
        ),
        "domain_terms": motif_domain_terms(trigger_text(trigger)),
    }


def motif_domain_terms(text: str) -> list[str]:
    terms = {
        token.casefold()
        for token in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]*", text or "")
        if token.casefold() in _MOTIF_DOMAIN_KEYWORDS
    }
    return sorted(terms)[:16]


def motif_signature_match_score(
    stored: dict[str, Any],
    current: dict[str, Any],
) -> float:
    score = 0.0
    if stored.get("signal_type") == current.get("signal_type"):
        score += 0.24
    if stored.get("signal_class") == current.get("signal_class"):
        score += 0.16
    if stored.get("question_primitive") == current.get("question_primitive"):
        score += 0.20
    score += 0.20 * set_overlap_ratio(
        stored.get("entity_types"),
        current.get("entity_types"),
    )
    domain_overlap = set_overlap_ratio(
        stored.get("domain_terms"),
        current.get("domain_terms"),
    )
    if domain_overlap == 0.0 and not stored.get("domain_terms"):
        domain_overlap = 0.5
    score += 0.20 * domain_overlap
    return round(min(score, 1.0), 4)


def set_overlap_ratio(left: Any, right: Any) -> float:
    left_set = {str(v) for v in left or [] if str(v)}
    right_set = {str(v) for v in right or [] if str(v)}
    if not left_set or not right_set:
        return 0.0
    return len(left_set & right_set) / max(len(left_set | right_set), 1)


def action_motif_uuid(action: RetrievalAction) -> UUID | None:
    return safe_uuid((action.filters or {}).get("_motif_id"))


def safe_uuid(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def safe_int(value: Any) -> int:
    try:
        return max(0, int(value or 0))
    except (TypeError, ValueError):
        return 0


def packet_used_evidence_ids(packet: dict[str, Any]) -> set[str]:
    tiers = (packet or {}).get("tiers") or {}
    used: set[str] = set()
    for item in tiers.get("decisive_evidence", []) or []:
        if isinstance(item, dict) and item.get("evidence_id"):
            used.add(str(item["evidence_id"]))
    for group in tiers.get("supporting_evidence_groups", []) or []:
        if not isinstance(group, dict):
            continue
        for evidence_id in group.get("evidence_ids", []) or []:
            used.add(str(evidence_id))
    return used


def motif_plan_from_actions(
    actions: list[RetrievalAction],
) -> dict[str, Any]:
    initializer_paths = {"focused_index", "structural"}
    has_initializer = any(action.path in initializer_paths for action in actions)
    out: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for action in actions:
        key = (action.path, action.target)
        if key in seen:
            continue
        seen.add(key)
        if action.filters.get("_motif_stage"):
            try:
                stage = max(1, int(action.filters.get("_motif_stage") or 1))
            except (TypeError, ValueError):
                stage = 1
            bind_previous = bool(action.filters.get("_bind_previous_scope"))
        else:
            stage = 1 if action.path in initializer_paths or not has_initializer else 2
            bind_previous = stage > 1 and action.path in {
                "focused_index",
                "model_edge",
                "semantic",
                "temporal",
            }
        out.append(
            {
                "path": action.path,
                "target": action.target,
                "budget": int(action.budget),
                "stage": stage,
                "bind_previous_scope": bind_previous,
            }
        )
    out.sort(
        key=lambda item: (
            int(item["stage"]),
            str(item["path"]),
            str(item["target"]),
        )
    )
    return {
        "version": 1,
        "execution": "staged",
        "actions": out[:5],
    }


__all__ = [
    "action_motif_uuid",
    "json_obj",
    "motif_domain_terms",
    "motif_plan_from_actions",
    "motif_signature_for",
    "motif_signature_match_score",
    "packet_used_evidence_ids",
    "safe_int",
    "safe_uuid",
    "set_overlap_ratio",
]
