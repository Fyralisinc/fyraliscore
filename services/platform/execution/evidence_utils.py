"""Pure evidence and context-packet helpers for inquiry execution."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from .types import EvidenceCard

_OVERLAP_STOPWORDS = {
    "about",
    "active",
    "again",
    "blocked",
    "blocker",
    "blocking",
    "commitment",
    "critical",
    "customer",
    "deadline",
    "delay",
    "deliver",
    "dependency",
    "goal",
    "issue",
    "launch",
    "missing",
    "owner",
    "promised",
    "resolved",
    "risk",
    "same",
    "ship",
    "signal",
    "the",
    "this",
    "unable",
    "unblocked",
}


def stable_hash(value: Any) -> str:
    payload = json.dumps(value, sort_keys=True, default=str, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def evidence_to_dict(card: EvidenceCard) -> dict[str, Any]:
    return {
        "evidence_id": str(card.evidence_id),
        "source_type": card.source_type,
        "source_ref": card.source_ref,
        "summary": card.summary,
        "trust_tier": card.trust_tier,
        "timestamp": card.timestamp.isoformat() if card.timestamp else None,
        "retrieval_paths": sorted(card.retrieval_paths),
        "retrieved_for_questions": sorted(card.retrieved_for_questions),
        "supports_hypotheses": sorted(card.supports_hypotheses),
        "weakens_hypotheses": sorted(card.weakens_hypotheses),
        "contradicts_hypotheses": sorted(card.contradicts_hypotheses),
        "raw_content_ref": card.raw_content_ref,
        "token_estimate": card.token_estimate,
        "access_scope": card.access_scope,
        "sensitivity": card.sensitivity,
        "score": round(card.score, 4),
    }


def jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(jsonable(v) for v in value)
    if isinstance(value, tuple):
        return [jsonable(v) for v in value]
    if isinstance(value, list):
        return [jsonable(v) for v in value]
    if isinstance(value, dict):
        return {str(k): jsonable(v) for k, v in value.items()}
    return value


def compact(text: Any, limit: int) -> str:
    value = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(value) <= limit:
        return value
    return value[: max(0, limit - 3)].rstrip() + "..."


def estimate_tokens(text: Any) -> int:
    return max(1, len(str(text or "")) // 4)


def timestamp_sort_value(value: datetime | None) -> float:
    if value is None:
        return float("-inf")
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def is_stale_relative_to_trigger(
    card: EvidenceCard,
    *,
    trigger_occurred_at: datetime | None,
    stale_after_days: int,
) -> bool:
    if card.timestamp is None or trigger_occurred_at is None:
        return False
    card_time = card.timestamp
    trigger_time = trigger_occurred_at
    if card_time.tzinfo is None:
        card_time = card_time.replace(tzinfo=timezone.utc)
    if trigger_time.tzinfo is None:
        trigger_time = trigger_time.replace(tzinfo=timezone.utc)
    return card_time < trigger_time - timedelta(days=max(1, stale_after_days))


def is_counterevidence_for_leading_hypothesis(card: EvidenceCard) -> bool:
    return (
        "H1" in card.contradicts_hypotheses
        or "H1" in card.weakens_hypotheses
        or "H0" in card.supports_hypotheses
    )


def evidence_supports_ownership(card: EvidenceCard) -> bool:
    lower = card.summary.casefold()
    if "owner=unassigned" in lower:
        return False
    if re.search(
        r"\b(no recorded|no accountable|missing|unresolved|unknown|unclear)\b.{0,40}\bowner\b",
        lower,
    ):
        return False
    if re.search(r"\bowner=[0-9a-f]{8}-[0-9a-f-]{27,}\b", lower):
        return True
    if card.source_type in {"observation", "model"}:
        if re.search(r"\b(owner|owns|responsible|assigned to|dri)\b", lower):
            return not any(
                marker in lower
                for marker in (
                    "owner unknown",
                    "owner unresolved",
                    "no owner",
                    "no recorded",
                    "missing owner",
                    "unassigned",
                    "pending owner",
                )
            )
    return False


def material_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", text.casefold())
        if token not in _OVERLAP_STOPWORDS and not token.isdigit()
    }


def has_material_trigger_overlap(summary_lower: str, trigger_lower: str) -> bool:
    trigger_tokens = material_tokens(trigger_lower)
    if not trigger_tokens:
        return True
    summary_tokens = material_tokens(summary_lower)
    return bool(trigger_tokens & summary_tokens)


def declares_unrelated_to_trigger(summary_lower: str) -> bool:
    return bool(
        re.search(
            r"\b(unrelated to|not related to|different customer|wrong tenant|wrong account)\b",
            summary_lower,
        )
    )


def trust_score(trust: str | None) -> float:
    if trust == "authoritative":
        return 0.30
    if trust == "authoritative_external":
        return 0.26
    if trust == "attested_agent":
        return 0.22
    if trust == "reputable":
        return 0.14
    if trust in {"inferential", "inferential_external"}:
        return 0.06
    if trust == "model":
        return 0.12
    return 0.04


def sensitivity(text: Any) -> str:
    lower = str(text or "").casefold()
    if any(
        word in lower
        for word in ("password", "secret", "api key", "private key", "ssn")
    ):
        return "sensitive"
    return "normal"


__all__ = [
    "compact",
    "declares_unrelated_to_trigger",
    "estimate_tokens",
    "evidence_supports_ownership",
    "evidence_to_dict",
    "has_material_trigger_overlap",
    "is_counterevidence_for_leading_hypothesis",
    "is_stale_relative_to_trigger",
    "jsonable",
    "material_tokens",
    "sensitivity",
    "stable_hash",
    "timestamp_sort_value",
    "trust_score",
]
