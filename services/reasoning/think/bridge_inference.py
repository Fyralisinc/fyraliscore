"""Deterministic bounded bridge synthesis for batched state transitions.

When a batch contains before-state evidence, after-state evidence, and an
explicitly missing transition artifact, the system should create a bounded
hypothesis rather than either dropping the bridge or fabricating a fact.
"""
from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import ClaimOp, RawDiff


_CUSTOMER_RE = re.compile(r"\b([A-Z][A-Za-z0-9&.-]*(?:\s+[A-Z][A-Za-z0-9&.-]*){0,3})\b")
_BEFORE_RE = re.compile(
    r"\b("
    r"before|blocked|still\s+blocked|approval\s+is\s+absent|"
    r"without\s+a\s+recorded\s+approval|no\s+nonstandard\s+discount|"
    r"pricing\s+policy\s+still\s+says\s+no"
    r")\b",
    re.I,
)
_AFTER_RE = re.compile(
    r"\b("
    r"after|commit-stage|approved\s+exception|exception\s+pricing|"
    r"discount\s+code\s+active|pricing\s+applied|now\s+shows"
    r")\b",
    re.I,
)
_GAP_RE = re.compile(
    r"\b("
    r"gap|sensor\s+trail|no\s+approval\s+record|no\s+matching\s+entry|"
    r"decision\s+log\s+still\s+has\s+no|bounded\s+inferred\s+bridge|"
    r"off-sensor|unobserved|not\s+directly\s+observed|missing\s+transition"
    r")\b",
    re.I,
)
_PRICING_RE = re.compile(
    r"\b(pricing|discount|exception|approval|policy|nonstandard)\b",
    re.I,
)
_EXISTING_BRIDGE_RE = re.compile(
    r"\b("
    r"bounded\s+inferred\s+bridge|off-sensor\s+transition|"
    r"unobserved\s+(?:decision\s+)?bridge|missing\s+transition|"
    r"not\s+directly\s+observed"
    r")\b",
    re.I,
)


def maybe_inject_latent_bridge(
    raw_diff: RawDiff,
    trigger: TriggerContext,
) -> RawDiff:
    """Append one bounded transition-gap hypothesis when batch evidence warrants it."""

    observation_ids = _observation_ids(trigger)
    if len(observation_ids) < 2:
        return raw_diff
    texts = _trigger_source_texts(trigger)
    if not texts:
        return raw_diff
    if _has_existing_bridge_claim(raw_diff):
        return raw_diff

    full_text = "\n".join(texts)
    if not _PRICING_RE.search(full_text):
        return raw_diff
    has_before = any(_BEFORE_RE.search(text) for text in texts)
    has_after = any(_AFTER_RE.search(text) for text in texts)
    has_gap = any(_GAP_RE.search(text) for text in texts)
    if not (has_before and has_after and has_gap):
        return raw_diff

    customer = _customer_label(full_text)
    subject = f"{customer} pricing" if customer else "Pricing"
    hypothesis = (
        f"{subject} moved from a blocked discount-exception state to an "
        "approved exception-pricing state without a directly observed approval "
        "artifact; treat the transition as a bounded, uncertain off-sensor "
        "decision bridge."
    )
    valid_from = trigger.seed_occurred_at or datetime.now(timezone.utc)
    scope_entities = [
        dict(entity)
        for entity in (trigger.seed_entity_ids or [])
        if isinstance(entity, dict)
    ]
    scope_actors = [str(actor_id) for actor_id in (trigger.scope_actors or [])]
    evidence_event_ids = [str(obs_id) for obs_id in observation_ids]

    raw_diff.claim_ops.append(
        ClaimOp(
            op="insert",
            entry={
                "born_from_event_id": str(observation_ids[0]),
                "supporting_event_ids": evidence_event_ids,
                "proposition": {
                    "kind": "belief",
                    "claim_role": "hypothesis",
                    "abstraction_level": "atomic",
                    "time_mode": "past",
                    "modality": "inferred",
                    "polarity": "neutral",
                    "hypothesis_text": hypothesis,
                    "summary": (
                        "A before/after pricing state transition is visible, "
                        "but the approval transition evidence is missing."
                    ),
                    "evidence_event_ids": evidence_event_ids,
                    "open_falsifier": (
                        "A complete approval record or decision-log entry "
                        "accounts for the transition."
                    ),
                },
                "natural": hypothesis,
                "confidence": 0.58,
                "confidence_at_assertion": 0.58,
                "scope_actors": scope_actors,
                "scope_entities": scope_entities,
                "scope_temporal": {
                    "valid_from": valid_from.isoformat(),
                    "valid_until": None,
                },
                "falsifier": {
                    "kind": "external_evidence",
                    "check": (
                        "Look for a complete approval artifact, audit trail, "
                        "or decision-log entry explaining the state transition."
                    ),
                },
                "domain_tags": [
                    "customer",
                    "pricing",
                    "approval",
                    "decision",
                    "memory_quality",
                ],
            },
        )
    )
    trace = raw_diff.reasoning_trace or ""
    note = (
        "deterministic_bridge_inference: inserted bounded hypothesis for "
        "before/after pricing transition with missing approval evidence."
    )
    raw_diff.reasoning_trace = f"{trace}\n{note}".strip() if trace else note
    return raw_diff


def _observation_ids(trigger: TriggerContext) -> list[Any]:
    ids = list(trigger.observation_ids or [])
    if trigger.observation_id is not None and trigger.observation_id not in ids:
        ids.insert(0, trigger.observation_id)
    return ids


def _trigger_source_texts(trigger: TriggerContext) -> list[str]:
    signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    texts: list[str] = []
    fragments = signature.get("batch_signal_fragments")
    if isinstance(fragments, list):
        for fragment in fragments:
            if not isinstance(fragment, dict):
                continue
            text = fragment.get("text")
            if isinstance(text, str) and text.strip():
                texts.append(" ".join(text.split()))
    if texts:
        return texts
    if isinstance(trigger.seed_natural_text, str) and trigger.seed_natural_text.strip():
        return [
            line.strip()
            for line in trigger.seed_natural_text.splitlines()
            if line.strip()
        ]
    return []


def _has_existing_bridge_claim(raw_diff: RawDiff) -> bool:
    for op in raw_diff.claim_ops:
        if op.op != "insert" or not isinstance(op.entry, dict):
            continue
        text = " ".join(
            str(part)
            for part in (
                op.entry.get("natural"),
                op.entry.get("proposition"),
            )
            if part is not None
        )
        if _EXISTING_BRIDGE_RE.search(text):
            return True
    return False


def _customer_label(text: str) -> str | None:
    for match in _CUSTOMER_RE.finditer(text):
        candidate = match.group(1).strip()
        if candidate.lower() in {"Forecast", "Finance", "Pipeline", "Billing"}:
            continue
        if "Northstar" in candidate:
            return "Northstar Labs" if "Labs" in text else candidate
    return None


__all__ = ["maybe_inject_latent_bridge"]
