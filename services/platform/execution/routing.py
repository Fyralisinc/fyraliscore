"""Route and budget helpers for adaptive inquiry execution."""

from __future__ import annotations

from typing import Any, Literal

from services.reasoning.retrieval.primary import TriggerContext

from .config import InquiryConfig
from .language_signals import (
    has_broad_signal_language,
    signal_has_material_update_intent,
)
from .types import SignalRoute


def route_for_trigger(trigger: TriggerContext) -> SignalRoute:
    if trigger.kind == "T2":
        return "DETERMINISTIC_UPDATE"
    if trigger.kind == "T3":
        return "BACKGROUND_PATH"
    if trigger.kind == "T4":
        return "BACKGROUND_PATH"
    return "DEEP_INQUIRY_PATH"


def signal_class_for_trigger(trigger: TriggerContext) -> str:
    lower = trigger_text(trigger).casefold()
    if trigger.kind == "T1" and declares_no_material_update(lower):
        return "weak"
    if has_broad_signal_language(lower):
        return "broad"
    if trigger.kind == "T1" and not signal_has_material_update_intent(lower):
        return "weak"
    return "material"


def cold_weak_noop_gate(
    trigger: TriggerContext,
    signal_class: str,
) -> dict[str, Any]:
    if signal_class != "weak":
        return {"used": False, "reason": "not_weak_signal"}
    lower = trigger_text(trigger).casefold()
    if not declares_no_material_update(lower):
        return {"used": False, "reason": "weak_signal_needs_disambiguation"}
    return {
        "used": True,
        "reason": (
            "weak signal is non-actionable workspace chatter or explicitly "
            "declares no material update"
        ),
    }


def declares_no_material_update(lower: str) -> bool:
    grouped_no_update_phrases = (
        "no blocker, owner change, decision, customer risk, or commitment update",
        "no blocker, no owner change, no decision, no customer risk, or no commitment update",
    )
    no_update_phrases = (
        "no blocker",
        "no owner change",
        "no decision",
        "no customer risk",
        "no commitment update",
        "no risk",
        "no action",
        "no actionable",
    )
    weak_chatter_phrases = (
        "workspace chatter",
        "weak workspace noise",
        "lunch notes",
        "travel plans",
        "general team coordination",
    )
    no_update_count = sum(1 for phrase in no_update_phrases if phrase in lower)
    chatter_count = sum(1 for phrase in weak_chatter_phrases if phrase in lower)
    grouped_declared = any(phrase in lower for phrase in grouped_no_update_phrases)
    if chatter_count >= 3:
        return True
    return (grouped_declared or no_update_count >= 2) and chatter_count >= 1


def adaptive_baseline_top_n(candidate_top_n: int, signal_class: str) -> int:
    if signal_class == "weak":
        return min(candidate_top_n, 80)
    if signal_class == "broad":
        return min(candidate_top_n, 220)
    return min(candidate_top_n, 150)


def adaptive_evidence_limit(
    cfg: InquiryConfig,
    *,
    route: SignalRoute,
    mode: Literal["deep", "fast"],
    signal_class: str,
) -> int:
    if mode == "fast" or route == "FAST_PATH" or signal_class == "weak":
        return max(1, int(cfg.fast_path_evidence_limit))
    configured = max(1, int(cfg.evidence_reservoir_limit))
    if signal_class == "broad":
        return min(configured, max(320, min(560, configured)))
    return min(configured, max(160, min(360, configured)))


def trigger_text(trigger: TriggerContext) -> str:
    return (trigger.seed_natural_text or "").strip()


__all__ = [
    "adaptive_baseline_top_n",
    "adaptive_evidence_limit",
    "cold_weak_noop_gate",
    "declares_no_material_update",
    "route_for_trigger",
    "signal_class_for_trigger",
    "trigger_text",
]
