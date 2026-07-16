"""Governed company-intent acquisition and constitutive application."""

from .repo import (
    AppliedIntentMutation,
    IntentApplyResult,
    IntentApplier,
    ProposalAppender,
    ensure_legacy_intent_baseline,
)

__all__ = [
    "AppliedIntentMutation",
    "IntentApplyResult",
    "IntentApplier",
    "ProposalAppender",
    "ensure_legacy_intent_baseline",
]
