"""Latent SAGE pattern discovery.

This package finds and scores structural regularities for adaptive policy.
It does not write canonical Models.
"""
from services.reasoning.sage.patterns.counterexamples import (
    attach_counterexamples,
    find_pattern_counterexamples,
)
from services.reasoning.sage.patterns.feedback import (
    PatternReviewFeedbackReport,
    PatternReviewOutcome,
    record_pattern_review_feedback,
)
from services.reasoning.sage.patterns.drift import (
    PatternModelRepairProposal,
    pattern_model_repair_proposals_from_profile,
)
from services.reasoning.sage.patterns.global_scouts import (
    GlobalScoutReport,
    scout_global_patterns,
)
from services.reasoning.sage.patterns.promotion import (
    assess_promotion_readiness,
    think_review_notes,
)
from services.reasoning.sage.patterns.signatures import (
    build_structural_signature,
    structural_neighborhood_keys,
    structural_signature_from_model,
    structural_signature_from_outcome_event,
)
from services.reasoning.sage.patterns.types import (
    PatternCounterexample,
    PatternScoutCandidate,
    PromotionAssessment,
    StructuralSignature,
)


__all__ = [
    "GlobalScoutReport",
    "PatternCounterexample",
    "PatternModelRepairProposal",
    "PatternReviewFeedbackReport",
    "PatternReviewOutcome",
    "PatternScoutCandidate",
    "PromotionAssessment",
    "StructuralSignature",
    "assess_promotion_readiness",
    "attach_counterexamples",
    "build_structural_signature",
    "find_pattern_counterexamples",
    "pattern_model_repair_proposals_from_profile",
    "record_pattern_review_feedback",
    "scout_global_patterns",
    "structural_neighborhood_keys",
    "structural_signature_from_model",
    "structural_signature_from_outcome_event",
    "think_review_notes",
]
