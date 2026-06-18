"""Edge Intelligence Kernel.

This package owns pre-truth edge learning: explicit relation evidence,
cross-model pair evidence, and conservative promotion into the existing
relationship candidate lifecycle.
"""

from .compiler import (
    EdgeCompilerConfig,
    compile_pair_evidence_candidate,
    confidence_from_pair_evidence,
)
from .context_feedback import record_context_use_pair_feedback
from .promoter import PairEvidencePromotionReport, promote_pair_evidence_candidates
from .relation_extractor import ExtractedRelation, extract_relation_evidence
from .relation_frames import (
    RelationFrameProjectionReport,
    RelationProjectionRule,
    project_relation_frame,
    projection_rules_for_relation_kind,
)
from .repo import EdgeIntelligenceMetrics, EdgeIntelligenceRepo
from .types import (
    ModelPairEvidence,
    PairEvidenceObservation,
    RelationClaim,
    RelationEdgeProjection,
    RelationEvidence,
    RelationFrame,
    RelationParticipant,
    canonical_model_pair,
    normalize_primitive,
)

__all__ = [
    "EdgeCompilerConfig",
    "EdgeIntelligenceMetrics",
    "EdgeIntelligenceRepo",
    "ExtractedRelation",
    "ModelPairEvidence",
    "PairEvidenceObservation",
    "PairEvidencePromotionReport",
    "RelationEdgeProjection",
    "RelationFrame",
    "RelationFrameProjectionReport",
    "RelationClaim",
    "RelationEvidence",
    "RelationParticipant",
    "RelationProjectionRule",
    "canonical_model_pair",
    "compile_pair_evidence_candidate",
    "confidence_from_pair_evidence",
    "extract_relation_evidence",
    "normalize_primitive",
    "project_relation_frame",
    "projection_rules_for_relation_kind",
    "promote_pair_evidence_candidates",
    "record_context_use_pair_feedback",
]
