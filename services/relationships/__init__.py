"""Relationship intelligence primitives.

This package owns candidate-level relationship and situation work: cheap
deterministic generation, scoring, ranking, and persistence before a
candidate becomes accepted memory (`model_edges` or a `situation` Model).
"""

from .candidates import (
    CandidateBasis,
    CandidateKind,
    JudgmentScores,
    ModelSignal,
    RelationshipCandidate,
    generate_scope_overlap_candidates,
    make_edge_candidate,
    make_situation_candidate,
    rank_candidates,
)
from .repo import RelationshipCandidatesRepo

__all__ = [
    "CandidateBasis",
    "CandidateKind",
    "JudgmentScores",
    "ModelSignal",
    "RelationshipCandidate",
    "generate_scope_overlap_candidates",
    "make_edge_candidate",
    "make_situation_candidate",
    "rank_candidates",
    "RelationshipCandidatesRepo",
]
