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
    make_edge_type_candidate,
    make_situation_candidate,
    rank_candidates,
)
from .repo import RelationshipCandidateMetrics, RelationshipCandidatesRepo
from .ontology_proposals import (
    RelationshipOntologyProposal,
    RelationshipOntologyProposalsRepo,
    aggregate_edge_type_candidates,
    normalize_proposed_edge_kind,
)

_ADJUDICATION_EXPORTS = {
    "CandidateAdjudication",
    "adjudicate_candidate_for_trigger",
    "adjudicate_candidates_for_trigger",
    "candidate_id_from_trigger",
    "candidate_ids_from_trigger",
    "load_candidate_for_trigger",
}


def __getattr__(name: str):
    """Load adjudication helpers lazily to avoid topology import cycles."""
    if name in _ADJUDICATION_EXPORTS:
        from . import adjudication

        value = getattr(adjudication, name)
        globals()[name] = value
        return value
    raise AttributeError(name)

__all__ = [
    "CandidateAdjudication",
    "CandidateBasis",
    "CandidateKind",
    "JudgmentScores",
    "ModelSignal",
    "RelationshipCandidate",
    "RelationshipCandidateMetrics",
    "RelationshipOntologyProposal",
    "RelationshipOntologyProposalsRepo",
    "generate_scope_overlap_candidates",
    "aggregate_edge_type_candidates",
    "make_edge_candidate",
    "make_edge_type_candidate",
    "make_situation_candidate",
    "normalize_proposed_edge_kind",
    "rank_candidates",
    "RelationshipCandidatesRepo",
    "adjudicate_candidate_for_trigger",
    "adjudicate_candidates_for_trigger",
    "candidate_id_from_trigger",
    "candidate_ids_from_trigger",
    "load_candidate_for_trigger",
]
