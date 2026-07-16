from types import SimpleNamespace

from services.domain.entity_grounding import GroundingCandidateInput
from services.workers.entity_resolver.worker import (
    _narrow_candidates_by_type_assessment,
)


def _candidate(entity_type: str, identity: str) -> GroundingCandidateInput:
    return GroundingCandidateInput(
        canonical_ref={"type": entity_type, "id": identity},
        candidate_source="test_registry",
        positive_evidence_refs=(f"test:{identity}",),
    )


def test_ambiguous_identifier_type_confidence_retains_full_candidate_search():
    candidates = [_candidate("goal", "g1"), _candidate("resource", "r1")]
    assessment = SimpleNamespace(type_distribution={"goal": 0.79, "unknown": 0.21})

    assert _narrow_candidates_by_type_assessment(candidates, assessment) == candidates


def test_explicitly_supported_type_confidence_can_narrow_candidates():
    candidates = [_candidate("goal", "g1"), _candidate("resource", "r1")]
    assessment = SimpleNamespace(type_distribution={"goal": 0.93, "unknown": 0.07})

    assert _narrow_candidates_by_type_assessment(candidates, assessment) == [
        candidates[0]
    ]


def test_unknown_registry_type_never_erases_full_candidate_search():
    candidates = [_candidate("goal", "g1"), _candidate("resource", "r1")]
    assessment = SimpleNamespace(type_distribution={"team": 0.94, "unknown": 0.06})

    assert _narrow_candidates_by_type_assessment(candidates, assessment) == candidates
