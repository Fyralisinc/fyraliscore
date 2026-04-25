"""Phase 6a structural metrics.

All helpers compare a SUT-produced diff against a reference diff. They are
pure and deterministic so unit tests can assert expected values exactly.
"""

from __future__ import annotations

from collections.abc import Iterable

from lsob_contracts import ClaimOp, DiffOp

_HIGH_CONFIDENCE_THRESHOLD = 0.8
_CONFIDENCE_TOLERANCE = 0.15
_OVER_SPLIT_SUT_THRESHOLD = 5
_OVER_SPLIT_REF_THRESHOLD = 3
_UNDER_SPLIT_SUT_VALUE = 1
_UNDER_SPLIT_REF_THRESHOLD = 2


def state_transition_accuracy(sut: DiffOp, ref: DiffOp) -> float:
    """Fraction of SUT act_ops whose (entity_ref, to_state) matches some ref act_op.

    The reference's act_ops form the ground-truth set of expected transitions.
    We score the SUT side: of the transitions the SUT proposed, how many match
    a reference transition on the (entity_ref, to_state) pair?
    """
    if not sut.act_ops:
        # No transitions proposed: trivially "accurate" only when the reference
        # also had none. This keeps the metric bounded when nothing is expected.
        return 1.0 if not ref.act_ops else 0.0
    ref_pairs: set[tuple[str, str]] = {
        (op.entity_ref, op.to_state) for op in ref.act_ops
    }
    if not ref_pairs:
        return 0.0
    matches = sum(
        1 for op in sut.act_ops if (op.entity_ref, op.to_state) in ref_pairs
    )
    return matches / len(sut.act_ops)


def _claim_match_key(op: ClaimOp) -> tuple[str, frozenset[str]]:
    return (op.proposition_kind, frozenset(op.entities))


def _find_ref_match(
    claim: ClaimOp, ref_claims: Iterable[ClaimOp]
) -> ClaimOp | None:
    """Find a reference claim with same kind and overlapping entities."""
    target_kind = claim.proposition_kind
    target_entities = set(claim.entities)
    best: ClaimOp | None = None
    best_overlap = 0
    for ref in ref_claims:
        if ref.proposition_kind != target_kind:
            continue
        overlap = len(target_entities & set(ref.entities))
        # Require at least one entity overlap OR both sides having no entities.
        if overlap == 0 and (target_entities or ref.entities):
            continue
        if overlap >= best_overlap:
            best = ref
            best_overlap = overlap
    return best


def confidence_alignment_rate(sut: DiffOp, ref: DiffOp) -> float:
    """Fraction of SUT claim_ops whose confidence is within ±0.15 of ref.

    A SUT claim matches a reference claim by (proposition_kind + entity overlap).
    Unmatched SUT claims count as misaligned (denominator includes them).
    """
    if not sut.claim_ops:
        return 1.0 if not ref.claim_ops else 0.0
    aligned = 0
    for claim in sut.claim_ops:
        match = _find_ref_match(claim, ref.claim_ops)
        if match is None:
            continue
        if abs(claim.asserted_confidence - match.asserted_confidence) <= _CONFIDENCE_TOLERANCE:
            aligned += 1
    return aligned / len(sut.claim_ops)


def falsifier_adequacy_rate(diff: DiffOp) -> float:
    """Fraction of SUT high-confidence claims (>=0.8) that include a non-empty falsifier.

    Denominator is the count of high-confidence claims. If there are none, the
    metric is vacuously 1.0 (nothing to fault).
    """
    high = [
        c for c in diff.claim_ops if c.asserted_confidence >= _HIGH_CONFIDENCE_THRESHOLD
    ]
    if not high:
        return 1.0
    with_falsifier = sum(
        1 for c in high if c.falsifier is not None and c.falsifier.strip() != ""
    )
    return with_falsifier / len(high)


def is_over_split(sut: DiffOp, ref: DiffOp) -> bool:
    """True when SUT produced >5 claim_ops while reference had ≤3."""
    return (
        len(sut.claim_ops) > _OVER_SPLIT_SUT_THRESHOLD
        and len(ref.claim_ops) <= _OVER_SPLIT_REF_THRESHOLD
    )


def is_under_split(sut: DiffOp, ref: DiffOp) -> bool:
    """True when SUT produced 1 claim_op while reference had ≥2."""
    return (
        len(sut.claim_ops) == _UNDER_SPLIT_SUT_VALUE
        and len(ref.claim_ops) >= _UNDER_SPLIT_REF_THRESHOLD
    )


def _rate(flags: list[bool]) -> float:
    if not flags:
        return 0.0
    return sum(1 for f in flags if f) / len(flags)


def over_splitting_rate(pairs: list[tuple[DiffOp, DiffOp]]) -> float:
    return _rate([is_over_split(sut, ref) for sut, ref in pairs])


def under_splitting_rate(pairs: list[tuple[DiffOp, DiffOp]]) -> float:
    return _rate([is_under_split(sut, ref) for sut, ref in pairs])


__all__ = [
    "confidence_alignment_rate",
    "falsifier_adequacy_rate",
    "is_over_split",
    "is_under_split",
    "over_splitting_rate",
    "state_transition_accuracy",
    "under_splitting_rate",
]
