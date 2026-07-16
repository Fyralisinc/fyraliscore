from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from lib.contracts import (
    ArchitectureCommitmentClass,
    ArchitectureDecision,
    ArchitectureDecisionRecord,
    ArchitectureHypothesis,
)


def _hypothesis(**overrides: object) -> ArchitectureHypothesis:
    now = datetime.now(UTC)
    values = {
        "hypothesis_id": "joint-entity-inference",
        "version": "v1",
        "statement": "Joint extraction preserves semantic decisions at lower cost.",
        "predicted_metric_changes": {"entity.f1": 0.02, "cost.tokens": -0.2},
        "credible_alternatives": ("staged", "hybrid"),
        "population": "slack-like entity scenarios",
        "operating_region": "small and mid-market tenants",
        "budget": "two sealed component runs",
        "minimum_effect": 0.01,
        "safety_noninferiority": ("INV-05", "INV-35"),
        "rollback_plan": "restore staged topology",
        "registered_at": now,
        "expires_at": now + timedelta(days=30),
    }
    values.update(overrides)
    return ArchitectureHypothesis(**values)


def test_architecture_hypothesis_is_empirical_by_construction() -> None:
    with pytest.raises(ValidationError, match="empirical hypothesis"):
        _hypothesis(
            commitment_class=ArchitectureCommitmentClass.CONSTITUTIONAL_INVARIANT
        )


def test_hypothesis_must_be_preregistered_before_expiry() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError, match="expiry"):
        _hypothesis(registered_at=now, expires_at=now)


def test_substantive_architecture_decision_requires_evidence() -> None:
    with pytest.raises(ValidationError, match="require evidence"):
        ArchitectureDecisionRecord(
            decision_id="decision-1",
            hypothesis_id="joint-entity-inference",
            hypothesis_version="v1",
            decision=ArchitectureDecision.RETAIN,
            evidence_refs=(),
            operating_region="slack-like entity scenarios",
            rationale="looks better",
            compatibility_impact="none",
            decided_by="architecture-council",
            decided_at=datetime.now(UTC),
        )


def test_deferred_architecture_decision_opens_follow_up_obligation() -> None:
    record = ArchitectureDecisionRecord(
        decision_id="decision-1",
        hypothesis_id="joint-entity-inference",
        hypothesis_version="v1",
        decision=ArchitectureDecision.DEFER,
        evidence_refs=(),
        operating_region="slack-like entity scenarios",
        rationale="insufficient rare-entity coverage",
        residual_uncertainty=("rare entity recall",),
        compatibility_impact="none until decided",
        follow_up_obligations=("run rare-entity sealed suite",),
        decided_by="architecture-council",
        decided_at=datetime.now(UTC),
    )
    assert record.follow_up_obligations
