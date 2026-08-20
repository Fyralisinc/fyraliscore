from __future__ import annotations

from datetime import datetime, timezone

import pytest
from pydantic import ValidationError

from lib.shared.ids import uuid7
from services.domain.episodes.contracts import (
    EpisodeAccessManifest,
    EpisodeConstitution,
    EpisodeCoverage,
    EpisodeSnapshot,
    TopicIntent,
)


NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def test_constitution_makes_episode_semantics_explicit() -> None:
    constitution = EpisodeConstitution()
    assert constitution.episode_is_evidence_batch
    assert constitution.membership_is_asserted
    assert constitution.settlement_claims_completeness_not_truth


def test_query_seeded_topic_requires_request_context() -> None:
    with pytest.raises(ValidationError, match="query text and requester"):
        TopicIntent(
            id=uuid7(),
            tenant_id=uuid7(),
            origin="query_seeded",
            label="Audit state",
            router_name="query-topic-seeder",
            router_version="1.0.0",
            created_at=NOW,
        )


def test_snapshot_seal_detects_manifest_tampering() -> None:
    snapshot = EpisodeSnapshot.seal(
        id=uuid7(),
        tenant_id=uuid7(),
        topic_id=uuid7(),
        episode_id=uuid7(),
        version=1,
        lifecycle_state="open",
        observation_ids=(uuid7(),),
        evidence_ids=(uuid7(),),
        claim_ids=(uuid7(),),
        membership_assertion_ids=(uuid7(),),
        access=EpisodeAccessManifest(
            visibility="tenant",
            audience=(),
            policy_hash="a" * 64,
            evidence_policy_hashes=("b" * 64,),
            composition_version="intersection-v1",
            evaluated_at=NOW,
        ),
        coverage=EpisodeCoverage(
            eligible_observation_count=1,
            included_observation_count=1,
            reviewed_exclusion_count=0,
            unresolved_candidate_count=0,
            coverage_recall_proxy=1,
            contamination_precision_proxy=1,
            citation_completeness=1,
            contradiction_preservation=1,
            authorization_violation_count=0,
        ),
        opened_at=NOW,
        cutoff_at=NOW,
        created_at=NOW,
    )
    assert len(snapshot.snapshot_hash) == 64

    tampered = snapshot.model_dump(mode="json")
    tampered["version"] = 2
    with pytest.raises(ValidationError, match="hash does not match"):
        EpisodeSnapshot.model_validate(tampered)
