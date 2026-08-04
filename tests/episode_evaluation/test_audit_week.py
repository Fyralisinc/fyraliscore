from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from services.domain.episodes.evaluation import (
    evaluate_episode_prediction,
    validate_evaluation_corpus,
)


FIXTURE = Path(__file__).parent / "fixtures" / "audit_week.json"


def _corpus():
    return json.loads(FIXTURE.read_text())


def _perfect_prediction(corpus):
    episode = corpus["episodes"][0]
    by_id = {item["id"]: item for item in corpus["observations"]}
    positives = episode["positive_observation_ids"]
    return {
        "memberships": [
            {
                "observation_id": observation_id,
                "evidence_id": by_id[observation_id]["evidence_id"],
            }
            for observation_id in positives
        ],
        "contradictions": [["obs-04", "obs-05"]],
        "replay_membership_runs": [positives, list(reversed(positives))],
    }


def test_audit_week_gold_corpus_and_perfect_shadow_run_pass() -> None:
    corpus = _corpus()
    validate_evaluation_corpus(corpus)
    result = evaluate_episode_prediction(
        corpus,
        episode_id="audit-week-mainnet",
        prediction=_perfect_prediction(corpus),
    )

    assert result.passed
    assert result.recall == 1
    assert result.precision == 1
    assert result.citation_completeness == 1
    assert result.contradiction_preservation == 1
    assert result.authorization_violations == 0


def test_contamination_bad_citations_acl_leak_and_unstable_replay_fail() -> None:
    corpus = _corpus()
    prediction = _perfect_prediction(corpus)
    prediction["memberships"] = prediction["memberships"][:5]
    prediction["memberships"].extend(
        [
            {"observation_id": "obs-09", "evidence_id": "wrong-evidence"},
            {"observation_id": "obs-11", "evidence_id": "evidence-11"},
            {"observation_id": "obs-12", "evidence_id": "evidence-12"},
        ]
    )
    prediction["contradictions"] = []
    prediction["replay_membership_runs"] = [
        ["obs-01", "obs-02"],
        ["obs-01"],
    ]
    result = evaluate_episode_prediction(
        corpus,
        episode_id="audit-week-mainnet",
        prediction=prediction,
    )

    assert not result.passed
    assert result.false_positive_count == 3
    assert result.false_negative_count == 5
    assert result.citation_completeness < 1
    assert result.contradiction_preservation == 0
    assert result.replay_stability == 0
    assert result.authorization_violations == 2


def test_gold_corpus_rejects_positive_negative_overlap() -> None:
    corpus = copy.deepcopy(_corpus())
    corpus["episodes"][0]["hard_negative_ids"].append("obs-01")
    with pytest.raises(ValueError, match="overlap"):
        validate_evaluation_corpus(corpus)
