"""Deterministic quality gates for episode-constructor shadow runs."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class EpisodeEvaluationThresholds:
    minimum_recall: float = 0.90
    minimum_precision: float = 0.90
    minimum_citation_completeness: float = 1.0
    minimum_contradiction_preservation: float = 1.0
    minimum_replay_stability: float = 1.0
    maximum_authorization_violations: int = 0


@dataclass(frozen=True)
class EpisodeEvaluationResult:
    recall: float
    precision: float
    citation_completeness: float
    contradiction_preservation: float
    replay_stability: float
    authorization_violations: int
    true_positive_count: int
    false_positive_count: int
    false_negative_count: int
    passed: bool


def validate_evaluation_corpus(corpus: dict[str, Any]) -> None:
    """Reject internally inconsistent gold data before scoring a router."""

    observations = corpus.get("observations")
    episodes = corpus.get("episodes")
    contradictions = corpus.get("contradictions", [])
    if not isinstance(observations, list) or not isinstance(episodes, list):
        raise ValueError("corpus requires observation and episode lists")
    observation_by_id = {
        str(observation.get("id")): observation
        for observation in observations
        if isinstance(observation, dict) and observation.get("id")
    }
    if len(observation_by_id) != len(observations):
        raise ValueError("observation ids must be present and unique")
    evidence_ids = [str(item.get("evidence_id")) for item in observations]
    if any(not item.get("evidence_id") for item in observations):
        raise ValueError("every observation requires an evidence id")
    if len(set(evidence_ids)) != len(evidence_ids):
        raise ValueError("each fixture observation requires distinct evidence")

    episode_ids: set[str] = set()
    for episode in episodes:
        if not isinstance(episode, dict) or not episode.get("id"):
            raise ValueError("episode ids must be present")
        episode_id = str(episode["id"])
        if episode_id in episode_ids:
            raise ValueError("episode ids must be unique")
        episode_ids.add(episode_id)
        positives = {str(item) for item in episode.get("positive_observation_ids", [])}
        negatives = {str(item) for item in episode.get("hard_negative_ids", [])}
        if positives.intersection(negatives):
            raise ValueError("gold positives and hard negatives overlap")
        missing = positives.union(negatives).difference(observation_by_id)
        if missing:
            raise ValueError(f"episode references unknown observations: {sorted(missing)}")

    for contradiction in contradictions:
        episode_id = str(contradiction.get("episode_id"))
        claim_observations = {
            str(item) for item in contradiction.get("observation_ids", [])
        }
        if episode_id not in episode_ids:
            raise ValueError("contradiction references an unknown episode")
        if len(claim_observations) < 2:
            raise ValueError("contradictions require at least two observations")
        if not claim_observations.issubset(observation_by_id):
            raise ValueError("contradiction references an unknown observation")


def _source_acl_allows(observation: dict[str, Any], requester_actor_id: str) -> bool:
    policy = observation.get("access_policy")
    if not isinstance(policy, dict):
        return False
    visibility = policy.get("visibility")
    if visibility in {"public", "tenant"}:
        return True
    if visibility != "restricted":
        return False
    expected = {"type": "actor", "id": requester_actor_id}
    return expected in policy.get("audience", [])


def evaluate_episode_prediction(
    corpus: dict[str, Any],
    *,
    episode_id: str,
    prediction: dict[str, Any],
    thresholds: EpisodeEvaluationThresholds | None = None,
) -> EpisodeEvaluationResult:
    validate_evaluation_corpus(corpus)
    thresholds = thresholds or EpisodeEvaluationThresholds()
    episode = next(
        (item for item in corpus["episodes"] if str(item["id"]) == episode_id),
        None,
    )
    if episode is None:
        raise ValueError(f"unknown evaluation episode {episode_id!r}")

    observation_by_id = {
        str(observation["id"]): observation
        for observation in corpus["observations"]
    }
    gold = {str(item) for item in episode["positive_observation_ids"]}
    memberships = prediction.get("memberships", [])
    predicted = {
        str(item.get("observation_id"))
        for item in memberships
        if isinstance(item, dict) and item.get("observation_id")
    }
    true_positives = gold.intersection(predicted)
    false_positives = predicted.difference(gold)
    false_negatives = gold.difference(predicted)
    recall = len(true_positives) / len(gold) if gold else 1.0
    precision = len(true_positives) / len(predicted) if predicted else 0.0

    valid_citations = 0
    for membership in memberships:
        if not isinstance(membership, dict):
            continue
        observation = observation_by_id.get(str(membership.get("observation_id")))
        if observation is not None:
            if str(membership.get("evidence_id")) == str(observation["evidence_id"]):
                valid_citations += 1
    citation_completeness = (
        valid_citations / len(memberships) if memberships else 0.0
    )

    declared_contradictions = {
        frozenset(str(item) for item in pair)
        for pair in prediction.get("contradictions", [])
        if isinstance(pair, list) and len(pair) >= 2
    }
    gold_contradictions = [
        frozenset(str(item) for item in item["observation_ids"])
        for item in corpus.get("contradictions", [])
        if str(item["episode_id"]) == episode_id
    ]
    preserved = sum(
        1
        for pair in gold_contradictions
        if pair.issubset(predicted) and pair in declared_contradictions
    )
    contradiction_preservation = (
        preserved / len(gold_contradictions) if gold_contradictions else 1.0
    )

    replay_runs = prediction.get("replay_membership_runs", [])
    replay_sets = [frozenset(str(item) for item in run) for run in replay_runs]
    replay_stability = (
        1.0
        if replay_sets and all(run == replay_sets[0] for run in replay_sets[1:])
        else 0.0
    )

    requester = str(episode["requester_actor_id"])
    authorization_violations = sum(
        1
        for observation_id in predicted
        if observation_id not in observation_by_id
        or not _source_acl_allows(observation_by_id[observation_id], requester)
    )

    passed = (
        recall >= thresholds.minimum_recall
        and precision >= thresholds.minimum_precision
        and citation_completeness >= thresholds.minimum_citation_completeness
        and contradiction_preservation
        >= thresholds.minimum_contradiction_preservation
        and replay_stability >= thresholds.minimum_replay_stability
        and authorization_violations <= thresholds.maximum_authorization_violations
    )
    return EpisodeEvaluationResult(
        recall=recall,
        precision=precision,
        citation_completeness=citation_completeness,
        contradiction_preservation=contradiction_preservation,
        replay_stability=replay_stability,
        authorization_violations=authorization_violations,
        true_positive_count=len(true_positives),
        false_positive_count=len(false_positives),
        false_negative_count=len(false_negatives),
        passed=passed,
    )


__all__ = [
    "EpisodeEvaluationResult",
    "EpisodeEvaluationThresholds",
    "evaluate_episode_prediction",
    "validate_evaluation_corpus",
]
