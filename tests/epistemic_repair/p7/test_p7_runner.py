import pytest

from lib.evaluation.epistemic_repair.p7_runner import (
    P7Artifact,
    build_p7_artifact,
    select_strategic_verdict,
)


def test_provider_free_p7_proves_mechanics_but_not_semantic_strategy() -> None:
    artifact = build_p7_artifact()
    assert artifact.deterministic_mechanical_ready is True
    assert artifact.phase_exit_ready is False
    assert artifact.strategic_verdict == "insufficient_evidence"
    assert artifact.executed_world_count == 3
    assert len(artifact.member_results) == 15
    assert all(g.passed for g in artifact.hard_gates)
    assert all(g.state_contract_met for g in artifact.guards)
    primary = next(
        i for i in artifact.paired_intervals if i.comparator_arm == "observation_only"
    )
    assert primary.lower_95 > 0
    assert {row.comparator_arm for row in artifact.paired_comparisons} == {
        "frozen", "observation_only", "memory_hidden", "corrupted"
    }


def test_deterministic_metrics_can_never_select_semantic_strategy() -> None:
    artifact = build_p7_artifact()
    by_arm = {
        row.arm_id: row.mature
        for row in artifact.member_results
        if row.world_id == "p7-world-01"
    }
    verdict = select_strategic_verdict(
        provider_mode="deterministic_mechanical",
        comparisons=artifact.paired_comparisons,
        facet_interval=next(
            row for row in artifact.paired_intervals
            if row.comparator_arm == "observation_only"
        ),
        adaptive=by_arm["adaptive"],
        frozen=by_arm["frozen"],
        observation_only=by_arm["observation_only"],
    )
    assert verdict == "insufficient_evidence"


def test_real_provider_decision_policy_has_exact_strategic_forks() -> None:
    artifact = build_p7_artifact()
    by_arm = {
        row.arm_id: row.mature
        for row in artifact.member_results
        if row.world_id == "p7-world-01"
    }
    interval = next(
        row for row in artifact.paired_intervals
        if row.comparator_arm == "observation_only"
    )
    common = {
        "provider_mode": "real_provider",
        "comparisons": artifact.paired_comparisons,
        "facet_interval": interval,
    }
    assert select_strategic_verdict(
        **common,
        adaptive=by_arm["adaptive"],
        frozen=by_arm["frozen"],
        observation_only=by_arm["observation_only"],
    ) == "primary_memory_earned"

    saturated = by_arm["observation_only"]
    compressed = saturated.model_copy(
        update={"prompt_tokens": int(saturated.prompt_tokens * 0.70)}
    )
    assert select_strategic_verdict(
        **common,
        adaptive=compressed,
        frozen=saturated,
        observation_only=saturated,
    ) == "limited_compression_value"

    degraded = saturated.model_copy(
        update={"direct_thesis_accuracy": 0.2, "atomic_claim_f1": 0.2}
    )
    assert select_strategic_verdict(
        **common,
        adaptive=degraded,
        frozen=saturated,
        observation_only=saturated,
    ) == "not_earned"


def test_artifact_rejects_provider_free_strategic_overclaim() -> None:
    artifact = build_p7_artifact()
    payload = artifact.model_dump(mode="json", exclude={"content_digest"})
    payload["strategic_verdict"] = "primary_memory_earned"
    from lib.contracts.kernel import canonical_sha256

    payload["content_digest"] = canonical_sha256(payload)
    with pytest.raises(ValueError, match="provider-free evidence"):
        P7Artifact.model_validate(payload)


def test_artifact_rejects_dropped_paired_member_even_with_rehashed_payload() -> None:
    artifact = build_p7_artifact()
    payload = artifact.model_dump(mode="json", exclude={"content_digest"})
    payload["member_results"] = payload["member_results"][:-1]
    from lib.contracts.kernel import canonical_sha256

    payload["content_digest"] = canonical_sha256(payload)
    with pytest.raises(ValueError, match="every world-arm unit"):
        P7Artifact.model_validate(payload)
