import pytest

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p7_population import build_p7_population
from lib.evaluation.epistemic_repair.p7_real_runner import (
    P7RealArtifact,
    _variant_population,
)


def test_three_world_variants_are_sealed_and_distinct() -> None:
    worlds = build_p7_population().worlds[:3]
    variants = [_variant_population(world, index) for index, world in enumerate(worlds)]
    assert len({variant.population_digest for variant in variants}) == 3
    assert all(len(variant.signals) == 300 for variant in variants)
    assert all(len(variant.batches) == 12 for variant in variants)
    assert all(len({signal.signal_id for signal in variant.signals}) == 300 for variant in variants)


def test_real_artifact_rejects_dropped_call_even_when_rehashed() -> None:
    payload = {
        "schema_version": "epistemic-repair-p7-real-provider-v1",
        "population_digest": "a" * 64,
        "commit_sha": "abc",
        "provider": "codex",
        "model": "gpt-5.4",
        "transport": "cli",
        "world_count": 3,
        "arms": ("adaptive", "frozen", "observation_only", "memory_hidden", "corrupted"),
        "observation_budget": 60,
        "max_output_tokens": 1200,
        "max_attempts_per_call": 1,
        "call_limit_per_unit": 3,
        "call_receipts": (),
        "endpoints": (),
        "unit_evidence": (),
        "paired_mature_comparisons": (),
        "paired_facet_intervals": (),
        "economics_status": "token_usage_unavailable",
        "strategic_decision_reasons": (),
        "failed_paired_units": (),
        "hard_gates": {},
        "strategic_verdict": "insufficient_evidence",
        "phase_exit_ready": False,
        "proof_boundary": (),
    }
    normalized = P7RealArtifact.model_construct(
        **payload, content_digest=""
    ).model_dump(mode="json", exclude={"content_digest"})
    payload["content_digest"] = canonical_sha256(normalized)
    with pytest.raises(ValueError, match="paired units"):
        P7RealArtifact.model_validate(payload)
