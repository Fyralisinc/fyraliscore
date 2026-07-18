from dataclasses import asdict

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.core_fast_path_gold import (
    build_core_fast_path_gold,
)
from lib.evaluation.epistemic_repair.core_fast_path_population import (
    CORE_FAST_PATH_BATCH_COUNT,
    CORE_FAST_PATH_SIGNAL_COUNT,
    CORE_FAST_PATH_SIGNALS_PER_BATCH,
    build_core_fast_path_population,
)


def test_core_fast_path_population_is_deterministic_batched_and_provider_blind() -> None:
    first = build_core_fast_path_population()
    second = build_core_fast_path_population()

    assert first == second
    assert first.population_digest == second.population_digest
    assert len(first.batches) == CORE_FAST_PATH_BATCH_COUNT
    assert len(first.signals) == CORE_FAST_PATH_SIGNAL_COUNT
    assert all(
        len(batch.signals) == CORE_FAST_PATH_SIGNALS_PER_BATCH
        for batch in first.batches
    )
    assert all(
        tuple(signal.position for signal in batch.signals) == tuple(range(1, 26))
        for batch in first.batches
    )
    assert len({signal.signal_id for signal in first.signals}) == 100
    assert all(signal.signal_id not in signal.text for signal in first.signals)
    assert all(
        forbidden not in signal.text.casefold()
        for signal in first.signals
        for forbidden in (
            "benchmark", "gold label", "create a model", "update memory",
            "synthesis target", "storyline id",
        )
    )

    payload = {
        "version": first.version,
        "batches": [asdict(batch) for batch in first.batches],
    }
    assert first.population_digest == canonical_sha256(payload)


def test_core_fast_path_population_preserves_mixed_sources_and_zero_unbatched_input() -> None:
    population = build_core_fast_path_population()

    for batch in population.batches:
        channels = {signal.source_channel for signal in batch.signals}
        assert {
            "slack:message", "jira:issue", "email:message", "crm:activity"
        }.issubset(channels)
        assert sum(signal.source_space == "slack:harbor-release" for signal in batch.signals) == 2
    assert {
        signal.trust_tier for signal in population.batches[3].signals
        if signal.signal_id.startswith("cf2-harbor-")
    } == {"unvetted", "authoritative"}


def test_core_fast_path_gold_is_complete_separate_and_digest_bound() -> None:
    population = build_core_fast_path_population()
    gold = build_core_fast_path_gold()

    assert gold.population_digest == population.population_digest
    assert len(gold.signals) == CORE_FAST_PATH_SIGNAL_COUNT
    assert {row.signal_id for row in gold.signals} == {
        signal.signal_id for signal in population.signals
    }
    assert sum(row.role == "synthesis_conclusion" for row in gold.signals) == 1
    assert sum(row.role == "authoritative_correction" for row in gold.signals) == 1
    assert gold.expected_scope_ref == "workstream:harbor-release"
    assert gold.synthesis_signal_id != gold.correction_signal_id

    body = {
        "population_digest": gold.population_digest,
        "signals": [
            {
                "signal_id": row.signal_id,
                "storyline_id": row.storyline_id,
                "role": row.role,
                "canonical_ref": row.canonical_ref,
                "expected_surface": row.expected_surface,
                "expected_authority": row.expected_authority,
            }
            for row in gold.signals
        ],
        "synthesis_signal_id": gold.synthesis_signal_id,
        "correction_signal_id": gold.correction_signal_id,
        "expected_scope_ref": gold.expected_scope_ref,
        "expected_thesis": gold.expected_thesis,
        "expected_relation_kind": gold.expected_relation_kind,
    }
    assert gold.gold_digest == canonical_sha256(body)
