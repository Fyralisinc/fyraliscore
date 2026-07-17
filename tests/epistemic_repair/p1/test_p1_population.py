from lib.evaluation.epistemic_repair.p1_population import (
    build_p1_population,
    production_payload,
)


def test_population_is_two_batches_of_ten_with_stable_identity():
    first = build_p1_population()
    second = build_p1_population()

    assert [len(batch) for batch in first.batches] == [10, 10]
    assert len({signal.signal_id for batch in first.batches for signal in batch}) == 20
    assert first.digest == second.digest
    assert len(first.digest) == 64


def test_population_contains_mixed_sources_and_subtle_noise():
    population = build_p1_population()
    sources = {signal.source for batch in population.batches for signal in batch}
    dispositions = {
        signal.expected_disposition
        for batch in population.batches
        for signal in batch
    }

    assert sources == {"chat", "issue", "email"}
    assert dispositions == {"actionable", "context", "noise"}
    assert sum(
        signal.expected_disposition == "noise" for signal in population.batches[1]
    ) >= 2


def test_faults_cover_timeout_and_invalid_structure_on_distinct_calls():
    faults = build_p1_population().faults

    assert {(fault.logical_call_ordinal, fault.outcome) for fault in faults} == {
        (1, "timeout"),
        (2, "invalid_structured_response"),
    }
    assert all(fault.physical_attempt_ordinal == 1 for fault in faults)


def test_evaluator_labels_are_stripped_from_production_payload():
    signal = build_p1_population().batches[0][0]
    payload = production_payload(signal)

    assert "expected_disposition" not in payload
    assert "batch_id" not in payload
    assert payload["content"] == signal.content


def test_contents_do_not_contain_evaluator_or_fixture_hints():
    forbidden = (
        "benchmark",
        "fixture",
        "storyline",
        "expected_disposition",
        "gold label",
        "score",
    )
    contents = " ".join(
        signal.content.lower()
        for batch in build_p1_population().batches
        for signal in batch
    )

    assert all(term not in contents for term in forbidden)
