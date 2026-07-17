from collections import Counter

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p6_population import build_p6_population


def test_population_is_exact_interleaved_and_preregistered() -> None:
    population = build_p6_population()
    assert len(population.batches) == 12
    assert len(population.signals) == 300
    assert all(len(batch.signals) == 25 for batch in population.batches)
    counts = Counter(item.storyline_id for item in population.gold)
    assert {key: counts[key] for key in ("atlas", "beacon", "cobalt", "delta")} == {
        "atlas": 60, "beacon": 60, "cobalt": 60, "delta": 60,
    }
    assert counts[None] == 60
    for batch in population.batches:
        roles = Counter(next(g.role for g in population.gold if g.signal_id == s.signal_id)
                        for s in batch.signals)
        assert roles == {"storyline": 20, "noise": 3,
                         "high_similarity_distractor": 2} or roles == {
                             "storyline": 19, "synthesis": 1, "noise": 3,
                             "high_similarity_distractor": 2,
                         }
    assert len(population.synthesis_signal_by_storyline) == 4
    source_sets = {
        storyline: {
            signal.source_channel.split(":", 1)[0]
            for signal in population.signals
            if next(g.storyline_id for g in population.gold
                    if g.signal_id == signal.signal_id) == storyline
        }
        for storyline in ("atlas", "beacon", "cobalt", "delta")
    }
    assert source_sets == {
        "atlas": {"slack"},
        "beacon": {"jira", "slack"},
        "cobalt": {"email", "crm"},
        "delta": {"slack", "jira", "email", "crm"},
    }
    assert population.population_digest == build_p6_population().population_digest
    assert population.preregistration_digest == build_p6_population().preregistration_digest
    assert all(not any(term in signal.text.casefold() for term in
                       ("confirms", "falsifies", "update memory"))
               for signal in population.signals)


def test_runtime_signal_has_no_oracle_fields() -> None:
    signal = build_p6_population().signals[0]
    assert set(signal.__slots__) == {
        "signal_id", "batch_number", "position", "source_channel",
        "source_space", "text",
    }
    assert canonical_sha256(signal.text) != build_p6_population().population_digest
