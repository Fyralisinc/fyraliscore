from collections import Counter

from tests.evaluation.learned_entity_discovery_quality_corpus_v4 import (
    FROZEN_CORPUS_V4,
    FROZEN_SHA256_V4,
    ONTOLOGY_TYPES,
    computed_sha256_v4,
)


def test_broad_v4_holdout_is_frozen_batched_and_well_formed():
    assert computed_sha256_v4() == FROZEN_SHA256_V4
    assert len(FROZEN_CORPUS_V4) == 40
    assert len({row["signal_id"] for row in FROZEN_CORPUS_V4}) == 40
    batches = Counter(row["batch_id"] for row in FROZEN_CORPUS_V4)
    assert batches == {f"v4-batch-{index}": 10 for index in range(1, 5)}
    for batch_id in batches:
        rows = [row for row in FROZEN_CORPUS_V4 if row["batch_id"] == batch_id]
        assert sum(bool(row["gold"]) for row in rows) == 5
    gold = [mention for row in FROZEN_CORPUS_V4 for mention in row["gold"]]
    assert len(gold) == 69
    assert {mention["entity_type"] for mention in gold} == ONTOLOGY_TYPES
    for row in FROZEN_CORPUS_V4:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]


def test_broad_v4_contains_required_difficulty_strata():
    contexts = {row["slack_context"] for row in FROZEN_CORPUS_V4}
    assert {
        "thread_reply", "thread_reply_delayed", "cross_thread_reference",
        "cross_channel_temporal", "temporal_sequence", "channel_followup",
        "standalone", "not_slack",
    } <= contexts
    text = "\n".join(row["text"] for row in FROZEN_CORPUS_V4)
    assert text.count("Team Copper Aurora") == 2
    assert text.count("Commitment MD-14") == 2
    assert "called P-Lantern internally" in text
    assert "system Orchard" in text and "customer Orchard Mutual" in text
