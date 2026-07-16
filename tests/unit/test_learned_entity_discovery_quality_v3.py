from collections import Counter
from datetime import date

from tests.evaluation.learned_entity_discovery_development_corpus import DEVELOPMENT_CORPUS
from tests.evaluation.learned_entity_discovery_quality_corpus_v1 import FROZEN_CORPUS
from tests.evaluation.learned_entity_discovery_quality_corpus_v2 import FROZEN_CORPUS_V2
from tests.evaluation.learned_entity_discovery_quality_corpus_v3 import (
    FROZEN_CORPUS_V3,
    FROZEN_SHA256_V3,
    ONE_SHOT_EVIDENCE_METADATA,
    ONTOLOGY_TYPES,
    computed_sha256_v3,
)


def _surfaces(corpus):
    return {mention["surface"].casefold() for row in corpus for mention in row["gold"]}


def test_v3_corpus_is_frozen_and_one_shot_before_provider_execution():
    assert computed_sha256_v3() == FROZEN_SHA256_V3
    assert ONE_SHOT_EVIDENCE_METADATA == {
        "benchmark": "learned-entity-discovery-quality-v3",
        "evidence_class": "sealed_untouched_holdout",
        "sealed_before_first_provider_call": True,
        "provider_execution_count_at_seal": 0,
        "evidence_status": "not_executed",
        "allowed_provider_executions": 1,
        "split_policy": "organization_entity_time_text_disjoint_from_v1_v2_and_development",
        "time_window": "2031-01-01/2032-12-31",
        "canonical_link_claim_permitted": False,
    }


def test_v3_design_is_complete_and_balanced():
    assert len(FROZEN_CORPUS_V3) == 40
    batches = Counter(row["batch_id"] for row in FROZEN_CORPUS_V3)
    assert batches == {f"v3-batch-{index}": 10 for index in range(1, 5)}
    for batch_id in batches:
        rows = [row for row in FROZEN_CORPUS_V3 if row["batch_id"] == batch_id]
        assert sum(bool(row["gold"]) for row in rows) == 5
        assert sum(not row["gold"] for row in rows) == 5
    observed_types = {m["entity_type"] for row in FROZEN_CORPUS_V3 for m in row["gold"]}
    assert observed_types == ONTOLOGY_TYPES
    contexts = {row["slack_context"] for row in FROZEN_CORPUS_V3 if row["source_type"] == "slack"}
    assert contexts == {
        "standalone", "thread_reply", "thread_reply_delayed",
        "cross_thread_reference", "temporal_sequence", "channel_followup",
        "cross_channel_temporal",
    }
    assert sum(len(row["gold"]) >= 4 for row in FROZEN_CORPUS_V3) >= 8
    assert sum(row["source_type"] == "slack" and not row["gold"] for row in FROZEN_CORPUS_V3) >= 8


def test_v3_gold_has_exact_complete_designation_boundaries():
    for row in FROZEN_CORPUS_V3:
        for mention in row["gold"]:
            assert row["text"][mention["start"]:mention["end"]] == mention["surface"]
            assert row["text"].count(mention["surface"]) == 1
            assert mention["canonical_referent"] is None
    complete_designators = {
        "Project Ivory Current", "Commitment MC-41", "Obsidian Meadow workstream",
        "product BrightLedger", "customer Fjord & Fable", "system Riverlock-Prime",
    }
    assert {value.casefold() for value in complete_designators} <= _surfaces(
        FROZEN_CORPUS_V3
    )


def test_v3_is_text_entity_organization_and_time_disjoint():
    prior = tuple(FROZEN_CORPUS) + tuple(FROZEN_CORPUS_V2) + tuple(DEVELOPMENT_CORPUS)
    assert {row["text"] for row in FROZEN_CORPUS_V3}.isdisjoint(
        row["text"] for row in prior
    )
    assert _surfaces(FROZEN_CORPUS_V3).isdisjoint(_surfaces(prior))
    assert all("2031-" in row["text"] or "2032-" in row["text"] or row["source_type"] != "slack" for row in FROZEN_CORPUS_V3)
    assert date.fromisoformat(ONE_SHOT_EVIDENCE_METADATA["time_window"].split("/")[0]).year == 2031
    assert date.fromisoformat(ONE_SHOT_EVIDENCE_METADATA["time_window"].split("/")[1]).year == 2032


def test_v3_negative_design_includes_transport_and_syntax_pressure():
    negative_text = "\n".join(row["text"] for row in FROZEN_CORPUS_V3 if not row["gold"])
    for marker in ("/v3/accounts", "request_id", "customer_ref", "trace-883", "/internal/flush", "0x91fe", "$REDACTED", "/api/owner"):
        assert marker in negative_text
