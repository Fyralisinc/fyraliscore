import json

from tests.evaluation.entity_extraction_gold_corpus import executable_report, sealed_corpus


def test_sealed_company_entity_corpus_is_batched_broad_and_deterministic() -> None:
    signals, mentions, phenomena = sealed_corpus()
    assert len(signals) == 120
    assert len({signal.batch_id for signal in signals}) == 12
    assert {signal.source_type for signal in signals} == {"jira", "email", "slack"}
    assert {signal.slack_context for signal in signals if signal.source_type == "slack"} == {
        "standalone", "threaded", "cross_thread", "temporally_distributed"
    }
    assert {
        "person", "team", "customer", "project", "workstream", "goal",
        "decision", "commitment", "resource", "system", "service", "incident",
        "issue", "contract", "role", "channel",
    } <= {mention.entity_type for mention in mentions}
    assert sum(not any(mention.signal_id == signal.signal_id for mention in mentions) for signal in signals) >= 12
    assert {"negative", "ambiguous", "abbreviation", "nested", "quoted", "duplicate_surface", "anaphora"} <= {
        item for values in phenomena.values() for item in values
    }
    for mention in mentions:
        signal = next(item for item in signals if item.signal_id == mention.signal_id)
        assert signal.text[mention.start:mention.end]
        assert mention.canonical_referent


def test_current_bootstrap_extractor_has_continuous_stratum_diagnostics() -> None:
    report = executable_report()
    overall = report["overall"]
    assert report["schema_version"] == "gold-entity-extraction-v1"
    assert report["corpus"]["sealed_sha256"] == "37f3ecc89dae02ce7009882870cdad23f1066f25173ba0257a34bc11bee2517c"
    assert overall["signal_count"] == 120
    assert len(report["by_batch"]) == 12
    assert all(metrics["signal_count"] == 10 for metrics in report["by_batch"].values())
    assert 0.0 < overall["span_recall"] < 1.0
    assert 0.0 < overall["boundary_credit_recall"] <= 1.0
    assert overall["candidate_fate_coverage"] == 1.0
    # Candidate discovery cannot honestly claim classification or identity resolution.
    assert overall["type_accuracy"] == 0.0
    assert overall["canonical_link_coverage"] == 0.0
    assert 0.0 <= report["negative_control"]["clean_signal_rate"] <= 1.0
    assert report["prediction_adapter"]["capability_boundary"] == "candidate_surface_discovery_only"
    assert report["pre_improvement_baseline"]["span_f1"] == 0.7384615384615385
    assert {"jira", "email", "slack"} == set(report["by_source"])
    assert "anaphora" in report["by_phenomenon"]
    assert "customer" in report["by_entity_type"]
    # The report is JSON-safe and can be emitted by any evaluation orchestrator.
    json.dumps(report, sort_keys=True)
