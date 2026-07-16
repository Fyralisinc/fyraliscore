import json

from lib.evaluation.entity_extraction_gold import (
    GoldMention,
    GoldSignal,
    PredictedMention,
    evaluate_gold_entity_extraction,
)
from scripts.evaluate_gold_entity_extraction import main


def test_scores_batched_gold_mentions_continuously_and_by_context() -> None:
    signals = (
        GoldSignal(signal_id="s1", batch_id="b1", source_type="slack", text="Acme is blocked", slack_context="threaded"),
        GoldSignal(signal_id="s2", batch_id="b1", source_type="jira", text="Ship Atlas"),
        GoldSignal(signal_id="s3", batch_id="b2", source_type="slack", text="Ask Pat about Acme", slack_context="temporally_distributed"),
    )
    gold = (
        GoldMention(mention_id="g1", signal_id="s1", start=0, end=4, entity_type="customer", canonical_referent="customer:acme"),
        GoldMention(mention_id="g2", signal_id="s2", start=5, end=10, entity_type="project", canonical_referent="project:atlas"),
        GoldMention(mention_id="g3", signal_id="s3", start=4, end=7, entity_type="person", canonical_referent="person:pat"),
        GoldMention(mention_id="g4", signal_id="s3", start=14, end=18, entity_type="customer", canonical_referent="customer:acme"),
    )
    predictions = (
        PredictedMention(prediction_id="p1", signal_id="s1", start=0, end=4, entity_type="customer", canonical_referent="customer:acme", confidence=.95, candidate_fate="resolved"),
        PredictedMention(prediction_id="p2", signal_id="s2", start=5, end=9, entity_type="workstream", canonical_referent="project:wrong", confidence=.9, candidate_fate="review"),
        PredictedMention(prediction_id="p3", signal_id="s3", start=4, end=7, entity_type="person", confidence=.4, abstained=True),
        PredictedMention(prediction_id="p4", signal_id="s3", start=14, end=18, entity_type="customer", canonical_referent="customer:acme", confidence=.8),
        PredictedMention(prediction_id="p5", signal_id="s3", start=14, end=18, entity_type="customer", canonical_referent="customer:acme", confidence=.8),
    )

    report = evaluate_gold_entity_extraction(signals=signals, gold_mentions=gold, predictions=predictions)

    assert report.overall.batch_count == 2
    assert report.overall.span_precision == 3 / 5
    assert report.overall.span_recall == 3 / 4
    assert report.overall.boundary_credit_recall == (1 + .8 + 1 + 1) / 4
    assert report.overall.type_accuracy == 3 / 4
    assert report.overall.canonical_link_accuracy == 2 / 3
    assert report.overall.canonical_link_coverage == 3 / 4
    assert report.overall.duplicate_rate == 1 / 5
    assert report.overall.candidate_fate_coverage == 2 / 5
    assert report.overall.area_under_risk_coverage is not None
    assert report.by_source["jira"].canonical_link_accuracy == 0
    assert report.by_slack_context["temporally_distributed"].canonical_link_coverage == .5


def test_empty_denominators_are_unknown_not_perfect() -> None:
    signal = GoldSignal(signal_id="s", batch_id="b", source_type="slack", text="hello", slack_context="standalone")
    report = evaluate_gold_entity_extraction(signals=(signal,), gold_mentions=(), predictions=())
    assert report.overall.span_precision is None
    assert report.overall.span_recall is None
    assert report.overall.candidate_fate_coverage is None
    assert report.uncertainties == ("no_gold_mentions_in_scope",)


def test_rejects_annotations_outside_persisted_signal_boundary() -> None:
    signal = GoldSignal(signal_id="s", batch_id="b", source_type="email", text="Acme")
    gold = GoldMention(mention_id="g", signal_id="missing", start=0, end=4, entity_type="customer")
    try:
        evaluate_gold_entity_extraction(signals=(signal,), gold_mentions=(gold,), predictions=())
    except ValueError as exc:
        assert "unknown signal" in str(exc)
    else:
        raise AssertionError("expected unknown signal rejection")


def test_cli_writes_versioned_continuous_report(tmp_path, monkeypatch) -> None:
    source = tmp_path / "corpus.json"
    output = tmp_path / "report.json"
    source.write_text(
        json.dumps(
            {
                "signals": [
                    {
                        "signal_id": "s",
                        "batch_id": "batch",
                        "source_type": "jira",
                        "text": "Acme",
                    }
                ],
                "gold_mentions": [
                    {
                        "mention_id": "g",
                        "signal_id": "s",
                        "start": 0,
                        "end": 4,
                        "entity_type": "customer",
                        "canonical_referent": "customer:acme",
                    }
                ],
                "predictions": [],
            }
        )
    )
    monkeypatch.setattr(
        "sys.argv", ["evaluate", "--input", str(source), "--output", str(output)]
    )
    assert main() == 0
    written = json.loads(output.read_text())
    assert written["schema_version"] == "gold-entity-extraction-v1"
    assert written["overall"]["span_recall"] == 0
