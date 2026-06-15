from __future__ import annotations

from uuid import uuid4

from services.platform.execution import inquiry, sage_reader_notes
from services.platform.execution.types import InquiryQuestion
from services.reasoning.retrieval.pathways import PathwayResult
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext


def _trigger(**overrides: object) -> TriggerContext:
    values = {
        "kind": "T1",
        "tenant_id": uuid4(),
    }
    values.update(overrides)
    return TriggerContext(**values)


def _result_with_plan(
    plan: dict[str, object],
    *,
    models: list[object] | None = None,
    projected_evidence_count: int = 0,
    reader_total_ms: object = "123",
) -> RetrievalResult:
    return RetrievalResult(
        trigger=_trigger(),
        models=list(models or []),
        notes={
            "sage_reader": {
                "debug": {
                    "learned_read_plan": plan,
                    "stage_timings_ms": {"reader_total_ms": reader_total_ms},
                },
                "projected_evidence_count": projected_evidence_count,
            }
        },
    )


def test_sage_reader_helpers_keep_legacy_inquiry_identity() -> None:
    assert inquiry._action_cache_summary is sage_reader_notes.action_cache_summary
    assert (
        inquiry._compact_inquiry_notes_for_persistence
        is sage_reader_notes.compact_inquiry_notes_for_persistence
    )
    assert (
        inquiry._compact_sage_question_note_for_persistence
        is sage_reader_notes.compact_sage_question_note_for_persistence
    )
    assert (
        inquiry._compact_sage_reader_debug_for_persistence
        is sage_reader_notes.compact_sage_reader_debug_for_persistence
    )
    assert (
        inquiry._compact_sage_reader_notes_for_persistence
        is sage_reader_notes.compact_sage_reader_notes_for_persistence
    )
    assert (
        inquiry._record_sage_reader_notes is sage_reader_notes.record_sage_reader_notes
    )
    assert (
        inquiry._sage_only_retrieval_results
        is sage_reader_notes.sage_only_retrieval_results
    )
    assert inquiry._sage_reader_action_gate is sage_reader_notes.sage_reader_action_gate
    assert (
        inquiry._sage_reader_controller_summary
        is sage_reader_notes.sage_reader_controller_summary
    )
    assert (
        inquiry._sage_reader_plan_from_read_note
        is sage_reader_notes.sage_reader_plan_from_read_note
    )
    assert (
        inquiry._sage_reader_plan_from_result
        is sage_reader_notes.sage_reader_plan_from_result
    )
    assert (
        inquiry._sage_reader_plan_hard_abstained
        is sage_reader_notes.sage_reader_plan_hard_abstained
    )
    assert inquiry._sage_reader_total_ms is sage_reader_notes.sage_reader_total_ms
    assert (
        inquiry._trigger_has_explicit_model_anchor
        is sage_reader_notes.trigger_has_explicit_model_anchor
    )


def test_record_sage_reader_notes_collects_question_totals_and_dedupes() -> None:
    notes = {
        "signatures": [],
        "selected_model_ids": ["m0"],
        "projected_evidence_count": 2,
        "activation_trace_count": 3,
    }
    question = InquiryQuestion(
        question_id="Q_OWNER",
        question="Who owns the launch risk?",
        primitive="OWNERSHIP",
        tests_hypotheses=("H1",),
        expected_value=0.8,
        expected_cost=0.2,
        retrieval_target="owner evidence",
        stop_condition="owner found",
        score=0.7,
    )
    read_note = {
        "question_id": "Q_OWNER",
        "signature": {"entities": ["acme"]},
        "selected_model_ids": ["m0", "m1"],
        "projected_evidence_count": 4,
        "activation_trace_count": 5,
    }
    result = RetrievalResult(trigger=_trigger(), notes={"sage_reader": read_note})

    sage_reader_notes.record_sage_reader_notes(notes, question, result)
    sage_reader_notes.record_sage_reader_notes(notes, question, result)

    assert notes["questions"]["Q_OWNER"] is read_note
    assert notes["signatures"] == [{"entities": ["acme"]}]
    assert notes["selected_model_ids"] == ["m0", "m1"]
    assert notes["projected_evidence_count"] == 10
    assert notes["activation_trace_count"] == 13


def test_compact_inquiry_notes_for_persistence_moves_large_sage_payloads() -> None:
    notes = {
        "context_packet": {"tiers": {"decisive_evidence": [{"id": "e1"}]}},
        "sage_reader": {
            "questions": {
                "Q_OWNER": {
                    "question_id": "Q_OWNER",
                    "question_primitive": "OWNERSHIP",
                    "signature": {"entities": ["acme"]},
                    "selected_model_ids": ["m1"],
                    "projected_evidence_count": 1,
                    "activations": [{"model_id": "m1"}, {"model_id": "m2"}],
                    "debug": {
                        "stage_timings_ms": {"reader_total_ms": 123},
                        "intents": [{"intent": "find_owner", "target": "x" * 300}],
                        "selector": {
                            "selected_nodes": ["n1"],
                            "selected_edges": ["e1", "e2"],
                            "bridge_nodes": ["b1"],
                        },
                        "gate_scores": {"m1": 0.9},
                        "activation_reasons": ["strong_anchor", "ownership"],
                    },
                }
            }
        },
    }

    compact = sage_reader_notes.compact_inquiry_notes_for_persistence(
        notes,
        persist_full_sage_reader_notes=False,
    )

    assert compact is not notes
    assert compact["context_packet"] == {"stored_in_context_packet_column": True}
    assert compact["persist_compaction"] == {
        "sage_reader_full_notes": False,
        "context_packet_stored_once": True,
    }
    question = compact["sage_reader"]["questions"]["Q_OWNER"]
    assert "activations" not in question
    assert question["activation_trace_count"] == 2
    assert question["activations_stored_in"] == "sage_reader_activations"
    assert question["debug"]["stage_timings_ms"] == {"reader_total_ms": 123}
    assert question["debug"]["selector"] == {
        "selected_node_count": 1,
        "selected_edge_count": 2,
        "bridge_node_count": 1,
        "coverage_metrics": {},
    }
    assert question["debug"]["gate_score_count"] == 1
    assert question["debug"]["activation_reason_count"] == 2
    assert len(question["debug"]["intents"][0]["target"]) <= 180
    assert notes["sage_reader"]["questions"]["Q_OWNER"]["activations"]


def test_compact_inquiry_notes_can_keep_full_sage_payloads() -> None:
    notes = {"sage_reader": {"questions": {"Q": {"activations": [{"id": "m"}]}}}}

    assert (
        sage_reader_notes.compact_inquiry_notes_for_persistence(
            notes,
            persist_full_sage_reader_notes=True,
        )
        is notes
    )


def test_sage_reader_plan_and_total_ms_are_defensive() -> None:
    result = _result_with_plan({"mode": "focused"}, reader_total_ms="45")

    assert sage_reader_notes.sage_reader_total_ms(result) == 45
    assert sage_reader_notes.sage_reader_plan_from_result(result) == {"mode": "focused"}
    assert (
        sage_reader_notes.sage_reader_total_ms(
            _result_with_plan({}, reader_total_ms="not-an-int")
        )
        is None
    )
    assert (
        sage_reader_notes.sage_reader_plan_from_result(
            RetrievalResult(trigger=_trigger(), notes={"sage_reader": "bad"})
        )
        == {}
    )
    assert (
        sage_reader_notes.sage_reader_plan_from_read_note(
            {"debug": {"learned_read_plan": "bad"}}
        )
        == {}
    )


def test_sage_reader_action_gate_modes() -> None:
    assert sage_reader_notes.sage_reader_action_gate(
        _result_with_plan({"mode": "focused"}),
        gate_broad_actions=False,
    ) == (None, None)
    assert sage_reader_notes.sage_reader_action_gate(
        _result_with_plan({"mode": "abstain", "abstain_early": True})
    ) == ("all", "sage_reader_negative_memory_abstain")
    assert sage_reader_notes.sage_reader_action_gate(
        _result_with_plan(
            {
                "mode": "focused",
                "gate_broad_actions": True,
                "skip_broad_discovery": True,
            }
        )
    ) == ("broad", "sage_reader_focused_broad_gate")
    assert sage_reader_notes.sage_reader_action_gate(
        _result_with_plan(
            {"mode": "rerank", "gate_broad_actions": True},
            models=[object()],
            projected_evidence_count=1,
        )
    ) == ("broad", "sage_reader_rerank_sufficient_evidence")


def test_sage_reader_controller_summary_counts_questions_and_global_gate() -> None:
    no_anchor = _trigger()
    notes = {
        "questions": {
            "q1": {
                "debug": {
                    "learned_read_plan": {
                        "mode": "abstain",
                        "abstain_early": True,
                    }
                },
                "selected_model_ids": [],
            },
            "bad": "ignored",
        }
    }

    summary = sage_reader_notes.sage_reader_controller_summary(
        notes,
        trigger=no_anchor,
    )

    assert summary["used"] is True
    assert summary["question_count"] == 1
    assert summary["hard_abstain_count"] == 1
    assert summary["selected_model_count"] == 0
    assert summary["explicit_model_anchor"] is False
    assert summary["global_negative_route_gate"] is True
    assert summary["questions"]["q1"]["hard_abstained"] is True

    anchored = sage_reader_notes.sage_reader_controller_summary(
        notes,
        trigger=_trigger(model_id=uuid4()),
    )
    assert anchored["explicit_model_anchor"] is True
    assert anchored["global_negative_route_gate"] is False


def test_sage_reader_controller_summary_keeps_selected_and_skipped_counts() -> None:
    notes = {
        "questions": {
            "q1": {
                "debug": {
                    "learned_read_plan": {
                        "mode": "focused",
                        "confidence": 0.7,
                        "skip_broad_discovery": True,
                        "gate_broad_actions": True,
                    }
                },
                "selected_model_ids": ["m1", "m2"],
            }
        }
    }

    summary = sage_reader_notes.sage_reader_controller_summary(
        notes,
        trigger=_trigger(member_model_ids=[uuid4()]),
    )

    assert summary["skipped_broad_count"] == 1
    assert summary["selected_model_count"] == 2
    assert summary["explicit_model_anchor"] is True
    assert summary["global_negative_route_gate"] is False
    assert summary["questions"]["q1"] == {
        "mode": "focused",
        "confidence": 0.7,
        "abstain_early": False,
        "skip_broad_discovery": True,
        "gate_broad_actions": True,
        "selected_model_count": 2,
        "hard_abstained": False,
    }


def test_sage_only_retrieval_results_preserves_reader_sourced_results() -> None:
    via_notes = RetrievalResult(
        trigger=_trigger(),
        notes={"pathways_run": ["sage_reader"]},
    )
    via_pathway = RetrievalResult(
        trigger=_trigger(),
        pathway_results=[PathwayResult(source_pathway="SAGE")],
    )
    unrelated = RetrievalResult(
        trigger=_trigger(),
        pathway_results=[PathwayResult(source_pathway="A")],
    )

    assert sage_reader_notes.sage_only_retrieval_results(
        [unrelated, via_notes, via_pathway]
    ) == [via_notes, via_pathway]


def test_action_cache_summary_counts_hits_misses_and_path_timings() -> None:
    summary = sage_reader_notes.action_cache_summary(
        [
            {"path": "semantic", "elapsed_ms": 12, "cache_hit": True},
            {"path": "semantic", "elapsed_ms": 8, "cache_hit": False},
            {"path": "sage_reader", "elapsed_ms": 5, "cache_hit": False},
            {"path": "temporal", "elapsed_ms": "3", "cache_hit": True},
        ]
    )

    assert summary == {
        "hits": 2,
        "misses": 1,
        "elapsed_ms_by_path": {
            "sage_reader": 5,
            "semantic": 20,
            "temporal": 3,
        },
        "cache_hits_by_path": {
            "semantic": 1,
            "temporal": 1,
        },
    }
