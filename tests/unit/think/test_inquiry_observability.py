from services.execution.inquiry import (
    _compact_inquiry_notes_for_persistence,
    _runtime_residual_summary,
)


def test_runtime_residual_summary_splits_actions_stages_and_unknown() -> None:
    summary = _runtime_residual_summary(
        total_ms=1000,
        action_timings=[
            {"path": "sage_reader", "elapsed_ms": 300},
            {"path": "semantic", "elapsed_ms": "25"},
        ],
        stage_timings=[
            {"stage": "primary_retrieve", "elapsed_ms": 400},
            {"stage": "final_result_merge", "elapsed_ms": 50},
        ],
    )

    assert summary == {
        "total_ms": 1000,
        "retrieval_action_timings_ms_total": 325,
        "retrieval_stage_timings_ms_total": 450,
        "measured_ms_total": 775,
        "unaccounted_ms": 225,
    }


def test_persisted_inquiry_notes_compact_sage_payloads_and_context_packet() -> None:
    notes = {
        "context_packet": {"large": ["packet"]},
        "sage_reader": {
            "activation_trace_count": 2,
            "questions": {
                "Q_OWNER": {
                    "question_id": "Q_OWNER",
                    "question_primitive": "OWNERSHIP",
                    "signature": {"entities": ["acme"]},
                    "selected_model_ids": ["m1"],
                    "projected_evidence_count": 1,
                    "activations": [
                        {"model_id": "m1", "selected": True},
                        {"model_id": "m2", "selected": False},
                    ],
                    "debug": {
                        "stage_timings_ms": {"reader_total_ms": 123},
                        "learned_read_plan": {"mode": "default"},
                        "candidate_pool": {
                            "before_edge_seed_count": 100,
                            "edge_seed_count": 40,
                            "edge_seed_pruned_count": 60,
                        },
                        "row_cache": {"model_hits": 4, "model_misses": 2},
                        "gate_scores": {"e1": {"score": 0.8}, "e2": {"score": 0.7}},
                        "activation_reasons": {"m1": ["exact"], "m2": ["lexical"]},
                        "selector": {
                            "selected_nodes": ["m1", "m2"],
                            "selected_edges": ["e1"],
                            "bridge_nodes": ["m3"],
                            "coverage_metrics": {"role_coverage": 1.0},
                        },
                        "intents": [
                            {
                                "intent": "find_owner",
                                "paths": ["exact"],
                                "target": "x" * 300,
                                "expected_value": 0.8,
                                "expected_cost": 0.2,
                            }
                        ],
                    },
                }
            },
        },
    }

    compact = _compact_inquiry_notes_for_persistence(
        notes,
        persist_full_sage_reader_notes=False,
    )

    assert compact["context_packet"] == {"stored_in_context_packet_column": True}
    assert compact["persist_compaction"]["sage_reader_full_notes"] is False
    question = compact["sage_reader"]["questions"]["Q_OWNER"]
    assert "activations" not in question
    assert question["activation_trace_count"] == 2
    assert question["activations_stored_in"] == "sage_reader_activations"
    assert question["debug"]["gate_score_count"] == 2
    assert question["debug"]["activation_reason_count"] == 2
    assert question["debug"]["selector"]["selected_node_count"] == 2
    assert question["debug"]["selector"]["selected_edge_count"] == 1
    assert question["debug"]["stage_timings_ms"]["reader_total_ms"] == 123
    assert question["debug"]["candidate_pool"]["edge_seed_pruned_count"] == 60
    assert question["debug"]["row_cache"] == {"model_hits": 4, "model_misses": 2}
    assert len(question["debug"]["intents"][0]["target"]) <= 180
    assert notes["sage_reader"]["questions"]["Q_OWNER"]["activations"]


def test_persisted_inquiry_notes_can_keep_full_sage_payloads() -> None:
    notes = {"sage_reader": {"questions": {"Q": {"activations": [{"id": "m"}]}}}}

    assert (
        _compact_inquiry_notes_for_persistence(
            notes,
            persist_full_sage_reader_notes=True,
        )
        is notes
    )
