import argparse
import asyncio
import json

from scripts import run_storyline_batch_benchmark as benchmark
from scripts.run_storyline_batch_benchmark import (
    _LATENT_BRIDGE_STORYLINE_ID,
    _PRODUCT_VALUE_EVAL_KEYS,
    STORYLINES,
    StorylineScore,
    _accumulate_edge_ops_stats,
    _benchmark_summary,
    _company_intelligence_scorecard,
    _empty_edge_ops_stats,
    _future_wave_trigger_ids,
    _latent_pattern_assessment,
    _noise_noop_score,
    _retrieval_context_budget_fit_score,
    _render_benchmark_markdown,
    _render_variance_markdown,
    _story_id_from_external_id,
    _storyline_calibration_report,
    build_variance_report,
    build_storyline_scenario,
)


class _StopAfterMigration(Exception):
    pass


class _FakeAcquire:
    async def __aenter__(self):
        return object()

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakePool:
    def acquire(self):
        return _FakeAcquire()


def test_prepare_benchmark_tenant_uses_warn_migration_policy(monkeypatch) -> None:
    calls: list[dict] = []

    async def _fake_apply_migrations_dir(conn, migrations_dir, *, on_error="stop"):
        calls.append({
            "conn": conn,
            "migrations_dir": migrations_dir,
            "on_error": on_error,
        })
        raise _StopAfterMigration

    monkeypatch.setattr(
        benchmark,
        "apply_migrations_dir",
        _fake_apply_migrations_dir,
    )
    args = argparse.Namespace(skip_migrations=False)

    try:
        asyncio.run(
            benchmark._prepare_storyline_benchmark_tenant(
                args,
                pool=_FakePool(),
                scenario=None,
                append_context=None,
                run_id="unit-migration-policy",
                horizon_start_batch=0,
            )
        )
    except _StopAfterMigration:
        pass
    else:  # pragma: no cover - this helper must stop before tenant seeding.
        raise AssertionError("expected migration sentinel")

    assert calls
    assert calls[0]["on_error"] == "warn"


def test_storyline_scenario_builds_expected_batch_waves() -> None:
    scenario, gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=5,
        future_validation_signals_per_storyline=3,
    )

    assert len(gold) == len(STORYLINES)
    assert len(scenario.signal_sequences) == len(STORYLINES) + 2
    assert sum(len(v) for v in scenario.signal_sequences.values()) == (
        len(STORYLINES) * 20 + len(STORYLINES) * 3 + 5
    )
    assert len(scenario.signal_sequences["future_validation"]) == (len(STORYLINES) * 3)

    for story in STORYLINES:
        wave = scenario.signal_sequences[f"{story.id}_wave"]
        assert len(wave) == 20
        assert {
            _story_id_from_external_id(signal.get("external_id")) for signal in wave
        } == {story.id}
        assert all(
            "storyline_id" not in (signal.get("content_dict") or {}) for signal in wave
        )
        assert all(
            "storyline_title" not in (signal.get("content_dict") or {})
            for signal in wave
        )


def test_storyline_scenario_builds_long_horizon_400_t1_batches() -> None:
    scenario, gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon",
        signals_per_storyline=25,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=400,
    )

    assert len(gold) == len(STORYLINES)
    assert len(scenario.signal_sequences) == 400
    assert sum(len(v) for v in scenario.signal_sequences.values()) == 10000
    assert {len(v) for v in scenario.signal_sequences.values()} == {25}
    assert any(
        name.startswith("future_validation_wave_") for name in scenario.signal_sequences
    )
    assert any(
        name.startswith("capability_probe_wave_")
        for name in scenario.signal_sequences
    )
    assert any(
        name.startswith("background_noise_wave_") for name in scenario.signal_sequences
    )
    assert (scenario.raw or {})["scenario_mode"] == "long_horizon"
    assert (scenario.raw or {})["target_t1_batches"] == 400


def test_storyline_scenario_builds_long_horizon_10_t1_batches_with_validation() -> None:
    scenario, gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon-10",
        signals_per_storyline=5,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=10,
    )

    assert len(gold) == len(STORYLINES)
    assert len(scenario.signal_sequences) == 10
    assert sum(len(v) for v in scenario.signal_sequences.values()) == 50
    assert any(
        name.startswith("future_validation_wave_") for name in scenario.signal_sequences
    )
    probe_waves = {
        name: signals
        for name, signals in scenario.signal_sequences.items()
        if name.startswith("capability_probe_wave_")
    }
    assert probe_waves
    probe_kinds = {
        kind
        for signals in probe_waves.values()
        for signal in signals
        for kind in signal["content_dict"]["capability_probe_kinds"]
    }
    assert probe_kinds == {
        "prediction",
        "resource",
        "ontology_gap",
        "archive",
        "evidence_attachment",
        "question_policy",
    }
    assert (scenario.raw or {})["scenario_mode"] == "long_horizon"
    assert (scenario.raw or {})["warmup_batches"] < 10


def test_storyline_scenario_builds_append_horizon_without_reused_signal_ids() -> None:
    base, _gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon",
        signals_per_storyline=25,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=400,
    )
    append, _gold = build_storyline_scenario(
        run_id="unit-storyline-long-horizon-plus-200",
        foundation_namespace="unit-storyline-long-horizon",
        signals_per_storyline=25,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
        target_t1_batches=200,
        horizon_start_batch=400,
    )

    assert len(append.signal_sequences) == 200
    assert sum(len(v) for v in append.signal_sequences.values()) == 5000
    assert next(iter(append.signal_sequences)).endswith("_wave_401")
    assert (append.raw or {})["horizon_start_batch"] == 400
    assert (append.raw or {})["horizon_end_batch"] == 600
    assert (append.raw or {})["foundation_namespace"] == ("unit-storyline-long-horizon")

    base_external_ids = {
        signal["external_id"]
        for signals in base.signal_sequences.values()
        for signal in signals
    }
    append_external_ids = {
        signal["external_id"]
        for signals in append.signal_sequences.values()
        for signal in signals
    }
    assert not (base_external_ids & append_external_ids)

    first_signal = next(iter(append.signal_sequences.values()))[0]
    assert first_signal["content_dict"]["signal_index"] == 10000
    assert first_signal["content_dict"]["horizon_wave_index"] == 401


def test_latent_bridge_storyline_has_sensor_gap_without_initial_hallway_leak() -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=0,
        future_validation_signals_per_storyline=3,
    )

    bridge_wave = scenario.signal_sequences[f"{_LATENT_BRIDGE_STORYLINE_ID}_wave"]
    bridge_text = "\n".join(signal["content"].lower() for signal in bridge_wave)
    future_bridge_text = "\n".join(
        signal["content"].lower()
        for signal in scenario.signal_sequences["future_validation"]
        if _story_id_from_external_id(signal.get("external_id"))
        == _LATENT_BRIDGE_STORYLINE_ID
    )

    assert "sensor trail has a gap" in bridge_text
    assert "before and after states" in bridge_text
    assert "hallway" not in bridge_text
    assert "hallway" in future_bridge_text


def test_future_wave_trigger_ids_mark_future_edge_evolution() -> None:
    waves = [
        {
            "sequence": "atlas_wave_001",
            "t1_batch": {"trigger_id": "ordinary-trigger"},
        },
        {
            "sequence": "future_validation_wave_005",
            "t1_batch": {"trigger_id": "future-trigger"},
            "downstream": [{"trigger_id": "future-downstream"}],
        },
    ]
    future_trigger_ids = _future_wave_trigger_ids(waves)
    assert future_trigger_ids == {"future-trigger", "future-downstream"}

    stats = _empty_edge_ops_stats()
    _accumulate_edge_ops_stats(
        stats,
        {
            "edge_ops": [
                {
                    "op": "add",
                    "edge_kind": "weakens",
                    "review_status": "accepted",
                }
            ],
            "relation_frame_ops": [
                {
                    "status": "accepted",
                    "write_policy": "project_edges",
                    "relation_kind": "blocked_workstream",
                    "projected_edge_count": 3,
                }
            ],
        },
        is_future="future-trigger" in future_trigger_ids,
    )

    assert stats["future_edge_ops"] == 1
    assert stats["future_relation_frame_ops"] == 1


def test_storyline_signal_metadata_does_not_persist_gold_answers() -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=0,
    )

    forbidden_metadata_keys = {
        "expected_term",
        "expected_action",
        "expected_relationship",
        "storyline_id",
        "storyline_title",
    }
    forbidden_text = (
        "Important term:",
        "Likely operating implication:",
        "Potential relationship shape:",
    )
    for signals in scenario.signal_sequences.values():
        for signal in signals:
            content = signal.get("content_dict") or {}
            assert content["benchmark"] == "storyline_batch"
            assert not (forbidden_metadata_keys & set(content))
        assert all(marker not in signal["content"] for marker in forbidden_text)


def test_story_id_from_external_id_accepts_run_prefixed_ids() -> None:
    assert _story_id_from_external_id("storyline:atlas:001") == "atlas"
    assert _story_id_from_external_id("capability-400:storyline:atlas:001") == "atlas"
    assert (
        _story_id_from_external_id(
            "capability-400-plus-200:storyline:northstar_gap:future:004"
        )
        == "northstar_gap"
    )


def test_storyline_signals_do_not_leak_hidden_thesis() -> None:
    scenario, _gold = build_storyline_scenario(
        run_id="unit-storyline-benchmark",
        signals_per_storyline=20,
        noise_signals=0,
    )

    thesis_by_story = {story.id: story.thesis for story in STORYLINES}
    for signals in scenario.signal_sequences.values():
        for signal in signals:
            story_id = _story_id_from_external_id(signal.get("external_id"))
            thesis = thesis_by_story.get(story_id)
            if thesis:
                assert thesis not in signal["content"]


def test_latent_pattern_assessment_scores_concrete_model_coverage() -> None:
    story = STORYLINES[0]
    model = {
        "natural": (
            "Atlas renewal risk is driven by missing security evidence, "
            "usage drop, and procurement waiting on approval."
        ),
        "proposition": {
            "claim_role": "situation",
            "summary": "Security evidence, usage decay, and procurement wait combine.",
        },
    }

    assessment = _latent_pattern_assessment(model, story)

    assert assessment["coverage"] == 1.0
    assert assessment["missing"] == []


def test_noise_noop_score_credits_explicit_empty_diff_without_noop_grade() -> None:
    assert (
        _noise_noop_score(
            [
                {
                    "sequence": "background_noise_wave_010",
                    "t1_batch": {
                        "run": {
                            "validation_error_count": 0,
                            "ops_applied": {
                                "claim_ops": [],
                                "relation_claim_ops": [],
                                "relation_frame_ops": [],
                                "edge_ops": [],
                                "act_ops": [],
                                "resource_ops": [],
                                "ontology_gap_ops": [],
                                "state_changes_emitted": 0,
                                "context_use": {
                                    "context_use_grade": "unused_selected_context"
                                },
                                "reasoning_trace": (
                                    "Empty diff; "
                                    "relation_lifecycle_kernel="
                                    "packet_obligations_skipped:explicit_noop"
                                ),
                                "synthesis_decisions": [
                                    {
                                        "bucket": "diff",
                                        "decision": "discard_as_noise",
                                    }
                                ],
                            },
                        }
                    },
                }
            ]
        )
        == 1.0
    )


def test_noise_noop_score_rejects_zero_state_change_relation_writes() -> None:
    assert (
        _noise_noop_score(
            [
                {
                    "sequence": "background_noise_wave_010",
                    "t1_batch": {
                        "run": {
                            "validation_error_count": 0,
                            "ops_applied": {
                                "claim_ops": [],
                                "relation_claim_ops": [
                                    {"edge_kind": "weakens", "op": "upsert"}
                                ],
                                "relation_frame_ops": [],
                                "edge_ops": [],
                                "act_ops": [],
                                "resource_ops": [],
                                "ontology_gap_ops": [],
                                "state_changes_emitted": 0,
                                "reasoning_trace": "No durable diff emitted.",
                            },
                        }
                    },
                }
            ]
        )
        == 0.0
    )


def test_retrieval_context_budget_fit_score_rewards_efficient_band() -> None:
    assert _retrieval_context_budget_fit_score(0) == 0.0
    assert _retrieval_context_budget_fit_score(3) == 0.5
    assert _retrieval_context_budget_fit_score(8) == 1.0
    assert _retrieval_context_budget_fit_score(16) == 1.0
    assert _retrieval_context_budget_fit_score(22) == 0.5
    assert _retrieval_context_budget_fit_score(28) == 0.0


def test_company_intelligence_scorecard_penalizes_over_budget_context() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[24],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    retrieval = scorecard["dimensions"]["retrieval_usefulness"]

    assert retrieval["metrics"]["retrieval_budget_fit_score"] < 0.5
    assert scorecard["proof_coverage"]["retrieval_budget_fit_score"] < 0.5
    assert any(
        "Selected Model context is above the efficient batch budget" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_dimensions_and_gaps() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    assert 0.0 <= scorecard["overall_score"] <= 1.0
    assert {
        "memory_truth",
        "compression",
        "retrieval_usefulness",
        "reasoning_value",
        "temporal_improvement",
        "edge_intelligence",
        "adaptive_lifecycle",
        "robustness",
        "efficiency",
    } == set(scorecard["dimensions"])
    assert any("No future validation events" in gap for gap in scorecard["proof_gaps"])
    assert any(
        "Resource/action-resource operations are untested" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_distinguishes_probed_from_untested() -> None:
    model_summary = _sample_model_summary()
    model_summary["capability_probe_counts"] = {
        "prediction": 1,
        "resource": 1,
        "ontology_gap": 1,
        "archive": 1,
        "evidence_attachment": 1,
        "question_policy": 1,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    gaps = "\n".join(scorecard["proof_gaps"])
    assert "Resource/action-resource probe ran" in gaps
    assert "Ontology-gap probe ran" in gaps
    assert "Archive probe ran" in gaps
    assert "Evidence probe ran" in gaps
    assert "Question-policy probe ran" in gaps
    assert "Resource/action-resource operations are untested" not in gaps
    assert "Ontology-gap write path is untested" not in gaps


def test_company_intelligence_scorecard_flags_topology_missing_model_skips() -> None:
    model_summary = _sample_model_summary()
    model_summary["topology_optimizer_metric_totals"] = {
        **model_summary["topology_optimizer_metric_totals"],
        "shortcut_missing_model_skips": 2,
        "structural_missing_model_skips": 1,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    assert (
        scorecard["dimensions"]["robustness"]["metrics"]["topology_missing_model_skips"]
        == 3.0
    )
    assert any(
        "Topology optimizer skipped missing model references" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_scores_future_validation_evidence() -> None:
    model_summary = _sample_model_summary()
    model_summary["future_validation_events"] = 24

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    temporal = scorecard["dimensions"]["temporal_improvement"]
    retrieval = scorecard["dimensions"]["retrieval_usefulness"]

    assert temporal["metrics"]["future_validation_events"] == 24
    assert temporal["metrics"]["future_validation_success_rate"] == 1.0
    assert (
        temporal["metrics"]["future_validation_model_or_graph_context_use_score"] == 1.0
    )
    assert temporal["score"] > 0.55
    assert retrieval["metrics"]["avg_historical_observations_per_t1_batch"] == 3.5
    assert not any(
        "No future validation events" in gap for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_adaptive_lifecycle() -> None:
    model_summary = _sample_model_summary()
    model_summary["future_validation_events"] = 24
    model_summary["discovery_layer_counts"] = {
        "negative_memory": 2,
        "question_policy_stats": 1,
    }
    model_summary["topology_optimizer_metric_totals"] = {
        **model_summary["topology_optimizer_metric_totals"],
        "shortcut_creates_or_bumps": 12,
        "affordance_reinforces": 10,
        "negative_memory_inserts": 1,
        "question_policy_updates": 1,
    }
    model_summary["post_commit_status"] = {
        "processed": 5,
        "failed": 0,
        "dead_lettered": 0,
        "iterations": 2,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    adaptive = scorecard["dimensions"]["adaptive_lifecycle"]

    assert adaptive["score"] > 0.65
    assert adaptive["metrics"]["policy_feedback_score"] == 1.0
    assert adaptive["metrics"]["negative_learning_score"] == 1.0
    assert adaptive["metrics"]["temporal_closure_score"] > 0.0
    assert (
        scorecard["proof_coverage"]["adaptive_lifecycle"]
        == adaptive["metrics"]
    )
    assert not any(
        "Adaptive temporal closure is weak" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_edge_intelligence() -> None:
    model_summary = _sample_model_summary()
    model_summary["edge_kind_distribution"] = {
        "supports": 2,
        "early_warning_for": 1,
        "blocks": 1,
        "weakens": 1,
        "explains": 1,
        "contributes_to_resolution": 1,
    }
    model_summary["relationship_candidates"] = 2
    model_summary["relationship_candidate_status_distribution"] = {"accepted": 2}
    model_summary["edge_lifecycle"] = {
        "total_edges": 7,
        "accepted_edges": 7,
        "accepted_edge_kind_distribution": {
            "supports": 2,
            "early_warning_for": 1,
            "blocks": 1,
            "weakens": 1,
            "explains": 1,
            "contributes_to_resolution": 1,
        },
        "reconfirmed_edges": 1,
        "reconfirmation_events": 2,
        "retired_or_inert_edges": 1,
        "ontology_proposals": 0,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    edge = scorecard["dimensions"]["edge_intelligence"]

    assert edge["metrics"]["required_registered_edge_kind_coverage"] == 1.0
    assert edge["metrics"]["precise_required_edge_kind_coverage"] == 1.0
    assert edge["metrics"]["future_validation_edge_ops"] == 1
    assert edge["metrics"]["reconfirmation_events"] == 2.0
    assert edge["metrics"]["graph_relation_contract_score"] == 1.0
    assert "missing_registered_edge_kinds" in scorecard["proof_coverage"]


def test_company_intelligence_scorecard_counts_relation_frames_as_structure() -> None:
    model_summary = _sample_model_summary()
    model_summary["edge_kind_distribution"] = {"supports": 1}
    model_summary["edge_lifecycle"] = {
        **model_summary["edge_lifecycle"],
        "accepted_edge_kind_distribution": {"supports": 1},
    }
    model_summary["relation_frame_lifecycle"] = {
        "available": True,
        "total_relation_frames": 1,
        "accepted_relation_frames": 1,
        "projectable_relation_frames": 1,
        "bound_relation_frames": 1,
        "relation_participants": 5,
        "relation_edge_projections": 3,
        "relation_frame_kind_distribution": {"blocked_workstream": 1},
        "relation_projection_kind_distribution": {
            "blocks": 1,
            "early_warning_for": 1,
            "contributes_to_resolution": 1,
        },
    }
    story = _sample_storyline_score()
    story.scoped_edge_count = 0
    story.relation_frame_count = 1
    story.accepted_relation_frame_count = 1
    story.relation_frame_projection_count = 3
    story.relation_frame_kind_hits = ["blocked_workstream"]

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[story],
        waves=[_sample_success_wave_with_relation_frame()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    edge = scorecard["dimensions"]["edge_intelligence"]
    proof = scorecard["proof_coverage"]

    assert edge["metrics"]["relation_frame_score"] > 0
    assert edge["metrics"]["storyline_edge_presence"] == 1.0
    assert edge["metrics"]["relation_frame_ops"] == 1
    assert edge["metrics"]["relation_frame_projected_edges_from_ops"] == 3
    assert proof["accepted_relation_frames"] == 1.0
    assert "blocks" in proof["structural_edge_kinds_observed"]
    assert not any(
        "N-ary relation frames were not exercised" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_flags_graph_relation_contract_failure() -> None:
    model_summary = _sample_model_summary()
    model_summary["context_use_relation_contract"] = {
        "context_use_runs": 4,
        "graph_selected_runs": 4,
        "graph_relation_op_runs": 1,
        "graph_no_edge_rationale_runs": 1,
        "graph_selected_without_relation_ops_runs": 3,
        "graph_relation_contract_satisfied_runs": 2,
        "graph_relation_contract_failed_runs": 2,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    retrieval = scorecard["dimensions"]["retrieval_usefulness"]
    edge = scorecard["dimensions"]["edge_intelligence"]

    assert retrieval["metrics"]["graph_relation_contract_score"] == 0.5
    assert edge["metrics"]["graph_relation_contract_failed_runs"] == 2
    assert any(
        "Graph-selected context failed the relationship contract" in gap
        for gap in scorecard["proof_gaps"]
    )


def test_company_intelligence_scorecard_reports_product_value_evals() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    product_value = scorecard["product_value_evals"]

    assert 0.0 <= product_value["overall_score"] <= 1.0
    assert set(product_value["evals"]) == set(_PRODUCT_VALUE_EVAL_KEYS)
    assert scorecard["proof_coverage"]["product_value_eval_keys"] == list(
        _PRODUCT_VALUE_EVAL_KEYS
    )
    assert (
        product_value["evals"]["negative_learning"]["metrics"][
            "negative_learning_events"
        ]
        == 0
    )
    assert any("Negative learning eval" in gap for gap in product_value["proof_gaps"])
    assert any("Question policy eval" in gap for gap in product_value["proof_gaps"])
    assert any("Customer value eval" in gap for gap in product_value["proof_gaps"])
    assert any(
        "Latent bridge inference eval" in gap for gap in product_value["proof_gaps"]
    )


def test_product_value_gaps_distinguish_probed_from_untested() -> None:
    model_summary = _sample_model_summary()
    model_summary["capability_probe_counts"] = {
        "resource": 1,
        "archive": 1,
        "evidence_attachment": 1,
        "question_policy": 1,
    }

    scorecard = _company_intelligence_scorecard(
        model_summary=model_summary,
        storyline_scores=[_sample_storyline_score()],
        waves=[_sample_success_wave()],
        retrieval_model_counts=[22],
        retrieval_observation_counts=[29],
        validation_errors=0,
    )

    gaps = "\n".join(scorecard["product_value_evals"]["proof_gaps"])
    assert "resource/action-resource probe" in gaps
    assert "archive probe" in gaps
    assert "evidence probe" in gaps
    assert "Question policy eval ran a probe" in gaps
    assert "Question policy eval did not exercise" not in gaps


def test_company_intelligence_scorecard_scores_latent_bridge_inference() -> None:
    scorecard = _company_intelligence_scorecard(
        model_summary=_sample_model_summary(),
        storyline_scores=[
            _sample_storyline_score(),
            _sample_latent_bridge_storyline_score(),
        ],
        waves=[_sample_success_wave(), _sample_future_validation_wave()],
        retrieval_model_counts=[22, 24],
        retrieval_observation_counts=[29, 28],
        validation_errors=0,
    )

    bridge = scorecard["product_value_evals"]["evals"]["latent_bridge_inference"]

    assert bridge["metrics"]["inferred_bridge_model_count"] == 1
    assert bridge["metrics"]["transition_supported_bridge_model_count"] == 1
    assert bridge["metrics"]["future_confirmed_bridge_model_count"] == 1
    assert bridge["metrics"]["unsupported_specific_claim_count"] == 0
    assert bridge["score"] > 0.8
    assert not any(
        "Latent bridge inference eval did not create" in gap
        for gap in scorecard["product_value_evals"]["proof_gaps"]
    )


def test_benchmark_summary_renders_company_intelligence_scorecard() -> None:
    summary = _benchmark_summary(
        model_summary=_sample_model_summary(),
        storyline_scores=[_sample_storyline_score_with_calibration()],
        waves=[_sample_success_wave()],
        elapsed_seconds=12.0,
    )
    markdown = _render_benchmark_markdown(summary)

    assert "company_intelligence_scorecard" in summary
    assert summary["calibration"]["n"] == 2
    assert summary["calibration"]["expected_calibration_error"] is not None
    assert "## Company Intelligence Scorecard" in markdown
    assert "## Calibration" in markdown
    assert "### Product Value Evals" in markdown
    assert "### Proof Gaps" in markdown


def test_storyline_calibration_report_bins_future_validation_samples() -> None:
    report = _storyline_calibration_report(
        [
            _sample_storyline_score_with_calibration(),
        ]
    )

    assert report["n"] == 2
    assert report["positive_outcomes"] == 1
    assert report["negative_outcomes"] == 1
    assert 0.0 <= report["expected_calibration_error"] <= 1.0
    assert any(bucket["n"] for bucket in report["bins"])


def test_storyline_calibration_report_is_empty_without_future_validation_samples() -> (
    None
):
    report = _storyline_calibration_report([_sample_storyline_score()])

    assert report["n"] == 0
    assert report["expected_calibration_error"] is None


def test_variance_report_summarizes_scores_and_judged_rate(tmp_path) -> None:
    report_root = tmp_path / "runs"
    _write_run_artifact(
        report_root,
        "run-a",
        average=0.70,
        company=0.80,
        product=0.60,
        thesis_average=0.75,
        thesis_correct=7,
        thesis_incorrect=2,
    )
    _write_run_artifact(
        report_root,
        "run-b",
        average=0.76,
        company=0.84,
        product=0.63,
        thesis_average=0.80,
        thesis_correct=8,
        thesis_incorrect=1,
    )
    report = build_variance_report(report_root, ["run-a", "run-b"])
    markdown = _render_variance_markdown(report)

    average = report["metrics"]["average_storyline_score"]
    thesis_rate = report["judged_rates"]["thesis_recovery_correct_rate"]

    assert average["n"] == 2
    assert average["mean"] == 0.73
    assert average["min"] == 0.70
    assert average["max"] == 0.76
    assert average["stddev"] > 0
    assert thesis_rate["n"] == 18
    assert thesis_rate["correct"] == 15
    assert thesis_rate["wilson_95_ci"]["low"] < thesis_rate["rate"]
    assert "Wilson 95% CI" in markdown


def _sample_storyline_score() -> StorylineScore:
    return StorylineScore(
        storyline_id="atlas_renewal_risk",
        title="Atlas renewal risk is really security plus usage decay",
        signal_count=25,
        relevant_model_count=8,
        evidence_supported_model_count=4,
        keyword_hits=["atlas", "renewal", "security"],
        missing_keywords=[],
        situation_model_count=1,
        recommendation_model_count=1,
        scoped_edge_count=2,
        edge_kind_hits=["supports", "early_warning_for"],
        missing_edge_kinds=[],
        review_candidate_count=1,
        accepted_candidate_count=1,
        needs_review_candidate_count=0,
        latent_pattern_score=0.9,
        latent_pattern_model_count=1,
        latent_pattern_evidence_supported_model_count=1,
        latent_pattern_best_coverage=1.0,
        latent_pattern_group_hits=["security/evidence", "renewal/risk"],
        missing_latent_pattern_groups=[],
        latent_pattern_model_ids=["model-1"],
        score=0.85,
    )


def _sample_storyline_score_with_calibration() -> StorylineScore:
    score = _sample_storyline_score()
    score.calibration_samples = [
        {
            "storyline_id": score.storyline_id,
            "model_id": "model-1",
            "confidence": 0.82,
            "outcome": 1.0,
            "basis": "future_validation_wave_proxy",
        },
        {
            "storyline_id": score.storyline_id,
            "model_id": "model-2",
            "confidence": 0.74,
            "outcome": 0.0,
            "basis": "future_validation_wave_proxy",
        },
    ]
    return score


def _write_run_artifact(
    report_root,
    run_id: str,
    *,
    average: float,
    company: float,
    product: float,
    thesis_average: float,
    thesis_correct: int,
    thesis_incorrect: int,
) -> None:
    run_dir = report_root / run_id
    run_dir.mkdir(parents=True)
    (run_dir / "storyline_scores.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "signals": 225,
                "storyline_count": 9,
                "elapsed_seconds": 12.0,
                "average_storyline_score": average,
                "company_intelligence_scorecard": {
                    "overall_score": company,
                    "product_value_evals": {"overall_score": product},
                },
                "thesis_recovery_judge": {
                    "enabled": True,
                    "n": thesis_correct + thesis_incorrect,
                    "average_score": thesis_average,
                    "correct_count": thesis_correct,
                    "incorrect_count": thesis_incorrect,
                },
            }
        )
    )
    (run_dir / "run_config.json").write_text(
        json.dumps(
            {
                "run_id": run_id,
                "cache_bypass_env": {"LLM_CACHE_BYPASS": "1"},
            }
        )
    )


def _sample_latent_bridge_storyline_score() -> StorylineScore:
    return StorylineScore(
        storyline_id=_LATENT_BRIDGE_STORYLINE_ID,
        title="Northstar pricing shift implies an unobserved decision bridge",
        signal_count=23,
        relevant_model_count=4,
        evidence_supported_model_count=3,
        keyword_hits=[
            "northstar",
            "pricing",
            "discount",
            "exception",
            "before",
            "after",
            "inferred",
            "unobserved",
            "confidence",
        ],
        missing_keywords=[],
        situation_model_count=1,
        recommendation_model_count=1,
        scoped_edge_count=2,
        edge_kind_hits=["explains", "early_warning_for"],
        missing_edge_kinds=[],
        review_candidate_count=1,
        accepted_candidate_count=1,
        needs_review_candidate_count=0,
        latent_pattern_score=0.88,
        latent_pattern_model_count=1,
        latent_pattern_evidence_supported_model_count=1,
        latent_pattern_best_coverage=1.0,
        latent_pattern_group_hits=[
            "before/after/state/transition",
            "unobserved/inferred/missing/gap",
            "discount/exception/pricing/policy",
        ],
        missing_latent_pattern_groups=[],
        latent_pattern_model_ids=["bridge-model-1"],
        score=0.9,
        inferred_bridge_model_count=1,
        inferred_bridge_transition_supported_model_count=1,
        inferred_bridge_future_confirmed_model_count=1,
        unsupported_bridge_specific_claim_count=0,
        bridge_epistemic_marker_hits=["confidence", "gap", "inferred"],
    )


def _sample_model_summary() -> dict:
    return {
        "run_id": "unit-scorecard",
        "tenant_id": "tenant",
        "signal_count": 25,
        "think_runs_success": 1,
        "think_runs_failed": 0,
        "pending_triggers": 0,
        "active_models": 15005,
        "archived_models": 0,
        "model_edges": 3,
        "relationship_candidates": 1,
        "relationship_candidate_status_distribution": {"accepted": 1},
        "capability_probe_counts": {},
        "model_kind_distribution": {"belief": 15005},
        "context_use_distribution": {"graph_context_used": 1},
        "context_use_relation_contract": {
            "context_use_runs": 1,
            "graph_selected_runs": 1,
            "graph_relation_op_runs": 1,
            "graph_no_edge_rationale_runs": 0,
            "graph_selected_without_relation_ops_runs": 0,
            "graph_relation_contract_satisfied_runs": 1,
            "graph_relation_contract_failed_runs": 0,
        },
        "edge_kind_distribution": {"supports": 2, "early_warning_for": 1},
        "edge_lifecycle": {
            "total_edges": 3,
            "active_edges": 3,
            "accepted_edges": 3,
            "accepted_edge_kind_distribution": {
                "supports": 2,
                "early_warning_for": 1,
            },
            "candidate_edges": 0,
            "needs_review_edges": 0,
            "retired_or_inert_edges": 0,
            "reconfirmed_edges": 0,
            "reconfirmation_events": 0,
            "distinct_edge_kinds": 2,
            "ontology_proposals": 0,
        },
        "graph_health": {"exact_duplicate_natural_groups": 0},
        "discovery_layer_counts": {
            "negative_memory": 0,
            "question_policy_stats": 0,
        },
        "topology_optimizer_metric_totals": {
            "shortcut_creates_or_bumps": 3,
            "affordance_reinforces": 2,
            "negative_memory_inserts": 0,
            "question_policy_updates": 0,
        },
        "post_commit_status": {"dead_lettered": 0},
        "cost": {"llm_calls": 1, "cost_usd": 0.01},
    }


def _sample_success_wave() -> dict:
    return {
        "sequence": "atlas_renewal_risk_wave",
        "signals": 25,
        "t1_batch": {
            "elapsed_s": 30.0,
            "observation_count": 25,
            "run": {
                "status": "success",
                "validation_error_count": 0,
                "retrieval_model_count": 22,
                "retrieval_observation_count": 29,
                "ops_applied": {
                    "claim_ops": [{}, {}, {}],
                    "edge_ops": [
                        {
                            "op": "add",
                            "edge_kind": "supports",
                            "review_status": "accepted",
                        },
                        {
                            "op": "add",
                            "edge_kind": "early_warning_for",
                            "review_status": "accepted",
                        },
                    ],
                    "act_ops": [{}],
                    "resource_ops": [],
                    "ontology_gap_ops": [],
                    "state_changes_emitted": 6,
                    "memory_aggregation": {
                        "model_inserts": 3,
                        "model_updates": 1,
                        "situation_model_updates": 1,
                    },
                    "context_use": {
                        "context_use_grade": "graph_context_used",
                        "selected_trigger_observation_count": 25,
                        "selected_historical_observation_count": 4,
                    },
                },
            },
        },
    }


def _sample_success_wave_with_relation_frame() -> dict:
    wave = json.loads(json.dumps(_sample_success_wave()))
    ops = wave["t1_batch"]["run"]["ops_applied"]
    ops["relation_claim_ops"] = [
        {
            "op": "upsert",
            "relation_kind": "blocked_workstream",
            "status": "accepted",
        }
    ]
    ops["relation_frame_ops"] = [
        {
            "op": "upsert",
            "relation_kind": "blocked_workstream",
            "status": "accepted",
            "write_policy": "project_edges",
            "participant_count": 5,
            "projected_edge_count": 3,
        }
    ]
    return wave


def _sample_future_validation_wave() -> dict:
    return {
        "sequence": "future_validation",
        "signals": 24,
        "t1_batch": {
            "elapsed_s": 34.0,
            "observation_count": 24,
            "run": {
                "status": "success",
                "validation_error_count": 0,
                "retrieval_model_count": 24,
                "retrieval_observation_count": 28,
                "ops_applied": {
                    "claim_ops": [{"op": "update"}],
                    "edge_ops": [
                        {
                            "op": "add",
                            "edge_kind": "blocks",
                            "review_status": "accepted",
                        }
                    ],
                    "act_ops": [],
                    "resource_ops": [],
                    "ontology_gap_ops": [],
                    "memory_aggregation": {
                        "model_updates": 2,
                        "evidence_attachments": 1,
                    },
                    "context_use": {
                        "context_use_grade": "model_context_used",
                        "selected_trigger_observation_count": 24,
                        "selected_historical_observation_count": 3,
                    },
                },
            },
        },
    }
