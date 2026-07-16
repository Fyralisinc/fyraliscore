from __future__ import annotations

import json
from pathlib import Path
from uuid import uuid4

import pytest

from scripts.company_vitals import (
    _collect_company_learning_evaluation,
    apply_db_trace_to_signal_rows,
    build_vitals_from_report_dir,
    write_vitals_artifacts,
)
from scripts.run_company_vitals_harness import main as vitals_main


def test_company_vitals_writes_artifacts_from_e2e_report(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path)

    result = write_vitals_artifacts(report_dir)

    assert result.output_dir == report_dir / "vitals"
    assert (result.output_dir / "vitals_scorecard.json").exists()
    assert (result.output_dir / "vitals_summary.md").exists()
    assert (result.output_dir / "signal_metabolism.jsonl").exists()
    assert (result.output_dir / "residual_trace.jsonl").exists()
    assert (result.output_dir / "coherence_repair_candidates.jsonl").exists()
    assert (result.output_dir / "retrieval_outcome_learning.jsonl").exists()
    assert (result.output_dir / "latent_gap_candidates.jsonl").exists()
    assert (result.output_dir / "graph_coherence.json").exists()
    assert (result.output_dir / "company_learning_evaluation.json").exists()

    scorecard = json.loads((result.output_dir / "vitals_scorecard.json").read_text())
    assert scorecard["status"] == "ok"
    assert scorecard["hard_failures"] == []
    assert scorecard["vitals"]["control_plane_health"]["score"] == 1.0
    assert scorecard["vitals"]["retrieval_roi"]["metrics"]["useful_context_ratio"] == 0.75
    assert scorecard["vitals"]["authority_safety"]["status"] == "not_observed"
    assert scorecard["company_physics"]["status"] == "not_observed"
    assert scorecard["company_physics"]["noncompensatory"] is True

    signal_rows = [
        json.loads(line)
        for line in (result.output_dir / "signal_metabolism.jsonl").read_text().splitlines()
    ]
    assert len(signal_rows) == 2
    assert signal_rows[0]["final_fate"] == "trace_unresolved"
    assert "artifact_only_trace_unresolved" in signal_rows[0]["leak_flags"]


def test_company_vitals_reports_hard_gate_failure(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path, pending_triggers=3)

    scorecard = build_vitals_from_report_dir(report_dir)

    assert scorecard["status"] == "fail"
    assert any("trigger queue did not drain" in item for item in scorecard["hard_failures"])
    assert scorecard["vitals"]["control_plane_health"]["status"] == "watch"


def test_company_physics_is_precise_and_does_not_change_overall_score(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    baseline = build_vitals_from_report_dir(report_dir)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "by_observation": {},
        "company_learning_evaluation": {
            "available": True,
            "status": "substantiated",
            "state": {
                "status": "substantiated",
                "scope": {
                    "run_id": "synthetic-vitals",
                    "tenant_id": "tenant-1",
                },
                "learning_loop": {
                    "governed_alias_replay_exposures": 2,
                    "governed_alias_replays_resolved": 2,
                    "governed_alias_replay_resolution_rate": 1.0,
                    "grounding_to_interpretation_coverage": 1.0,
                    "one_model_cardinality_rate": 1.0,
                },
                "conversation_context": {"selection_count": 2},
                "entity_grounding": {"alias_replay_exposure_count": 2},
                "source_semantics": {"eligible_grounding_count": 2},
                "incident_counts": {},
                "proof_gaps": [],
                "artifact_refs": ["pytest://company-physics"],
            },
        },
    }

    scorecard = build_vitals_from_report_dir(report_dir, db_trace=db_trace)

    assert scorecard["vitals"].keys() == baseline["vitals"].keys()
    assert scorecard["overall_score"] == baseline["overall_score"]
    assert scorecard["scored_vitals"] == baseline["scored_vitals"]
    assert scorecard["total_vitals"] == baseline["total_vitals"]
    assert scorecard["score_coverage"] == baseline["score_coverage"]
    assert "company_physics" not in scorecard["vitals"]
    assert scorecard["company_physics"]["status"] == "substantiated"
    assert scorecard["company_physics"]["learning_loop"][
        "governed_alias_replay_resolution_rate"
    ] == 1.0
    assert scorecard["company_physics"]["components"]["entity_grounding"][
        "alias_replay_exposure_count"
    ] == 2


def test_company_physics_incidents_are_noncompensatory_hard_failures(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "by_observation": {},
        "company_learning_evaluation": {
            "available": True,
            "status": "contradicted",
            "state": {
                "status": "contradicted",
                "scope": {"run_id": "synthetic-vitals"},
                "learning_loop": {},
                "conversation_context": {},
                "entity_grounding": {},
                "source_semantics": {},
                "incident_counts": {
                    "entity_grounding.unsafe_governed_alias_replay": 1
                },
                "proof_gaps": [],
                "artifact_refs": ["pytest://company-physics-incident"],
            },
        },
    }

    scorecard = build_vitals_from_report_dir(report_dir, db_trace=db_trace)

    assert scorecard["status"] == "fail"
    assert any(
        "unsafe_governed_alias_replay" in failure
        for failure in scorecard["hard_failures"]
    )


def test_artifact_only_rerender_rejects_invalid_saved_company_learning_evidence(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "by_observation": {},
        "company_learning_evaluation": {
            "available": True,
            "status": "insufficient",
            "observed_slice_health": "healthy",
            "evaluation_cutoff": "2026-07-16T01:00:00+00:00",
            "state": {
                "status": "insufficient",
                "observed_slice_health": "healthy",
                "scope": {"run_id": "synthetic-vitals"},
                "learning_loop": {
                    "governed_alias_replay_exposures": 1,
                    "governed_alias_replays_resolved": 1,
                },
                "conversation_context": {"selection_count": 1},
                "entity_grounding": {"alias_replay_exposure_count": 1},
                "source_semantics": {"eligible_grounding_count": 1},
                "incident_counts": {},
                "proof_gaps": ["E4 scenarios remain unexecuted."],
                "artifact_refs": ["pytest://frozen-company-learning"],
            },
            "evidence_manifest": {
                "manifest_version": "company-learning-evidence-v1",
                "run_id": "synthetic-vitals",
            },
            "invariant_proof": {"records": []},
        },
    }

    first = write_vitals_artifacts(report_dir, db_trace=db_trace)
    assert (
        first.output_dir / "company_learning_evidence_manifest.json"
    ).exists()
    second = write_vitals_artifacts(report_dir)
    rerendered = json.loads(
        (second.output_dir / "company_learning_evaluation.json").read_text()
    )

    assert rerendered["available"] is False
    assert rerendered["status"] == "unavailable"
    assert rerendered["error"] == "persisted_evaluation_invalid"
    assert second.scorecard["company_physics"]["status"] == "unavailable"
    assert not (
        second.output_dir / "company_learning_evidence_manifest.json"
    ).exists()


def test_custom_output_dir_is_the_persisted_evaluation_source(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    output_dir = tmp_path / "custom-vitals"
    unavailable = {
        "available": False,
        "status": "unavailable",
        "error": "sealed-test-unavailable",
        "proof_gaps": ["custom output marker"],
    }

    write_vitals_artifacts(
        report_dir,
        output_dir=output_dir,
        db_trace={
            "available": True,
            "by_observation": {},
            "company_learning_evaluation": unavailable,
        },
    )
    rerender = write_vitals_artifacts(
        report_dir,
        output_dir=output_dir,
    )

    persisted = json.loads(
        (output_dir / "company_learning_evaluation.json").read_text()
    )
    assert persisted == unavailable
    assert rerender.scorecard["company_physics"]["error"] == (
        "sealed-test-unavailable"
    )
    assert "custom output marker" in rerender.scorecard["proof_gaps"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "missing_table",
    (
        "entity_review_queue",
        "agency_command_results",
        "agency_canonical_events",
        "agency_outbox_records",
        "entity_aliases",
        "clarification_requests",
    ),
)
async def test_company_learning_preflight_reports_each_required_table(
    tmp_path: Path,
    monkeypatch,
    missing_table: str,
) -> None:
    async def table_exists(_conn, table: str) -> bool:
        return table != missing_table

    monkeypatch.setattr("scripts.company_vitals._table_exists", table_exists)

    result = await _collect_company_learning_evaluation(
        object(),
        report_path=tmp_path,
        bundle={
            "benchmark_summary": {},
            "storyline_scores": {},
            "run_summary": {"run_id": "preflight-run"},
            "run_config": {},
        },
        tenant_id=uuid4(),
        observation_ids=(uuid4(),),
    )

    assert result["available"] is False
    assert result["status"] == "unavailable"
    assert result["error"] == "required_tables_missing"
    assert result["missing_tables"] == [missing_table]


def test_company_vitals_projection_metabolism_flags_missing_entity_surfaces(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(
        tmp_path,
        projection_metabolism={
            "available": True,
            "status": "watch",
            "entity_projection_coverage_ratio": 0.5,
            "missing_entity_projection_families": ["commitments", "decisions"],
            "refresh_job_count": 12,
            "pending_refresh_jobs": 0,
            "failed_refresh_jobs": 0,
            "jobs_to_snapshots_ratio": 2.4,
        },
    )

    scorecard = build_vitals_from_report_dir(report_dir)

    projection = scorecard["vitals"]["projection_freshness"]
    assert projection["status"] == "watch"
    assert projection["metrics"]["entity_projection_coverage_ratio"] == 0.5
    assert projection["metrics"]["missing_entity_projection_families"] == [
        "commitments",
        "decisions",
    ]
    assert any(
        "Missing first-class projection surfaces" in gap
        for gap in projection["proof_gaps"]
    )


def test_company_vitals_db_trace_resolves_signal_metabolism(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "table_presence": {"think_trigger_queue": True, "models": True},
        "summary": {"trace_coverage": 1.0},
        "by_observation": {
            "obs-1": {
                "triggers": [
                    {
                        "id": "trigger-1",
                        "observation_id": "obs-1",
                        "trigger_kind": "T1",
                        "completed_at": "2026-07-01T00:00:00+00:00",
                    }
                ],
                "think_runs": [
                    {
                        "id": "run-1",
                        "trigger_id": "trigger-1",
                        "status": "success",
                        "ops_applied": {
                            "context_use": {
                                "selected_model_ids": ["prior-model"],
                                "selected_observation_ids": ["obs-1"],
                                "referenced_model_ids": ["model-1"],
                                "referenced_observation_ids": ["obs-1"],
                            }
                        },
                    }
                ],
                "models": [
                    {
                        "id": "model-1",
                        "born_from_event_id": "obs-1",
                        "provenance": "born_from_event",
                    }
                ],
                "model_edges": [{"id": "edge-1", "created_by_event_id": "obs-1"}],
                "model_events": [{"id": "event-1", "model_id": "model-1"}],
                "projection_snapshots": [
                    {
                        "projection_name": "customer_health",
                        "subject_key": "atlas",
                        "source_model_ids": ["model-1"],
                    }
                ],
                "inquiry_outcome_events": [
                    {"id": "outcome-1", "event_type": "recommendation_acted_on"}
                ],
            },
            "obs-2": {
                "triggers": [
                    {
                        "id": "trigger-2",
                        "observation_id": "obs-2",
                        "trigger_kind": "T1",
                        "completed_at": "2026-07-01T00:00:00+00:00",
                    }
                ],
                "think_runs": [
                    {
                        "id": "run-2",
                        "trigger_id": "trigger-2",
                        "status": "success",
                    }
                ],
                "model_signal_readings": [
                    {
                        "id": "reading-1",
                        "model_id": "model-1",
                        "source_event_id": "obs-2",
                        "reading_kind": "confirm",
                    }
                ],
            },
        },
    }

    scorecard = build_vitals_from_report_dir(report_dir, db_trace=db_trace)

    metabolism = scorecard["vitals"]["metabolism_yield"]
    assert metabolism["status"] == "ok"
    assert metabolism["metrics"]["trace_resolved_signals"] == 2
    assert metabolism["metrics"]["fate_counts"]["decision_outcome_recorded"] == 1
    assert metabolism["metrics"]["fate_counts"]["evidence_attached"] == 1

    rows = apply_db_trace_to_signal_rows(
        [
            {"observation_id": "obs-1", "final_fate": "trace_unresolved"},
            {"observation_id": "obs-2", "final_fate": "trace_unresolved"},
        ],
        db_trace,
    )
    assert rows[0]["final_fate"] == "decision_outcome_recorded"
    assert rows[0]["projection_subjects"] == ["customer_health:atlas"]
    assert rows[1]["final_fate"] == "evidence_attached"


def test_company_vitals_emits_residual_repair_retrieval_and_latent_traces(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "table_presence": {"think_trigger_queue": True, "think_runs": True},
        "summary": {"trace_coverage": 1.0},
        "by_observation": {
            "obs-1": {"triggers": []},
            "obs-2": {
                "triggers": [
                    {
                        "id": "trigger-2",
                        "observation_id": "obs-2",
                        "trigger_kind": "T1",
                        "completed_at": "2026-07-01T00:00:00+00:00",
                    }
                ],
                "think_runs": [
                    {
                        "id": "run-2",
                        "trigger_id": "trigger-2",
                        "status": "success",
                        "ops_applied": {
                            "context_use": {
                                "selected_model_ids": ["model-prior"],
                                "selected_observation_ids": ["obs-2"],
                                "referenced_model_ids": [],
                                "referenced_observation_ids": [],
                            }
                        },
                    }
                ],
                "omitted_evidence": [
                    {
                        "id": "omit-1",
                        "inquiry_session_id": "session-1",
                        "omission_reason": "budget",
                    }
                ],
            },
        },
    }

    result = write_vitals_artifacts(report_dir, db_trace=db_trace)

    residuals = _read_jsonl(result.output_dir / "residual_trace.jsonl")
    residual_kinds = {row["residual_kind"] for row in residuals}
    assert residual_kinds == {"valuable_unmodeled", "counterevidence_unattached"}

    repairs = _read_jsonl(result.output_dir / "coherence_repair_candidates.jsonl")
    repair_kinds = {row["candidate_kind"] for row in repairs}
    assert "skipped_metabolism_repair" in repair_kinds
    assert "residual_counterevidence_unattached" in repair_kinds

    retrieval = _read_jsonl(result.output_dir / "retrieval_outcome_learning.jsonl")
    obs2 = next(row for row in retrieval if row["observation_id"] == "obs-2")
    assert obs2["outcome_class"] == "no_durable_fate"
    assert obs2["learning_signal"] == "penalize_omission_or_packet_selection"

    latent = _read_jsonl(result.output_dir / "latent_gap_candidates.jsonl")
    assert all(row["supporting_residual_ids"] for row in latent)
    assert all(row["falsifier"] for row in latent)
    assert all(row["next_evidence_needed"] for row in latent)

    scorecard = json.loads((result.output_dir / "vitals_scorecard.json").read_text())
    assert scorecard["vitals"]["residual_channel"]["metrics"][
        "residual_candidate_count"
    ] == 2
    assert scorecard["vitals"]["retrieval_outcome_learning"]["metrics"][
        "learnable_retrieval_decisions"
    ] == 1


def test_company_vitals_prefers_persisted_residual_lifecycle_rows(
    tmp_path: Path,
) -> None:
    report_dir = _write_report_dir(tmp_path)
    db_trace = {
        "available": True,
        "tenant_id": "tenant-1",
        "observation_count": 2,
        "table_presence": {
            "model_residual_evidence": True,
            "sage_latent_gap_hypotheses": True,
        },
        "summary": {"trace_coverage": 1.0},
        "by_observation": {
            "obs-1": {
                "triggers": [
                    {
                        "id": "trigger-1",
                        "observation_id": "obs-1",
                        "trigger_kind": "T1",
                    }
                ],
                "model_residual_evidence": [
                    {
                        "id": "residual-1",
                        "source_observation_id": "obs-1",
                        "residual_kind": "valuable_unmodeled",
                        "compact_summary": "Atlas blocker was not modeled.",
                        "reason": "model compression missed the blocker",
                        "status": "absorbed",
                        "absorption_object_kind": "model",
                        "absorption_object_id": "model-1",
                        "metadata": {"absorbed_by": "test"},
                    }
                ],
            },
            "obs-2": {
                "triggers": [
                    {
                        "id": "trigger-2",
                        "observation_id": "obs-2",
                        "trigger_kind": "T1",
                    }
                ],
                "model_residual_evidence": [
                    {
                        "id": "residual-2",
                        "source_observation_id": "obs-2",
                        "residual_kind": "counterevidence_unattached",
                        "compact_summary": "Future validation needs attachment.",
                        "reason": "counterevidence did not attach",
                        "status": "open",
                        "metadata": {},
                    }
                ],
                "sage_latent_gap_hypotheses": [
                    {
                        "id": "gap-1",
                        "gap_kind": "counterevidence_unattached",
                        "status": "candidate",
                        "residual_cluster_hash": "cluster-1",
                        "supporting_residual_ids": ["residual-2"],
                        "supporting_observation_ids": ["obs-2"],
                        "missing_evidence_statement": "Counterevidence lacks target.",
                        "falsifier": "A later trace attaches it.",
                        "next_evidence_needed": "Find target model.",
                        "confidence": 0.45,
                        "hypothesis_text": "Latent counterevidence target gap.",
                        "metadata": {"source": "test"},
                    }
                ],
            },
        },
    }

    result = write_vitals_artifacts(report_dir, db_trace=db_trace)

    signal_rows = _read_jsonl(result.output_dir / "signal_metabolism.jsonl")
    obs1 = next(row for row in signal_rows if row["observation_id"] == "obs-1")
    assert obs1["persisted_residual_ids"] == ["residual-1"]
    assert obs1["persisted_residual_statuses"] == ["absorbed"]

    residuals = _read_jsonl(result.output_dir / "residual_trace.jsonl")
    assert [row["residual_id"] for row in residuals] == ["residual-1", "residual-2"]
    absorbed = residuals[0]
    assert absorbed["status"] == "absorbed"
    assert absorbed["source"] == "model_residual_evidence"
    assert absorbed["absorption_object_kind"] == "model"

    repairs = _read_jsonl(result.output_dir / "coherence_repair_candidates.jsonl")
    repair_kinds = {row["candidate_kind"] for row in repairs}
    assert "residual_counterevidence_unattached" in repair_kinds
    assert "residual_valuable_unmodeled" not in repair_kinds

    latent = _read_jsonl(result.output_dir / "latent_gap_candidates.jsonl")
    persisted = next(row for row in latent if row["candidate_id"] == "gap-1")
    assert persisted["source"] == "sage_latent_gap_hypotheses"
    assert persisted["status"] == "candidate"
    assert persisted["supporting_residual_ids"] == ["residual-2"]
    assert persisted["status_semantics"] == "non_canonical_until_confirmed"


def test_company_vitals_cli_can_fail_on_hard_gates(tmp_path: Path) -> None:
    report_dir = _write_report_dir(tmp_path, pending_triggers=1)

    exit_code = vitals_main(["--report-dir", str(report_dir), "--fail-on-hard-gates"])

    assert exit_code == 1
    assert (report_dir / "vitals" / "vitals_scorecard.json").exists()


def _write_report_dir(
    tmp_path: Path,
    *,
    pending_triggers: int = 0,
    projection_metabolism: dict | None = None,
) -> Path:
    report_dir = tmp_path / "synthetic-e2e-report"
    report_dir.mkdir()
    _write_json(
        report_dir / "run_summary.json",
        {
            "run_id": "synthetic-vitals",
            "tenant_id": "tenant-1",
            "signal_count": 2,
            "observation_count": 2,
            "active_models": 2,
            "pending_triggers": pending_triggers,
            "pending_post_commit_actions": 0,
            "dead_lettered_post_commit_actions": 0,
            "think_runs_success": 4,
            "think_runs_failed": 0,
            "context_use_distribution": {
                "model_context_used": 2,
                "graph_context_used": 1,
                "no_selected_context": 1,
            },
            "context_use_relation_contract": {
                "context_use_runs": 4,
                "graph_relation_contract_failed_runs": 0,
            },
            "post_commit_status": {"processed": 2, "dead_lettered": 0},
            "topology_optimizer_status": {"status": "drained", "failed": 0},
            "projection_metabolism": projection_metabolism or {},
            "graph_health": {
                "active_model_count": 2,
                "active_edge_count": 1,
                "isolated_model_ratio": 0.0,
                "largest_component_ratio": 1.0,
                "duplicate_directed_edge_count": 0,
                "exact_duplicate_natural_groups": 0,
                "orphan_edge_count": 0,
                "self_edge_count": 0,
            },
        },
    )
    _write_json(
        report_dir / "benchmark_summary.json",
        {
            "run_id": "synthetic-vitals",
            "tenant_id": "tenant-1",
            "status": "passed",
            "required_run_failures": [],
            "run_amplification": {
                "pending_triggers": pending_triggers,
                "think_runs_failed": 0,
                "think_runs_success": 4,
                "validation_error_count": 0,
            },
            "company_intelligence_scorecard": {
                "overall_score": 0.9,
                "interpretation": "synthetic healthy run",
                "proof_gaps": [],
                "dimensions": {
                    "adaptive_lifecycle": {"score": 0.9, "metrics": {}},
                    "compression": {
                        "score": 1.0,
                        "metrics": {"model_updates": 1, "model_inserts": 1},
                    },
                    "edge_intelligence": {"score": 1.0, "metrics": {}},
                    "efficiency": {"score": 0.9, "metrics": {"cost_per_signal_usd": 0.01}},
                    "retrieval_usefulness": {
                        "score": 0.9,
                        "metrics": {"model_or_graph_context_use_score": 0.75},
                    },
                    "temporal_improvement": {
                        "score": 0.8,
                        "metrics": {
                            "future_validation_events": 2,
                            "future_validation_memory_touch_ops": 1,
                            "future_validation_model_or_graph_context_use_score": 1.0,
                        },
                    },
                },
                "product_value_evals": {
                    "overall_score": 0.88,
                    "proof_gaps": [],
                    "evals": {
                        "decision_impact": {"score": 0.8, "metrics": {}},
                        "experience_metabolism": {"score": 0.9, "metrics": {}},
                        "negative_learning": {"score": 0.9, "metrics": {}},
                        "prediction_lifecycle": {"score": 0.8, "metrics": {}},
                        "question_policy": {"score": 0.9, "metrics": {}},
                    },
                },
            },
        },
    )
    _write_jsonl(
        report_dir / "planned_signals.jsonl",
        [
            {
                "index": 0,
                "storyline_id": "atlas",
                "sequence": "atlas_wave_001",
                "family": "customer_escalation",
                "content": "Atlas renewal evidence is blocked.",
            },
            {
                "index": 1,
                "storyline_id": "atlas",
                "sequence": "atlas_future_validation_wave_002",
                "family": "future_validation",
                "content": "Atlas later confirms the renewal blocker.",
            },
        ],
    )
    _write_jsonl(
        report_dir / "signal_manifest.jsonl",
        [
            {
                "index": 0,
                "channel": "slack:customer-success",
                "family": "customer_escalation",
                "observation_id": "obs-1",
            },
            {
                "index": 1,
                "channel": "email:customer-success",
                "family": "future_validation",
                "observation_id": "obs-2",
            },
        ],
    )
    _write_jsonl(
        report_dir / "models.jsonl",
        [
            {
                "id": "model-1",
                "status": "active",
                "natural": "Atlas renewal blocker is evidence readiness.",
                "confidence": 0.8,
                "proposition_kind": "belief",
                "claim_role": "situation",
                "supporting_event_ids": ["obs-1"],
                "falsifier": {"kind": "explicit_contestation"},
            },
            {
                "id": "model-2",
                "status": "active",
                "natural": "Atlas blocker was later confirmed.",
                "confidence": 0.7,
                "proposition_kind": "belief",
                "claim_role": "fact",
                "supporting_event_ids": ["obs-2"],
                "falsifier": {"kind": "explicit_contestation"},
            },
        ],
    )
    _write_jsonl(
        report_dir / "model_edges.jsonl",
        [
            {
                "id": "edge-1",
                "status": "active",
                "edge_kind": "supports",
                "source_model_id": "model-2",
                "target_model_id": "model-1",
            }
        ],
    )
    return report_dir


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("".join(json.dumps(row, sort_keys=True) + "\n" for row in rows))


def _read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line]
