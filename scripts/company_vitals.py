"""Company-understanding vitals renderer for Fyralis report artifacts.

The vitals harness is intentionally additive. It reads the artifacts produced by
existing end-to-end runs and emits a normalized "where did value flow or leak?"
report without rerunning Think or requiring a live database.
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import UUID

from lib.architecture_registry import load_architecture_registry
from lib.contracts.kernel import canonical_sha256
from lib.evaluation.company_learning import (
    ACTIVE_COMPANY_LEARNING_INVARIANT_IDS,
    CompanyLearningEvaluationScope,
    CompanyLearningEvaluationState,
    build_company_learning_evidence_manifest,
    company_learning_assurance_status,
    evaluate_company_learning_state,
)
from lib.evaluation.company_learning_experiment import (
    CorrectiveMemoryExperimentReport,
    CorrectiveMemoryExperimentSpec,
    evaluate_corrective_memory_experiment,
)
from lib.evaluation.company_learning_experiment_proof import (
    build_corrective_memory_experiment_evidence_manifest,
)
from lib.evaluation.proof import (
    InvariantEvidenceBundle,
    InvariantEvidenceManifest,
    aggregate_invariant_evidence_manifests,
    compile_invariant_proof_matrix,
)


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_ARCHITECTURE_REGISTRY = ROOT / "architecture" / "registry.yaml"

USEFUL_CONTEXT_GRADES = {
    "graph_context_used",
    "model_context_used",
    "observation_context_used",
    "justified_noop_context_used",
}

VALUABLE_SIGNAL_FATES = {
    "model_created",
    "model_updated",
    "evidence_attached",
    "counterevidence_attached",
    "falsifier_created",
    "open_question_created",
    "relationship_candidate_created",
    "edge_created",
    "relation_frame_created",
    "projection_updated",
    "product_surface_updated",
    "human_feedback_requested",
    "decision_outcome_recorded",
    "self_improvement_event_created",
    "think_noop_justified",
}

LEAK_FATES = {
    "raw_only_unmodeled",
    "no_think_trigger",
    "trigger_pending",
    "think_failed",
    "think_noop_suspicious",
    "validation_dropped",
    "trace_unresolved",
}

RESIDUAL_KIND_BY_FATE = {
    "raw_only_unmodeled": "valuable_unmodeled",
    "no_think_trigger": "valuable_unmodeled",
    "trigger_pending": "valuable_unmodeled",
    "think_failed": "valuable_unmodeled",
    "validation_dropped": "validation_dropped_value",
    "think_noop_suspicious": "compression_uncertain",
}

POSITIVE_OUTCOME_FATES = {
    "model_created",
    "model_updated",
    "evidence_attached",
    "counterevidence_attached",
    "falsifier_created",
    "open_question_created",
    "relationship_candidate_created",
    "edge_created",
    "relation_frame_created",
    "projection_updated",
    "product_surface_updated",
    "decision_outcome_recorded",
    "self_improvement_event_created",
}

GENERIC_EDGE_KINDS = {
    "related_to",
    "associated_with",
    "impacts",
    "affects",
    "influences",
    "connected_to",
    "linked_to",
    "depends_on",
}

MODEL_WRAPPER_PHRASES = {
    "evidence window containing",
    "the window wrapper is not itself a business fact",
    "open operating questions remain",
    "review owner and next action",
}

MODEL_CONJUNCTION_MARKERS = (" and ", ";", " / ", " or ")

_UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)


@dataclass(frozen=True)
class VitalsArtifacts:
    report_dir: Path
    output_dir: Path
    scorecard: dict[str, Any]
    signal_metabolism_rows: list[dict[str, Any]]
    db_trace_summary: dict[str, Any]


def build_vitals_from_report_dir(
    report_dir: Path | str,
    *,
    db_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a vitals scorecard from an existing E2E report directory."""
    report_path = Path(report_dir)
    bundle = _load_artifact_bundle(report_path)
    signal_rows = _build_signal_metabolism_rows(bundle)
    if db_trace is not None:
        signal_rows = apply_db_trace_to_signal_rows(signal_rows, db_trace)
    return _build_vitals_scorecard(report_path, bundle, signal_rows, db_trace=db_trace)


def _build_vitals_scorecard(
    report_path: Path,
    bundle: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    *,
    db_trace: dict[str, Any] | None = None,
    company_learning_evaluation: dict[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark = bundle["benchmark_summary"] or bundle["storyline_scores"]
    run_summary = bundle["run_summary"]
    models = bundle["models"]
    edges = bundle["model_edges"]
    residual_rows = _residual_trace_rows(signal_rows)
    repair_rows = _coherence_repair_candidate_rows(bundle, signal_rows, residual_rows)
    retrieval_outcome_rows = _retrieval_outcome_learning_rows(signal_rows)
    latent_gap_rows = _latent_gap_candidate_rows(signal_rows, residual_rows)

    source_scorecard = _json_obj(benchmark.get("company_intelligence_scorecard"))
    source_product_value = _json_obj(source_scorecard.get("product_value_evals"))
    generated_at = datetime.now(timezone.utc).isoformat()
    if company_learning_evaluation is None:
        company_learning_evaluation = _company_learning_evaluation_for_report(
            report_path,
            bundle=bundle,
            db_trace=db_trace,
        )
    company_physics = _company_physics_section(
        company_learning_evaluation,
        experiment=_json_obj(bundle.get("company_learning_experiment")),
    )

    vitals: dict[str, Any] = {
        "metabolism_yield": _metabolism_vital(signal_rows, bundle),
        "control_plane_health": _control_plane_vital(run_summary, benchmark),
        "retrieval_roi": _retrieval_vital(run_summary, source_scorecard),
        "reasoning_throughput": _reasoning_vital(run_summary, benchmark),
        "model_atomicity": _model_atomicity_vital(models),
        "company_object_spine_health": _company_object_spine_vital(bundle, models),
        "compression_health": _compression_vital(
            run_summary,
            source_scorecard,
            models,
        ),
        "residual_channel": _residual_channel_vital(signal_rows, residual_rows),
        "model_coherence": _coherence_vital(run_summary, source_scorecard, edges),
        "edge_specificity": _edge_specificity_vital(edges, run_summary),
        "coherence_repair": _coherence_repair_vital(repair_rows, run_summary),
        "active_frontier_health": _active_frontier_vital(
            run_summary,
            source_scorecard,
            models,
            edges,
        ),
        "create_update_balance": _create_update_balance_vital(signal_rows),
        "temporal_learning": _temporal_vital(run_summary, source_scorecard),
        "projection_freshness": _projection_vital(run_summary),
        "product_utility": _product_utility_vital(source_product_value),
        "human_loop_closure": _human_loop_vital(source_product_value),
        "decision_outcome_learning": _decision_outcome_vital(source_product_value),
        "retrieval_outcome_learning": _retrieval_outcome_learning_vital(
            signal_rows,
            retrieval_outcome_rows,
        ),
        "organizational_change_memory": _organizational_change_vital(bundle),
        "self_improvement": _self_improvement_vital(
            source_scorecard,
            source_product_value,
        ),
        "latent_gap_modeling": _latent_gap_vital(
            signal_rows,
            residual_rows,
            latent_gap_rows,
        ),
        "dark_matter_loop": _dark_matter_loop_vital(
            signal_rows,
            residual_rows,
            latent_gap_rows,
            source_product_value,
        ),
        "sage_policy_effect": _sage_policy_effect_vital(
            run_summary,
            source_scorecard,
            source_product_value,
        ),
        "pattern_cascade": _pattern_cascade_vital(
            signal_rows,
            source_scorecard,
            source_product_value,
        ),
        "ask_signal_learning": _ask_signal_learning_vital(source_product_value),
        "simplification_pressure": _simplification_pressure_vital(
            run_summary,
            source_scorecard,
        ),
        "governance_health": _governance_vital(models, run_summary),
        "authority_safety": _authority_safety_vital(bundle),
        "efficiency": _efficiency_vital(run_summary, source_scorecard),
    }
    measurement_profile = str(
        run_summary.get("vitals_measurement_profile") or "full"
    )
    if measurement_profile not in {"full", "company_learning_only"}:
        raise ValueError(
            f"unknown Vitals measurement profile: {measurement_profile}"
        )
    if measurement_profile == "company_learning_only":
        vitals = _company_learning_only_vitals(vitals)

    hard_failures = _hard_failures(vitals, benchmark)
    hard_failures.extend(
        str(item)
        for item in _json_list(company_physics.get("hard_failures"))
    )
    scored = [v["score"] for v in vitals.values() if isinstance(v.get("score"), (int, float))]
    overall_score = round(sum(scored) / len(scored), 4) if scored else None
    status = "fail" if hard_failures else _status_from_score(overall_score)
    proof_gaps = _proof_gaps(vitals, source_scorecard, source_product_value)
    proof_gaps = sorted(
        {
            *proof_gaps,
            *(
                str(item)
                for item in _json_list(company_physics.get("proof_gaps"))
            ),
        }
    )
    ranked_findings = _ranked_findings(vitals, hard_failures)

    return {
        "schema_version": 1,
        "generated_at": generated_at,
        "source_report_dir": str(report_path),
        "run_id": run_summary.get("run_id") or benchmark.get("run_id") or report_path.name,
        "tenant_id": run_summary.get("tenant_id") or benchmark.get("tenant_id"),
        "status": status,
        "overall_score": overall_score,
        "scored_vitals": len(scored),
        "total_vitals": len(vitals),
        "score_coverage": round(len(scored) / max(1, len(vitals)), 4),
        "vitals_measurement_profile": measurement_profile,
        "hard_failures": hard_failures,
        "ranked_findings": ranked_findings,
        "proof_gaps": proof_gaps,
        "vitals": vitals,
        "company_physics": company_physics,
        "source_company_intelligence": {
            "overall_score": source_scorecard.get("overall_score"),
            "interpretation": source_scorecard.get("interpretation"),
            "proof_gaps": _json_list(source_scorecard.get("proof_gaps")),
            "product_value_overall_score": source_product_value.get("overall_score"),
        },
        "db_trace": _trace_summary(db_trace),
    }


def write_vitals_artifacts(
    report_dir: Path | str,
    *,
    output_dir: Path | str | None = None,
    db_trace: dict[str, Any] | None = None,
) -> VitalsArtifacts:
    """Render vitals artifacts under ``<report_dir>/vitals`` by default."""
    report_path = Path(report_dir)
    out = Path(output_dir) if output_dir is not None else report_path / "vitals"
    out.mkdir(parents=True, exist_ok=True)

    bundle = _load_artifact_bundle(report_path)
    signal_rows = _build_signal_metabolism_rows(bundle)
    if db_trace is not None:
        signal_rows = apply_db_trace_to_signal_rows(signal_rows, db_trace)
    company_learning_evaluation = _company_learning_evaluation_for_report(
        report_path,
        bundle=bundle,
        db_trace=db_trace,
        persisted_path=out / "company_learning_evaluation.json",
    )
    scorecard = _build_vitals_scorecard(
        report_path,
        bundle,
        signal_rows,
        db_trace=db_trace,
        company_learning_evaluation=company_learning_evaluation,
    )
    graph_coherence = _graph_coherence_payload(bundle, scorecard)
    db_summary = _trace_summary(db_trace)
    residual_rows = _residual_trace_rows(signal_rows)
    repair_rows = _coherence_repair_candidate_rows(bundle, signal_rows, residual_rows)
    retrieval_outcome_rows = _retrieval_outcome_learning_rows(signal_rows)
    latent_gap_rows = _latent_gap_candidate_rows(signal_rows, residual_rows)

    _write_json(out / "vitals_run.json", _vitals_run_payload(bundle, scorecard))
    _write_json(out / "vitals_scorecard.json", scorecard)
    if not company_learning_evaluation:
        company_learning_evaluation = {
            "available": False,
            "status": "not_observed",
            "proof_gaps": _json_list(
                _json_obj(scorecard.get("company_physics")).get("proof_gaps")
            ),
        }
    _write_json(
        out / "company_learning_evaluation.json",
        company_learning_evaluation,
    )
    evidence_manifest_path = out / "company_learning_evidence_manifest.json"
    evidence_manifest = _json_obj(
        company_learning_evaluation.get("evidence_manifest")
    )
    if evidence_manifest:
        _write_json(
            evidence_manifest_path,
            evidence_manifest,
        )
    else:
        evidence_manifest_path.unlink(missing_ok=True)
    evidence_bundle_path = out / "company_learning_evidence_bundle.json"
    evidence_bundle = _json_obj(
        company_learning_evaluation.get("evidence_bundle")
    )
    if evidence_bundle:
        _write_json(evidence_bundle_path, evidence_bundle)
    else:
        evidence_bundle_path.unlink(missing_ok=True)
    experiment = _json_obj(bundle.get("company_learning_experiment"))
    experiment_payload = _json_obj(experiment.get("canonical_payload"))
    if experiment.get("valid") is True and experiment_payload:
        _write_json(
            out / "company_learning_scenario_evidence.json",
            experiment_payload,
        )
    _write_json(out / "db_trace_summary.json", db_summary)
    _write_json(out / "graph_coherence.json", graph_coherence)
    _write_json(out / "proof_gaps.json", {"proof_gaps": scorecard["proof_gaps"]})
    _write_jsonl(out / "signal_metabolism.jsonl", signal_rows)
    _write_jsonl(out / "residual_trace.jsonl", residual_rows)
    _write_jsonl(out / "coherence_repair_candidates.jsonl", repair_rows)
    _write_jsonl(out / "retrieval_outcome_learning.jsonl", retrieval_outcome_rows)
    _write_jsonl(out / "latent_gap_candidates.jsonl", latent_gap_rows)
    _write_jsonl(out / "trigger_trace.jsonl", [_trigger_trace_row(bundle)])
    _write_jsonl(out / "retrieval_trace.jsonl", [_retrieval_trace_row(bundle, scorecard)])
    _write_jsonl(out / "think_trace.jsonl", [_think_trace_row(bundle, scorecard)])
    _write_jsonl(out / "validation_trace.jsonl", [_validation_trace_row(bundle, scorecard)])
    _write_jsonl(out / "model_delta.jsonl", _model_delta_rows(bundle))
    _write_jsonl(out / "projection_trace.jsonl", [_projection_trace_row(bundle, scorecard)])
    _write_jsonl(
        out / "product_surface_trace.jsonl",
        [_product_surface_trace_row(bundle, scorecard)],
    )
    _write_jsonl(
        out / "human_feedback_trace.jsonl",
        [_human_feedback_trace_row(bundle, scorecard)],
    )
    _write_jsonl(
        out / "decision_outcome_trace.jsonl",
        [_decision_outcome_trace_row(bundle, scorecard)],
    )
    _write_jsonl(
        out / "self_improvement_trace.jsonl",
        [_self_improvement_trace_row(bundle, scorecard)],
    )
    _write_jsonl(out / "governance_trace.jsonl", [_governance_trace_row(bundle, scorecard)])
    _write_jsonl(
        out / "authority_safety_trace.jsonl",
        [_authority_safety_trace_row(bundle, scorecard)],
    )
    (out / "vitals_summary.md").write_text(
        render_vitals_markdown(scorecard),
        encoding="utf-8",
    )
    return VitalsArtifacts(
        report_dir=report_path,
        output_dir=out,
        scorecard=scorecard,
        signal_metabolism_rows=signal_rows,
        db_trace_summary=db_summary,
    )


async def collect_db_trace_for_report_dir(
    report_dir: Path | str,
    *,
    database_url: str,
    tenant_id: str | None = None,
) -> dict[str, Any]:
    """Collect DB-backed lineage for the report's observation ids.

    The collector is deliberately best-effort. Missing tables are represented in
    ``table_presence`` and skipped instead of failing the whole harness, because
    older report databases may not have every long-term feedback surface yet.
    """
    import asyncpg

    report_path = Path(report_dir)
    bundle = _load_artifact_bundle(report_path)
    selected_tenant_id = (
        tenant_id
        or bundle["run_summary"].get("tenant_id")
        or bundle["benchmark_summary"].get("tenant_id")
        or bundle["storyline_scores"].get("tenant_id")
    )
    if not selected_tenant_id:
        return {
            "available": False,
            "error": "tenant_id_missing",
            "by_observation": {},
            "table_presence": {},
        }

    observation_ids = [
        str(row.get("observation_id"))
        for row in bundle["signal_manifest"]
        if _coerce_uuid(row.get("observation_id")) is not None
    ]
    observation_ids = sorted(dict.fromkeys(observation_ids))
    if not observation_ids:
        return {
            "available": False,
            "error": "observation_ids_missing",
            "by_observation": {},
            "table_presence": {},
        }

    conn = await asyncpg.connect(database_url)
    try:
        async with conn.transaction(isolation="repeatable_read", readonly=True):
            trace = await _collect_db_trace(
                conn,
                tenant_id=str(selected_tenant_id),
                observation_ids=observation_ids,
            )
            trace["company_learning_evaluation"] = (
                await _collect_company_learning_evaluation(
                    conn,
                    report_path=report_path,
                    bundle=bundle,
                    tenant_id=UUID(str(selected_tenant_id)),
                    observation_ids=tuple(UUID(value) for value in observation_ids),
                )
            )
    finally:
        await conn.close()
    return trace


async def _collect_company_learning_evaluation(
    conn: Any,
    *,
    report_path: Path,
    bundle: dict[str, Any],
    tenant_id: UUID,
    observation_ids: tuple[UUID, ...],
) -> dict[str, Any]:
    required_tables = (
        "observations",
        "interpretation_context_heads",
        "interpretation_context_snapshots",
        "conversation_context_candidate_records",
        "entity_mention_detection_heads",
        "entity_mention_detections",
        "entity_grounding_work_items",
        "grounding_traces",
        "entity_candidate_generation_requests",
        "entity_candidate_sets",
        "resolution_assessments",
        "grounding_admission_decisions",
        "source_semantic_interpretations",
        "source_semantic_admission_decisions",
        "models",
        "agency_command_results",
        "agency_canonical_events",
        "agency_outbox_records",
        "clarification_requests",
        "entity_aliases",
    )
    missing = [
        table for table in required_tables if not await _table_exists(conn, table)
    ]
    if missing:
        return {
            "available": False,
            "status": "unavailable",
            "error": "required_tables_missing",
            "missing_tables": missing,
            "proof_gaps": [
                "Company-physics evaluators were unavailable because required "
                "tables are missing: " + ", ".join(missing)
            ],
        }
    bounds = await conn.fetchrow(
        """
        SELECT count(*) AS matched_count,
               min(occurred_at) AS start_at,
               max(occurred_at) AS end_at
        FROM observations
        WHERE tenant_id=$1 AND id=ANY($2::uuid[])
        """,
        tenant_id,
        list(observation_ids),
    )
    if (
        bounds is None
        or int(bounds["matched_count"] or 0) != len(observation_ids)
        or bounds["start_at"] is None
        or bounds["end_at"] is None
    ):
        return {
            "available": False,
            "status": "unavailable",
            "error": "observation_population_incomplete",
            "proof_gaps": [
                "The selected tenant does not contain the complete manifest "
                "observation population."
            ],
        }
    run_id = _company_learning_run_id(bundle, report_path=report_path)
    snapshot_started_at = await conn.fetchval("SELECT transaction_timestamp()")
    evaluation_cutoff = max(
        snapshot_started_at,
        bounds["end_at"] + timedelta(microseconds=1),
    )
    try:
        state = await evaluate_company_learning_state(
            conn,
            scope=CompanyLearningEvaluationScope(
                tenant_id=tenant_id,
                observation_ids=observation_ids,
                start=bounds["start_at"] - timedelta(microseconds=1),
                end=evaluation_cutoff,
                run_id=run_id,
            ),
            artifact_refs=(f"report-directory:{report_path}",),
        )
        registry = load_architecture_registry(DEFAULT_ARCHITECTURE_REGISTRY)
        experiment_manifest_ref = _company_learning_experiment_manifest_ref(
            bundle,
            run_id=run_id,
        )
        manifest = build_company_learning_evidence_manifest(
            state,
            registry=registry,
            system_version=_company_learning_system_version(bundle),
            experiment_manifest_ref=experiment_manifest_ref,
            executed_scenario_ids=frozenset(),
        )
        evidence_bundle = _company_learning_evidence_bundle(
            manifest,
            artifact_bundle=bundle,
            report_cutoff=evaluation_cutoff.isoformat(),
        )
        proof = compile_invariant_proof_matrix(
            registry,
            run_id=run_id,
            evidence=(
                evidence_bundle.evidence
                if evidence_bundle is not None
                else manifest.evidence
            ),
        )
        assurance_status = company_learning_assurance_status(state, proof)
    except Exception as exc:  # noqa: BLE001 - best-effort old-report rerender
        return {
            "available": False,
            "status": "unavailable",
            "error": type(exc).__name__,
            "detail": str(exc)[:1000],
            "proof_gaps": [
                "Company-physics evaluation failed without invalidating the "
                "artifact-only vitals rerender."
            ],
        }
    return {
        "available": True,
        "status": assurance_status,
        "observed_slice_health": state.observed_slice_health,
        "evaluation_cutoff": evaluation_cutoff.isoformat(),
        "state": state.model_dump(mode="json"),
        "evidence_manifest": manifest.model_dump(mode="json"),
        "evidence_bundle": (
            evidence_bundle.model_dump(mode="json")
            if evidence_bundle is not None
            else None
        ),
        "invariant_proof": proof.model_dump(mode="json"),
    }


def _company_learning_evaluation_for_report(
    report_path: Path,
    *,
    bundle: dict[str, Any],
    db_trace: dict[str, Any] | None,
    persisted_path: Path | None = None,
) -> dict[str, Any]:
    live = _json_obj(
        _json_obj(db_trace).get("company_learning_evaluation")
    )
    if live:
        return live
    persisted = _read_json(
        persisted_path
        or report_path / "vitals" / "company_learning_evaluation.json"
    )
    if not persisted or persisted.get("available") is not True:
        return persisted
    try:
        registry = load_architecture_registry(DEFAULT_ARCHITECTURE_REGISTRY)
        state = CompanyLearningEvaluationState.model_validate(
            persisted.get("state")
        )
        manifest = InvariantEvidenceManifest.model_validate(
            persisted.get("evidence_manifest")
        )
        expected_run_id = _company_learning_run_id(
            bundle,
            report_path=report_path,
        )
        if state.scope.run_id != expected_run_id or manifest.run_id != expected_run_id:
            raise ValueError("persisted company-learning run identity mismatch")
        if manifest.architecture_digest != registry.digest:
            raise ValueError("persisted architecture digest is stale")
        if manifest.system_version != _company_learning_system_version(bundle):
            raise ValueError("persisted system version does not match report")
        expected_experiment = _company_learning_experiment_manifest_ref(
            bundle,
            run_id=expected_run_id,
        )
        if manifest.experiment_manifest_ref != expected_experiment:
            raise ValueError("persisted experiment manifest does not match report")
        evidence_ids = {row.invariant_id for row in manifest.evidence}
        if evidence_ids != ACTIVE_COMPANY_LEARNING_INVARIANT_IDS:
            raise ValueError("persisted active invariant population is incomplete")
        report_observation_ids = _company_learning_manifest_observation_ids(bundle)
        if set(state.scope.observation_ids) != report_observation_ids:
            raise ValueError("persisted observation population does not match report")
        expected_tenant = _coerce_uuid(
            _json_obj(bundle.get("run_summary")).get("tenant_id")
            or _json_obj(bundle.get("benchmark_summary")).get("tenant_id")
            or _json_obj(bundle.get("storyline_scores")).get("tenant_id")
        )
        if expected_tenant is None or state.scope.tenant_id != expected_tenant:
            raise ValueError("persisted tenant identity does not match report")
        cutoff = datetime.fromisoformat(
            str(persisted.get("evaluation_cutoff")).replace("Z", "+00:00")
        )
        if cutoff.tzinfo is None or cutoff.utcoffset() is None:
            raise ValueError("persisted evaluation cutoff is not timezone-aware")
        evidence_bundle = _company_learning_evidence_bundle(
            manifest,
            artifact_bundle=bundle,
            report_cutoff=cutoff.isoformat(),
        )
        saved_bundle = _json_obj(persisted.get("evidence_bundle"))
        saved_evidence_bundle = (
            InvariantEvidenceBundle.model_validate(saved_bundle)
            if saved_bundle
            else None
        )
        if saved_evidence_bundle is not None and (
            evidence_bundle is None
            or saved_evidence_bundle != evidence_bundle
        ):
            raise ValueError(
                "persisted company-learning evidence aggregation is stale"
            )
        proof = compile_invariant_proof_matrix(
            registry,
            run_id=expected_run_id,
            evidence=(
                evidence_bundle.evidence
                if evidence_bundle is not None
                else manifest.evidence
            ),
        )
        return {
            **persisted,
            "status": company_learning_assurance_status(state, proof),
            "observed_slice_health": state.observed_slice_health,
            "state": state.model_dump(mode="json"),
            "evidence_manifest": manifest.model_dump(mode="json"),
            "evidence_bundle": (
                saved_bundle
                if saved_evidence_bundle is not None
                else evidence_bundle.model_dump(mode="json")
                if evidence_bundle is not None
                else None
            ),
            "invariant_proof": proof.model_dump(mode="json"),
        }
    except Exception as exc:  # noqa: BLE001 - fail closed on persisted proof
        return {
            "available": False,
            "status": "unavailable",
            "error": "persisted_evaluation_invalid",
            "detail": f"{type(exc).__name__}: {str(exc)[:500]}",
            "proof_gaps": [
                "Saved company-learning evidence failed identity, schema or "
                "architecture-proof validation and was not reused."
            ],
        }


def _company_learning_run_id(
    bundle: dict[str, Any],
    *,
    report_path: Path,
) -> str:
    benchmark = _json_obj(bundle.get("benchmark_summary")) or _json_obj(
        bundle.get("storyline_scores")
    )
    run_summary = _json_obj(bundle.get("run_summary"))
    return str(
        run_summary.get("run_id")
        or benchmark.get("run_id")
        or report_path.name
    )


def _company_learning_manifest_observation_ids(
    bundle: dict[str, Any],
) -> set[UUID]:
    return {
        value
        for row in _json_list(bundle.get("signal_manifest"))
        if isinstance(row, dict)
        and (value := _coerce_uuid(row.get("observation_id"))) is not None
    }


def _company_learning_system_version(bundle: dict[str, Any]) -> str:
    for source in (
        _json_obj(bundle.get("run_config")),
        _json_obj(bundle.get("run_summary")),
        _json_obj(bundle.get("benchmark_summary")),
        _json_obj(bundle.get("storyline_scores")),
    ):
        for key in (
            "system_version",
            "git_commit",
            "commit_sha",
            "revision",
            "image_digest",
        ):
            value = source.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return "unreported-system-version"


def _company_learning_experiment_manifest_ref(
    bundle: dict[str, Any],
    *,
    run_id: str,
) -> str:
    digest = canonical_sha256(
        {
            "run_id": run_id,
            "run_config": _json_obj(bundle.get("run_config")),
            "planned_signals": _json_list(bundle.get("planned_signals")),
            "signal_manifest": _json_list(bundle.get("signal_manifest")),
        }
    )
    return f"company-learning-experiment:sha256:{digest}"


def _company_learning_evidence_bundle(
    manifest: InvariantEvidenceManifest,
    *,
    artifact_bundle: dict[str, Any],
    report_cutoff: str,
) -> InvariantEvidenceBundle | None:
    experiment = _json_obj(artifact_bundle.get("company_learning_experiment"))
    if experiment.get("valid") is not True:
        return None
    report = CorrectiveMemoryExperimentReport.model_validate(
        experiment.get("report")
    )
    experiment_manifest = (
        build_corrective_memory_experiment_evidence_manifest(
            report,
            architecture_digest=manifest.architecture_digest,
            experiment_manifest_ref=manifest.experiment_manifest_ref,
            report_cutoff=report_cutoff,
        )
    )
    return aggregate_invariant_evidence_manifests(
        (manifest, experiment_manifest)
    )


def _executed_scenario_ids(bundle: dict[str, Any]) -> frozenset[str]:
    experiment = _json_obj(bundle.get("company_learning_experiment"))
    if experiment.get("valid") is not True:
        return frozenset()
    report = _json_obj(experiment.get("report"))
    return frozenset(
        str(item)
        for item in _json_list(report.get("scenario_ids"))
        if str(item).strip()
    )


def _active_company_learning_proof_summary(
    proof: dict[str, Any],
) -> dict[str, Any]:
    records = [
        row
        for row in _json_list(proof.get("records"))
        if isinstance(row, dict)
        and str(row.get("invariant_id")) in ACTIVE_COMPANY_LEARNING_INVARIANT_IDS
    ]
    return {
        "architecture_digest": proof.get("architecture_digest"),
        "active_invariant_count": len(records),
        "state_counts": dict(
            sorted(
                {
                    state: sum(
                        str(row.get("substantiation_state")) == state
                        for row in records
                    )
                    for state in {
                        str(row.get("substantiation_state"))
                        for row in records
                    }
                }.items()
            )
        ),
        "confirmed_incident_count": sum(
            _as_int(row.get("confirmed_incident_count"))
            for row in records
        ),
        "violation_count": sum(
            _as_int(row.get("violation_count"))
            for row in records
        ),
        "records": records,
    }


def _company_physics_section(
    evaluation: dict[str, Any] | None,
    *,
    experiment: dict[str, Any] | None = None,
) -> dict[str, Any]:
    evaluation = _json_obj(evaluation)
    experiment_summary = _corrective_memory_experiment_summary(experiment)
    experiment_gaps = {
        str(item)
        for item in _json_list(experiment_summary.get("proof_gaps"))
        if str(item).strip()
    }
    experiment_failures = [
        str(item)
        for item in _json_list(experiment_summary.get("hard_failures"))
        if str(item).strip()
    ]
    experiments = (
        {"corrective_memory_recurrence": experiment_summary}
        if experiment_summary
        else {}
    )
    state = _json_obj(evaluation.get("state"))
    if evaluation.get("available") is not True or not state:
        gaps = {
            str(item)
            for item in _json_list(evaluation.get("proof_gaps"))
            if str(item).strip()
        }
        if not gaps:
            gaps = {
                "DB-backed company-physics evaluation was not collected; "
                "context, grounding and source-semantic state are unknown."
            }
        gaps.update(experiment_gaps)
        return {
            "status": str(evaluation.get("status") or "not_observed"),
            "noncompensatory": True,
            "learning_loop": {},
            "components": {},
            "incident_counts": {},
            "hard_failures": experiment_failures,
            "proof_gaps": sorted(gaps),
            "error": evaluation.get("error"),
            "experiments": experiments,
        }
    incidents = _json_obj(state.get("incident_counts"))
    status = str(evaluation.get("status") or "insufficient")
    proof_summary = _active_company_learning_proof_summary(
        _json_obj(evaluation.get("invariant_proof"))
    )
    hard_failures = [
        f"company-physics incident {name}: count={count}"
        for name, count in incidents.items()
        if _as_int(count) > 0
    ]
    if (
        status == "contradicted"
        and not hard_failures
        and (
            _as_int(proof_summary.get("confirmed_incident_count")) > 0
            or _as_int(proof_summary.get("violation_count")) > 0
        )
    ):
        hard_failures.append(
            "company-physics registered invariant proof is contradicted"
        )
    proof_gaps = {
        str(item)
        for item in _json_list(state.get("proof_gaps"))
        if str(item).strip()
    }
    for row in _json_list(proof_summary.get("records")):
        if not isinstance(row, dict):
            continue
        invariant_id = str(row.get("invariant_id") or "unknown")
        proof_gaps.update(
            f"{invariant_id}: {gap}"
            for gap in _json_list(row.get("proof_gaps"))
            if str(gap).strip()
        )
    hard_failures.extend(experiment_failures)
    proof_gaps.update(experiment_gaps)
    return {
        "status": status,
        "observed_slice_health": str(
            evaluation.get("observed_slice_health")
            or state.get("observed_slice_health")
            or "unknown"
        ),
        "noncompensatory": True,
        "evaluation_cutoff": evaluation.get("evaluation_cutoff"),
        "scope": _json_obj(state.get("scope")),
        "learning_loop": _json_obj(state.get("learning_loop")),
        "components": {
            "conversation_context": _json_obj(
                state.get("conversation_context")
            ),
            "entity_grounding": _json_obj(state.get("entity_grounding")),
            "source_semantics": _json_obj(state.get("source_semantics")),
        },
        "incident_counts": incidents,
        "hard_failures": hard_failures,
        "proof_gaps": sorted(proof_gaps),
        "artifact_refs": _json_list(state.get("artifact_refs")),
        "invariant_proof": proof_summary,
        "experiments": experiments,
    }


def _corrective_memory_experiment_summary(
    experiment: dict[str, Any] | None,
) -> dict[str, Any]:
    experiment = _json_obj(experiment)
    if not experiment:
        return {}
    if experiment.get("valid") is not True:
        gap = (
            "Typed corrective-memory experiment evidence failed validation and "
            "cannot credit scenario execution."
        )
        detail = str(experiment.get("detail") or "").strip()
        return {
            "available": False,
            "status": "invalid",
            "source_path": experiment.get("source_path"),
            "error": experiment.get("error") or "experiment_artifact_invalid",
            "detail": detail or None,
            "scenario_ids": [],
            "metrics": {},
            "hard_safety_incident_count": 0,
            "hard_failures": [],
            "proof_gaps": [gap],
        }
    report = _json_obj(experiment.get("report"))
    incidents = _json_list(report.get("incidents"))
    incident_class_counts: dict[str, int] = {}
    for incident in incidents:
        if not isinstance(incident, dict):
            continue
        name = str(incident.get("incident_class") or "unknown")
        incident_class_counts[name] = incident_class_counts.get(name, 0) + 1
    hard_failures = (
        [
            "corrective-memory recurrence experiment recorded "
            f"{len(incidents)} hard safety incident(s)"
        ]
        if incidents
        else []
    )
    return {
        "available": True,
        "status": report.get("status"),
        "source_path": experiment.get("source_path"),
        "experiment_id": report.get("experiment_id"),
        "run_id": report.get("run_id"),
        "system_version": report.get("system_version"),
        "scenario_ids": _json_list(report.get("scenario_ids")),
        "report_digest": experiment.get("report_digest"),
        "spec_digest": report.get("spec_digest"),
        "case_manifest_digest": report.get("case_manifest_digest"),
        "gold_digest": report.get("gold_digest"),
        "arm_assignment_digest": report.get("arm_assignment_digest"),
        "pair_results_digest": report.get("pair_results_digest"),
        "metrics": _json_obj(report.get("metrics")),
        "hard_safety_incident_count": len(incidents),
        "incident_class_counts": dict(sorted(incident_class_counts.items())),
        "hard_failures": hard_failures,
        "proof_gaps": _json_list(report.get("proof_gaps")),
        "artifact_refs": _json_list(report.get("artifact_refs")),
    }


def render_vitals_markdown(scorecard: dict[str, Any]) -> str:
    """Render a compact operator-facing vitals summary."""
    lines = [
        "# Company Understanding Vitals",
        "",
        f"- Run: `{scorecard.get('run_id')}`",
        f"- Status: **{scorecard.get('status')}**",
        f"- Overall score: {_fmt_score(scorecard.get('overall_score'))}",
        (
            f"- Score coverage: {scorecard.get('scored_vitals')}/"
            f"{scorecard.get('total_vitals')} vitals"
        ),
        "",
    ]
    hard_failures = _json_list(scorecard.get("hard_failures"))
    if hard_failures:
        lines.append("## Hard Failures")
        lines.extend(f"- {failure}" for failure in hard_failures)
        lines.append("")

    findings = _json_list(scorecard.get("ranked_findings"))
    if findings:
        lines.append("## Ranked Findings")
        for item in findings[:12]:
            if isinstance(item, dict):
                lines.append(
                    f"- [{item.get('severity', 'info')}] "
                    f"{item.get('vital', 'run')}: {item.get('finding')}"
                )
        lines.append("")

    company_physics = _json_obj(scorecard.get("company_physics"))
    lines.extend(
        [
            "## Company Physics",
            "",
            f"- Status: **{company_physics.get('status', 'not_observed')}**",
        ]
    )
    metrics = _json_obj(company_physics.get("learning_loop"))
    if metrics:
        lines.extend(
            [
                (
                    "- Governed alias replay: "
                    f"{metrics.get('governed_alias_replays_resolved')}/"
                    f"{metrics.get('governed_alias_replay_exposures')}"
                ),
                (
                    "- Grounding to interpretation: "
                    f"{_fmt_score(metrics.get('grounding_to_interpretation_coverage'))}"
                ),
                (
                    "- Exactly-one-Model rate: "
                    f"{_fmt_score(metrics.get('one_model_cardinality_rate'))}"
                ),
            ]
        )
    experiment = _json_obj(
        _json_obj(company_physics.get("experiments")).get(
            "corrective_memory_recurrence"
        )
    )
    if experiment:
        experiment_metrics = _json_obj(experiment.get("metrics"))
        lines.extend(
            [
                (
                    "- Corrective-memory recurrence: "
                    f"{experiment.get('status', 'invalid')}"
                ),
                (
                    "- Adaptive vs frozen correctness: "
                    f"{_fmt_score(experiment_metrics.get('adaptive_correctness_rate'))}"
                    " vs "
                    f"{_fmt_score(experiment_metrics.get('frozen_correctness_rate'))}"
                ),
                (
                    "- Adaptive correctness lift: "
                    f"{_fmt_score(experiment_metrics.get('adaptive_minus_frozen_correctness'))}"
                ),
            ]
        )
    lines.append("")

    lines.extend(
        [
            "## Vitals",
            "",
            "| Vital | Score | Status | Key Metrics |",
            "| --- | ---: | --- | --- |",
        ]
    )
    for name, vital in _json_obj(scorecard.get("vitals")).items():
        metrics = _short_metrics(_json_obj(vital.get("metrics")))
        lines.append(
            f"| `{name}` | {_fmt_score(vital.get('score'))} | "
            f"{vital.get('status')} | {metrics} |"
        )

    proof_gaps = _json_list(scorecard.get("proof_gaps"))
    if proof_gaps:
        lines.extend(["", "## Proof Gaps"])
        lines.extend(f"- {gap}" for gap in proof_gaps)

    return "\n".join(lines).rstrip() + "\n"


def _load_artifact_bundle(report_dir: Path) -> dict[str, Any]:
    bundle = {
        "report_dir": report_dir,
        "run_summary": _read_json(report_dir / "run_summary.json"),
        "benchmark_summary": _read_json(report_dir / "benchmark_summary.json"),
        "storyline_scores": _read_json(report_dir / "storyline_scores.json"),
        "run_config": _read_json(report_dir / "run_config.json"),
        "waves": _read_json(report_dir / "waves.json"),
        "planned_signals": _read_jsonl(report_dir / "planned_signals.jsonl"),
        "signal_manifest": _read_jsonl(report_dir / "signal_manifest.jsonl"),
        "models": _read_jsonl(report_dir / "models.jsonl"),
        "model_edges": _read_jsonl(report_dir / "model_edges.jsonl"),
    }
    bundle["company_learning_experiment"] = (
        _company_learning_experiment_for_report(
            report_dir,
            bundle=bundle,
        )
    )
    return bundle


def _company_learning_experiment_for_report(
    report_dir: Path,
    *,
    bundle: dict[str, Any],
) -> dict[str, Any]:
    source_path = next(
        (
            path
            for path in (
                report_dir / "company_learning_scenario_evidence.json",
                report_dir / "vitals" / "company_learning_scenario_evidence.json",
            )
            if path.exists()
        ),
        None,
    )
    if source_path is None:
        return {}
    try:
        payload = json.loads(source_path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("experiment artifact root must be a JSON object")
        spec = CorrectiveMemoryExperimentSpec.model_validate(payload.get("spec"))
        saved_report = CorrectiveMemoryExperimentReport.model_validate(
            payload.get("report")
        )
        supplied_report_digest = str(payload.get("report_digest") or "")
        recomputed_report = evaluate_corrective_memory_experiment(
            spec=spec,
            pairs=saved_report.pairs,
            artifact_refs=saved_report.artifact_refs,
        )
        if saved_report.model_dump(mode="json") != recomputed_report.model_dump(
            mode="json"
        ):
            raise ValueError(
                "saved corrective-memory report does not match recomputation"
            )
        if supplied_report_digest != recomputed_report.digest:
            raise ValueError("corrective-memory report digest mismatch")
        expected_run_id = _company_learning_run_id(
            bundle,
            report_path=report_dir,
        )
        if spec.run_id != expected_run_id or saved_report.run_id != expected_run_id:
            raise ValueError(
                "corrective-memory experiment run identity does not match report"
            )
        expected_system_version = _company_learning_system_version(bundle)
        if (
            spec.system_version != expected_system_version
            or saved_report.system_version != expected_system_version
        ):
            raise ValueError(
                "corrective-memory experiment system version does not match report"
            )
        canonical_payload = {
            "spec": spec.model_dump(mode="json"),
            "report": recomputed_report.model_dump(mode="json"),
            "report_digest": recomputed_report.digest,
        }
        return {
            "available": True,
            "valid": True,
            "source_path": str(source_path),
            "spec": canonical_payload["spec"],
            "report": canonical_payload["report"],
            "report_digest": canonical_payload["report_digest"],
            "canonical_payload": canonical_payload,
        }
    except Exception as exc:  # noqa: BLE001 - untrusted supporting artifact
        return {
            "available": True,
            "valid": False,
            "source_path": str(source_path),
            "error": "experiment_artifact_invalid",
            "detail": f"{type(exc).__name__}: {str(exc)[:500]}",
        }


def apply_db_trace_to_signal_rows(
    signal_rows: list[dict[str, Any]],
    db_trace: dict[str, Any],
) -> list[dict[str, Any]]:
    """Merge DB-backed lineage into artifact-derived signal rows."""
    by_observation = _json_obj(db_trace.get("by_observation"))
    enriched: list[dict[str, Any]] = []
    for row in signal_rows:
        obs_id = str(row.get("observation_id") or "")
        trace = _json_obj(by_observation.get(obs_id))
        if not obs_id or not trace:
            enriched.append(dict(row))
            continue
        merged = dict(row)
        trigger_rows = _json_list(trace.get("triggers"))
        think_rows = _json_list(trace.get("think_runs"))
        model_rows = _json_list(trace.get("models"))
        reading_rows = _json_list(trace.get("model_signal_readings"))
        edge_rows = _json_list(trace.get("model_edges"))
        relation_claim_rows = _json_list(trace.get("relation_claims"))
        relation_instance_rows = _json_list(trace.get("relation_instances"))
        model_event_rows = _json_list(trace.get("model_events"))
        projection_rows = _json_list(trace.get("projection_snapshots"))
        inquiry_rows = _json_list(trace.get("inquiry_sessions"))
        inquiry_evidence_rows = _json_list(trace.get("inquiry_evidence_items"))
        omitted_rows = _json_list(trace.get("omitted_evidence"))
        outcome_rows = _json_list(trace.get("inquiry_outcome_events"))
        post_commit_rows = _json_list(trace.get("post_commit_actions"))
        routing_rows = _json_list(trace.get("routing_decisions"))
        residual_rows = _json_list(trace.get("model_residual_evidence"))
        latent_gap_rows = _json_list(trace.get("sage_latent_gap_hypotheses"))

        merged.update(
            {
                "trigger_ids": _ids(trigger_rows),
                "think_run_ids": _ids(think_rows),
                "retrieved_model_ids": _context_ids(think_rows, "selected_model_ids"),
                "retrieved_observation_ids": _context_ids(
                    think_rows,
                    "selected_observation_ids",
                ),
                "referenced_model_ids": _context_ids(think_rows, "referenced_model_ids"),
                "referenced_observation_ids": _context_ids(
                    think_rows,
                    "referenced_observation_ids",
                ),
                "applied_model_ids": _ids(model_rows, key="id"),
                "applied_edge_ids": _ids(edge_rows, key="id"),
                "relation_claim_ids": _ids(relation_claim_rows, key="id"),
                "relation_instance_ids": _ids(relation_instance_rows, key="id"),
                "model_event_ids": _ids(model_event_rows, key="id"),
                "persisted_residual_ids": _ids(residual_rows, key="id"),
                "persisted_residual_statuses": sorted(
                    {
                        str(residual.get("status") or "unknown")
                        for residual in residual_rows
                        if isinstance(residual, dict)
                    }
                ),
                "persisted_residuals": residual_rows,
                "persisted_latent_gap_ids": _ids(latent_gap_rows, key="id"),
                "persisted_latent_gap_hypotheses": latent_gap_rows,
                "projection_subjects": _projection_subjects(projection_rows),
                "product_surface_refs": _product_surface_refs(outcome_rows),
                "db_trace": {
                    "trigger_count": len(trigger_rows),
                    "think_run_count": len(think_rows),
                    "model_count": len(model_rows),
                    "model_signal_reading_count": len(reading_rows),
                    "edge_count": len(edge_rows),
                    "relation_claim_count": len(relation_claim_rows),
                    "relation_instance_count": len(relation_instance_rows),
                    "model_event_count": len(model_event_rows),
                    "projection_snapshot_count": len(projection_rows),
                    "inquiry_session_count": len(inquiry_rows),
                    "inquiry_evidence_item_count": len(inquiry_evidence_rows),
                    "omitted_evidence_count": len(omitted_rows),
                    "inquiry_outcome_event_count": len(outcome_rows),
                    "post_commit_action_count": len(post_commit_rows),
                    "routing_decision_count": len(routing_rows),
                    "model_residual_evidence_count": len(residual_rows),
                    "sage_latent_gap_hypothesis_count": len(latent_gap_rows),
                },
            }
        )
        final_fate, fate_reasons, leak_flags = classify_signal_fate(trace)
        merged["final_fate"] = final_fate
        merged["fate_reasons"] = fate_reasons
        merged["leak_flags"] = leak_flags
        enriched.append(merged)
    return enriched


def classify_signal_fate(trace: dict[str, Any]) -> tuple[str, list[str], list[str]]:
    """Classify one observation's deepest proven fate."""
    trigger_rows = _json_list(trace.get("triggers"))
    think_rows = _json_list(trace.get("think_runs"))
    model_rows = _json_list(trace.get("models"))
    reading_rows = _json_list(trace.get("model_signal_readings"))
    edge_rows = _json_list(trace.get("model_edges"))
    relation_claim_rows = _json_list(trace.get("relation_claims"))
    relation_instance_rows = _json_list(trace.get("relation_instances"))
    projection_rows = _json_list(trace.get("projection_snapshots"))
    inquiry_rows = _json_list(trace.get("inquiry_sessions"))
    outcome_rows = _json_list(trace.get("inquiry_outcome_events"))
    routing_rows = _json_list(trace.get("routing_decisions"))
    reasons: list[str] = []
    leaks: list[str] = []

    if not trigger_rows and not routing_rows and not inquiry_rows:
        return "no_think_trigger", ["no trigger, route, or inquiry row found"], [
            "no_think_trigger"
        ]

    if trigger_rows:
        pending = [
            row for row in trigger_rows
            if not row.get("completed_at") and not _json_list(trace.get("think_runs"))
        ]
        if pending:
            leaks.append("trigger_pending")
            reasons.append(f"{len(pending)} trigger row(s) have no Think run.")

    failed_think = [
        row for row in think_rows if str(row.get("status") or "") == "failed"
    ]
    successful_think = [
        row for row in think_rows
        if str(row.get("status") or "") in {"success", "skipped_idempotent"}
    ]
    if failed_think and not successful_think:
        return "think_failed", [f"{len(failed_think)} Think run(s) failed."], [
            "think_failed"
        ]

    if model_rows:
        born = [row for row in model_rows if row.get("provenance") == "born_from_event"]
        supported = [
            row for row in model_rows if row.get("provenance") == "supporting_event"
        ]
        if born:
            reasons.append(f"{len(born)} model(s) born from this observation.")
        if supported:
            reasons.append(f"{len(supported)} model(s) cite this observation as support.")

    if reading_rows:
        kinds = sorted({str(row.get("reading_kind") or "") for row in reading_rows})
        reasons.append(f"model signal readings: {', '.join(kinds)}.")

    if edge_rows:
        reasons.append(f"{len(edge_rows)} model edge(s) cite this observation.")
    if relation_claim_rows:
        reasons.append(f"{len(relation_claim_rows)} relation claim(s) cite this observation.")
    if relation_instance_rows:
        reasons.append(
            f"{len(relation_instance_rows)} relation instance(s) cite this observation."
        )
    if projection_rows:
        reasons.append(
            f"{len(projection_rows)} projection snapshot(s) depend on this observation's model/event lineage."
        )
    if outcome_rows:
        event_types = sorted({str(row.get("event_type") or "") for row in outcome_rows})
        reasons.append(f"inquiry outcome events: {', '.join(event_types)}.")

    if any(str(row.get("event_type") or "").startswith("recommendation_") for row in outcome_rows):
        return "decision_outcome_recorded", reasons, leaks
    if outcome_rows:
        return "self_improvement_event_created", reasons, leaks
    if projection_rows:
        return "projection_updated", reasons, leaks
    if relation_instance_rows:
        return "relation_frame_created", reasons, leaks
    if edge_rows or relation_claim_rows:
        return "edge_created", reasons, leaks
    if any(str(row.get("reading_kind") or "") == "falsify" for row in reading_rows):
        return "falsifier_created", reasons, leaks
    if any(str(row.get("reading_kind") or "") == "contest" for row in reading_rows):
        return "counterevidence_attached", reasons, leaks
    if reading_rows:
        return "evidence_attached", reasons, leaks
    if model_rows:
        if any(row.get("provenance") == "born_from_event" for row in model_rows):
            return "model_created", reasons, leaks
        return "model_updated", reasons, leaks
    if any(str(row.get("stop_status") or "") == "human_validation_required" for row in inquiry_rows):
        return "human_feedback_requested", reasons, leaks
    if successful_think:
        grades = {
            str(_json_obj(_json_obj(row.get("ops_applied")).get("context_use")).get("context_use_grade") or "")
            for row in successful_think
        }
        if "justified_noop_context_used" in grades:
            return "think_noop_justified", reasons or ["successful justified no-op"], leaks
        leaks.append("think_success_without_durable_trace")
        return "think_noop_suspicious", reasons or ["Think succeeded without durable trace."], leaks
    if trigger_rows:
        return "trigger_pending", reasons or ["trigger exists but no completion trace"], leaks
    return "raw_only_unmodeled", reasons or ["observation has no durable trace"], [
        *leaks,
        "raw_only_unmodeled",
    ]


async def _collect_db_trace(
    conn: Any,
    *,
    tenant_id: str,
    observation_ids: list[str],
) -> dict[str, Any]:
    obs_uuids = [UUID(value) for value in observation_ids if _coerce_uuid(value)]
    obs_text = [str(value) for value in obs_uuids]
    by_obs: dict[str, dict[str, list[dict[str, Any]]]] = {
        str(obs_id): {} for obs_id in obs_uuids
    }
    table_presence: dict[str, bool] = {}

    async def has_table(table: str) -> bool:
        exists = await _table_exists(conn, table)
        table_presence[table] = exists
        return exists

    if await has_table("think_trigger_queue"):
        trigger_rows = await conn.fetch(
            """
            SELECT id, observation_id, trigger_kind, trigger_subkind, payload,
                   enqueued_at, scheduled_for, attempts, completed_at, locked_at
            FROM think_trigger_queue q
            WHERE tenant_id = $1::uuid
              AND (
                observation_id = ANY($2::uuid[])
                OR EXISTS (
                  SELECT 1
                  FROM jsonb_array_elements_text(
                    COALESCE(q.payload->'batch_observation_ids', '[]'::jsonb)
                  ) AS batch_obs(value)
                  WHERE batch_obs.value = ANY($3::text[])
                )
              )
            """,
            tenant_id,
            obs_uuids,
            obs_text,
        )
        trigger_to_obs: dict[str, set[str]] = {}
        for raw in trigger_rows:
            row = _record_to_dict(raw)
            trigger_id = str(row.get("id"))
            related = _trigger_observation_ids(row, obs_text)
            if not related and row.get("observation_id"):
                related = [str(row["observation_id"])]
            for obs_id in related:
                _append_trace(by_obs, obs_id, "triggers", row)
                trigger_to_obs.setdefault(trigger_id, set()).add(obs_id)
    else:
        trigger_to_obs = {}

    trigger_ids = [UUID(value) for value in trigger_to_obs if _coerce_uuid(value)]
    think_run_to_obs: dict[str, set[str]] = {}
    if trigger_ids and await has_table("think_runs"):
        think_rows = await conn.fetch(
            """
            SELECT id, trigger_id, trigger_kind, started_at, ended_at, status,
                   error, retrieval_model_count, retrieval_observation_count,
                   llm_latency_ms, validation_error_count, ops_applied,
                   cascade_depth
            FROM think_runs
            WHERE tenant_id = $1::uuid AND trigger_id = ANY($2::uuid[])
            ORDER BY started_at
            """,
            tenant_id,
            trigger_ids,
        )
        for raw in think_rows:
            row = _record_to_dict(raw)
            run_id = str(row.get("id"))
            for obs_id in trigger_to_obs.get(str(row.get("trigger_id")), set()):
                _append_trace(by_obs, obs_id, "think_runs", row)
                think_run_to_obs.setdefault(run_id, set()).add(obs_id)
    else:
        table_presence.setdefault("think_runs", False)

    model_to_obs: dict[str, set[str]] = {}
    if await has_table("models"):
        model_rows = await conn.fetch(
            """
            SELECT id, born_from_event_id, supporting_event_ids, status,
                   "natural", proposition, scope_actors, scope_entities,
                   confidence, activation, falsifier, created_at,
                   last_retrieved_at, retrieval_count
            FROM models
            WHERE tenant_id = $1::uuid
              AND (
                born_from_event_id = ANY($2::uuid[])
                OR supporting_event_ids && $2::uuid[]
              )
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in model_rows:
            row = _record_to_dict(raw)
            related = set()
            if str(row.get("born_from_event_id")) in by_obs:
                row["provenance"] = "born_from_event"
                related.add(str(row["born_from_event_id"]))
            for obs_id in _uuid_strings(row.get("supporting_event_ids")):
                if obs_id in by_obs:
                    related.add(obs_id)
                    row.setdefault("provenance", "supporting_event")
            for obs_id in related:
                _append_trace(by_obs, obs_id, "models", row)
                model_to_obs.setdefault(str(row.get("id")), set()).add(obs_id)
    else:
        table_presence.setdefault("models", False)

    if await has_table("model_signal_readings"):
        rows = await conn.fetch(
            """
            SELECT id, model_id, source_event_id, reading_kind, observed_at, detail
            FROM model_signal_readings
            WHERE tenant_id = $1::uuid AND source_event_id = ANY($2::uuid[])
            ORDER BY observed_at
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            obs_id = str(row.get("source_event_id"))
            _append_trace(by_obs, obs_id, "model_signal_readings", row)
            model_to_obs.setdefault(str(row.get("model_id")), set()).add(obs_id)

    if await has_table("model_edges"):
        has_evidence_events = await _column_exists(conn, "model_edges", "evidence_event_ids")
        evidence_select = (
            "evidence_event_ids"
            if has_evidence_events
            else "'{}'::uuid[] AS evidence_event_ids"
        )
        evidence_condition = (
            "OR evidence_event_ids && $2::uuid[]"
            if has_evidence_events
            else ""
        )
        rows = await conn.fetch(
            f"""
            SELECT id, source_model_id, target_model_id, edge_kind, status,
                   detected_by, created_by_event_id, {evidence_select},
                   created_at, metadata
            FROM model_edges
            WHERE tenant_id = $1::uuid
              AND (
                created_by_event_id = ANY($2::uuid[])
                {evidence_condition}
              )
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            related = set()
            if str(row.get("created_by_event_id")) in by_obs:
                related.add(str(row["created_by_event_id"]))
            related.update(obs_id for obs_id in _uuid_strings(row.get("evidence_event_ids")) if obs_id in by_obs)
            for obs_id in related:
                _append_trace(by_obs, obs_id, "model_edges", row)

    if await has_table("relation_claims"):
        rows = await conn.fetch(
            """
            SELECT id, source_observation_id, think_run_id, source_model_id,
                   target_model_id, predicate, edge_kind, status, write_policy,
                   endpoint_binding_status, evidence_event_ids, evidence_model_ids,
                   accepted_edge_ids, confidence, created_at, updated_at
            FROM relation_claims
            WHERE tenant_id = $1::uuid
              AND (
                source_observation_id = ANY($2::uuid[])
                OR evidence_event_ids && $2::uuid[]
              )
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            related = set()
            if str(row.get("source_observation_id")) in by_obs:
                related.add(str(row["source_observation_id"]))
            related.update(obs_id for obs_id in _uuid_strings(row.get("evidence_event_ids")) if obs_id in by_obs)
            for obs_id in related:
                _append_trace(by_obs, obs_id, "relation_claims", row)

    if await has_table("relation_instances"):
        rows = await conn.fetch(
            """
            SELECT id, source_observation_id, think_run_id, relation_kind,
                   status, participant_binding_status, write_policy,
                   evidence_event_ids, evidence_model_ids, confidence,
                   created_at, updated_at
            FROM relation_instances
            WHERE tenant_id = $1::uuid
              AND (
                source_observation_id = ANY($2::uuid[])
                OR evidence_event_ids && $2::uuid[]
              )
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            related = set()
            if str(row.get("source_observation_id")) in by_obs:
                related.add(str(row["source_observation_id"]))
            related.update(obs_id for obs_id in _uuid_strings(row.get("evidence_event_ids")) if obs_id in by_obs)
            for obs_id in related:
                _append_trace(by_obs, obs_id, "relation_instances", row)

    model_ids = [UUID(value) for value in model_to_obs if _coerce_uuid(value)]
    model_event_to_obs: dict[str, set[str]] = {}
    if await has_table("model_events"):
        rows = await conn.fetch(
            """
            SELECT id, model_id, event_type, changed_fields, proposition_kind,
                   claim_role, domain_tags, scope_entities, source_event_id,
                   created_at
            FROM model_events
            WHERE tenant_id = $1::uuid
              AND (
                source_event_id = ANY($2::uuid[])
                OR model_id = ANY($3::uuid[])
              )
            ORDER BY created_at
            """,
            tenant_id,
            obs_uuids,
            model_ids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            related = set(model_to_obs.get(str(row.get("model_id")), set()))
            if str(row.get("source_event_id")) in by_obs:
                related.add(str(row["source_event_id"]))
            for obs_id in related:
                _append_trace(by_obs, obs_id, "model_events", row)
                model_event_to_obs.setdefault(str(row.get("id")), set()).add(obs_id)

    model_event_ids = [
        UUID(value) for value in model_event_to_obs if _coerce_uuid(value)
    ]
    if (model_ids or model_event_ids) and await has_table("projection_snapshots"):
        rows = await conn.fetch(
            """
            SELECT projection_name, projection_version, subject_key,
                   confidence, severity, source_model_ids, source_event_ids,
                   updated_at
            FROM projection_snapshots
            WHERE tenant_id = $1::uuid
              AND (
                source_model_ids && $2::uuid[]
                OR source_event_ids && $3::uuid[]
              )
            """,
            tenant_id,
            model_ids,
            model_event_ids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            related: set[str] = set()
            for model_id in _uuid_strings(row.get("source_model_ids")):
                related.update(model_to_obs.get(model_id, set()))
            for event_id in _uuid_strings(row.get("source_event_ids")):
                related.update(model_event_to_obs.get(event_id, set()))
            for obs_id in related:
                _append_trace(by_obs, obs_id, "projection_snapshots", row)

    session_to_obs: dict[str, set[str]] = {}
    if await has_table("signal_routing_decisions"):
        rows = await conn.fetch(
            """
            SELECT id, signal_ref_id, route, decision_status, score,
                   risk_level, sensitivity, reason, enqueued_trigger_id,
                   think_run_id, created_at
            FROM signal_routing_decisions
            WHERE tenant_id = $1::uuid
              AND signal_ref_type = 'observation'
              AND signal_ref_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            _append_trace(by_obs, str(row.get("signal_ref_id")), "routing_decisions", row)

    if await has_table("inquiry_sessions"):
        rows = await conn.fetch(
            """
            SELECT id, signal_ref_id, route, status, stop_status,
                   round_count, question_count, evidence_count, think_run_id,
                   created_at, completed_at
            FROM inquiry_sessions
            WHERE tenant_id = $1::uuid
              AND signal_ref_type = 'observation'
              AND signal_ref_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            obs_id = str(row.get("signal_ref_id"))
            _append_trace(by_obs, obs_id, "inquiry_sessions", row)
            session_to_obs.setdefault(str(row.get("id")), set()).add(obs_id)

    session_ids = [UUID(value) for value in session_to_obs if _coerce_uuid(value)]
    if session_ids and await has_table("inquiry_evidence_items"):
        rows = await conn.fetch(
            """
            SELECT id, session_id, source_type, source_ref, source_ref_id,
                   trust_tier, retrieval_paths, score, created_at
            FROM inquiry_evidence_items
            WHERE tenant_id = $1::uuid AND session_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            session_ids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            for obs_id in session_to_obs.get(str(row.get("session_id")), set()):
                _append_trace(by_obs, obs_id, "inquiry_evidence_items", row)

    if session_ids and await has_table("omitted_evidence"):
        rows = await conn.fetch(
            """
            SELECT id, inquiry_session_id, question_id, source_type,
                   source_ref, source_ref_id, retrieval_paths,
                   omission_reason, score, created_at
            FROM omitted_evidence
            WHERE tenant_id = $1::uuid AND inquiry_session_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            session_ids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            for obs_id in session_to_obs.get(str(row.get("inquiry_session_id")), set()):
                _append_trace(by_obs, obs_id, "omitted_evidence", row)

    if session_ids and await has_table("inquiry_outcome_events"):
        rows = await conn.fetch(
            """
            SELECT id, inquiry_session_id, event_type, payload, created_at
            FROM inquiry_outcome_events
            WHERE tenant_id = $1::uuid AND inquiry_session_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            session_ids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            for obs_id in session_to_obs.get(str(row.get("inquiry_session_id")), set()):
                _append_trace(by_obs, obs_id, "inquiry_outcome_events", row)

    if await has_table("model_residual_evidence"):
        rows = await conn.fetch(
            """
            SELECT id, source_observation_id, think_run_id, trigger_id, model_id,
                   residual_kind, compact_summary, reason, status,
                   absorption_object_kind, absorption_object_id, metadata,
                   created_at, updated_at, resolved_at
            FROM model_residual_evidence
            WHERE tenant_id = $1::uuid
              AND source_observation_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            obs_id = str(row.get("source_observation_id"))
            _append_trace(by_obs, obs_id, "model_residual_evidence", row)

    if await has_table("sage_latent_gap_hypotheses"):
        rows = await conn.fetch(
            """
            SELECT id, gap_kind, status, residual_cluster_hash,
                   supporting_residual_ids, supporting_observation_ids,
                   missing_evidence_statement, falsifier, next_evidence_needed,
                   confidence, hypothesis_text, metadata,
                   resolution_object_kind, resolution_object_id, resolution_reason,
                   created_at, updated_at, resolved_at
            FROM sage_latent_gap_hypotheses
            WHERE tenant_id = $1::uuid
              AND supporting_observation_ids && $2::uuid[]
            ORDER BY created_at
            """,
            tenant_id,
            obs_uuids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            for obs_id in _json_list(row.get("supporting_observation_ids")):
                _append_trace(by_obs, str(obs_id), "sage_latent_gap_hypotheses", row)

    if trigger_ids and await has_table("pending_post_commit_actions"):
        rows = await conn.fetch(
            """
            SELECT id, trigger_id, action_kind, action_payload, created_at,
                   processed_at, attempts, last_error, dead_lettered_at
            FROM pending_post_commit_actions
            WHERE tenant_id = $1::uuid AND trigger_id = ANY($2::uuid[])
            ORDER BY created_at
            """,
            tenant_id,
            trigger_ids,
        )
        for raw in rows:
            row = _record_to_dict(raw)
            for obs_id in trigger_to_obs.get(str(row.get("trigger_id")), set()):
                _append_trace(by_obs, obs_id, "post_commit_actions", row)

    return {
        "available": True,
        "tenant_id": tenant_id,
        "observation_count": len(obs_uuids),
        "by_observation": by_obs,
        "table_presence": table_presence,
        "summary": _db_trace_counts(by_obs),
    }


def _metabolism_vital(
    signal_rows: list[dict[str, Any]],
    bundle: dict[str, Any],
) -> dict[str, Any]:
    planned = len(bundle["planned_signals"])
    manifested = len(bundle["signal_manifest"])
    observed = sum(1 for row in signal_rows if row.get("observation_id"))
    trace_resolved = sum(1 for row in signal_rows if row.get("final_fate") != "trace_unresolved")
    valuable = sum(1 for row in signal_rows if row.get("gold_value_class") != "noise")
    leakage = sum(1 for row in signal_rows if row.get("leak_flags"))
    fate_counts: dict[str, int] = {}
    for row in signal_rows:
        fate = str(row.get("final_fate") or "unknown")
        fate_counts[fate] = fate_counts.get(fate, 0) + 1
    useful_valuable = sum(
        1
        for row in signal_rows
        if row.get("gold_value_class") != "noise"
        and row.get("final_fate") in VALUABLE_SIGNAL_FATES
    )
    healthy_noise = sum(
        1
        for row in signal_rows
        if row.get("gold_value_class") == "noise"
        and row.get("final_fate") == "noise_correctly_ignored"
    )
    noise = sum(1 for row in signal_rows if row.get("gold_value_class") == "noise")
    trace_coverage = _ratio(trace_resolved, len(signal_rows))
    useful_yield = _ratio(useful_valuable, valuable)
    noise_score = _ratio(healthy_noise, noise) if noise else 1.0
    if trace_resolved:
        score = (0.65 * useful_yield) + (0.25 * trace_coverage) + (0.10 * noise_score)
        findings = [
            f"DB-enriched trace resolved {trace_resolved}/{len(signal_rows)} signal fates.",
            f"Useful fate yield is {round(useful_yield, 4)} for non-noise signals.",
        ]
    else:
        score = None
        findings = [
            "Artifact-only vitals can verify signal observation coverage, but cannot yet prove per-signal durable fate."
        ]
    proof_gaps = []
    if planned or manifested:
        if trace_resolved:
            if trace_resolved < len(signal_rows):
                proof_gaps.append(
                    "Some signal fates remain unresolved after DB enrichment."
                )
        else:
            proof_gaps.append(
                "Signal metabolism trace is unresolved without DB-backed observation -> trigger -> Think -> model provenance."
            )
    return _vital(
        score=score,
        metrics={
            "planned_signals": planned,
            "manifested_signals": manifested,
            "observed_signals": observed,
            "valuable_signals": valuable,
            "trace_resolved_signals": trace_resolved,
            "trace_coverage": trace_coverage,
            "useful_valuable_signal_fates": useful_valuable,
            "useful_fate_yield": useful_yield,
            "healthy_noise_suppression": noise_score,
            "traceability_gap_rate": _ratio(
                len(signal_rows) - trace_resolved,
                len(signal_rows),
            ),
            "rows_with_leak_flags": leakage,
            "fate_counts": fate_counts,
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _control_plane_vital(
    run_summary: dict[str, Any],
    benchmark: dict[str, Any],
) -> dict[str, Any]:
    run_health = _json_obj(benchmark.get("run_health"))
    amplification = _json_obj(benchmark.get("run_amplification"))
    post_commit = _json_obj(run_summary.get("post_commit_status"))
    pending_triggers = _as_int(
        run_summary.get("pending_triggers"),
        _as_int(amplification.get("pending_triggers")),
    )
    failed_think = _as_int(
        run_summary.get("think_runs_failed"),
        _as_int(amplification.get("think_runs_failed")),
    )
    pending_post_commit = _as_int(run_summary.get("pending_post_commit_actions"))
    dead_lettered = _as_int(
        run_summary.get("dead_lettered_post_commit_actions"),
        _as_int(post_commit.get("dead_lettered")),
    )
    required_failures = _json_list(benchmark.get("required_run_failures"))
    components = [
        1.0 if pending_triggers == 0 else 0.0,
        1.0 if failed_think == 0 else 0.0,
        1.0 if pending_post_commit == 0 else 0.0,
        1.0 if dead_lettered == 0 else 0.0,
        1.0 if not required_failures else 0.0,
    ]
    findings = []
    if pending_triggers:
        findings.append(f"Trigger queue did not drain: pending={pending_triggers}.")
    if failed_think:
        findings.append(f"Think failures present: failed={failed_think}.")
    if pending_post_commit:
        findings.append(
            f"Post-commit queue did not drain: pending={pending_post_commit}."
        )
    if dead_lettered:
        findings.append(f"Post-commit dead letters present: dead_lettered={dead_lettered}.")
    if required_failures:
        findings.append("Required-run health failures are present.")
    if not findings:
        findings.append("Required queues and failure counters are clean in the report artifacts.")
    return _vital(
        score=_avg(components),
        metrics={
            "pending_triggers": pending_triggers,
            "think_runs_failed": failed_think,
            "pending_post_commit_actions": pending_post_commit,
            "dead_lettered_post_commit_actions": dead_lettered,
            "required_run_failures": len(required_failures),
            "source_status": benchmark.get("status") or run_health.get("status"),
        },
        findings=findings,
    )


def _retrieval_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
) -> dict[str, Any]:
    distribution = _json_obj(run_summary.get("context_use_distribution"))
    contract = _json_obj(run_summary.get("context_use_relation_contract"))
    retrieval_dim = _dimension(source_scorecard, "retrieval_usefulness")
    metrics = _json_obj(retrieval_dim.get("metrics"))
    total_context = sum(_as_int(v) for v in distribution.values())
    useful_context = sum(_as_int(distribution.get(key)) for key in USEFUL_CONTEXT_GRADES)
    context_ratio = _ratio(useful_context, total_context)
    contract_total = _as_int(contract.get("context_use_runs"))
    contract_failed = _as_int(contract.get("graph_relation_contract_failed_runs"))
    contract_score = 1.0 - _ratio(contract_failed, contract_total)
    proxy_scores = [
        context_ratio if total_context else None,
        contract_score if contract_total else None,
        _as_float_or_none(metrics.get("model_or_graph_context_use_score")),
        _as_float_or_none(retrieval_dim.get("score")),
    ]
    score = _avg([v for v in proxy_scores if v is not None])
    findings = [
        f"Useful context grade ratio is {round(context_ratio, 4)} across {total_context} context-use records."
    ]
    if contract_failed:
        findings.append(
            f"Graph relation contract failures remain: {contract_failed}/{contract_total}."
        )
    return _vital(
        score=score,
        metrics={
            "context_use_records": total_context,
            "useful_context_records": useful_context,
            "useful_context_ratio": context_ratio,
            "graph_relation_contract_failed_runs": contract_failed,
            "graph_relation_contract_runs": contract_total,
            "model_or_graph_context_use_score": metrics.get(
                "model_or_graph_context_use_score"
            ),
            "source_dimension_score": retrieval_dim.get("score"),
        },
        findings=findings,
    )


def _reasoning_vital(
    run_summary: dict[str, Any],
    benchmark: dict[str, Any],
) -> dict[str, Any]:
    amplification = _json_obj(benchmark.get("run_amplification"))
    success = _as_int(run_summary.get("think_runs_success"), _as_int(amplification.get("think_runs_success")))
    failed = _as_int(run_summary.get("think_runs_failed"), _as_int(amplification.get("think_runs_failed")))
    validation_errors = _as_int(amplification.get("validation_error_count"))
    total = success + failed
    success_score = _ratio(success, total)
    validation_score = 1.0 / (1.0 + validation_errors)
    score = _avg([success_score if total else None, validation_score])
    findings = [
        f"Think success ratio is {round(success_score, 4)} across {total} reported runs."
    ]
    if validation_errors:
        findings.append(f"Validation errors were reported: {validation_errors}.")
    return _vital(
        score=score,
        metrics={
            "think_runs_success": success,
            "think_runs_failed": failed,
            "validation_error_count": validation_errors,
        },
        findings=findings,
    )


def _model_atomicity_vital(models: list[dict[str, Any]]) -> dict[str, Any]:
    active = [
        row for row in models if str(row.get("status") or "active") == "active"
    ]
    if not active:
        return _vital(
            score=None,
            metrics={"active_models": 0},
            findings=["No active model artifact rows were present."],
            proof_gaps=["Model atomicity requires exported model artifacts."],
        )
    support_known = any(_model_support_count(row) is not None for row in active)
    falsifier_known = any("falsifier" in row for row in active)
    supported = sum(1 for row in active if (_model_support_count(row) or 0) > 0)
    falsifiable = sum(
        1 for row in active if _json_obj(row.get("falsifier")) or row.get("resolution_criteria")
    )
    wrapper_like = [row for row in active if _is_wrapper_model(row)]
    broad_scope = [row for row in active if _model_scope_size(row) > 14]
    conjunction_heavy = [row for row in active if _is_conjunction_heavy_model(row)]
    bounded_scope_score = 1.0 - _ratio(len(broad_scope), len(active))
    wrapper_score = 1.0 - _ratio(len(wrapper_like), len(active))
    conjunction_score = 1.0 - _ratio(len(conjunction_heavy), len(active))
    components = [bounded_scope_score, wrapper_score, conjunction_score]
    if support_known:
        components.append(_ratio(supported, len(active)))
    if falsifier_known:
        components.append(_ratio(falsifiable, len(active)))
    score = _avg(components)
    proof_gaps = []
    if not support_known:
        proof_gaps.append("Model artifacts do not expose support counts for atomicity scoring.")
    if not falsifier_known:
        proof_gaps.append("Model artifacts do not expose falsifiers for atomicity scoring.")
    findings = [
        (
            f"Atomicity proxy found {len(wrapper_like)} wrapper-like, "
            f"{len(conjunction_heavy)} conjunction-heavy, and "
            f"{len(broad_scope)} broad-scope active model(s)."
        )
    ]
    if wrapper_like:
        findings.append("Wrapper/window Models should usually be split or demoted into residual/inquiry state.")
    return _vital(
        score=score,
        metrics={
            "active_models": len(active),
            "support_known": support_known,
            "supported_active_models": supported,
            "falsifier_known": falsifier_known,
            "falsifiable_active_models": falsifiable,
            "wrapper_like_models": len(wrapper_like),
            "conjunction_heavy_models": len(conjunction_heavy),
            "broad_scope_models": len(broad_scope),
            "bounded_scope_score": bounded_scope_score,
            "wrapper_score": wrapper_score,
            "conjunction_score": conjunction_score,
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _company_object_spine_vital(
    bundle: dict[str, Any],
    models: list[dict[str, Any]],
) -> dict[str, Any]:
    active = [
        row for row in models if str(row.get("status") or "active") == "active"
    ]
    if not active:
        return _vital(
            score=None,
            metrics={"active_models": 0},
            findings=["No active model artifact rows were present."],
            proof_gaps=[
                "Company object spine health requires exported model artifacts."
            ],
        )

    anchor_refs_by_model = {
        str(row.get("id") or idx): _model_scope_anchor_refs(row)
        for idx, row in enumerate(active)
    }
    anchored_model_ids = {
        model_id for model_id, refs in anchor_refs_by_model.items() if refs
    }
    anchor_model_counts: dict[str, int] = {}
    anchor_types: dict[str, int] = {}
    for model_id, refs in anchor_refs_by_model.items():
        for ref in refs:
            anchor_model_counts[ref] = anchor_model_counts.get(ref, 0) + 1
            kind = ref.split(":", 1)[0]
            anchor_types[kind] = anchor_types.get(kind, 0) + 1

    mention_rows = [
        (row, _company_object_mentions(row))
        for row in active
        if _company_object_mentions(row)
    ]
    object_mention_count = len(mention_rows)
    object_mention_bound = sum(
        1
        for row, mention_types in mention_rows
        if _mentions_are_bound(row, mention_types)
    )
    rich_anchor_count = sum(1 for count in anchor_model_counts.values() if count >= 2)
    broad_anchor_models = [
        row for row in active if len(_model_scope_anchor_refs(row)) > 16
    ]
    expected_customers = _expected_customer_keys(bundle)
    customer_anchor_refs = [
        ref for ref in anchor_model_counts
        if ref.startswith("customer:") or ref.startswith("customer_resource:")
    ]
    expected_types = {
        "actor",
        "customer",
        "customer_resource",
        "commitment",
        "decision",
        "goal",
        "resource",
    }
    anchor_diversity_score = _ratio(
        len(set(anchor_types) & expected_types),
        len(expected_types),
    )
    binding_coverage = _ratio(len(anchored_model_ids), len(active))
    mention_binding_score = (
        _ratio(object_mention_bound, object_mention_count)
        if object_mention_count
        else None
    )
    richness_score = _ratio(rich_anchor_count, len(anchor_model_counts))
    overbroad_penalty_score = 1.0 - _ratio(len(broad_anchor_models), len(active))
    customer_presence_score = (
        min(1.0, _ratio(len(set(customer_anchor_refs)), len(expected_customers)))
        if expected_customers
        else None
    )
    score = _avg([
        binding_coverage,
        anchor_diversity_score,
        mention_binding_score,
        richness_score,
        overbroad_penalty_score,
        customer_presence_score,
    ])
    proof_gaps = []
    if not expected_customers:
        proof_gaps.append(
            "Scenario artifacts did not expose expected customer keys for customer-anchor recall."
        )
    findings = [
        (
            f"Company object spine proxy found {len(anchored_model_ids)}/"
            f"{len(active)} active Models bound to existing anchors across "
            f"{len(anchor_model_counts)} distinct anchor(s)."
        )
    ]
    if object_mention_count and object_mention_bound < object_mention_count:
        findings.append(
            f"{object_mention_count - object_mention_bound} object-like Model(s) mention durable company objects without matching scope binding."
        )
    if broad_anchor_models:
        findings.append(
            f"{len(broad_anchor_models)} Model(s) are bound to many anchors; inspect for over-broad scoping rather than richer object memory."
        )
    return _vital(
        score=score,
        metrics={
            "active_models": len(active),
            "anchored_active_models": len(anchored_model_ids),
            "binding_coverage": binding_coverage,
            "distinct_anchor_count": len(anchor_model_counts),
            "anchor_type_distribution": anchor_types,
            "anchor_diversity_score": anchor_diversity_score,
            "object_mention_models": object_mention_count,
            "object_mention_models_with_matching_binding": object_mention_bound,
            "object_mention_binding_score": mention_binding_score,
            "rich_anchor_count": rich_anchor_count,
            "richness_score": richness_score,
            "broad_anchor_models": len(broad_anchor_models),
            "overbroad_penalty_score": overbroad_penalty_score,
            "expected_customer_keys": len(expected_customers),
            "customer_anchor_refs": len(set(customer_anchor_refs)),
            "customer_presence_score": customer_presence_score,
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _compression_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
    models: list[dict[str, Any]],
) -> dict[str, Any]:
    compression_dim = _dimension(source_scorecard, "compression")
    graph = _json_obj(run_summary.get("graph_health"))
    observations = _as_int(run_summary.get("observation_count"), _as_int(run_summary.get("signal_count")))
    active_models = _as_int(run_summary.get("active_models"), len(models))
    duplicate_groups = _as_int(graph.get("exact_duplicate_natural_groups"))
    duplicate_score = 1.0 if duplicate_groups == 0 else 1.0 / (1.0 + duplicate_groups)
    score = _avg([
        _as_float_or_none(compression_dim.get("score")),
        duplicate_score,
        1.0 if active_models or observations == 0 else 0.0,
    ])
    metrics = {
        "active_models": active_models,
        "observations": observations,
        "model_to_observation_ratio": _ratio(active_models, observations),
        "exact_duplicate_natural_groups": duplicate_groups,
        "source_dimension_score": compression_dim.get("score"),
    }
    metrics.update(_json_obj(compression_dim.get("metrics")))
    findings = [
        "Uses existing compression score plus duplicate pressure and active model coverage."
    ]
    if duplicate_groups:
        findings.append(f"Exact duplicate natural groups remain: {duplicate_groups}.")
    return _vital(score=score, metrics=metrics, findings=findings)


def _coherence_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
    edges: list[dict[str, Any]],
) -> dict[str, Any]:
    graph = _json_obj(run_summary.get("graph_health"))
    edge_dim = _dimension(source_scorecard, "edge_intelligence")
    active_models = _as_int(graph.get("active_model_count"), _as_int(run_summary.get("active_models")))
    active_edges = _as_int(graph.get("active_edge_count"), len(edges))
    isolated_ratio = _as_float(graph.get("isolated_model_ratio"))
    largest_component_ratio = _as_float(graph.get("largest_component_ratio"))
    duplicate_edges = _as_int(graph.get("duplicate_directed_edge_count"))
    orphan_edges = _as_int(graph.get("orphan_edge_count"))
    self_edges = _as_int(graph.get("self_edge_count"))
    score = _avg(
        [
            largest_component_ratio if active_models else None,
            1.0 - isolated_ratio if active_models else None,
            1.0 if duplicate_edges == 0 else 0.5,
            1.0 if orphan_edges == 0 else 0.5,
            1.0 if self_edges == 0 else 0.5,
            _as_float_or_none(edge_dim.get("score")),
        ]
    )
    findings = [
        (
            f"Graph has {active_models} active models, {active_edges} active edges, "
            f"and isolated_model_ratio={round(isolated_ratio, 4)}."
        )
    ]
    if active_models and isolated_ratio > 0.35:
        findings.append("Isolated model pressure is high enough to inspect coherence.")
    return _vital(
        score=score,
        metrics={
            "active_models": active_models,
            "active_edges": active_edges,
            "isolated_model_ratio": isolated_ratio,
            "largest_component_ratio": largest_component_ratio,
            "duplicate_directed_edge_count": duplicate_edges,
            "orphan_edge_count": orphan_edges,
            "self_edge_count": self_edges,
            "source_edge_intelligence_score": edge_dim.get("score"),
        },
        findings=findings,
    )


def _edge_specificity_vital(
    edges: list[dict[str, Any]],
    run_summary: dict[str, Any],
) -> dict[str, Any]:
    active = [
        row for row in edges if str(row.get("status") or "active") == "active"
    ]
    if not active:
        return _vital(
            score=None,
            metrics={"active_edges": 0},
            findings=["No active edge artifact rows were present."],
            proof_gaps=["Edge specificity requires exported model edge artifacts."],
        )
    generic = [
        row for row in active
        if str(row.get("edge_kind") or "").casefold() in GENERIC_EDGE_KINDS
    ]
    missing_explanation = [
        row for row in active
        if not str(row.get("explanation") or row.get("metadata") or "").strip()
    ]
    kind_counts = _count_by(active, "edge_kind")
    kind_diversity = _ratio(len(kind_counts), len(active))
    confidence_known = any("confidence" in row or "weight" in row for row in active)
    confident = sum(
        1
        for row in active
        if _as_float(row.get("confidence", row.get("weight"))) >= 0.55
    )
    ontology_gaps = _as_int(_json_obj(run_summary.get("capability_probe_counts")).get("ontology_gap"))
    components = [
        1.0 - _ratio(len(generic), len(active)),
        1.0 - _ratio(len(missing_explanation), len(active)),
        min(1.0, kind_diversity * 4.0),
    ]
    if confidence_known:
        components.append(_ratio(confident, len(active)))
    score = _avg(components)
    findings = [
        (
            f"Edge specificity proxy saw {len(kind_counts)} edge kind(s) "
            f"across {len(active)} active edge(s); generic_share="
            f"{round(_ratio(len(generic), len(active)), 4)}."
        )
    ]
    if generic:
        findings.append("Generic edge kinds should become ontology-gap candidates or specific relationship kinds.")
    proof_gaps = []
    if not confidence_known:
        proof_gaps.append("Edge artifact rows do not expose confidence/weight.")
    return _vital(
        score=score,
        metrics={
            "active_edges": len(active),
            "edge_kind_count": len(kind_counts),
            "edge_kind_distribution": kind_counts,
            "generic_edge_count": len(generic),
            "generic_edge_share": _ratio(len(generic), len(active)),
            "missing_explanation_count": len(missing_explanation),
            "kind_diversity_ratio": kind_diversity,
            "confidence_known": confidence_known,
            "confident_edges": confident,
            "ontology_gap_probe_count": ontology_gaps,
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _active_frontier_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
    models: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> dict[str, Any]:
    graph = _json_obj(run_summary.get("graph_health"))
    active_models = _as_int(
        graph.get("active_model_count"),
        _as_int(run_summary.get("active_models"), len(models)),
    )
    observations = _as_int(
        run_summary.get("observation_count"),
        _as_int(run_summary.get("signal_count")),
    )
    active_edges = _as_int(
        graph.get("active_edge_count"),
        _as_int(run_summary.get("active_model_edges"), len(edges)),
    )
    isolated_ratio = _as_float(graph.get("isolated_model_ratio"))
    compression_dim = _dimension(source_scorecard, "compression")
    ratio = _ratio(active_models, observations)
    # Simulated runs vary by scenario. Treat this as a soft frontier proxy:
    # enough compression to avoid one-model-per-signal, enough active memory to
    # avoid raw-only loss, and enough graph linkage to make the frontier usable.
    if observations == 0 and active_models == 0:
        score = None
        proof_gaps = ["Active-frontier health requires observations or active model counts."]
    else:
        if observations == 0:
            size_score = 1.0
        elif ratio <= 0:
            size_score = 0.0
        elif ratio <= 0.75:
            size_score = 1.0
        elif ratio <= 1.25:
            size_score = 0.75
        else:
            size_score = max(0.2, 1.0 / ratio)
        edge_density = _ratio(active_edges, active_models)
        score = _avg([
            size_score,
            min(1.0, edge_density / 1.5) if active_models else None,
            1.0 - isolated_ratio if graph else None,
            _as_float_or_none(compression_dim.get("score")),
        ])
        proof_gaps = [] if graph else ["Graph health metrics were not present for active-frontier scoring."]
    return _vital(
        score=score,
        metrics={
            "observations": observations,
            "active_models": active_models,
            "active_edges": active_edges,
            "model_to_observation_ratio": ratio,
            "edge_density_per_active_model": _ratio(active_edges, active_models),
            "isolated_model_ratio": isolated_ratio,
            "compression_score": compression_dim.get("score"),
        },
        findings=[
            "Active-frontier health uses model/signal compression, edge density, and isolation pressure as retroactive proxies."
        ],
        proof_gaps=proof_gaps,
    )


def _create_update_balance_vital(signal_rows: list[dict[str, Any]]) -> dict[str, Any]:
    resolved = [
        row for row in signal_rows if row.get("final_fate") != "trace_unresolved"
    ]
    creates = sum(1 for row in resolved if row.get("final_fate") == "model_created")
    updates = sum(
        1
        for row in resolved
        if row.get("final_fate")
        in {
            "model_updated",
            "evidence_attached",
            "counterevidence_attached",
            "falsifier_created",
            "edge_created",
            "relation_frame_created",
            "projection_updated",
        }
    )
    total = creates + updates
    if not resolved:
        score = None
        proof_gaps = ["Create/update balance requires DB-backed signal fate resolution."]
        findings = ["Artifact-only traces cannot prove whether Think creates or edits dominantly."]
    elif total == 0:
        score = 0.0
        proof_gaps = []
        findings = ["Resolved signals did not create or update durable model-layer state."]
    else:
        create_share = creates / total
        # Best health is a mixed frontier. Penalize both all-create and all-update.
        score = max(0.0, 1.0 - abs(create_share - 0.5) * 2.0)
        proof_gaps = []
        findings = [
            f"Create/update balance saw creates={creates}, updates={updates}, create_share={round(create_share, 4)}."
        ]
        if create_share in {0.0, 1.0}:
            findings.append("Dominant one-sided create/update behavior should be treated as a model metabolism anomaly.")
    return _vital(
        score=score,
        metrics={
            "resolved_signal_fates": len(resolved),
            "model_create_fates": creates,
            "model_update_or_attachment_fates": updates,
            "create_update_total": total,
            "create_share": _ratio(creates, total),
            "update_share": _ratio(updates, total),
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _temporal_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
) -> dict[str, Any]:
    temporal_dim = _dimension(source_scorecard, "temporal_improvement")
    metrics = _json_obj(temporal_dim.get("metrics"))
    future_events = _as_int(
        metrics.get("future_validation_events"),
        _as_int(run_summary.get("future_validation_events")),
    )
    score = _as_float_or_none(temporal_dim.get("score"))
    proof_gaps = []
    if future_events == 0:
        proof_gaps.append("No future-validation events were observed in this report.")
    return _vital(
        score=score,
        metrics={
            "future_validation_events": future_events,
            "future_validation_memory_touch_ops": metrics.get(
                "future_validation_memory_touch_ops"
            ),
            "future_validation_model_or_graph_context_use_score": metrics.get(
                "future_validation_model_or_graph_context_use_score"
            ),
            "source_dimension_score": temporal_dim.get("score"),
        },
        findings=[
            "Temporal learning uses future-validation memory touch and later context reuse metrics when present."
        ],
        proof_gaps=proof_gaps,
    )


def _projection_vital(run_summary: dict[str, Any]) -> dict[str, Any]:
    post_commit = _json_obj(run_summary.get("post_commit_status"))
    topology = _json_obj(run_summary.get("topology_optimizer_status"))
    projection_report = _json_obj(run_summary.get("projection_metabolism"))
    pending_post_commit = _as_int(run_summary.get("pending_post_commit_actions"))
    dead_lettered = _as_int(
        run_summary.get("dead_lettered_post_commit_actions"),
        _as_int(post_commit.get("dead_lettered")),
    )
    processed = _as_int(post_commit.get("processed"))
    topology_failed = _as_int(topology.get("failed"))
    topology_status = str(topology.get("status") or "")
    queue_score = _avg(
        [
            1.0 if pending_post_commit == 0 else 0.0,
            1.0 if dead_lettered == 0 else 0.0,
            1.0 if topology_failed == 0 else 0.0,
        ]
    )
    projection_available = projection_report.get("available") is True
    entity_coverage_score = (
        _as_float_or_none(projection_report.get("entity_projection_coverage_ratio"))
        if projection_available
        else None
    )
    pending_refresh_jobs = _as_int(projection_report.get("pending_refresh_jobs"))
    failed_refresh_jobs = _as_int(projection_report.get("failed_refresh_jobs"))
    refresh_queue_score = (
        _avg(
            [
                1.0 if pending_refresh_jobs == 0 else 0.0,
                1.0 if failed_refresh_jobs == 0 else 0.0,
            ]
        )
        if projection_available
        else None
    )
    score = _avg(
        [
            queue_score,
            entity_coverage_score,
            refresh_queue_score,
        ]
    )
    proof_gaps = []
    if topology_status == "skipped":
        proof_gaps.append("Topology optimizer was skipped, so projection/topology closure is only partially proven.")
    missing_families = _json_list(
        projection_report.get("missing_entity_projection_families")
    )
    if projection_available and missing_families:
        proof_gaps.append(
            "Missing first-class projection surfaces: "
            + ", ".join(str(item) for item in missing_families)
        )
    if projection_available and pending_refresh_jobs:
        proof_gaps.append(
            f"Projection refresh queue still has {pending_refresh_jobs} pending/leased job(s)."
        )
    if projection_available and failed_refresh_jobs:
        proof_gaps.append(
            f"Projection refresh queue has {failed_refresh_jobs} failed/dead job(s)."
        )
    findings = [
        "Projection freshness is estimated from post-commit drain and topology optimizer status in artifact-only mode."
    ]
    if projection_available:
        findings.append(
            "Projection metabolism report provides entity-surface coverage and delta refresh queue health."
        )
    return _vital(
        score=score,
        metrics={
            "post_commit_processed": processed,
            "pending_post_commit_actions": pending_post_commit,
            "dead_lettered_post_commit_actions": dead_lettered,
            "topology_optimizer_status": topology_status,
            "topology_optimizer_failed": topology_failed,
            "entity_projection_coverage_ratio": entity_coverage_score,
            "missing_entity_projection_families": missing_families,
            "projection_refresh_jobs": projection_report.get("refresh_job_count"),
            "pending_projection_refresh_jobs": pending_refresh_jobs,
            "failed_projection_refresh_jobs": failed_refresh_jobs,
            "jobs_to_snapshots_ratio": projection_report.get(
                "jobs_to_snapshots_ratio"
            ),
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _product_utility_vital(product_value: dict[str, Any]) -> dict[str, Any]:
    score = _as_float_or_none(product_value.get("overall_score"))
    return _vital(
        score=score,
        metrics={"product_value_overall_score": score},
        findings=[
            "Uses the existing product-value eval overall score as the product utility proxy."
        ],
        proof_gaps=[] if score is not None else ["Product value evals were not present."],
    )


def _human_loop_vital(product_value: dict[str, Any]) -> dict[str, Any]:
    evals = _json_obj(product_value.get("evals"))
    scores = [
        _eval_score(evals, "question_policy"),
        _eval_score(evals, "negative_learning"),
        _eval_score(evals, "experience_metabolism"),
    ]
    score = _avg([value for value in scores if value is not None])
    return _vital(
        score=score,
        metrics={
            "question_policy_score": _eval_score(evals, "question_policy"),
            "negative_learning_score": _eval_score(evals, "negative_learning"),
            "experience_metabolism_score": _eval_score(evals, "experience_metabolism"),
        },
        findings=[
            "Artifact-only human-loop health uses question-policy, negative-learning, and experience-metabolism proxies."
        ],
        proof_gaps=[] if score is not None else ["Human feedback outcome events were not observed."],
    )


def _decision_outcome_vital(product_value: dict[str, Any]) -> dict[str, Any]:
    evals = _json_obj(product_value.get("evals"))
    scores = [
        _eval_score(evals, "decision_impact"),
        _eval_score(evals, "prediction_lifecycle"),
    ]
    score = _avg([value for value in scores if value is not None])
    return _vital(
        score=score,
        metrics={
            "decision_impact_score": _eval_score(evals, "decision_impact"),
            "prediction_lifecycle_score": _eval_score(evals, "prediction_lifecycle"),
        },
        findings=[
            "Decision outcome learning uses decision-impact and prediction-lifecycle proxies until explicit outcome waves are traced."
        ],
        proof_gaps=[] if score is not None else ["Decision outcome evals were not present."],
    )


def _organizational_change_vital(bundle: dict[str, Any]) -> dict[str, Any]:
    planned = bundle["planned_signals"]
    change_terms = ("pivot", "reorg", "renamed", "pricing", "strategy", "segment")
    matched = 0
    for row in planned:
        text = " ".join(
            str(row.get(key) or "") for key in ("content", "sequence", "family")
        ).casefold()
        if any(term in text for term in change_terms):
            matched += 1
    score = None if matched == 0 else min(1.0, matched / 5.0)
    return _vital(
        score=score,
        metrics={"organizational_change_signal_candidates": matched},
        findings=[
            "Looks for explicit org/strategy/segment-change signals in artifact-only mode."
        ],
        proof_gaps=[] if matched else ["No explicit organizational-change wave was detected."],
    )


def _self_improvement_vital(
    source_scorecard: dict[str, Any],
    product_value: dict[str, Any],
) -> dict[str, Any]:
    adaptive = _dimension(source_scorecard, "adaptive_lifecycle")
    evals = _json_obj(product_value.get("evals"))
    score = _avg(
        [
            _as_float_or_none(adaptive.get("score")),
            _eval_score(evals, "experience_metabolism"),
            _eval_score(evals, "negative_learning"),
            _eval_score(evals, "question_policy"),
        ]
    )
    metrics = dict(_json_obj(adaptive.get("metrics")))
    metrics.update(
        {
            "adaptive_lifecycle_score": adaptive.get("score"),
            "experience_metabolism_score": _eval_score(evals, "experience_metabolism"),
            "negative_learning_score": _eval_score(evals, "negative_learning"),
            "question_policy_score": _eval_score(evals, "question_policy"),
        }
    )
    return _vital(
        score=score,
        metrics=metrics,
        findings=[
            "Self-improvement health uses adaptive lifecycle plus experience, negative-learning, and question-policy proxies."
        ],
        proof_gaps=[] if score is not None else ["No self-improvement proxy metrics were present."],
    )


def _governance_vital(
    models: list[dict[str, Any]],
    run_summary: dict[str, Any],
) -> dict[str, Any]:
    active_models = [
        row for row in models if str(row.get("status") or "active") == "active"
    ]
    support_known = any("supporting_event_ids" in row for row in active_models)
    falsifier_known = any("falsifier" in row for row in active_models)
    unsupported = [
        row for row in active_models
        if "supporting_event_ids" in row and not _json_list(row.get("supporting_event_ids"))
    ]
    without_falsifier = [
        row for row in active_models
        if "falsifier" in row and not _json_obj(row.get("falsifier"))
    ]
    high_conf_unsupported = [
        row for row in unsupported if _as_float(row.get("confidence")) >= 0.8
    ]
    components = []
    if support_known:
        components.append(1.0 - _ratio(len(unsupported), len(active_models)))
        components.append(1.0 - _ratio(len(high_conf_unsupported), len(active_models)))
    if falsifier_known:
        components.append(1.0 - _ratio(len(without_falsifier), len(active_models)))
    score = _avg(components)
    proof_gaps = []
    if score is None:
        proof_gaps.append("Model artifact rows do not expose enough support/falsifier fields for governance scoring.")
    return _vital(
        score=score,
        metrics={
            "active_models": len(active_models) or _as_int(run_summary.get("active_models")),
            "supporting_event_ids_known": support_known,
            "falsifier_known": falsifier_known,
            "unsupported_active_models": len(unsupported),
            "active_without_falsifier": len(without_falsifier),
            "high_confidence_unsupported": len(high_conf_unsupported),
        },
        findings=[
            "Governance health checks stale/unsupported/falsifier surfaces when model artifacts expose them."
        ],
        proof_gaps=proof_gaps,
    )


def _authority_safety_vital(bundle: dict[str, Any]) -> dict[str, Any]:
    # Artifact-only reports intentionally avoid exported sensitive authority
    # state. Treat this as an explicit unproven safety loop, not a pass.
    return _vital(
        score=None,
        metrics={"artifact_authority_probe_present": False},
        findings=[
            "Authority safety requires DB/API probes; artifact-only vitals do not prove tenant isolation or authorization."
        ],
        proof_gaps=[
            "No authority/tenant-isolation probe artifact was present in this vitals run."
        ],
    )


def _efficiency_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
) -> dict[str, Any]:
    efficiency_dim = _dimension(source_scorecard, "efficiency")
    metrics = dict(_json_obj(efficiency_dim.get("metrics")))
    cost = _json_obj(run_summary.get("cost")) or _json_obj(run_summary.get("think_cost_profile"))
    if cost:
        metrics["reported_cost"] = cost
    return _vital(
        score=_as_float_or_none(efficiency_dim.get("score")),
        metrics=metrics,
        findings=[
            "Uses existing efficiency dimension; cost-per-useful-mutation needs DB-backed applied-op trace."
        ],
        proof_gaps=[] if efficiency_dim else ["Efficiency dimension was not present."],
    )


def _residual_channel_vital(
    signal_rows: list[dict[str, Any]],
    residual_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    resolved = [
        row for row in signal_rows
        if row.get("final_fate") != "trace_unresolved"
    ]
    valuable = [
        row for row in resolved
        if row.get("gold_value_class") != "noise"
    ]
    by_kind = _count_by(residual_rows, "residual_kind")
    absorption_ready = [
        row for row in residual_rows
        if row.get("status") in {"candidate", "open"}
    ]
    if not resolved:
        score = None
        proof_gaps = [
            "Residual-channel health requires DB-backed signal fate resolution."
        ]
        findings = [
            "Residual candidates are suppressed until signal metabolism is traceable."
        ]
    else:
        residual_rate = _ratio(len(absorption_ready), len(valuable))
        score = 1.0 - residual_rate
        proof_gaps = []
        findings = [
            (
                f"Detected {len(absorption_ready)} open residual candidate(s) "
                f"across {len(valuable)} valuable resolved signal(s)."
            )
        ]
        if absorption_ready:
            findings.append(
                "Residuals should be absorbed into models, rejected, expired, "
                "or routed to human/coherence repair."
            )
    return _vital(
        score=score,
        metrics={
            "resolved_signal_fates": len(resolved),
            "valuable_resolved_signals": len(valuable),
            "residual_candidate_count": len(residual_rows),
            "open_residual_candidates": len(absorption_ready),
            "residual_rate": _ratio(len(absorption_ready), len(valuable)),
            "residuals_by_kind": by_kind,
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _coherence_repair_vital(
    repair_rows: list[dict[str, Any]],
    run_summary: dict[str, Any],
) -> dict[str, Any]:
    graph = _json_obj(run_summary.get("graph_health"))
    has_graph = bool(graph)
    high = sum(1 for row in repair_rows if row.get("priority") == "high")
    medium = sum(1 for row in repair_rows if row.get("priority") == "medium")
    if not has_graph and not repair_rows:
        score = None
        proof_gaps = [
            "Coherence repair requires graph health or residual-coherence debt."
        ]
        findings = ["No graph-health or repair-candidate evidence was present."]
    else:
        score = 1.0 / (1.0 + high + (0.5 * medium))
        proof_gaps = []
        findings = [
            f"Identified {len(repair_rows)} coherence repair candidate(s)."
        ]
        if repair_rows:
            findings.append(
                "Repair candidates should reduce duplicate, isolated, "
                "unsupported, contradictory, or unanchored model fragments."
            )
    return _vital(
        score=score,
        metrics={
            "repair_candidate_count": len(repair_rows),
            "high_priority_repair_candidates": high,
            "medium_priority_repair_candidates": medium,
            "repair_candidates_by_kind": _count_by(repair_rows, "candidate_kind"),
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _retrieval_outcome_learning_vital(
    signal_rows: list[dict[str, Any]],
    outcome_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    resolved = [
        row for row in signal_rows
        if row.get("final_fate") != "trace_unresolved"
    ]
    learnable = [
        row for row in outcome_rows
        if row.get("selected_context_count") or row.get("omitted_evidence_count")
    ]
    if not resolved:
        score = None
        proof_gaps = [
            "Outcome-based retrieval learning requires DB-backed signal fates."
        ]
        findings = [
            "Retrieval outcome rewards are not computed for artifact-only traces."
        ]
    elif not learnable:
        score = None
        proof_gaps = [
            "No selected or omitted retrieval context was attributable to signal fates."
        ]
        findings = ["No retrieval decisions were attributable to downstream fates."]
    else:
        reward = _avg([
            _as_float(row.get("outcome_reward"))
            for row in learnable
        ])
        score = reward
        proof_gaps = []
        findings = [
            (
                f"Computed downstream retrieval rewards for {len(learnable)} "
                "signal-context decision(s)."
            )
        ]
    return _vital(
        score=score,
        metrics={
            "resolved_signal_fates": len(resolved),
            "outcome_rows": len(outcome_rows),
            "learnable_retrieval_decisions": len(learnable),
            "average_outcome_reward": _avg([
                _as_float(row.get("outcome_reward"))
                for row in learnable
            ]),
            "outcome_classes": _count_by(outcome_rows, "outcome_class"),
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _latent_gap_vital(
    signal_rows: list[dict[str, Any]],
    residual_rows: list[dict[str, Any]],
    latent_gap_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    resolved = [
        row for row in signal_rows
        if row.get("final_fate") != "trace_unresolved"
    ]
    open_residuals = [
        row for row in residual_rows
        if row.get("status") in {"candidate", "open"}
    ]
    if not resolved:
        score = None
        proof_gaps = [
            "Latent-gap modeling requires measured missingness from resolved fates."
        ]
        findings = ["No latent-gap candidates are emitted from artifact-only traces."]
    elif open_residuals and not latent_gap_rows:
        score = 0.0
        proof_gaps = []
        findings = [
            "Residual debt exists, but no structured latent-gap candidate was emitted."
        ]
    elif latent_gap_rows:
        complete = sum(
            1
            for row in latent_gap_rows
            if row.get("supporting_residual_ids")
            and row.get("falsifier")
            and row.get("next_evidence_needed")
        )
        score = _ratio(complete, len(latent_gap_rows))
        proof_gaps = []
        findings = [
            (
                f"Emitted {len(latent_gap_rows)} latent-gap candidate(s) "
                "from measured residual clusters."
            )
        ]
    else:
        score = 1.0
        proof_gaps = []
        findings = ["No residual clusters required latent-gap hypotheses."]
    return _vital(
        score=score,
        metrics={
            "resolved_signal_fates": len(resolved),
            "residual_candidate_count": len(residual_rows),
            "open_residual_candidates": len(open_residuals),
            "latent_gap_candidate_count": len(latent_gap_rows),
            "latent_gaps_by_kind": _count_by(latent_gap_rows, "gap_kind"),
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _dark_matter_loop_vital(
    signal_rows: list[dict[str, Any]],
    residual_rows: list[dict[str, Any]],
    latent_gap_rows: list[dict[str, Any]],
    product_value: dict[str, Any],
) -> dict[str, Any]:
    evals = _json_obj(product_value.get("evals"))
    latent_bridge_score = _eval_score(evals, "latent_bridge_inference")
    question_policy_score = _eval_score(evals, "question_policy")
    resolved = [
        row for row in signal_rows if row.get("final_fate") != "trace_unresolved"
    ]
    open_residuals = [
        row for row in residual_rows if row.get("status") in {"candidate", "open"}
    ]
    human_requests = sum(
        1 for row in signal_rows if row.get("final_fate") == "human_feedback_requested"
    )
    if not resolved and latent_bridge_score is None and question_policy_score is None:
        score = None
        proof_gaps = [
            "Dark-matter loop health requires DB-backed missingness traces or latent-bridge/question-policy evals."
        ]
    else:
        gap_conversion = _ratio(len(latent_gap_rows), len(open_residuals)) if open_residuals else 1.0
        human_route_score = _ratio(human_requests, len(latent_gap_rows)) if latent_gap_rows else None
        score = _avg([
            latent_bridge_score,
            question_policy_score,
            gap_conversion,
            human_route_score,
        ])
        proof_gaps = []
        if latent_gap_rows and human_requests == 0:
            proof_gaps.append("Latent gaps were detected, but no human-validation route was proven.")
    return _vital(
        score=score,
        metrics={
            "resolved_signal_fates": len(resolved),
            "open_residual_candidates": len(open_residuals),
            "latent_gap_candidates": len(latent_gap_rows),
            "human_feedback_requested_fates": human_requests,
            "latent_bridge_inference_score": latent_bridge_score,
            "question_policy_score": question_policy_score,
        },
        findings=[
            (
                f"Dark-matter proxy saw residuals={len(open_residuals)}, "
                f"latent_gap_candidates={len(latent_gap_rows)}, "
                f"human_feedback_requests={human_requests}."
            )
        ],
        proof_gaps=proof_gaps,
    )


def _sage_policy_effect_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
    product_value: dict[str, Any],
) -> dict[str, Any]:
    topology = _json_obj(run_summary.get("topology_optimizer_status"))
    adaptive = _dimension(source_scorecard, "adaptive_lifecycle")
    evals = _json_obj(product_value.get("evals"))
    topology_completed = _as_int(topology.get("completed"), _as_int(topology.get("processed")))
    topology_failed = _as_int(topology.get("failed"))
    topology_status = str(topology.get("status") or "")
    experience_score = _eval_score(evals, "experience_metabolism")
    negative_learning_score = _eval_score(evals, "negative_learning")
    adaptive_score = _as_float_or_none(adaptive.get("score"))
    if topology_status == "skipped" and not any(
        value is not None
        for value in (experience_score, negative_learning_score, adaptive_score)
    ):
        score = None
        proof_gaps = ["No SAGE/topology or experience-metabolism evidence was present."]
    else:
        topology_score = None
        if topology_status:
            topology_score = 1.0 if topology_completed > 0 and topology_failed == 0 else 0.0
            if topology_status == "skipped":
                topology_score = 0.25
        score = _avg([
            topology_score,
            experience_score,
            negative_learning_score,
            adaptive_score,
        ])
        proof_gaps = []
        if topology_status == "skipped":
            proof_gaps.append("SAGE topology optimizer was skipped.")
    return _vital(
        score=score,
        metrics={
            "topology_optimizer_status": topology_status,
            "topology_optimizer_completed": topology_completed,
            "topology_optimizer_failed": topology_failed,
            "adaptive_lifecycle_score": adaptive_score,
            "experience_metabolism_score": experience_score,
            "negative_learning_score": negative_learning_score,
        },
        findings=[
            "SAGE policy-effect health asks whether outcome/experience signals become future behavior levers."
        ],
        proof_gaps=proof_gaps,
    )


def _pattern_cascade_vital(
    signal_rows: list[dict[str, Any]],
    source_scorecard: dict[str, Any],
    product_value: dict[str, Any],
) -> dict[str, Any]:
    evals = _json_obj(product_value.get("evals"))
    edge_dim = _dimension(source_scorecard, "edge_intelligence")
    reasoning_dim = _dimension(source_scorecard, "reasoning_value")
    pattern_signals = [
        row for row in signal_rows
        if any(
            term in " ".join(
                str(row.get(key) or "")
                for key in ("family", "sequence", "source_channel", "storyline_id")
            ).casefold()
            for term in ("pattern", "recurring", "repeated", "trend", "cascade")
        )
    ]
    downstream_fates = {
        "edge_created",
        "relation_frame_created",
        "projection_updated",
        "product_surface_updated",
        "decision_outcome_recorded",
        "self_improvement_event_created",
        "human_feedback_requested",
    }
    cascaded = [
        row for row in pattern_signals if row.get("final_fate") in downstream_fates
    ]
    score = _avg([
        _ratio(len(cascaded), len(pattern_signals)) if pattern_signals else None,
        _as_float_or_none(edge_dim.get("score")),
        _as_float_or_none(reasoning_dim.get("score")),
        _eval_score(evals, "latent_bridge_inference"),
        _eval_score(evals, "decision_impact"),
    ])
    proof_gaps = []
    if not pattern_signals:
        proof_gaps.append("No explicit pattern/recurrence signals were detectable in artifacts.")
    return _vital(
        score=score,
        metrics={
            "pattern_signal_candidates": len(pattern_signals),
            "pattern_signals_with_downstream_fate": len(cascaded),
            "pattern_cascade_ratio": _ratio(len(cascaded), len(pattern_signals)),
            "edge_intelligence_score": edge_dim.get("score"),
            "reasoning_value_score": reasoning_dim.get("score"),
            "latent_bridge_inference_score": _eval_score(evals, "latent_bridge_inference"),
            "decision_impact_score": _eval_score(evals, "decision_impact"),
        },
        findings=[
            "Pattern cascade health checks whether repeated/latent patterns lead to graph, product, decision, or learning effects."
        ],
        proof_gaps=proof_gaps,
    )


def _ask_signal_learning_vital(product_value: dict[str, Any]) -> dict[str, Any]:
    evals = _json_obj(product_value.get("evals"))
    question_policy_score = _eval_score(evals, "question_policy")
    negative_learning_score = _eval_score(evals, "negative_learning")
    experience_score = _eval_score(evals, "experience_metabolism")
    score = _avg([question_policy_score, negative_learning_score, experience_score])
    return _vital(
        score=score,
        metrics={
            "question_policy_score": question_policy_score,
            "negative_learning_score": negative_learning_score,
            "experience_metabolism_score": experience_score,
        },
        findings=[
            "Ask/user-signal learning is currently scored through question-policy, negative-learning, and experience-metabolism proxies."
        ],
        proof_gaps=[] if score is not None else ["Ask conversation-to-user-model learning was not proven by artifacts."],
    )


def _simplification_pressure_vital(
    run_summary: dict[str, Any],
    source_scorecard: dict[str, Any],
) -> dict[str, Any]:
    graph = _json_obj(run_summary.get("graph_health"))
    robustness = _dimension(source_scorecard, "robustness")
    compression = _dimension(source_scorecard, "compression")
    duplicate_groups = _as_int(graph.get("exact_duplicate_natural_groups"))
    isolated_ratio = _as_float(graph.get("isolated_model_ratio"))
    orphan_edges = _as_int(graph.get("orphan_edge_count"))
    duplicate_edges = _as_int(graph.get("duplicate_directed_edge_count"))
    score = _avg([
        1.0 if duplicate_groups == 0 else 1.0 / (1.0 + duplicate_groups),
        1.0 - isolated_ratio if graph else None,
        1.0 if orphan_edges == 0 else 0.5,
        1.0 if duplicate_edges == 0 else 0.5,
        _as_float_or_none(compression.get("score")),
        _as_float_or_none(robustness.get("score")),
    ])
    proof_gaps = [] if graph else ["Graph health metrics were not present for simplification scoring."]
    findings = [
        "Simplification pressure uses duplicate, isolated, orphan, and robustness/compression proxies."
    ]
    if duplicate_groups or duplicate_edges or orphan_edges:
        findings.append("Redundant graph artifacts should feed repair or deletion/merge decisions.")
    return _vital(
        score=score,
        metrics={
            "exact_duplicate_natural_groups": duplicate_groups,
            "isolated_model_ratio": isolated_ratio,
            "orphan_edge_count": orphan_edges,
            "duplicate_directed_edge_count": duplicate_edges,
            "compression_score": compression.get("score"),
            "robustness_score": robustness.get("score"),
        },
        findings=findings,
        proof_gaps=proof_gaps,
    )


def _build_signal_metabolism_rows(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    planned = bundle["planned_signals"]
    manifest = bundle["signal_manifest"]
    rows = []
    total = max(len(planned), len(manifest))
    for idx in range(total):
        planned_row = planned[idx] if idx < len(planned) else {}
        manifest_row = manifest[idx] if idx < len(manifest) else {}
        source = {**planned_row, **manifest_row}
        sequence = str(source.get("sequence") or "")
        family = str(source.get("family") or "")
        gold_class = _gold_value_class(sequence=sequence, family=family)
        observation_id = source.get("observation_id")
        leak_flags = []
        if not observation_id:
            leak_flags.append("planned_signal_missing_observation")
        if observation_id:
            leak_flags.append("artifact_only_trace_unresolved")
        rows.append(
            {
                "signal_id": source.get("signal_id") or f"signal-{idx:05d}",
                "index": source.get("index", idx),
                "storyline_id": source.get("storyline_id"),
                "sequence": sequence or None,
                "wave": _wave_class(sequence),
                "source_channel": source.get("channel") or source.get("source_channel"),
                "family": family or None,
                "customer": source.get("customer"),
                "gold_value_class": gold_class,
                "observation_id": observation_id,
                "trigger_ids": [],
                "think_run_ids": [],
                "retrieved_model_ids": [],
                "retrieved_observation_ids": [],
                "applied_model_ids": [],
                "applied_edge_ids": [],
                "projection_subjects": [],
                "product_surface_refs": [],
                "final_fate": "trace_unresolved" if observation_id else "raw_only_unmodeled",
                "fate_reasons": [
                    "artifact_only_renderer_cannot_join_signal_to_think_run"
                    if observation_id
                    else "no_observation_id_in_manifest"
                ],
                "leak_flags": leak_flags,
                "safety_flags": [],
            }
        )
    return rows


def _residual_trace_rows(signal_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in signal_rows:
        persisted = _json_list(row.get("persisted_residuals"))
        if persisted:
            for residual in persisted:
                if not isinstance(residual, dict):
                    continue
                rows.append(
                    {
                        "residual_id": residual.get("id"),
                        "status": residual.get("status") or "open",
                        "residual_kind": residual.get("residual_kind"),
                        "signal_id": row.get("signal_id"),
                        "observation_id": row.get("observation_id"),
                        "storyline_id": row.get("storyline_id"),
                        "family": row.get("family"),
                        "gold_value_class": row.get("gold_value_class"),
                        "final_fate": row.get("final_fate"),
                        "leak_flags": _json_list(row.get("leak_flags")),
                        "reason": residual.get("reason"),
                        "compact_summary": residual.get("compact_summary"),
                        "absorption_object_kind": residual.get(
                            "absorption_object_kind"
                        ),
                        "absorption_object_id": residual.get("absorption_object_id"),
                        "metadata": _json_obj(residual.get("metadata")),
                        "source": "model_residual_evidence",
                        "success_criteria": [
                            "absorbed_into_model_or_relation",
                            "rejected_as_noise_or_duplicate",
                            "expired_after_no_longer_relevant",
                            "routed_to_human_or_coherence_repair",
                        ],
                    }
                )
            continue
        residual_kind = _residual_kind_for_signal(row)
        if residual_kind is None:
            continue
        anchor = row.get("observation_id") or row.get("signal_id")
        residual_id = f"residual:{anchor}"
        rows.append(
            {
                "residual_id": residual_id,
                "status": "candidate",
                "residual_kind": residual_kind,
                "signal_id": row.get("signal_id"),
                "observation_id": row.get("observation_id"),
                "storyline_id": row.get("storyline_id"),
                "family": row.get("family"),
                "gold_value_class": row.get("gold_value_class"),
                "final_fate": row.get("final_fate"),
                "leak_flags": _json_list(row.get("leak_flags")),
                "reason": _residual_reason(row, residual_kind),
                "compact_summary": _residual_summary(row, residual_kind),
                "success_criteria": [
                    "absorbed_into_model_or_relation",
                    "rejected_as_noise_or_duplicate",
                    "expired_after_no_longer_relevant",
                    "routed_to_human_or_coherence_repair",
                ],
            }
        )
    return rows


def _coherence_repair_candidate_rows(
    bundle: dict[str, Any],
    signal_rows: list[dict[str, Any]],
    residual_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    run_summary = bundle["run_summary"]
    graph = _json_obj(run_summary.get("graph_health"))
    rows: list[dict[str, Any]] = []

    def add(
        kind: str,
        *,
        priority: str,
        reason: str,
        evidence: dict[str, Any],
        action: str,
        success_metric: str,
    ) -> None:
        rows.append(
            {
                "candidate_id": f"repair:{kind}:{len(rows) + 1}",
                "candidate_kind": kind,
                "priority": priority,
                "reason": reason,
                "evidence": evidence,
                "suggested_action": action,
                "success_metric": success_metric,
            }
        )

    duplicate_groups = _as_int(graph.get("exact_duplicate_natural_groups"))
    duplicate_edges = _as_int(graph.get("duplicate_directed_edge_count"))
    orphan_edges = _as_int(graph.get("orphan_edge_count"))
    self_edges = _as_int(graph.get("self_edge_count"))
    isolated_ratio = _as_float(graph.get("isolated_model_ratio"))
    if duplicate_groups:
        add(
            "duplicate_model_pressure",
            priority="high",
            reason=f"{duplicate_groups} exact duplicate natural model group(s).",
            evidence={"exact_duplicate_natural_groups": duplicate_groups},
            action="merge_or_supersede_duplicate_models",
            success_metric="exact_duplicate_natural_groups decreases",
        )
    if duplicate_edges:
        add(
            "duplicate_edge_pressure",
            priority="medium",
            reason=f"{duplicate_edges} duplicate directed edge(s).",
            evidence={"duplicate_directed_edge_count": duplicate_edges},
            action="dedupe_or_reconcile_parallel_edges",
            success_metric="duplicate_directed_edge_count decreases",
        )
    if orphan_edges:
        add(
            "orphan_edge_pressure",
            priority="high",
            reason=f"{orphan_edges} orphan edge(s) were reported.",
            evidence={"orphan_edge_count": orphan_edges},
            action="reattach_or_delete_edges_without_valid_endpoints",
            success_metric="orphan_edge_count reaches zero",
        )
    if self_edges:
        add(
            "self_edge_pressure",
            priority="medium",
            reason=f"{self_edges} self edge(s) were reported.",
            evidence={"self_edge_count": self_edges},
            action="remove_or_rewrite_self_referential_edges",
            success_metric="self_edge_count reaches zero",
        )
    if isolated_ratio > 0.35:
        add(
            "isolated_model_pressure",
            priority="high",
            reason=f"isolated_model_ratio={round(isolated_ratio, 4)}.",
            evidence={"isolated_model_ratio": isolated_ratio},
            action="attach_evidence_edges_or_archive_isolated_models",
            success_metric="isolated_model_ratio decreases",
        )

    actionable_residual_rows = [
        row for row in residual_rows
        if row.get("status") in {"candidate", "open"}
    ]
    residuals_by_kind = _group_by(actionable_residual_rows, "residual_kind")
    for kind, grouped in sorted(residuals_by_kind.items()):
        priority = "high" if kind in {
            "counterevidence_unattached",
            "relation_unanchored",
            "valuable_unmodeled",
        } else "medium"
        add(
            f"residual_{kind}",
            priority=priority,
            reason=f"{len(grouped)} residual candidate(s) of kind {kind}.",
            evidence={
                "residual_kind": kind,
                "residual_count": len(grouped),
                "observation_ids": [
                    str(row.get("observation_id"))
                    for row in grouped[:10]
                    if row.get("observation_id")
                ],
            },
            action=_repair_action_for_residual_kind(kind),
            success_metric="open residual count for this kind decreases",
        )

    unresolved = [
        row for row in signal_rows
        if row.get("final_fate") in {"no_think_trigger", "trigger_pending"}
    ]
    if unresolved:
        add(
            "skipped_metabolism_repair",
            priority="high",
            reason=f"{len(unresolved)} signal(s) did not reach Think.",
            evidence={"signal_count": len(unresolved)},
            action="enqueue_or_repair_missing_event_arrival_triggers",
            success_metric="no_think_trigger and trigger_pending fates reach zero",
        )
    return rows


def _retrieval_outcome_learning_rows(
    signal_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for row in signal_rows:
        final_fate = str(row.get("final_fate") or "unknown")
        if final_fate == "trace_unresolved":
            continue
        selected_context_count = (
            len(_json_list(row.get("retrieved_model_ids")))
            + len(_json_list(row.get("retrieved_observation_ids")))
        )
        referenced_context_count = (
            len(_json_list(row.get("referenced_model_ids")))
            + len(_json_list(row.get("referenced_observation_ids")))
        )
        db_trace = _json_obj(row.get("db_trace"))
        omitted_count = _as_int(db_trace.get("omitted_evidence_count"))
        reward, outcome_class = _retrieval_outcome_reward(row)
        rows.append(
            {
                "signal_id": row.get("signal_id"),
                "observation_id": row.get("observation_id"),
                "storyline_id": row.get("storyline_id"),
                "family": row.get("family"),
                "final_fate": final_fate,
                "outcome_class": outcome_class,
                "outcome_reward": reward,
                "selected_context_count": selected_context_count,
                "referenced_context_count": referenced_context_count,
                "omitted_evidence_count": omitted_count,
                "learning_signal": _retrieval_learning_signal(
                    reward,
                    omitted_count=omitted_count,
                    selected_context_count=selected_context_count,
                ),
            }
        )
    return rows


def _latent_gap_candidate_rows(
    signal_rows: list[dict[str, Any]],
    residual_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    signal_by_id = {
        str(row.get("signal_id")): row
        for row in signal_rows
        if row.get("signal_id") is not None
    }
    rows: list[dict[str, Any]] = []
    persisted_cluster_keys: set[tuple[str, tuple[str, ...]]] = set()
    seen_persisted_ids: set[str] = set()
    for signal in signal_rows:
        for hypothesis in _json_list(signal.get("persisted_latent_gap_hypotheses")):
            if not isinstance(hypothesis, dict):
                continue
            candidate_id = str(hypothesis.get("id") or "")
            if candidate_id and candidate_id in seen_persisted_ids:
                continue
            if candidate_id:
                seen_persisted_ids.add(candidate_id)
            gap_kind = str(hypothesis.get("gap_kind") or "unknown_kind")
            supporting_ids = [
                str(value)
                for value in _json_list(hypothesis.get("supporting_residual_ids"))
            ]
            persisted_cluster_keys.add((gap_kind, tuple(sorted(supporting_ids))))
            rows.append(
                {
                    "candidate_id": candidate_id or hypothesis.get(
                        "residual_cluster_hash"
                    ),
                    "status": hypothesis.get("status") or "candidate",
                    "gap_kind": gap_kind,
                    "storyline_id": signal.get("storyline_id"),
                    "family": signal.get("family"),
                    "supporting_residual_ids": supporting_ids,
                    "supporting_observation_ids": [
                        str(value)
                        for value in _json_list(
                            hypothesis.get("supporting_observation_ids")
                        )
                    ],
                    "missing_evidence_statement": hypothesis.get(
                        "missing_evidence_statement"
                    ),
                    "falsifier": hypothesis.get("falsifier"),
                    "next_evidence_needed": hypothesis.get("next_evidence_needed"),
                    "confidence": hypothesis.get("confidence"),
                    "hypothesis_text": hypothesis.get("hypothesis_text"),
                    "resolution_object_kind": hypothesis.get("resolution_object_kind"),
                    "resolution_object_id": hypothesis.get("resolution_object_id"),
                    "resolution_reason": hypothesis.get("resolution_reason"),
                    "metadata": _json_obj(hypothesis.get("metadata")),
                    "source": "sage_latent_gap_hypotheses",
                    "status_semantics": "non_canonical_until_confirmed",
                }
            )
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for residual in residual_rows:
        if residual.get("status") not in {"candidate", "open"}:
            continue
        key = (
            str(residual.get("storyline_id") or "unknown_storyline"),
            str(residual.get("family") or "unknown_family"),
            str(residual.get("residual_kind") or "unknown_kind"),
        )
        groups.setdefault(key, []).append(residual)
    for (storyline_id, family, residual_kind), grouped in sorted(groups.items()):
        supporting_ids = [str(row.get("residual_id")) for row in grouped]
        cluster_key = (residual_kind, tuple(sorted(supporting_ids)))
        if cluster_key in persisted_cluster_keys:
            continue
        rows.append(
            {
                "candidate_id": (
                    f"latent_gap:{storyline_id}:{family}:{residual_kind}"
                ),
                "status": "candidate_not_canonical_fact",
                "gap_kind": residual_kind,
                "storyline_id": storyline_id,
                "family": family,
                "supporting_residual_ids": supporting_ids,
                "supporting_observation_ids": [
                    str(row.get("observation_id"))
                    for row in grouped
                    if row.get("observation_id")
                ],
                "missing_evidence_statement": _missing_evidence_statement(
                    residual_kind,
                    grouped,
                ),
                "falsifier": _latent_gap_falsifier(residual_kind),
                "next_evidence_needed": _next_evidence_needed(residual_kind),
                "confidence": min(0.85, 0.35 + (0.10 * len(grouped))),
                "example_signal_fates": [
                    signal_by_id.get(str(row.get("signal_id")), {}).get("final_fate")
                    for row in grouped[:5]
                ],
                "source": "artifact_derived_residual_cluster",
                "status_semantics": "non_canonical_until_confirmed",
            }
        )
    return rows


def _model_delta_rows(bundle: dict[str, Any]) -> list[dict[str, Any]]:
    rows = []
    for row in bundle["models"]:
        rows.append(
            {
                "model_id": row.get("id"),
                "status": row.get("status") or "active",
                "proposition_kind": row.get("proposition_kind"),
                "claim_role": row.get("claim_role"),
                "confidence": row.get("confidence"),
                "natural": row.get("natural"),
                "supporting_event_count": len(_json_list(row.get("supporting_event_ids"))),
                "scope_entity_count": len(_json_list(row.get("scope_entities"))),
                "scope_actor_count": len(_json_list(row.get("scope_actors"))),
            }
        )
    return rows


def _graph_coherence_payload(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    run_summary = bundle["run_summary"]
    graph = _json_obj(run_summary.get("graph_health"))
    return {
        "graph_health": graph,
        "model_coherence_vital": _json_obj(scorecard["vitals"].get("model_coherence")),
        "edge_kind_distribution": _json_obj(run_summary.get("edge_kind_distribution")),
        "edge_review_distribution": _json_obj(run_summary.get("edge_review_distribution")),
    }


def _vitals_run_payload(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "run_id": scorecard.get("run_id"),
        "tenant_id": scorecard.get("tenant_id"),
        "source_report_dir": str(bundle["report_dir"]),
        "status": scorecard.get("status"),
        "overall_score": scorecard.get("overall_score"),
        "score_coverage": scorecard.get("score_coverage"),
        "artifact_counts": {
            "planned_signals": len(bundle["planned_signals"]),
            "signal_manifest": len(bundle["signal_manifest"]),
            "models": len(bundle["models"]),
            "model_edges": len(bundle["model_edges"]),
        },
    }


def _trigger_trace_row(bundle: dict[str, Any]) -> dict[str, Any]:
    run_summary = bundle["run_summary"]
    benchmark = bundle["benchmark_summary"] or bundle["storyline_scores"]
    return {
        "trace_type": "aggregate",
        "pending_triggers": run_summary.get("pending_triggers"),
        "think_runs_success": run_summary.get("think_runs_success"),
        "think_runs_failed": run_summary.get("think_runs_failed"),
        "run_amplification": benchmark.get("run_amplification"),
    }


def _retrieval_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    run_summary = bundle["run_summary"]
    return {
        "trace_type": "aggregate",
        "context_use_distribution": run_summary.get("context_use_distribution"),
        "context_use_relation_contract": run_summary.get(
            "context_use_relation_contract"
        ),
        "vital": scorecard["vitals"]["retrieval_roi"],
    }


def _think_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    run_summary = bundle["run_summary"]
    return {
        "trace_type": "aggregate",
        "think_runs_success": run_summary.get("think_runs_success"),
        "think_runs_failed": run_summary.get("think_runs_failed"),
        "latency_breakdown": run_summary.get("latency_breakdown"),
        "think_cost_profile": run_summary.get("think_cost_profile") or run_summary.get("cost"),
        "vital": scorecard["vitals"]["reasoning_throughput"],
    }


def _validation_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    benchmark = bundle["benchmark_summary"] or bundle["storyline_scores"]
    amplification = _json_obj(benchmark.get("run_amplification"))
    return {
        "trace_type": "aggregate",
        "validation_error_count": amplification.get("validation_error_count"),
        "required_run_failures": benchmark.get("required_run_failures"),
        "vital": scorecard["vitals"]["reasoning_throughput"],
    }


def _projection_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    run_summary = bundle["run_summary"]
    return {
        "trace_type": "aggregate",
        "post_commit_status": run_summary.get("post_commit_status"),
        "topology_optimizer_status": run_summary.get("topology_optimizer_status"),
        "projection_metabolism": run_summary.get("projection_metabolism"),
        "vital": scorecard["vitals"]["projection_freshness"],
    }


def _product_surface_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    source = scorecard.get("source_company_intelligence") or {}
    return {
        "trace_type": "aggregate",
        "product_value_overall_score": source.get("product_value_overall_score"),
        "vital": scorecard["vitals"]["product_utility"],
    }


def _human_feedback_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    return {
        "trace_type": "aggregate",
        "vital": scorecard["vitals"]["human_loop_closure"],
    }


def _decision_outcome_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    return {
        "trace_type": "aggregate",
        "vital": scorecard["vitals"]["decision_outcome_learning"],
    }


def _self_improvement_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    return {
        "trace_type": "aggregate",
        "vital": scorecard["vitals"]["self_improvement"],
    }


def _governance_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    return {
        "trace_type": "aggregate",
        "vital": scorecard["vitals"]["governance_health"],
    }


def _authority_safety_trace_row(
    bundle: dict[str, Any],
    scorecard: dict[str, Any],
) -> dict[str, Any]:
    return {
        "trace_type": "aggregate",
        "vital": scorecard["vitals"]["authority_safety"],
    }


def _hard_failures(
    vitals: dict[str, Any],
    benchmark: dict[str, Any],
) -> list[str]:
    failures: list[str] = []
    control = _json_obj(vitals.get("control_plane_health"))
    control_metrics = _json_obj(control.get("metrics"))
    if _as_int(control_metrics.get("pending_triggers")):
        failures.append(
            f"trigger queue did not drain: pending={control_metrics['pending_triggers']}"
        )
    if _as_int(control_metrics.get("think_runs_failed")):
        failures.append(
            f"Think failures present: failed={control_metrics['think_runs_failed']}"
        )
    if _as_int(control_metrics.get("pending_post_commit_actions")):
        failures.append(
            "post-commit queue did not drain: "
            f"pending={control_metrics['pending_post_commit_actions']}"
        )
    if _as_int(control_metrics.get("dead_lettered_post_commit_actions")):
        failures.append(
            "post-commit actions dead-lettered: "
            f"dead_lettered={control_metrics['dead_lettered_post_commit_actions']}"
        )
    for item in _json_list(benchmark.get("required_run_failures")):
        failures.append(str(item))

    retrieval = _json_obj(vitals.get("retrieval_roi"))
    retrieval_metrics = _json_obj(retrieval.get("metrics"))
    if (
        _as_int(retrieval_metrics.get("context_use_records")) > 0
        and _as_int(retrieval_metrics.get("useful_context_records")) == 0
    ):
        failures.append("retrieval selected context but no useful context was used")
    return failures


def _company_learning_only_vitals(
    vitals: dict[str, Any],
) -> dict[str, Any]:
    """Leave general product vitals unscored in a focused learning report."""

    proof_gap = (
        "This focused report measures the company-learning loop only; the "
        "general product vital was not measured."
    )
    return {
        name: _vital(
            score=None,
            metrics={
                "measurement_profile": "company_learning_only",
                "measurement_status": "not_measured",
            },
            findings=[
                "See the Company Physics section for the focused learning-loop "
                "evidence collected by this report."
            ],
            proof_gaps=[proof_gap],
        )
        for name in vitals
    }


def _proof_gaps(
    vitals: dict[str, Any],
    source_scorecard: dict[str, Any],
    product_value: dict[str, Any],
) -> list[str]:
    gaps = []
    for vital in vitals.values():
        gaps.extend(str(gap) for gap in _json_list(vital.get("proof_gaps")))
    gaps.extend(str(gap) for gap in _json_list(source_scorecard.get("proof_gaps")))
    gaps.extend(str(gap) for gap in _json_list(product_value.get("proof_gaps")))
    return sorted(dict.fromkeys(gaps))


def _ranked_findings(
    vitals: dict[str, Any],
    hard_failures: list[str],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = [
        {"severity": "critical", "vital": "hard_gate", "finding": failure}
        for failure in hard_failures
    ]
    for name, vital in vitals.items():
        score = vital.get("score")
        status = str(vital.get("status") or "")
        severity = "info"
        if status == "not_observed":
            severity = "proof_gap"
        elif isinstance(score, (int, float)) and score < 0.65:
            severity = "high"
        elif isinstance(score, (int, float)) and score < 0.85:
            severity = "medium"
        else:
            continue
        for finding in _json_list(vital.get("findings"))[:2]:
            findings.append(
                {
                    "severity": severity,
                    "vital": name,
                    "finding": str(finding),
                    "score": score,
                }
            )
    severity_order = {"critical": 0, "high": 1, "medium": 2, "proof_gap": 3, "info": 4}
    return sorted(
        findings,
        key=lambda row: (
            severity_order.get(str(row.get("severity")), 9),
            str(row.get("vital")),
        ),
    )


def _vital(
    *,
    score: float | None,
    metrics: dict[str, Any],
    findings: list[str],
    proof_gaps: list[str] | None = None,
) -> dict[str, Any]:
    clean_score = round(_clamp(score), 4) if score is not None else None
    return {
        "score": clean_score,
        "status": _status_from_score(clean_score),
        "metrics": _json_safe(metrics),
        "findings": findings,
        "proof_gaps": proof_gaps or [],
    }


def _dimension(source_scorecard: dict[str, Any], name: str) -> dict[str, Any]:
    return _json_obj(_json_obj(source_scorecard.get("dimensions")).get(name))


def _eval_score(evals: dict[str, Any], name: str) -> float | None:
    return _as_float_or_none(_json_obj(evals.get(name)).get("score"))


def _gold_value_class(*, sequence: str, family: str) -> str:
    text = f"{sequence} {family}".casefold()
    if "noise" in text:
        return "noise"
    if "future" in text or "validation" in text:
        return "counterevidence"
    if "decision" in text or "outcome" in text:
        return "decision"
    return "core_fact"


def _wave_class(sequence: str) -> str:
    text = sequence.casefold()
    if "noise" in text:
        return "noise"
    if "future" in text or "validation" in text:
        return "future_validation"
    if "outcome" in text:
        return "outcome"
    return "initial"


def _residual_kind_for_signal(row: dict[str, Any]) -> str | None:
    if row.get("gold_value_class") == "noise":
        return None
    final_fate = str(row.get("final_fate") or "")
    if final_fate == "trace_unresolved":
        return None
    if (
        row.get("gold_value_class") == "counterevidence"
        and final_fate not in {
            "counterevidence_attached",
            "falsifier_created",
            "evidence_attached",
            "model_updated",
            "edge_created",
            "relation_frame_created",
            "projection_updated",
            "decision_outcome_recorded",
            "self_improvement_event_created",
        }
    ):
        return "counterevidence_unattached"
    if final_fate in RESIDUAL_KIND_BY_FATE:
        return RESIDUAL_KIND_BY_FATE[final_fate]
    leak_flags = {str(value) for value in _json_list(row.get("leak_flags"))}
    if "think_success_without_durable_trace" in leak_flags:
        return "compression_uncertain"
    return None


def _residual_reason(row: dict[str, Any], residual_kind: str) -> str:
    final_fate = row.get("final_fate")
    if residual_kind == "counterevidence_unattached":
        return (
            "Counterevidence signal did not attach as evidence, contestation, "
            f"or falsifier; final_fate={final_fate}."
        )
    if residual_kind == "valuable_unmodeled":
        return f"Valuable signal did not reach durable model metabolism; final_fate={final_fate}."
    if residual_kind == "validation_dropped_value":
        return "Useful value appears to have been dropped during validation."
    if residual_kind == "compression_uncertain":
        return "Think completed without a durable trace proving model compression."
    if residual_kind == "relation_unanchored":
        return "Relationship evidence could not be anchored into an edge or relation frame."
    return f"Residual candidate produced by final_fate={final_fate}."


def _residual_summary(row: dict[str, Any], residual_kind: str) -> str:
    signal = row.get("signal_id") or row.get("observation_id") or "unknown signal"
    family = row.get("family") or "unknown family"
    return (
        f"{residual_kind} for {signal} in {family}; "
        f"fate={row.get('final_fate')}"
    )


def _repair_action_for_residual_kind(kind: str) -> str:
    return {
        "counterevidence_unattached": "attach_contest_or_falsifier_reading",
        "relation_unanchored": "promote_relation_claim_or_create_open_question",
        "valuable_unmodeled": "rerun_or_route_signal_to_model_update",
        "validation_dropped_value": "inspect_validation_drop_and_create_residual",
        "compression_uncertain": "summarize_residual_and_retry_model_absorption",
        "authority_blocked": "request_authorized_human_or_scope_repair",
        "open_question_needed": "create_or_update_model_open_question",
    }.get(kind, "inspect_residual_cluster")


def _retrieval_outcome_reward(row: dict[str, Any]) -> tuple[float, str]:
    final_fate = str(row.get("final_fate") or "")
    if final_fate in POSITIVE_OUTCOME_FATES:
        if final_fate in {"decision_outcome_recorded", "projection_updated"}:
            return 1.0, "downstream_product_or_decision_outcome"
        return 0.9, "durable_memory_outcome"
    if final_fate == "human_feedback_requested":
        return 0.65, "human_feedback_needed"
    if final_fate == "think_noop_justified":
        return 0.5, "justified_noop"
    if final_fate in LEAK_FATES or final_fate == "think_noop_suspicious":
        return 0.0, "no_durable_fate"
    return 0.25, "weak_or_unknown_outcome"


def _retrieval_learning_signal(
    reward: float,
    *,
    omitted_count: int,
    selected_context_count: int,
) -> str:
    if omitted_count and reward < 0.5:
        return "penalize_omission_or_packet_selection"
    if selected_context_count and reward >= 0.85:
        return "reinforce_selected_route"
    if selected_context_count and reward < 0.5:
        return "penalize_selected_route"
    if reward >= 0.85:
        return "reinforce_route_without_explicit_context_trace"
    return "observe_more_outcomes"


def _missing_evidence_statement(
    residual_kind: str,
    residuals: list[dict[str, Any]],
) -> str:
    count = len(residuals)
    if residual_kind == "counterevidence_unattached":
        return (
            f"{count} counterevidence signal(s) did not attach to the model layer; "
            "the missing structure is the contested belief or falsifier target."
        )
    if residual_kind == "valuable_unmodeled":
        return (
            f"{count} valuable signal(s) did not become durable memory; "
            "the missing structure is the model or relation they should update."
        )
    if residual_kind == "compression_uncertain":
        return (
            f"{count} Think no-op signal(s) lack proof of compression; "
            "the missing structure is the compact model delta or justified ignore."
        )
    if residual_kind == "validation_dropped_value":
        return (
            f"{count} validation-dropped signal(s) may contain useful value; "
            "the missing structure is the valid mutation shape."
        )
    return f"{count} residual signal(s) share missingness kind {residual_kind}."


def _latent_gap_falsifier(residual_kind: str) -> str:
    return {
        "counterevidence_unattached": (
            "A later trace shows the counterevidence was attached to a specific "
            "model as contestation or falsification."
        ),
        "valuable_unmodeled": (
            "A later trace shows the source observation was absorbed into a model, "
            "edge, relation, projection, or justified ignore."
        ),
        "compression_uncertain": (
            "A later trace shows the no-op was justified or the missing compact "
            "model delta was applied."
        ),
        "validation_dropped_value": (
            "A later validation trace shows the dropped operation was invalid "
            "noise rather than useful value."
        ),
    }.get(
        residual_kind,
        "A later trace resolves the residual without adding a new hypothesis.",
    )


def _next_evidence_needed(residual_kind: str) -> str:
    return {
        "counterevidence_unattached": (
            "Retrieve the likely target model and attach confirm/contest/falsify evidence."
        ),
        "valuable_unmodeled": (
            "Rerun metabolism for the source observation with model-spine context."
        ),
        "compression_uncertain": (
            "Inspect Think context_use and applied ops to prove justified no-op or missed delta."
        ),
        "validation_dropped_value": (
            "Inspect validation errors and convert valid dropped value into a residual or op."
        ),
    }.get(residual_kind, "Collect a targeted follow-up signal or human clarification.")


def _model_support_count(row: dict[str, Any]) -> int | None:
    if "supporting_event_ids" in row:
        return len(_json_list(row.get("supporting_event_ids")))
    if "supporting_events" in row:
        value = row.get("supporting_events")
        if isinstance(value, list):
            return len(value)
        if value is not None:
            return _as_int(value)
    if "supporting_model_ids" in row:
        return len(_json_list(row.get("supporting_model_ids")))
    if "supporting_models" in row:
        value = row.get("supporting_models")
        if isinstance(value, list):
            return len(value)
        if value is not None:
            return _as_int(value)
    return None


def _model_scope_size(row: dict[str, Any]) -> int:
    return len(_json_list(row.get("scope_actors"))) + len(
        _json_list(row.get("scope_entities"))
    )


def _model_scope_anchor_refs(row: dict[str, Any]) -> set[str]:
    refs = {
        f"actor:{actor_id}"
        for actor_id in _json_list(row.get("scope_actors"))
        if str(actor_id or "").strip()
    }
    for entity in _json_list(row.get("scope_entities")):
        if isinstance(entity, dict):
            kind = str(
                entity.get("type")
                or entity.get("kind")
                or entity.get("object_type")
                or "entity"
            ).strip()
            value = str(
                entity.get("id")
                or entity.get("object_id")
                or entity.get("key")
                or entity.get("name")
                or ""
            ).strip()
            if kind and value:
                refs.add(f"{kind}:{value}")
        elif str(entity or "").strip():
            refs.add(f"entity:{entity}")
    return refs


def _company_object_mentions(row: dict[str, Any]) -> set[str]:
    natural = str(row.get("natural") or "").casefold()
    mentions: set[str] = set()
    if any(term in natural for term in ("customer", "renewal", "account")):
        mentions.add("customer")
    if any(
        term in natural
        for term in ("employee", "owner", "manager", "founder", "team", "hiring")
    ):
        mentions.add("actor")
    if any(
        term in natural
        for term in ("commitment", "deadline", "deliverable", "blocked", "blocker")
    ):
        mentions.add("commitment")
    if any(term in natural for term in ("decision", "approved", "revisit")):
        mentions.add("decision")
    if any(
        term in natural
        for term in ("recurring", "repeated", "weekly", "monthly", "cadence")
    ):
        mentions.add("recurring_event")
    return mentions


def _mentions_are_bound(row: dict[str, Any], mention_types: set[str]) -> bool:
    entity_types = {
        str(entity.get("type") or entity.get("kind") or entity.get("object_type") or "")
        for entity in _json_list(row.get("scope_entities"))
        if isinstance(entity, dict)
    }
    has_actor = bool(_json_list(row.get("scope_actors"))) or bool(
        {"actor", "employee"} & entity_types
    )
    checks = {
        "customer": bool({"customer", "customer_resource"} & entity_types),
        "actor": has_actor,
        "commitment": bool({"commitment", "goal"} & entity_types),
        "decision": "decision" in entity_types,
        "recurring_event": bool(
            {"event", "recurring_event", "pattern"} & entity_types
        ),
    }
    return all(checks.get(kind, True) for kind in mention_types)


def _expected_customer_keys(bundle: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for row in [
        *_json_list(bundle.get("planned_signals")),
        *_json_list(bundle.get("signal_manifest")),
    ]:
        if not isinstance(row, dict):
            continue
        value = row.get("customer") or row.get("customer_name") or row.get("account")
        if value:
            keys.add(str(value))
    return keys


def _is_wrapper_model(row: dict[str, Any]) -> bool:
    natural = str(row.get("natural") or "").casefold()
    return any(phrase in natural for phrase in MODEL_WRAPPER_PHRASES)


def _is_conjunction_heavy_model(row: dict[str, Any]) -> bool:
    natural = str(row.get("natural") or "")
    if len(natural) < 90:
        return False
    marker_count = sum(natural.casefold().count(marker) for marker in MODEL_CONJUNCTION_MARKERS)
    comma_count = natural.count(",")
    return marker_count + comma_count >= 4


def _count_by(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        counts[value] = counts.get(value, 0) + 1
    return counts


def _group_by(
    rows: list[dict[str, Any]],
    key: str,
) -> dict[str, list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        value = str(row.get(key) or "unknown")
        groups.setdefault(value, []).append(row)
    return groups


async def _table_exists(conn: Any, table: str) -> bool:
    return bool(await conn.fetchval("SELECT to_regclass($1)", f"public.{table}"))


async def _column_exists(conn: Any, table: str, column: str) -> bool:
    return bool(
        await conn.fetchval(
            """
            SELECT EXISTS (
              SELECT 1
              FROM information_schema.columns
              WHERE table_schema = 'public'
                AND table_name = $1
                AND column_name = $2
            )
            """,
            table,
            column,
        )
    )


def _append_trace(
    by_observation: dict[str, dict[str, list[dict[str, Any]]]],
    observation_id: str,
    key: str,
    row: dict[str, Any],
) -> None:
    if observation_id not in by_observation:
        return
    by_observation[observation_id].setdefault(key, []).append(row)


def _trigger_observation_ids(row: dict[str, Any], allowed: list[str]) -> list[str]:
    allowed_set = set(allowed)
    out: list[str] = []
    observation_id = str(row.get("observation_id") or "")
    if observation_id in allowed_set:
        out.append(observation_id)
    payload = _json_obj(row.get("payload"))
    for value in _json_list(payload.get("batch_observation_ids")):
        value_str = str(value)
        if value_str in allowed_set:
            out.append(value_str)
    return sorted(dict.fromkeys(out))


def _ids(rows: list[Any], *, key: str = "id") -> list[str]:
    out = []
    for row in rows:
        if isinstance(row, dict) and row.get(key):
            out.append(str(row[key]))
    return sorted(dict.fromkeys(out))


def _context_ids(think_rows: list[dict[str, Any]], key: str) -> list[str]:
    out: list[str] = []
    for row in think_rows:
        ops = _json_obj(row.get("ops_applied"))
        context = _json_obj(ops.get("context_use"))
        for value in _json_list(context.get(key)):
            out.append(str(value))
    return sorted(dict.fromkeys(out))


def _projection_subjects(rows: list[dict[str, Any]]) -> list[str]:
    out = []
    for row in rows:
        name = row.get("projection_name")
        subject = row.get("subject_key")
        if name and subject:
            out.append(f"{name}:{subject}")
    return sorted(dict.fromkeys(out))


def _product_surface_refs(rows: list[dict[str, Any]]) -> list[str]:
    out = []
    for row in rows:
        event_type = str(row.get("event_type") or "")
        if event_type:
            out.append(f"inquiry_outcome:{event_type}")
    return sorted(dict.fromkeys(out))


def _uuid_strings(value: Any) -> list[str]:
    values = value if isinstance(value, list) else []
    out = []
    for item in values:
        coerced = _coerce_uuid(item)
        if coerced is not None:
            out.append(str(coerced))
    return sorted(dict.fromkeys(out))


def _coerce_uuid(value: Any) -> UUID | None:
    if value is None:
        return None
    try:
        return UUID(str(value))
    except (TypeError, ValueError):
        return None


def _record_to_dict(row: Any) -> dict[str, Any]:
    if row is None:
        return {}
    if isinstance(row, dict):
        return _json_safe(row)
    return _json_safe(dict(row))


def _db_trace_counts(by_observation: dict[str, dict[str, list[dict[str, Any]]]]) -> dict[str, Any]:
    totals: dict[str, int] = {}
    resolved = 0
    for trace in by_observation.values():
        if trace:
            resolved += 1
        for key, rows in trace.items():
            totals[key] = totals.get(key, 0) + len(rows)
    return {
        "observations_with_any_trace": resolved,
        "observation_count": len(by_observation),
        "trace_coverage": _ratio(resolved, len(by_observation)),
        "row_counts": totals,
    }


def _trace_summary(db_trace: dict[str, Any] | None) -> dict[str, Any]:
    if not db_trace:
        return {"available": False}
    summary = _json_obj(db_trace.get("summary"))
    company_learning = _json_obj(db_trace.get("company_learning_evaluation"))
    return {
        "available": bool(db_trace.get("available")),
        "tenant_id": db_trace.get("tenant_id"),
        "observation_count": db_trace.get("observation_count"),
        "summary": summary,
        "table_presence": _json_obj(db_trace.get("table_presence")),
        "error": db_trace.get("error"),
        "company_physics_status": company_learning.get("status"),
        "company_physics_available": company_learning.get("available") is True,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    value = json.loads(path.read_text(encoding="utf-8"))
    return value if isinstance(value, dict) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        value = json.loads(line)
        if isinstance(value, dict):
            rows.append(value)
    return rows


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(
        json.dumps(_json_safe(payload), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(
        "".join(json.dumps(_json_safe(row), sort_keys=True) + "\n" for row in rows),
        encoding="utf-8",
    )


def _json_obj(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _json_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _json_safe(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe(v) for v in value]
    return value


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _as_float(value: Any, default: float = 0.0) -> float:
    value_float = _as_float_or_none(value)
    return default if value_float is None else value_float


def _as_float_or_none(value: Any) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ratio(numerator: int | float, denominator: int | float) -> float:
    return float(numerator) / float(denominator) if denominator else 0.0


def _avg(values: list[float | None]) -> float | None:
    usable = [float(value) for value in values if isinstance(value, (int, float))]
    if not usable:
        return None
    return sum(usable) / len(usable)


def _clamp(value: float | None) -> float:
    if value is None:
        return 0.0
    return max(0.0, min(1.0, float(value)))


def _status_from_score(score: float | None) -> str:
    if score is None:
        return "not_observed"
    if score >= 0.85:
        return "ok"
    if score >= 0.65:
        return "watch"
    return "weak"


def _fmt_score(value: Any) -> str:
    return "n/a" if value is None else f"{float(value):.4f}"


def _short_metrics(metrics: dict[str, Any]) -> str:
    if not metrics:
        return ""
    parts = []
    for key in sorted(metrics)[:5]:
        value = metrics[key]
        if isinstance(value, float):
            value = round(value, 4)
        if isinstance(value, (dict, list)):
            continue
        parts.append(f"`{key}`={value}")
    return ", ".join(parts)
