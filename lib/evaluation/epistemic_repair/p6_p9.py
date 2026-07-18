"""Strict P6-to-P9 normalization from frozen execution and post-freeze scores."""

from __future__ import annotations

from copy import deepcopy
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

from lib.contracts.kernel import canonical_sha256


GATE_IDS = (
    "immutable_inputs_match", "complete_execution", "exact_300_signals_12_batches",
    "complete_signal_fates", "complete_boundary_mention_mutation_fates",
    "high_consequence_incidents_zero", "wrapper_control_models_zero",
    "active_candidate_review_leakage_zero", "invalid_relations_zero",
    "external_outcome_instruction_leakage_zero", "one_coherent_synthesis_per_thesis",
    "all_truth_critical_barriers_close", "single_commit_provider_configuration",
    "postfreeze_evidence_digest_valid", "durable_call_receipts",
    "exact_token_usage_receipts", "zero_seed_canonical_truth",
    "semantic_evidence_metadata_coherent", "all_hg_gates",
)
METRIC_SPECS: dict[str, tuple[str, float]] = {
    "boundary_b_cubed_f1": (">=", .90),
    "selected_context_contamination": ("<=", .05),
    "sufficient_context_recall": (">=", .95),
    "exact_mention_f1": (">=", .92),
    "entity_type_accuracy": (">=", .95),
    "canonical_link_precision": (">=", .98),
    "canonical_link_recall": (">=", .90),
    "uncertainty_fate_precision": (">=", .95),
    "uncertainty_fate_coverage": (">=", .95),
    "atomic_claim_precision": (">=", .90),
    "atomic_claim_recall": (">=", .85),
    "atomic_claim_f1": (">=", .875),
    "evidence_lineage_coverage": ("=", 1.0),
    "scope_precision": (">=", .95),
    "scope_recall": (">=", .90),
    "direct_thesis_accuracy": ("=", 1.0),
    "mean_thesis_facet_completeness": (">=", .90),
    "relation_joint_precision": (">=", .95),
    "relation_joint_recall": (">=", .80),
    "lifecycle_expected_transition_accuracy": ("=", 1.0),
    "historical_reopening_reason_coverage": ("=", 1.0),
    "mature_actual_model_use_share": (">=", .70),
    "mature_unnecessary_historical_observation_use": ("<=", .10),
    "resolved_outcome_model_ece": ("<=", .15),
    "resolved_outcome_model_brier": ("<=", .20),
    "selected_context_utilization": (">=", .80),
    "false_model_relation_from_noise": ("=", 0.0),
    "duplicate_causal_credit_fanout": ("=", 0.0),
    "clean_t1_p95_seconds": ("<=", 120.0),
    "clean_max_over_median": ("<=", 3.0),
    "metered_llm_calls_per_signal": ("<=", .08),
    "question_planning_batch_share": ("<=", .25),
    "truth_critical_pending_at_barriers": ("=", 0.0),
    "refresh_key_duplicate_processing_ratio": ("<=", 1.10),
}
CALIBRATION_METRICS = {"resolved_outcome_model_ece", "resolved_outcome_model_brier"}
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")


def _read(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    value = json.loads(raw)
    if not isinstance(value, dict):
        raise ValueError(f"{path} root is not an object")
    return value, sha256(raw).hexdigest()


def _passes(value: float, operator: str, threshold: float) -> bool:
    return value >= threshold if operator == ">=" else value <= threshold if operator == "<=" else value == threshold


def build_p6_p9_sidecar(
    *, execution_path: Path, evidence_path: Path, score_path: Path,
) -> dict[str, Any]:
    execution, execution_sha = _read(execution_path)
    evidence_artifact, evidence_sha = _read(evidence_path)
    score, score_sha = _read(score_path)
    if execution.get("schema_version") != "epistemic-repair-p6-production-think-v1":
        raise ValueError("unexpected P6 execution schema")
    if evidence_artifact.get("schema_version") != "epistemic-repair-p6-postfreeze-evidence-artifact-v1":
        raise ValueError("unexpected P6 post-freeze evidence artifact schema")
    if score.get("schema_version") != "epistemic-repair-p6-postfreeze-score-v1":
        raise ValueError("unexpected P6 post-freeze score schema")
    evidence_artifact_body = dict(evidence_artifact)
    evidence_artifact_digest = evidence_artifact_body.pop("content_digest", None)
    if (
        not isinstance(evidence_artifact_digest, str)
        or evidence_artifact_digest != canonical_sha256(evidence_artifact_body)
        or evidence_artifact.get("raw_execution_digest") != canonical_sha256(execution)
    ):
        raise ValueError("P6 evidence artifact digest or raw execution binding is invalid")
    score_body = dict(score)
    score_digest = score_body.pop("content_digest", None)
    if not isinstance(score_digest, str) or score_digest != canonical_sha256(score_body):
        raise ValueError("P6 score content digest does not match reopened content")
    input_digests = score.get("input_digests")
    if not isinstance(input_digests, dict) or set(input_digests) != {
        "raw_execution", "sealed_population", "preregistration",
    } or any(not _HEX64.fullmatch(str(value or "")) for value in input_digests.values()):
        raise ValueError("P6 score input digest contract is incomplete")
    frozen_execution = {**execution, "postfreeze_evidence": evidence_artifact.get("postfreeze_evidence")}
    if input_digests["raw_execution"] != canonical_sha256(frozen_execution):
        raise ValueError("P6 score is not bound to the canonical raw-plus-evidence composition")
    if execution.get("population_digest") != input_digests["sealed_population"]:
        raise ValueError("P6 execution and score bind different sealed populations")
    evidence = evidence_artifact.get("postfreeze_evidence")
    if not isinstance(evidence, dict):
        raise ValueError("P6 post-freeze evidence is missing")
    evidence_body = dict(evidence)
    evidence_digest = evidence_body.pop("source_digest", None)
    if not isinstance(evidence_digest, str) or evidence_digest != canonical_sha256(evidence_body):
        raise ValueError("P6 post-freeze source digest is missing or invalid")
    receipts = evidence.get("query_receipts")
    if not isinstance(receipts, list) or not receipts or any(
        not _HEX64.fullmatch(str(row.get("result_digest") or "")) for row in receipts
    ):
        raise ValueError("P6 query receipt source digests are incomplete")

    gates = score.get("hard_gates")
    metrics = score.get("continuous_metrics")
    if (
        not isinstance(gates, dict) or set(gates) != set(GATE_IDS)
        or len(gates) != len(GATE_IDS)
        or any(not isinstance(value, bool) for value in gates.values())
    ):
        raise ValueError("P6 score hard gates do not exactly match the preregistered 19-gate contract")
    if not isinstance(metrics, dict) or set(metrics) != set(METRIC_SPECS) or len(metrics) != len(METRIC_SPECS):
        raise ValueError("P6 score metrics do not exactly match the preregistered 34-metric contract")

    provenance = execution.get("run_provenance") or {}
    config = execution.get("expected_llm_configuration") or {}
    commit = provenance.get("git_commit")
    provider, model, transport = (config.get("provider"), config.get("model"), config.get("transport"))
    if (
        not _HEX40.fullmatch(str(commit or "")) or provenance.get("worktree_clean") is not True
        or not all(isinstance(item, str) and item for item in (provider, model, transport))
        or execution.get("mixed_llm_attempt_count") != 0
    ):
        raise ValueError("P6 execution has missing or mixed commit/provider/model/transport identity")
    if evidence_artifact.get("commit") != commit:
        raise ValueError("P6 evidence artifact has mixed commit identity")
    attempts = execution.get("llm_attempt_receipts")
    if not isinstance(attempts, list) or not attempts or any(
        row.get("provider") != provider or row.get("model") != model
        or row.get("transport", transport) != transport
        for row in attempts
    ):
        raise ValueError("P6 attempt receipts have missing or mixed provider/model/transport identity")

    normalized_metrics = []
    metric_members: dict[str, list[dict[str, Any]]] = {}
    for name, (operator, threshold) in METRIC_SPECS.items():
        row = metrics[name]
        if not isinstance(row, dict) or row.get("operator") != operator or row.get("threshold") != threshold:
            raise ValueError(f"P6 metric contract mismatch: {name}")
        source_ids, worst_cases = row.get("source_ids"), row.get("worst_cases")
        if not isinstance(source_ids, list) or not isinstance(worst_cases, list):
            raise ValueError(f"P6 metric lacks source IDs or worst cases: {name}")
        status = row.get("status")
        uncertainty: str | dict[str, Any] = "not_applicable"
        if status == "insufficient_population":
            eligible = row.get("denominator")
            if name not in CALIBRATION_METRICS or not isinstance(eligible, int) or not 0 < eligible < 20 or row.get("numerator") is not None:
                raise ValueError(f"invalid calibration insufficient-population state: {name}")
            numerator, denominator = None, eligible
            uncertainty = {
                "status": "insufficient_population", "eligible_population": eligible,
                "minimum_required": 20, "value_interpretation": "not_observed",
            }
            normalized_status = "insufficient_population"
        else:
            numerator, denominator = row.get("numerator"), row.get("denominator")
            if status not in {"pass", "fail"} or not isinstance(numerator, (int, float)) or not isinstance(denominator, (int, float)) or denominator <= 0:
                raise ValueError(f"P6 metric is unmeasured, missing, or malformed: {name}")
            value = numerator / denominator
            if not isinstance(row.get("value"), (int, float)) or abs(float(row["value"]) - value) > 1e-12:
                raise ValueError(f"P6 metric arithmetic mismatch: {name}")
            passed = _passes(value, operator, threshold)
            if (status == "pass") != passed:
                raise ValueError(f"P6 metric status mismatch: {name}")
            normalized_status = status
        member = {
            "member_id": f"{name}:aggregate", "raw_source_digest": score_sha,
            "numerator": numerator, "denominator": denominator,
            "source_ids": deepcopy(source_ids), "worst_cases": deepcopy(worst_cases),
        }
        metric_members[name] = [member]
        normalized_metrics.append({
            "name": name, "numerator": numerator, "denominator": denominator,
            "value": None if numerator is None else numerator / denominator,
            "coverage": 1.0, "uncertainty": uncertainty,
            "status": normalized_status, "operator": operator, "threshold": threshold,
            "source_artifact_digest": score_digest, "source_ids": deepcopy(source_ids),
            "worst_cases": deepcopy(worst_cases),
        })

    contributions = {
        "schema_version": "epistemic-repair-p9-member-contributions-v1",
        "preregistered_contract_digest": canonical_sha256({"gates": GATE_IDS, "metrics": METRIC_SPECS}),
        "source_artifact_sha256": {
            "execution": execution_sha, "evidence": evidence_sha, "score": score_sha,
        },
        "gate_members": {name: [{
            "member_id": name, "raw_source_digest": score_sha, "conforms": gates[name],
        }] for name in GATE_IDS},
        "metric_members": metric_members,
        "member_source_digests": sorted({score_sha}),
    }
    body = {
        "schema_version": "epistemic-repair-p6-p9-normalized-v1", "commit": commit,
        "phase_exit_ready": bool(
            score.get("phase_exit_ready") is True and all(gates.values())
            and all(row["status"] == "pass" for row in normalized_metrics)
        ),
        "hard_gates": deepcopy(gates), "p9_continuous_metrics": normalized_metrics,
        "p9_member_contributions": contributions,
        "run_provenance": {
            "git_commit": commit, "worktree_clean": True, "provider": provider,
            "model": model, "transport": transport,
        },
        "source_execution_sha256": execution_sha, "source_evidence_sha256": evidence_sha,
        "source_score_sha256": score_sha,
        "source_evidence_content_digest": evidence_artifact_digest,
        "source_score_content_digest": score_digest, "source_postfreeze_evidence_digest": evidence_digest,
    }
    return {**body, "content_digest": canonical_sha256(body)}


__all__ = ["CALIBRATION_METRICS", "GATE_IDS", "METRIC_SPECS", "build_p6_p9_sidecar"]
