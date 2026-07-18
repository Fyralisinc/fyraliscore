"""Typed P0-P5 normalization preflight; never upgrades summary-only evidence."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

from lib.contracts.kernel import canonical_sha256


_HEX40 = re.compile(r"[0-9a-f]{40}")

PHASE_CONTRACTS = {
    "p0": {
        "gates": ("P0-baseline-integrity", "P0-preregistration-integrity", "P0-inventory-completeness"),
        "metrics": (),
        "members": ("inventory_receipts", "validation_receipts", "preregistration_receipts"),
        "command": "rerun P0 inventory and preregistration validation on the release commit",
    },
    "p1": {
        "gates": ("HG-01_benchmark_blindness", "HG-13_observability_integrity"),
        "metrics": ("attempt_receipt_coverage", "count_reconciliation", "cost_coverage", "timing_reconciliation"),
        "members": ("attempt_history", "batches", "hook_scan", "cost_reconciliation"),
        "command": "rerun P1 deterministic observability plus authorized exact Codex receipts",
    },
    "p2": {
        "gates": tuple(f"HG-{index:02d}" for index in range(4, 11)),
        "metrics": ("active_unexplained_perfect_confidence_relation_rate", "active_wrapper_contamination",
                    "background_repair_latency_ms", "evidence_lineage_coverage",
                    "lifecycle_transition_latency_ms", "relation_joint_accuracy", "scope_precision",
                    "semantic_duplicate_absorption"),
        "members": ("case_results", "command_receipts", "race_results", "truth_snapshots"),
        "command": "rerun P2 truth-kernel and race probes on the release commit",
    },
    "p3": {
        "gates": ("HG-02", "HG-03", "HG-06", "HG-14"),
        "metrics": ("b_cubed_boundary_f1", "canonical_link_precision", "canonical_link_recall",
                    "context_budget_adherence", "correction_replay_convergence_coverage", "exact_mention_f1",
                    "future_context_exclusion", "grounding_fate_accuracy", "pairwise_boundary_precision",
                    "pairwise_boundary_recall", "safe_abstention_precision", "selected_context_contamination",
                    "sufficient_context_recall", "type_accuracy"),
        "members": ("member_receipts", "correction_receipts", "sealed_manifest"),
        "command": "rerun P3 sealed perception/grounding population on the release commit",
    },
    "p4": {
        "gates": ("HG-10", "HG-11", "HG-12", "HG-13"),
        "metrics": ("causal_barrier_p95_seconds", "delayed_attribution_coverage",
                    "duplicate_refresh_key_processing_ratio", "immediate_attribution_coverage",
                    "late_actual_model_use_share", "late_historical_observation_selected_count",
                    "late_unnecessary_historical_observation_count", "late_unnecessary_historical_observation_use",
                    "optional_queue_growth_slope_after_drain", "selected_context_utilization"),
        "members": ("batch_results", "telemetry_reconciliation", "component_checks"),
        "command": "rerun P4 online causal closure and feedback attribution on the release commit",
    },
    "p5": {
        "gates": tuple(f"P5-HG-{index:02d}" for index in range(1, 11)),
        "metrics": ("accepted_model_retrieval_and_reference", "barrier_completion", "batch_cardinality",
                    "corrected_state_reuse", "cross_tenant_contamination", "exact_model_falsification",
                    "explicit_signal_fate_coverage", "normalized_signal_persistence", "provider_independence",
                    "relation_or_no_relation_correctness", "semantic_restraint", "stale_truth_exclusion"),
        "members": ("signal_receipts", "barrier_receipts", "vertical_receipt", "database_evidence"),
        "command": "rerun P5 vertical canary on the release commit",
    },
}


@dataclass(frozen=True)
class RegenerationRequirement:
    phase: str
    status: str
    source_path: str
    source_sha256: str | None
    release_commit: str
    required_gate_ids: tuple[str, ...]
    required_metric_ids: tuple[str, ...]
    missing_evidence: tuple[str, ...]
    regeneration_command: str
    summary_flags_trusted: bool = False


def assess_phase(*, phase: str, source_path: Path, release_commit: str) -> dict[str, Any]:
    if phase not in PHASE_CONTRACTS:
        raise ValueError("phase must be p0 through p5")
    if not _HEX40.fullmatch(release_commit):
        raise ValueError("release commit must be a full lowercase 40-character SHA")
    contract = PHASE_CONTRACTS[phase]
    missing: list[str] = []
    source_sha = None
    try:
        raw = source_path.read_bytes()
        source_sha = sha256(raw).hexdigest()
        artifact = json.loads(raw)
        if not isinstance(artifact, dict):
            raise ValueError("root_not_object")
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        artifact = {}
        missing.append(f"readable_source_artifact:{type(exc).__name__}")
    provenance = artifact.get("run_provenance") if isinstance(artifact.get("run_provenance"), dict) else {}
    observed_commit = artifact.get("commit") or provenance.get("git_commit")
    if observed_commit != release_commit:
        missing.append("full_release_commit_provenance")
    if provenance.get("worktree_clean") is not True:
        missing.append("clean_worktree_provenance")
    for key in contract["members"]:
        value = artifact.get(key)
        if value is None or value == [] or value == {}:
            missing.append(f"member_evidence:{key}")
    # Normalization requires raw per-member contributions. Existing scalar
    # continuous_metrics and declared hard_gates are summaries, never inputs.
    contributions = artifact.get("p9_member_contributions")
    if not isinstance(contributions, dict):
        missing.append("p9_member_contributions")
    else:
        if set(contributions.get("gate_members", {})) != set(contract["gates"]):
            missing.append("exact_gate_member_denominators")
        if set(contributions.get("metric_members", {})) != set(contract["metrics"]):
            missing.append("exact_metric_member_denominators")
        if not contributions.get("preregistered_contract_digest"):
            missing.append("preregistered_metric_contract_digest")
    requirement = RegenerationRequirement(
        phase=phase, status="rerun_required" if missing else "normalization_ready",
        source_path=str(source_path), source_sha256=source_sha, release_commit=release_commit,
        required_gate_ids=contract["gates"], required_metric_ids=contract["metrics"],
        missing_evidence=tuple(sorted(set(missing))), regeneration_command=contract["command"],
    )
    body = asdict(requirement)
    return {**body, "requirement_digest": canonical_sha256(body)}


__all__ = ["PHASE_CONTRACTS", "RegenerationRequirement", "assess_phase"]
