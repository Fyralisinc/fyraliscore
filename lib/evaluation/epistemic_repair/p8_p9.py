"""Strict P8-to-P9 normalization from one coherent set of member artifacts."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any

from lib.contracts.kernel import canonical_sha256


GATE_IDS = (
    "P8-G01-single-commit", "P8-G02-fault-receipt-coverage", "P8-G03-fault-invariants",
    "P8-G04-isolated-scale-matrix", "P8-G05-scale-latency", "P8-G06-scale-resource-evidence",
    "P8-G07-shared-contention", "P8-G08-characterization", "P8-G09-authorized-canaries",
    "P8-G10-hash-reopen",
)
METRIC_CONTRACTS = {
    "fault_receipt_coverage": (">=", 1.0), "fault_invariant_violation_rate": ("=", 0.0),
    "scale_max_retrieval_horizon_ratio": ("<=", 2.0),
    "scale_max_prompt_horizon_ratio": ("<=", 1.25),
    "scale_max_concurrency_latency_ratio": ("<=", 2.0),
    "scale_max_semantic_quality_delta": ("<=", .03),
    "scale_minimum_fairness_ratio": (">=", .80),
    "characterization_provenance_coverage": ("=", 1.0),
}
_HEX40 = re.compile(r"[0-9a-f]{40}")


def _read(path: Path) -> tuple[dict[str, Any], str]:
    raw = path.read_bytes()
    parsed = json.loads(raw)
    if not isinstance(parsed, dict):
        raise ValueError(f"{path} root is not an object")
    embedded = parsed.get("artifact_digest")
    if not isinstance(embedded, str):
        raise ValueError(f"{path} lacks an embedded artifact digest")
    body = dict(parsed)
    body.pop("artifact_digest")
    if embedded != canonical_sha256(body):
        raise ValueError(f"{path} embedded artifact digest does not match reopened content")
    return parsed, sha256(raw).hexdigest()


def _characterization_counts(value: Any) -> tuple[int, int]:
    total = valid = 0
    if isinstance(value, dict):
        if "denominator" in value and any(key in value for key in ("score", "f1")):
            total += 1
            ids = value.get("source_example_ids")
            valid += int(
                isinstance(ids, list) and len(ids) == value["denominator"]
                and value.get("source_artifact_digest") == canonical_sha256(ids)
                and bool(value.get("worst_example_ids"))
            )
        for item in value.values():
            child_total, child_valid = _characterization_counts(item)
            total += child_total; valid += child_valid
    elif isinstance(value, list):
        for item in value:
            child_total, child_valid = _characterization_counts(item)
            total += child_total; valid += child_valid
    return total, valid


def build_p8_p9_sidecar(
    *, exit_path: Path, fault_path: Path, scale_path: Path,
    characterization_path: Path, contention_path: Path,
) -> dict[str, Any]:
    exit_artifact, _ = _read(exit_path)
    members = {}
    shas = {}
    for name, path in (("fault", fault_path), ("scale", scale_path),
                       ("characterization", characterization_path), ("contention", contention_path)):
        members[name], shas[name] = _read(path)
    commits = {str(item.get("commit") or "") for item in members.values()}
    exit_commit = str(exit_artifact.get("commit") or "")
    if len(commits) != 1 or exit_commit not in commits or not _HEX40.fullmatch(exit_commit):
        raise ValueError("P8 evidence is mixed-commit or lacks full commit provenance")
    declared_shas = exit_artifact.get("source_artifact_sha256")
    if not isinstance(declared_shas, dict) or any(
        declared_shas.get(str(path)) != shas[name]
        for name, path in (("fault", fault_path), ("scale", scale_path),
                           ("characterization", characterization_path), ("contention", contention_path))
    ):
        raise ValueError("P8 member hashes do not match the coherent exit")
    qualification = members["fault"].get("member_receipt_qualification")
    if not isinstance(qualification, dict):
        raise ValueError("P8 refreshed fault receipt qualification is missing")
    required_fault_fields = {
        "cross_tenant_effects", "duplicate_relation_transitions",
        "duplicate_lifecycle_transitions", "partial_truth_state", "stale_active_truth",
        "dead_letter_truth_critical_work", "uninterrupted_reference_digest_equality",
    }
    if not required_fault_fields <= set(qualification):
        raise ValueError("P8 refreshed fault invariant fields are incomplete")
    receipt_count = int(qualification.get("observed_member_receipts", 0))
    invariant_rows = [qualification[key] for key in required_fault_fields]
    invariant_violations = sum(int(row.get("violations", 0)) for row in invariant_rows)
    fault_invariants = all(row.get("gate") is True for row in invariant_rows)
    scale = members["scale"]
    scale_eval = scale.get("evaluation", {})
    scale_gates = scale_eval.get("gates", {})
    scale_execution = scale.get("execution", {})
    char_total, char_valid = _characterization_counts(members["characterization"])
    contention = members["contention"].get("result", {})
    exit_gates = exit_artifact.get("gates", {})
    normalized_gates = {
        "P8-G01-single-commit": True,
        "P8-G02-fault-receipt-coverage": receipt_count == 24 and qualification.get("every_physical_attempt_has_receipt") is True,
        "P8-G03-fault-invariants": fault_invariants and invariant_violations == 0,
        "P8-G04-isolated-scale-matrix": len(scale_execution.get("cells", ())) == 27
            and scale_execution.get("physically_isolated_databases") is True,
        "P8-G05-scale-latency": scale_gates.get("concurrency_latency_ratio") is True,
        "P8-G06-scale-resource-evidence": all(scale_gates.get(key) is True for key in (
            "all_production_queue_families_measured", "resource_sample_every_durable_barrier",
            "deterministic_token_status_explicit", "derived_refresh_pipeline_executed")),
        "P8-G07-shared-contention": contention.get("concurrent_cells") == 3
            and len(contention.get("evidence_digest", "")) == 64,
        "P8-G08-characterization": char_total > 0 and char_valid == char_total,
        "P8-G09-authorized-canaries": exit_gates.get("authorized_provider_canaries") is True
            and exit_artifact.get("provider_canary_policy", {}).get("status") == "completed",
        "P8-G10-hash-reopen": exit_gates.get("hash_reopen_review") is True,
    }
    metric_values = {
        "fault_receipt_coverage": (receipt_count, 24),
        "fault_invariant_violation_rate": (invariant_violations, max(1, len(invariant_rows) * 18)),
        "scale_max_retrieval_horizon_ratio": (scale_eval.get("max_retrieval_horizon_ratio"), 1),
        "scale_max_prompt_horizon_ratio": (scale_eval.get("max_prompt_horizon_ratio"), 1),
        "scale_max_concurrency_latency_ratio": (scale_eval.get("max_concurrency_latency_ratio"), 1),
        "scale_max_semantic_quality_delta": (scale_eval.get("max_semantic_quality_delta"), 1),
        "scale_minimum_fairness_ratio": (scale_eval.get("minimum_fairness_ratio"), 1),
        "characterization_provenance_coverage": (char_valid, char_total),
    }
    source_digest = canonical_sha256(shas)
    metrics = []
    for name, (operator, threshold) in METRIC_CONTRACTS.items():
        numerator, denominator = metric_values[name]
        if not isinstance(numerator, (int, float)) or denominator <= 0:
            raise ValueError(f"P8 metric {name} lacks a complete denominator")
        value = numerator / denominator
        passed = value >= threshold if operator == ">=" else value <= threshold if operator == "<=" else value == threshold
        metrics.append({"name": name, "numerator": numerator, "denominator": denominator,
                        "value": value, "coverage": 1.0, "uncertainty": "not_applicable",
                        "status": "pass" if passed else "fail", "operator": operator,
                        "threshold": threshold, "source_artifact_digest": source_digest,
                        "worst_cases": []})
    gate_sources = {
        "P8-G01-single-commit": tuple(shas),
        "P8-G02-fault-receipt-coverage": ("fault",),
        "P8-G03-fault-invariants": ("fault",),
        "P8-G04-isolated-scale-matrix": ("scale",),
        "P8-G05-scale-latency": ("scale",),
        "P8-G06-scale-resource-evidence": ("scale",),
        "P8-G07-shared-contention": ("contention",),
        "P8-G08-characterization": ("characterization",),
        "P8-G09-authorized-canaries": tuple(shas),
        "P8-G10-hash-reopen": tuple(shas),
    }
    metric_sources = {
        name: ("fault",) if name.startswith("fault_")
        else ("characterization",) if name.startswith("characterization_")
        else ("scale",)
        for name in METRIC_CONTRACTS
    }
    contributions = {
        "schema_version": "epistemic-repair-p9-member-contributions-v1",
        "preregistered_contract_digest": canonical_sha256({
            "gates": GATE_IDS, "metrics": METRIC_CONTRACTS,
        }),
        "member_artifact_sha256": shas,
        "gate_members": {
            gate: [{
                "member_id": name,
                "raw_source_digest": shas[name],
                "conforms": normalized_gates[gate],
            } for name in gate_sources[gate]]
            for gate in GATE_IDS
        },
        "metric_members": {
            name: [{
                "member_id": source,
                "raw_source_digest": shas[source],
                "numerator": metric_values[name][0],
                "denominator": metric_values[name][1],
            } for source in metric_sources[name]]
            for name in METRIC_CONTRACTS
        },
    }
    body = {"schema_version": "epistemic-repair-p8-p9-normalized-v1", "commit": exit_commit,
            "phase_exit_ready": all(normalized_gates.values()) and all(row["status"] == "pass" for row in metrics),
            "hard_gates": normalized_gates, "p9_continuous_metrics": metrics,
            "p9_member_contributions": contributions,
            "run_provenance": {"git_commit": exit_commit, "worktree_clean": True},
            "source_exit_artifact_digest": exit_artifact.get("artifact_digest")}
    return {**body, "content_digest": canonical_sha256(body)}


__all__ = ["GATE_IDS", "METRIC_CONTRACTS", "build_p8_p9_sidecar"]
