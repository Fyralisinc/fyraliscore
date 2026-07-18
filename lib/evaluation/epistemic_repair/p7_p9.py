"""Strict P7 post-freeze oracle to P9 normalized sidecar."""

from __future__ import annotations

from copy import deepcopy
import re
from typing import Any, Mapping

from lib.contracts.kernel import canonical_sha256


_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")

P7_ORACLE_GATES = (
    "all_failures_preserved",
    "corrupted_memory_safe_within_two_batches",
    "corrupted_memory_unsafe_accepted_persistence_zero",
    "durable_attempt_receipts",
    "exact_bootstrap_clones",
    "exact_paired_population",
    "identical_budgets",
    "isolated_tenants",
    "no_frozen_or_observation_mutation",
    "no_hidden_model_access",
    "semantic_outcome_calibration",
    "zero_false_truth_from_noise",
)
P7_P9_GATES = (*P7_ORACLE_GATES, "codex_cli_transport")
P7_METRIC_SPECS: dict[str, tuple[str, float]] = {
    "adaptive_direct_thesis_accuracy": (">=", 0.90),
    "adaptive_atomic_claim_f1": (">=", 0.875),
    "adaptive_boundary_entity_safety": (">=", 0.90),
    "adaptive_relation_joint_precision": (">=", 0.95),
    "adaptive_external_outcome_calibration_ece": ("<=", 0.15),
    "adaptive_false_truth_from_noise_rate": ("=", 0.0),
}
_ENDPOINT_FIELDS = {
    "adaptive_direct_thesis_accuracy": "direct_thesis_accuracy",
    "adaptive_atomic_claim_f1": "atomic_claim_f1",
    "adaptive_boundary_entity_safety": "boundary_entity_safety",
    "adaptive_relation_joint_precision": "relation_joint_precision",
    "adaptive_external_outcome_calibration_ece": "external_outcome_calibration_ece",
}


def p7_metric_contract_digest() -> str:
    return canonical_sha256({
        "phase": "p7", "gate_ids": P7_P9_GATES,
        "metric_specs": P7_METRIC_SPECS,
        "source_schema": "epistemic-repair-p7-postfreeze-oracle-v1",
    })


def _validate_source(score: Mapping[str, Any]) -> None:
    body = dict(score)
    content_digest = body.pop("content_digest", None)
    if (
        score.get("schema_version") != "epistemic-repair-p7-postfreeze-oracle-v1"
        or content_digest != canonical_sha256(body)
    ):
        raise ValueError("P7 oracle schema or content digest is invalid")
    provenance = score.get("run_provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError("P7 oracle lacks run provenance")
    if not _HEX40.fullmatch(str(provenance.get("git_commit") or "")):
        raise ValueError("P7 run provenance requires a full commit SHA")
    if provenance.get("worktree_clean") is not True:
        raise ValueError("P7 P9 sidecar requires a clean execution worktree")
    if provenance.get("codex_transport") != "cli":
        raise ValueError("P7 P9 sidecar requires CODEX_TRANSPORT=cli")


def build_p7_p9_sidecar(score: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize only audited member evidence; never trust scalar summaries."""

    _validate_source(score)
    gate_members = score.get("hard_gate_members")
    if not isinstance(gate_members, Mapping) or set(gate_members) != set(P7_ORACLE_GATES):
        raise ValueError("P7 oracle gate member set is incomplete")
    normalized_gate_members: dict[str, list[dict[str, Any]]] = {}
    for name in P7_ORACLE_GATES:
        members = gate_members[name]
        if not isinstance(members, list) or not members:
            raise ValueError(f"P7 gate denominator is empty: {name}")
        ids = []
        normalized_gate_members[name] = []
        for member in members:
            if not isinstance(member, Mapping) or not isinstance(member.get("conforms"), bool):
                raise ValueError(f"P7 gate member is malformed: {name}")
            if not member.get("member_id") or not _HEX64.fullmatch(
                str(member.get("raw_source_digest") or "")
            ):
                raise ValueError(f"P7 gate member lacks source identity: {name}")
            ids.append(str(member["member_id"]))
            normalized_gate_members[name].append(dict(member))
        if len(ids) != len(set(ids)):
            raise ValueError(f"P7 gate member IDs are duplicated: {name}")
    derived_oracle_gates = {
        name: all(member["conforms"] for member in normalized_gate_members[name])
        for name in P7_ORACLE_GATES
    }
    if derived_oracle_gates != score.get("hard_gates"):
        raise ValueError("P7 hard-gate summary contradicts raw members")
    provenance = dict(score["run_provenance"])
    transport_raw = {
        "codex_transport": provenance["codex_transport"],
        "git_commit": provenance["git_commit"],
    }
    normalized_gate_members["codex_cli_transport"] = [{
        "member_id": "execution-transport",
        "conforms": provenance["codex_transport"] == "cli",
        "raw_source_digest": canonical_sha256(transport_raw),
    }]

    endpoints = [
        dict(row) for row in score.get("endpoints") or ()
        if row.get("arm_id") == "adaptive" and row.get("stage_batch") == 12
    ]
    endpoint_keys = {
        (str(row.get("world_id")), str(row.get("storyline_id"))) for row in endpoints
    }
    expected_endpoint_count = int(score.get("world_count") or 0) * 4
    if (
        not endpoints or len(endpoints) != expected_endpoint_count
        or len(endpoint_keys) != len(endpoints)
    ):
        raise ValueError("P7 adaptive mature endpoint denominator is not exact")
    metric_members: dict[str, list[dict[str, Any]]] = {}
    for name, field in _ENDPOINT_FIELDS.items():
        members = []
        for row in endpoints:
            measurement = row.get(field)
            value = measurement.get("value") if isinstance(measurement, Mapping) else None
            if not isinstance(value, (int, float)):
                raise ValueError(f"P7 endpoint metric is unmeasured: {name}")
            member_id = f"{row['world_id']}:{row['storyline_id']}:{name}"
            members.append({
                "member_id": member_id, "numerator": float(value),
                "denominator": 1, "raw_source_digest": canonical_sha256(row),
            })
        metric_members[name] = members
    metric_members["adaptive_false_truth_from_noise_rate"] = [{
        "member_id": f"{row['world_id']}:{row['storyline_id']}:noise",
        "numerator": int(row.get("false_truth_from_noise") or 0),
        "denominator": 1, "raw_source_digest": canonical_sha256(row),
    } for row in endpoints]

    source_digest = str(score["content_digest"])
    metrics = []
    for name, (operator, threshold) in P7_METRIC_SPECS.items():
        members = metric_members[name]
        numerator = sum(float(member["numerator"]) for member in members)
        denominator = sum(float(member["denominator"]) for member in members)
        value = numerator / denominator
        passed = (
            value >= threshold if operator == ">="
            else value <= threshold if operator == "<="
            else value == threshold
        )
        metrics.append({
            "name": name, "numerator": numerator, "denominator": denominator,
            "value": value, "coverage": 1.0, "uncertainty": "not_applicable",
            "status": "pass" if passed else "fail", "operator": operator,
            "threshold": threshold, "source_artifact_digest": source_digest,
            "worst_cases": sorted(
                (deepcopy(member) for member in members),
                key=lambda member: float(member["numerator"]) / float(member["denominator"]),
                reverse=operator == "<=",
            )[:10],
        })
    normalized_gates = {
        name: all(member["conforms"] for member in normalized_gate_members[name])
        for name in P7_P9_GATES
    }
    strategic_decision = str(score.get("strategic_verdict") or "")
    if strategic_decision not in {
        "primary_memory_earned", "limited_compression_value",
        "not_earned", "insufficient_evidence",
    }:
        raise ValueError("P7 strategic decision is absent or invalid")
    contributions = {
        "schema_version": "epistemic-repair-p7-p9-member-contributions-v1",
        "preregistered_contract_digest": p7_metric_contract_digest(),
        "gate_members": normalized_gate_members,
        "metric_members": metric_members,
    }
    payload = {
        "schema_version": "epistemic-repair-p7-p9-normalized-v1",
        "commit": provenance["git_commit"], "run_provenance": provenance,
        "source_oracle_digest": source_digest,
        "source_execution_artifact_digest": score.get("execution_artifact_digest"),
        "preregistered_metric_contract_digest": p7_metric_contract_digest(),
        "hard_gates": normalized_gates,
        "p9_continuous_metrics": metrics,
        "p9_member_contributions": contributions,
        "strategic_decision": strategic_decision,
        "strategic_decision_evidence": deepcopy(score.get("memory_earns_decision")),
        "phase_exit_ready": bool(
            score.get("phase_exit_ready")
            and strategic_decision != "insufficient_evidence"
            and all(normalized_gates.values())
            and all(metric["status"] == "pass" for metric in metrics)
        ),
    }
    return {**payload, "content_digest": canonical_sha256(payload)}


__all__ = [
    "P7_METRIC_SPECS", "P7_ORACLE_GATES", "P7_P9_GATES",
    "build_p7_p9_sidecar", "p7_metric_contract_digest",
]
