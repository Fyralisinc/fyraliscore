"""Strict P0-P2 member-evidence envelopes for P9 normalization."""

from __future__ import annotations

from copy import deepcopy
from pathlib import Path
import re
import subprocess
from typing import Any, Mapping

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p9_phase_regeneration import PHASE_CONTRACTS


_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")

P9_METRIC_SPECS: dict[str, dict[str, tuple[str, float]]] = {
    "p0": {},
    "p1": {
        "attempt_receipt_coverage": ("=", 1.0),
        "count_reconciliation": ("=", 1.0),
        "cost_coverage": ("=", 1.0),
        "timing_reconciliation": (">=", 0.99),
    },
    "p2": {
        "active_unexplained_perfect_confidence_relation_rate": ("=", 0.0),
        "active_wrapper_contamination": ("=", 0.0),
        "background_repair_latency_ms": ("<=", 5_000.0),
        "evidence_lineage_coverage": ("=", 1.0),
        "lifecycle_transition_latency_ms": ("<=", 5_000.0),
        "relation_joint_accuracy": ("=", 1.0),
        "scope_precision": ("=", 1.0),
        "semantic_duplicate_absorption": (">=", 0.90),
    },
}


def git_run_provenance(repository_root: Path) -> dict[str, Any]:
    root = repository_root.resolve()
    commit = subprocess.check_output(
        ["git", "rev-parse", "HEAD"], cwd=root, text=True,
    ).strip()
    dirty = subprocess.check_output(
        ["git", "status", "--porcelain"], cwd=root, text=True,
    ).strip()
    if not _HEX40.fullmatch(commit):
        raise ValueError("git provenance did not produce a full commit SHA")
    return {
        "git_commit": commit,
        "worktree_clean": not bool(dirty),
        "repository_root": str(root),
    }


def metric_contract_digest(phase: str) -> str:
    contract = PHASE_CONTRACTS[phase]
    return canonical_sha256({
        "phase": phase,
        "gate_ids": contract["gates"],
        "metric_specs": P9_METRIC_SPECS[phase],
        "required_raw_members": contract["members"],
    })


def _validate_member(member: Mapping[str, Any], *, metric: bool) -> None:
    if not member.get("member_id") or not _HEX64.fullmatch(
        str(member.get("raw_source_digest") or "")
    ):
        raise ValueError("every member needs an ID and raw source digest")
    if metric:
        if not isinstance(member.get("numerator"), (int, float)):
            raise ValueError("metric member numerator must be numeric")
        if not isinstance(member.get("denominator"), (int, float)) or member["denominator"] <= 0:
            raise ValueError("metric member denominator must be positive")
    elif not isinstance(member.get("conforms"), bool):
        raise ValueError("gate member conforms must be raw boolean evidence")


def attach_p9_member_evidence(
    artifact: Mapping[str, Any], *, phase: str,
    gate_members: Mapping[str, list[dict[str, Any]]],
    metric_members: Mapping[str, list[dict[str, Any]]],
    run_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    """Attach normalized metrics derived only from raw member contributions."""

    contract = PHASE_CONTRACTS[phase]
    if set(gate_members) != set(contract["gates"]):
        raise ValueError("gate member IDs must exactly match the preregistered contract")
    if set(metric_members) != set(contract["metrics"]):
        raise ValueError("metric member IDs must exactly match the preregistered contract")
    worktree_clean = run_provenance.get("worktree_clean") is True
    if not _HEX40.fullmatch(str(run_provenance.get("git_commit") or "")):
        raise ValueError("P9 regeneration artifacts require a full commit SHA")
    for name in contract["members"]:
        value = artifact.get(name)
        if value is None or value == () or value == "" or value == [] or value == {}:
            raise ValueError(f"required raw member evidence is missing: {name}")
    for members in gate_members.values():
        if not members:
            raise ValueError("gate member denominator cannot be empty")
        for member in members:
            _validate_member(member, metric=False)
    for members in metric_members.values():
        if not members:
            raise ValueError("metric member denominator cannot be empty")
        for member in members:
            _validate_member(member, metric=True)
    for group in (*gate_members.values(), *metric_members.values()):
        ids = [str(member["member_id"]) for member in group]
        if len(ids) != len(set(ids)):
            raise ValueError("member IDs must be unique within each denominator")

    contract_digest = metric_contract_digest(phase)
    contributions = {
        "schema_version": "epistemic-repair-p9-member-contributions-v1",
        "preregistered_contract_digest": contract_digest,
        "gate_members": deepcopy(dict(gate_members)),
        "metric_members": deepcopy(dict(metric_members)),
        "member_source_digests": sorted({
            str(item["raw_source_digest"])
            for group in (*gate_members.values(), *metric_members.values())
            for item in group
        }),
    }
    contribution_digest = canonical_sha256(contributions)
    metrics = []
    for name in contract["metrics"]:
        members = metric_members[name]
        numerator = sum(float(item["numerator"]) for item in members)
        denominator = sum(float(item["denominator"]) for item in members)
        value = numerator / denominator
        operator, threshold = P9_METRIC_SPECS[phase][name]
        passed = (
            value >= threshold if operator == ">="
            else value <= threshold if operator == "<="
            else value == threshold
        )
        metrics.append({
            "name": name, "value": value, "numerator": numerator,
            "denominator": denominator, "coverage": 1.0,
            "uncertainty": "not_applicable", "status": "pass" if passed else "fail",
            "operator": operator, "threshold": threshold,
            "source_artifact_digest": contribution_digest,
            "worst_cases": [
                dict(item) for item in members
                if float(item["numerator"]) / float(item["denominator"])
                != (1.0 if operator in {">=", "="} else 0.0)
            ][:10],
        })
    result = {
        **deepcopy(dict(artifact)),
        "commit": run_provenance["git_commit"],
        "run_provenance": deepcopy(dict(run_provenance)),
        "hard_gates": {
            name: all(bool(item["conforms"]) for item in gate_members[name])
            for name in contract["gates"]
        },
        "preregistered_metric_contract_digest": contract_digest,
        "p9_member_contributions": contributions,
        "p9_continuous_metrics": metrics,
    }
    result["phase_exit_ready"] = bool(
        result.get("phase_exit_ready") and worktree_clean
        and all(result["hard_gates"].values())
        and all(item["status"] == "pass" for item in metrics)
    )
    result["content_digest"] = canonical_sha256(result)
    return result


__all__ = [
    "P9_METRIC_SPECS", "attach_p9_member_evidence", "git_run_provenance",
    "metric_contract_digest",
]
