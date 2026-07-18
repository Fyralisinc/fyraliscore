"""Strict, fail-closed P9 release manifest and report composition."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from pathlib import Path
import re
from typing import Any, Mapping


SCHEMA_VERSION = "epistemic-repair-p9-release-candidate-v2"
MANIFEST_SCHEMA_VERSION = "epistemic-repair-p9-release-manifest-v1"
REVIEW_SCHEMA_VERSION = "epistemic-repair-p9-reviewer-reproduction-v1"
REQUIRED_PHASES = tuple(f"p{index}" for index in range(9))
READY_VERDICT = "ready_for_bounded_internal_company_learning"
ALLOWED_VERDICTS = {
    READY_VERDICT, "mechanically_ready_semantically_insufficient",
    "memory_not_earned_simplification_required", "operationally_insufficient",
    "safety_or_truth_blocked", "insufficient_evidence",
}
CURRENT_EVIDENCE_CLASSES = {"integrated_current", "bounded_current"}
DIAGNOSTIC_EVIDENCE_CLASSES = {"historical_falsifying", "unmeasured"}
CONTENT_DIGEST_ALGORITHM = "canonical-json-sha256-excluding-declared-field"
_HEX40 = re.compile(r"[0-9a-f]{40}")
_HEX64 = re.compile(r"[0-9a-f]{64}")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def _digest(value: Any) -> str:
    return sha256(_canonical_bytes(value)).hexdigest()


def _file_sha(path: Path) -> str:
    return sha256(path.read_bytes()).hexdigest()


def _embedded_digest(artifact: Mapping[str, Any], field: str, algorithm: str) -> str:
    if algorithm != CONTENT_DIGEST_ALGORITHM:
        raise ValueError(f"unsupported content digest algorithm: {algorithm}")
    body = dict(artifact)
    body.pop(field, None)
    return _digest(body)


@dataclass(frozen=True)
class ManifestEvidence:
    path: str
    schema_version: str
    commit: str
    sha256: str
    content_digest: str
    content_digest_field: str
    content_digest_algorithm: str
    evidence_class: str
    required_gate_ids: tuple[str, ...]
    required_metric_ids: tuple[str, ...]


def seal_manifest(
    *, coordinator_id: str, release_commit: str,
    required_current: Mapping[str, ManifestEvidence],
    diagnostics: tuple[ManifestEvidence, ...] = (),
) -> dict[str, Any]:
    if not coordinator_id:
        raise ValueError("coordinator_id is required")
    if not _HEX40.fullmatch(release_commit):
        raise ValueError("release_commit must be a full lowercase 40-character SHA")
    if set(required_current) != set(REQUIRED_PHASES):
        raise ValueError("required_current must contain exactly p0 through p8")
    if any(item.evidence_class not in CURRENT_EVIDENCE_CLASSES for item in required_current.values()):
        raise ValueError("required phase has non-current evidence class")
    if any(item.commit != release_commit for item in required_current.values()):
        raise ValueError("every required phase must bind the release commit")
    if any(
        len(set(item.required_gate_ids)) != len(item.required_gate_ids)
        or len(set(item.required_metric_ids)) != len(item.required_metric_ids)
        or item.content_digest_algorithm != CONTENT_DIGEST_ALGORITHM
        for item in (*required_current.values(), *diagnostics)
    ):
        raise ValueError("manifest contains duplicate IDs or unsupported digest algorithm")
    if any(item.evidence_class not in DIAGNOSTIC_EVIDENCE_CLASSES for item in diagnostics):
        raise ValueError("diagnostic has non-diagnostic evidence class")
    body = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "coordinator_id": coordinator_id,
        "release_commit": release_commit,
        "required_current": {key: asdict(required_current[key]) for key in REQUIRED_PHASES},
        "diagnostics": [asdict(item) for item in diagnostics],
    }
    return {**body, "manifest_digest": _digest(body)}


def _gate_value(value: Any) -> bool | None:
    if isinstance(value, bool):
        return value
    if isinstance(value, Mapping) and value.get("status") in {"pass", "fail"}:
        return value["status"] == "pass"
    return None


def _metric_valid(metric: Any) -> tuple[bool, bool]:
    required = {
        "name", "value", "numerator", "denominator", "coverage", "uncertainty",
        "status", "operator", "threshold", "source_artifact_digest", "worst_cases",
    }
    if not isinstance(metric, Mapping) or not required <= set(metric):
        return False, False
    denominator, numerator = metric["denominator"], metric["numerator"]
    if not isinstance(denominator, (int, float)) or denominator <= 0:
        return False, False
    if not isinstance(numerator, (int, float)) or not isinstance(metric["value"], (int, float)):
        return False, False
    coverage = metric["coverage"]
    if not isinstance(coverage, (int, float)) or not 0 <= coverage <= 1:
        return False, False
    uncertainty = metric["uncertainty"]
    if uncertainty != "not_applicable" and not isinstance(uncertainty, Mapping):
        return False, False
    if not _HEX64.fullmatch(str(metric["source_artifact_digest"])):
        return False, False
    if not isinstance(metric["worst_cases"], list):
        return False, False
    computed = numerator / denominator
    if abs(float(metric["value"]) - computed) > 1e-12:
        return False, False
    operator, threshold = metric["operator"], metric["threshold"]
    if operator not in {">=", "<=", "="} or not isinstance(threshold, (int, float)):
        return False, False
    passed = computed >= threshold if operator == ">=" else computed <= threshold if operator == "<=" else computed == threshold
    if metric["status"] not in {"pass", "fail"} or (metric["status"] == "pass") != passed:
        return False, False
    return True, passed


def _read_entry(entry: Mapping[str, Any], *, phase: str | None) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    record = {"phase": phase, "path": entry.get("path"), "evidence_class": entry.get("evidence_class")}
    try:
        path = Path(str(entry["path"]))
        raw = path.read_bytes()
        artifact = json.loads(raw)
        if not isinstance(artifact, dict):
            raise ValueError("artifact root must be an object")
        gates = artifact.get("hard_gates")
        metrics = artifact.get("p9_continuous_metrics")
        required_gates = set(entry["required_gate_ids"])
        required_metrics = set(entry["required_metric_ids"])
        gate_ids = set(gates) if isinstance(gates, Mapping) else set()
        metric_ids = {
            str(item.get("name")) for item in metrics if isinstance(item, Mapping)
        } if isinstance(metrics, list) else set()
        gate_values = [_gate_value(gates[key]) for key in sorted(gate_ids)] if isinstance(gates, Mapping) else []
        metric_results = [_metric_valid(item) for item in metrics] if isinstance(metrics, list) else []
        embedded = _embedded_digest(
            artifact, str(entry["content_digest_field"]), str(entry["content_digest_algorithm"]),
        )
        checks = {
            "sha256_matches": sha256(raw).hexdigest() == entry["sha256"],
            "schema_matches": artifact.get("schema_version") == entry["schema_version"],
            "commit_matches": artifact.get("commit") == entry["commit"],
            "embedded_digest_matches": artifact.get(entry["content_digest_field"]) == entry["content_digest"] == embedded,
            "exact_gate_set": gate_ids == required_gates,
            "gates_decodable": bool(gate_values) and all(value is not None for value in gate_values),
            "all_gates_green": bool(gate_values) and all(gate_values),
            "metrics_present": isinstance(metrics, list),
            "exact_metric_set": metric_ids == required_metrics and len(metric_ids) == len(metrics or ()),
            "metrics_valid": len(metric_results) == len(required_metrics) and all(valid for valid, _ in metric_results),
            "all_metrics_green": all(passed for _, passed in metric_results),
            "phase_ready": artifact.get("phase_exit_ready") is True,
        }
        record.update(checks, reopened=True)
        return artifact, record
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        record.update(reopened=False, error=f"{type(exc).__name__}: {exc}")
        return None, record


def _manifest_valid(manifest: Mapping[str, Any]) -> bool:
    body = dict(manifest)
    digest = body.pop("manifest_digest", None)
    return bool(
        manifest.get("schema_version") == MANIFEST_SCHEMA_VERSION
        and _HEX40.fullmatch(str(manifest.get("release_commit", "")))
        and set(manifest.get("required_current", {})) == set(REQUIRED_PHASES)
        and all(
            entry.get("commit") == manifest.get("release_commit")
            and entry.get("evidence_class") in CURRENT_EVIDENCE_CLASSES
            and entry.get("content_digest_algorithm") == CONTENT_DIGEST_ALGORITHM
            for entry in manifest.get("required_current", {}).values()
        )
        and digest == _digest(body)
    )


def reproduce(manifest: Mapping[str, Any]) -> dict[str, Any]:
    records, artifacts = [], {}
    for phase in REQUIRED_PHASES:
        artifact, record = _read_entry(manifest["required_current"][phase], phase=phase)
        records.append(record)
        if artifact is not None:
            artifacts[phase] = artifact
    diagnostic_records = [
        _read_entry(entry, phase=None)[1] for entry in manifest.get("diagnostics", ())
    ]
    contract_complete = _manifest_valid(manifest) and all(
        row.get("reopened") and row.get("sha256_matches") and row.get("schema_matches")
        and row.get("commit_matches") and row.get("embedded_digest_matches")
        and row.get("exact_gate_set") and row.get("gates_decodable") and row.get("metrics_present")
        and row.get("exact_metric_set") and row.get("metrics_valid")
        for row in records
    )
    phase_green = {
        row["phase"]: bool(
            row.get("all_gates_green") and row.get("all_metrics_green") and row.get("phase_ready")
        ) for row in records
    }
    required_green = contract_complete and all(phase_green.values())
    p7_decision = artifacts.get("p7", {}).get("strategic_decision")
    core = {
        "manifest_digest": manifest.get("manifest_digest"),
        "phase_evidence": records, "diagnostic_evidence": diagnostic_records,
        "evidence_contract_complete": contract_complete,
        "phase_green": phase_green, "required_evidence_green": required_green,
        "memory_decision": p7_decision,
    }
    return {**core, "reproduced_report_digest": _digest(core)}


def build_release_report(
    *, manifest: Mapping[str, Any], verified_release_commit: str,
    verified_worktree_clean: bool, reviewer_receipt: Mapping[str, Any] | None,
) -> dict[str, Any]:
    reproduction = reproduce(manifest)
    receipt = dict(reviewer_receipt or {})
    receipt_body = dict(receipt)
    receipt_digest = receipt_body.pop("receipt_digest", None)
    reviewer_valid = bool(
        receipt.get("schema_version") == REVIEW_SCHEMA_VERSION
        and receipt.get("status") == "reproduced"
        and receipt.get("reviewer_id")
        and receipt.get("reviewer_id") != manifest.get("coordinator_id")
        and receipt.get("reviewed_manifest_digest") == manifest.get("manifest_digest")
        and receipt.get("reproduced_report_digest") == reproduction["reproduced_report_digest"]
        and receipt_digest == _digest(receipt_body)
    )
    evidence_complete = bool(
        verified_worktree_clean
        and verified_release_commit == manifest.get("release_commit")
        and _HEX40.fullmatch(verified_release_commit)
        and reproduction["evidence_contract_complete"]
        and reviewer_valid
    )
    memory = reproduction.get("memory_decision")
    green = reproduction["phase_green"]
    constitutional_green = all(green.get(f"p{i}", False) for i in range(5))
    semantic_green = all(green.get(phase, False) for phase in ("p5", "p6"))
    operational_green = all(green.get(phase, False) for phase in ("p7", "p8"))
    if not evidence_complete:
        verdict = "insufficient_evidence"
    elif not constitutional_green:
        verdict = "safety_or_truth_blocked"
    elif memory in {"not_earned", "limited_compression_value"}:
        verdict = "memory_not_earned_simplification_required"
    elif not operational_green:
        verdict = "operationally_insufficient"
    elif not semantic_green or memory != "primary_memory_earned":
        verdict = "mechanically_ready_semantically_insufficient"
    else:
        verdict = READY_VERDICT
    report = {
        "schema_version": SCHEMA_VERSION, "commit": verified_release_commit,
        "worktree_clean": verified_worktree_clean, **reproduction,
        "reviewer_receipt_valid": reviewer_valid, "evidence_complete": evidence_complete,
        "constitutional_green": constitutional_green, "semantic_green": semantic_green,
        "operational_green": operational_green,
        "verdict": verdict, "completion_authorized": verdict == READY_VERDICT,
        "scope_boundaries": {"task_autonomy": "excluded", "connector_transport": "excluded",
                             "customer_value": "not_claimed", "future_large_real_provider_run": "separately_authorized"},
    }
    return {**report, "report_digest": _digest(report)}


__all__ = ["ALLOWED_VERDICTS", "CONTENT_DIGEST_ALGORITHM", "MANIFEST_SCHEMA_VERSION",
           "ManifestEvidence", "READY_VERDICT", "REQUIRED_PHASES", "REVIEW_SCHEMA_VERSION",
           "SCHEMA_VERSION", "build_release_report", "reproduce", "seal_manifest"]
