"""Strict P3 P9 sidecar from validated member receipts and raw DB probe evidence."""

from __future__ import annotations

import re
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p3_runner import P3Artifact

_HEX40 = re.compile(r"[0-9a-f]{40}")


def build_p3_p9_sidecar(*, report: dict[str, Any], postgres_proof: dict[str, Any] | None,
                        commit: str, worktree_clean: bool) -> dict[str, Any]:
    artifact = P3Artifact.model_validate(report)
    if not _HEX40.fullmatch(commit) or not worktree_clean:
        raise ValueError("P3 P9 evidence requires a clean full release commit")
    ids = [row.signal_id for row in artifact.member_receipts]
    if len(ids) != 120 or len(set(ids)) != 120:
        raise ValueError("P3 member denominator is not exact")
    if postgres_proof is None:
        raise ValueError("P3 raw PostgreSQL probe evidence is required")
    source_digest = canonical_sha256({
        "members": [row.model_dump(mode="json") for row in artifact.member_receipts],
        "corrections": list(artifact.correction_receipts), "postgres_proof": postgres_proof,
    })
    gates = {key: {"status": value.status, "eligible_count": value.eligible_count,
                   "observed_count": value.observed_count, "incident_ids": list(value.incident_ids)}
             for key, value in artifact.hard_gates.items()}
    metrics = [{"name": key, "numerator": value.numerator, "denominator": value.denominator,
                "value": value.value, "coverage": value.denominator / 120,
                "uncertainty": ({"confidence_interval": list(value.confidence_interval)}
                                if value.confidence_interval is not None else "not_applicable"),
                "status": "pass" if value.threshold_met is True else "fail",
                "operator": value.threshold_operator, "threshold": value.threshold,
                "source_artifact_digest": source_digest,
                "worst_cases": [{"source_id": item} for item in value.worst_example_ids]}
               for key, value in sorted(artifact.continuous_metrics.items())]
    contributions = {
        "preregistered_contract_digest": artifact.sealed_manifest.evaluation_policy_sha256,
        "gate_members": {key: {"source_member_ids": ids, "source_member_digest": source_digest} for key in gates},
        "metric_members": {row["name"]: {"source_member_ids": ids, "source_member_digest": source_digest} for row in metrics},
    }
    body = {"schema_version": "epistemic-repair-p3-p9-normalized-v1", "commit": commit,
            "phase_exit_ready": all(x["status"] == "pass" for x in gates.values()) and all(x["status"] == "pass" for x in metrics),
            "hard_gates": gates, "p9_continuous_metrics": metrics,
            "p9_member_contributions": contributions,
            "run_provenance": {"git_commit": commit, "worktree_clean": True},
            "source_phase_artifact_digest": artifact.artifact_content_digest}
    return {**body, "content_digest": canonical_sha256(body)}


__all__ = ["build_p3_p9_sidecar"]
