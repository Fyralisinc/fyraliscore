"""Strict P3 P9 sidecar from validated member receipts and raw DB probe evidence."""

from __future__ import annotations

import re
import json
from pathlib import Path
from typing import Any

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p3_runner import P3Artifact

_HEX40 = re.compile(r"[0-9a-f]{40}")


def build_p3_p9_sidecar(
    *, report_path: Path, postgres_proof_path: Path, commit: str, worktree_clean: bool,
) -> dict[str, Any]:
    report = json.loads(report_path.read_text())
    artifact = P3Artifact.model_validate(report)
    if not _HEX40.fullmatch(commit) or not worktree_clean:
        raise ValueError("P3 P9 evidence requires a clean full release commit")
    ids = [row.signal_id for row in artifact.member_receipts]
    if len(ids) != 120 or len(set(ids)) != 120:
        raise ValueError("P3 member denominator is not exact")
    proof_artifact = json.loads(postgres_proof_path.read_text())
    proof_body = dict(proof_artifact)
    proof_digest = proof_body.pop("content_digest", None)
    if (
        proof_artifact.get("schema_version") != "epistemic-repair-p3-postgres-proof-v1"
        or proof_artifact.get("commit") != commit
        or proof_digest != canonical_sha256(proof_body)
        or not isinstance(proof_artifact.get("proof"), dict)
    ):
        raise ValueError("P3 reopened PostgreSQL proof digest is invalid")
    postgres_proof = proof_artifact["proof"]
    source_digest = canonical_sha256({
        "members": [row.model_dump(mode="json") for row in artifact.member_receipts],
        "corrections": list(artifact.correction_receipts), "postgres_proof": postgres_proof,
    })
    gates = {key: {"status": value.status, "eligible_count": value.eligible_count,
                   "observed_count": value.observed_count, "incident_ids": list(value.incident_ids)}
             for key, value in artifact.hard_gates.items()}
    metrics = [{"name": key, "numerator": value.numerator, "denominator": value.denominator,
                "value": value.value, "coverage": 1.0,
                "uncertainty": ({"confidence_interval": list(value.confidence_interval)}
                                if value.confidence_interval is not None else "not_applicable"),
                "status": "pass" if value.threshold_met is True else "fail",
                "operator": value.threshold_operator, "threshold": value.threshold,
                "source_artifact_digest": source_digest,
                "worst_cases": [{"source_id": item} for item in value.worst_example_ids]}
               for key, value in sorted(artifact.continuous_metrics.items())]
    contributions = {
        "schema_version": "epistemic-repair-p9-member-contributions-v1",
        "preregistered_contract_digest": artifact.sealed_manifest.evaluation_policy_sha256,
        "gate_members": {key: [{
            "member_id": f"{key}:population", "raw_source_digest": source_digest,
            "conforms": gates[key]["status"] == "pass", "source_member_ids": ids,
        }] for key in gates},
        "metric_members": {row["name"]: [{
            "member_id": f"{row['name']}:population", "raw_source_digest": source_digest,
            "numerator": row["numerator"], "denominator": row["denominator"],
            "source_member_ids": ids,
        }] for row in metrics},
        "member_source_digests": [source_digest],
    }
    body = {"schema_version": "epistemic-repair-p3-p9-normalized-v1", "commit": commit,
            "phase_exit_ready": all(x["status"] == "pass" for x in gates.values()) and all(x["status"] == "pass" for x in metrics),
            "hard_gates": gates, "p9_continuous_metrics": metrics,
            "p9_member_contributions": contributions,
            "run_provenance": {"git_commit": commit, "worktree_clean": True},
            "source_phase_artifact_digest": artifact.artifact_content_digest,
            "source_postgres_proof_digest": proof_digest}
    return {**body, "content_digest": canonical_sha256(body)}


__all__ = ["build_p3_p9_sidecar"]
