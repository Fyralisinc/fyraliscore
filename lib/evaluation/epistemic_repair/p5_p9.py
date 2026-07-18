"""Strict P5-to-P9 sidecar built only from a validated in-memory P5 artifact."""

from __future__ import annotations

from typing import Any
import re

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p5_oracles import P5Artifact


_HEX40 = re.compile(r"[0-9a-f]{40}")


def build_p5_p9_sidecar(*, artifact: P5Artifact, commit: str, worktree_clean: bool) -> dict[str, Any]:
    # Revalidation verifies member cardinality, internal consistency, and the
    # embedded digest before any P9 projection is constructed.
    artifact = P5Artifact.model_validate(artifact.model_dump(mode="json"))
    if not _HEX40.fullmatch(commit) or not worktree_clean:
        raise ValueError("P5 P9 evidence requires a clean full release commit")
    member_ids = [row.signal_id for row in artifact.signal_receipts]
    if len(member_ids) != 75 or len(set(member_ids)) != 75:
        raise ValueError("P5 member denominator is not exact")
    source_digest = canonical_sha256({
        "signals": [row.model_dump(mode="json") for row in artifact.signal_receipts],
        "barriers": [row.model_dump(mode="json") for row in artifact.barrier_receipts],
        "vertical": artifact.vertical_receipt.model_dump(mode="json"),
        "database": artifact.database_evidence,
    })
    hard_gates = {
        key: {"status": value.status, "eligible_count": value.eligible_count,
              "conforming_count": value.conforming_count, "incident_ids": list(value.incident_ids)}
        for key, value in artifact.hard_gates.items()
    }
    metrics = [{
        "name": key, "numerator": value.numerator, "denominator": value.denominator,
        "value": value.value, "coverage": 1.0, "uncertainty": "not_applicable",
        "status": "pass" if value.threshold_met else "fail",
        "operator": value.threshold_operator, "threshold": value.threshold,
        "source_artifact_digest": source_digest, "worst_cases": [],
    } for key, value in sorted(artifact.continuous_metrics.items())]
    contributions = {
        "schema_version": "epistemic-repair-p9-member-contributions-v1",
        "preregistered_contract_digest": canonical_sha256({
            "gates": sorted(hard_gates), "metrics": [row["name"] for row in metrics],
            "population": artifact.population_digest,
        }),
        "gate_members": {
            key: [{"member_id": f"{key}:population", "source_member_ids": member_ids,
                   "raw_source_digest": source_digest, "conforms": hard_gates[key]["status"] == "pass"}]
            for key in hard_gates
        },
        "metric_members": {
            row["name"]: [{"member_id": f"{row['name']}:population", "source_member_ids": member_ids,
                           "raw_source_digest": source_digest, "numerator": row["numerator"],
                           "denominator": row["denominator"]}]
            for row in metrics
        },
        "member_source_digests": [source_digest],
    }
    body = {
        "schema_version": "epistemic-repair-p5-p9-normalized-v1", "commit": commit,
        "phase_exit_ready": (
            all(row["status"] == "pass" for row in hard_gates.values())
            and all(row["status"] == "pass" for row in metrics)
        ),
        "hard_gates": hard_gates, "p9_continuous_metrics": metrics,
        "p9_member_contributions": contributions,
        "run_provenance": {"git_commit": commit, "worktree_clean": True},
        "source_phase_artifact_digest": artifact.content_digest,
    }
    return {**body, "content_digest": canonical_sha256(body)}


__all__ = ["build_p5_p9_sidecar"]
