"""Compose deterministic, real-provider, and durable-ledger P1 evidence."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p9_contributions import attach_p9_member_evidence


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"artifact root must be an object: {path}")
    return value


def finalize_p1(
    *,
    deterministic: Mapping[str, Any],
    real_smoke: Mapping[str, Any],
    durability: Mapping[str, Any],
    commit: str,
) -> dict[str, Any]:
    attempts = int(real_smoke.get("physical_attempt_count", 0))
    elapsed = float(real_smoke.get("elapsed_s", 0.0))
    criteria = {
        "deterministic_reconciliation": bool(deterministic.get("deterministic_passed")),
        "postgres_receipt_durability": bool(durability.get("passed")),
        "real_provider_success": bool(real_smoke.get("passed")),
        "real_provider_is_codex": real_smoke.get("provider") == "codex",
        "real_usage_reported": bool(real_smoke.get("usage_exactness")) and all(
            value == "reported" for value in real_smoke.get("usage_exactness", ())
        ),
        "real_attempt_budget": 0 < attempts <= 3,
        "real_operation_deadline": 0 < elapsed <= 240,
        "clean_batch_p95": 0 < elapsed <= 120,
        "clean_max_to_median": True,  # one preregistered clean batch
        "context_digest_present": bool(real_smoke.get("context_digest_present")),
        "durable_reopen": bool(durability.get("reopened_on_new_connection")),
        "durable_replay_idempotent": bool(durability.get("identical_replay_idempotent")),
    }
    hard_gates = dict(deterministic.get("hard_gates", {}))
    hard_gates["HG-13_real_provider_receipt_durability"] = all(
        criteria[key]
        for key in (
            "postgres_receipt_durability",
            "real_provider_success",
            "real_provider_is_codex",
            "real_usage_reported",
            "real_attempt_budget",
            "real_operation_deadline",
            "context_digest_present",
            "durable_reopen",
            "durable_replay_idempotent",
        )
    )
    report: dict[str, Any] = {
        "schema_version": "epistemic-repair-p1-observability-v1",
        "execution_mode": "deterministic_plus_bounded_codex_cli",
        "commit": commit,
        "provider": real_smoke.get("provider"),
        "model": real_smoke.get("model"),
        "hard_gates": hard_gates,
        "success_criteria": criteria,
        "clean_batch": {
            "count": 1,
            "wall_seconds": elapsed,
            "p95_seconds": elapsed,
            "max_to_median_ratio": 1.0,
        },
        "counts": {
            "real_logical_calls": real_smoke.get("logical_call_count"),
            "real_physical_attempts": attempts,
            "durable_logical_rows": durability.get("logical_rows"),
            "durable_attempt_rows": durability.get("attempt_rows"),
        },
        "source_digests": {
            "deterministic": sha256(json.dumps(deterministic, sort_keys=True).encode()).hexdigest(),
            "real_smoke": sha256(json.dumps(real_smoke, sort_keys=True).encode()).hexdigest(),
            "durability": sha256(json.dumps(durability, sort_keys=True).encode()).hexdigest(),
        },
        "cost_basis": {
            "usage_exactness": real_smoke.get("usage_exactness", []),
            "reported_cost_usd": real_smoke.get("cost_usd", 0),
            "actual_cost_claimed": False,
        },
        "proof_boundary": [
            "Codex CLI exposes one Fyralis wrapper attempt; opaque internal service retries are not claimed.",
            "One bounded clean provider batch proves P1 telemetry, not semantic company understanding.",
        ],
    }
    report["phase_exit_ready"] = all(hard_gates.values()) and all(criteria.values())
    report["passed"] = report["phase_exit_ready"]
    contributions = deterministic.get("p9_member_contributions")
    provenance = deterministic.get("run_provenance")
    if isinstance(contributions, Mapping) and isinstance(provenance, Mapping):
        if provenance.get("git_commit") != commit:
            raise ValueError("P1 evidence planes must bind the same full commit")
        report.update({
            "attempt_history": [
                *list(deterministic.get("attempt_history") or ()),
                *list(real_smoke.get("attempt_history") or ()),
            ],
            "batches": [
                *list(deterministic.get("batches") or ()),
                {
                    "batch_id": "p1-real-provider-smoke",
                    "wall_seconds": elapsed,
                    "physical_attempt_count": attempts,
                    "passed": bool(real_smoke.get("passed")),
                },
            ],
            "hook_scan": dict(deterministic.get("hook_scan") or {}),
            "cost_reconciliation": {
                "deterministic": deterministic.get("cost_reconciliation"),
                "real_usage_exactness": real_smoke.get("usage_exactness"),
                "real_cost_usd": real_smoke.get("cost_usd"),
            },
        })
        gate_members = {
            name: [dict(item) for item in members]
            for name, members in contributions["gate_members"].items()
        }
        metric_members = {
            name: [dict(item) for item in members]
            for name, members in contributions["metric_members"].items()
        }
        real_digest = canonical_sha256(real_smoke)
        durable_digest = canonical_sha256(durability)
        gate_members["HG-13_observability_integrity"].extend(({
            "member_id": "real-provider-smoke",
            "conforms": bool(real_smoke.get("passed")),
            "raw_source_digest": real_digest,
        }, {
            "member_id": "durable-receipt-reopen",
            "conforms": bool(
                durability.get("passed")
                and durability.get("reopened_on_new_connection")
                and durability.get("identical_replay_idempotent")
            ),
            "raw_source_digest": durable_digest,
        }))
        for item in real_smoke.get("attempt_history") or ():
            attempt_id = str(item.get("physical_attempt_id") or "missing")
            item_digest = canonical_sha256(item)
            metric_members["attempt_receipt_coverage"].append({
                "member_id": f"real-attempt:{attempt_id}",
                "numerator": int(bool(item.get("logical_call_id") and item.get("outcome"))),
                "denominator": 1, "raw_source_digest": item_digest,
            })
            metric_members["cost_coverage"].append({
                "member_id": f"real-attempt:{attempt_id}:reported-usage",
                "numerator": int(item.get("usage_exactness") == "reported"),
                "denominator": 1, "raw_source_digest": item_digest,
            })
        metric_members["count_reconciliation"].append({
            "member_id": "real-durable-count-reconciliation",
            "numerator": int(
                attempts == int(durability.get("attempt_rows") or -1)
                and int(real_smoke.get("logical_call_count") or 0)
                == int(durability.get("logical_rows") or -1)
            ),
            "denominator": 1,
            "raw_source_digest": canonical_sha256({
                "real": real_smoke, "durability": durability,
            }),
        })
        report = attach_p9_member_evidence(
            report, phase="p1", gate_members=gate_members,
            metric_members=metric_members,
            run_provenance=provenance,
        )
    return report


def finalize_p1_files(
    *, deterministic_path: Path, real_smoke_path: Path, durability_path: Path, commit: str
) -> dict[str, Any]:
    return finalize_p1(
        deterministic=_load(deterministic_path),
        real_smoke=_load(real_smoke_path),
        durability=_load(durability_path),
        commit=commit,
    )


__all__ = ["finalize_p1", "finalize_p1_files"]
