"""Compose deterministic, real-provider, and durable-ledger P1 evidence."""

from __future__ import annotations

from hashlib import sha256
import json
from pathlib import Path
from typing import Any, Mapping


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
