"""Fail-closed P8 exit composition and hash-reopen adversarial review."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from lib.contracts.kernel import canonical_sha256


def _read_reopened(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    first = path.read_bytes()
    first_sha = hashlib.sha256(first).hexdigest()
    parsed = json.loads(first)
    second = path.read_bytes()
    review = {
        "path": str(path), "sha256": first_sha,
        "stable_on_reopen": first == second,
        "nonempty": bool(parsed),
    }
    embedded = parsed.get("artifact_digest") or parsed.get("evidence_digest")
    if isinstance(embedded, str):
        body = dict(parsed)
        body.pop("artifact_digest", None)
        review["embedded_digest"] = embedded
        review["canonical_digest_matches"] = embedded == canonical_sha256(body)
    else:
        review["canonical_digest_matches"] = None
    return parsed, review


def _metrics_complete(value: Any) -> bool:
    if isinstance(value, dict):
        if "denominator" in value and any(key in value for key in ("score", "f1")):
            source_ids = value.get("source_example_ids")
            if (
                not value.get("worst_example_ids")
                or not isinstance(source_ids, list)
                or len(source_ids) != value["denominator"]
                or value.get("source_artifact_digest") != canonical_sha256(source_ids)
                or not set(value["worst_example_ids"]).issubset(source_ids)
            ):
                return False
        return all(_metrics_complete(item) for item in value.values())
    if isinstance(value, list):
        return all(_metrics_complete(item) for item in value)
    return True


def _contention_complete(artifact: dict[str, Any]) -> bool:
    result = artifact.get("result", {})
    expected = {"p8-bs10-h12-t1", "p8-bs25-h12-t5", "p8-bs50-h12-t20"}
    return bool(
        artifact.get("schema_version") == "p8-shared-contention-v2"
        and set(result.get("selected_cell_ids", ())) == expected
        and result.get("concurrent_cells") == len(expected)
        and isinstance(result.get("wall_time_ms"), (int, float))
        and result["wall_time_ms"] > 0
        and isinstance(result.get("individual_wall_time_sum_ms"), (int, float))
        and result["individual_wall_time_sum_ms"] > 0
        and isinstance(result.get("contention_ratio"), (int, float))
        and result["contention_ratio"] > 0
        and len(result.get("evidence_digest", "")) == 64
    )


def compose_p8_exit(
    *, fault_path: Path, scale_path: Path, characterization_path: Path,
    contention_path: Path, provider_canary_path: Path,
) -> dict[str, Any]:
    artifacts, reviews = {}, []
    for name, path in (
        ("fault", fault_path), ("scale", scale_path),
        ("characterization", characterization_path), ("contention", contention_path),
    ):
        artifacts[name], review = _read_reopened(path)
        reviews.append(review)
    canary_bytes = provider_canary_path.read_bytes()
    canary_rows = [json.loads(line) for line in canary_bytes.splitlines() if line.strip()]
    usage = next((row["usage"] for row in reversed(canary_rows) if row.get("type") == "turn.completed"), None)
    scale_gates = artifacts["scale"].get("evaluation", {}).get("gates", {})
    latency_green = bool(scale_gates.get("concurrency_latency_ratio"))
    characterization_complete = _metrics_complete(artifacts["characterization"])
    fault_qualification = artifacts["fault"].get("member_receipt_qualification")
    gates = {
        "fault_schedule_12x2": len(artifacts["fault"].get("bound_execution_evidence", {}).get("fault_execution_keys", [])) == 24,
        "fault_zero_counts_denominator_complete": bool(fault_qualification) and all(
            not isinstance(value, dict) or value.get("gate") is not False
            for value in fault_qualification.values()
        ),
        "isolated_scale_27": len(artifacts["scale"].get("execution", {}).get("cells", [])) == 27
            and bool(artifacts["scale"].get("execution", {}).get("physically_isolated_databases")),
        "scale_latency": latency_green,
        "scale_queue_growth_resource_complete": all(scale_gates.get(key) is True for key in (
            "all_production_queue_families_measured", "resource_sample_every_durable_barrier",
            "deterministic_token_status_explicit", "derived_refresh_pipeline_executed",
        )),
        "deterministic_token_status_exact_not_estimated": scale_gates.get("exact_provider_prompt_token_measurement") is False,
        "shared_contention_separate": _contention_complete(artifacts["contention"]),
        "characterization_reporting_complete": characterization_complete,
        "authorized_provider_canaries": False if not latency_green else False,
        "provider_usage_observability": isinstance(usage, dict) and usage.get("input_tokens", 0) > 0,
        "hash_reopen_review": all(
            row["stable_on_reopen"] and row["nonempty"]
            and row["canonical_digest_matches"] is not False
            for row in reviews
        ),
        "single_commit_evidence": False,
    }
    result = {
        "schema_version": "epistemic-repair-p8-fault-scale-v1",
        "artifact_reviews": reviews,
        "provider_usage_receipt": usage,
        "provider_canary_policy": {
            "status": "gated_off", "reason": "deterministic_latency_red",
            "authorized_points": ["p8-bs25-h12-t1", "largest_deterministic_passing_cell"],
        },
        "gates": gates,
        "exit_ready": all(gates.values()),
        "source_artifact_sha256": {row["path"]: row["sha256"] for row in reviews},
    }
    result["artifact_digest"] = canonical_sha256(result)
    return result
