"""Independent P8 result contracts and hard-gate oracle."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from lib.contracts.kernel import canonical_sha256


@dataclass(frozen=True, slots=True)
class AttemptReceipt:
    receipt_id: str
    fault_case_id: str
    duplicate_delivery: bool
    attempt_number: int
    boundary: str
    fate: str


@dataclass(frozen=True, slots=True)
class FaultResult:
    fault_case_id: str
    duplicate_delivery: bool
    reference_digest: str
    recovered_canonical_digest: str
    recovered_derived_digest: str
    attempts: tuple[AttemptReceipt, ...]
    duplicate_truth_rows: int
    partial_truth_rows: int
    stale_active_truth: int
    dead_lettered_truth_work: int
    cross_tenant_effects: int


@dataclass(frozen=True, slots=True)
class ScaleResult:
    cell_id: str
    batch_size: int
    memory_horizon_batches: int
    tenant_concurrency: int
    queue_depth_slope_final_half: float
    retrieval_p95_ms: float
    prompt_token_p95: int
    first_quartile_model_rate: float
    last_quartile_model_rate: float
    new_gold_theses: int
    refresh_per_unique_version: float
    candidate_growth_slope: float
    residual_growth_slope: float
    review_growth_slope: float
    negative_memory_growth_slope: float
    latency_p95_ms: float
    fairness_ratio: float
    cross_tenant_leakage: int
    semantic_quality: float
    hg_gates_green: bool


@dataclass(frozen=True, slots=True)
class MetricDistribution:
    population: str
    denominator: int
    score: float
    ci95_low: float
    ci95_high: float
    worst_example_ids: tuple[str, ...]
    source_artifact_digest: str


@dataclass(frozen=True, slots=True)
class ProductionExecutionEvidence:
    """Durable evidence binding; never inferred from reference vectors."""

    database_run_id: str
    commit_sha: str
    database_evidence_digest: str
    fault_execution_keys: tuple[str, ...]
    scale_execution_cell_ids: tuple[str, ...]
    characterization_population_digests: tuple[str, ...]
    attempt_receipts_persisted: bool
    canonical_digests_queried_after_restart: bool
    isolated_database_per_scale_cell: bool


def reference_state_digest() -> str:
    return canonical_sha256({
        "models": [{"address": "project:harbor/status", "version": 2, "state": "accepted", "value": "ready"}],
        "relations": [],
        "obligations": [{"subject": "project:harbor", "state": "resolved"}],
        "projection_generation": 2,
    })


def evaluate_p8(
    *, faults: tuple[FaultResult, ...], scale: tuple[ScaleResult, ...],
    distributions: tuple[MetricDistribution, ...], schedule_digest: str,
    manifest_digests: tuple[str, ...],
    production_evidence: ProductionExecutionEvidence | None = None,
) -> dict[str, Any]:
    fault_ok = all(
        row.reference_digest == row.recovered_canonical_digest == row.recovered_derived_digest
        and row.attempts and len(row.attempts) <= 2
        and all(receipt.fate in {"injected_fault", "applied", "duplicate_noop"} for receipt in row.attempts)
        and row.duplicate_truth_rows == row.partial_truth_rows == row.stale_active_truth
        == row.dead_lettered_truth_work == row.cross_tenant_effects == 0
        for row in faults
    )
    exact_fault_coverage = len(faults) == 24 and len({(x.fault_case_id, x.duplicate_delivery) for x in faults}) == 24

    by_key = {(x.batch_size, x.memory_horizon_batches, x.tenant_concurrency): x for x in scale}
    exact_scale_coverage = len(scale) == 27 and len(by_key) == 27
    scale_ok = exact_scale_coverage and all(
        x.hg_gates_green and x.queue_depth_slope_final_half <= 0
        and x.refresh_per_unique_version <= 1.10
        and max(x.candidate_growth_slope, x.residual_growth_slope, x.review_growth_slope, x.negative_memory_growth_slope) <= 0
        and x.fairness_ratio >= .80 and x.cross_tenant_leakage == 0
        for x in scale
    )
    if scale_ok:
        for batch in (10, 25, 50):
            for tenants in (1, 5, 20):
                short, long = by_key[(batch, 12, tenants)], by_key[(batch, 100, tenants)]
                scale_ok &= long.retrieval_p95_ms <= 2 * short.retrieval_p95_ms
                scale_ok &= long.prompt_token_p95 <= 1.25 * short.prompt_token_p95
                scale_ok &= abs(long.semantic_quality - short.semantic_quality) <= .03
            for horizon in (12, 50, 100):
                one, twenty = by_key[(batch, horizon, 1)], by_key[(batch, horizon, 20)]
                scale_ok &= twenty.latency_p95_ms <= 2 * one.latency_p95_ms
    scale_ok &= all(x.last_quartile_model_rate <= .5 * x.first_quartile_model_rate for x in scale)
    characterization_ok = len(distributions) == 5 and len(manifest_digests) == 5 and all(
        d.denominator > 0 and 0 <= d.ci95_low <= d.score <= d.ci95_high <= 1
        and d.worst_example_ids and len(d.source_artifact_digest) == 64 for d in distributions
    )
    contract_gates = {
        "P8-CONTRACT-01_exact_fault_schedule": exact_fault_coverage,
        "P8-CONTRACT-02_fault_oracle_vectors": fault_ok,
        "P8-CONTRACT-03_complete_scale_matrix": exact_scale_coverage,
        "P8-CONTRACT-04_scale_oracle_vectors": bool(scale_ok),
        "P8-CONTRACT-05_sealed_component_manifests": characterization_ok,
    }
    expected_fault_keys = {
        f"{row.fault_case_id}:{int(row.duplicate_delivery)}" for row in faults
    }
    expected_scale_cells = {row.cell_id for row in scale}
    production_faults = bool(
        production_evidence
        and production_evidence.attempt_receipts_persisted
        and production_evidence.canonical_digests_queried_after_restart
        and set(production_evidence.fault_execution_keys) == expected_fault_keys
        and len(production_evidence.database_evidence_digest) == 64
    )
    production_scale = bool(
        production_evidence
        and production_evidence.isolated_database_per_scale_cell
        and set(production_evidence.scale_execution_cell_ids) == expected_scale_cells
    )
    production_characterization = bool(
        production_evidence
        and set(production_evidence.characterization_population_digests)
        == set(manifest_digests)
    )
    execution_gates = {
        "P8-HG-01_durable_fault_execution": production_faults,
        "P8-HG-02_production_restart_digest_convergence": production_faults,
        "P8-HG-03_production_scale_matrix": production_scale,
        "P8-HG-04_executed_component_characterization": production_characterization,
        "P8-HG-05_real_provider_canaries": False,
    }
    artifact = {
        "schema_version": "epistemic-repair-p8-fault-scale-v1",
        "schedule_digest": schedule_digest,
        "manifest_digests": list(manifest_digests),
        "fault_reference_vectors": [asdict(x) for x in faults],
        "scale_reference_vectors": [asdict(x) for x in scale],
        "component_reference_distributions": [asdict(x) for x in distributions],
        "contract_gates": contract_gates,
        "hard_gates": execution_gates,
        "production_execution_evidence": asdict(production_evidence) if production_evidence else None,
        "real_provider_canaries": {
            "status": "not_run",
            "authorized_cells": ["p8-bs25-h12-t1", "largest_deterministic_passing_cell"],
            "reason": "deterministic qualification artifact; provider evidence must remain separate",
        },
        "shared_resource_contention": {"status": "not_claimed", "isolated_from_semantic_matrix": True},
        "execution_mode": "sealed_evaluator_contract_only",
        "evaluator_contract_ready": all(contract_gates.values()),
        "deterministic_qualification_ready": all(execution_gates[key] for key in execution_gates if key != "P8-HG-05_real_provider_canaries"),
        "phase_exit_ready": all(execution_gates.values()),
    }
    artifact["artifact_digest"] = canonical_sha256(artifact)
    return artifact
