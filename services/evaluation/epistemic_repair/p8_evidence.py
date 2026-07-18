"""Bind independently queried P8 DB and provider receipts to the sealed schedule."""

from __future__ import annotations

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.p8_oracles import ProductionExecutionEvidence
from lib.evaluation.epistemic_repair.p8_population import (
    build_fault_schedule,
    build_scale_matrix,
    fault_injection_points,
)
from services.evaluation.epistemic_repair.p8_postgres_runner import (
    P8_PHYSICAL_BATCH_SIZE,
    P8_SIMULATED_SOURCE,
    PostgresFaultSlice,
)
from lib.evaluation.epistemic_repair.p8_provider_runner import ProviderFaultSlice
from services.evaluation.epistemic_repair.p8_scale_runner import ScaleExecution


def summarize_fault_member_receipts(
    *, postgres: PostgresFaultSlice, provider: ProviderFaultSlice,
) -> dict[str, object]:
    """Render only claims directly derivable from denominator member receipts."""
    db, llm = postgres.receipts, provider.receipts
    attempts = len(db) + len(llm)
    return {
        "preregistered_physical_attempt_budget": 24,
        "observed_member_receipts": attempts,
        "attempt_budget_respected": attempts <= 24,
        "every_physical_attempt_has_receipt": attempts == 24 and all(
            row.persisted_attempt_receipts == 1 for row in llm
        ),
        "database_fault_batches": {
            "denominator": len(db),
            "exact_25_signal_batches": sum(
                row.persisted_observation_count == P8_PHYSICAL_BATCH_SIZE for row in db
            ),
            "post_ingestion_sources": sorted({row.simulated_source_channel for row in db}),
            "gate": bool(db) and all(
                row.persisted_observation_count == P8_PHYSICAL_BATCH_SIZE
                and row.simulated_source_channel == P8_SIMULATED_SOURCE
                for row in db
            ),
        },
        "exact_fault_boundaries_reached": {
            "denominator": len(db),
            "matched": sum(
                row.reached_boundary == fault_injection_points()[row.boundary]
                and len(row.reached_boundary_receipt_digest) == 64
                for row in db
            ),
            "gate": bool(db) and all(
                row.reached_boundary == fault_injection_points()[row.boundary]
                and len(row.reached_boundary_receipt_digest) == 64
                for row in db
            ),
        },
        "exactly_once_barrier_violations": sum(row.post_restart_barrier_count != 1 for row in db),
        "duplicate_model_violations": sum(row.post_restart_model_count > 1 for row in db),
        "pending_truth_critical_violations": sum(row.post_restart_pending_count != 0 for row in db),
        "member_receipt_digest_failures": sum(
            len(row.queried_state_digest) != 64 or len(row.replay_receipt_digest) != 64 for row in db
        ) + sum(len(row.queried_receipt_digest) != 64 for row in llm),
        "terminal_fates": sorted({row.pre_restart_fate for row in db}) + sorted(
            {row.observed_outcome for row in llm}
        ),
        "cross_tenant_effects": {"violations": sum(row.cross_tenant_model_hits for row in db),
                                 "gate": all(row.cross_tenant_model_hits == 0 for row in db)},
        "duplicate_relation_transitions": {"violations": sum(row.relation_version_count for row in db),
                                           "gate": all(row.relation_version_count == 0 for row in db)},
        "duplicate_lifecycle_transitions": {
            "violations": sum(row.duplicate_lifecycle_transition_count for row in db),
            "gate": all(row.duplicate_lifecycle_transition_count == 0 for row in db),
        },
        "partial_truth_state": {"violations": sum(row.partial_truth_state_count for row in db),
                                "gate": all(row.partial_truth_state_count == 0 for row in db)},
        "stale_active_truth": {"violations": sum(row.stale_active_truth_count for row in db),
                               "gate": all(row.stale_active_truth_count == 0 for row in db)},
        "dead_letter_truth_critical_work": {
            "violations": sum(row.dead_letter_truth_critical_count for row in db),
            "gate": all(row.dead_letter_truth_critical_count == 0 for row in db),
        },
        "uninterrupted_reference_digest_equality": {
            "matched": sum(row.uninterrupted_reference_matches for row in db),
            "denominator": len(db),
            "gate": bool(db) and all(
                row.uninterrupted_reference_matches and len(row.uninterrupted_reference_digest) == 64
                for row in db
            ),
        },
    }


def bind_fault_execution_evidence(
    *, postgres: PostgresFaultSlice, provider: ProviderFaultSlice, commit_sha: str,
) -> ProductionExecutionEvidence:
    schedule = build_fault_schedule()
    case_by_boundary = {case.boundary: case.case_id for case in schedule.cases}
    observed = {
        (row.boundary, row.duplicate_delivery)
        for row in (*postgres.receipts, *provider.receipts)
    }
    expected = {(case.boundary, duplicate) for case in schedule.cases for duplicate in (False, True)}
    if observed != expected:
        missing = sorted(expected - observed)
        extra = sorted(observed - expected)
        raise ValueError(f"fault execution evidence is not denominator complete: missing={missing} extra={extra}")
    if any(
        row.post_restart_barrier_count != 1
        or row.post_restart_model_count != 1
        or row.post_restart_pending_count != 0
        or len(row.replay_receipt_digest) != 64
        or len(row.queried_state_digest) != 64
        or row.persisted_observation_count != P8_PHYSICAL_BATCH_SIZE
        or row.simulated_source_channel != P8_SIMULATED_SOURCE
        or row.reached_boundary != fault_injection_points()[row.boundary]
        or len(row.reached_boundary_receipt_digest) != 64
        for row in postgres.receipts
    ):
        raise ValueError("PostgreSQL fault receipt failed durable-state binding")
    if any(
        row.persisted_logical_receipts != 1
        or row.persisted_attempt_receipts != 1
        or len(row.queried_receipt_digest) != 64
        for row in provider.receipts
    ):
        raise ValueError("provider fault receipt failed durable-attempt binding")
    execution_keys = tuple(sorted(
        f"{case_by_boundary[boundary]}:{int(duplicate)}"
        for boundary, duplicate in observed
    ))
    evidence_digest = canonical_sha256({
        "schedule_digest": schedule.digest,
        "postgres_evidence_digest": postgres.evidence_digest,
        "provider_evidence_digest": provider.evidence_digest,
        "execution_keys": execution_keys,
    })
    return ProductionExecutionEvidence(
        database_run_id=f"{postgres.database_run_id}+{provider.database_run_id}",
        commit_sha=commit_sha,
        database_evidence_digest=evidence_digest,
        fault_execution_keys=execution_keys,
        scale_execution_cell_ids=(),
        characterization_population_digests=(),
        attempt_receipts_persisted=True,
        canonical_digests_queried_after_restart=True,
        isolated_database_per_scale_cell=False,
    )


def bind_scale_execution_evidence(
    *, prior: ProductionExecutionEvidence, scale: ScaleExecution,
) -> ProductionExecutionEvidence:
    """Bind exact measured cell IDs without overstating isolation strength."""

    expected = {cell.cell_id for cell in build_scale_matrix()}
    observed = {cell.cell_id for cell in scale.cells}
    if observed != expected or not scale.exact_matrix_coverage:
        raise ValueError("scale execution evidence is not exact 27-cell coverage")
    return ProductionExecutionEvidence(
        database_run_id=prior.database_run_id,
        commit_sha=prior.commit_sha,
        database_evidence_digest=canonical_sha256({
            "prior": prior.database_evidence_digest,
            "scale": scale.evidence_digest,
            "cells": sorted(observed),
        }),
        fault_execution_keys=prior.fault_execution_keys,
        scale_execution_cell_ids=tuple(sorted(observed)),
        characterization_population_digests=prior.characterization_population_digests,
        attempt_receipts_persisted=prior.attempt_receipts_persisted,
        canonical_digests_queried_after_restart=prior.canonical_digests_queried_after_restart,
        isolated_database_per_scale_cell=scale.physically_isolated_databases,
    )
