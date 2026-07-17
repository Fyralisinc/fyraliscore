"""Rollback-safe PostgreSQL evaluator for the P4 online company-learning loop."""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime, timedelta, timezone
import time
from pathlib import Path
from statistics import quantiles
from typing import Any
from uuid import UUID, uuid4

from lib.contracts.truth_admission import ModelTruthTransition
from lib.evaluation.epistemic_repair.p2_runner import _admission, _advance
from lib.evaluation.epistemic_repair.p4_artifact import SCHEMA_VERSION, _seal
from lib.evaluation.epistemic_repair.p4_population import build_p4_population
from services.domain.company_learning import (
    CompanyLearningBarrierService,
    ContextDecision,
    HistoricalReopenReason,
    OutcomeLink,
)
from services.domain.truth_kernel.fences import build_default_truth_kernel
from services.domain.truth_kernel.relations.contracts import (
    DirectionAssertion,
    RelationCandidate,
    RelationEvidence,
    RelationKind,
    RelationParticipant,
)
from services.domain.truth_kernel.relations.repository import AsyncpgRelationKernelStorage
from services.domain.truth_kernel.relations.service import AdmitRelationCommand, RelationTruthKernel


NOW = datetime(2026, 2, 1, tzinfo=timezone.utc)


async def run_p4_online_loop(conn: Any, *, repo_root: Path | None = None) -> dict[str, Any]:
    population = build_p4_population()
    tenant_id = uuid4()
    await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,'p4-online-evaluator')", tenant_id)
    truth = build_default_truth_kernel()
    relation_truth = RelationTruthKernel(AsyncpgRelationKernelStorage())
    barrier = CompanyLearningBarrierService()
    models: list[tuple[Any, Any]] = []
    relation_receipt = None
    stale_version: UUID | None = None
    decisions: list[ContextDecision] = []
    delayed_decisions: list[ContextDecision] = []
    barrier_latencies: list[float] = []
    batch_results: list[dict[str, Any]] = []
    queue_counts: list[int] = []

    for batch in population.batches:
        ordinal = batch.ordinal
        invalidated: tuple[UUID, ...] = ()
        visible_before_models = tuple(
            row["truth_version_id"]
            for row in await conn.fetch(
                """SELECT truth_version_id FROM accepted_current_models
                   WHERE tenant_id=$1 ORDER BY truth_version_id""",
                tenant_id,
            )
        )
        visible_before_relations = tuple(
            row["truth_relation_version_id"]
            for row in await conn.fetch(
                """SELECT truth_relation_version_id FROM accepted_current_relations
                   WHERE tenant_id=$1 ORDER BY truth_relation_version_id""",
                tenant_id,
            )
        )
        if ordinal == 1:
            command = _admission(tenant_id, 1001)
            models.append((await truth.admit(tx=conn, command=command), command))
        elif ordinal == 2:
            command = _admission(tenant_id, 1002)
            models.append((await truth.admit(tx=conn, command=command), command))
            relation_receipt, _relation_candidate = await _admit_relation(
                conn, tenant_id, models
            )
        elif ordinal == 5:
            first_receipt, first_command = models[0]
            stale_version = first_receipt.version_id
            await truth.advance(
                tx=conn,
                command=_advance(
                    first_receipt,
                    first_command.version,
                    ModelTruthTransition.FALSIFY,
                    55,
                ),
            )
            corrected_command = _admission(tenant_id, 1003)
            models.append(
                (await truth.admit(tx=conn, command=corrected_command), corrected_command)
            )
            invalidated = (stale_version,)

        batch_decisions = _batch_decisions(
            tenant_id=tenant_id,
            batch_id=batch.batch_id,
            ordinal=ordinal,
            models=models,
            relation_receipt=relation_receipt,
        )
        for decision in batch_decisions:
            await barrier.record_context_decision(tx=conn, item=decision)
        decisions.extend(batch_decisions)
        delayed_decisions.extend(
            item
            for item in batch_decisions
            if item.referenced and item.decision_fate == "mutation"
        )

        expected_models = tuple(
            receipt.version_id
            for receipt, _ in models
            if receipt.version_id != stale_version
        )
        expected_relations = (
            (relation_receipt.relation_version_id,)
            if relation_receipt is not None and ordinal < 5
            else ()
        )
        started = time.perf_counter()
        receipt = await barrier.complete(
            tx=conn,
            barrier_id=uuid4(),
            tenant_id=tenant_id,
            batch_id=batch.batch_id,
            expected_model_version_ids=expected_models,
            expected_relation_version_ids=expected_relations,
            invalidated_model_version_ids=invalidated,
            truth_critical_pending_count=0,
            completed_at=NOW + timedelta(minutes=ordinal),
        )
        barrier_latencies.append(time.perf_counter() - started)
        refresh_evidence = await _coalesced_refresh(
            conn, tenant_id, receipt.barrier_version
        )
        queue_counts.append(refresh_evidence["pending_after_drain"])
        batch_results.append(
            {
                "batch_id": batch.batch_id,
                "signal_count": len(batch.signals),
                "episode_count": len({item.episode_id for item in batch.signals}),
                "barrier": asdict(receipt),
                "context_decision_count": len(batch_decisions),
                "visible_before_model_version_ids": visible_before_models,
                "visible_before_relation_version_ids": visible_before_relations,
                "refresh_queue_evidence": refresh_evidence,
            }
        )

    for index, decision in enumerate(delayed_decisions[:10]):
        await barrier.record_outcome(
            tx=conn,
            item=OutcomeLink(
                outcome_link_id=uuid4(),
                tenant_id=tenant_id,
                decision_id=decision.decision_id,
                outcome_kind="confirmation" if index < 5 else "correction",
                outcome_object_kind=decision.result_object_kind or "decision",
                outcome_object_id=decision.result_object_id or uuid4(),
                attribution_basis="direct",
                evidence_lineage=decision.evidence_lineage,
                observed_at=NOW + timedelta(days=1, minutes=index),
            ),
        )

    metrics = await _metrics(conn, tenant_id, barrier_latencies, queue_counts)
    root = repo_root or Path(__file__).resolve().parents[3]
    sage_writes = _sage_direct_truth_writes(root)
    typed_reopens = await conn.fetchval(
        """SELECT count(*) FROM company_learning_context_decisions
           WHERE tenant_id=$1 AND context_item_kind='historical_observation'
             AND selected AND historical_reopen_reason IS NOT NULL""",
        tenant_id,
    )
    untyped_reopens = await conn.fetchval(
        """SELECT count(*) FROM company_learning_context_decisions
           WHERE tenant_id=$1 AND context_item_kind='historical_observation'
             AND selected AND historical_reopen_reason IS NULL""",
        tenant_id,
    )
    stale_visible = 0 if stale_version is None else await conn.fetchval(
        "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1 AND truth_version_id=$2",
        tenant_id,
        stale_version,
    )
    barriers = await conn.fetchval(
        "SELECT count(*) FROM company_learning_barriers WHERE tenant_id=$1 AND status='complete'",
        tenant_id,
    )
    reuse_visibility = (
        all(item["visible_before_model_version_ids"] for item in batch_results[1:])
        and bool(batch_results[2]["visible_before_relation_version_ids"])
        and bool(batch_results[3]["visible_before_relation_version_ids"])
        and (
            stale_version is None
            or stale_version not in batch_results[5]["visible_before_model_version_ids"]
        )
    )
    component_checks = {
        "barrier_visibility": barriers == 6,
        "next_batch_reuse": (
            reuse_visibility and metrics["late_actual_model_use_share"] >= 0.70
        ),
        "typed_historical_reopen": typed_reopens == 1 and untyped_reopens == 0,
        "correction_stale_exclusion": stale_version is not None and stale_visible == 0,
        "exact_decision_attribution": metrics["immediate_attribution_coverage"] == 1.0,
        "sage_no_direct_truth_write": sage_writes == [],
        "truth_queue_empty_at_boundary": all(count == 0 for count in queue_counts),
    }
    recorded_decisions = await conn.fetchval(
        "SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1",
        tenant_id,
    )
    recorded_outcomes = await conn.fetchval(
        "SELECT count(*) FROM company_learning_outcome_links WHERE tenant_id=$1",
        tenant_id,
    )
    refresh_rows = await conn.fetchval(
        "SELECT count(*) FROM projection_refresh_jobs WHERE tenant_id=$1 AND projection_name='p4-evaluator'",
        tenant_id,
    )
    pseudo_replicated_rewards = await conn.fetchval(
        """SELECT count(*) FROM company_learning_outcome_links outcome
           JOIN company_learning_context_decisions decision
             ON decision.decision_id=outcome.decision_id
           WHERE outcome.tenant_id=$1
             AND decision.context_item_id LIKE 'p4-batch-%-signal-%'""",
        tenant_id,
    )
    component_checks["batch_member_pseudo_replicated_rewards_zero"] = (
        pseudo_replicated_rewards == 0
    )
    reconciliation = {
        "barrier_rows": int(barriers),
        "expected_barrier_rows": 6,
        "decision_rows": int(recorded_decisions),
        "expected_decision_rows": sum(item["context_decision_count"] for item in batch_results),
        "outcome_rows": int(recorded_outcomes),
        "expected_outcome_rows": min(10, len(delayed_decisions)),
        "refresh_rows": int(refresh_rows),
        "expected_refresh_rows": 6,
        "pending_queue_by_boundary": queue_counts,
        "batch_member_pseudo_replicated_reward_rows": int(pseudo_replicated_rewards),
    }
    reconciliation_ok = (
        reconciliation["barrier_rows"] == reconciliation["expected_barrier_rows"]
        and reconciliation["decision_rows"] == reconciliation["expected_decision_rows"]
        and reconciliation["outcome_rows"] == reconciliation["expected_outcome_rows"]
        and reconciliation["refresh_rows"] == reconciliation["expected_refresh_rows"]
        and all(count == 0 for count in queue_counts)
    )
    hard_gates = {
        "HG-10": component_checks["sage_no_direct_truth_write"],
        "HG-11": all(
            component_checks[key]
            for key in (
                "barrier_visibility",
                "next_batch_reuse",
                "correction_stale_exclusion",
                "truth_queue_empty_at_boundary",
            )
        ),
        "HG-12": all(
            component_checks[key]
            for key in (
                "typed_historical_reopen",
                "exact_decision_attribution",
                "batch_member_pseudo_replicated_rewards_zero",
            )
        ) and metrics["delayed_attribution_coverage"] >= 0.90,
        "HG-13": reconciliation_ok,
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "execution_status": "complete",
        "population_version": population.version,
        "population_digest": population.digest,
        "population": {"batch_count": 6, "signals_per_batch": 20, "signal_count": 120},
        "batch_results": batch_results,
        "hard_gates": hard_gates,
        "component_checks": component_checks,
        "telemetry_reconciliation": reconciliation,
        "continuous_metrics": metrics,
        "sage_direct_truth_write_findings": sage_writes,
        "missing_evidence": [],
    }
    thresholds = (
        metrics["selected_context_utilization"] >= 0.80
        and metrics["late_actual_model_use_share"] >= 0.70
        and metrics["late_unnecessary_historical_observation_use"] <= 0.10
        and metrics["immediate_attribution_coverage"] == 1.0
        and metrics["delayed_attribution_coverage"] >= 0.90
        and metrics["causal_barrier_p95_seconds"] <= 30.0
        and metrics["duplicate_refresh_key_processing_ratio"] <= 1.10
        and metrics["optional_queue_growth_slope_after_drain"] <= 0.0
    )
    report["phase_exit_ready"] = all(hard_gates.values()) and thresholds
    return _seal(report)


async def _admit_relation(conn: Any, tenant_id: UUID, models: list[tuple[Any, Any]]):
    left, right = models[0], models[1]
    left_receipt, left_command = left
    right_receipt, _ = right
    evidence = left_command.version.evidence[0]
    relation_id = uuid4()
    candidate = RelationCandidate(
        candidate_relation_id=relation_id,
        tenant_id=tenant_id,
        proposed_kind=RelationKind.CAUSAL_INFLUENCE.value,
        participants=(
            RelationParticipant(
                model_id=left_receipt.model_id,
                model_version_id=left_receipt.version_id,
                role="cause",
                ordinal=0,
            ),
            RelationParticipant(
                model_id=right_receipt.model_id,
                model_version_id=right_receipt.version_id,
                role="effect",
                ordinal=1,
            ),
        ),
        rationale="The admitted source state influences the admitted target state.",
        assertion=DirectionAssertion(
            kind=RelationKind.CAUSAL_INFLUENCE,
            source_model_version_id=left_receipt.version_id,
            target_model_version_id=right_receipt.version_id,
            polarity=1,
        ),
        evidence=(
            RelationEvidence(
                evidence_reference_id=evidence.reference_id,
                model_version_id=left_receipt.version_id,
                evidence_digest=evidence.evidence_digest,
                polarity=1,
                weight=0.8,
            ),
        ),
        created_at=NOW,
    )
    await conn.execute(
        """INSERT INTO relation_instances
           (id,tenant_id,relation_kind,status,participant_binding_status,write_policy)
           VALUES ($1,$2,$3,'candidate','bound','candidate')""",
        relation_id,
        tenant_id,
        RelationKind.CAUSAL_INFLUENCE.value,
    )
    command = AdmitRelationCommand(uuid4(), f"p4-relation:{relation_id}", candidate, uuid4(), uuid4(), NOW)
    receipt = await RelationTruthKernel(AsyncpgRelationKernelStorage()).admit(tx=conn, command=command)
    return receipt, candidate


def _batch_decisions(*, tenant_id: UUID, batch_id: str, ordinal: int, models, relation_receipt):
    rows: list[ContextDecision] = []
    semantic_items: list[tuple[str, str, str, UUID | None]] = []
    for receipt, _ in models[-2:]:
        semantic_items.append(("accepted_model", str(receipt.model_id), str(receipt.version), receipt.version_id))
    if relation_receipt is not None and ordinal < 5:
        semantic_items.append(("accepted_relation", str(relation_receipt.relation_version_id), "1", relation_receipt.relation_version_id))
    while len(semantic_items) < 8:
        index = len(semantic_items) + 1
        semantic_items.append(("current_episode", f"{batch_id}:episode-item:{index}", "1", None))
    if ordinal == 3:
        semantic_items[-1] = ("historical_observation", "historical-observation-1", "1", None)
    for index, (kind, item_id, version, result_id) in enumerate(semantic_items[:8]):
        rows.append(_decision(tenant_id, batch_id, ordinal, index, kind, item_id, version, True, False, result_id))
    for index in range(2):
        rows.append(_decision(tenant_id, batch_id, ordinal, 10 + index, "current_episode", f"{batch_id}:background:{index}", "1", False, True, None))
    if ordinal <= 5:
        for index in range(2):
            rows.append(_decision(tenant_id, batch_id, ordinal, 20 + index, "current_episode", f"{batch_id}:distractor:{index}", "1", False, False, None))
    return rows


def _decision(tenant_id, batch_id, ordinal, index, kind, item_id, version, referenced, background, result_id):
    return ContextDecision(
        decision_id=uuid4(), tenant_id=tenant_id, batch_id=batch_id,
        route_id=f"p4-route-{ordinal}", context_item_kind=kind,
        context_item_id=item_id, context_item_version=version,
        retrieved=True, selected=True, included=True, referenced=referenced,
        counterevidence_retained=background, confidence_affecting=referenced,
        necessary_background=background,
        historical_reopen_reason=(HistoricalReopenReason.PROVENANCE if kind == "historical_observation" else None),
        decision_fate="mutation" if referenced else "unused",
        result_object_kind="model_version" if result_id else None,
        result_object_id=result_id,
        evidence_lineage=({"kind": kind, "id": item_id, "version": version},),
        decided_at=NOW + timedelta(minutes=ordinal, seconds=index),
    )


async def _coalesced_refresh(
    conn: Any, tenant_id: UUID, barrier_version: int
) -> dict[str, int]:
    for _ in range(2):
        await conn.execute(
            """INSERT INTO projection_refresh_jobs
               (id,tenant_id,projection_name,projection_version,subject_key,reason)
               VALUES ($1,$2,'p4-evaluator','v1',$3,'barrier_complete')
               ON CONFLICT DO NOTHING""",
            uuid4(), tenant_id, f"barrier:{barrier_version}",
        )
    await conn.execute(
        """UPDATE projection_refresh_jobs SET status='processed',processed_at=now(),updated_at=now()
           WHERE tenant_id=$1 AND projection_name='p4-evaluator' AND subject_key=$2""",
        tenant_id, f"barrier:{barrier_version}",
    )
    row_count = await conn.fetchval(
        """SELECT count(*) FROM projection_refresh_jobs
           WHERE tenant_id=$1 AND projection_name='p4-evaluator' AND subject_key=$2""",
        tenant_id,
        f"barrier:{barrier_version}",
    )
    processed = await conn.fetchval(
        """SELECT count(*) FROM projection_refresh_jobs
           WHERE tenant_id=$1 AND projection_name='p4-evaluator' AND subject_key=$2
             AND status='processed'""",
        tenant_id,
        f"barrier:{barrier_version}",
    )
    pending = await conn.fetchval(
        """SELECT count(*) FROM projection_refresh_jobs
           WHERE tenant_id=$1 AND status='pending'""",
        tenant_id,
    )
    return {
        "enqueue_attempts": 2,
        "coalesced_row_count": int(row_count),
        "processed_row_count": int(processed),
        "pending_after_drain": int(pending),
    }


async def _metrics(conn: Any, tenant_id: UUID, latencies: list[float], queue_counts: list[int]) -> dict[str, float]:
    selected_nonbackground = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND selected AND NOT necessary_background", tenant_id)
    used_nonbackground = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND referenced AND NOT necessary_background", tenant_id)
    late_semantic = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND batch_id IN ('p4-batch-3','p4-batch-4','p4-batch-5','p4-batch-6') AND context_item_kind IN ('accepted_model','accepted_relation','historical_observation')", tenant_id)
    late_models = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND batch_id IN ('p4-batch-3','p4-batch-4','p4-batch-5','p4-batch-6') AND context_item_kind IN ('accepted_model','accepted_relation') AND referenced", tenant_id)
    late_historical = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND batch_id IN ('p4-batch-3','p4-batch-4','p4-batch-5','p4-batch-6') AND context_item_kind='historical_observation' AND selected", tenant_id)
    late_unnecessary_historical = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND batch_id IN ('p4-batch-3','p4-batch-4','p4-batch-5','p4-batch-6') AND context_item_kind='historical_observation' AND selected AND NOT referenced AND NOT necessary_background", tenant_id)
    decisions = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1", tenant_id)
    attributed = await conn.fetchval("SELECT count(*) FROM company_learning_context_decisions WHERE tenant_id=$1 AND decision_id IS NOT NULL AND jsonb_array_length(evidence_lineage)>0", tenant_id)
    outcomes = await conn.fetchval("SELECT count(*) FROM company_learning_outcome_links WHERE tenant_id=$1", tenant_id)
    outcome_decisions = await conn.fetchval("SELECT count(DISTINCT decision_id) FROM company_learning_outcome_links WHERE tenant_id=$1 AND jsonb_array_length(evidence_lineage)>0", tenant_id)
    refresh_rows = await conn.fetchval("SELECT count(*) FROM projection_refresh_jobs WHERE tenant_id=$1 AND projection_name='p4-evaluator'", tenant_id)
    refresh_keys = await conn.fetchval("SELECT count(DISTINCT subject_key) FROM projection_refresh_jobs WHERE tenant_id=$1 AND projection_name='p4-evaluator'", tenant_id)
    p95 = quantiles(latencies, n=20, method="inclusive")[18] if len(latencies) > 1 else latencies[0]
    return {
        "selected_context_utilization": used_nonbackground / selected_nonbackground,
        "late_actual_model_use_share": late_models / late_semantic if late_semantic else 0.0,
        "late_unnecessary_historical_observation_use": (
            late_unnecessary_historical / late_historical
            if late_historical
            else 0.0
        ),
        "late_historical_observation_selected_count": float(late_historical),
        "late_unnecessary_historical_observation_count": float(
            late_unnecessary_historical
        ),
        "immediate_attribution_coverage": attributed / decisions,
        "delayed_attribution_coverage": outcome_decisions / outcomes if outcomes else 0.0,
        "causal_barrier_p95_seconds": p95,
        "duplicate_refresh_key_processing_ratio": refresh_rows / refresh_keys,
        "optional_queue_growth_slope_after_drain": float(queue_counts[-1] - queue_counts[0]) / max(1, len(queue_counts) - 1),
    }


def _sage_direct_truth_writes(root: Path) -> list[str]:
    findings = []
    for path in (root / "services/reasoning/sage").rglob("*.py"):
        if "tests" in path.parts:
            continue
        source = path.read_text(encoding="utf-8").lower()
        if any(f"{verb} {table}" in source for verb in ("insert into", "update", "delete from") for table in ("model_truth_versions", "model_truth_heads", "relation_truth_versions", "relation_truth_heads")):
            findings.append(str(path.relative_to(root)))
    return findings


__all__ = ["run_p4_online_loop"]
