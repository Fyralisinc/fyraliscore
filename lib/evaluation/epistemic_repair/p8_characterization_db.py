"""PostgreSQL-backed retrieval, feedback, queue, and projection characterization."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

from lib.evaluation.epistemic_repair.p2_runner import _admission
from lib.evaluation.epistemic_repair.p8_characterization_population import (
    build_feedback_population,
    build_retrieval_population,
)
from lib.evaluation.epistemic_repair.p8_characterization_runner import _metric
from lib.evaluation.epistemic_repair.p8_measurement_contracts import QUEUE_FAMILIES, queue_curve_is_usable
from services.domain.company_learning.barrier import (
    CompanyLearningBarrierService,
    ContextDecision,
    HistoricalReopenReason,
    OutcomeLink,
)
from services.domain.projections.store import complete_projection_refresh_job, enqueue_projection_refresh_job
from services.domain.truth_kernel import build_default_truth_kernel


_NOW = datetime(2026, 7, 18, tzinfo=timezone.utc)


async def _sample_queues(conn, tenant_id, samples):
    for item in QUEUE_FAMILIES:
        where = item.pending_predicate
        tenant = f" AND {item.tenant_column}=$1" if item.tenant_column else ""
        exists = await conn.fetchval("SELECT to_regclass($1)::text", item.table)
        if exists is not None:
            value = await conn.fetchval(f"SELECT count(*)::int FROM {item.table} WHERE ({where}){tenant}", *([tenant_id] if item.tenant_column else []))
        else:
            value = -1  # Explicit missing-family sentinel; never a green zero.
        samples[item.family].append(int(value))


async def run_db_characterization(conn) -> dict[str, object]:
    tenant_id = uuid4()
    await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,'p8-characterization')", tenant_id)
    truth = build_default_truth_kernel()
    admitted = await truth.admit(tx=conn, command=_admission(tenant_id, 8801))
    barrier = CompanyLearningBarrierService()
    retrieval_pop = build_retrieval_population()
    retrieval_outcomes, retrieval_slices = [], {}
    feedback_outcomes, feedback_slices = [], {}
    queues = {item.family: [] for item in QUEUE_FAMILIES}
    projection_attempts = projection_rows = projection_processed = 0

    for index, case in enumerate(retrieval_pop.cases):
        labels = set(case.evaluator_labels)
        rows = await conn.fetch(
            "SELECT truth_version_id FROM accepted_current_models WHERE tenant_id=$1",
            tenant_id,
        )
        has_model = any(row["truth_version_id"] == admitted.version_id for row in rows)
        if "multi_hop_relation" in labels:
            kind, referenced, ok = "accepted_relation", False, False
        elif "sparse_no_match_raw_reopen" in labels:
            kind, referenced, ok = "historical_observation", True, True
        elif "noise_noop" in labels:
            kind, referenced, ok = "current_episode", False, True
        else:
            kind, referenced, ok = "accepted_model", has_model, has_model
        decision = ContextDecision(
            decision_id=uuid4(), tenant_id=tenant_id, batch_id=f"retrieval-{index // 50}",
            route_id=f"p8-characterization:{case.maturity}", context_item_kind=kind,
            context_item_id=str(admitted.model_id), context_item_version="1",
            retrieved=True, selected=True, included=True, referenced=referenced,
            counterevidence_retained="contradiction_lifecycle" in labels,
            confidence_affecting=referenced, necessary_background=False,
            historical_reopen_reason=(HistoricalReopenReason.SPARSE_COVERAGE if kind == "historical_observation" else None),
            decision_fate="mutation" if referenced else "justified_noop",
            result_object_kind="model_version" if referenced and kind == "accepted_model" else None,
            result_object_id=admitted.version_id if referenced and kind == "accepted_model" else None,
            evidence_lineage=({"kind": kind, "case_id": case.case_id},),
            decided_at=_NOW + timedelta(seconds=index),
        )
        await barrier.record_context_decision(tx=conn, item=decision)
        retrieval_outcomes.append((case.case_id, ok))
        for label in case.evaluator_labels:
            retrieval_slices.setdefault(label, []).append((case.case_id, ok))
        if (index + 1) % 50 == 0:
            await _sample_queues(conn, tenant_id, queues)
            projection_attempts += 2
            subject = f"retrieval-chunk:{index // 50}"
            first = await enqueue_projection_refresh_job(
                conn, tenant_id=tenant_id, projection_name="p8-characterization",
                subject_key=subject, reason="barrier_complete",
            )
            second = await enqueue_projection_refresh_job(
                conn, tenant_id=tenant_id, projection_name="p8-characterization",
                subject_key=subject, reason="barrier_complete",
            )
            if first != second:
                raise AssertionError("projection enqueue did not coalesce by subject/version")
            projection_rows += 1
            await complete_projection_refresh_job(conn, tenant_id=tenant_id, job_id=first)
            projection_processed += 1

    feedback_pop = build_feedback_population()
    for policy in ("models_first", "models_plus_raw_control"):
        for index, case in enumerate(feedback_pop.cases):
            outcome_label = case.evaluator_labels[0]
            decision_id = uuid4()
            decision = ContextDecision(
                decision_id=decision_id, tenant_id=tenant_id, batch_id=f"feedback-{index // 30}",
                route_id=policy, context_item_kind="accepted_model",
                context_item_id=str(admitted.model_id), context_item_version="1",
                retrieved=True, selected=True, included=True, referenced=True,
                counterevidence_retained=outcome_label in {"revised", "falsified"},
                confidence_affecting=True, necessary_background=False,
                historical_reopen_reason=None,
                decision_fate="justified_noop" if outcome_label == "justified_noop" else "mutation",
                result_object_kind="model_version", result_object_id=admitted.version_id,
                evidence_lineage=({"kind": "accepted_model", "case_id": case.case_id, "policy": policy},),
                decided_at=_NOW + timedelta(hours=1, seconds=index),
            )
            await barrier.record_context_decision(tx=conn, item=decision)
            observable = outcome_label != "no_observable_outcome_control"
            if observable:
                outcome_kind = {
                    "later_confirmed": "confirmation", "revised": "revision",
                    "falsified": "falsification", "justified_noop": "confirmation",
                    "entity_human_correction": "human_adjudication",
                }[outcome_label]
                await barrier.record_outcome(tx=conn, item=OutcomeLink(
                    outcome_link_id=uuid4(), tenant_id=tenant_id, decision_id=decision_id,
                    outcome_kind=outcome_kind, outcome_object_kind="model_version",
                    outcome_object_id=admitted.version_id, attribution_basis="direct",
                    evidence_lineage=decision.evidence_lineage,
                    observed_at=_NOW + timedelta(days=1, seconds=index),
                ))
            linked = await conn.fetchval(
                "SELECT count(*)::int FROM company_learning_outcome_links WHERE tenant_id=$1 AND decision_id=$2",
                tenant_id, decision_id,
            )
            ok = (linked == 1) if observable else (linked == 0)
            key = f"{case.case_id}:{policy}"
            feedback_outcomes.append((key, ok))
            feedback_slices.setdefault(outcome_label, []).append((key, ok))
            feedback_slices.setdefault(policy, []).append((key, ok))
        await _sample_queues(conn, tenant_id, queues)

    refresh = await conn.fetchrow(
        """SELECT count(*)::int AS total,
                  count(*) FILTER (WHERE status='processed')::int AS processed,
                  count(*) FILTER (WHERE status='dead_letter')::int AS dead,
                  count(DISTINCT (projection_name,projection_version,subject_key))::int AS unique_keys
           FROM projection_refresh_jobs WHERE tenant_id=$1 AND projection_name='p8-characterization'""",
        tenant_id,
    )
    return {
        "retrieval": _metric("claim_local_retrieval", retrieval_outcomes, slices=retrieval_slices),
        "feedback": _metric("paired_feedback_attribution", feedback_outcomes, slices=feedback_slices),
        "queue_samples": queues,
        "queue_measurement_complete": queue_curve_is_usable(queues),
        "projection_refresh": {
            "enqueue_attempts": projection_attempts, "enqueued_jobs": refresh["total"],
            "processed_jobs": refresh["processed"], "dead_letter_jobs": refresh["dead"],
            "unique_subject_family_versions": refresh["unique_keys"],
            "coalescing_ratio": refresh["total"] / refresh["unique_keys"],
            "locally_counted_rows": projection_rows,
            "locally_counted_processed": projection_processed,
        },
    }
