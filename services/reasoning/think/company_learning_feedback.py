"""Translate validated Think context-use into decision-level learning credit."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import NAMESPACE_URL, UUID, uuid5

from services.domain.company_learning import (
    CompanyLearningBarrierService,
    ContextDecision,
    HistoricalReopenReason,
)


_REOPEN_ALIASES = {
    "cold_start": HistoricalReopenReason.COLD_START,
    "sparse_coverage": HistoricalReopenReason.SPARSE_COVERAGE,
    "contradiction": HistoricalReopenReason.CONTRADICTION,
    "provenance": HistoricalReopenReason.PROVENANCE,
    "novelty": HistoricalReopenReason.NOVELTY,
    "correction": HistoricalReopenReason.CORRECTION,
    "unresolved_question": HistoricalReopenReason.UNRESOLVED_QUESTION,
}


async def record_company_learning_context_credit(
    conn: Any,
    *,
    tenant_id: UUID,
    run_id: UUID,
    batch_id: str,
    route_id: str,
    context_use: Mapping[str, Any],
    applied: Mapping[str, Any],
    decided_at: datetime | None = None,
) -> tuple[UUID, ...]:
    """Persist one exact fate for every selected Model/Observation context item."""

    at = decided_at or datetime.now(timezone.utc)
    result_kind, result_id = _result(applied)
    op_count = _op_count(applied)
    service = CompanyLearningBarrierService()
    rows: list[ContextDecision] = []

    model_selected = _ids(context_use.get("selected_model_ids")) | _ids(
        context_use.get("graph_selected_model_ids")
    )
    model_referenced = _ids(context_use.get("referenced_model_ids"))
    for item_id in sorted(model_selected):
        rows.append(
            _decision(
                tenant_id=tenant_id, run_id=run_id, batch_id=batch_id,
                route_id=route_id, kind="accepted_model", item_id=item_id,
                referenced=item_id in model_referenced, op_count=op_count,
                result_kind=result_kind, result_id=result_id, at=at,
            )
        )

    observation_selected = _ids(context_use.get("selected_observation_ids"))
    observation_referenced = _ids(context_use.get("referenced_observation_ids"))
    historical_count = int(
        context_use.get("selected_historical_observation_count") or 0
    )
    reopen = _reopen_reason(context_use)
    ordered_observations = sorted(observation_selected)
    historical_ids = set(ordered_observations[-historical_count:]) if historical_count else set()
    for item_id in ordered_observations:
        historical = item_id in historical_ids
        rows.append(
            _decision(
                tenant_id=tenant_id, run_id=run_id, batch_id=batch_id,
                route_id=route_id,
                kind="historical_observation" if historical else "current_episode",
                item_id=item_id, referenced=item_id in observation_referenced,
                op_count=op_count, result_kind=result_kind, result_id=result_id,
                at=at, reopen_reason=reopen if historical else None,
            )
        )
    for row in rows:
        await service.record_context_decision(tx=conn, item=row)
    return tuple(row.decision_id for row in rows)


async def record_uncertainty_dispositions(
    conn: Any,
    *,
    tenant_id: UUID,
    run_id: UUID,
    batch_id: str,
    route_id: str,
    uncertainty_signals: list[dict[str, Any]],
    decided_at: datetime | None = None,
) -> tuple[UUID, ...]:
    """Persist non-truth terminal fates for deterministic uncertainty signals."""

    at = decided_at or datetime.now(timezone.utc)
    service = CompanyLearningBarrierService()
    rows: list[ContextDecision] = []
    for signal in uncertainty_signals:
        observation_id = str(signal.get("observation_id") or "").strip()
        uncertainty_id = str(signal.get("uncertainty_id") or "").strip()
        routing = str(signal.get("routing") or "").strip()
        if not observation_id or not uncertainty_id or routing not in {
            "open_question",
            "clarification_residual",
        }:
            continue
        decision_id = uuid5(
            NAMESPACE_URL,
            (
                "fyralis:uncertainty-disposition:"
                f"{tenant_id}:{run_id}:{observation_id}:{uncertainty_id}"
            ),
        )
        rows.append(
            ContextDecision(
                decision_id=decision_id,
                tenant_id=tenant_id,
                batch_id=batch_id,
                route_id=route_id,
                context_item_kind="candidate",
                context_item_id=observation_id,
                context_item_version=uncertainty_id,
                retrieved=False,
                selected=False,
                included=False,
                referenced=False,
                counterevidence_retained=False,
                confidence_affecting=False,
                necessary_background=False,
                historical_reopen_reason=None,
                decision_fate="justified_noop",
                result_object_kind=routing,
                result_object_id=None,
                evidence_lineage=(
                    {
                        "kind": "uncertainty_signal",
                        "observation_id": observation_id,
                        "uncertainty_id": uncertainty_id,
                        "uncertainty_kind": signal.get("kind"),
                        "routing": routing,
                        "reason": "nonassertable_signal_retained_outside_truth",
                    },
                ),
                decided_at=at,
            )
        )
    for row in rows:
        await service.record_context_decision(tx=conn, item=row)
    return tuple(row.decision_id for row in rows)


def _decision(
    *, tenant_id: UUID, run_id: UUID, batch_id: str, route_id: str,
    kind: str, item_id: str, referenced: bool, op_count: int,
    result_kind: str | None, result_id: UUID | None, at: datetime,
    reopen_reason: HistoricalReopenReason | None = None,
) -> ContextDecision:
    decision_id = uuid5(
        NAMESPACE_URL,
        f"fyralis:context-credit:{tenant_id}:{run_id}:{kind}:{item_id}",
    )
    fate = "mutation" if referenced and op_count else (
        "justified_noop" if referenced else "unused"
    )
    return ContextDecision(
        decision_id=decision_id, tenant_id=tenant_id, batch_id=batch_id,
        route_id=route_id, context_item_kind=kind, context_item_id=item_id,
        context_item_version="selected-at-run-v1", retrieved=True, selected=True,
        included=True, referenced=referenced,
        counterevidence_retained=False, confidence_affecting=referenced,
        necessary_background=False, historical_reopen_reason=reopen_reason,
        decision_fate=fate, result_object_kind=result_kind,
        result_object_id=result_id,
        evidence_lineage=({"kind": kind, "id": item_id, "run_id": str(run_id)},),
        decided_at=at,
    )


def _ids(value: Any) -> set[str]:
    if not isinstance(value, (list, tuple, set)):
        return set()
    return {str(item) for item in value if str(item).strip()}


def _reopen_reason(context_use: Mapping[str, Any]) -> HistoricalReopenReason:
    reasons = context_use.get("raw_observation_reopening_reasons") or []
    for reason in reasons:
        normalized = str(reason).strip().lower().replace("-", "_")
        if normalized in _REOPEN_ALIASES:
            return _REOPEN_ALIASES[normalized]
    # A selected historical item without a recognized reason is not silently
    # untyped. The existing selection itself establishes sparse coverage.
    return HistoricalReopenReason.SPARSE_COVERAGE


def _op_count(applied: Mapping[str, Any]) -> int:
    return sum(
        len(value)
        for key, value in applied.items()
        if key.endswith("_ops") and isinstance(value, list)
    )


def _result(applied: Mapping[str, Any]) -> tuple[str | None, UUID | None]:
    for kind, key in (
        ("model", "applied_model_ids"),
        ("relation", "applied_relation_ids"),
    ):
        values = applied.get(key)
        if isinstance(values, list) and values:
            try:
                return kind, UUID(str(values[0]))
            except ValueError:
                pass
    return None, None


__all__ = [
    "record_company_learning_context_credit",
    "record_uncertainty_dispositions",
]
