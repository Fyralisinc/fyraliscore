"""Residual evidence creation for Think compression failures.

Models remain the primary memory. This module only emits small source-backed
residual obligations when a successful Think run visibly failed to compress a
signal into a durable model-layer or product-layer outcome.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping
from uuid import UUID

import asyncpg

from services.reasoning.sage.model_residuals import (
    ModelResidualEvidence,
    ModelResidualEvidenceRepo,
)


MAX_RESIDUAL_SOURCE_OBSERVATIONS = 20
MAX_RESIDUAL_ERROR_COUNT = 10
MAX_RESIDUAL_TEXT_CHARS = 700

_DURABLE_OP_KEYS = (
    "claim_ops",
    "memory_lifecycle_ops",
    "relation_claim_ops",
    "relation_frame_ops",
    "edge_ops",
    "ontology_gap_ops",
    "open_question_ops",
    "formation_resolutions",
    "act_ops",
    "resource_ops",
)
_JUSTIFIED_NOOP_GRADES = {
    "justified_noop_context_used",
    "noop_trace_accounted",
}
_NOISE_NOOP_TRACE_MARKERS = (
    "discard_as_noise",
    "noise-only",
    "noise only",
    "noisy path",
)


@dataclass(frozen=True)
class ThinkResidualContext:
    tenant_id: UUID
    think_run_id: UUID
    trigger_id: UUID
    trigger_kind: str
    source_observation_ids: tuple[UUID, ...] = ()
    trigger_subkind: str | None = None
    validation_dropped_op_count: int = 0
    validation_dropped_op_errors: tuple[str, ...] = ()
    apply_dropped_op_count: int = 0
    apply_dropped_op_errors: tuple[str, ...] = ()
    reasoning_trace: str | None = None
    ops_applied_summary: Mapping[str, Any] = field(default_factory=dict)
    repair_source: str | None = None
    repair_key: str | None = None
    repair_residual_id: UUID | None = None
    repair_residual_kind: str | None = None
    repair_intent: str | None = None
    repair_cascade_depth: int | None = None


@dataclass(frozen=True)
class ResidualAbsorptionTarget:
    object_kind: str
    object_id: UUID


def residuals_for_think_context(
    context: ThinkResidualContext,
) -> list[ModelResidualEvidence]:
    """Return compact residuals implied by a successful Think outcome."""

    source_ids = _bounded_source_ids(context.source_observation_ids)
    if not source_ids:
        return []
    if _is_justified_noop(context):
        return []

    dropped_count = max(
        0,
        int(context.validation_dropped_op_count or 0)
        + int(context.apply_dropped_op_count or 0),
    )
    if dropped_count > 0:
        return [
            _build_validation_drop_residual(context, source_id)
            for source_id in source_ids
        ]

    if _has_durable_outcome(context.ops_applied_summary):
        return []

    return [
        _build_compression_uncertain_residual(context, source_id)
        for source_id in source_ids
    ]


async def persist_think_residuals(
    pool: asyncpg.Pool,
    context: ThinkResidualContext,
) -> int:
    """Ensure residual rows for a Think outcome and return rows attempted.

    Idempotency is enforced by ModelResidualEvidenceRepo. Callers intentionally
    catch failures outside the Think transaction so this optional telemetry
    cannot change the Think run result.
    """

    residuals = residuals_for_think_context(context)
    if not residuals:
        return 0
    repo = ModelResidualEvidenceRepo(tenant_id=context.tenant_id)
    async with pool.acquire() as conn:
        for residual in residuals:
            await repo.insert_open(residual, conn=conn)
    return len(residuals)


async def absorb_think_residuals(
    pool: asyncpg.Pool,
    context: ThinkResidualContext,
) -> int:
    """Absorb open residuals for the context's sources when the run wrote value."""

    source_ids = _bounded_source_ids(context.source_observation_ids)
    if not source_ids:
        return 0
    target = absorption_target_for_applied_summary(context.ops_applied_summary)
    if target is None:
        return 0

    repo = ModelResidualEvidenceRepo(tenant_id=context.tenant_id)
    absorbed = 0
    async with pool.acquire() as conn:
        residuals = await repo.list_for_observations(list(source_ids), conn=conn)
        for residual in residuals:
            if residual.id is None or residual.status != "open":
                continue
            row = await repo.absorb(
                residual.id,
                object_kind=target.object_kind,
                object_id=target.object_id,
                metadata={
                    "source": "think_success_residual_absorber",
                    "think_run_id": str(context.think_run_id),
                    "trigger_id": str(context.trigger_id),
                    "trigger_kind": context.trigger_kind,
                    "trigger_subkind": context.trigger_subkind,
                },
                conn=conn,
            )
            if row is not None:
                absorbed += 1
    return absorbed


def absorption_target_for_applied_summary(
    summary: Mapping[str, Any],
) -> ResidualAbsorptionTarget | None:
    if not _has_durable_outcome(summary):
        return None

    for object_kind, fields in (
        ("model_signal_reading", ("model_signal_reading_id", "reading_id")),
        (
            "relation_instance",
            ("relation_instance_id", "relation_id"),
        ),
        ("relation_claim", ("relation_claim_id",)),
        (
            "model_edge",
            (
                "edge_id",
                "edge_ids",
                "accepted_edge_ids",
                "projected_edge_ids",
                "promoted_edge_ids",
            ),
        ),
        ("model_open_question", ("open_question_id", "question_id")),
        ("projection_snapshot", ("projection_snapshot_id",)),
        ("inquiry_outcome_event", ("inquiry_outcome_event_id",)),
        ("clarification_request", ("clarification_request_id",)),
        ("model", ("model_id", "accepted_model_id")),
    ):
        object_id = _first_uuid_from_summary(summary, fields)
        if object_id is not None:
            return ResidualAbsorptionTarget(
                object_kind=object_kind,
                object_id=object_id,
            )

    model_id = _first_uuid_from_value(summary.get("applied_model_ids"))
    if model_id is not None:
        return ResidualAbsorptionTarget(object_kind="model", object_id=model_id)
    return None


def _bounded_source_ids(source_ids: tuple[UUID, ...]) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for source_id in source_ids:
        if source_id in seen:
            continue
        seen.add(source_id)
        out.append(source_id)
        if len(out) >= MAX_RESIDUAL_SOURCE_OBSERVATIONS:
            break
    return tuple(out)


def _build_validation_drop_residual(
    context: ThinkResidualContext,
    source_observation_id: UUID,
) -> ModelResidualEvidence:
    errors = _drop_errors(context)
    total_dropped = int(context.validation_dropped_op_count or 0) + int(
        context.apply_dropped_op_count or 0
    )
    first_error = errors[0] if errors else "unknown drop"
    compact_summary = _bounded_text(
        "Think dropped "
        f"{total_dropped} operation(s) while compressing this signal; "
        f"first error: {first_error}"
    )
    reason = _bounded_text(
        "think_success_validation_or_apply_drop:"
        f"validation={int(context.validation_dropped_op_count or 0)}:"
        f"apply={int(context.apply_dropped_op_count or 0)}:"
        f"errors={_error_signature(errors)}",
        limit=500,
    )
    return ModelResidualEvidence(
        tenant_id=context.tenant_id,
        source_observation_id=source_observation_id,
        think_run_id=context.think_run_id,
        trigger_id=context.trigger_id,
        residual_kind="validation_dropped_value",
        compact_summary=compact_summary,
        reason=reason,
        metadata={
            "source": "think_success_residual_writer",
            "trigger_kind": context.trigger_kind,
            "trigger_subkind": context.trigger_subkind,
            "validation_dropped_op_count": int(
                context.validation_dropped_op_count or 0
            ),
            "apply_dropped_op_count": int(context.apply_dropped_op_count or 0),
            "dropped_op_errors": errors[:MAX_RESIDUAL_ERROR_COUNT],
            "repair_source": context.repair_source,
            "repair_key": context.repair_key,
            "repair_residual_id": (
                str(context.repair_residual_id)
                if context.repair_residual_id is not None
                else None
            ),
            "repair_residual_kind": context.repair_residual_kind,
            "repair_intent": context.repair_intent,
            "repair_cascade_depth": context.repair_cascade_depth,
        },
    )


def _build_compression_uncertain_residual(
    context: ThinkResidualContext,
    source_observation_id: UUID,
) -> ModelResidualEvidence:
    compact_summary = (
        "Think succeeded but this signal did not produce a durable model, "
        "reading, edge, relation, open question, projection, act, resource, "
        "or justified-noise outcome."
    )
    reason = (
        "think_success_without_durable_fate:"
        f"trigger_kind={context.trigger_kind}:"
        f"trigger_subkind={context.trigger_subkind or 'none'}"
    )
    return ModelResidualEvidence(
        tenant_id=context.tenant_id,
        source_observation_id=source_observation_id,
        think_run_id=context.think_run_id,
        trigger_id=context.trigger_id,
        residual_kind="compression_uncertain",
        compact_summary=_bounded_text(compact_summary),
        reason=_bounded_text(reason, limit=500),
        metadata={
            "source": "think_success_residual_writer",
            "trigger_kind": context.trigger_kind,
            "trigger_subkind": context.trigger_subkind,
            "context_use_grade": _context_use_grade(context.ops_applied_summary),
            "reasoning_trace": _bounded_text(context.reasoning_trace or "", limit=500),
        },
    )


def _drop_errors(context: ThinkResidualContext) -> list[str]:
    errors = [
        str(error)
        for error in (
            list(context.validation_dropped_op_errors or ())
            + list(context.apply_dropped_op_errors or ())
        )
        if str(error).strip()
    ]
    return errors[:MAX_RESIDUAL_ERROR_COUNT]


def _error_signature(errors: list[str]) -> str:
    if not errors:
        return "none"
    signature = "|".join(_bounded_text(error, limit=120) for error in errors[:3])
    return signature or "none"


def _is_justified_noop(context: ThinkResidualContext) -> bool:
    summary = context.ops_applied_summary
    if _has_durable_outcome(summary):
        return False
    grade = _context_use_grade(summary)
    if grade in _JUSTIFIED_NOOP_GRADES:
        return True
    trace = str(context.reasoning_trace or summary.get("reasoning_trace") or "").lower()
    return any(marker in trace for marker in _NOISE_NOOP_TRACE_MARKERS)


def _context_use_grade(summary: Mapping[str, Any]) -> str | None:
    context_use = summary.get("context_use")
    if not isinstance(context_use, Mapping):
        return None
    grade = context_use.get("context_use_grade")
    return str(grade) if grade is not None else None


def _has_durable_outcome(summary: Mapping[str, Any]) -> bool:
    if _positive_int(summary.get("state_changes_emitted")):
        return True
    if _non_empty_sequence(summary.get("applied_model_ids")):
        return True
    if _positive_int(summary.get("negative_memory_inserts")):
        return True
    if _non_empty_sequence(summary.get("negative_memory_ops")):
        return True
    if _non_empty_sequence(summary.get("representation_repair_triggers")):
        return True
    if _non_empty_sequence(summary.get("relationship_candidate_adjudications")):
        return True
    if isinstance(summary.get("relationship_candidate_adjudication"), Mapping):
        return True
    return any(_has_durable_items(summary.get(key)) for key in _DURABLE_OP_KEYS)


def _has_durable_items(value: Any) -> bool:
    if isinstance(value, Mapping):
        return _item_is_durable(value)
    if isinstance(value, (list, tuple, set)):
        return any(_item_is_durable(item) for item in value)
    return bool(value)


def _item_is_durable(item: Any) -> bool:
    if item is None:
        return False
    if not isinstance(item, Mapping):
        return True
    if item.get("error") or item.get("status") == "dropped":
        return False
    op = str(item.get("op") or "").lower()
    if op in {"skip", "noop", "no_op"}:
        return False
    decision = str(item.get("decision") or "").lower()
    if decision.startswith("skipped"):
        return False
    return bool(item)


def _positive_int(value: Any) -> bool:
    try:
        return int(value or 0) > 0
    except (TypeError, ValueError):
        return False


def _non_empty_sequence(value: Any) -> bool:
    return isinstance(value, (list, tuple, set)) and bool(value)


def _first_uuid_from_summary(
    summary: Mapping[str, Any],
    fields: tuple[str, ...],
) -> UUID | None:
    direct = _first_uuid_from_mapping(summary, fields)
    if direct is not None:
        return direct
    for item in _iter_summary_items(summary):
        if not isinstance(item, Mapping):
            continue
        found = _first_uuid_from_mapping(item, fields)
        if found is not None:
            return found
    return None


def _iter_summary_items(summary: Mapping[str, Any]) -> list[Any]:
    items: list[Any] = []
    for key in (
        *_DURABLE_OP_KEYS,
        "negative_memory_ops",
        "relationship_candidate_adjudications",
    ):
        value = summary.get(key)
        if isinstance(value, (list, tuple, set)):
            items.extend(value)
        elif isinstance(value, Mapping):
            items.append(value)
    relationship_adjudication = summary.get("relationship_candidate_adjudication")
    if isinstance(relationship_adjudication, Mapping):
        items.append(relationship_adjudication)
    return items


def _first_uuid_from_mapping(
    item: Mapping[str, Any],
    fields: tuple[str, ...],
) -> UUID | None:
    for field_name in fields:
        found = _first_uuid_from_value(item.get(field_name))
        if found is not None:
            return found
    return None


def _first_uuid_from_value(value: Any) -> UUID | None:
    if isinstance(value, UUID):
        return value
    if isinstance(value, str):
        try:
            return UUID(value)
        except ValueError:
            return None
    if isinstance(value, (list, tuple, set)):
        for item in value:
            found = _first_uuid_from_value(item)
            if found is not None:
                return found
    return None


def _bounded_text(value: str, *, limit: int = MAX_RESIDUAL_TEXT_CHARS) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


__all__ = [
    "MAX_RESIDUAL_SOURCE_OBSERVATIONS",
    "ResidualAbsorptionTarget",
    "ThinkResidualContext",
    "absorb_think_residuals",
    "absorption_target_for_applied_summary",
    "persist_think_residuals",
    "residuals_for_think_context",
]
