"""Coherence-repair scheduling from measured residual debt.

This module does not repair models directly. It turns compact residual evidence
into bounded T4 representation-repair triggers so the existing Think repair lane
can retrieve context, preserve provenance, and apply the usual validator/applier
contracts.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from services.domain.triggers import enqueue_trigger
from services.reasoning.sage.model_residuals import (
    ModelResidualEvidence,
    ModelResidualEvidenceRepo,
)
from services.reasoning.think.residuals import (
    ThinkResidualContext,
    absorption_target_for_applied_summary,
)


_REPAIR_INTENTS_BY_RESIDUAL_KIND = {
    "valuable_unmodeled": "absorb_unmodeled_value",
    "counterevidence_unattached": "attach_counterevidence",
    "relation_unanchored": "anchor_relation_evidence",
    "open_question_needed": "create_or_answer_open_question",
    "validation_dropped_value": "repair_validation_dropped_value",
    "authority_blocked": "route_authority_blocked_evidence",
    "compression_uncertain": "retry_model_absorption",
}

_SUCCESS_METRICS_BY_RESIDUAL_KIND = {
    "valuable_unmodeled": "residual becomes absorbed into a model or reading",
    "counterevidence_unattached": "counterevidence attaches as contest/falsify",
    "relation_unanchored": "relation evidence becomes a claim/frame or is rejected",
    "open_question_needed": "open question is created, answered, or rejected",
    "validation_dropped_value": "valid dropped value is represented or rejected",
    "authority_blocked": "authority-safe route or human clarification is created",
    "compression_uncertain": "source signal receives a durable fate",
}


@dataclass(frozen=True)
class ResidualRepairTrigger:
    id: UUID
    repair_key: str
    residual_id: UUID
    residual_kind: str
    deduped: bool


@dataclass(frozen=True)
class ResidualRepairResolution:
    residual_id: UUID
    status: str
    reason: str
    terminal: bool
    resolved: bool


def residual_repair_max_triggers(default: int = 3) -> int:
    raw = os.environ.get("THINK_RESIDUAL_REPAIR_MAX_TRIGGERS")
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


def residual_repair_max_cascade_depth(default: int = 3) -> int:
    raw = os.environ.get("THINK_RESIDUAL_REPAIR_MAX_CASCADE_DEPTH")
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(0, int(raw))
    except ValueError:
        return default


async def resolve_residual_repair_outcome(
    pool: asyncpg.Pool,
    context: ThinkResidualContext,
) -> ResidualRepairResolution | None:
    """Resolve the parent residual consumed by a T4 repair run.

    A repair should either absorb the residual, reject it as terminally invalid,
    expire it after a bounded retry chain, or leave it open. It should not turn a
    correctly rejected repair artifact into fresh repair fuel.
    """

    if (
        context.trigger_kind != "T4:representation_repair"
        or context.trigger_subkind != "representation_repair"
        or context.repair_residual_id is None
    ):
        return None

    residual_id = context.repair_residual_id
    repo = ModelResidualEvidenceRepo(pool, tenant_id=context.tenant_id)

    terminal_reason = _terminal_invalid_repair_reason(context)
    if terminal_reason is not None:
        row = await repo.reject(
            residual_id,
            reason=terminal_reason,
        )
        return ResidualRepairResolution(
            residual_id=residual_id,
            status="rejected",
            reason=terminal_reason,
            terminal=True,
            resolved=row is not None,
        )

    target = absorption_target_for_applied_summary(context.ops_applied_summary)
    if target is not None:
        row = await repo.absorb(
            residual_id,
            object_kind=target.object_kind,
            object_id=target.object_id,
            metadata=_resolution_metadata(
                context,
                resolution_reason="repair_absorbed_parent_residual",
            ),
        )
        return ResidualRepairResolution(
            residual_id=residual_id,
            status="absorbed",
            reason="repair_absorbed_parent_residual",
            terminal=True,
            resolved=row is not None,
        )

    max_depth = residual_repair_max_cascade_depth()
    depth = int(context.repair_cascade_depth or 0)
    if depth >= max_depth:
        reason = f"repair_cascade_budget_exhausted:{depth}>={max_depth}"
        row = await repo.expire(
            residual_id,
            reason=reason,
        )
        return ResidualRepairResolution(
            residual_id=residual_id,
            status="expired",
            reason=reason,
            terminal=True,
            resolved=row is not None,
        )

    return None


def repair_payload_for_residual(
    residual: ModelResidualEvidence,
    *,
    cascade_depth: int = 0,
) -> dict[str, Any] | None:
    if residual.id is None or residual.status != "open":
        return None
    residual_kind = str(residual.residual_kind)
    repair_intent = _REPAIR_INTENTS_BY_RESIDUAL_KIND.get(
        residual_kind,
        "inspect_residual_debt",
    )
    repair_key = f"residual:{residual.id}"
    payload: dict[str, Any] = {
        "repair_key": repair_key,
        "repair_intent": repair_intent,
        "repair_source": "model_residual_evidence",
        "residual_id": str(residual.id),
        "residual_kind": residual_kind,
        "residual_status": residual.status,
        "residual_compact_summary": _bounded_text(residual.compact_summary, 700),
        "residual_reason": _bounded_text(residual.reason, 700),
        "success_metric": _SUCCESS_METRICS_BY_RESIDUAL_KIND.get(
            residual_kind,
            "open residual count decreases",
        ),
        "seed_natural_text": _repair_seed_text(residual, repair_intent),
    }
    if residual.source_observation_id is not None:
        payload["source_observation_id"] = str(residual.source_observation_id)
        payload["observation_ids"] = [str(residual.source_observation_id)]
    if residual.model_id is not None:
        payload["model_id"] = str(residual.model_id)
        payload["model_ids"] = [str(residual.model_id)]
    if residual.think_run_id is not None:
        payload["source_think_run_id"] = str(residual.think_run_id)
    if residual.trigger_id is not None:
        payload["source_trigger_id"] = str(residual.trigger_id)
    if cascade_depth > 0:
        payload["cascade_depth"] = cascade_depth
    return payload


async def enqueue_residual_repair_triggers_for_sources(
    pool: asyncpg.Pool,
    *,
    tenant_id: UUID,
    source_observation_ids: tuple[UUID, ...],
    cascade_depth: int = 0,
    max_triggers: int | None = None,
) -> list[ResidualRepairTrigger]:
    source_ids = _dedupe_source_ids(source_observation_ids)
    if not source_ids:
        return []
    limit = residual_repair_max_triggers() if max_triggers is None else max_triggers
    if limit <= 0:
        return []
    repo = ModelResidualEvidenceRepo(tenant_id=tenant_id)
    async with pool.acquire() as conn:
        residuals = await repo.list_for_observations(list(source_ids), conn=conn)
        return await enqueue_residual_repair_triggers(
            conn,
            tenant_id=tenant_id,
            residuals=residuals,
            cascade_depth=cascade_depth,
            max_triggers=limit,
        )


async def enqueue_residual_repair_triggers(
    conn: asyncpg.Connection,
    *,
    tenant_id: UUID,
    residuals: list[ModelResidualEvidence],
    cascade_depth: int = 0,
    max_triggers: int | None = None,
) -> list[ResidualRepairTrigger]:
    limit = residual_repair_max_triggers() if max_triggers is None else max_triggers
    if limit <= 0:
        return []

    queued: list[ResidualRepairTrigger] = []
    seen_keys: set[str] = set()
    for residual in residuals:
        if len(queued) >= limit:
            break
        if residual.tenant_id != tenant_id:
            continue
        payload = repair_payload_for_residual(residual, cascade_depth=cascade_depth)
        if payload is None or residual.id is None:
            continue
        repair_key = str(payload["repair_key"])
        if repair_key in seen_keys:
            continue
        seen_keys.add(repair_key)
        existing_id = await _find_existing_repair_trigger(conn, tenant_id, repair_key)
        if existing_id is not None:
            queued.append(
                ResidualRepairTrigger(
                    id=existing_id,
                    repair_key=repair_key,
                    residual_id=residual.id,
                    residual_kind=residual.residual_kind,
                    deduped=True,
                )
            )
            continue
        trigger_id = await enqueue_trigger(
            conn,
            tenant_id=tenant_id,
            trigger_kind="T4",
            trigger_subkind="representation_repair",
            observation_id=residual.source_observation_id,
            model_id=residual.model_id,
            payload=payload,
        )
        queued.append(
            ResidualRepairTrigger(
                id=trigger_id,
                repair_key=repair_key,
                residual_id=residual.id,
                residual_kind=residual.residual_kind,
                deduped=False,
            )
        )
    return queued


async def _find_existing_repair_trigger(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    repair_key: str,
) -> UUID | None:
    return await conn.fetchval(
        """
        SELECT id
        FROM think_trigger_queue
        WHERE tenant_id = $1
          AND trigger_kind = 'T4'
          AND trigger_subkind = 'representation_repair'
          AND payload->>'repair_key' = $2
        ORDER BY enqueued_at ASC
        LIMIT 1
        """,
        tenant_id,
        repair_key,
    )


def _repair_seed_text(
    residual: ModelResidualEvidence,
    repair_intent: str,
) -> str:
    parts = [
        f"Coherence repair needed: {repair_intent}.",
        str(residual.compact_summary or "").strip(),
    ]
    reason = str(residual.reason or "").strip()
    if reason:
        parts.append(f"Reason: {reason}")
    return _bounded_text(" ".join(part for part in parts if part), 900)


def _terminal_invalid_repair_reason(context: ThinkResidualContext) -> str | None:
    if context.repair_intent != "repair_validation_dropped_value":
        return None
    text = " ".join(
        [
            *(str(error) for error in context.validation_dropped_op_errors),
            *(str(error) for error in context.apply_dropped_op_errors),
            str(context.reasoning_trace or ""),
        ]
    ).lower()
    if (
        "self-edge" in text
        or "self edge" in text
        or "self_edge" in text
        or "same source/target" in text
    ):
        return "terminal_invalid_self_edge"
    return None


def _resolution_metadata(
    context: ThinkResidualContext,
    *,
    resolution_reason: str,
) -> dict[str, Any]:
    return {
        "source": "residual_repair_resolution",
        "resolution_reason": resolution_reason,
        "think_run_id": str(context.think_run_id),
        "trigger_id": str(context.trigger_id),
        "trigger_kind": context.trigger_kind,
        "trigger_subkind": context.trigger_subkind,
        "repair_key": context.repair_key,
        "repair_intent": context.repair_intent,
        "repair_cascade_depth": context.repair_cascade_depth,
    }


def _dedupe_source_ids(source_ids: tuple[UUID, ...]) -> tuple[UUID, ...]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for source_id in source_ids:
        if source_id in seen:
            continue
        seen.add(source_id)
        out.append(source_id)
    return tuple(out)


def _bounded_text(value: str, limit: int) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 3)].rstrip() + "..."


__all__ = [
    "ResidualRepairTrigger",
    "enqueue_residual_repair_triggers",
    "enqueue_residual_repair_triggers_for_sources",
    "repair_payload_for_residual",
    "residual_repair_max_triggers",
]
