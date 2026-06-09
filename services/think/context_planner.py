"""Context planning for Think.

This module owns the pre-reasoning context path: active retrieval, optional
second-pass expansion, reasoning-frame construction, prompt-facing assembly,
active-work augmentation, actor context, and dynamic-signal notes. It is
intentionally mutation-light; validation/apply/cascade stay in
``services.think.reason``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMProvider
from services.actors.operating_context import (
    load_actor_operating_context,
    summarize_actor_operating_context,
)
from services.dynamics import (
    detect_dynamic_signals,
    emit_missing_transition_triggers,
)
from services.execution.inquiry import InquiryResult, retrieve_for_execution
from services.execution.question_planning_provider import (
    select_question_planning_provider,
)
from services.retrieval.assembler import (
    AccessContext,
    ContextBundle,
    assemble_context,
)
from services.retrieval.config import CONFIG as RETRIEVAL_CONFIG
from services.retrieval.primary import RetrievalResult, TriggerContext
from services.retrieval.second_pass import (
    log_second_pass_decision,
    second_pass_expand,
    should_run_second_pass,
)

from .deterministic import is_authoritative
from .debug_capture import capture as debug_capture
from .reasoning_frame import ReasoningFrame
from .region_locks import touched_entity_ids


_log = structlog.get_logger(__name__)
_diag_log = structlog.get_logger("think.diag")


@dataclass(frozen=True, slots=True)
class ContextPlan:
    """The context Think should reason over for one trigger attempt."""

    retrieval_result: RetrievalResult
    inquiry_result: InquiryResult | None
    reasoning_frame: ReasoningFrame


@dataclass(frozen=True, slots=True)
class ReasoningContext:
    """The assembled, model-facing context and its mutation region."""

    bundle: ContextBundle
    allowed_region: list[tuple[str, str]]
    actor_operating_summary: str | None = None


def retrieval_question_planning_provider(
    llm_provider: LLMProvider | None,
) -> LLMProvider | None:
    """Return the provider allowed for inquiry question planning.

    Production provider objects from ``lib.llm.provider`` are allowed by
    default, then remapped by the inquiry provider selector when the app-wide
    provider is Codex. Custom test/double providers opt in explicitly because
    inquiry question planning can otherwise make unit tests unexpectedly call
    custom structured-output methods.
    """

    if llm_provider is None:
        return None
    allow_custom = os.environ.get(
        "INQUIRY_ALLOW_CUSTOM_LLM_QUESTION_PROVIDER",
        "",
    ).strip().lower()
    if allow_custom in {"1", "true", "yes", "on"}:
        return llm_provider
    if isinstance(llm_provider, LLMProvider):
        return select_question_planning_provider(llm_provider)
    if llm_provider.__class__.__module__ == "lib.llm.provider":
        return llm_provider
    return None


async def plan_context(
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    embedder: Any | None = None,
    llm_provider: LLMProvider | None = None,
) -> ContextPlan:
    """Build the retrieval/context plan used by Think reasoning.

    The returned ``retrieval_result`` may include second-pass expansion and
    dynamic-signal metadata. When the active retrieval path used inquiry
    execution, ``inquiry_result`` retains its persisted session and trace
    metadata so the later validator/applier can emit SAGE outcome events.
    """

    active_retrieval = await retrieve_for_execution(
        trigger,
        conn,
        embedder=embedder,
        llm_provider=retrieval_question_planning_provider(llm_provider),
        mode="deep",
    )
    inquiry_result = (
        active_retrieval
        if isinstance(active_retrieval, InquiryResult)
        else None
    )
    retrieval_result = (
        active_retrieval.retrieval_result
        if isinstance(active_retrieval, InquiryResult)
        else active_retrieval
    )
    retrieval_result = await _maybe_expand_second_pass(
        retrieval_result,
        trigger,
        conn,
    )
    reasoning_frame = ReasoningFrame.from_trigger(
        trigger,
        retrieval_result=retrieval_result,
    )
    reasoning_frame = await _attach_dynamic_signals(
        reasoning_frame,
        retrieval_result,
        trigger,
        conn,
    )
    retrieval_result.notes["reasoning_frame"] = reasoning_frame.to_dict()
    return ContextPlan(
        retrieval_result=retrieval_result,
        inquiry_result=inquiry_result,
        reasoning_frame=reasoning_frame,
    )


async def assemble_reasoning_context(
    context_plan: ContextPlan,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
    *,
    access_context: AccessContext | None = None,
    expanded_region: set[tuple[str, str]] | None = None,
    run_id: UUID | None = None,
) -> ReasoningContext:
    """Assemble the prompt-facing bundle and final allowed region.

    Retrieval answers "what is relevant?" This step answers "what
    should the model actually see, and what entities may the resulting
    diff touch?" Keeping both in the planner makes the Think kernel
    smaller and keeps region expansion colocated with the context
    augmentation that caused it.
    """

    retrieval_result = context_plan.retrieval_result
    allowed_region = touched_entity_ids(retrieval_result)
    if expanded_region:
        allowed_region = sorted(set(allowed_region) | set(expanded_region))

    access = access_context or AccessContext(tenant_id=trigger.tenant_id)
    bundle = await assemble_context(retrieval_result, access, conn)
    _diag_log.warning(
        "augmentation.entry",
        run_id=str(run_id) if run_id is not None else None,
        bundle_commitments=len(bundle.acts_summary.get("commitments", [])),
    )

    try:
        allowed_region = await _augment_active_acts(
            conn,
            trigger,
            bundle,
            allowed_region=allowed_region,
        )
    except Exception as exc:  # noqa: BLE001
        _raise_if_postgres_error(exc)
        if run_id is not None:
            await debug_capture(
                conn,
                run_id=run_id,
                tenant_id=trigger.tenant_id,
                stage="error",
                payload={
                    "phase": "acts_augmentation",
                    "error": repr(exc),
                },
            )

    if run_id is not None:
        await debug_capture(
            conn,
            run_id=run_id,
            tenant_id=trigger.tenant_id,
            stage="retrieval",
            payload={
                "phase": "post_augmentation",
                "commitment_count": len(
                    bundle.acts_summary.get("commitments", [])
                ),
                "commitment_titles": [
                    getattr(commitment, "title", None)
                    for commitment in bundle.acts_summary.get(
                        "commitments", []
                    )
                ][:80],
            },
        )

    actor_operating_summary: str | None = None
    try:
        actor_contexts = await load_actor_operating_context(
            conn,
            tenant_id=trigger.tenant_id,
            actor_ids=_actor_ids_for_operating_context(trigger, bundle),
        )
        actor_operating_summary = summarize_actor_operating_context(
            actor_contexts
        )
    except Exception as exc:  # noqa: BLE001
        _raise_if_postgres_error(exc)
        if run_id is not None:
            await debug_capture(
                conn,
                run_id=run_id,
                tenant_id=trigger.tenant_id,
                stage="error",
                payload={
                    "phase": "actor_operating_context",
                    "error": repr(exc),
                },
            )

    return ReasoningContext(
        bundle=bundle,
        allowed_region=allowed_region,
        actor_operating_summary=actor_operating_summary,
    )


async def _maybe_expand_second_pass(
    retrieval_result: RetrievalResult,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
) -> RetrievalResult:
    try:
        decision = should_run_second_pass(
            retrieval_result,
            trigger,
            sparse_threshold=RETRIEVAL_CONFIG.second_pass_sparse_threshold,
            customer_confidence_threshold=(
                RETRIEVAL_CONFIG.second_pass_customer_confidence_threshold
            ),
            t2_has_authoritative_handler=(
                trigger.kind == "T2" and is_authoritative(trigger)
            ),
        )
        log_second_pass_decision(
            decision,
            trigger=trigger,
            tenant_id=trigger.tenant_id,
        )
        retrieval_result.notes["second_pass_decision"] = {
            "run": decision.run,
            "trigger_condition": decision.trigger_condition,
            "suggested_dimensions": list(decision.suggested_dimensions),
            "reason_detail": dict(decision.reason_detail),
        }
        if decision.run:
            retrieval_result = await second_pass_expand(
                retrieval_result,
                decision.suggested_dimensions,
                conn,
            )
            retrieval_result.notes["second_pass_decision"] = {
                "run": decision.run,
                "trigger_condition": decision.trigger_condition,
                "suggested_dimensions": list(decision.suggested_dimensions),
                "reason_detail": dict(decision.reason_detail),
            }
    except Exception as exc:  # noqa: BLE001
        _raise_if_postgres_error(exc)
        retrieval_result.notes["second_pass_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        _log.warning(
            "think.second_pass_failed",
            tenant_id=str(trigger.tenant_id),
            trigger_kind=trigger.kind,
            error=str(exc),
        )
    return retrieval_result


async def _attach_dynamic_signals(
    reasoning_frame: ReasoningFrame,
    retrieval_result: RetrievalResult,
    trigger: TriggerContext,
    conn: asyncpg.Connection,
) -> ReasoningFrame:
    try:
        dynamic_signals = await detect_dynamic_signals(
            conn,
            tenant_id=trigger.tenant_id,
            model_ids=[
                getattr(model, "id")
                for model in retrieval_result.models
                if getattr(model, "id", None)
            ],
            actor_ids=trigger.scope_actors,
        )
        if not dynamic_signals:
            return reasoning_frame
        reasoning_frame = reasoning_frame.with_dynamic_signals(
            [signal.to_dict() for signal in dynamic_signals]
        )
        try:
            emitted = await emit_missing_transition_triggers(
                conn,
                tenant_id=trigger.tenant_id,
                signals=dynamic_signals,
            )
            if emitted:
                retrieval_result.notes["missing_transition_triggers_emitted"] = [
                    str(trigger_id) for trigger_id in emitted
                ]
        except Exception as exc:  # noqa: BLE001
            _raise_if_postgres_error(exc)
            retrieval_result.notes["missing_transition_emission_error"] = {
                "type": type(exc).__name__,
                "message": str(exc),
            }
            _log.warning(
                "think.missing_transition_emission_failed",
                tenant_id=str(trigger.tenant_id),
                trigger_kind=trigger.kind,
                error=str(exc),
            )
    except Exception as exc:  # noqa: BLE001
        _raise_if_postgres_error(exc)
        retrieval_result.notes["dynamic_signal_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        _log.warning(
            "think.dynamic_signal_detection_failed",
            tenant_id=str(trigger.tenant_id),
            trigger_kind=trigger.kind,
            error=str(exc),
        )
    return reasoning_frame


def _raise_if_postgres_error(exc: Exception) -> None:
    if isinstance(exc, asyncpg.PostgresError):
        raise exc


async def _augment_active_acts(
    conn: asyncpg.Connection,
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    allowed_region: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """Attach active work ledgers and expand the allowed mutation region."""

    region = set(allowed_region)
    existing_ids = {
        getattr(commitment, "id", None)
        for commitment in bundle.acts_summary.get("commitments", [])
    }
    rows = await conn.fetch(
        """
        SELECT id, tenant_id, title, state, owner_id, due_date,
               last_state_change_at, created_at
        FROM commitments
        WHERE tenant_id = $1
          AND terminal_at IS NULL
          AND state != 'closed'
        ORDER BY last_state_change_at DESC NULLS LAST,
                 created_at DESC
        LIMIT 25
        """,
        trigger.tenant_id,
    )
    for row in rows:
        if row["id"] in existing_ids:
            continue
        stub = SimpleNamespace(
            id=row["id"],
            tenant_id=row["tenant_id"],
            title=row["title"],
            state=row["state"],
            owner_id=row["owner_id"],
            due_date=row["due_date"],
            last_state_change_at=row["last_state_change_at"],
            created_at=row["created_at"],
        )
        bundle.acts_summary.setdefault("commitments", []).append(stub)
        existing_ids.add(row["id"])
        region.add(("commitment", str(row["id"])))

    existing_decision_ids = {
        getattr(decision, "id", None)
        for decision in bundle.acts_summary.get("decisions", [])
    }
    decision_rows = await conn.fetch(
        """
        SELECT id, tenant_id, title, state, created_at,
               last_state_change_at
        FROM decisions
        WHERE tenant_id = $1
          AND archived_at IS NULL
          AND state != 'archived'
        ORDER BY last_state_change_at DESC NULLS LAST,
                 created_at DESC
        LIMIT 25
        """,
        trigger.tenant_id,
    )
    for row in decision_rows:
        if row["id"] in existing_decision_ids:
            continue
        stub = SimpleNamespace(
            id=row["id"],
            tenant_id=row["tenant_id"],
            title=row["title"],
            state=row["state"],
            created_at=row["created_at"],
            last_state_change_at=row["last_state_change_at"],
        )
        bundle.acts_summary.setdefault("decisions", []).append(stub)
        existing_decision_ids.add(row["id"])
        region.add(("decision", str(row["id"])))

    return sorted(region)


def _actor_ids_for_operating_context(
    trigger: TriggerContext,
    bundle: ContextBundle,
    *,
    limit: int = 3,
) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()

    def add(actor_id: Any) -> None:
        if actor_id is None:
            return
        try:
            value = (
                actor_id
                if isinstance(actor_id, UUID)
                else UUID(str(actor_id))
            )
        except (TypeError, ValueError):
            return
        if value in seen:
            return
        seen.add(value)
        out.append(value)

    for actor_id in trigger.scope_actors:
        add(actor_id)
        if len(out) >= limit:
            return out
    for model in bundle.models:
        for actor_id in getattr(model, "scope_actors", []) or []:
            add(actor_id)
            if len(out) >= limit:
                return out
    return out
