"""services/sage/inquiry_traces/emitter.py — Phase 1 trace emission helpers.

Centralizes the wiring between the existing Think/inquiry pipeline and
the three Phase-1 trace surfaces (`retrieval_plans`, `omitted_evidence`,
`inquiry_outcome_events`). The pipeline calls site only needs to:

  * install a `TraceContext` for the in-flight inquiry session;
  * call `emit_event(...)` / `emit_retrieval_plan(...)` /
    `emit_omitted_evidence(...)` at the natural points;
  * clear the context on the way out.

Design constraints (Phase 1 wiring agent):

  * **Best-effort.** Every emit swallows repo failures with a structured
    warning. A failing Sage write must NEVER crash the Think pipeline.
  * **Tenant/session aware via ContextVar.** No new function-signature
    changes in `services/think/*` or `services/execution/inquiry.py`.
    Producers set the context, consumers read it.
  * **Connection-aware.** When the caller has an open asyncpg
    transaction the context exposes it so the emit reuses that
    connection (atomic-with-pipeline writes). Otherwise the repos fall
    back to the pool.
  * **Gated.** `SAGE_TRACE_EMIT` env var (default "1"). Setting it to a
    falsy value (`0` / `false` / `no` / `off`) makes every entry point
    a no-op without touching the repos.
"""
from __future__ import annotations

import os
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from services.sage.inquiry_traces.repo import (
    OmittedEvidenceRepo,
    OutcomeEventsRepo,
    RetrievalPlansRepo,
)
from services.sage.inquiry_traces.types import (
    OMISSION_REASONS,
    OUTCOME_EVENT_TYPES,
    OmittedEvidenceRow,
    RetrievalPlanRow,
)


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Trace context
# ---------------------------------------------------------------------


@dataclass(slots=True)
class TraceContext:
    """Per-inquiry-session emission context.

    `pool` and `conn` are mutually-exclusive-preferred: when `conn` is
    set the emitter uses it (so the trace writes ride the caller's
    transaction). Otherwise we fall back to the pool. If neither is set
    the emit is a no-op (with a structured warning).
    """

    tenant_id: UUID
    inquiry_session_id: UUID
    pool: asyncpg.Pool | None = None
    conn: asyncpg.Connection | None = None
    # Free-form room for callers to attach diagnostic notes (e.g. the
    # trigger kind so log lines are easier to grep).
    metadata: dict[str, Any] = field(default_factory=dict)


_CTX: ContextVar[TraceContext | None] = ContextVar(
    "sage_inquiry_trace_ctx", default=None,
)


def set_trace_context(ctx: TraceContext | None) -> Token[TraceContext | None]:
    """Install `ctx` as the current trace context.

    Returns a Token the caller can pass to `reset_trace_context` to
    restore the previous value. Mirrors the standard ContextVar idiom.
    """
    return _CTX.set(ctx)


def reset_trace_context(token: Token[TraceContext | None]) -> None:
    """Restore the previous trace context for this asyncio task."""
    try:
        _CTX.reset(token)
    except Exception:  # noqa: BLE001 — defensive; resetting must never crash
        _log.debug("sage_trace.reset_failed")


def current_trace_context() -> TraceContext | None:
    """Return the active context (or None when not inside an inquiry)."""
    return _CTX.get()


# ---------------------------------------------------------------------
# Feature gate
# ---------------------------------------------------------------------


def emission_enabled() -> bool:
    """Env-gated kill switch — checked on every entry point.

    Default is "on" (per the wiring spec). Operators can disable
    emission without redeploy by setting `SAGE_TRACE_EMIT=0`.
    """
    raw = os.environ.get("SAGE_TRACE_EMIT", "1").strip().lower()
    return raw not in {"0", "false", "no", "off", ""}


# ---------------------------------------------------------------------
# Emission primitives
# ---------------------------------------------------------------------


async def emit_event(
    event_type: str,
    payload: dict[str, Any] | None = None,
    *,
    ctx: TraceContext | None = None,
) -> None:
    """Append a row to `inquiry_outcome_events`. Best-effort.

    `ctx` defaults to the active TraceContext (set via
    `set_trace_context`). When no context is installed and none is
    passed in, the emit is a no-op — this lets validator/applier call
    sites stay agnostic about whether an inquiry session is in flight.
    """
    if not emission_enabled():
        return
    ctx = ctx or current_trace_context()
    if ctx is None:
        return
    if event_type not in OUTCOME_EVENT_TYPES:
        # Coerce-and-log rather than crash: a typo in a caller should
        # surface as a warning, not a 500.
        _log.warning(
            "sage_trace.unknown_event_type",
            event_type=event_type,
            session_id=str(ctx.inquiry_session_id),
        )
        return
    repo = OutcomeEventsRepo(ctx.pool, tenant_id=ctx.tenant_id)
    try:
        await repo.append(
            ctx.inquiry_session_id,
            event_type,
            payload or {},
            conn=ctx.conn,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort
        _log.warning(
            "sage_trace.event_write_failed",
            event_type=event_type,
            session_id=str(ctx.inquiry_session_id),
            error=str(exc),
            error_type=type(exc).__name__,
        )


async def emit_events_batch(
    events: list[tuple[str, dict[str, Any]]],
    *,
    ctx: TraceContext | None = None,
) -> None:
    """Convenience: append many events using a single context lookup.

    Each event is still attempted independently — one failure does not
    abort the rest. Useful for the inquiry packet-compilation step that
    needs to emit one `retrieved_evidence_used_in_packet` /
    `retrieved_evidence_omitted` per evidence card.
    """
    if not emission_enabled():
        return
    ctx = ctx or current_trace_context()
    if ctx is None:
        return
    for event_type, payload in events:
        await emit_event(event_type, payload, ctx=ctx)


async def emit_retrieval_plan(
    *,
    question_id: str,
    plan_revision: int = 0,
    intents: list[dict[str, Any]] | None = None,
    paths: list[dict[str, Any]] | None = None,
    budgets: dict[str, Any] | None = None,
    success_conditions: list[dict[str, Any]] | None = None,
    notes: dict[str, Any] | None = None,
    ctx: TraceContext | None = None,
) -> None:
    """Insert one row into `retrieval_plans`. Best-effort.

    Called by the inquiry engine right after question planning, before
    the retrieval actions execute. Keyword-only so callers don't get
    surprised by positional drift when we add new optional fields.
    """
    if not emission_enabled():
        return
    ctx = ctx or current_trace_context()
    if ctx is None:
        return
    repo = RetrievalPlansRepo(ctx.pool, tenant_id=ctx.tenant_id)
    try:
        await repo.insert(
            RetrievalPlanRow(
                inquiry_session_id=ctx.inquiry_session_id,
                question_id=question_id,
                plan_revision=int(plan_revision),
                intents=list(intents or []),
                paths=list(paths or []),
                budgets=dict(budgets or {}),
                success_conditions=list(success_conditions or []),
                notes=dict(notes or {}),
            ),
            conn=ctx.conn,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort
        _log.warning(
            "sage_trace.retrieval_plan_write_failed",
            session_id=str(ctx.inquiry_session_id),
            question_id=question_id,
            plan_revision=plan_revision,
            error=str(exc),
            error_type=type(exc).__name__,
        )


async def emit_omitted_evidence(
    *,
    source_type: str,
    source_ref: str,
    omission_reason: str,
    source_ref_id: UUID | None = None,
    question_id: str | None = None,
    retrieval_paths: list[dict[str, Any]] | None = None,
    reason_detail: str | None = None,
    score: float = 0.0,
    metadata: dict[str, Any] | None = None,
    ctx: TraceContext | None = None,
) -> None:
    """Insert one row into `omitted_evidence`. Best-effort.

    `omission_reason` is validated against `OMISSION_REASONS` here so a
    typo doesn't bubble into a Postgres CHECK violation that would taint
    the transaction (this matters because the emit may share the
    caller's connection).
    """
    if not emission_enabled():
        return
    ctx = ctx or current_trace_context()
    if ctx is None:
        return
    if omission_reason not in OMISSION_REASONS:
        _log.warning(
            "sage_trace.invalid_omission_reason",
            omission_reason=omission_reason,
            session_id=str(ctx.inquiry_session_id),
        )
        return
    repo = OmittedEvidenceRepo(ctx.pool, tenant_id=ctx.tenant_id)
    try:
        await repo.insert(
            OmittedEvidenceRow(
                inquiry_session_id=ctx.inquiry_session_id,
                question_id=question_id,
                source_type=source_type,
                source_ref=source_ref,
                source_ref_id=source_ref_id,
                retrieval_paths=list(retrieval_paths or []),
                omission_reason=omission_reason,
                reason_detail=reason_detail,
                score=float(score),
                metadata=dict(metadata or {}),
            ),
            conn=ctx.conn,
        )
    except Exception as exc:  # noqa: BLE001 — best-effort
        _log.warning(
            "sage_trace.omitted_evidence_write_failed",
            session_id=str(ctx.inquiry_session_id),
            source_type=source_type,
            source_ref=source_ref,
            error=str(exc),
            error_type=type(exc).__name__,
        )


__all__ = [
    "TraceContext",
    "emission_enabled",
    "emit_event",
    "emit_events_batch",
    "emit_omitted_evidence",
    "emit_retrieval_plan",
    "current_trace_context",
    "reset_trace_context",
    "set_trace_context",
]
