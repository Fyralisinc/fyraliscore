"""Context planning for Think.

This module owns the pre-reasoning context path: active retrieval, optional
second-pass expansion, reasoning-frame construction, prompt-facing assembly,
active-work augmentation, actor context, and dynamic-signal notes. It is
intentionally mutation-light; validation/apply/cascade stay in
``services.reasoning.think.reason``.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, replace
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMProvider
from services.domain.actors.operating_context import (
    load_actor_operating_context,
    summarize_actor_operating_context,
)
from services.reasoning.dynamics import (
    detect_dynamic_signals,
    emit_missing_transition_triggers,
)
from services.platform.execution.inquiry import (
    InquiryConfig,
    InquiryResult,
    retrieve_for_execution,
)
from services.platform.execution.question_planning_provider import (
    select_question_planning_provider,
)
from services.reasoning.retrieval.assembler import (
    AccessContext,
    ContextBundle,
    assemble_context,
)
from services.reasoning.retrieval.config import CONFIG as RETRIEVAL_CONFIG
from services.reasoning.retrieval.primary import RetrievalResult, TriggerContext
from services.reasoning.think.hooks import augment_context
from services.reasoning.retrieval.second_pass import (
    log_second_pass_decision,
    second_pass_expand,
    should_run_second_pass,
)

from .deterministic import is_authoritative
from .debug_capture import capture as debug_capture
from .reasoning_frame import ReasoningFrame
from .region_locks import touched_entity_ids
from .substrate_builder import build_substrate_candidates


_log = structlog.get_logger(__name__)
_diag_log = structlog.get_logger("think.diag")
_BATCH_CONTEXT_MODEL_BUDGET_DEFAULT = 16
_BATCH_CONTEXT_HISTORICAL_OBSERVATION_CAP_DEFAULT = 2
_BATCH_CONTEXT_MODEL_BUDGET_FLOOR = 8


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

    The production path is Codex-only: a real Codex Think provider is remapped
    by the inquiry provider selector to the dedicated low-effort Codex planner.
    Custom test/double providers opt in explicitly because inquiry question
    planning can otherwise make unit tests unexpectedly call custom
    structured-output methods.
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
        return select_question_planning_provider(llm_provider)
    return None


def _plan_mode_for_trigger(trigger: TriggerContext) -> str:
    """Cost-plan §2.5: choose the retrieval/planning mode for a trigger.

    Default `deep` (unchanged). `THINK_FAST_PLAN_TRIGGER_KINDS` is a comma-
    separated allowlist of trigger classes (`kind` or `kind:subkind`) that
    should use `fast` mode, which the inquiry early-return collapses to a
    single planning round — cutting the up-to-2 ~900-tok planning calls on
    low-value triggers. The value policy lives in ops (it needs prod sizing),
    so by default nothing changes."""
    fast_kinds = os.environ.get("THINK_FAST_PLAN_TRIGGER_KINDS", "").strip()
    if not fast_kinds:
        return "deep"
    wanted = {part.strip() for part in fast_kinds.split(",") if part.strip()}
    keys = {trigger.kind}
    if trigger.subkind:
        keys.add(f"{trigger.kind}:{trigger.subkind}")
    return "fast" if keys & wanted else "deep"


def _env_int_min(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return max(minimum, default)
    try:
        return max(minimum, int(raw))
    except ValueError:
        return max(minimum, default)


def _is_t1_event_batch(trigger: TriggerContext) -> bool:
    if trigger.kind != "T1":
        return False
    if trigger.subkind == "event_batch":
        return True
    seed_signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    return bool(seed_signature.get("batch")) or len(trigger.observation_ids) > 1


def _retrieval_config_for_trigger(trigger: TriggerContext):
    if not _is_t1_event_batch(trigger):
        return RETRIEVAL_CONFIG
    model_budget = _env_int_min(
        "THINK_BATCH_CONTEXT_MODEL_BUDGET",
        _BATCH_CONTEXT_MODEL_BUDGET_DEFAULT,
        minimum=1,
    )
    historical_observation_cap = _env_int_min(
        "THINK_BATCH_HISTORICAL_OBSERVATION_CAP",
        _BATCH_CONTEXT_HISTORICAL_OBSERVATION_CAP_DEFAULT,
    )
    return replace(
        RETRIEVAL_CONFIG,
        assembler_budget_models=_adaptive_t1_batch_model_budget(
            trigger,
            max_budget=min(RETRIEVAL_CONFIG.assembler_budget_models, model_budget),
        ),
        historical_observation_cap=min(
            RETRIEVAL_CONFIG.historical_observation_cap,
            historical_observation_cap,
        ),
    )


def _adaptive_t1_batch_model_budget(
    trigger: TriggerContext,
    *,
    max_budget: int,
) -> int:
    """Scale T1 batch context with batch size instead of using one static cap."""
    cap = max(1, int(max_budget))
    seed_signature = (
        trigger.seed_signature if isinstance(trigger.seed_signature, dict) else {}
    )
    signature_count = _coerce_positive_int(
        seed_signature.get("batch_size")
        or seed_signature.get("signal_count")
        or seed_signature.get("observation_count")
    )
    batch_size = max(
        len(trigger.observation_ids),
        signature_count or 0,
        1,
    )
    adaptive = _BATCH_CONTEXT_MODEL_BUDGET_FLOOR + (batch_size // 2)
    return min(cap, max(1, adaptive))


def _coerce_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _think_inquiry_config() -> InquiryConfig:
    """Think consumes Models as its memory substrate.

    Product inquiry/Ask can keep the broader default. Think is a write path:
    raw observation evidence should enter the prompt only as the existing
    ``models_only`` fallback when no Model evidence exists.
    """

    mode = os.environ.get(
        "THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE",
        "models_only",
    ).strip().lower()
    if mode not in {"models_only", "model_first", "all"}:
        mode = "models_only"
    return replace(
        InquiryConfig.from_env(),
        context_packet_evidence_mode=mode,
    )


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
        mode=_plan_mode_for_trigger(trigger),
        config=_think_inquiry_config(),
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
    bundle = await assemble_context(
        retrieval_result,
        access,
        conn,
        config=_retrieval_config_for_trigger(trigger),
    )
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

    try:
        substrate_candidates = await build_substrate_candidates(
            conn,
            tenant_id=trigger.tenant_id,
            observations=bundle.observations,
            models=bundle.models,
            run_id=run_id,
        )
        bundle.notes["substrate_candidates"] = substrate_candidates
        candidate_region = _substrate_candidate_region(substrate_candidates)
        if candidate_region:
            allowed_region = sorted(set(allowed_region) | set(candidate_region))
            bundle.notes["substrate_candidate_region_count"] = len(candidate_region)
    except asyncpg.exceptions.UndefinedTableError as exc:
        bundle.notes["substrate_candidates"] = []
        bundle.notes["substrate_candidates_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        _log.warning(
            "think.substrate_candidates_table_missing",
            tenant_id=str(trigger.tenant_id),
            run_id=str(run_id) if run_id is not None else None,
        )
    except Exception as exc:  # noqa: BLE001
        _raise_if_postgres_error(exc)
        bundle.notes["substrate_candidates"] = []
        bundle.notes["substrate_candidates_error"] = {
            "type": type(exc).__name__,
            "message": str(exc),
        }
        _log.warning(
            "think.substrate_candidates_failed",
            tenant_id=str(trigger.tenant_id),
            run_id=str(run_id) if run_id is not None else None,
            error=str(exc),
        )
        if run_id is not None:
            await debug_capture(
                conn,
                run_id=run_id,
                tenant_id=trigger.tenant_id,
                stage="error",
                payload={
                    "phase": "substrate_candidates",
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
                "retrieved_model_count": len(retrieval_result.models),
                "retrieved_observation_count": len(retrieval_result.observations),
                "selected_model_count": len(bundle.models),
                "selected_observation_count": len(bundle.observations),
                "observation_selection": bundle.notes.get(
                    "observation_selection"
                ),
                "substrate_candidate_count": len(
                    bundle.notes.get("substrate_candidates") or []
                ),
                "substrate_candidate_kinds": _substrate_candidate_kind_counts(
                    bundle.notes.get("substrate_candidates") or []
                ),
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
                # Cost-plan §3.2: propagate lineage depth onto emitted T3s.
                parent_payload=(
                    trigger.seed_signature
                    if isinstance(trigger.seed_signature, dict) else None
                ),
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
    """Expand the allowed mutation region via the installable context-augmentor
    seam (``services.reasoning.think.hooks.augment_context``).

    main governs structure: rather than hard-coding the active commitment/
    decision ledger fetch here (dev's pre-relayer approach), delegate to the
    overlay seam. Core ships no augmentor (strict-retrieval default → no-op);
    the demo overlay attaches the full active ledger and extends the region.
    """
    return await augment_context(
        conn=conn,
        trigger=trigger,
        bundle=bundle,
        allowed_region=allowed_region,
    )


def _substrate_candidate_kind_counts(candidates: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for candidate in candidates:
        kind = str(candidate.get("kind") or "unknown")
        counts[kind] = counts.get(kind, 0) + 1
    return counts


def _substrate_candidate_region(
    candidates: list[dict[str, Any]],
) -> list[tuple[str, str]]:
    region: set[tuple[str, str]] = set()
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        scope_ref = candidate.get("scope_ref")
        if isinstance(scope_ref, dict):
            ref_type = str(scope_ref.get("type") or "").strip()
            ref_id = str(scope_ref.get("id") or "").strip()
        else:
            kind = str(candidate.get("kind") or "").strip()
            ref_type = f"candidate_{kind}" if kind else ""
            ref_id = str(candidate.get("id") or "").strip()
        if ref_type and ref_id:
            region.add((ref_type, ref_id))
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
