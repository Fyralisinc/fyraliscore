"""services/reasoning/think/llm_reason.py — the inferential reasoning path.

Spec §7 "LLM reasoning". BUILD-PLAN §4 Prompt 3.B item 3.

Wraps `LLMProvider.structured(schema=RawDiff)` with exponential
backoff on transport failures. Parse-failure retry (up to 2) is built
into the provider itself.

Note: we ask the LLM to return a RawDiff (which has the same shape as
ValidatedDiff but hasn't been validated yet). The validator rejects
ops that fail; the retry-at-apply path is a Wave-5 enhancement.
"""
from __future__ import annotations

import asyncio
import os
import random
import time
from typing import TYPE_CHECKING

import structlog

from lib.llm.provider import (
    LLMError,
    LLMParseError,
    LLMProvider,
    classify_error,
    retry_policy_for,
)
from lib.shared.errors import CompanyOSError

from services.reasoning.retrieval.assembler import ContextBundle
from services.reasoning.retrieval.primary import TriggerContext

from .diff_schema import RawDiff, RawDiffClaimsOnly
from .prompt import build_prompt
from .reasoning_frame import reasoning_job_from_trigger

if TYPE_CHECKING:
    from .reasoning_frame import ReasoningFrame


_log = structlog.get_logger(__name__)
_CLAIMS_ONLY_MAX_TOKENS_DEFAULT = 1024


class ReasoningFailure(CompanyOSError):
    default_code = "reasoning_failure"


async def llm_reason(
    trigger: TriggerContext,
    bundle: ContextBundle,
    provider: LLMProvider,
    *,
    triggering_content: str | None = None,
    triggering_actor_summary: str | None = None,
    reason_for_trigger: str | None = None,
    reasoning_frame: ReasoningFrame | None = None,
    temperature: float = 0.2,
    max_tokens: int = 2048,
    max_attempts: int = 3,
) -> tuple[RawDiff, int]:
    """
    Return (raw_diff, elapsed_ms).

    Exponential backoff on transport failures (LLMError) — up to
    `max_attempts` total calls. LLMParseError from the provider is
    already retried internally; if it escapes, we bubble as terminal.
    """
    schema = _select_output_schema(trigger, bundle)
    effective_max_tokens = _effective_max_tokens(max_tokens, schema)
    pair = build_prompt(
        trigger,
        bundle,
        triggering_content=triggering_content,
        triggering_actor_summary=triggering_actor_summary,
        reason_for_trigger=reason_for_trigger,
        reasoning_frame=reasoning_frame,
        claims_only=schema is RawDiffClaimsOnly,
        # Cost-plan §1.2: only lean the shape prose when the provider actually
        # enforces the schema server-side (DeepSeek strict). The flag itself is
        # checked inside build_prompt; hint-only providers report False here.
        lean_output_contract=provider.enforces_output_schema(schema),
    )

    # Cost-plan §2.4: if a prior attempt persisted validator feedback into the
    # trigger payload, append it so this retry avoids the dropped ops. Only
    # present when THINK_VALIDATION_MAX_ATTEMPTS drove the worker to persist it.
    user_message = pair.user
    feedback = None
    if isinstance(trigger.seed_signature, dict):
        raw_feedback = trigger.seed_signature.get("validation_feedback")
        if isinstance(raw_feedback, str) and raw_feedback.strip():
            feedback = raw_feedback.strip()
    if feedback:
        user_message = (
            f"{pair.user}\n\n<prior_validation_feedback>\n"
            "A previous attempt at this trigger had operations dropped by the "
            "validator. Correct these and do not re-introduce them:\n"
            f"{feedback}\n</prior_validation_feedback>"
        )

    last_err: Exception | None = None
    started = time.monotonic()

    for attempt in range(max_attempts):
        try:
            diff_like = await provider.structured(
                system=pair.system,
                user=user_message,
                schema=schema,
                temperature=temperature,
                max_tokens=effective_max_tokens,
            )
            diff = _coerce_raw_diff(diff_like)
            elapsed_ms = int((time.monotonic() - started) * 1000)
            return diff, elapsed_ms
        except LLMParseError as e:
            # Terminal — provider already exhausted its own retries.
            raise ReasoningFailure(
                f"LLM output failed to parse after provider retries: {e}",
                attempt=attempt,
            ) from e
        except LLMError as e:
            last_err = e
            policy = retry_policy_for(e)
            total_allowed = min(max_attempts, 1 + policy.max_attempts)
            if attempt < total_allowed - 1:
                base_backoff_s = policy.delay_for(attempt + 1)
                # Jitter ±25% to avoid thundering herd when many
                # concurrent triggers hit a provider rate-limit at once.
                jittered = base_backoff_s * random.uniform(0.75, 1.25)
                backoff_s = max(base_backoff_s, jittered)
                _log.warning(
                    "think.llm_retryable_failure",
                    attempt=attempt,
                    backoff_s=backoff_s,
                    error_class=classify_error(e).value,
                    error=str(e),
                )
                if backoff_s > 0:
                    await asyncio.sleep(backoff_s)
                continue
            break
        except Exception as e:
            last_err = e
            break

    raise ReasoningFailure(
        f"llm_reason exhausted {max_attempts} attempts: {last_err}",
        attempts=max_attempts,
    ) from last_err


def _select_output_schema(
    trigger: TriggerContext,
    bundle: ContextBundle,
) -> type[RawDiff] | type[RawDiffClaimsOnly]:
    job = reasoning_job_from_trigger(trigger)
    if job.intent == "propagate_consequence":
        if _has_graph_anchor_models(bundle):
            return RawDiff
        return RawDiffClaimsOnly
    if not bundle.models and not _has_acts(bundle) and not bundle.resources_summary:
        return RawDiffClaimsOnly
    return RawDiff


def _effective_max_tokens(
    max_tokens: int,
    schema: type[RawDiff] | type[RawDiffClaimsOnly],
) -> int:
    if schema is not RawDiffClaimsOnly:
        return max_tokens
    cap = _env_int(
        "THINK_CLAIMS_ONLY_MAX_TOKENS",
        _CLAIMS_ONLY_MAX_TOKENS_DEFAULT,
        minimum=256,
    )
    return min(max_tokens, cap)


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return max(minimum, int(raw))
    except ValueError:
        return default


def _has_graph_anchor_models(bundle: ContextBundle) -> bool:
    notes = bundle.notes if isinstance(bundle.notes, dict) else {}
    selection = notes.get("model_selection")
    if not isinstance(selection, dict):
        return False
    pathway_survival = selection.get("pathway_survival")
    if not isinstance(pathway_survival, dict):
        return False
    graph = pathway_survival.get("G")
    if not isinstance(graph, dict):
        return False
    return bool(graph.get("selected_model_ids"))


def _has_acts(bundle: ContextBundle) -> bool:
    for rows in (bundle.acts_summary or {}).values():
        if rows:
            return True
    return False


def _coerce_raw_diff(diff: RawDiff | RawDiffClaimsOnly) -> RawDiff:
    if isinstance(diff, RawDiff):
        return diff
    return RawDiff(
        trigger_ref=diff.trigger_ref,
        tenant_id=diff.tenant_id,
        claim_ops=list(diff.claim_ops),
        edge_ops=[],
        act_ops=[],
        resource_ops=[],
        new_predictions=[],
        reasoning_trace=diff.reasoning_trace,
    )


__all__ = ["llm_reason", "ReasoningFailure"]
