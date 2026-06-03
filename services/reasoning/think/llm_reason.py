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

if TYPE_CHECKING:
    from .reasoning_frame import ReasoningFrame


_log = structlog.get_logger(__name__)


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
    pair = build_prompt(
        trigger,
        bundle,
        triggering_content=triggering_content,
        triggering_actor_summary=triggering_actor_summary,
        reason_for_trigger=reason_for_trigger,
        reasoning_frame=reasoning_frame,
        claims_only=schema is RawDiffClaimsOnly,
    )

    last_err: Exception | None = None
    started = time.monotonic()

    for attempt in range(max_attempts):
        try:
            diff_like = await provider.structured(
                system=pair.system,
                user=pair.user,
                schema=schema,
                temperature=temperature,
                max_tokens=max_tokens,
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
    if trigger.kind == "T2" and trigger.subkind == "belief_updated":
        if _has_graph_anchor_models(bundle):
            return RawDiff
        return RawDiffClaimsOnly
    if not bundle.models and not _has_acts(bundle) and not bundle.resources_summary:
        return RawDiffClaimsOnly
    return RawDiff


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
