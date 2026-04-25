"""services/think/llm_reason.py — the inferential reasoning path.

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
import time
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMError, LLMParseError, LLMProvider
from lib.shared.errors import CompanyOSError

from services.retrieval.assembler import ContextBundle
from services.retrieval.primary import TriggerContext

from .diff_schema import RawDiff
from .prompt import build_prompt


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
    temperature: float = 0.2,
    max_tokens: int = 4096,
    max_attempts: int = 3,
) -> tuple[RawDiff, int]:
    """
    Return (raw_diff, elapsed_ms).

    Exponential backoff on transport failures (LLMError) — up to
    `max_attempts` total calls. LLMParseError from the provider is
    already retried internally; if it escapes, we bubble as terminal.
    """
    pair = build_prompt(
        trigger,
        bundle,
        triggering_content=triggering_content,
        triggering_actor_summary=triggering_actor_summary,
        reason_for_trigger=reason_for_trigger,
    )

    last_err: Exception | None = None
    started = time.monotonic()

    for attempt in range(max_attempts):
        try:
            diff = await provider.structured(
                system=pair.system,
                user=pair.user,
                schema=RawDiff,
                temperature=temperature,
                max_tokens=max_tokens,
            )
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
            if attempt < max_attempts - 1:
                backoff_s = 2 ** attempt
                _log.warning(
                    "think.llm_transient_failure",
                    attempt=attempt,
                    backoff_s=backoff_s,
                    error=str(e),
                )
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


__all__ = ["llm_reason", "ReasoningFailure"]
