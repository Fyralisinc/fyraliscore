"""services/rendering/core.py — RenderingService.

Phase 3 of the Agent-RND build plan.

For each rendering type:
  1. Build the prompt via the right prompts/ module.
  2. Call the LLM through lib.llm.provider (circuit-breakered).
  3. Run voice_rules.check_all on the output.
  4. If any REJECT violations, retry once with a correction prompt.
     If still rejected, return the flagged response + log.
  5. Record cost to view_render_costs (if a pool is configured).

The service takes an `LLMProvider` at construction time so tests can
inject a ScriptedProvider; real runs pass a DeepSeekProvider pinned to
`deepseek-chat` per the build plan (faster + cheaper than reasoner).
"""
from __future__ import annotations

import json
import time
from decimal import Decimal
from typing import Any, Callable
from uuid import UUID, uuid4

import structlog

from lib.llm.provider import (
    LLMError,
    LLMProvider,
    LLMUsage,
    LLMUsageAggregator,
    build_provider,
    using_usage_aggregator,
)
from lib.shared.errors import CompanyOSError

from .contracts import (
    RenderCardReasoningRequest,
    RenderCardReasoningResponse,
    RenderCardRequest,
    RenderCardResponse,
    RenderCloseLineRequest,
    RenderCloseLineResponse,
    RenderConversationTurnRequest,
    RenderConversationTurnResponse,
    RenderedEvidenceEntry,
    RenderedQueryChip,
    RenderGreetingRequest,
    RenderGreetingResponse,
    RenderMeta,
    RenderQueryGridRequest,
    RenderQueryGridResponse,
)
from .prompts import PromptPair
from .prompts import (
    card_decision as prompts_card_decision,
    card_observation as prompts_card_observation,
    card_question as prompts_card_question,
    card_reasoning as prompts_card_reasoning,
    close_line as prompts_close_line,
    conversation_turn as prompts_conversation_turn,
    greeting as prompts_greeting,
    query_grid_item as prompts_query_grid_item,
)
from .voice_rules import (
    RuleContext,
    Severity,
    Violation,
    check_all,
    format_corrections,
    has_rejections,
)


_log = structlog.get_logger(__name__)


class RenderingError(CompanyOSError):
    default_code = "rendering_error"


# ---------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------


class RenderingService:
    """Orchestrates prompt \u2192 LLM \u2192 voice rules \u2192 retry.

    Parameters
    ----------
    provider:
        LLMProvider instance. Required. If not supplied, callers can use
        `RenderingService.from_env()` which calls `build_provider()`.
    default_model:
        Model name reported in the response + cost records. Normally
        tracks `provider.config.model` but can be overridden for test
        determinism.
    pool:
        Optional asyncpg pool; when set, cost rows land in
        `view_render_costs` after each call. Tests pass `None`.
    max_tokens / temperature:
        LLM call knobs. Low temperature (0.2) by default because voice
        consistency matters more than creativity.
    """

    def __init__(
        self,
        *,
        provider: LLMProvider,
        pool: Any = None,
        default_model: str | None = None,
        max_tokens: int = 1024,
        temperature: float = 0.2,
    ) -> None:
        self._provider = provider
        self._pool = pool
        self._default_model = default_model or provider.config.model
        self._max_tokens = max_tokens
        self._temperature = temperature

    # -----------------------------------------------------------------
    # Factory
    # -----------------------------------------------------------------
    @classmethod
    def from_env(cls, *, pool: Any = None) -> "RenderingService":
        """Build with a provider from env. Used by the FastAPI app."""
        provider = build_provider()
        return cls(provider=provider, pool=pool)

    # -----------------------------------------------------------------
    # Public render methods
    # -----------------------------------------------------------------

    async def render_greeting(
        self, request: RenderGreetingRequest
    ) -> RenderGreetingResponse:
        started = time.monotonic()
        prompt = prompts_greeting.build_prompt(request)
        body, usage, violations, retried, flagged = await self._run_with_rules(
            prompt=prompt,
            kind="greeting",
            rule_context=RuleContext(kind="greeting"),
        )
        cost = self._record_cost(
            tenant_id=request.tenant_id,
            render_kind="greeting",
            usage=usage,
            retry_count=1 if retried else 0,
            started=started,
            flagged=flagged,
            violations=violations,
        )
        return RenderGreetingResponse(
            body_html=body,
            meta=RenderMeta(
                signals_watched_count=request.substrate_state.signals_watched_count
            ),
            rendering_model_used=self._default_model,
            cost_usd=cost,
            violations=[v.to_dict() for v in violations],
            retried=retried,
            flagged=flagged,
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    async def render_card_observation(
        self, request: RenderCardRequest
    ) -> RenderCardResponse:
        return await self._render_card(
            request, prompts_card_observation.build_prompt, "card_observation"
        )

    async def render_card_decision(
        self, request: RenderCardRequest
    ) -> RenderCardResponse:
        return await self._render_card(
            request, prompts_card_decision.build_prompt, "card_decision"
        )

    async def render_card_question(
        self, request: RenderCardRequest
    ) -> RenderCardResponse:
        return await self._render_card(
            request, prompts_card_question.build_prompt, "card_question"
        )

    async def _render_card(
        self,
        request: RenderCardRequest,
        build_prompt_fn: Callable[[RenderCardRequest], PromptPair],
        render_kind: str,
    ) -> RenderCardResponse:
        started = time.monotonic()
        prompt = build_prompt_fn(request)
        body, usage, violations, retried, flagged = await self._run_with_rules(
            prompt=prompt,
            kind=render_kind,
            rule_context=RuleContext(kind=render_kind),
        )
        # CONTRACTS.md §5 Rev-2 Change 3: decision-card bodies must carry
        # .card-content / .dec-text / .dec-chips structural hooks. Enforce
        # at the service boundary so Agent-UI always has the classes,
        # regardless of LLM compliance.
        if render_kind == "card_decision":
            body = _ensure_decision_wrappers(body, request.card_focus or {})
        cost = self._record_cost(
            tenant_id=request.tenant_id,
            render_kind=render_kind,
            usage=usage,
            retry_count=1 if retried else 0,
            started=started,
            flagged=flagged,
            violations=violations,
        )
        return RenderCardResponse(
            body_html=body,
            rendering_model_used=self._default_model,
            cost_usd=cost,
            violations=[v.to_dict() for v in violations],
            retried=retried,
            flagged=flagged,
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    async def render_query_grid(
        self, request: RenderQueryGridRequest
    ) -> RenderQueryGridResponse:
        started = time.monotonic()
        prompt = prompts_query_grid_item.build_prompt(request)
        # Query-grid raw output is a JSON array of labels, not prose.
        # Skip the prose voice-rule loop here; each label is checked
        # individually below so rejects still trigger the retry.
        raw, usage, violations, retried, flagged = await self._run_with_rules(
            prompt=prompt,
            kind="query_grid_item",
            rule_context=RuleContext(kind="query_grid_item"),
            skip_voice_rules=True,
        )
        labels = _parse_label_array(raw, expected_count=len(request.specs))
        # If parse failed, labels will be empty or short — fall back
        # to intent-based labels so the caller still gets a usable grid.
        if len(labels) < len(request.specs):
            flagged = True
            _log.warning(
                "rendering.query_grid_label_parse_partial",
                got=len(labels),
                want=len(request.specs),
                raw=raw[:500],
            )
            labels = _pad_labels(labels, request.specs)

        # Per-label voice rule check. Reject-level violations on any
        # label would mark the grid as flagged (no second retry on
        # individual labels; that would explode cost). Most rules
        # don't fire on short chip labels anyway.
        label_rule_ctx = RuleContext(kind="query_grid_item")
        for label in labels:
            if has_rejections(check_all(label, label_rule_ctx)):
                flagged = True
                break

        chips: list[RenderedQueryChip] = []
        for spec, label in zip(request.specs, labels):
            chips.append(
                RenderedQueryChip(
                    id=spec.id,
                    icon=spec.icon,
                    label=label,
                    tag=spec.tag,
                    hot=spec.hot,
                )
            )

        cost = self._record_cost(
            tenant_id=request.tenant_id,
            render_kind="query_grid",
            usage=usage,
            retry_count=1 if retried else 0,
            started=started,
            flagged=flagged,
            violations=violations,
        )
        return RenderQueryGridResponse(
            queries=chips,
            rendering_model_used=self._default_model,
            cost_usd=cost,
            violations=[v.to_dict() for v in violations],
            retried=retried,
            flagged=flagged,
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    async def render_conversation_turn(
        self, request: RenderConversationTurnRequest
    ) -> RenderConversationTurnResponse:
        started = time.monotonic()
        prompt = prompts_conversation_turn.build_prompt(request)
        body, usage, violations, retried, flagged = await self._run_with_rules(
            prompt=prompt,
            kind="conversation_turn",
            rule_context=RuleContext(kind="conversation_turn"),
        )
        # CONTRACTS.md §5 Rev-2 Change 3: conversation-turn bodies must
        # carry the .t-body structural hook so Agent-UI styling attaches.
        # Wrap deterministically at the service boundary if the model
        # didn't already include one.
        body = _ensure_turn_body_wrapper(body)
        cost = self._record_cost(
            tenant_id=request.tenant_id,
            render_kind="conversation_turn",
            usage=usage,
            retry_count=1 if retried else 0,
            started=started,
            flagged=flagged,
            violations=violations,
        )
        return RenderConversationTurnResponse(
            response_html=body,
            rendering_model_used=self._default_model,
            cost_usd=cost,
            violations=[v.to_dict() for v in violations],
            retried=retried,
            flagged=flagged,
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    async def render_card_reasoning(
        self, request: RenderCardReasoningRequest
    ) -> RenderCardReasoningResponse:
        """Gate 4b fix: compose the expanded-card `reasoning_html` +
        `evidence[]` via LLM. Output is a JSON object parsed into
        RenderedEvidenceEntry rows.

        Voice rules apply to the reasoning prose only; evidence bodies
        are short and structurally-shaped, so rule checks skip them.
        Cite-span (.cite) and note-span (.note) presence is validated
        after parsing and triggers a single retry if missing — those
        are contractual per §5 Rev 2.
        """
        started = time.monotonic()
        prompt = prompts_card_reasoning.build_prompt(request)

        # Aggregator scopes all LLM calls for this render (first + retry).
        agg = LLMUsageAggregator()
        parsed, raw_first, retried, flagged = (None, "", False, False)
        violations: list[Violation] = []

        with using_usage_aggregator(agg):
            raw_first = await self._raw_text_call(
                system=prompt.system, user=prompt.user,
            )

        parsed = _parse_reasoning_payload(raw_first)
        if parsed is None:
            # Parse failure → retry once with an explicit correction.
            retried = True
            correction = (
                "\n\nYour prior output did not parse as a JSON object with "
                "the exact keys 'reasoning_html' and 'evidence'. Return only "
                "that object, nothing else. Do not wrap in code fences."
            )
            with using_usage_aggregator(agg):
                raw_second = await self._raw_text_call(
                    system=prompt.system,
                    user=prompt.user + correction,
                )
            parsed = _parse_reasoning_payload(raw_second)
            if parsed is None:
                flagged = True
                # Degrade to a minimal shape so the caller always gets a
                # usable RenderCardReasoningResponse; voice_rules logs it.
                parsed = {
                    "reasoning_html": _safe_strip(raw_second or raw_first),
                    "evidence": [],
                }

        reasoning_html = str(parsed.get("reasoning_html", "")).strip()
        evidence_raw = parsed.get("evidence") or []
        evidence: list[RenderedEvidenceEntry] = []
        for item in evidence_raw:
            if not isinstance(item, dict):
                continue
            label = str(item.get("label", "")).strip()
            body_html = str(item.get("body_html", "")).strip()
            if label and body_html:
                evidence.append(
                    RenderedEvidenceEntry(label=label, body_html=body_html)
                )

        # Structural guarantees: at least one `.cite` span across the
        # evidence list. If the model omitted it, retry once with a
        # correction that demands the span. We piggy-back on the same
        # aggregator so cost attribution still rolls up.
        if evidence and not _has_cite_span_anywhere(evidence) and not retried:
            retried = True
            correction = (
                "\n\nYour prior output had no <span class=\"cite\"> anywhere "
                "in the evidence bodies. Rewrite so each evidence.body_html "
                "contains at least one <span class=\"cite\">actor \u2014 Day HH:MM</span> "
                "citation. Same JSON shape."
            )
            with using_usage_aggregator(agg):
                raw_third = await self._raw_text_call(
                    system=prompt.system,
                    user=prompt.user + correction,
                )
            reparsed = _parse_reasoning_payload(raw_third)
            if reparsed is not None:
                reasoning_html = str(reparsed.get("reasoning_html", reasoning_html)).strip()
                new_evs: list[RenderedEvidenceEntry] = []
                for item in reparsed.get("evidence", []) or []:
                    if not isinstance(item, dict):
                        continue
                    lbl = str(item.get("label", "")).strip()
                    bhtml = str(item.get("body_html", "")).strip()
                    if lbl and bhtml:
                        new_evs.append(
                            RenderedEvidenceEntry(label=lbl, body_html=bhtml)
                        )
                if new_evs:
                    evidence = new_evs
            if not _has_cite_span_anywhere(evidence):
                flagged = True

        # Voice rules run on the reasoning prose only (HTML stripped
        # inside the rule). Evidence bodies are structural shards.
        rule_ctx = RuleContext(kind="card_reasoning")
        violations = check_all(reasoning_html, rule_ctx)
        if has_rejections(violations):
            # One extra retry on voice rejects (orthogonal to the cite
            # retry above; we cap total retries at 2).
            if not retried:
                retried = True
                correction_text = format_corrections(violations)
                with using_usage_aggregator(agg):
                    raw_voice = await self._raw_text_call(
                        system=prompt.system,
                        user=prompt.user + "\n\n" + correction_text,
                    )
                reparsed = _parse_reasoning_payload(raw_voice)
                if reparsed is not None:
                    new_reasoning = str(reparsed.get("reasoning_html", "")).strip()
                    if new_reasoning:
                        reasoning_html = new_reasoning
                    new_evs: list[RenderedEvidenceEntry] = []
                    for item in reparsed.get("evidence", []) or []:
                        if not isinstance(item, dict):
                            continue
                        lbl = str(item.get("label", "")).strip()
                        bhtml = str(item.get("body_html", "")).strip()
                        if lbl and bhtml:
                            new_evs.append(
                                RenderedEvidenceEntry(label=lbl, body_html=bhtml)
                            )
                    if new_evs:
                        evidence = new_evs
                violations = check_all(reasoning_html, rule_ctx)
            if has_rejections(violations):
                flagged = True
                _log.warning(
                    "rendering.card_reasoning_voice_rejected_after_retry",
                    violations=[v.to_dict() for v in violations],
                )
        elif any(v.severity is Severity.FLAG for v in violations):
            flagged = True

        total_usage = _aggregate(agg, model_name=self._provider.config.model)
        cost = self._record_cost(
            tenant_id=request.tenant_id,
            render_kind="card_reasoning",
            usage=total_usage,
            retry_count=1 if retried else 0,
            started=started,
            flagged=flagged,
            violations=violations,
        )
        return RenderCardReasoningResponse(
            reasoning_html=reasoning_html,
            evidence=evidence,
            rendering_model_used=self._default_model,
            cost_usd=cost,
            violations=[v.to_dict() for v in violations],
            retried=retried,
            flagged=flagged,
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    async def render_close_line(
        self, request: RenderCloseLineRequest
    ) -> RenderCloseLineResponse:
        started = time.monotonic()
        prompt = prompts_close_line.build_prompt(request)
        body, usage, violations, retried, flagged = await self._run_with_rules(
            prompt=prompt,
            kind="close_line",
            rule_context=RuleContext(kind="close_line"),
        )
        cost = self._record_cost(
            tenant_id=request.tenant_id,
            render_kind="close_line",
            usage=usage,
            retry_count=1 if retried else 0,
            started=started,
            flagged=flagged,
            violations=violations,
        )
        return RenderCloseLineResponse(
            body=body.strip(),
            metadata={
                "signal_count": request.signals_watched_count,
                "external_moves": request.external_moves,
                "calibration_pct": request.calibration_pct,
            },
            rendering_model_used=self._default_model,
            cost_usd=cost,
            violations=[v.to_dict() for v in violations],
            retried=retried,
            flagged=flagged,
            latency_ms=int((time.monotonic() - started) * 1000),
        )

    # -----------------------------------------------------------------
    # Internal: single LLM call with voice-rule retry
    # -----------------------------------------------------------------

    async def _run_with_rules(
        self,
        *,
        prompt: PromptPair,
        kind: str,
        rule_context: RuleContext,
        skip_voice_rules: bool = False,
    ) -> tuple[str, LLMUsage, list[Violation], bool, bool]:
        """Return (body_text, usage, violations, retried, flagged).

        Retries exactly once if the first output has REJECT-severity
        violations; the retry includes a correction prompt appended to
        the user message.

        `skip_voice_rules=True` bypasses the post-LLM rule check — used
        by the query-grid path where the raw output is a JSON array,
        not prose; the caller runs rules per-label instead.
        """
        # Week 5: aggregator is task-local (ContextVar) so concurrent
        # render calls sharing a provider don't overwrite each other's
        # `_usage_aggregator` instance attribute. Fixes the render-cost
        # attribution holes where greeting/query_grid/close_line landed
        # as $0.00 under concurrent scheduler fan-out.
        agg = LLMUsageAggregator()
        with using_usage_aggregator(agg):
            first = await self._raw_text_call(
                system=prompt.system, user=prompt.user,
            )

        if skip_voice_rules:
            total_usage = _aggregate(agg, model_name=self._provider.config.model)
            return first.strip(), total_usage, [], False, False

        violations = check_all(first, rule_context)
        retried = False
        flagged = False
        body = first.strip()

        if has_rejections(violations):
            retried = True
            correction = format_corrections(violations)
            retry_agg = LLMUsageAggregator()
            with using_usage_aggregator(retry_agg):
                second = await self._raw_text_call(
                    system=prompt.system,
                    user=prompt.user + "\n\n" + correction,
                )

            second_violations = check_all(second, rule_context)
            # Combine token usage from both calls.
            for c in retry_agg.calls:
                agg.record(c)

            if has_rejections(second_violations):
                # Still failing after one retry; emit flagged.
                flagged = True
                _log.warning(
                    "rendering.voice_rules_rejected_after_retry",
                    kind=kind,
                    violations=[v.to_dict() for v in second_violations],
                )
                body = second.strip()
                violations = second_violations
            else:
                body = second.strip()
                violations = second_violations

        # Flag-severity violations: pass through but mark.
        if not has_rejections(violations) and any(
            v.severity is Severity.FLAG for v in violations
        ):
            flagged = True

        # Snapshot total usage for the caller. Service-visible cost
        # aggregates across both LLM calls if a retry happened.
        total_usage = _aggregate(agg, model_name=self._provider.config.model)
        return body, total_usage, violations, retried, flagged

    async def _raw_text_call(self, *, system: str, user: str) -> str:
        """
        Thin adapter around the provider's `_raw_call`. We intentionally
        bypass `structured()` because our outputs are HTML fragments or
        plain text, not JSON-validated schemas.

        Uses the provider's existing circuit breaker (Anthropic /
        OpenAI / DeepSeek _raw_call is wrapped via _through_breaker).

        Week 5 stabilization: DeepSeek-chat sometimes returned
        `{"greeting_html":"..."}` JSON-wrapped prose rather than raw
        HTML (root cause was JSON-mode forced in provider `_raw_call`;
        that's now gated on `schema_hint` being non-empty). We keep the
        `_unwrap_json_wrapped_html` post-processor as a belt-and-braces
        safety net for the known wrapper shape so a one-off
        misbehavior doesn't leak JSON braces into the UI.
        """
        try:
            raw = await self._provider._raw_call(
                system=system,
                user=user,
                temperature=self._temperature,
                max_tokens=self._max_tokens,
                schema_hint="",  # no JSON schema
            )
        except LLMError:
            raise
        except Exception as exc:
            # Keep the failure surface consistent for callers.
            raise RenderingError(
                f"LLM call failed: {exc}",
                kind="llm_failure",
            ) from exc
        return _unwrap_json_wrapped_html(raw)

    # -----------------------------------------------------------------
    # Cost bookkeeping
    # -----------------------------------------------------------------

    def _record_cost(
        self,
        *,
        tenant_id: UUID,
        render_kind: str,
        usage: LLMUsage,
        retry_count: int,
        started: float,
        flagged: bool,
        violations: list[Violation],
    ) -> Decimal:
        """Return the call's cost as Decimal and, if a pool is set,
        persist a row to view_render_costs. Persistence is fire-and-log;
        a DB write failure is never allowed to mask the rendered prose.
        """
        cost_decimal = Decimal(str(round(usage.cost_usd, 6)))
        if self._pool is None:
            return cost_decimal

        outcome = _derive_outcome(violations=violations, flagged=flagged)
        latency_ms = int((time.monotonic() - started) * 1000)
        render_id = uuid4()

        async def _do_insert() -> None:
            try:
                async with self._pool.acquire() as conn:
                    await conn.execute(
                        """
                        INSERT INTO view_render_costs (
                            render_id, tenant_id, render_kind,
                            llm_calls_count, llm_input_tokens_total,
                            llm_output_tokens_total, llm_cost_usd,
                            latency_total_ms, retry_count, flagged,
                            outcome, model_name
                        )
                        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
                        """,
                        render_id,
                        tenant_id,
                        render_kind,
                        1 + retry_count,
                        usage.input_tokens,
                        usage.output_tokens,
                        cost_decimal,
                        latency_ms,
                        retry_count,
                        flagged,
                        outcome,
                        usage.model_name or self._default_model,
                    )
            except Exception as exc:
                _log.warning(
                    "rendering.cost_record_failed",
                    kind=render_kind,
                    tenant_id=str(tenant_id),
                    error=str(exc),
                )

        # Fire-and-forget; caller does not await the insert.
        import asyncio

        try:
            loop = asyncio.get_running_loop()
            loop.create_task(_do_insert())
        except RuntimeError:
            # No running loop (unusual path); skip persistence.
            pass

        return cost_decimal


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------


def _aggregate(agg: LLMUsageAggregator, *, model_name: str) -> LLMUsage:
    """Reduce an aggregator into a single LLMUsage carrying totals."""
    return LLMUsage(
        input_tokens=agg.total_input_tokens,
        output_tokens=agg.total_output_tokens,
        model_name=model_name,
        cost_usd=agg.total_cost_usd,
    )


def _derive_outcome(*, violations: list[Violation], flagged: bool) -> str:
    if has_rejections(violations):
        return "rejected_after_retry"
    if flagged:
        return "success_with_flags"
    return "success"


def _parse_label_array(raw: str, *, expected_count: int) -> list[str]:
    """The query-grid prompt returns a JSON array of label strings. Be
    forgiving: strip code fences, try to locate the first array if the
    model prefixed prose.
    """
    if not raw:
        return []
    text = raw.strip()
    # Strip code fences.
    if text.startswith("```"):
        lines = text.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        text = "\n".join(lines).strip()
    # Try whole-text parse first.
    labels: list[str] = []
    try:
        parsed = json.loads(text)
        if isinstance(parsed, list):
            labels = [str(x).strip() for x in parsed]
    except Exception:
        # Find the first [...] substring.
        start = text.find("[")
        end = text.rfind("]")
        if start != -1 and end != -1 and end > start:
            try:
                parsed = json.loads(text[start : end + 1])
                if isinstance(parsed, list):
                    labels = [str(x).strip() for x in parsed]
            except Exception:
                pass
    # Trim to expected_count if the model was over-enthusiastic.
    return labels[:expected_count]


def _pad_labels(labels: list[str], specs: list) -> list[str]:
    """Fill missing label slots with a sane intent-based fallback."""
    out = list(labels)
    for i in range(len(labels), len(specs)):
        out.append(specs[i].intent[:60] if specs[i].intent else "Ask the system")
    return out


# ---------------------------------------------------------------------
# Week 5 — DeepSeek JSON-wrap unwrap safety net
# ---------------------------------------------------------------------


# Keys the JSON wrapper was observed to use in the live Week-4 run
# (`greeting_html`, `close_line_html`, `card_html`, etc.). We match
# any single-key object whose key ends with `_html` OR exactly matches
# one of the known labels, and whose value is a string. Narrow: if the
# object has multiple keys or non-string values, we leave it alone and
# voice-rules will flag it — we are not trying to rescue arbitrary
# malformed output, only the one DeepSeek wrapping pattern.
_JSON_WRAP_HTML_KEYS = frozenset({
    "html", "body_html", "greeting_html", "close_line_html",
    "card_html", "response_html", "turn_html", "query_html",
    "content", "content_html", "prose", "prose_html", "output",
    "output_html",
})


def _unwrap_json_wrapped_html(raw: str) -> str:
    """Unwrap the `{"<name>_html":"..."}` shape DeepSeek-chat
    occasionally emits in place of raw HTML prose.

    - If the string parses as a single-key JSON object whose key is
      in `_JSON_WRAP_HTML_KEYS` (or ends with `_html`) and whose value
      is a string, return that string.
    - Otherwise return `raw` unchanged. Code fences are tolerated via
      the existing `_strip_code_fences` import path; we reuse that
      stripper here too so wrapped-in-fences is also unwrapped.
    - Parse failure → raw unchanged. Voice rules will catch genuine
      malformed output.
    """
    if not raw:
        return raw
    candidate = raw.strip()
    if not candidate:
        return raw
    # Tolerate ```json ... ``` fences around the wrapper.
    stripped = _strip_code_fences_safe(candidate)
    if not stripped.startswith("{"):
        return raw
    try:
        parsed = json.loads(stripped)
    except Exception:
        return raw
    if not isinstance(parsed, dict) or len(parsed) != 1:
        return raw
    (key, value), = parsed.items()
    if not isinstance(value, str):
        return raw
    key_lc = key.lower()
    if key_lc in _JSON_WRAP_HTML_KEYS or key_lc.endswith("_html"):
        return value
    return raw


def _parse_reasoning_payload(raw: str) -> dict | None:
    """Parse the card-reasoning LLM output. Expect a single JSON object
    with keys `reasoning_html` (str) and `evidence` (list[dict]).

    Forgiving:
      - Strip ```json / ``` fences if present.
      - Locate the first `{` and matching last `}` if prose wraps the JSON.
      - Return None if the shape is wrong (caller retries).
    """
    if not raw:
        return None
    text = _strip_code_fences_safe(raw.strip())
    if not text.startswith("{"):
        # Try to locate an embedded object.
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            return None
        text = text[start : end + 1]
    try:
        parsed = json.loads(text)
    except Exception:
        return None
    if not isinstance(parsed, dict):
        return None
    if "reasoning_html" not in parsed:
        return None
    ev = parsed.get("evidence")
    if ev is not None and not isinstance(ev, list):
        return None
    return parsed


def _has_cite_span_anywhere(evidence: list) -> bool:
    """True when at least one evidence entry's body_html carries
    `class="cite"` or `class='cite'` (Rev-2 §5 hook). Structural, not
    strict HTML parse."""
    for e in evidence:
        body = getattr(e, "body_html", None)
        if not body:
            continue
        if 'class="cite"' in body or "class='cite'" in body:
            return True
    return False


def _safe_strip(text: str | None) -> str:
    if not text:
        return ""
    return _strip_code_fences_safe(text).strip()


def _strip_code_fences_safe(text: str) -> str:
    """Mirror of `lib.llm.provider._strip_code_fences` kept local so
    the rendering module doesn't have to import a private helper."""
    s = text.strip()
    if s.startswith("```"):
        lines = s.splitlines()
        if lines and lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        s = "\n".join(lines).strip()
    return s


# ---------------------------------------------------------------------
# Rev-2 Change 3 structural wrappers
# ---------------------------------------------------------------------


def _ensure_turn_body_wrapper(body: str) -> str:
    """Ensure the conversation-turn output is wrapped in
    `<div class="t-body">…</div>`. Idempotent: if the LLM already
    emitted the wrapper, return as-is."""
    stripped = body.strip()
    if not stripped:
        return stripped
    # Case-insensitive check for an outermost t-body wrapper at either
    # end; don't try to parse arbitrary HTML, just see if the class hook
    # is present anywhere (the UI only needs the class, not a specific
    # position).
    if 'class="t-body"' in stripped or "class='t-body'" in stripped:
        return stripped
    return f'<div class="t-body">{stripped}</div>'


def _ensure_decision_wrappers(body: str, card_focus: dict) -> str:
    """Ensure a decision-card body_html carries .card-content /
    .dec-text / .dec-chips wrappers.

    If the model already emitted `class="card-content"`, pass through.
    Otherwise wrap the prose in the required scaffold, pulling chip
    values from `card_focus.deadline` + `card_focus.at_stake`.
    """
    stripped = body.strip()
    if not stripped:
        return stripped
    has_content = 'class="card-content"' in stripped or "class='card-content'" in stripped
    if has_content:
        return stripped

    # Extract deadline + at_stake from card_focus; fall back to
    # readable defaults so the structure is always present.
    deadline = str(card_focus.get("deadline") or "soon")
    at_stake = str(card_focus.get("at_stake") or "significant value")
    dec_text = stripped
    # If the model already supplied a <p class="dec-text">, do not re-wrap.
    if 'class="dec-text"' not in stripped and "class='dec-text'" not in stripped:
        dec_text = f'<p class="dec-text">{stripped}</p>'
    chips = (
        '<div class="dec-chips">'
        f'<span class="dec-chip hot">decide by <b>{deadline}</b></span>'
        f'<span class="dec-chip">at stake <b>{at_stake}</b></span>'
        '</div>'
    )
    return f'<div class="card-content">{dec_text}{chips}</div>'


__all__ = [
    "RenderingError",
    "RenderingService",
]
