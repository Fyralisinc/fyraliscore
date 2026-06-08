"""Fixed answerers used to isolate retrieval quality in benchmark runs."""

from __future__ import annotations

import asyncio
import os
import re
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, Field

from benchmarks.adapters.base import BenchmarkQuery
from benchmarks.fyralis_eval.reader import RetrievalOutput
from lib.llm.provider import LLMProvider, LLMUsageAggregator, build_provider


@dataclass(frozen=True)
class AnswerResult:
    answer: str
    metadata: dict[str, Any] = field(default_factory=dict)


class FixedExtractiveAnswerer:
    """Tiny deterministic answerer for the built-in toy benchmark.

    Public benchmark adapters should replace this with a model-backed fixed
    answerer, but this keeps CI deterministic and dependency-free.
    """

    def answer(self, query: BenchmarkQuery, retrieval: RetrievalOutput) -> str:
        return self.answer_result(query, retrieval).answer

    def answer_result(
        self,
        query: BenchmarkQuery,
        retrieval: RetrievalOutput,
    ) -> AnswerResult:
        evidence_texts = [
            str(item.get("content", ""))
            for item in retrieval.context_packet.get("evidence", [])
        ]
        if not evidence_texts:
            return AnswerResult("I don't know", {"answerer": "extractive"})

        query_text = query.query_text.casefold()
        if "allerg" in query_text:
            found = _first_match(evidence_texts, r"allergic to ([a-zA-Z][a-zA-Z -]+)")
            if found:
                return AnswerResult(found, {"answerer": "extractive"})
        if "drink" in query_text:
            for pattern in (
                r"drinks ([a-zA-Z][a-zA-Z -]+?) instead of",
                r"prefers ([a-zA-Z][a-zA-Z -]+)",
                r"prefer ([a-zA-Z][a-zA-Z -]+)",
            ):
                found = _first_match(evidence_texts, pattern)
                if found:
                    return AnswerResult(found, {"answerer": "extractive"})
        if "passport" in query_text:
            return AnswerResult("I don't know", {"answerer": "extractive"})
        return AnswerResult(evidence_texts[0], {"answerer": "extractive"})


class PassthroughAnswerer:
    """Return an answer produced by an upstream product surface.

    Some benchmark systems, notably Ask Fyralis, run their own retrieval and
    answer composition in one product path. The runner still expects a fixed
    answerer stage, so this adapter reads the product answer attached to the
    retrieval metadata instead of generating a second answer.
    """

    def answer(self, query: BenchmarkQuery, retrieval: RetrievalOutput) -> str:
        return self.answer_result(query, retrieval).answer

    def answer_result(
        self,
        query: BenchmarkQuery,
        retrieval: RetrievalOutput,
    ) -> AnswerResult:
        del query
        metadata = _passthrough_metadata(retrieval)
        answer = str(metadata.get("answer") or "I don't know").strip() or "I don't know"
        return AnswerResult(
            answer,
            {
                "answerer": "passthrough",
                **metadata,
            },
        )


class _LLMAnswer(BaseModel):
    answer: str = Field(
        min_length=1,
        max_length=1200,
        description=(
            "Short answer supported by the evidence, or exactly "
            "\"I don't know\" if the evidence is insufficient."
        ),
    )
    confidence: float = Field(ge=0.0, le=1.0)
    supporting_evidence_ids: list[str] = Field(default_factory=list, max_length=8)
    fulfilled_requirements: list[str] = Field(default_factory=list, max_length=16)
    missing_requirements: list[str] = Field(default_factory=list, max_length=16)


class LLMFixedAnswerer:
    """Fixed model answerer for public benchmark answer accuracy.

    The answerer is deliberately conservative: it may only answer from the
    retrieved packet. This keeps the benchmark focused on retrieval/context
    quality instead of rewarding a model for outside knowledge.
    """

    def __init__(
        self,
        *,
        provider: LLMProvider | None = None,
        temperature: float = 0.0,
        max_tokens: int = 512,
        max_evidence_chars: int = 3200,
    ) -> None:
        self.provider = provider or build_provider()
        self.temperature = temperature
        self.max_tokens = max_tokens
        self.max_evidence_chars = max_evidence_chars

    def answer(self, query: BenchmarkQuery, retrieval: RetrievalOutput) -> str:
        return self.answer_result(query, retrieval).answer

    def answer_result(
        self,
        query: BenchmarkQuery,
        retrieval: RetrievalOutput,
    ) -> AnswerResult:
        return _run_coro_sync(self.answer_result_async(query, retrieval))

    async def answer_result_async(
        self,
        query: BenchmarkQuery,
        retrieval: RetrievalOutput,
    ) -> AnswerResult:
        usage = LLMUsageAggregator()
        self.provider.set_usage_aggregator(usage)
        try:
            output = await self.provider.structured(
                system=_ANSWER_SYSTEM_PROMPT,
                user=_answer_user_prompt(
                    query,
                    retrieval,
                    max_evidence_chars=self.max_evidence_chars,
                ),
                schema=_LLMAnswer,
                temperature=self.temperature,
                max_tokens=self.max_tokens,
            )
        finally:
            self.provider.set_usage_aggregator(None)
        forced_abstention = _forced_abstention_reason(retrieval, output)
        answer = "I don't know" if forced_abstention else output.answer
        return AnswerResult(
            answer,
            {
                "answerer": "llm_fixed",
                "provider": self.provider.config.provider,
                "model": self.provider.config.model,
                "confidence": output.confidence,
                "supporting_evidence_ids": output.supporting_evidence_ids,
                "fulfilled_requirements": output.fulfilled_requirements,
                "missing_requirements": output.missing_requirements,
                "forced_abstention": forced_abstention,
                "llm": {
                    "calls": usage.call_count,
                    "input_tokens": usage.total_input_tokens,
                    "output_tokens": usage.total_output_tokens,
                    "cost_usd": usage.total_cost_usd,
                },
            },
        )


class CodexFixedAnswerer(LLMFixedAnswerer):
    """Fixed benchmark answerer pinned to the local Codex CLI/app-server auth."""

    def __init__(
        self,
        *,
        temperature: float = 0.0,
        max_tokens: int = 512,
        max_evidence_chars: int = 3200,
    ) -> None:
        super().__init__(
            provider=_build_codex_provider(),
            temperature=temperature,
            max_tokens=max_tokens,
            max_evidence_chars=max_evidence_chars,
        )


_ANSWER_SYSTEM_PROMPT = """You are a fixed benchmark answerer.

Answer the user's question using only the supplied evidence snippets.
Return the shortest benchmark-style answer, not an explanation.

Rules:
- First identify every component the question asks for. If it asks for
  multiple things joined by "and", "with", "plus", a requested format, or
  several clauses, include every requested component that is supported.
- If the question is yes/no, answer exactly "yes" or "no".
- If the answer is a person, place, organization, title, date, number, or
  other single span, return only that span.
- For "specific", "exact", "root cause", "reason", "mechanism", or
  "what evidence" questions, preserve the concrete mechanism, metric,
  owner, artifact, or constraint. Do not collapse a specific answer into a
  vague category.
- For timeline questions, respect before/after/during/first/final wording.
- Do not add citations or explanatory sentences unless the requested answer
  itself has multiple supported components.
- Respect the exact category requested by the question. If the question asks
  for a route, field, title, owner, status, code, token, or other specific
  category, answer only when the evidence explicitly supports that category.
  Do not substitute a related object, action, or UI element.
- If the question explicitly requires inspecting an external repository,
  filesystem, ticket system, or other tool surface, answer only if the
  supplied evidence already contains the resulting value. Breadcrumbs that
  merely say such inspection is needed are insufficient.
- If any answer requirement is unsupported, list it in missing_requirements
  and return exactly "I don't know" unless the question explicitly asks for a
  partial/bounded answer.
- If the evidence does not contain the answer, return exactly "I don't know".
"""


def _answer_user_prompt(
    query: BenchmarkQuery,
    retrieval: RetrievalOutput,
    *,
    max_evidence_chars: int,
) -> str:
    lines = [f"Question: {query.query_text}"]
    requirements = _packet_requirements(query, retrieval)
    if requirements:
        lines.append("")
        lines.append("Answer requirements:")
        lines.extend(f"- {item}" for item in requirements)
    sufficiency = retrieval.context_packet.get("sufficiency")
    if isinstance(sufficiency, dict):
        lines.append("")
        lines.append(
            "Packet sufficiency: "
            f"required_roles={sufficiency.get('required_roles', [])}; "
            f"covered_roles={sufficiency.get('covered_roles', [])}; "
            f"missing_roles={sufficiency.get('missing_roles', [])}; "
            f"has_finality_evidence={sufficiency.get('has_finality_evidence')}; "
            f"has_external_tool_result={sufficiency.get('has_external_tool_result')}"
        )
    warnings = [
        item
        for item in retrieval.omission_ledger
        if isinstance(item, dict) and item.get("severity") == "warning"
    ]
    if warnings:
        lines.append("")
        lines.append("Packet warnings:")
        for warning in warnings:
            lines.append(f"- {warning.get('reason')}: {warning}")

    lines.extend(["", "Evidence:"])
    evidence = retrieval.context_packet.get("evidence", [])
    if not evidence:
        lines.append("(none)")
    for index, item in enumerate(evidence, start=1):
        evidence_id = item.get("observation_id", f"evidence_{index}")
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
        metadata_line = _evidence_metadata_line(metadata)
        content = _clip_text(str(item.get("content", "")).strip(), max_evidence_chars)
        if metadata_line:
            lines.append(f"[{index}] id={evidence_id}\n{metadata_line}\n{content}")
        else:
            lines.append(f"[{index}] id={evidence_id}\n{content}")
    lines.append("")
    lines.append('Return JSON matching the schema. Use "I don\'t know" when unsupported.')
    return "\n\n".join(lines)


def _packet_requirements(
    query: BenchmarkQuery,
    retrieval: RetrievalOutput,
) -> list[str]:
    raw = retrieval.context_packet.get("answer_requirements")
    if isinstance(raw, list) and raw:
        requirements = []
        for item in raw:
            if isinstance(item, dict):
                kind = str(item.get("kind") or "requirement")
                description = str(item.get("description") or "").strip()
                requirements.append(f"{kind}: {description}" if description else kind)
        if requirements:
            return requirements
    return _answer_requirements(query)


def _forced_abstention_reason(
    retrieval: RetrievalOutput,
    output: _LLMAnswer,
) -> str | None:
    if output.answer.strip().casefold() in {"i don't know", "unknown"}:
        return None
    warning_reasons = {
        str(item.get("reason"))
        for item in retrieval.omission_ledger
        if isinstance(item, dict) and item.get("severity") == "warning"
    }
    if "external_tool_surface_not_materialized" in warning_reasons:
        return "external_tool_surface_not_materialized"
    if "missing_finality_evidence" in warning_reasons:
        return "missing_finality_evidence"
    if output.missing_requirements:
        return "answerer_reported_missing_requirements"
    return None


def _evidence_metadata_line(metadata: dict[str, Any]) -> str:
    parts = []
    for key in (
        "event_index",
        "timestamp_raw",
        "platform",
        "sender",
        "lead",
        "team",
        "status",
        "priority",
        "title",
        "commit_id",
        "pr",
    ):
        value = metadata.get(key)
        if value not in (None, ""):
            parts.append(f"{key}={value}")
    if not parts:
        return ""
    return "Structured fields: " + "; ".join(parts)


def _answer_requirements(query: BenchmarkQuery) -> list[str]:
    text = f"{query.query_text} {query.query_type}".casefold()
    requirements: list[str] = []
    if any(marker in text for marker in (" and ", " plus ", " along with ", "format:")):
        requirements.append("Include every requested component, not just the head noun.")
    if any(marker in text for marker in ("who", "team member", "owner", "lead", "championed")):
        requirements.append("Include the exact actor/team requested by the question.")
    if any(marker in text for marker in ("specific", "exact", "metric", "evidence")):
        requirements.append("Keep concrete details such as metrics, files, tickets, code symbols, or evidence source.")
    if any(marker in text for marker in ("why", "caused", "root cause", "mechanism", "reason")):
        requirements.append("Answer with the causal mechanism, not only the symptom or affected area.")
    if any(marker in text for marker in ("before", "after", "during", "first", "final", "most recent")):
        requirements.append("Apply the temporal constraint exactly.")
    if any(
        marker in text
        for marker in (
            "clone the repository",
            "examine the source",
            "actual repository",
            "repository code",
            "filesystem",
            "line number",
            ".py",
            "commit message",
            "exact filename",
        )
    ):
        requirements.append(
            "For external-tool questions, answer only if the evidence contains the computed final value."
        )
    return requirements


def _run_coro_sync(coro):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(coro)
    raise RuntimeError(
        "LLMFixedAnswerer.answer_result cannot be called from an active event loop; "
        "use answer_result_async instead."
    )


def _passthrough_metadata(retrieval: RetrievalOutput) -> dict[str, Any]:
    for item in retrieval.retrieved_evidence:
        metadata = item.metadata if isinstance(item.metadata, dict) else {}
        ask = metadata.get("passthrough_answer")
        if isinstance(ask, dict):
            return dict(ask)
    packet_answer = retrieval.context_packet.get("passthrough_answer")
    if isinstance(packet_answer, dict):
        return dict(packet_answer)
    return {}


def _clip_text(text: str, max_chars: int) -> str:
    if max_chars <= 0 or len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n[truncated]"


def _build_codex_provider() -> LLMProvider:
    previous = os.environ.get("LLM_PROVIDER")
    os.environ["LLM_PROVIDER"] = "codex"
    try:
        return build_provider()
    finally:
        if previous is None:
            os.environ.pop("LLM_PROVIDER", None)
        else:
            os.environ["LLM_PROVIDER"] = previous


def _first_match(texts: list[str], pattern: str) -> str | None:
    compiled = re.compile(pattern, re.IGNORECASE)
    for text in texts:
        match = compiled.search(text)
        if not match:
            continue
        answer = match.group(1).strip(" .,'\"")
        if answer:
            return answer.casefold()
    return None
