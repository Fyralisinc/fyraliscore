"""Shared LLM prompt/rendering helpers for document summarization."""
from __future__ import annotations

import json
import os
from typing import Any, Protocol

from pydantic import BaseModel, Field

from lib.llm.provider import LLMConfig, LLMProvider, build_provider


DEFAULT_SUMMARIZER_MODEL = "gpt-5.3-codex-spark"
DEFAULT_SUMMARY_MAX_CHARS = 1800
DEFAULT_SUMMARY_MAX_TOKENS = 1200


class SummaryResult(BaseModel):
    summary_text: str
    model: str | None = None
    # Structured extraction (summary/key_points/decisions/action_items/risks),
    # retained verbatim instead of being discarded after render_summary().
    # Persisted to content.summarization.structured; consumed by the
    # document-memory substrate (see docs/plans/document-memory-substrate.md).
    structured: dict[str, Any] | None = None


class Summarizer(Protocol):
    async def summarize(
        self,
        text: str,
        *,
        metadata: dict[str, Any],
    ) -> SummaryResult:
        ...


class DocumentSummarySchema(BaseModel):
    summary: str = Field(min_length=1)
    key_points: list[str] = Field(default_factory=list)
    decisions: list[str] = Field(default_factory=list)
    action_items: list[str] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)


def summary_limit_from_env() -> int:
    try:
        return int(os.environ.get("INGEST_SUMMARY_MAX_CHARS", str(DEFAULT_SUMMARY_MAX_CHARS)))
    except ValueError:
        return DEFAULT_SUMMARY_MAX_CHARS


def truncate_summary_part(text: str, limit: int) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def render_summary(out: DocumentSummarySchema, *, max_chars: int) -> str:
    lines: list[str] = [truncate_summary_part(out.summary, max_chars)]
    for label, values in (
        ("Key points", out.key_points),
        ("Decisions", out.decisions),
        ("Actions", out.action_items),
        ("Risks", out.risks),
    ):
        cleaned = [truncate_summary_part(str(v), 220) for v in values if str(v).strip()]
        if cleaned:
            lines.append(f"{label}: " + "; ".join(cleaned[:5]))
    return truncate_summary_part("\n".join(lines), max_chars)


def build_summary_prompt(
    text: str,
    *,
    metadata: dict[str, Any],
    max_chars: int,
) -> tuple[str, str]:
    title = metadata.get("title") or metadata.get("name") or "document"
    source_channel = metadata.get("source_channel") or "unknown"
    system = (
        "You summarize business documents for a reasoning engine. Preserve "
        "decisions, commitments, owners, dates, metrics, risks, blockers, "
        "and concrete facts. Do not invent facts. Write a compact brief "
        "that can replace the original document in an observation."
    )
    user = (
        f"Source channel: {source_channel}\n"
        f"Title: {title}\n\n"
        "Return JSON with summary, key_points, decisions, action_items, "
        "and risks. Keep the rendered brief under about "
        f"{max_chars} characters.\n\n"
        "Document text:\n"
        f"{text}"
    )
    return system, user


def parse_summary_text(
    text: str,
    *,
    model: str | None,
    max_chars: int,
) -> SummaryResult:
    try:
        parsed = DocumentSummarySchema.model_validate_json(text)
    except Exception:
        parsed = DocumentSummarySchema.model_validate(json.loads(text))
    return SummaryResult(
        summary_text=render_summary(parsed, max_chars=max_chars),
        model=model,
        structured=parsed.model_dump(),
    )


class LLMSummarizer:
    def __init__(self, provider: LLMProvider, *, max_chars: int) -> None:
        self._provider = provider
        self._max_chars = max_chars

    @property
    def model_name(self) -> str | None:
        return getattr(self._provider.config, "model", None)

    async def summarize(
        self,
        text: str,
        *,
        metadata: dict[str, Any],
    ) -> SummaryResult:
        system, user = build_summary_prompt(
            text,
            metadata=metadata,
            max_chars=self._max_chars,
        )
        out = await self._provider.structured(
            system=system,
            user=user,
            schema=DocumentSummarySchema,
            temperature=0.0,
            max_tokens=DEFAULT_SUMMARY_MAX_TOKENS,
        )
        return SummaryResult(
            summary_text=render_summary(out, max_chars=self._max_chars),
            model=self.model_name,
            structured=out.model_dump(),
        )


def build_default_summarizer() -> LLMSummarizer:
    base = LLMConfig.from_env()
    model = os.environ.get("INGEST_SUMMARIZER_MODEL", DEFAULT_SUMMARIZER_MODEL)
    timeout_s = float(os.environ.get("INGEST_SUMMARIZER_TIMEOUT_SECONDS", "60"))
    max_retries = int(os.environ.get("INGEST_SUMMARIZER_MAX_RETRIES", "0"))
    provider = build_provider(
        LLMConfig(
            provider=base.provider,
            api_key=base.api_key,
            model=model,
            timeout_s=timeout_s,
            max_retries=max_retries,
            reasoning_effort="low" if base.provider == "codex" else None,
        )
    )
    return LLMSummarizer(provider, max_chars=summary_limit_from_env())


__all__ = [
    "DEFAULT_SUMMARIZER_MODEL",
    "DEFAULT_SUMMARY_MAX_CHARS",
    "DEFAULT_SUMMARY_MAX_TOKENS",
    "DocumentSummarySchema",
    "LLMSummarizer",
    "SummaryResult",
    "Summarizer",
    "build_default_summarizer",
    "build_summary_prompt",
    "parse_summary_text",
    "render_summary",
    "summary_limit_from_env",
]
