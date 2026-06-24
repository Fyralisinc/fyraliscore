"""Shared LLM prompt/rendering helpers for document summarization."""
from __future__ import annotations

import json
import logging
import os
from typing import Any, Protocol

from pydantic import BaseModel, Field, field_validator

from lib.llm.provider import LLMConfig, LLMProvider, build_provider
from lib.observability.metrics import DOC_MEMORY_MAPREDUCE_SECTIONS


log = logging.getLogger(__name__)


DEFAULT_SUMMARIZER_MODEL = "gpt-5.3-codex-spark"
DEFAULT_SUMMARY_MAX_CHARS = 1800
DEFAULT_SUMMARY_MAX_TOKENS = 1200

# Map-reduce knobs (see docs/plans/document-memory-substrate.md §3.2/§3.3).
# Below INGEST_SUMMARY_MAPREDUCE_CHARS the existing single LLM call is unchanged;
# above it the source text is split into INGEST_SUMMARY_SECTION_CHARS-sized
# sections (with INGEST_SUMMARY_SECTION_OVERLAP overlap), each summarized into a
# partial schema, then merged + one final reduce pass.
DEFAULT_MAPREDUCE_CHARS = 24000
DEFAULT_SECTION_CHARS = 12000
DEFAULT_SECTION_OVERLAP = 800

# The batch lane (OpenAI Batch API) cannot run multi-stage map-reduce in one
# request line, so it sends the whole document in a single call. This cap bounds
# the input it will send; items above it are LOGGED (never silently truncated).
DEFAULT_BATCH_INPUT_CAP_CHARS = 120000


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


class ActionItem(BaseModel):
    """A commitment/action item carrying optional owner and due date.

    The summarizer prompt already asks for owners and dates; making the schema
    structured lets the document-memory substrate mint commitment Models with
    `evaluate_at`/scope-actor (see docs/plans/document-memory-substrate.md §3.1).
    Back-compat: a bare string is coerced to ``{"what": <string>}`` so models that
    still emit a plain list of strings continue to parse.
    """

    who: str | None = None
    what: str = Field(min_length=1)
    due: str | None = None


def _coerce_action_item(value: Any) -> Any:
    """Accept either a structured {who?, what, due?} dict or a bare string."""
    if isinstance(value, str):
        return {"what": value}
    return value


class DocumentSummarySchema(BaseModel):
    summary: str = Field(min_length=1)
    key_points: list[str] = Field(default_factory=list)
    decisions: list[str] = Field(default_factory=list)
    # Structured commitments with optional owner/due; bare strings still accepted
    # (back-compat) and normalized to {"what": ...} by the field validator below.
    action_items: list[ActionItem] = Field(default_factory=list)
    risks: list[str] = Field(default_factory=list)

    @field_validator("action_items", mode="before")
    @classmethod
    def _normalize_action_items(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [_coerce_action_item(item) for item in value]
        return value


# `from __future__ import annotations` defers the `list[ActionItem]` annotation;
# rebuild now so the forward reference is resolved at import time (rather than
# lazily on first validation, which is fragile under non-standard module loads).
DocumentSummarySchema.model_rebuild()


def _int_from_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


def summary_limit_from_env() -> int:
    try:
        return int(os.environ.get("INGEST_SUMMARY_MAX_CHARS", str(DEFAULT_SUMMARY_MAX_CHARS)))
    except ValueError:
        return DEFAULT_SUMMARY_MAX_CHARS


def mapreduce_threshold_from_env() -> int:
    """Source-text length (chars) above which map-reduce engages."""
    return _int_from_env("INGEST_SUMMARY_MAPREDUCE_CHARS", DEFAULT_MAPREDUCE_CHARS)


def section_chars_from_env() -> int:
    """Per-section size (chars) for the map step."""
    return max(1, _int_from_env("INGEST_SUMMARY_SECTION_CHARS", DEFAULT_SECTION_CHARS))


def section_overlap_from_env() -> int:
    """Overlap (chars) carried between adjacent sections."""
    return max(0, _int_from_env("INGEST_SUMMARY_SECTION_OVERLAP", DEFAULT_SECTION_OVERLAP))


def batch_input_cap_from_env() -> int:
    """Single-call input cap (chars) for the batch lane (no silent truncation)."""
    return max(1, _int_from_env("INGEST_SUMMARY_BATCH_INPUT_CAP_CHARS", DEFAULT_BATCH_INPUT_CAP_CHARS))


def truncate_summary_part(text: str, limit: int) -> str:
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    return text[: max(0, limit - 1)].rstrip() + "..."


def _action_item_text(item: ActionItem) -> str:
    """Flatten an action item to one line for the rendered brief.

    A bare-string action item (who/due unset) renders as just `what`, so
    `content_text` is byte-for-byte unchanged from the legacy `list[str]` schema;
    owner/due are appended only when the model actually supplied them.
    """
    parts: list[str] = []
    if item.who:
        parts.append(f"{item.who}:")
    parts.append(item.what)
    if item.due:
        parts.append(f"(due {item.due})")
    return " ".join(parts)


def render_summary(out: DocumentSummarySchema, *, max_chars: int) -> str:
    action_lines = [_action_item_text(item) for item in out.action_items]
    lines: list[str] = [truncate_summary_part(out.summary, max_chars)]
    for label, values in (
        ("Key points", out.key_points),
        ("Decisions", out.decisions),
        ("Actions", action_lines),
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


# ---------------------------------------------------------------------------
# Map-reduce over very large documents (docs/plans/document-memory-substrate.md §3.2)
# ---------------------------------------------------------------------------


def split_into_sections(
    text: str,
    *,
    section_chars: int,
    overlap: int,
) -> list[str]:
    """Split ``text`` into char-bounded sections with overlap.

    Each section is up to ``section_chars`` long; consecutive sections share the
    trailing ``overlap`` characters so a fact straddling a boundary still lands
    whole inside at least one section. Returns ``[text]`` when no split is needed.
    """
    if section_chars <= 0:
        return [text]
    if len(text) <= section_chars:
        return [text]
    overlap = max(0, min(overlap, section_chars - 1))
    step = max(1, section_chars - overlap)
    sections: list[str] = []
    start = 0
    while start < len(text):
        sections.append(text[start : start + section_chars])
        if start + section_chars >= len(text):
            break
        start += step
    return sections


def _action_item_key(item: dict[str, Any] | ActionItem | str) -> str:
    if isinstance(item, ActionItem):
        who, what, due = item.who, item.what, item.due
    elif isinstance(item, dict):
        who, what, due = item.get("who"), item.get("what", ""), item.get("due")
    else:  # bare string
        who, what, due = None, str(item), None
    return " ".join(
        " ".join(str(part or "").split()).lower()
        for part in (who, what, due)
    ).strip()


def _dedup_strings(values: list[Any]) -> list[str]:
    """Concat + dedup a list of string-ish items, preserving first-seen order.

    Case/whitespace-insensitive on the key; the first-seen original is kept.
    """
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = str(value).strip()
        if not text:
            continue
        key = " ".join(text.split()).lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(text)
    return out


def _dedup_action_items(values: list[Any]) -> list[dict[str, Any]]:
    """Concat + dedup action items (each normalized to a {who?, what, due?} dict)."""
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for value in values:
        coerced = _coerce_action_item(value)
        item = ActionItem.model_validate(coerced)
        if not item.what.strip():
            continue
        key = _action_item_key(item)
        if key in seen:
            continue
        seen.add(key)
        out.append(item.model_dump())
    return out


def merge_partials(partials: list[DocumentSummarySchema]) -> dict[str, list[Any]]:
    """Merge map-step partials by concatenating + deduping each list field.

    The ``summary`` strings are *not* merged here (they are concatenated as the
    reduce-step input); this returns only the deduped list fields, which the
    reduce pass and the final schema both reuse.
    """
    return {
        "key_points": _dedup_strings([v for p in partials for v in p.key_points]),
        "decisions": _dedup_strings([v for p in partials for v in p.decisions]),
        "action_items": _dedup_action_items(
            [v for p in partials for v in p.action_items]
        ),
        "risks": _dedup_strings([v for p in partials for v in p.risks]),
    }


def _build_map_prompt(
    section: str,
    *,
    metadata: dict[str, Any],
    section_index: int,
    section_count: int,
    max_chars: int,
) -> tuple[str, str]:
    system, user = build_summary_prompt(section, metadata=metadata, max_chars=max_chars)
    user = (
        f"This is section {section_index + 1} of {section_count} of a larger "
        "document. Summarize ONLY this section; do not invent content from other "
        "sections.\n\n" + user
    )
    return system, user


def _build_reduce_prompt(
    merged: dict[str, list[Any]],
    section_summaries: list[str],
    *,
    metadata: dict[str, Any],
    max_chars: int,
) -> tuple[str, str]:
    reduce_input = json.dumps(
        {
            "section_summaries": section_summaries,
            "key_points": merged["key_points"],
            "decisions": merged["decisions"],
            "action_items": merged["action_items"],
            "risks": merged["risks"],
        },
        ensure_ascii=False,
    )
    system, _ = build_summary_prompt("", metadata=metadata, max_chars=max_chars)
    title = metadata.get("title") or metadata.get("name") or "document"
    source_channel = metadata.get("source_channel") or "unknown"
    user = (
        f"Source channel: {source_channel}\n"
        f"Title: {title}\n\n"
        "Below are per-section summaries and pre-merged structured extractions of "
        "a large document. Reduce them into ONE consolidated JSON object with "
        "summary, key_points, decisions, action_items, and risks. De-duplicate, "
        "resolve overlaps, and keep owners/dates on commitments. Do not invent "
        f"facts. Keep the rendered brief under about {max_chars} characters.\n\n"
        "Per-section material (JSON):\n"
        f"{reduce_input}"
    )
    return system, user


async def summarize_mapreduce(
    source_text: str,
    metadata: dict[str, Any],
    *,
    provider: LLMProvider,
    max_chars: int,
    model: str | None = None,
    section_chars: int | None = None,
    overlap: int | None = None,
    max_tokens: int = DEFAULT_SUMMARY_MAX_TOKENS,
) -> SummaryResult:
    """Summarize a large document via map-reduce.

    MAP: split ``source_text`` into overlapping sections, summarize each into a
    partial ``DocumentSummarySchema``. REDUCE: merge the partials' list fields
    (concat + dedup) and run one final reduce pass into a single schema.

    Callers should only invoke this when ``len(source_text)`` exceeds the
    map-reduce threshold; for shorter text the single-call path is unchanged.
    """
    section_chars = section_chars if section_chars is not None else section_chars_from_env()
    overlap = overlap if overlap is not None else section_overlap_from_env()
    sections = split_into_sections(
        source_text, section_chars=section_chars, overlap=overlap
    )

    # MAP
    partials: list[DocumentSummarySchema] = []
    for index, section in enumerate(sections):
        system, user = _build_map_prompt(
            section,
            metadata=metadata,
            section_index=index,
            section_count=len(sections),
            max_chars=max_chars,
        )
        partial = await provider.structured(
            system=system,
            user=user,
            schema=DocumentSummarySchema,
            temperature=0.0,
            max_tokens=max_tokens,
        )
        partials.append(partial)

    log.info(
        "summarization.mapreduce",
        extra={
            "source_chars": len(source_text),
            "section_count": len(sections),
            "section_chars": section_chars,
            "section_overlap": overlap,
        },
    )
    # Observability (document-memory substrate §7 step 12): record the section
    # fan-out distribution for map-reduced documents. This is the only site that
    # knows the section count (it is not surfaced on SummaryResult).
    DOC_MEMORY_MAPREDUCE_SECTIONS.observe(len(sections))

    if len(partials) == 1:
        out = partials[0]
        return SummaryResult(
            summary_text=render_summary(out, max_chars=max_chars),
            model=model,
            structured=out.model_dump(),
        )

    # REDUCE
    merged = merge_partials(partials)
    system, user = _build_reduce_prompt(
        merged,
        [p.summary for p in partials],
        metadata=metadata,
        max_chars=max_chars,
    )
    reduced = await provider.structured(
        system=system,
        user=user,
        schema=DocumentSummarySchema,
        temperature=0.0,
        max_tokens=max_tokens,
    )
    return SummaryResult(
        summary_text=render_summary(reduced, max_chars=max_chars),
        model=model,
        structured=reduced.model_dump(),
    )


class LLMSummarizer:
    def __init__(
        self,
        provider: LLMProvider,
        *,
        max_chars: int,
        mapreduce_chars: int | None = None,
    ) -> None:
        self._provider = provider
        self._max_chars = max_chars
        self._mapreduce_chars = (
            mapreduce_chars if mapreduce_chars is not None else mapreduce_threshold_from_env()
        )

    @property
    def model_name(self) -> str | None:
        return getattr(self._provider.config, "model", None)

    async def summarize(
        self,
        text: str,
        *,
        metadata: dict[str, Any],
    ) -> SummaryResult:
        # Engage map-reduce only for very large documents; below the threshold the
        # legacy single LLM call is unchanged (docs/plans/document-memory-substrate.md §3.2).
        if len(text) > self._mapreduce_chars:
            return await summarize_mapreduce(
                text,
                metadata,
                provider=self._provider,
                max_chars=self._max_chars,
                model=self.model_name,
            )
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
    "DEFAULT_BATCH_INPUT_CAP_CHARS",
    "DEFAULT_MAPREDUCE_CHARS",
    "DEFAULT_SECTION_CHARS",
    "DEFAULT_SECTION_OVERLAP",
    "DEFAULT_SUMMARIZER_MODEL",
    "DEFAULT_SUMMARY_MAX_CHARS",
    "DEFAULT_SUMMARY_MAX_TOKENS",
    "ActionItem",
    "DocumentSummarySchema",
    "LLMSummarizer",
    "SummaryResult",
    "Summarizer",
    "batch_input_cap_from_env",
    "build_default_summarizer",
    "build_summary_prompt",
    "mapreduce_threshold_from_env",
    "merge_partials",
    "parse_summary_text",
    "render_summary",
    "section_chars_from_env",
    "section_overlap_from_env",
    "split_into_sections",
    "summarize_mapreduce",
    "summary_limit_from_env",
]
