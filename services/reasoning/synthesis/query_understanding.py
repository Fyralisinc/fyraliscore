"""Lightweight query understanding helpers shared by product readers.

These helpers stay deterministic and domain-neutral. They are meant to
surface structural handles in user questions before retrieval, not to
answer benchmark-specific prompts.
"""
from __future__ import annotations

import re


_CHOICE_LINE_RE = re.compile(
    r"(?m)^\s*(?:[-*]\s*)?([A-Z])[\.)]\s+(.+?)\s*$"
)
_BACKTICK_RE = re.compile(r"`([^`]{2,120})`")
_QUOTED_RE = re.compile(r'"([^"\n]{2,120})"')
_BETWEEN_RE = re.compile(
    r"\bbetween\s+(.+?)\s+and\s+(.+?)(?:[?.;,]|\s+(?:for|in|on|with|when|where|which|who|why|how)\b|$)",
    flags=re.IGNORECASE,
)
_WHETHER_RE = re.compile(
    r"\bwhether\s+(.+?)\s+or\s+(.+?)(?:[?.;,]|\s+(?:for|in|on|with|when|where|which|who|why|how)\b|$)",
    flags=re.IGNORECASE,
)


def extract_query_alternatives(
    query: str,
    *,
    limit: int = 12,
    include_quoted: bool | None = None,
) -> tuple[str, ...]:
    """Return explicit alternatives/options named in a user question.

    The output is conservative: only reasonably short labels are kept,
    and duplicates are removed case-insensitively while preserving
    first-seen order.
    """

    alternatives: list[str] = []
    include_quoted = _has_alternative_frame(query) if include_quoted is None else include_quoted

    def add(raw: str) -> None:
        clean = _clean_alternative(raw)
        if not clean:
            return
        key = clean.casefold()
        if any(existing.casefold() == key for existing in alternatives):
            return
        alternatives.append(clean)

    for match in _CHOICE_LINE_RE.finditer(query or ""):
        label = match.group(2)
        if _looks_like_real_choice_label(label):
            add(label)

    if include_quoted:
        for pattern in (_BACKTICK_RE, _QUOTED_RE):
            for match in pattern.finditer(query or ""):
                add(match.group(1))

    for pattern in (_BETWEEN_RE, _WHETHER_RE):
        for match in pattern.finditer(query or ""):
            add(match.group(1))
            add(match.group(2))

    return tuple(alternatives[: max(0, int(limit))])


def compact_alternative_key(value: str) -> str:
    """Stable compact key used to bridge spacing/punctuation variants."""

    return re.sub(r"[^a-z0-9]", "", str(value or "").casefold())


def alternative_terms(value: str) -> tuple[str, ...]:
    """Search terms for one alternative label."""

    clean = _clean_alternative(value)
    if not clean:
        return ()
    folded = re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", clean.casefold())).strip()
    compact = compact_alternative_key(clean)
    terms: list[str] = [folded]
    if compact and " " in folded:
        terms.append(compact)
    for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", folded):
        if token not in terms:
            terms.append(token)
    return tuple(term for term in terms if len(term) >= 3)


def _clean_alternative(raw: str) -> str:
    clean = " ".join(str(raw or "").strip().split())
    clean = clean.strip(" .,:;!?\"'")
    if not clean:
        return ""
    if len(clean) > 120:
        return ""
    if len(clean) < 2:
        return ""
    return clean


def _looks_like_real_choice_label(label: str) -> bool:
    clean = _clean_alternative(label)
    if not clean:
        return False
    lowered = clean.casefold()
    return lowered not in {
        "yes",
        "no",
        "none",
        "n/a",
    } or len(clean.split()) > 1


def _has_alternative_frame(query: str) -> bool:
    text = str(query or "").casefold()
    if len(list(_CHOICE_LINE_RE.finditer(query or ""))) >= 2:
        return True
    return any(
        marker in text
        for marker in (
            " between ",
            " compare ",
            " compared ",
            " comparing ",
            " versus ",
            " vs ",
            " whether ",
            " which of ",
            " which one ",
            " which option ",
            " which page has the largest",
            " which page has the least",
            " which page has the most",
            " which page has the fewest",
            " largest number of",
            " least number of",
            " most number of",
            " fewest number of",
        )
    )


__all__ = [
    "alternative_terms",
    "compact_alternative_key",
    "extract_query_alternatives",
]
