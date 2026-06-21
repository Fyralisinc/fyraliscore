"""Deterministic relation-evidence extraction from signal text.

This module preserves explicit predicates before they are compressed into
models. It does not resolve endpoints to Models and never writes `model_edges`;
it creates relation-evidence facts that later resolvers/compilers can use.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Iterable


@dataclass(frozen=True)
class ExtractedRelation:
    subject_text: str
    predicate: str
    object_text: str
    edge_kind_hint: str
    confidence: float
    evidence_text: str


_CONNECTOR = r"\s+(?:the\s+|a\s+|an\s+|our\s+)?"
_TERM = r"(?P<{name}>[A-Za-z0-9][A-Za-z0-9 _/:&+#'()-]{{1,96}}?)"
_END = r"(?=$|[.;,\n])"


_PATTERNS: tuple[tuple[re.Pattern[str], str, bool, float], ...] = (
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:blocks|is blocking|keeps blocking|prevents)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "blocks",
        False,
        0.82,
    ),
    (
        re.compile(
            _TERM.format(name="object")
            + _CONNECTOR
            + r"(?:is blocked by|was blocked by|blocked by|depends on|is gated by)"
            + _CONNECTOR
            + _TERM.format(name="subject")
            + _END,
            re.IGNORECASE,
        ),
        "blocks",
        False,
        0.84,
    ),
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:enables|unblocks|allows)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "enables",
        False,
        0.78,
    ),
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:contradicts|conflicts with)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "contradicts",
        False,
        0.76,
    ),
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:weakens|undermines)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "weakens",
        False,
        0.76,
    ),
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:causes|drives|creates)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "causes",
        False,
        0.74,
    ),
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:explains|accounts for)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "explains",
        False,
        0.74,
    ),
    (
        re.compile(
            _TERM.format(name="subject")
            + _CONNECTOR
            + r"(?:predicts|is an early warning for|warns of)"
            + _CONNECTOR
            + _TERM.format(name="object")
            + _END,
            re.IGNORECASE,
        ),
        "early_warning_for",
        False,
        0.72,
    ),
)


def extract_relation_evidence(text: str, *, limit: int = 8) -> list[ExtractedRelation]:
    """Extract explicit relation predicates from one signal-like text."""
    if not text or not text.strip():
        return []
    out: list[ExtractedRelation] = []
    seen: set[tuple[str, str, str]] = set()
    for pattern, edge_kind, reverse, confidence in _PATTERNS:
        for match in pattern.finditer(text):
            subject = _clean_endpoint(match.group("subject"))
            obj = _clean_endpoint(match.group("object"))
            if reverse:
                subject, obj = obj, subject
            if not _endpoint_ok(subject) or not _endpoint_ok(obj):
                continue
            key = (subject.lower(), edge_kind, obj.lower())
            if key in seen:
                continue
            seen.add(key)
            out.append(
                ExtractedRelation(
                    subject_text=subject,
                    predicate=edge_kind,
                    object_text=obj,
                    edge_kind_hint=edge_kind,
                    confidence=confidence,
                    evidence_text=_sentence_window(text, match.start(), match.end()),
                )
            )
            if len(out) >= limit:
                return out
    return out


def _clean_endpoint(value: str) -> str:
    text = " ".join(str(value or "").strip().split())
    return text.strip(" -:;,.'\"")


def _endpoint_ok(value: str) -> bool:
    if len(value) < 2 or len(value) > 96:
        return False
    lowered = value.lower()
    return not any(
        lowered.startswith(prefix)
        for prefix in (
            "and ",
            "but ",
            "that ",
            "because ",
            "if ",
            "when ",
        )
    )


def _sentence_window(text: str, start: int, end: int) -> str:
    left_candidates: Iterable[int] = (
        text.rfind(".", 0, start),
        text.rfind("\n", 0, start),
        text.rfind(";", 0, start),
    )
    left = max(left_candidates) + 1
    right_candidates = [
        idx for idx in (text.find(".", end), text.find("\n", end), text.find(";", end))
        if idx != -1
    ]
    right = min(right_candidates) if right_candidates else len(text)
    return " ".join(text[left:right].strip().split())


__all__ = ["ExtractedRelation", "extract_relation_evidence"]
