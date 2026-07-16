"""Deterministic source-coordinate helpers for explicit mention detection."""

from __future__ import annotations

import re


_WORD_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*(?:-[A-Za-z0-9]+)*")
_SLACK_NATIVE_REFERENCE_RE = re.compile(
    r"<(?:@[A-Z0-9]+|#[A-Z0-9]+)(?:\|[^>\r\n]+)?>"
)
_DEFINITE_ENTITY_NOUNS = frozenset({
    "account",
    "candidate",
    "contract",
    "customer",
    "deal",
    "incident",
    "issue",
    "launch",
    "migration",
    "project",
    "renewal",
    "role",
    "service",
    "team",
    "workflow",
})
_DEFINITE_ENTITY_RE = re.compile(
    rf"\bthe\s+(?:{'|'.join(sorted(_DEFINITE_ENTITY_NOUNS))})\b",
    flags=re.IGNORECASE,
)
_LEADING_SENTENCE_WORDS = frozenset({
    "a",
    "an",
    "fyi",
    "i",
    "our",
    "please",
    "that",
    "the",
    "this",
    "we",
})


def extract_bootstrap_mention_opportunities(
    content_text: str,
    *,
    max_opportunities: int = 50,
) -> tuple[str, ...]:
    """Return bounded, exact source surfaces worth contextual mention analysis.

    This is deliberately a small bootstrap locator, not an entity classifier.
    It finds maximal proper-name/acronym/hyphen runs and a bounded vocabulary
    of Slack-style definite references. Identity lookup happens later and must
    not determine whether an observed source surface receives a mention fate.
    """

    if not content_text or max_opportunities <= 0:
        return ()

    candidates: list[tuple[int, int]] = [
        match.span() for match in _SLACK_NATIVE_REFERENCE_RE.finditer(content_text)
    ]
    candidates.extend(
        match.span() for match in _DEFINITE_ENTITY_RE.finditer(content_text)
    )
    words = list(_WORD_RE.finditer(content_text))
    index = 0
    while index < len(words):
        if not _is_proper_acronym_or_hyphen(words[index].group(0)):
            index += 1
            continue
        end = index + 1
        while (
            end < len(words)
            and content_text[words[end - 1].end() : words[end].start()].isspace()
            and _is_proper_acronym_or_hyphen(words[end].group(0))
        ):
            end += 1

        start = index
        while (
            start < end
            and words[start].group(0).casefold() in _LEADING_SENTENCE_WORDS
        ):
            start += 1
        if start < end:
            candidates.append((words[start].start(), words[end - 1].end()))
        index = end

    # Prefer the largest source span when candidate families overlap, then
    # restore source order for stable downstream work scheduling.
    selected: list[tuple[int, int]] = []
    for start, end in sorted(
        candidates,
        key=lambda span: (-(span[1] - span[0]), span[0], span[1]),
    ):
        if any(start < chosen_end and chosen_start < end for chosen_start, chosen_end in selected):
            continue
        selected.append((start, end))
    selected.sort()

    opportunities: list[str] = []
    seen: set[str] = set()
    for start, end in selected:
        surface = content_text[start:end]
        normalized = " ".join(surface.casefold().split())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        opportunities.append(surface)
        if len(opportunities) >= max_opportunities:
            break
    return tuple(opportunities)


def _is_proper_acronym_or_hyphen(token: str) -> bool:
    letters = [character for character in token if character.isalpha()]
    if not letters:
        return False
    if "-" in token:
        return True
    if len(letters) >= 2 and all(character.isupper() for character in letters):
        return True
    return token[0].isupper()


def locate_explicit_surface_spans(
    content_text: str,
    candidate_surface: str,
) -> tuple[tuple[int, int], ...]:
    """Locate full-token, case-insensitive surface occurrences exactly."""

    tokens = candidate_surface.split()
    if not tokens:
        return ()
    body = r"\s+".join(re.escape(token) for token in tokens)
    if tokens[0][0].isalnum():
        body = rf"(?<!\w){body}"
    if tokens[-1][-1].isalnum():
        body = rf"{body}(?!\w)"
    return tuple(
        (match.start(), match.end())
        for match in re.finditer(body, content_text, flags=re.IGNORECASE)
    )


__all__ = [
    "extract_bootstrap_mention_opportunities",
    "locate_explicit_surface_spans",
]
