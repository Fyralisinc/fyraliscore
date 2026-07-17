"""Deterministic source-coordinate helpers for explicit mention detection."""

from __future__ import annotations

import re


_UNICODE_LETTER_RE = r"[^\W\d_]"
_UNICODE_ALNUM_OR_MARK_RE = (
    r"(?:[^\W_]|[\u0300-\u036f\u1ab0-\u1aff\u1dc0-\u1dff"
    r"\u20d0-\u20ff\ufe20-\ufe2f])"
)
_WORD_RE = re.compile(
    rf"{_UNICODE_LETTER_RE}{_UNICODE_ALNUM_OR_MARK_RE}*"
    rf"(?:(?:[-‐‑]{_UNICODE_ALNUM_OR_MARK_RE}+)|"
    rf"(?:['’]{_UNICODE_LETTER_RE}{_UNICODE_ALNUM_OR_MARK_RE}+)|"
    rf"(?:\.{_UNICODE_ALNUM_OR_MARK_RE}+))*"
    r"(?:['’]s)?"
)
_DOTTED_ACRONYM_RE = re.compile(
    r"(?<!\w)(?:[A-Z]\.){2,}[A-Z]?(?:\.)?(?!\w)"
)
_SLACK_NATIVE_REFERENCE_RE = re.compile(
    r"<(?:@[A-Z0-9]+|#[A-Z0-9]+|!subteam\^[A-Z0-9]+)(?:\|[^>\r\n]+)?>"
)
_SLACK_PLAIN_REFERENCE_RE = re.compile(
    r"(?<![\w@#])(?:@(?P<user>[A-Za-z][\w.-]{1,63})|"
    r"#(?P<channel>[A-Za-z][\w-]{1,79}))(?!\w)"
)
_EXPLICIT_IDENTIFIER_RE = re.compile(
    r"(?<![\w-])(?:[A-Z][A-Z0-9]{0,15}-\d{1,12}|"
    r"[A-Z]{2,12}_\d{1,12})(?![\w-])"
)
_UPPER_TOKEN_RE = r"[A-ZÀ-ÖØ-ÞĀ-ŽΑ-ΩА-Я][\w'’.-]*"
_AMPERSAND_NAME_RE = re.compile(
    rf"(?<!\w){_UPPER_TOKEN_RE}\s*&\s*{_UPPER_TOKEN_RE}"
    rf"(?:\s+{_UPPER_TOKEN_RE}){{0,3}}"
)
_PROPER_NAMESPACE_SUFFIX_RE = re.compile(
    r"(?<=\.)[A-Z][A-Za-z0-9]*(?:\.[A-Za-z0-9]+)+"
)
_INLINE_CODE_RE = re.compile(r"`[^`\r\n]*`")
_JSON_OBJECT_RE = re.compile(r"\{[^{}\r\n]*\}")
_DOCUMENT_HEADING_RE = re.compile(
    r"^\s*(?:title|subject|acceptance criteria|sprint goal|"
    r"(?:expected|actual) result|current state)\s*:",
    flags=re.IGNORECASE,
)
_CJK_RUN_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff]{2,}")
_DEFINITE_ENTITY_NOUNS = frozenset({
    "account",
    "candidate",
    "contract",
    "customer",
    "decision",
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
_QUOTED_ENTITY_NAME_RE = re.compile(
    rf"\b(?:{'|'.join(sorted(_DEFINITE_ENTITY_NOUNS))})\s+"
    rf"(?:['\"‘“])(?P<surface>{_UNICODE_ALNUM_OR_MARK_RE}"
    rf"+(?:[ .&/_-]{_UNICODE_ALNUM_OR_MARK_RE}+){{0,5}})(?:['\"’”])",
    flags=re.IGNORECASE,
)
_QUOTED_ENTITY_CUE_RE = re.compile(
    rf"\b(?:{'|'.join(sorted(_DEFINITE_ENTITY_NOUNS))})\s+['\"‘“]",
    flags=re.IGNORECASE,
)
_DEFINITE_ENTITY_RE = re.compile(
    rf"\bthe\s+(?:{'|'.join(sorted(_DEFINITE_ENTITY_NOUNS))})\b",
    flags=re.IGNORECASE,
)
_MODIFIED_DEFINITE_ENTITY_RE = re.compile(
    rf"\b(?i:the)\s+[A-Z][\w.-]*\s+"
    rf"(?i:{'|'.join(sorted(_DEFINITE_ENTITY_NOUNS))})\b"
)
_ELLIPTICAL_ENTITY_RE = re.compile(r"\bsame\s+owner\b", flags=re.IGNORECASE)
_LEADING_SENTENCE_WORDS = frozenset({
    "a",
    "an",
    "are",
    "can",
    "could",
    "did",
    "does",
    "fyi",
    "he",
    "has",
    "have",
    "i",
    "it",
    "is",
    "our",
    "please",
    "she",
    "someone",
    "that",
    "the",
    "they",
    "this",
    "we",
    "was",
    "were",
    "will",
    "would",
    "should",
    "you",
})
_AMBIGUOUS_SINGLETONS = frozenset({
    "account",
    "also",
    "ask",
    "blocked",
    "cc",
    "child",
    "customer",
    "decision",
    "do",
    "done",
    "escalate",
    "external",
    "fix",
    "forwarded",
    "from",
    "hello",
    "hi",
    "incident",
    "jira",
    "legal",
    "meet",
    "monday",
    "next",
    "nested",
    "new",
    "no",
    "note",
    "owner",
    "project",
    "resolved",
    "route",
    "routine",
    "said",
    "ship",
    "someone",
    "thanks",
    "title",
    "today",
    "tomorrow",
    "update",
    "yesterday",
    "friday",
    "quoted",
    "re",
    "release",
    "subject",
})
_NAME_CONNECTORS = frozenset({"of", "the"})
_ROLE_LEADS = frozenset({"CEO", "CFO", "CMO", "COO", "CPO", "CRO", "CTO", "EVP", "SVP", "VP"})
_ROLE_FUNCTIONS = frozenset({
    "customer",
    "engineering",
    "finance",
    "legal",
    "marketing",
    "operations",
    "people",
    "product",
    "revenue",
    "sales",
    "security",
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
    for match in _SLACK_PLAIN_REFERENCE_RE.finditer(content_text):
        candidates.append(
            match.span("user") if match.group("user") is not None else match.span("channel")
        )
    candidates.extend(
        match.span() for match in _DOTTED_ACRONYM_RE.finditer(content_text)
    )
    candidates.extend(
        match.span() for match in _EXPLICIT_IDENTIFIER_RE.finditer(content_text)
    )
    candidates.extend(
        match.span()
        for match in _AMPERSAND_NAME_RE.finditer(content_text)
        if _is_proper_ampersand_name(match.group(0))
    )
    candidates.extend(
        match.span() for match in _PROPER_NAMESPACE_SUFFIX_RE.finditer(content_text)
    )
    candidates.extend(match.span() for match in _CJK_RUN_RE.finditer(content_text))
    candidates.extend(
        match.span()
        for match in _DEFINITE_ENTITY_RE.finditer(content_text)
        if _keep_definite_reference(content_text, match.start(), match.end())
    )
    for match in _MODIFIED_DEFINITE_ENTITY_RE.finditer(content_text):
        # A title-cased noun phrase is already an explicit proper-name run;
        # its determiner is not part of the name. Acronym-modified lowercase
        # references such as ``The API team`` retain the determiner because
        # the whole phrase is contextual.
        if content_text[match.start() : match.end()].rsplit(maxsplit=1)[-1][0].isupper():
            continue
        candidates.append(match.span())
    candidates.extend(match.span() for match in _ELLIPTICAL_ENTITY_RE.finditer(content_text))
    candidates.extend(
        match.span("surface") for match in _QUOTED_ENTITY_NAME_RE.finditer(content_text)
    )
    candidates.extend(_quoted_entity_name_spans(content_text))
    words = list(_WORD_RE.finditer(content_text))
    index = 0
    while index < len(words):
        if not _is_proper_acronym_or_hyphen(words[index].group(0)):
            index += 1
            continue
        if words[index].group(0) in _ROLE_LEADS:
            end = index + 1
            if (
                end < len(words)
                and content_text[words[index].end() : words[end].start()].isspace()
                and words[end].group(0).casefold() in _ROLE_FUNCTIONS
            ):
                end += 1
            candidates.append((words[index].start(), words[end - 1].end()))
            index = end
            continue
        end = index + 1
        while end < len(words):
            separator = content_text[words[end - 1].end() : words[end].start()]
            if not separator.isspace():
                break
            next_token = words[end].group(0)
            if _is_explicit_identifier(next_token):
                break
            if _is_proper_acronym_or_hyphen(next_token):
                end += 1
                continue
            if next_token.casefold() in _NAME_CONNECTORS:
                connector_end = end
                while (
                    connector_end < len(words)
                    and words[connector_end].group(0).casefold() in _NAME_CONNECTORS
                    and content_text[
                        words[connector_end - 1].end() : words[connector_end].start()
                    ].isspace()
                ):
                    connector_end += 1
                if (
                    connector_end < len(words)
                    and content_text[
                        words[connector_end - 1].end() : words[connector_end].start()
                    ].isspace()
                    and _is_proper_acronym_or_hyphen(words[connector_end].group(0))
                ):
                    end = connector_end + 1
                    continue
            break

        start = index
        while (
            start < end
            and words[start].group(0).casefold() in _LEADING_SENTENCE_WORDS
        ):
            start += 1
        run_tokens = (
            words[position]
            .group(0)
            .casefold()
            .removesuffix("'s")
            .removesuffix("’s")
            for position in range(start, end)
        )
        if start < end and not all(
            token in _AMBIGUOUS_SINGLETONS or token in _NAME_CONNECTORS
            for token in run_tokens
        ):
            candidates.append((words[start].start(), words[end - 1].end()))
        index = end

    excluded = tuple(match.span() for match in _INLINE_CODE_RE.finditer(content_text))
    excluded += tuple(match.span() for match in _JSON_OBJECT_RE.finditer(content_text))
    heading = _DOCUMENT_HEADING_RE.match(content_text)
    candidates = [
        span
        for span in candidates
        if not any(_spans_overlap(span, blocked) for blocked in excluded)
        and not _is_explicit_non_entity_context(content_text, *span)
        and not (
            heading is not None
            and not _is_explicit_identifier(content_text[span[0] : span[1]])
        )
    ]

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


def _is_explicit_identifier(token: str) -> bool:
    return _EXPLICIT_IDENTIFIER_RE.fullmatch(token) is not None


def _keep_definite_reference(content_text: str, start: int, end: int) -> bool:
    before = content_text[:start]
    previous = list(_WORD_RE.finditer(before))
    if previous:
        token = previous[-1]
        if before[token.end() :].isspace() and _is_proper_acronym_or_hyphen(token.group(0)):
            return False
    following = content_text[end:]
    # A definite common noun followed by a complement names the relationship
    # to the complemented entity, not a second entity surface.  For example,
    # ``the decision for Orion`` and ``the contract with Acme`` should leave
    # Orion/Acme as the mention opportunity rather than manufacture an
    # identity-bearing ``the decision``/``the contract`` candidate.  Keep
    # bare anaphoric references (``the decision is blocked``), which can be
    # resolved from source topology or contextual evidence downstream.
    if re.match(
        r"\s+(?:about|for|of|on|regarding|with)\b",
        following,
        flags=re.IGNORECASE,
    ):
        return False
    wrapper = re.match(r"\s+(program|initiative|effort)\b", following, flags=re.IGNORECASE)
    return wrapper is None


def _quoted_entity_name_spans(content_text: str) -> tuple[tuple[int, int], ...]:
    closing = {'"': '"', "'": "'", "‘": "’", "“": "”"}
    spans: list[tuple[int, int]] = []
    for match in _QUOTED_ENTITY_CUE_RE.finditer(content_text):
        opener = content_text[match.end() - 1]
        end = content_text.find(closing[opener], match.end())
        if end < 0 or end - match.end() > 100:
            continue
        start = match.end()
        while start < end and content_text[start].isspace():
            start += 1
        while end > start and content_text[end - 1].isspace():
            end -= 1
        if start < end:
            spans.append((start, end))
    return tuple(spans)


def _is_proper_ampersand_name(surface: str) -> bool:
    components = [part.strip() for part in surface.split("&", maxsplit=1)]
    return len(components) == 2 and all(
        part and any(token[0].isupper() for token in part.split() if token)
        for part in components
    )


def _spans_overlap(left: tuple[int, int], right: tuple[int, int]) -> bool:
    return left[0] < right[1] and right[0] < left[1]


def _is_explicit_non_entity_context(content_text: str, start: int, end: int) -> bool:
    surface = content_text[start:end]
    if _is_explicit_identifier(surface) or surface.startswith(("<@", "<#", "<!")):
        return False
    before = content_text[max(0, start - 40) : start].casefold()
    after = content_text[end : min(len(content_text), end + 100)].casefold()
    whole = content_text.casefold()
    if re.search(r"(?:word|words|phrase|heading|inline code)\s+[\"'‘“`]*$", before):
        return True
    if re.search(r"(?:forwarded message from)\s+$", before):
        return True
    if re.match(
        r"\s*(?:describes\s+(?:an?\s+)?|(?:is|are)\s+(?:only\s+)?(?:an?\s+)?)"
        r"(?:heading|function|ordinary words?|greeting|adjective|section labels?|"
        r"descriptive|literal code|data|transport metadata|closings?)\b",
        after,
    ):
        return True
    if any(
        marker in whole
        for marker in (
            "those are ordinary words",
            "are section labels",
            "are closings",
            "none names an entity",
        )
    ):
        return True
    return False


def _literal_surface_spans(
    content_text: str,
    candidate_surface: str,
) -> tuple[tuple[int, int], ...]:
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
    spans = tuple(
        (match.start(), match.end())
        for match in re.finditer(body, content_text, flags=re.IGNORECASE)
    )
    larger_spans: list[tuple[int, int]] = []
    for other_surface in extract_bootstrap_mention_opportunities(content_text):
        if len(other_surface) <= len(candidate_surface):
            continue
        larger_spans.extend(_literal_surface_spans(content_text, other_surface))
    return tuple(
        span
        for span in spans
        if not any(
            outer_start <= span[0] and span[1] <= outer_end
            for outer_start, outer_end in larger_spans
        )
    )


__all__ = [
    "extract_bootstrap_mention_opportunities",
    "locate_explicit_surface_spans",
]
