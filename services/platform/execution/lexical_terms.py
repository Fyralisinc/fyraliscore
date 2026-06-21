"""Lexical term extraction helpers for inquiry retrieval."""

from __future__ import annotations

import re

from services.reasoning.retrieval.primary import TriggerContext

SPARSE_STRONG_SINGLE_MATCH_MAX_DF = 32

_RELEVANCE_STOPWORDS = {
    "about",
    "after",
    "also",
    "and",
    "are",
    "around",
    "because",
    "been",
    "before",
    "case",
    "company",
    "context",
    "from",
    "has",
    "have",
    "into",
    "need",
    "needs",
    "now",
    "only",
    "signal",
    "that",
    "the",
    "their",
    "there",
    "this",
    "today",
    "with",
    "without",
}

_HYBRID_LOOKUP_GENERIC_TERMS = {
    "accountable",
    "active",
    "alternate",
    "assigned",
    "block",
    "blocked",
    "blocking",
    "blocker",
    "caused",
    "commitment",
    "constraint",
    "counterevidence",
    "customer",
    "dependency",
    "evidence",
    "explanation",
    "goal",
    "impact",
    "issue",
    "launch",
    "model",
    "models",
    "observation",
    "observations",
    "owner",
    "owned",
    "owns",
    "pattern",
    "recent",
    "recurring",
    "related",
    "repeated",
    "resource",
    "responsible",
    "risk",
    "similar",
    "status",
}

_FOCUSED_INDEX_EXTRA_STOPWORDS = {
    "accountable",
    "active",
    "assigned",
    "before",
    "block",
    "blocked",
    "blocking",
    "blocker",
    "currently",
    "critical",
    "evidence",
    "existing",
    "found",
    "issue",
    "likely",
    "matching",
    "model",
    "models",
    "next",
    "owner",
    "owns",
    "path",
    "question",
    "recent",
    "related",
    "responsible",
    "risk",
    "same",
    "specific",
    "stable",
    "showing",
    "currently",
    "today",
    "whether",
    "which",
    "who",
    "what",
    "where",
    "when",
    "why",
    "how",
}


def _trigger_text(trigger: TriggerContext) -> str:
    return (trigger.seed_natural_text or "").strip()


def relevance_tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9][a-z0-9_-]{2,}", str(text).casefold())
        if token not in _RELEVANCE_STOPWORDS and not token.isdigit()
    }


def focused_index_terms(
    question_text: str,
    trigger: TriggerContext,
    *,
    max_terms: int,
) -> list[str]:
    max_terms = max(1, int(max_terms))
    combined = f"{question_text}\n{_trigger_text(trigger)}"
    terms: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        clean = " ".join(str(value or "").strip(" '\"`.,;:()[]{}").split())
        if not clean:
            return
        tokens = focused_material_tokens(clean)
        if not tokens:
            return
        normalized = " ".join(tokens[:4])
        if normalized in seen:
            return
        seen.add(normalized)
        terms.append(normalized)

    for quoted in re.findall(r"['\"]([^'\"]{4,100})['\"]", combined):
        add(quoted)

    for match in re.finditer(
        r"\b(?:[A-Z][A-Za-z0-9_-]{2,}|[A-Z]{2,})(?:\s+(?:[A-Z][A-Za-z0-9_-]{2,}|[A-Z]{2,})){1,4}",
        combined,
    ):
        phrase = match.group(0)
        if phrase.casefold().startswith(("who ", "what ", "which ", "does ", "is ")):
            continue
        add(phrase)

    tokens = focused_material_tokens(combined)
    for width in (3, 2):
        for index in range(0, max(0, len(tokens) - width + 1)):
            window = tokens[index : index + width]
            if any(is_focused_strong_token(token) for token in window):
                add(" ".join(window))
            if len(terms) >= max_terms:
                return terms[:max_terms]
    for token in tokens:
        if is_focused_strong_token(token):
            add(token)
        if len(terms) >= max_terms:
            break
    return terms[:max_terms]


def focused_material_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    seen: set[str] = set()
    for raw in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{2,}", str(text)):
        token = raw.casefold()
        if (
            token in _RELEVANCE_STOPWORDS
            or token in _FOCUSED_INDEX_EXTRA_STOPWORDS
            or token.isdigit()
        ):
            continue
        if len(token) < 4 and not raw.isupper():
            continue
        if token in seen:
            continue
        seen.add(token)
        tokens.append(token)
    return tokens


def is_focused_strong_token(token: str) -> bool:
    value = str(token or "")
    return (
        len(value) >= 6
        or "-" in value
        or "_" in value
        or any(ch.isdigit() for ch in value)
    )


def focused_index_lookup_groups(
    terms: list[str] | tuple[str, ...],
) -> list[list[str]]:
    groups = hybrid_sparse_lookup_groups(terms)
    if groups:
        return groups
    out: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for token in focused_material_tokens(" ".join(str(t) for t in terms)):
        if not is_focused_strong_token(token):
            continue
        key = (token,)
        if key in seen:
            continue
        seen.add(key)
        out.append([token])
        if len(out) >= 8:
            break
    return out


def hybrid_lexical_terms(
    query_text: str,
    trigger: TriggerContext,
    *,
    max_terms: int,
) -> list[str]:
    max_terms = max(1, int(max_terms))
    strong: list[str] = []
    weak: list[str] = []

    def add(raw_text: str, *, trigger_side: bool = False) -> None:
        for raw in re.findall(r"[A-Za-z0-9][A-Za-z0-9_-]{2,}", str(raw_text)):
            token = raw.casefold()
            if token in _RELEVANCE_STOPWORDS or token.isdigit():
                continue
            has_symbol = (
                "-" in token or "_" in token or any(ch.isdigit() for ch in token)
            )
            is_acronym = (
                len(raw) <= 6 and raw.upper() == raw and any(ch.isalpha() for ch in raw)
            )
            is_strong = (
                has_symbol or is_acronym or len(token) >= (5 if trigger_side else 4)
            )
            target = strong if is_strong else weak
            if token not in strong and token not in weak:
                target.append(token)

    add(query_text, trigger_side=False)
    add(_trigger_text(trigger), trigger_side=True)
    return (strong + weak)[:max_terms]


def like_patterns_for_terms(terms: list[str] | tuple[str, ...]) -> list[str]:
    patterns: list[str] = []
    for term in terms:
        value = str(term or "").casefold().strip()
        if not value:
            continue
        escaped = value.replace("!", "!!").replace("%", "!%").replace("_", "!_")
        pattern = f"%{escaped}%"
        if pattern not in patterns:
            patterns.append(pattern)
    return patterns


def hybrid_lookup_terms(
    terms: list[str] | tuple[str, ...],
    *,
    max_terms: int = 8,
) -> list[str]:
    out: list[str] = []
    max_terms = max(1, int(max_terms))
    for raw in terms:
        for token in re.findall(
            r"[a-z0-9][a-z0-9_-]{2,}",
            str(raw or "").casefold(),
        ):
            if token in _RELEVANCE_STOPWORDS or token.isdigit():
                continue
            symbol_specific = (
                "-" in token or "_" in token or any(ch.isdigit() for ch in token)
            )
            if token in _HYBRID_LOOKUP_GENERIC_TERMS and not symbol_specific:
                continue
            if len(token) < 4 and not symbol_specific:
                continue
            if token not in out:
                out.append(token)
                if len(out) >= max_terms:
                    return out
    return out[:max_terms]


def hybrid_sparse_lookup_terms(terms: list[str] | tuple[str, ...]) -> list[str]:
    return hybrid_lookup_terms(terms, max_terms=8)


def hybrid_sparse_strong_single_match_terms(terms: list[str]) -> list[str]:
    return [
        term
        for term in terms
        if len(term) >= 4 and any(ch.isdigit() or ch in {"-", "_"} for ch in term)
    ]


def hybrid_sparse_lookup_groups(terms: list[str] | tuple[str, ...]) -> list[list[str]]:
    out: list[list[str]] = []
    seen: set[tuple[str, ...]] = set()
    for raw in terms:
        tokens = [
            token
            for token in re.findall(
                r"[a-z0-9][a-z0-9_-]{2,}", str(raw or "").casefold()
            )
            if token not in _RELEVANCE_STOPWORDS and not token.isdigit()
        ]
        tokens = list(dict.fromkeys(tokens))
        if len(tokens) >= 2:
            group = tokens[:4]
        elif tokens and len(tokens[0]) >= 6:
            group = tokens
        else:
            continue
        key = tuple(group)
        if key in seen:
            continue
        seen.add(key)
        out.append(group)
        if len(out) >= 8:
            break
    return out


__all__ = [
    "SPARSE_STRONG_SINGLE_MATCH_MAX_DF",
    "focused_index_lookup_groups",
    "focused_index_terms",
    "focused_material_tokens",
    "hybrid_lexical_terms",
    "hybrid_lookup_terms",
    "hybrid_sparse_lookup_groups",
    "hybrid_sparse_lookup_terms",
    "hybrid_sparse_strong_single_match_terms",
    "is_focused_strong_token",
    "like_patterns_for_terms",
    "relevance_tokens",
]
