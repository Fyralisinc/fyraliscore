"""Model-level semantic term extraction.

Semantic terms are compact lexical handles for a Model's belief content.
They intentionally exclude scope actors/entities and broad domain tags, which
already live in dedicated columns. Query-time lookup also mirrors them into the
typed model representation feature postings table as the `lexical` feature
family.
"""
from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from typing import Any


MAX_SEMANTIC_TERMS = 24
MAX_TERM_WORDS = 4
MAX_TERM_CHARS = 72

_TOKEN_RE = re.compile(r"[a-z0-9][a-z0-9_'-]*", re.I)
_UUID_RE = re.compile(
    r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
    r"[0-9a-f]{4}-[0-9a-f]{12}\b",
    re.I,
)
_URL_RE = re.compile(r"https?://|www\.", re.I)
_EMAIL_RE = re.compile(r"\S+@\S+")
_HANDLE_RE = re.compile(
    r"\b(?:pr|pull request|issue|ticket)\s*#?\d+\b|\b[A-Z][A-Z0-9]{1,12}-\d+\b",
    re.I,
)
_CAMEL_BOUNDARY_RE = re.compile(r"(?<=[a-z])(?=[A-Z])")
_SPACE_RE = re.compile(r"\s+")

_TEXT_KEYS = {
    "about",
    "assertion",
    "assessment",
    "check",
    "expected",
    "event",
    "nature",
    "object",
    "observed_tendency",
    "open_falsifier",
    "pattern",
    "proposed_change",
    "qualitative_impact",
    "relationship_summary",
    "resolution",
    "shared_mechanism",
    "signature",
    "situation",
    "subject",
    "summary",
    "trigger_conditions",
}
_SKIP_KEYS = {
    "abstraction_level",
    "belief_address",
    "claim_role",
    "contextual_frame",
    "coverage_roles",
    "domain_tags",
    "evidence_event_ids",
    "id",
    "kind",
    "legacy_kind",
    "member_model_ids",
    "modality",
    "model_contract_version",
    "polarity",
    "retrieval_tags",
    "scope_actors",
    "scope_entities",
    "semantic_address",
    "semantic_terms",
    "supporting_event_ids",
    "target_actor_id",
    "time_mode",
}

_STOPWORDS = {
    "a",
    "about",
    "above",
    "after",
    "again",
    "against",
    "all",
    "also",
    "am",
    "an",
    "and",
    "any",
    "are",
    "as",
    "at",
    "be",
    "because",
    "been",
    "being",
    "between",
    "but",
    "by",
    "can",
    "cannot",
    "could",
    "did",
    "do",
    "does",
    "doing",
    "done",
    "during",
    "each",
    "for",
    "from",
    "had",
    "has",
    "have",
    "having",
    "if",
    "in",
    "into",
    "is",
    "it",
    "its",
    "may",
    "might",
    "must",
    "not",
    "now",
    "of",
    "on",
    "or",
    "our",
    "over",
    "should",
    "said",
    "say",
    "says",
    "so",
    "than",
    "that",
    "the",
    "their",
    "then",
    "there",
    "these",
    "this",
    "those",
    "to",
    "under",
    "up",
    "was",
    "we",
    "were",
    "when",
    "where",
    "which",
    "while",
    "who",
    "will",
    "with",
    "within",
    "would",
}
_GENERIC_TERMS = {
    "action",
    "actor",
    "actors",
    "business",
    "claim",
    "company",
    "context",
    "derive",
    "durable",
    "customer",
    "customers",
    "data",
    "decision",
    "decisions",
    "entity",
    "event",
    "evidence",
    "fact",
    "goal",
    "goals",
    "issue",
    "memory",
    "model",
    "models",
    "norm",
    "observation",
    "observations",
    "organization",
    "people",
    "person",
    "project",
    "resource",
    "resources",
    "signal",
    "signals",
    "source",
    "system",
    "systems",
    "team",
    "teams",
    "thing",
    "user",
    "belief",
    "window",
    "work",
    "wrapper",
}
_LOW_SIGNAL_SUFFIXES = {"status", "state", "thing", "item", "record", "context"}
_SOURCE_CHANNEL_TERMS = {
    "calendar",
    "discord",
    "doc",
    "docs",
    "email",
    "github",
    "gmail",
    "hubspot",
    "intercom",
    "jira",
    "linear",
    "notion",
    "salesforce",
    "slack",
    "source",
    "thread",
    "ticket",
    "zendesk",
    "zoom",
}
_BATCH_WRAPPER_TERMS = {
    "claim",
    "claims",
    "containing",
    "derive",
    "durable",
    "individual",
    "only",
    "signal",
    "signals",
    "source",
    "window",
    "wrapper",
}


def derive_semantic_terms(
    *,
    natural: str,
    proposition: Mapping[str, Any] | None = None,
    falsifier: Mapping[str, Any] | None = None,
    resolution_criteria: Mapping[str, Any] | None = None,
    scope_entities: Sequence[Mapping[str, Any]] = (),
    domain_tags: Sequence[str] = (),
    suggested_terms: Sequence[str] = (),
    limit: int = MAX_SEMANTIC_TERMS,
) -> list[str]:
    """Return a bounded, normalized lexical signature for one Model."""
    texts = [natural]
    texts.extend(_iter_text_values(proposition or {}))
    texts.extend(_iter_text_values(falsifier or {}))
    texts.extend(_iter_text_values(resolution_criteria or {}))
    source_text = " ".join(text for text in texts if text)
    normalized_source = _normalize_text(source_text)
    domain_exclusions = {_normalize_term(tag) for tag in domain_tags if tag}
    entity_exclusions = _entity_exclusions(scope_entities)

    scores: Counter[str] = Counter()
    for raw in suggested_terms:
        term = _normalize_term(raw)
        if _term_allowed(
            term,
            normalized_source=normalized_source,
            domain_exclusions=domain_exclusions,
            entity_exclusions=entity_exclusions,
        ):
            scores[term] += 30 + _specificity_score(term)

    for term in _candidate_terms_from_text(source_text):
        if _term_allowed(
            term,
            normalized_source=normalized_source,
            domain_exclusions=domain_exclusions,
            entity_exclusions=entity_exclusions,
        ):
            scores[term] += _specificity_score(term)

    ordered = sorted(scores, key=lambda term: (-scores[term], -len(term), term))
    return _dedupe_subsumed(ordered)[: max(0, int(limit))]


def derive_query_semantic_terms(
    seed_text: str | None,
    *,
    seed_signature: Mapping[str, Any] | None = None,
    limit: int = MAX_SEMANTIC_TERMS,
) -> list[str]:
    """Derive lexical lookup terms from a retrieval seed."""
    parts = [seed_text or ""]
    if seed_signature:
        parts.extend(_iter_text_values(seed_signature))
    text = " ".join(part for part in parts if part)
    scores: Counter[str] = Counter()
    for term in _candidate_terms_from_text(text):
        if _term_allowed(
            term,
            normalized_source=_normalize_text(text),
            domain_exclusions=set(),
            entity_exclusions=set(),
        ):
            scores[term] += _specificity_score(term)
    ordered = sorted(scores, key=lambda term: (-scores[term], -len(term), term))
    return _dedupe_subsumed(ordered)[: max(0, int(limit))]


def _iter_text_values(value: Any, *, parent_key: str | None = None) -> Iterable[str]:
    if value is None:
        return
    if isinstance(value, str):
        text = value.strip()
        if text:
            yield text
        return
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            if key_text in _SKIP_KEYS:
                continue
            if key_text in _TEXT_KEYS or isinstance(item, (str, Mapping, list, tuple)):
                yield from _iter_text_values(item, parent_key=key_text)
        return
    if isinstance(value, (list, tuple)):
        if parent_key in _SKIP_KEYS:
            return
        for item in value:
            if isinstance(item, (str, Mapping, list, tuple)):
                yield from _iter_text_values(item, parent_key=parent_key)
        return
    if parent_key in _TEXT_KEYS:
        try:
            yield json.dumps(value, sort_keys=True, default=str)
        except TypeError:
            yield str(value)


def _candidate_terms_from_text(text: str) -> Iterable[str]:
    normalized = _normalize_text(text)
    tokens = _tokens(normalized)
    for width in range(MAX_TERM_WORDS, 1, -1):
        for start in range(0, max(0, len(tokens) - width + 1)):
            gram = tokens[start : start + width]
            if _bad_ngram_edges(gram):
                continue
            yield " ".join(gram)
    for token in tokens:
        if len(token) >= 7:
            yield token


def _term_allowed(
    term: str,
    *,
    normalized_source: str,
    domain_exclusions: set[str],
    entity_exclusions: set[str],
) -> bool:
    if not term or len(term) > MAX_TERM_CHARS:
        return False
    if term in domain_exclusions or term in _GENERIC_TERMS:
        return False
    if _URL_RE.search(term) or _EMAIL_RE.search(term) or _UUID_RE.search(term):
        return False
    if _HANDLE_RE.search(term):
        return False
    words = term.split()
    if not words or len(words) > MAX_TERM_WORDS:
        return False
    if all(word in _STOPWORDS or word in _GENERIC_TERMS for word in words):
        return False
    if words[0] in _STOPWORDS or words[-1] in _STOPWORDS:
        return False
    if words[-1] in _LOW_SIGNAL_SUFFIXES and len(words) <= 2:
        return False
    if any(_is_numeric_or_tiny(word) for word in words):
        return False
    if any(word in _SOURCE_CHANNEL_TERMS for word in words):
        return False
    if all(
        word in _BATCH_WRAPPER_TERMS or word in _STOPWORDS
        for word in words
    ):
        return False
    if any(word in entity_exclusions for word in words):
        return False
    if term in entity_exclusions:
        return False
    if term not in normalized_source and not all(word in normalized_source for word in words):
        return False
    return True


def _specificity_score(term: str) -> int:
    words = term.split()
    score = len(words) * 8
    score += min(len(term), MAX_TERM_CHARS) // 8
    if len(words) >= 2:
        score += 8
    if any(len(word) >= 9 for word in words):
        score += 4
    if any(word.endswith(("ing", "tion", "ment", "ity", "ance", "ence")) for word in words):
        score += 3
    if term in _GENERIC_TERMS:
        score -= 20
    return score


def _dedupe_subsumed(terms: Sequence[str]) -> list[str]:
    out: list[str] = []
    for term in terms:
        if term in out:
            continue
        words = term.split()
        if len(words) == 1 and any(term in existing.split() for existing in out):
            continue
        out.append(term)
    return out


def _bad_ngram_edges(words: Sequence[str]) -> bool:
    if not words:
        return True
    return (
        words[0] in _STOPWORDS
        or words[-1] in _STOPWORDS
        or words[0] in _GENERIC_TERMS
        or words[-1] in _GENERIC_TERMS
    )


def _entity_exclusions(scope_entities: Sequence[Mapping[str, Any]]) -> set[str]:
    exclusions: set[str] = set()
    for entity in scope_entities or ():
        if not isinstance(entity, Mapping):
            continue
        for key in (
            "id",
            "name",
            "label",
            "title",
            "display_name",
            "external_id",
            "source_ref",
            "email",
        ):
            raw = entity.get(key)
            if raw is None:
                continue
            normalized = _normalize_term(raw)
            if not normalized:
                continue
            exclusions.add(normalized)
            exclusions.update(_tokens(normalized))
    return exclusions


def _normalize_text(value: Any) -> str:
    text = _CAMEL_BOUNDARY_RE.sub(" ", str(value or ""))
    text = _UUID_RE.sub(" ", text)
    text = _URL_RE.sub(" ", text)
    text = _EMAIL_RE.sub(" ", text)
    text = text.replace("_", " ").casefold()
    text = re.sub(r"[^a-z0-9\s'-]+", " ", text)
    return _SPACE_RE.sub(" ", text).strip()


def _normalize_term(value: Any) -> str:
    text = _normalize_text(value)
    return " ".join(_tokens(text))


def _tokens(text: str) -> list[str]:
    tokens: list[str] = []
    for match in _TOKEN_RE.finditer(text):
        token = match.group(0).strip("'-_").casefold()
        if not token or token in _STOPWORDS:
            continue
        tokens.append(token)
    return tokens


def _is_numeric_or_tiny(value: str) -> bool:
    if len(value) <= 1:
        return True
    if value.isdigit():
        return True
    return False


__all__ = [
    "MAX_SEMANTIC_TERMS",
    "derive_query_semantic_terms",
    "derive_semantic_terms",
]
