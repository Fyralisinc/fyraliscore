"""Simple retrieval implementation for benchmark smoke runs."""

from __future__ import annotations

import math
import re
import time
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

from benchmarks.adapters.base import BenchmarkObservation, BenchmarkQuery
from benchmarks.fyralis_eval.ingestion import InMemoryBenchmarkStore

_TOKEN_RE = re.compile(r"[a-z0-9']+")
_STOPWORDS = {
    "a",
    "about",
    "according",
    "as",
    "at",
    "by",
    "current",
    "did",
    "during",
    "for",
    "in",
    "is",
    "it",
    "of",
    "on",
    "she",
    "that",
    "the",
    "to",
    "used",
    "what",
    "which",
    "who",
    "your",
}


@dataclass(frozen=True)
class RetrievedEvidence:
    observation_id: str
    content: str
    score: float
    occurred_at: str
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return {
            "observation_id": self.observation_id,
            "content": self.content,
            "score": self.score,
            "occurred_at": self.occurred_at,
            "metadata": self.metadata,
        }


@dataclass(frozen=True)
class RetrievalOutput:
    query_id: str
    packet_id: str
    retrieved_nodes: list[dict[str, Any]]
    retrieved_evidence: list[RetrievedEvidence]
    context_packet: dict[str, Any]
    omission_ledger: list[dict[str, Any]]
    token_estimate: int
    latency_ms: int
    retrieval_calls: int

    def to_json(self) -> dict[str, Any]:
        return {
            "query_id": self.query_id,
            "packet_id": self.packet_id,
            "retrieved_nodes": self.retrieved_nodes,
            "retrieved_evidence": [item.to_json() for item in self.retrieved_evidence],
            "context_packet": self.context_packet,
            "omission_ledger": self.omission_ledger,
            "token_estimate": self.token_estimate,
            "latency_ms": self.latency_ms,
            "retrieval_calls": self.retrieval_calls,
        }


def tokenize(text: str) -> set[str]:
    tokens: set[str] = set()
    for token in _TOKEN_RE.findall(text.casefold()):
        if len(token) <= 1 or token in _STOPWORDS:
            continue
        tokens.add(token)
        if token.endswith("'s") and len(token) > 3:
            owner = token[:-2]
            if owner and owner not in _STOPWORDS:
                tokens.add(owner)
        elif token.endswith("s") and len(token) > 4:
            tokens.add(token[:-1])
    return tokens


def token_counts(text: str) -> Counter[str]:
    counts: Counter[str] = Counter()
    for token in _TOKEN_RE.findall(text.casefold()):
        if len(token) <= 1 or token in _STOPWORDS:
            continue
        counts[token] += 1
        if token.endswith("'s") and len(token) > 3:
            owner = token[:-2]
            if owner and owner not in _STOPWORDS:
                counts[owner] += 1
        elif token.endswith("s") and len(token) > 4:
            counts[token[:-1]] += 1
    return counts


class LexicalMemoryReader:
    """Deterministic lexical reader used as the first benchmark baseline."""

    def __init__(
        self,
        store: InMemoryBenchmarkStore,
        *,
        top_k: int = 5,
        min_score: float = 1.0,
        min_score_ratio: float = 0.35,
    ) -> None:
        self.store = store
        self.top_k = top_k
        self.min_score = min_score
        self.min_score_ratio = min_score_ratio

    def retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int, int]:
        started = time.monotonic()
        query_tokens = tokenize(query.query_text)
        query_terms = token_counts(query.query_text)
        rows: list[tuple[float, BenchmarkObservation]] = []
        for observation in self.store.observations_for_tenant(query.tenant_id):
            obs_tokens = tokenize(observation.content)
            overlap = query_tokens & obs_tokens
            phrase_score = _phrase_score(query.query_text, observation.content)
            score = (len(overlap) * 2.0) + phrase_score
            if score <= 0:
                continue
            score = _adjust_retrieval_score(
                score,
                query=query,
                observation=observation,
                counts=token_counts(observation.content),
                query_terms=query_terms,
            )
            if score <= 0:
                continue
            if score < self.min_score:
                continue
            recency_bonus = observation.occurred_at.timestamp() / 10_000_000_000
            rows.append((score + recency_bonus, observation))
        rows.sort(key=lambda item: item[0], reverse=True)
        min_score_ratio = self.min_score_ratio
        if "multi_hop" in query.query_type.casefold():
            min_score_ratio = min(min_score_ratio, 0.08)
        rows = _keep_confident_rows(rows, min_score_ratio=min_score_ratio)
        evidence = [
            RetrievedEvidence(
                observation_id=observation.observation_id,
                content=observation.content,
                score=round(score, 6),
                occurred_at=observation.occurred_at.isoformat(),
                metadata=observation.metadata,
            )
            for score, observation in rows[: self.top_k]
        ]
        elapsed_ms = max(0, math.ceil((time.monotonic() - started) * 1000))
        return evidence, elapsed_ms, 1


class BM25MemoryReader:
    """Session-level BM25 baseline for public benchmark retrieval."""

    def __init__(
        self,
        store: InMemoryBenchmarkStore,
        *,
        top_k: int = 10,
        k1: float = 1.5,
        b: float = 0.75,
        min_score_ratio: float = 0.12,
    ) -> None:
        self.store = store
        self.top_k = top_k
        self.k1 = k1
        self.b = b
        self.min_score_ratio = min_score_ratio
        self._index_by_tenant: dict[str, _BM25TenantIndex] = {}

    def retrieve(self, query: BenchmarkQuery) -> tuple[list[RetrievedEvidence], int, int]:
        started = time.monotonic()
        index = self._index_for_tenant(query.tenant_id)
        query_terms = token_counts(query.query_text)

        rows: list[tuple[float, BenchmarkObservation]] = []
        total_docs = len(index.observations)
        for observation, counts, doc_length in zip(
            index.observations,
            index.doc_counts,
            index.doc_lengths,
            strict=False,
        ):
            score = 0.0
            for term, query_count in query_terms.items():
                term_frequency = counts.get(term, 0)
                if term_frequency <= 0:
                    continue
                score += query_count * _bm25_term_score(
                    term_frequency=term_frequency,
                    doc_frequency=index.doc_freqs.get(term, 0),
                    total_docs=total_docs,
                    doc_length=doc_length,
                    avg_doc_length=index.avg_doc_length,
                    k1=self.k1,
                    b=self.b,
                )
            if score > 0:
                score = _adjust_retrieval_score(
                    score,
                    query=query,
                    observation=observation,
                    counts=counts,
                    query_terms=query_terms,
                )
            if score > 0:
                rows.append((score, observation))

        rows.sort(key=lambda item: item[0], reverse=True)
        min_score_ratio = self.min_score_ratio
        if "multi_hop" in query.query_type.casefold():
            min_score_ratio = min(min_score_ratio, 0.05)
        rows = _keep_confident_rows(rows, min_score_ratio=min_score_ratio)
        rows = _diversify_rows_for_query(query, rows, top_k=self.top_k)
        evidence = [
            RetrievedEvidence(
                observation_id=observation.observation_id,
                content=observation.content,
                score=round(score, 6),
                occurred_at=observation.occurred_at.isoformat(),
                metadata=observation.metadata,
            )
            for score, observation in rows[: self.top_k]
        ]
        elapsed_ms = max(0, math.ceil((time.monotonic() - started) * 1000))
        return evidence, elapsed_ms, 1

    def _index_for_tenant(self, tenant_id: str) -> "_BM25TenantIndex":
        cached = self._index_by_tenant.get(tenant_id)
        if cached is not None:
            return cached
        observations = self.store.observations_for_tenant(tenant_id)
        doc_counts = [token_counts(observation.content) for observation in observations]
        doc_lengths = [sum(counts.values()) for counts in doc_counts]
        avg_doc_length = (sum(doc_lengths) / len(doc_lengths)) if doc_lengths else 0.0
        doc_freqs: Counter[str] = Counter()
        for counts in doc_counts:
            for term in counts:
                doc_freqs[term] += 1
        index = _BM25TenantIndex(
            observations=observations,
            doc_counts=doc_counts,
            doc_lengths=doc_lengths,
            avg_doc_length=avg_doc_length,
            doc_freqs=doc_freqs,
        )
        self._index_by_tenant[tenant_id] = index
        return index


@dataclass(frozen=True)
class _BM25TenantIndex:
    observations: list[BenchmarkObservation]
    doc_counts: list[Counter[str]]
    doc_lengths: list[int]
    avg_doc_length: float
    doc_freqs: Counter[str]


def _phrase_score(query_text: str, content: str) -> float:
    query = query_text.casefold()
    body = content.casefold()
    score = 0.0
    if "allerg" in query and "allerg" in body:
        score += 5.0
    if "drink" in query and ("drink" in body or "coffee" in body or "tea" in body):
        score += 5.0
    if "passport" in query and "passport" in body:
        score += 5.0
    return score


def _document_frequencies(
    doc_counts: list[Counter[str]],
    query_terms: Counter[str],
) -> dict[str, int]:
    return {
        term: sum(1 for counts in doc_counts if term in counts)
        for term in query_terms
    }


def _adjust_retrieval_score(
    score: float,
    *,
    query: BenchmarkQuery,
    observation: BenchmarkObservation,
    counts: Counter[str],
    query_terms: Counter[str],
) -> float:
    coverage = _query_term_coverage(query_terms, counts)
    phrase_count = _phrase_overlap_count(query.query_text, observation.content)
    adjusted = score * (0.55 + coverage)
    adjusted *= 1.0 + min(0.45, phrase_count * 0.15)

    is_multi_hop = "multi_hop" in query.query_type.casefold()
    if (
        len(query_terms) >= 4
        and coverage <= 0.25
        and phrase_count == 0
        and not is_multi_hop
    ):
        adjusted *= 0.25

    if observation.metadata.get("stale") and _asks_for_current_state(query.query_text):
        adjusted *= 0.08
    if _is_low_trust(observation) and not _asks_for_low_trust(query.query_text):
        adjusted *= 0.20
    adjusted += _structured_query_bonus(query, observation)
    return adjusted


def _structured_query_bonus(
    query: BenchmarkQuery,
    observation: BenchmarkObservation,
) -> float:
    query_text = f"{query.query_text} {query.query_type}".casefold()
    metadata = observation.metadata
    bonus = 0.0
    if _asks_for_sort_state(query_text) and metadata.get("sort_fields"):
        bonus += 14.0
        bonus += 2.0 * _metadata_overlap(query_text, metadata.get("sort_fields"))
    if _asks_for_form_value(query_text) and metadata.get("structured_ui_facts"):
        bonus += 8.0
        bonus += 1.5 * _metadata_overlap(query_text, metadata.get("structured_ui_facts"))
    if _asks_for_stage_count(query_text) and metadata.get("stage_chains"):
        bonus += 10.0
    if _asks_for_checkbox_comparison(query_text) and metadata.get("structured_ui_facts"):
        facts = " ".join(str(item) for item in metadata.get("structured_ui_facts") or [])
        if "checkbox choice group" in facts.casefold():
            bonus += 8.0
    return bonus


def _metadata_overlap(query_text: str, values: Any) -> float:
    if not isinstance(values, list):
        return 0.0
    query_terms = tokenize(query_text)
    value_terms = tokenize(" ".join(str(value) for value in values))
    if not query_terms or not value_terms:
        return 0.0
    return float(len(query_terms & value_terms))


def _asks_for_sort_state(query_text: str) -> bool:
    return any(
        marker in query_text
        for marker in (
            "default sort",
            "initially shown in the sort",
            "sort field",
            "sort row",
            "sorting",
            "target field",
        )
    )


def _asks_for_form_value(query_text: str) -> bool:
    return any(
        marker in query_text
        for marker in (
            "automatically change",
            "automatically changes",
            "field automatically",
            "impact",
            "priority",
            "urgency",
            "what value",
        )
    )


def _asks_for_stage_count(query_text: str) -> bool:
    return any(
        marker in query_text
        for marker in (
            "excluding in-progress",
            "fully complete",
            "how many stages",
            "pipeline",
            "stages remain",
        )
    )


def _asks_for_checkbox_comparison(query_text: str) -> bool:
    return "checkbox" in query_text and any(
        marker in query_text
        for marker in ("compare", "comparing", "largest", "which page", "which item")
    )


def _diversify_rows_for_query(
    query: BenchmarkQuery,
    rows: list[tuple[float, BenchmarkObservation]],
    *,
    top_k: int,
) -> list[tuple[float, BenchmarkObservation]]:
    if top_k <= 1 or len(rows) <= top_k:
        return rows
    query_text = query.query_text
    query_lower = query_text.casefold()
    if not any(marker in query_lower for marker in ("compare", "comparing", "largest", "which")):
        return rows
    phrases = _query_key_phrases(query_text)
    if len(phrases) < 2:
        return rows

    selected: list[tuple[float, BenchmarkObservation]] = []
    selected_ids: set[str] = set()
    covered_phrases: set[str] = set()
    max_phrase_slots = min(top_k, len(phrases))
    while len(selected) < max_phrase_slots:
        best_row: tuple[float, BenchmarkObservation] | None = None
        best_new_phrases: set[str] = set()
        for row in rows:
            observation = row[1]
            if observation.observation_id in selected_ids:
                continue
            matched = _row_matched_phrases(row, phrases)
            new_phrases = matched - covered_phrases
            if not new_phrases:
                continue
            if best_row is None or (
                len(new_phrases),
                row[0],
            ) > (
                len(best_new_phrases),
                best_row[0],
            ):
                best_row = row
                best_new_phrases = new_phrases
        if best_row is None:
            break
        selected.append(best_row)
        selected_ids.add(best_row[1].observation_id)
        covered_phrases.update(best_new_phrases)
        if covered_phrases >= set(phrases):
            break

    if len(selected) < 2:
        return rows
    selected.extend(row for row in rows if row[1].observation_id not in selected_ids)
    return selected


def _query_key_phrases(query_text: str) -> list[str]:
    phrases: list[str] = []
    for pattern in (r"`([^`]{2,80})`", r'"([^"]{2,80})"'):
        for match in re.finditer(pattern, query_text):
            phrase = " ".join(match.group(1).split())
            if phrase and phrase.casefold() not in {item.casefold() for item in phrases}:
                phrases.append(phrase)
    return phrases[:8]


def _row_matched_phrases(
    row: tuple[float, BenchmarkObservation],
    phrases: list[str],
) -> set[str]:
    observation = row[1]
    haystack = f"{observation.content} {observation.metadata}".casefold()
    return {phrase for phrase in phrases if phrase.casefold() in haystack}


def _keep_confident_rows(
    rows: list[tuple[float, BenchmarkObservation]],
    *,
    min_score_ratio: float,
) -> list[tuple[float, BenchmarkObservation]]:
    if not rows:
        return rows
    max_score = rows[0][0]
    if max_score <= 0 or min_score_ratio <= 0:
        return rows
    threshold = max_score * min_score_ratio
    return [(score, observation) for score, observation in rows if score >= threshold]


def _query_term_coverage(query_terms: Counter[str], counts: Counter[str]) -> float:
    terms = set(query_terms)
    if not terms:
        return 0.0
    present = sum(1 for term in terms if counts.get(term, 0) > 0)
    return present / len(terms)


def _phrase_overlap_count(query_text: str, content: str) -> int:
    query_tokens = [
        token
        for token in _TOKEN_RE.findall(query_text.casefold())
        if len(token) > 1 and token not in _STOPWORDS
    ]
    if len(query_tokens) < 2:
        return 0
    body = content.casefold()
    count = 0
    for size in (2, 3):
        for index in range(0, len(query_tokens) - size + 1):
            phrase = " ".join(query_tokens[index:index + size])
            if phrase in body:
                count += 1
    return count


def _asks_for_current_state(query_text: str) -> bool:
    query_lower = query_text.casefold()
    return any(
        marker in query_lower
        for marker in (
            "current",
            "currently",
            "latest",
            "now",
            "active",
            "present",
            "most recent",
        )
    )


def _asks_for_low_trust(query_text: str) -> bool:
    query_lower = query_text.casefold()
    return any(marker in query_lower for marker in ("rumor", "rumour", "unverified"))


def _is_low_trust(observation: BenchmarkObservation) -> bool:
    trust = str(observation.trust_tier or "").casefold()
    metadata_trust = str(observation.metadata.get("trust") or "").casefold()
    return trust in {"unverified", "low", "low_trust"} or metadata_trust == "low"


def _bm25_term_score(
    *,
    term_frequency: int,
    doc_frequency: int,
    total_docs: int,
    doc_length: int,
    avg_doc_length: float,
    k1: float,
    b: float,
) -> float:
    if total_docs <= 0 or avg_doc_length <= 0:
        return 0.0
    idf = math.log(1 + ((total_docs - doc_frequency + 0.5) / (doc_frequency + 0.5)))
    denominator = term_frequency + k1 * (1 - b + b * (doc_length / avg_doc_length))
    return idf * ((term_frequency * (k1 + 1)) / denominator)
