"""
services/query/classifier.py — lightweight query classifier.

Routes a natural-language CEO query into one of six categories so
`core.QueryHandler` can dispatch to the appropriate retrieval strategy:

  - why      : "why is X happening"        (retrieve + reason)
  - show_me  : "show me ..."               (retrieve-heavy, light reasoning)
  - draft    : "draft a message to ..."    (communication composition)
  - what_if  : "what if we ..."            (counterfactual)
  - summary  : "what happened yesterday"   (time-bounded retrieval)
  - arbitrary: default bucket

Design:
  - Primary path: a deepseek-chat structured call (single short prompt).
  - Fast path: cheap keyword heuristic runs first; if it returns a
    confident label we skip the LLM entirely.
  - Brief in-memory LRU-ish cache on (tenant, normalized_query,
    card_context_bool) so repeated chip taps / grid prefetch + tap
    don't re-spend an LLM call.

Why a lightweight model: classification is a routing decision, not a
reasoning one. deepseek-chat at temperature 0 is fast (<1s typical),
cheap ($0.27/$1.10 per 1M tokens per MODEL_PRICING), and sufficient.

The classifier never raises on LLM failure — it falls back to
'arbitrary' so the user still gets an answer. The failure is recorded
in the trace dict returned alongside the category.

BUILD-PLAN §4 Phase 1.
"""
from __future__ import annotations

import asyncio
import logging
import os
import re
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Optional, Protocol
from uuid import UUID

from pydantic import BaseModel, Field

from lib.llm.provider import (
    DeepSeekProvider,
    LLMConfig,
    LLMError,
    LLMProvider,
)

log = logging.getLogger(__name__)


QueryCategory = Literal[
    "why", "show_me", "draft", "what_if", "summary", "arbitrary"
]

VALID_CATEGORIES: tuple[QueryCategory, ...] = (
    "why", "show_me", "draft", "what_if", "summary", "arbitrary",
)


# ---------------------------------------------------------------------
# Heuristic — cheap keyword prefilter.
# ---------------------------------------------------------------------

# Ordered tuples: (category, compiled-regex, rough confidence 0..1).
# We match at the FRONT of the normalized query where possible so
# e.g. "why is X happening" is "why" but "tell me about why we lost"
# falls through to the LLM. Confidence is used to gate whether we skip
# the LLM (>=0.9 = skip).
_HEURISTICS: tuple[tuple[QueryCategory, re.Pattern[str], float], ...] = (
    ("draft",    re.compile(r"^\s*(draft|compose|write)\b"), 0.95),
    ("show_me",  re.compile(r"^\s*(show me|show|list|give me)\b"), 0.92),
    ("what_if",  re.compile(r"^\s*what if\b"), 0.95),
    ("summary",  re.compile(
        r"^\s*(what happened|summary|summari[sz]e|what did|what's new|whats new|recap)\b"
    ), 0.9),
    ("why",      re.compile(r"^\s*(why|how come)\b"), 0.9),
)


def heuristic_classify(query: str) -> tuple[Optional[QueryCategory], float]:
    """Pure-function prefilter. Returns (category, confidence) or
    (None, 0.0) when no heuristic fires.

    Deliberately conservative: only rules that are very reliable fire.
    Ambiguous cases fall through to the LLM."""
    if not query or not query.strip():
        return None, 0.0
    q = query.strip().lower()
    for cat, pat, conf in _HEURISTICS:
        if pat.search(q):
            return cat, conf
    return None, 0.0


# ---------------------------------------------------------------------
# LLM classification — structured output via provider.structured()
# ---------------------------------------------------------------------


class _ClassifierOutput(BaseModel):
    """Structured output expected from deepseek-chat. `reason` is short
    diagnostic prose used only for logging/tests."""
    category: Literal["why", "show_me", "draft", "what_if", "summary", "arbitrary"] = Field(
        description="one of: why, show_me, draft, what_if, summary, arbitrary"
    )
    reason: str = Field(
        default="",
        description="brief explanation (<=15 words) of the choice",
    )


_SYSTEM_PROMPT = (
    "You are a routing classifier for a CEO's query interface. "
    "Classify the user's query into exactly ONE category so the "
    "backend can dispatch it to the correct retrieval strategy.\n"
    "\n"
    "Categories:\n"
    "  why       — asks for causal reasoning (why is X, how come Y happened)\n"
    "  show_me   — structural lookup, little reasoning (show the customers "
    "              with health != healthy; list open commitments)\n"
    "  draft     — asks the system to compose a message or reply\n"
    "  what_if   — counterfactual / hypothetical (what if we cut X, "
    "              what happens if we defer Y)\n"
    "  summary   — time-bounded recap (what happened yesterday, this week, "
    "              recap of Acme situation)\n"
    "  arbitrary — everything else\n"
    "\n"
    "Return JSON only. Pick the single best category; when multiple "
    "seem to fit, prefer the more specific one (why > arbitrary, "
    "summary > arbitrary)."
)


# ---------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------


@dataclass
class _CacheEntry:
    category: QueryCategory
    source: str              # 'heuristic' | 'llm' | 'fallback'
    confidence: float
    created_at: float


class _ClassifierCache:
    """Small TTL + size-capped cache. In-memory, single-process.

    Key: (tenant_id_str, normalized_query, has_card_context)
    Value: _CacheEntry

    Not a contract-level cache — we're fine with it being wiped on
    process restart. Prefetch warmth is a separate concern owned by
    prefetch.py."""

    def __init__(self, *, max_entries: int = 512, ttl_seconds: int = 600) -> None:
        self._store: dict[tuple[str, str, bool], _CacheEntry] = {}
        self._max = max_entries
        self._ttl = ttl_seconds
        self._lock = asyncio.Lock()

    @staticmethod
    def _norm(query: str) -> str:
        return re.sub(r"\s+", " ", query.strip().lower())

    async def get(
        self, tenant_id: UUID, query: str, *, has_card_context: bool
    ) -> Optional[_CacheEntry]:
        key = (str(tenant_id), self._norm(query), bool(has_card_context))
        async with self._lock:
            entry = self._store.get(key)
            if entry is None:
                return None
            if time.time() - entry.created_at > self._ttl:
                self._store.pop(key, None)
                return None
            return entry

    async def put(
        self,
        tenant_id: UUID,
        query: str,
        *,
        has_card_context: bool,
        entry: _CacheEntry,
    ) -> None:
        key = (str(tenant_id), self._norm(query), bool(has_card_context))
        async with self._lock:
            # Evict oldest when above cap (O(n) — fine at n=512).
            if len(self._store) >= self._max:
                oldest_key = min(
                    self._store, key=lambda k: self._store[k].created_at
                )
                self._store.pop(oldest_key, None)
            self._store[key] = entry

    async def clear(self) -> None:
        async with self._lock:
            self._store.clear()


# Module-level singleton so prefetch + API share the same cache.
_DEFAULT_CACHE = _ClassifierCache()


def get_default_cache() -> _ClassifierCache:
    return _DEFAULT_CACHE


# ---------------------------------------------------------------------
# Provider factory — default to DeepSeek chat for lightweight routing.
# ---------------------------------------------------------------------


def _build_classifier_provider() -> LLMProvider:
    """Pin the classifier to deepseek-chat regardless of the env-level
    `LLM_MODEL`. Think/rendering may use a heavier model; we want a
    routing call to stay cheap.

    We reuse the API key from whichever env var the shared LLMConfig
    would use. If `LLM_PROVIDER=deepseek`, great — we'll share creds.
    Otherwise we read `DEEPSEEK_API_KEY` directly; if that's empty the
    provider will raise on call and we fall back to heuristics."""
    api_key = (
        os.environ.get("DEEPSEEK_API_KEY")
        or os.environ.get("LLM_API_KEY")
        or ""
    )
    cfg = LLMConfig(
        provider="deepseek",
        api_key=api_key,
        model="deepseek-chat",
        timeout_s=15.0,  # classification should be snappy
        max_retries=1,
    )
    return DeepSeekProvider(cfg)


# ---------------------------------------------------------------------
# QueryClassifier
# ---------------------------------------------------------------------


@dataclass
class ClassificationResult:
    """Result handed to the query handler."""
    category: QueryCategory
    source: str  # 'heuristic' | 'llm' | 'cache' | 'fallback'
    confidence: float
    trace: dict[str, Any] = field(default_factory=dict)


class QueryClassifier:
    """
    Classify CEO queries.

    Usage:
        classifier = QueryClassifier()
        result = await classifier.classify(tenant_id, "why is Acme at risk?")
        # result.category == 'why'

    Construction args:
      - `provider`: override the default deepseek-chat provider
        (tests inject a ScriptedProvider-like double here).
      - `cache`: override the module-level cache (tests inject a
        fresh instance per test).
      - `heuristic_confidence_skip_threshold`: if the heuristic fires
        with confidence >= this, skip the LLM.
    """

    def __init__(
        self,
        *,
        provider: LLMProvider | None = None,
        cache: _ClassifierCache | None = None,
        heuristic_confidence_skip_threshold: float = 0.9,
    ) -> None:
        self._provider = provider  # lazily built
        self._cache = cache or _DEFAULT_CACHE
        self._skip_threshold = heuristic_confidence_skip_threshold

    def _get_provider(self) -> LLMProvider:
        if self._provider is None:
            self._provider = _build_classifier_provider()
        return self._provider

    async def classify(
        self,
        tenant_id: UUID,
        query: str,
        *,
        has_card_context: bool = False,
    ) -> ClassificationResult:
        trace: dict[str, Any] = {"query_len": len(query or "")}
        if not query or not query.strip():
            # Empty query: degrade gracefully. Core will surface a 400.
            return ClassificationResult(
                category="arbitrary",
                source="fallback",
                confidence=0.0,
                trace={"reason": "empty_query"},
            )

        # --- Cache check ---
        cached = await self._cache.get(
            tenant_id, query, has_card_context=has_card_context
        )
        if cached is not None:
            trace["cache"] = "hit"
            return ClassificationResult(
                category=cached.category,
                source="cache",
                confidence=cached.confidence,
                trace=trace,
            )

        # --- Heuristic path ---
        heur_cat, heur_conf = heuristic_classify(query)
        if heur_cat is not None and heur_conf >= self._skip_threshold:
            trace["heuristic_hit"] = heur_cat
            trace["heuristic_confidence"] = heur_conf
            entry = _CacheEntry(
                category=heur_cat,
                source="heuristic",
                confidence=heur_conf,
                created_at=time.time(),
            )
            await self._cache.put(
                tenant_id, query,
                has_card_context=has_card_context,
                entry=entry,
            )
            return ClassificationResult(
                category=heur_cat,
                source="heuristic",
                confidence=heur_conf,
                trace=trace,
            )

        # --- LLM path ---
        user_msg = (
            f"Query from the CEO: {query!r}\n"
            f"Has card context attached: {has_card_context}\n\n"
            "Return {\"category\": <label>, \"reason\": <=15 words}."
        )
        try:
            provider = self._get_provider()
            out: _ClassifierOutput = await provider.structured(
                system=_SYSTEM_PROMPT,
                user=user_msg,
                schema=_ClassifierOutput,
                temperature=0.0,
                max_tokens=128,
            )
            cat: QueryCategory = out.category  # type: ignore[assignment]
            trace["llm_reason"] = out.reason
            entry = _CacheEntry(
                category=cat,
                source="llm",
                confidence=0.8,
                created_at=time.time(),
            )
            await self._cache.put(
                tenant_id, query,
                has_card_context=has_card_context,
                entry=entry,
            )
            return ClassificationResult(
                category=cat,
                source="llm",
                confidence=0.8,
                trace=trace,
            )
        except (LLMError, Exception) as exc:  # noqa: BLE001
            # Never let a classifier hiccup break the query flow.
            log.warning(
                "classifier_llm_failed",
                extra={"error": str(exc), "query_len": len(query)},
            )
            trace["llm_error"] = str(exc)
            # If the heuristic suggested a category at any confidence,
            # use it; otherwise fall back to arbitrary.
            if heur_cat is not None:
                return ClassificationResult(
                    category=heur_cat,
                    source="fallback",
                    confidence=heur_conf,
                    trace=trace,
                )
            return ClassificationResult(
                category="arbitrary",
                source="fallback",
                confidence=0.0,
                trace=trace,
            )


__all__ = [
    "QueryCategory",
    "VALID_CATEGORIES",
    "ClassificationResult",
    "QueryClassifier",
    "heuristic_classify",
    "get_default_cache",
]
