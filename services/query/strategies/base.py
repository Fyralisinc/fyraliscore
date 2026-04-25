"""
services/query/strategies/base.py — shared types + helpers for all
per-category strategies.

Each strategy is a small module exporting a module-level `strategy`
object that conforms to `StrategyProtocol`. The actual implementation
is a tiny class or a dataclass so it can hold category-specific
knobs (pathway mix, default time window, etc.) without turning the
dispatch into module-function-pointer bookkeeping.

The parsing helpers here are deliberately cheap: regex-level entity
extraction, not an LLM pass. We want the strategy layer to be
predictable. If the query refers to an entity we miss, retrieval
still gets a useful seed via Pathway B (semantic) on the raw text.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Protocol
from uuid import UUID

import asyncpg

from services.retrieval.assembler import (
    AccessContext,
    ContextBundle,
    assemble_context,
)
from services.retrieval.primary import (
    RetrievalResult,
    TriggerContext,
    TriggerKind,
    primary_retrieve,
)


# ---------------------------------------------------------------------
# Parsed query shape
# ---------------------------------------------------------------------


@dataclass
class ParsedQuery:
    """Whatever the strategy extracted from the raw query + history.

    Fields are all optional and deliberately loose — different
    strategies populate different subsets. The rendering layer reads
    the ones it cares about via the retrieval_trace.
    """
    raw_query: str
    category: str
    entity_mentions: list[str] = field(default_factory=list)  # free-text nouns
    person_mentions: list[str] = field(default_factory=list)  # @alice style
    customer_mentions: list[str] = field(default_factory=list)  # heuristic
    time_window: Optional[timedelta] = None
    time_anchor: Optional[datetime] = None
    recipient: Optional[str] = None             # for draft queries
    sender: Optional[str] = None                # for draft queries (defaults to CEO)
    counterfactual_hypothesis: Optional[str] = None  # for what_if
    subject_keywords: list[str] = field(default_factory=list)
    trace: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Strategy execution context
# ---------------------------------------------------------------------


@dataclass
class StrategyContext:
    """What a strategy needs at execution time.

    `conn` must live inside the caller's transaction — retrieval runs
    reconsolidation (activation bumps) and we want that to land in the
    same tx that serves the request. API layer opens one.
    """
    tenant_id: UUID
    conn: asyncpg.Connection
    access_context: AccessContext
    conversation_history: list["Turn"] = field(default_factory=list)  # noqa: F821
    card_context: Optional["CardContext"] = None                       # noqa: F821
    now: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class StrategyResult:
    """What a strategy returns. `context_bundle` is the
    retrieval-assembled result (what rendering sees). `parsed` is the
    strategy's reading of the query (what trace shows). `notes` holds
    strategy-specific extras (e.g. voice-style hints for draft).
    """
    parsed: ParsedQuery
    retrieval_result: RetrievalResult
    context_bundle: ContextBundle
    notes: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Strategy protocol
# ---------------------------------------------------------------------


class StrategyProtocol(Protocol):
    """Every per-category strategy exposes these three methods."""

    def parse(
        self,
        query: str,
        *,
        conversation_history: list[Any] | None = None,
        card_context: Any | None = None,
    ) -> ParsedQuery: ...

    def build_trigger(
        self,
        parsed: ParsedQuery,
        tenant_id: UUID,
        *,
        now: datetime,
    ) -> TriggerContext: ...

    async def gather(
        self,
        parsed: ParsedQuery,
        ctx: StrategyContext,
    ) -> StrategyResult: ...


# ---------------------------------------------------------------------
# Regex helpers shared across strategies
# ---------------------------------------------------------------------

# @mention style — slack-ish
_PERSON_RE = re.compile(r"(?:^|\s)@([A-Za-z][\w\.\-]{0,40})")
# #channel style
_CHANNEL_RE = re.compile(r"(?:^|\s)#([A-Za-z][\w\-]{0,40})")
# Customer-style proper noun: Capitalized word, optionally with a second
# Capitalized word. Matches "Acme" and "Acme Corp"; misses lowercase
# company names — acceptable, the semantic pathway picks these up.
_CUSTOMER_RE = re.compile(r"\b([A-Z][A-Za-z0-9]{1,}(?:\s[A-Z][A-Za-z0-9]{1,})?)\b")

# Time-window heuristics. Return (anchor_delta_back, window_duration).
# anchor_delta_back: how far back from `now` the window starts.
# window_duration: how wide the window is.
_TIME_RULES: tuple[tuple[re.Pattern[str], timedelta, timedelta], ...] = (
    # Explicit "yesterday"
    (re.compile(r"\byesterday\b", re.I), timedelta(days=1), timedelta(days=1)),
    # "today" (use a full 24h anchored at now's midnight; but we
    # simplify to a 24h trailing window)
    (re.compile(r"\btoday\b", re.I), timedelta(hours=24), timedelta(hours=24)),
    # "last N days"
    # — handled explicitly by _parse_last_n_days below.
    # Standing windows
    (re.compile(r"\blast week\b|\bthis week\b|\bpast week\b", re.I),
     timedelta(days=7), timedelta(days=7)),
    (re.compile(r"\blast month\b|\bthis month\b|\bpast month\b", re.I),
     timedelta(days=30), timedelta(days=30)),
    (re.compile(r"\blast quarter\b|\bthis quarter\b", re.I),
     timedelta(days=90), timedelta(days=90)),
    (re.compile(r"\bovernight\b|\blast night\b", re.I),
     timedelta(hours=12), timedelta(hours=12)),
)

_LAST_N_DAYS_RE = re.compile(r"\blast\s+(\d{1,3})\s+days?\b", re.I)


def extract_persons(text: str) -> list[str]:
    """Lowercase + dedup @-mentions. Order-preserving."""
    seen: set[str] = set()
    out: list[str] = []
    for m in _PERSON_RE.finditer(text or ""):
        who = m.group(1).lower()
        if who in seen:
            continue
        seen.add(who)
        out.append(who)
    return out


def extract_customer_candidates(text: str) -> list[str]:
    """Cheap proper-noun extractor. Skips leading Capitalized words that
    look like sentence starts (first token). Order-preserving."""
    if not text:
        return []
    tokens = text.split()
    if not tokens:
        return []
    # Ignore the leading token when it's the first word of the sentence;
    # sentence-initial capitalization is ambiguous.
    body = " " + " ".join(tokens[1:]) if len(tokens) > 1 else ""
    seen: set[str] = set()
    out: list[str] = []
    for m in _CUSTOMER_RE.finditer(body):
        name = m.group(1).strip()
        key = name.lower()
        if key in seen:
            continue
        # Filter common stopwords that look like Capitalized nouns.
        if key in _STOPWORDS:
            continue
        seen.add(key)
        out.append(name)
    return out


_STOPWORDS = {
    "i", "we", "you", "the", "a", "an", "and", "or", "but", "if",
    "when", "while", "of", "on", "at", "to", "from", "into", "with",
    "about", "as", "is", "are", "was", "were", "be", "been",
    # Month names / weekdays (these frequently appear capitalized in
    # queries like "on Monday" but aren't customer/entity names).
    "monday", "tuesday", "wednesday", "thursday", "friday", "saturday",
    "sunday",
    "january", "february", "march", "april", "may", "june", "july",
    "august", "september", "october", "november", "december",
    "acme",  # explicitly a customer; leave it but... it IS a customer, keep it
}
# Oops — we want "Acme" recognized. Remove it from the stoplist:
_STOPWORDS.discard("acme")


def extract_time_window(
    text: str, *, now: datetime
) -> tuple[Optional[datetime], Optional[timedelta]]:
    """Return (anchor, window) where anchor is the start of the window
    and window is the duration. None / None when no time hint found.

    Strategies that need a seed timestamp pass `now` in from the
    StrategyContext so tests can freeze it.
    """
    if not text:
        return None, None
    # "last N days"
    m = _LAST_N_DAYS_RE.search(text)
    if m:
        n = max(1, min(int(m.group(1)), 365))
        window = timedelta(days=n)
        anchor = now - window
        return anchor, window
    for pat, back, window in _TIME_RULES:
        if pat.search(text):
            anchor = now - back
            return anchor, window
    return None, None


def extract_subject_keywords(text: str, *, max_tokens: int = 12) -> list[str]:
    """Extract the content-bearing tokens (drop stopwords and
    punctuation-only). Used by strategies to build seed_natural_text
    for Pathway B."""
    if not text:
        return []
    cleaned = re.sub(r"[^\w\s]", " ", text.lower())
    out: list[str] = []
    for tok in cleaned.split():
        if tok in _STOPWORDS or len(tok) <= 1:
            continue
        if tok.isdigit():
            continue
        out.append(tok)
        if len(out) >= max_tokens:
            break
    return out


def parse_recipient(text: str) -> Optional[str]:
    """For draft queries: look for 'to <name>' / 'reply to <name>' /
    '@name'. Returns the first match, lowercased."""
    if not text:
        return None
    # @mentions first.
    persons = extract_persons(text)
    if persons:
        return persons[0]
    # "to <Name>" / "for <Name>" / "message <Name>"
    # Ordered: longer phrases first so "reply to X" doesn't match as
    # just "to" → "X"; we pick the trailing token.
    patterns = (
        r"\breply\s+to\s+([A-Za-z][\w\-]{1,40})\b",
        r"\bmessage\s+to\s+([A-Za-z][\w\-]{1,40})\b",
        r"\bmessage\s+([A-Za-z][\w\-]{1,40})\b",
        r"\b(?:to|for)\s+([A-Za-z][\w\-]{1,40})\b",
    )
    for pat in patterns:
        m = re.search(pat, text)
        if m:
            cand = m.group(1).lower()
            # Filter obvious function words caught by the generic "to X" rule.
            if cand in {"the", "a", "an", "me", "us", "you", "him", "her", "them"}:
                continue
            return cand
    return None


# ---------------------------------------------------------------------
# Retrieval execution helpers
# ---------------------------------------------------------------------


async def run_retrieval(
    trigger: TriggerContext,
    ctx: StrategyContext,
    *,
    budget_models: int | None = None,
    budget_observations: int | None = None,
) -> tuple[RetrievalResult, ContextBundle]:
    """Run primary_retrieve + assemble_context inside the caller's
    transaction. Central so every strategy honours the same access
    control + size budgets."""
    from services.retrieval.assembler import (
        _BUDGET_MODELS,
        _BUDGET_OBSERVATIONS,
    )

    retrieval_result = await primary_retrieve(trigger, ctx.conn)
    bundle = await assemble_context(
        retrieval_result,
        ctx.access_context,
        ctx.conn,
        budget_models=budget_models or _BUDGET_MODELS,
        budget_observations=budget_observations or _BUDGET_OBSERVATIONS,
    )
    return retrieval_result, bundle


__all__ = [
    "ParsedQuery",
    "StrategyContext",
    "StrategyResult",
    "StrategyProtocol",
    "extract_persons",
    "extract_customer_candidates",
    "extract_time_window",
    "extract_subject_keywords",
    "parse_recipient",
    "run_retrieval",
]
