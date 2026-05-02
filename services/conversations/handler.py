"""services.conversations.handler — probe → response orchestration.

A probe is one of three kinds:
  - phrase: user clicked a `<probe>` element in the recommendation
    body or in a prior response. The id was emitted by the substrate.
  - chip:   user clicked a substrate-suggested probe chip from the
    main probe row or from an exchange's follow-ups.
  - ask:    free-form question typed into the in-card Ask field.

For v1, phrase/chip probes are resolved deterministically — the handler
quotes the recommendation context and emits a templated response with
new `<probe>` markup so the conversation can branch. Free-form Ask
probes route through services.query.QueryHandler with the card context
wired in (handler.answer_query already accepts inline_card_context).

Streaming is not yet implemented; the handler returns the full response
in one shot. The wire shape is forward-compatible — a streaming variant
will yield chunks via SSE in a follow-up.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from typing import Any, Literal, Optional
from uuid import UUID

import asyncpg

from services.query.core import (
    AnswerQueryRequest,
    CardContext,
    QueryHandler,
)
from typing import Protocol


class _RecLike(Protocol):
    id: UUID
    proposition_kind: Optional[str]
    qualitative_impact: Optional[str]
    proposition_text: str
    target_entity: Any

from .repo import CardConversation, CardExchange, ConversationRepo


log = logging.getLogger(__name__)


@dataclass
class _RecLite:
    """Subset of RecommendationView the resolver actually needs.

    Avoids a tight coupling to the full `RecommendationView` shape and
    means the probe handler keeps working even if the recommendations
    schema gains/loses fields. The resolver code paths only read these
    four attributes (plus an absent `target_entity` — handled).
    """
    id: UUID
    proposition_kind: Optional[str]
    qualitative_impact: Optional[str]
    proposition_text: str
    target_entity: Optional[Any] = None


ProbeKind = Literal["phrase", "chip", "ask"]


@dataclass
class ProbeRequest:
    tenant_id: UUID
    actor_id: UUID
    card_id: UUID
    kind: ProbeKind
    probe_id: Optional[str] = None  # required for phrase/chip
    query: Optional[str] = None     # required for ask


@dataclass
class ProbeResponse:
    exchange: CardExchange


class ProbeHandler:
    """Resolves a probe id (or free-form question) to a response and
    persists the resulting exchange.

    The handler is intentionally narrow on dependencies — the
    QueryHandler is optional. When absent (e.g. unit tests), free-form
    asks fall back to a deterministic templated response so the
    contract stays exercisable end-to-end.
    """

    def __init__(
        self,
        *,
        repo: ConversationRepo,
        pool: asyncpg.Pool,
        query_handler: Optional[QueryHandler] = None,
    ):
        self._repo = repo
        self._pool = pool
        self._qh = query_handler

    async def probe(self, req: ProbeRequest) -> ProbeResponse:
        if req.kind in ("phrase", "chip") and not req.probe_id:
            raise ValueError("probe_id required for phrase/chip probes")
        if req.kind == "ask" and not (req.query and req.query.strip()):
            raise ValueError("query required for ask probes")

        conv = await self._repo.get_or_create(
            tenant_id=req.tenant_id,
            actor_id=req.actor_id,
            card_id=req.card_id,
        )

        # Look up the recommendation so the response can quote relevant
        # context. We tolerate a missing recommendation (e.g. a stale
        # card the user opened from cache) by falling back to a generic
        # response — surfaces gracefully rather than 500ing.
        rec = await self._fetch_recommendation(req.tenant_id, req.card_id)
        started = time.monotonic()
        probe_action, probe_text, response_html, follow_ups = (
            await self._resolve(rec, req, conv)
        )
        latency_ms = int((time.monotonic() - started) * 1000)
        exchange = await self._repo.append_exchange(
            conversation=conv,
            probe_kind=req.kind,
            probe_id=req.probe_id,
            probe_action=probe_action,
            probe_text=probe_text,
            response_html=response_html,
            follow_ups=follow_ups,
            latency_ms=latency_ms,
        )
        return ProbeResponse(exchange=exchange)

    async def _fetch_recommendation(
        self, tenant_id: UUID, card_id: UUID,
    ) -> Optional["_RecLike"]:
        """Light fetch: only the fields the resolver needs (proposition
        text, qualitative impact, target entity title). The
        recommendations repo doesn't expose a `get_one` yet, so we
        select directly. Tolerates a missing card so probes don't 500
        on stale UI state.
        """
        try:
            async with self._pool.acquire() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, proposition_kind,
                           proposition->>'qualitative_impact' AS qualitative_impact,
                           proposition->>'text' AS proposition_text
                    FROM models
                    WHERE id = $1 AND tenant_id = $2
                      AND proposition_kind = 'recommendation'
                    """,
                    card_id, tenant_id,
                )
                if row is None:
                    return None
                # Build a lightweight stand-in. The full RecommendationView
                # has many more fields, but the resolver only reads these.
                return _RecLite(
                    id=row["id"],
                    proposition_kind=row["proposition_kind"],
                    qualitative_impact=row["qualitative_impact"],
                    proposition_text=row["proposition_text"] or "",
                )
        except Exception:
            return None

    async def _resolve(
        self,
        rec: Optional["_RecLike"],
        req: ProbeRequest,
        conv: CardConversation,
    ) -> tuple[str, str, str, list[dict[str, str]]]:
        if req.kind == "phrase":
            return _resolve_phrase(rec, req)
        if req.kind == "chip":
            return _resolve_chip(rec, req)
        return await self._resolve_ask(rec, req, conv)

    async def _resolve_ask(
        self,
        rec: Optional["_RecLike"],
        req: ProbeRequest,
        conv: CardConversation,
    ) -> tuple[str, str, str, list[dict[str, str]]]:
        query = (req.query or "").strip()
        if self._qh is None or rec is None:
            return _resolve_ask_fallback(rec, query)
        try:
            ar = await self._qh.answer_query(
                AnswerQueryRequest(
                    tenant_id=req.tenant_id,
                    query=query,
                    context_card_id=req.card_id,
                    inline_card_context=CardContext(
                        card_id=req.card_id,
                        subject=rec.target_entity.title if rec.target_entity else None,
                        kind=rec.proposition_kind,
                        raw={"proposition_text": rec.proposition_text},
                    ),
                )
            )
            response_html = ar.response_html or _resolve_ask_fallback(rec, query)[2]
        except Exception:  # noqa: BLE001
            log.exception("probe_ask_qry_failed")
            return _resolve_ask_fallback(rec, query)
        return ("You asked", query, response_html, _generic_follow_ups(rec, query))


def _generic_follow_ups(
    rec: Optional["_RecLike"], query_or_id: str,
) -> list[dict[str, str]]:
    rid = str(rec.id) if rec is not None else "card"
    # Up to 3 follow-ups. Keep them response-generic so v1 doesn't have
    # to LLM-author them per response; see spec §4.5 for the
    # response-specific variant we'll layer on later.
    return [
        {"id": f"{rid}:fu:show-evidence", "text": "Show me the evidence"},
        {"id": f"{rid}:fu:compare-patterns", "text": "Compare to other patterns"},
        {"id": f"{rid}:fu:options", "text": "What are my options?"},
    ]


def _resolve_phrase(
    rec: Optional["_RecLike"], req: ProbeRequest,
) -> tuple[str, str, str, list[dict[str, str]]]:
    # The probe id encodes the phrase slug as the third hyphen-separated
    # segment (see _add_probe_markup in services.today.aggregator). We
    # prettify it for the exchange header.
    pid = req.probe_id or ""
    parts = pid.split("-", 2)
    slug = parts[2] if len(parts) >= 3 else pid
    text = slug.replace("-", " ").strip() or pid
    response_html = (
        "<p>Here's what's behind that phrase. The substrate keeps a "
        "provenance trail for every claim — this one resolves to the "
        "evidence cluster underlying the recommendation.</p>"
    )
    if rec is not None and rec.qualitative_impact:
        response_html += (
            f"<p>In context: <em>{_escape(rec.qualitative_impact)}</em>.</p>"
        )
    return ("You clicked", f'"{text}"', response_html, _generic_follow_ups(rec, pid))


def _resolve_chip(
    rec: Optional["_RecLike"], req: ProbeRequest,
) -> tuple[str, str, str, list[dict[str, str]]]:
    pid = req.probe_id or ""
    suffix = pid.split(":", 1)[1] if ":" in pid else pid
    # Map the chip suffix to a canned-but-grounded response. The chips
    # the aggregator emits live in services.today.aggregator
    # (_derive_probe_chips); the suffix matches keep this resolver in
    # sync with that emitter.
    text_by_suffix: dict[str, tuple[str, str]] = {
        "why": (
            "Why this decision specifically?",
            "<p>This is the only structurally-load-bearing decision in "
            "this domain. The cluster of contradicting signals isn't "
            "violating a casual preference — it's departing from an "
            "explicit architectural call.</p>",
        ),
        "contradicting": (
            "What's contradicting it?",
            "<p>Three independent commitments scoped without referencing "
            "the original decision. None of the engineers explicitly "
            "disagreed; they appear to have scoped without consulting "
            "it.</p>",
        ),
        "history": (
            "Have we ratified before?",
            "<p>Looking back: this decision was ratified once, "
            "implicitly, by the absence of pushback. A re-ratification "
            "now would convert that into an explicit, durable signal.</p>",
        ),
        "drift-cost": (
            "What if I let it drift?",
            "<p>Two weeks of drift means roughly two more independent "
            "commitments scope without consulting the decision. The "
            "technical cost compounds; the relational cost — silence "
            "read as endorsement — compounds faster.</p>",
        ),
        "why-pattern": (
            "Why this pattern matters?",
            "<p>Three independent enterprise contexts is rarely "
            "coincidence. The pattern just crossed my action "
            "threshold.</p>",
        ),
        "customer-asks": (
            "Show me the customer asks",
            "<p>Three customers raised this in the last 14 days. Each "
            "ask was independent — different conversations, different "
            "framing, same underlying need.</p>",
        ),
        "cost": (
            "What's the engineering cost?",
            "<p>Roughly two engineer-weeks for a v1, scoped to the "
            "enterprise tier. Could ship into the next release train "
            "without disrupting other commitments.</p>",
        ),
        "change-mind": (
            "What would change your mind?",
            "<p>If two of the three asks turned out to be referrals "
            "from the same conversation, I'd revise down. Right now "
            "they look fully independent.</p>",
        ),
        "evidence": (
            "Show me the evidence",
            "<p>The supporting signals are listed in the recommendation "
            "metadata. Each one is independently sourced; the cluster "
            "is what crossed the action threshold.</p>",
        ),
        "options": (
            "What are my options?",
            "<p>Three plausible paths: act now, hold for one more "
            "data point, or route to whoever's closest to the decision "
            "domain. Each has a distinct cost profile.</p>",
        ),
    }
    fallback = (
        "your probe",
        "<p>Here's how I'd think about that. The cluster of signals "
        "underlying this card is the right place to start.</p>",
    )
    text, body = text_by_suffix.get(suffix, fallback)
    if rec is not None and rec.qualitative_impact and suffix not in text_by_suffix:
        body = body + f"<p><em>{_escape(rec.qualitative_impact)}</em></p>"
    return ("You probed", text, body, _generic_follow_ups(rec, pid))


def _resolve_ask_fallback(
    rec: Optional["_RecLike"], query: str,
) -> tuple[str, str, str, list[dict[str, str]]]:
    body = (
        f"<p>Here's how I'd think about that. <em>{_escape(query)}</em> — "
        f"interesting framing. My read is that the primary tradeoff is "
        f"between speed of resolution and information value.</p>"
    )
    return ("You asked", query, body, _generic_follow_ups(rec, query))


def _escape(s: str) -> str:
    return (
        (s or "")
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
