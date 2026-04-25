"""
'draft' strategy — compose a message.

Needs three things:
  1. Recipient's communication patterns (how do we usually talk to them?)
  2. Subject context (what's the situation the message is about?)
  3. Sender voice (the CEO's register).

(1) is pulled from prior Observations where `source_actor_ref` matches
the recipient alias — retrieval's Pathway A handles this once we seed
an actor lookup. For now the strategy returns the same bundle-shape
as the others, plus extra `draft_hints` on the notes dict that the
rendering layer consumes.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID

from services.retrieval.primary import TriggerContext

from .base import (
    ParsedQuery,
    StrategyContext,
    StrategyResult,
    extract_customer_candidates,
    extract_persons,
    extract_subject_keywords,
    extract_time_window,
    parse_recipient,
    run_retrieval,
)


class DraftStrategy:
    category = "draft"

    def parse(
        self,
        query: str,
        *,
        conversation_history: list[Any] | None = None,
        card_context: Any | None = None,
    ) -> ParsedQuery:
        now = datetime.now(timezone.utc)
        anchor, window = extract_time_window(query, now=now)
        parsed = ParsedQuery(
            raw_query=query,
            category=self.category,
            person_mentions=extract_persons(query),
            customer_mentions=extract_customer_candidates(query),
            subject_keywords=extract_subject_keywords(query),
            recipient=parse_recipient(query),
            sender="ceo",
            time_window=window,
            time_anchor=anchor,
        )
        # Card context frequently provides the subject of the draft.
        if card_context is not None:
            subject = getattr(card_context, "subject", None) or (
                card_context.get("subject")
                if isinstance(card_context, dict) else None
            )
            if subject:
                parsed.trace["card_subject"] = subject
                parsed.subject_keywords.append(str(subject).lower())
            # Draft-from-card often implies the recipient is the card's
            # "audience" verb (cards carry verbs like "draft to Marcus").
            verb_recipient = getattr(card_context, "recipient", None) or (
                card_context.get("recipient")
                if isinstance(card_context, dict) else None
            )
            if verb_recipient and not parsed.recipient:
                parsed.recipient = str(verb_recipient).lower()
        return parsed

    def build_trigger(
        self,
        parsed: ParsedQuery,
        tenant_id: UUID,
        *,
        now: datetime,
    ) -> TriggerContext:
        # We want BOTH the subject and the recipient in the retrieval
        # seed. The natural-text seed bundles them; Pathway B picks
        # up the subject, Pathway A's actor-lookup arm picks up the
        # recipient (once the call-site supplies actor seeds — see
        # scope_actors below).
        seed_text = parsed.raw_query
        if parsed.recipient:
            seed_text = f"{seed_text}\nrecipient: {parsed.recipient}"
        return TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=seed_text,
            seed_occurred_at=parsed.time_anchor or now,
            temporal_window=parsed.time_window or timedelta(days=21),
            semantic_k=50,
        )

    async def gather(
        self, parsed: ParsedQuery, ctx: StrategyContext
    ) -> StrategyResult:
        trigger = self.build_trigger(parsed, ctx.tenant_id, now=ctx.now)
        retrieval_result, bundle = await run_retrieval(trigger, ctx)
        notes = {
            "strategy": "draft",
            "recipient": parsed.recipient,
            "sender": parsed.sender,
            "subject_keywords": parsed.subject_keywords,
            "draft_hints": {
                # Rendering layer reads these to tune voice.
                "needs_recipient_voice": True,
                "needs_subject_context": True,
                "sender_voice_anchor": "ceo",
            },
            "models_surfaced": len(bundle.models),
        }
        return StrategyResult(
            parsed=parsed,
            retrieval_result=retrieval_result,
            context_bundle=bundle,
            notes=notes,
        )


strategy = DraftStrategy()
