"""
'why' strategy — causal reasoning.

Per the build plan: retrieves the Models about the subject entity,
their supporting_event_ids, and the Acts they relate to.

Trigger: T1-like (A+B+C, structural + semantic + temporal). We lean
A+B slightly heavier than vanilla T1 because the subject is usually
named in the query (so structural seeds are strong). Temporal window
is wider than usual because a "why" question can be causally reaching
back weeks.
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
    run_retrieval,
)


class WhyStrategy:
    """Concrete strategy. Module-level `strategy` is the singleton."""

    category = "why"

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
            time_window=window or timedelta(days=14),   # 'why' reaches back
            time_anchor=anchor,
        )
        # Pull the last user turn's subject into the keyword seed on
        # follow-ups ("why?" after "what about Acme?").
        if conversation_history:
            for turn in reversed(conversation_history):
                prev_query = getattr(turn, "query", None) or (
                    turn.get("query") if isinstance(turn, dict) else None
                )
                if prev_query:
                    parsed.subject_keywords += extract_subject_keywords(prev_query)
                    break
        # Card context contributes its subject label when present.
        if card_context is not None:
            subject = getattr(card_context, "subject", None) or (
                card_context.get("subject")
                if isinstance(card_context, dict) else None
            )
            if subject:
                parsed.subject_keywords.append(str(subject).lower())
                parsed.trace["card_subject"] = subject
        return parsed

    def build_trigger(
        self,
        parsed: ParsedQuery,
        tenant_id: UUID,
        *,
        now: datetime,
    ) -> TriggerContext:
        seed_text = " ".join(
            [parsed.raw_query] + parsed.subject_keywords
        ).strip()
        # Temporal seed: either the parsed anchor or now (falls back to
        # "recent" window).
        seed_time = parsed.time_anchor or now
        return TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=seed_text,
            seed_occurred_at=seed_time,
            temporal_window=parsed.time_window or timedelta(days=14),
            semantic_k=60,   # slightly deeper than default for 'why'
        )

    async def gather(
        self, parsed: ParsedQuery, ctx: StrategyContext
    ) -> StrategyResult:
        trigger = self.build_trigger(parsed, ctx.tenant_id, now=ctx.now)
        retrieval_result, bundle = await run_retrieval(trigger, ctx)
        # Annotate bundle with 'why'-specific notes for the renderer.
        notes = {
            "strategy": "why",
            "models_surfaced": len(bundle.models),
            "supporting_event_count": sum(
                len(m.supporting_event_ids or []) for m in bundle.models
            ),
            "subject_keywords": parsed.subject_keywords,
        }
        return StrategyResult(
            parsed=parsed,
            retrieval_result=retrieval_result,
            context_bundle=bundle,
            notes=notes,
        )


strategy = WhyStrategy()
