"""
'arbitrary' strategy — default bucket.

Runs a vanilla T1 retrieval with a moderate window. Deliberately does
not over-specialise: if the query genuinely doesn't fit a bucket we'd
rather give balanced retrieval than guess wrong.
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


class ArbitraryStrategy:
    category = "arbitrary"

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
            time_window=window,
            time_anchor=anchor,
        )
        if card_context is not None:
            subj = getattr(card_context, "subject", None) or (
                card_context.get("subject") if isinstance(card_context, dict) else None
            )
            if subj:
                parsed.trace["card_subject"] = subj
                parsed.subject_keywords.append(str(subj).lower())
        return parsed

    def build_trigger(
        self,
        parsed: ParsedQuery,
        tenant_id: UUID,
        *,
        now: datetime,
    ) -> TriggerContext:
        return TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=parsed.raw_query,
            seed_occurred_at=parsed.time_anchor or now,
            temporal_window=parsed.time_window or timedelta(days=7),
            semantic_k=40,
        )

    async def gather(
        self, parsed: ParsedQuery, ctx: StrategyContext
    ) -> StrategyResult:
        trigger = self.build_trigger(parsed, ctx.tenant_id, now=ctx.now)
        retrieval_result, bundle = await run_retrieval(trigger, ctx)
        notes = {
            "strategy": "arbitrary",
            "models_surfaced": len(bundle.models),
            "acts_surfaced": {
                k: len(v) for k, v in bundle.acts_summary.items()
            },
        }
        return StrategyResult(
            parsed=parsed,
            retrieval_result=retrieval_result,
            context_bundle=bundle,
            notes=notes,
        )


strategy = ArbitraryStrategy()
