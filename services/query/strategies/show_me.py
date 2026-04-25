"""
'show_me' strategy — direct structural retrieval, minimal reasoning.

The CEO wants a lookup, not a story. We favor Pathway A (structural)
and cap semantic to a minimum. Temporal is left off unless the query
mentions one (we don't want "show customers with health != healthy"
to filter by last-week's updates).
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


class ShowMeStrategy:
    category = "show_me"

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
        # T1 mix but with a tighter semantic_k because we want the
        # structural skeleton; semantic is here only to catch the
        # entity seed text.
        seed_text = parsed.raw_query
        return TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=seed_text,
            seed_occurred_at=parsed.time_anchor or now,
            temporal_window=parsed.time_window or timedelta(days=7),
            semantic_k=25,
        )

    async def gather(
        self, parsed: ParsedQuery, ctx: StrategyContext
    ) -> StrategyResult:
        trigger = self.build_trigger(parsed, ctx.tenant_id, now=ctx.now)
        # Lift the model budget: "show me" answers are often lists.
        retrieval_result, bundle = await run_retrieval(
            trigger, ctx, budget_models=60,
        )
        notes = {
            "strategy": "show_me",
            "lookup_mode": "structural",
            "models_surfaced": len(bundle.models),
            "resources_surfaced": len(bundle.resources_summary),
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


strategy = ShowMeStrategy()
