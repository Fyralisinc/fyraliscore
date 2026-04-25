"""
'summary' strategy — time-bounded retrieval.

The CEO asks "what happened yesterday" / "recap the week". We run
a temporal-heavy retrieval (T1 with a very wide C weight) over the
parsed window. Model set is secondary — the renderer wants
observations + state_changes in the window.
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


class SummaryStrategy:
    category = "summary"

    def parse(
        self,
        query: str,
        *,
        conversation_history: list[Any] | None = None,
        card_context: Any | None = None,
    ) -> ParsedQuery:
        now = datetime.now(timezone.utc)
        anchor, window = extract_time_window(query, now=now)
        # Default to "last 24h" if the user didn't say a window.
        if window is None:
            window = timedelta(days=1)
            anchor = now - window
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
        seed_time = parsed.time_anchor or (now - (parsed.time_window or timedelta(days=1)))
        return TriggerContext(
            kind="T1",
            tenant_id=tenant_id,
            seed_natural_text=parsed.raw_query,
            seed_occurred_at=seed_time,
            temporal_window=parsed.time_window or timedelta(days=1),
            # Lighter semantic pathway: summary wants the "what
            # happened" structure over the semantic near-neighbors.
            semantic_k=20,
        )

    async def gather(
        self, parsed: ParsedQuery, ctx: StrategyContext
    ) -> StrategyResult:
        trigger = self.build_trigger(parsed, ctx.tenant_id, now=ctx.now)
        retrieval_result, bundle = await run_retrieval(
            trigger, ctx,
            # Summary leans on observations — lift the obs budget.
            budget_observations=40,
        )
        notes = {
            "strategy": "summary",
            "time_window_seconds": int(
                (parsed.time_window or timedelta(days=1)).total_seconds()
            ),
            "time_anchor": (
                parsed.time_anchor.isoformat()
                if parsed.time_anchor else None
            ),
            "observations_surfaced": len(bundle.observations),
            "state_changes_hint": True,  # rendering joins state_changes
        }
        return StrategyResult(
            parsed=parsed,
            retrieval_result=retrieval_result,
            context_bundle=bundle,
            notes=notes,
        )


strategy = SummaryStrategy()
