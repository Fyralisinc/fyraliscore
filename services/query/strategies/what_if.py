"""
'what_if' strategy — counterfactual reasoning.

Retrieves Models that bear on the subject + any similar past patterns
(pathway D is where Precipitation's T4 patterns live). We mark the
retrieved bundle's notes with `counterfactual=True` so the rendering
layer knows to hedge (the answer is hypothetical).
"""
from __future__ import annotations

import re
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


_HYPOTHESIS_RE = re.compile(r"\bwhat if\b(.*?)(?:\?|$)", re.I)


class WhatIfStrategy:
    category = "what_if"

    def parse(
        self,
        query: str,
        *,
        conversation_history: list[Any] | None = None,
        card_context: Any | None = None,
    ) -> ParsedQuery:
        now = datetime.now(timezone.utc)
        anchor, window = extract_time_window(query, now=now)
        hyp = None
        m = _HYPOTHESIS_RE.search(query or "")
        if m:
            hyp = m.group(1).strip().rstrip(".!,") or None
        parsed = ParsedQuery(
            raw_query=query,
            category=self.category,
            person_mentions=extract_persons(query),
            customer_mentions=extract_customer_candidates(query),
            subject_keywords=extract_subject_keywords(query),
            counterfactual_hypothesis=hyp,
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
        # T2 mix (A + D) is the closest fit: we want Models + patterns.
        # T2 canonically carries a model_id; we don't have one. We
        # still use the T2 trigger because Pathway A will bootstrap
        # from the seed_entity_ids list even without a model_id (and
        # pathway_d happily runs on a seed_signature we build below).
        seed_signature: dict[str, Any] = {
            "kind": "counterfactual",
            "text": parsed.counterfactual_hypothesis or parsed.raw_query,
        }
        return TriggerContext(
            kind="T2",
            tenant_id=tenant_id,
            seed_natural_text=parsed.raw_query,
            seed_occurred_at=parsed.time_anchor or now,
            temporal_window=parsed.time_window or timedelta(days=30),
            seed_signature=seed_signature,
        )

    async def gather(
        self, parsed: ParsedQuery, ctx: StrategyContext
    ) -> StrategyResult:
        trigger = self.build_trigger(parsed, ctx.tenant_id, now=ctx.now)
        retrieval_result, bundle = await run_retrieval(trigger, ctx)
        notes = {
            "strategy": "what_if",
            "counterfactual": True,
            "hypothesis": parsed.counterfactual_hypothesis,
            "models_surfaced": len(bundle.models),
            # Rendering layer is expected to hedge: "if X, then Y is
            # likely because..." — never state a counterfactual as fact.
            "rendering_hints": {
                "hedge_language": True,
                "mark_uncertain": True,
            },
        }
        return StrategyResult(
            parsed=parsed,
            retrieval_result=retrieval_result,
            context_bundle=bundle,
            notes=notes,
        )


strategy = WhatIfStrategy()
