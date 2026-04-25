"""LLM-mediated diff translation layer.

Baselines 2-6 don't natively produce Company-OS-shaped diffs. Each of them
calls out to a :class:`DiffTranslator` to turn its native retrieval /
memory output into a :class:`~lsob_contracts.diff.DiffOp`.

For Phase 1, the only concrete implementation is
:class:`TemplateDiffTranslator`, which uses deterministic heuristic rules
and a ``"dummy-llm"`` rationale placeholder. An Anthropic-backed
``LLMDiffTranslator`` is a Phase 2 TODO (see ``heavy`` optional dependency
group).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Protocol

from lsob_contracts import ActOp, ClaimOp, DiffOp, Trigger


class DiffTranslator(Protocol):
    """Translate baseline-native retrieval output into a ``DiffOp``."""

    def translate(
        self,
        *,
        trigger: Trigger,
        retrieved_context: str,
        evidence_signal_ids: list[str],
        entities: list[str] | None = None,
    ) -> DiffOp: ...


@dataclass
class TemplateDiffTranslator:
    """Deterministic, rule-based diff translator for hermetic tests.

    The translator inspects the trigger kind and some keywords in the
    retrieved context to decide whether to emit a claim op, an act op, or
    both. All free-text fields are prefixed with ``dummy-llm:`` so that
    downstream evaluators can distinguish rule-based rationales from real
    LLM outputs.
    """

    confidence: float = 0.6
    model_name: str = "dummy-llm"
    keyword_map: dict[str, str] = field(
        default_factory=lambda: {
            "slip": "at_risk_of_slipping",
            "delay": "at_risk_of_slipping",
            "snag": "at_risk_of_slipping",
            "escalat": "customer_at_risk",
            "churn": "customer_at_risk",
            "degraded": "customer_at_risk",
            "merged": "commitment_resolved",
            "done": "commitment_resolved",
            "shipped": "commitment_resolved",
        }
    )

    def translate(
        self,
        *,
        trigger: Trigger,
        retrieved_context: str,
        evidence_signal_ids: list[str],
        entities: list[str] | None = None,
    ) -> DiffOp:
        ctx = (retrieved_context or "").lower()
        ents = list(entities or [])
        claim_kind = self._classify(ctx, trigger)
        target_ref = self._pick_target(trigger, ents)
        claim_id = f"claim-{uuid.uuid5(uuid.NAMESPACE_OID, trigger.trigger_id + claim_kind).hex[:12]}"

        claim = ClaimOp(
            op="upsert_claim",
            claim_id=claim_id,
            proposition=self._render_proposition(claim_kind, target_ref, ctx),
            proposition_kind=claim_kind,
            asserted_confidence=self.confidence,
            falsifier=f"dummy-llm: falsifier for {claim_kind}",
            evidence_signal_ids=list(evidence_signal_ids),
            entities=[target_ref] if target_ref else [],
        )

        act_ops: list[ActOp] = []
        if claim_kind == "at_risk_of_slipping" and target_ref:
            act_ops.append(
                ActOp(
                    op="transition",
                    entity_ref=target_ref,
                    from_state="open",
                    to_state="at_risk",
                    reason="dummy-llm: retrieval indicates slippage",
                )
            )
        elif claim_kind == "commitment_resolved" and target_ref:
            act_ops.append(
                ActOp(
                    op="transition",
                    entity_ref=target_ref,
                    from_state="open",
                    to_state="resolved",
                    reason="dummy-llm: retrieval indicates resolution",
                )
            )

        return DiffOp(
            diff_id=f"diff-{uuid.uuid4().hex[:12]}",
            produced_at=datetime.now(tz=timezone.utc),
            trigger_id=trigger.trigger_id,
            claim_ops=[claim],
            act_ops=act_ops,
            resource_ops=[],
            rationale=f"dummy-llm: {claim_kind} inferred from retrieved context",
            metadata={"translator": "template", "model": self.model_name},
        )

    def _classify(self, ctx: str, trigger: Trigger) -> str:
        for kw, kind in self.keyword_map.items():
            if kw in ctx:
                return kind
        kind_hint = (trigger.kind or "").lower()
        if "risk" in kind_hint or "slip" in kind_hint:
            return "at_risk_of_slipping"
        if "customer" in kind_hint:
            return "customer_at_risk"
        return "status_summary"

    def _pick_target(self, trigger: Trigger, entities: list[str]) -> str:
        payload_ref = (
            trigger.payload.get("entity_ref")
            or trigger.payload.get("commitment_ref")
            or trigger.payload.get("customer_ref")
        )
        if isinstance(payload_ref, str) and payload_ref:
            return payload_ref
        if entities:
            return entities[0]
        return ""

    def _render_proposition(self, kind: str, ref: str, ctx: str) -> str:
        snippet = (ctx[:80] + "…") if len(ctx) > 80 else ctx
        ref_txt = ref or "unknown-entity"
        return f"dummy-llm[{kind}]: {ref_txt} — {snippet}".strip()
