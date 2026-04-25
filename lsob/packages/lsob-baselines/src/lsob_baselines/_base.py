"""Shared scaffolding for in-memory baseline SUTs.

Pulls out the bits that are identical across baselines 2-6: async-safe
signal storage, ``apply_ablation``/``shutdown`` no-ops, and a utility for
picking evidence signal ids from a belief-query's entity reference.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterable

from lsob_contracts import (
    AblationConfig,
    AtRiskItem,
    AtRiskReport,
    Belief,
    BeliefQuery,
    DiffOp,
    EntityRef,
    Signal,
    SUTConfig,
    Trigger,
)


@dataclass
class BaselineState:
    """Common mutable state kept by every in-memory baseline."""

    signals: list[Signal] = field(default_factory=list)
    ablation: AblationConfig | None = None
    config: SUTConfig | None = None
    started: bool = False


def signals_mentioning(
    signals: Iterable[Signal], entity_ref: EntityRef, before: datetime
) -> list[Signal]:
    """Return signals whose metadata / text mentions ``entity_ref`` at or before ``before``."""

    key_variants = {
        entity_ref.id,
        entity_ref.id.lower(),
    }
    out: list[Signal] = []
    for sig in signals:
        if sig.timestamp > before:
            continue
        # Metadata match on any *_ref field.
        meta_hit = False
        for v in sig.metadata.values():
            if isinstance(v, str) and v in key_variants:
                meta_hit = True
                break
        if meta_hit or any(k in sig.content_text for k in key_variants):
            out.append(sig)
    return out


def make_belief_from_signals(
    query: BeliefQuery,
    signals: list[Signal],
    proposition_kind: str,
    confidence: float,
    source: str,
) -> Belief:
    """Construct a Belief from a collection of evidence signals."""

    evidence_ids = [s.signal_id for s in signals]
    last_ts = max((s.timestamp for s in signals), default=query.timestamp)
    summary = "; ".join(s.content_text[:60] for s in signals[-3:]) or "no evidence"
    claim_id = f"claim-{uuid.uuid5(uuid.NAMESPACE_OID, query.query_id + source).hex[:12]}"
    return Belief(
        claim_id=claim_id,
        proposition=f"{source}: {query.entity_ref.kind}:{query.entity_ref.id} — {summary}",
        proposition_kind=proposition_kind,
        asserted_confidence=confidence,
        last_updated=last_ts,
        entities=[query.entity_ref.id],
        evidence_signal_ids=evidence_ids,
    )


def empty_at_risk_report(ts: datetime) -> AtRiskReport:
    return AtRiskReport(timestamp=ts, items=[])


def simple_at_risk_from_signals(
    signals: list[Signal], ts: datetime
) -> AtRiskReport:
    """Very small rule to produce an at-risk report.

    Flags any commitment whose signals contain ``slip``/``snag`` and any
    customer whose signals contain ``escalat``/``degraded``.
    """

    slip_targets: dict[str, list[str]] = {}
    customer_targets: dict[str, list[str]] = {}
    for s in signals:
        if s.timestamp > ts:
            continue
        low = s.content_text.lower()
        cref = s.metadata.get("commitment_ref")
        if isinstance(cref, str) and ("slip" in low or "snag" in low or s.metadata.get("slip_signal")):
            slip_targets.setdefault(cref, []).append(s.signal_id)
        custref = s.metadata.get("customer_ref")
        if isinstance(custref, str) and (
            "escalat" in low or "degraded" in low or s.metadata.get("health_signal") == "degraded"
        ):
            customer_targets.setdefault(custref, []).append(s.signal_id)

    items: list[AtRiskItem] = []
    for cid, evs in slip_targets.items():
        items.append(
            AtRiskItem(
                entity_ref=EntityRef(kind="commitment", id=cid),
                risk_score=min(1.0, 0.4 + 0.2 * len(evs)),
                risk_kind="commitment_slip",
                rationale=f"dummy-llm: {len(evs)} slip-like signals",
            )
        )
    for cid, evs in customer_targets.items():
        items.append(
            AtRiskItem(
                entity_ref=EntityRef(kind="customer", id=cid),
                risk_score=min(1.0, 0.4 + 0.2 * len(evs)),
                risk_kind="customer_health",
                rationale=f"dummy-llm: {len(evs)} degradation-like signals",
            )
        )
    return AtRiskReport(timestamp=ts, items=items)


__all__ = [
    "AblationConfig",
    "AtRiskReport",
    "BaselineState",
    "Belief",
    "BeliefQuery",
    "DiffOp",
    "Signal",
    "SUTConfig",
    "Trigger",
    "empty_at_risk_report",
    "make_belief_from_signals",
    "signals_mentioning",
    "simple_at_risk_from_signals",
]
