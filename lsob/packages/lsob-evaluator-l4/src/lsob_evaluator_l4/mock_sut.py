"""A deterministic mock surfacing SUT used for tests and the CLI demo.

The real Layer 4 evaluator wires to any SUT implementing
:class:`lsob_contracts.SystemUnderTest` plus (optionally) the
:class:`AnomalyEmittingSUT` protocol from ``l4_protocol``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

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


class MockSurfacingSUT:
    """Canned responses for every SystemUnderTest surface L4 touches.

    The mock does not actually ingest anything; it just plays back whatever
    ``canned_at_risk`` / ``canned_anomalies`` the caller configures.
    """

    name: str = "mock-surfacing-sut"
    max_concurrent_ingestion: int = 1

    def __init__(
        self,
        *,
        canned_at_risk: dict[datetime, list[AtRiskItem]] | None = None,
        canned_anomalies: list[dict[str, Any]] | None = None,
    ) -> None:
        self.canned_at_risk = canned_at_risk or {}
        self.canned_anomalies = canned_anomalies or []

    # -- SystemUnderTest protocol methods --------------------------------

    async def startup(self, config: SUTConfig) -> None:
        return None

    async def apply_ablation(self, ablation: AblationConfig) -> None:
        return None

    async def ingest_signal(self, signal: Signal) -> None:
        return None

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        return []

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:
        # Exact key match first; fall back to the closest earlier configured
        # checkpoint so tests that pass slightly-offset timestamps still work.
        items = self.canned_at_risk.get(timestamp)
        if items is None and self.canned_at_risk:
            earlier = [ts for ts in self.canned_at_risk if ts <= timestamp]
            if earlier:
                items = self.canned_at_risk[max(earlier)]
        return AtRiskReport(timestamp=timestamp, items=list(items or []))

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        return DiffOp(
            diff_id=f"mock-diff-{trigger.trigger_id}",
            produced_at=trigger.timestamp,
            trigger_id=trigger.trigger_id,
        )

    async def shutdown(self) -> None:
        return None

    # -- AnomalyEmittingSUT extra surface --------------------------------

    async def emitted_anomalies(
        self, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        return [
            a
            for a in self.canned_anomalies
            if start <= a["timestamp"] < end
        ]


def make_commitment_at_risk(commitment_id: str, score: float = 0.8) -> AtRiskItem:
    return AtRiskItem(
        entity_ref=EntityRef(kind="commitment", id=commitment_id),
        risk_score=score,
        risk_kind="commitment_slip",
    )


def make_customer_at_risk(customer_id: str, score: float = 0.7) -> AtRiskItem:
    return AtRiskItem(
        entity_ref=EntityRef(kind="customer", id=customer_id),
        risk_score=score,
        risk_kind="customer_health",
    )
