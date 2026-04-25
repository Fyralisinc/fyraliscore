"""Test-only SUT that returns deterministic beliefs for prediction IDs it knows about."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from lsob_contracts import (
    AblationConfig,
    AtRiskReport,
    Belief,
    BeliefQuery,
    Signal,
    SUTConfig,
    Trigger,
)
from lsob_contracts.diff import DiffOp


class MockCalibratedSUT:
    """Returns pre-seeded beliefs. Used by tests and the CLI's ``--sut mock`` mode.

    ``seed_beliefs`` maps ``prediction_id -> Belief`` so the evaluator can look up the confidence
    the SUT would have asserted at resolution time. Any prediction id not in the map is treated as
    "prediction not made".
    """

    name: str = "mock-calibrated-sut"
    max_concurrent_ingestion: int = 1

    def __init__(self, seed_beliefs: dict[str, Belief] | None = None) -> None:
        self._beliefs: dict[str, Belief] = dict(seed_beliefs or {})

    # SystemUnderTest protocol ------------------------------------------------
    async def startup(self, config: SUTConfig) -> None:  # noqa: ARG002
        return None

    async def apply_ablation(self, ablation: AblationConfig) -> None:  # noqa: ARG002
        return None

    async def ingest_signal(self, signal: Signal) -> None:  # noqa: ARG002
        return None

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        # Metadata channel: prediction id lives in ``query_id`` for this mock.
        b = self._beliefs.get(query.query_id)
        return [b] if b is not None else []

    async def query_at_risk_at(self, timestamp: datetime) -> AtRiskReport:  # noqa: ARG002
        return AtRiskReport(timestamp=timestamp, items=[])

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        return DiffOp(
            diff_id=f"mock-diff-{trigger.trigger_id}",
            produced_at=trigger.timestamp,
            trigger_id=trigger.trigger_id,
        )

    async def shutdown(self) -> None:
        return None

    # Convenience helpers for tests ------------------------------------------
    def register(self, prediction_id: str, belief: Belief) -> None:
        self._beliefs[prediction_id] = belief

    @classmethod
    def from_predictions(
        cls,
        predictions: list[dict[str, Any]],
        *,
        actor_id: str = "unknown",
        proposition_kind: str = "unknown",
        confidence_override: dict[str, float] | None = None,
    ) -> "MockCalibratedSUT":
        """Build a SUT that mirrors the ground-truth predictions' asserted confidences.

        ``confidence_override`` can swap in a different confidence per prediction id (used by tests
        that want imperfect calibration).
        """
        beliefs: dict[str, Belief] = {}
        overrides = confidence_override or {}
        for pred in predictions:
            pid = pred["prediction_id"]
            conf = overrides.get(pid, pred["asserted_confidence"])
            resolves_at = pred["resolves_at"]
            if isinstance(resolves_at, str):
                resolves_at = datetime.fromisoformat(resolves_at.replace("Z", "+00:00"))
            beliefs[pid] = Belief(
                claim_id=pid,
                proposition=pred["proposition"],
                proposition_kind=pred.get("proposition_kind", proposition_kind),
                asserted_confidence=float(conf),
                last_updated=resolves_at,
                entities=[f"actor:{pred.get('actor_id', actor_id)}"],
                evidence_signal_ids=[],
            )
        return cls(beliefs)
