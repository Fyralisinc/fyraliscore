"""Lightweight MockBeliefSUT used exclusively in tests.

It satisfies the subset of the `SystemUnderTest` protocol that Layer 2
exercises — namely `query_beliefs_at` — plus startup/shutdown stubs.
Other protocol methods raise NotImplementedError so misuse is loud.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Callable

from lsob_contracts import Belief, BeliefQuery


@dataclass
class MockBelief:
    """Canned belief entry keyed by (entity_kind, entity_id)."""

    proposition: str
    proposition_kind: str
    asserted_confidence: float = 0.8
    entities: list[str] = field(default_factory=list)


class MockBeliefSUT:
    """A deterministic stand-in SUT with fully canned belief answers.

    Callers supply a dict keyed by (entity_kind, entity_id) → list of
    `MockBelief`. Unknown keys yield an empty list by default, but can be
    made to raise via `raise_on_unknown=True` — used by tests that want to
    verify the evaluator degrades to `layer_not_applicable`.
    """

    name: str = "mock-belief-sut"
    max_concurrent_ingestion: int = 1

    def __init__(
        self,
        canned: dict[tuple[str, str], list[MockBelief]] | None = None,
        *,
        raise_on_unknown: bool = False,
        fail_predicate: Callable[[BeliefQuery], bool] | None = None,
    ) -> None:
        self.canned = canned or {}
        self.raise_on_unknown = raise_on_unknown
        self.fail_predicate = fail_predicate
        self.queries_seen: list[BeliefQuery] = []

    # -- protocol stubs ------------------------------------------------------

    async def startup(self, config: Any) -> None:  # pragma: no cover - trivial
        return None

    async def apply_ablation(self, ablation: Any) -> None:  # pragma: no cover
        return None

    async def ingest_signal(self, signal: Any) -> None:  # pragma: no cover
        return None

    async def shutdown(self) -> None:  # pragma: no cover - trivial
        return None

    async def query_at_risk_at(self, timestamp: datetime) -> Any:  # pragma: no cover
        raise NotImplementedError

    async def produce_diff_for_trigger(self, trigger: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    # -- L2 surface ----------------------------------------------------------

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        self.queries_seen.append(query)
        if self.fail_predicate is not None and self.fail_predicate(query):
            raise RuntimeError(
                f"mock SUT cannot serve query {query.query_id}"
            )
        key = (query.entity_ref.kind, query.entity_ref.id)
        if key not in self.canned:
            if self.raise_on_unknown:
                raise RuntimeError(
                    f"no canned answer for {key} at {query.timestamp}"
                )
            return []
        out: list[Belief] = []
        for idx, mb in enumerate(self.canned[key]):
            out.append(
                Belief(
                    claim_id=f"mock-{query.query_id}-{idx}",
                    proposition=mb.proposition,
                    proposition_kind=mb.proposition_kind,
                    asserted_confidence=mb.asserted_confidence,
                    last_updated=query.timestamp,
                    entities=mb.entities or [query.entity_ref.id],
                    evidence_signal_ids=[],
                )
            )
        return out


def mock_from_ground_truth(
    ground_truths: list[Any],
    *,
    perfect_commitments: bool = True,
    perfect_customers: bool = True,
    perfect_patterns: bool = True,
    perfect_predictions: bool = True,
    prediction_confidence: float = 0.9,
) -> MockBeliefSUT:
    """Build a MockBeliefSUT that returns the ground-truth answer for every
    belief, optionally flipping specific dimensions to "wrong" for testing.
    """
    canned: dict[tuple[str, str], list[MockBelief]] = {}
    for gt in ground_truths:
        for c in gt.commitments:
            state = c["true_outcome"] if perfect_commitments else "will_succeed"
            canned.setdefault(("commitment", c["id"]), []).append(
                MockBelief(
                    proposition=f"state={state}",
                    proposition_kind="commitment_state",
                    entities=[c["id"]],
                )
            )
        for cu in gt.customers:
            health = cu["true_health"] if perfect_customers else "healthy"
            canned.setdefault(("customer", cu["id"]), []).append(
                MockBelief(
                    proposition=f"health={health}",
                    proposition_kind="customer_health",
                    entities=[cu["id"]],
                )
            )
        if perfect_patterns:
            for p in gt.patterns:
                canned.setdefault(("pattern", p["id"]), []).append(
                    MockBelief(
                        proposition=p["description"],
                        proposition_kind="pattern",
                        entities=[p["id"]],
                    )
                )
        for pr in gt.predictions_that_will_resolve:
            outcome = pr["outcome"] if perfect_predictions else (
                "true" if pr["outcome"] == "false" else "false"
            )
            canned.setdefault(("model", pr["prediction_id"]), []).append(
                MockBelief(
                    proposition=f"{pr['proposition']} -> {outcome}",
                    proposition_kind="prediction",
                    asserted_confidence=prediction_confidence,
                    entities=[pr["prediction_id"]],
                )
            )
    return MockBeliefSUT(canned=canned)
