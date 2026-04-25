"""MockTemporalSUT: a deterministic stand-in SUT used to test Layer 5.

It supports past-timestamp belief queries, exposes simple pattern-detection
metadata, and optionally implements the L1 retrieval-capable protocol so the
retrieval-drift sub-evaluation has something to probe.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from lsob_contracts import Belief, BeliefQuery


@dataclass
class TemporalBeliefRecord:
    """Per-timestamp belief registered for a given (kind, entity_id)."""

    proposition: str
    proposition_kind: str
    asserted_confidence: float = 0.8
    entities: list[str] = field(default_factory=list)


class MockTemporalSUT:
    """Mock SUT with time-travel belief queries + pattern detection hooks.

    `beliefs` is keyed by (kind, entity_id) → list of (timestamp, record).
    `query_beliefs_at` returns the most recent record at-or-before the query
    timestamp. Pattern detections are exposed via a simple dict
    `pattern_detected_at: {pattern_id: datetime}`; when a BeliefQuery comes in
    for a pattern, we return a synthetic belief iff the query timestamp is at
    or after the detection timestamp. Retrieval surface is optional and
    controlled by `retrieval_answers`.
    """

    name: str = "mock-temporal-sut"
    max_concurrent_ingestion: int = 1

    def __init__(
        self,
        beliefs: dict[
            tuple[str, str], list[tuple[datetime, TemporalBeliefRecord]]
        ]
        | None = None,
        pattern_detected_at: dict[str, datetime] | None = None,
        retrieval_answers: dict[str, list[str]] | None = None,
        *,
        fail_time_travel: bool = False,
    ) -> None:
        self.beliefs = beliefs or {}
        self.pattern_detected_at = pattern_detected_at or {}
        self.retrieval_answers = retrieval_answers
        self.fail_time_travel = fail_time_travel
        self.queries_seen: list[BeliefQuery] = []

    # -- SystemUnderTest protocol stubs -----------------------------------

    async def startup(self, config: Any) -> None:  # pragma: no cover
        return None

    async def apply_ablation(self, ablation: Any) -> None:  # pragma: no cover
        return None

    async def ingest_signal(self, signal: Any) -> None:  # pragma: no cover
        return None

    async def shutdown(self) -> None:  # pragma: no cover
        return None

    async def query_at_risk_at(self, timestamp: datetime) -> Any:  # pragma: no cover
        raise NotImplementedError

    async def produce_diff_for_trigger(self, trigger: Any) -> Any:  # pragma: no cover
        raise NotImplementedError

    # -- L5 belief surface ------------------------------------------------

    async def query_beliefs_at(self, query: BeliefQuery) -> list[Belief]:
        self.queries_seen.append(query)
        if self.fail_time_travel:
            raise RuntimeError("time-travel queries not supported")

        # Pattern lookups use the detection log.
        if query.entity_ref.kind == "pattern":
            detected = self.pattern_detected_at.get(query.entity_ref.id)
            if detected is None or query.timestamp < detected:
                return []
            return [
                Belief(
                    claim_id=f"mock-pattern-{query.entity_ref.id}-{query.query_id}",
                    proposition=f"pattern_detected:{query.entity_ref.id}",
                    proposition_kind="pattern",
                    asserted_confidence=0.9,
                    last_updated=detected,
                    entities=[query.entity_ref.id],
                )
            ]

        key = (query.entity_ref.kind, query.entity_ref.id)
        series = self.beliefs.get(key, [])
        latest_record: TemporalBeliefRecord | None = None
        latest_ts: datetime | None = None
        for ts, record in series:
            if ts <= query.timestamp and (latest_ts is None or ts > latest_ts):
                latest_record = record
                latest_ts = ts
        if latest_record is None:
            return []
        return [
            Belief(
                claim_id=f"mock-{query.query_id}",
                proposition=latest_record.proposition,
                proposition_kind=latest_record.proposition_kind,
                asserted_confidence=latest_record.asserted_confidence,
                last_updated=latest_ts or query.timestamp,
                entities=latest_record.entities or [query.entity_ref.id],
                evidence_signal_ids=[],
            )
        ]

    # -- Optional L1 retrieval surface ------------------------------------

    async def retrieval_semantic(self, query: str, k: int) -> list[str]:
        if self.retrieval_answers is None:
            raise RuntimeError("retrieval surface not enabled on this mock")
        items = self.retrieval_answers.get(query, [])
        return items[:k]

    async def retrieval_entity_resolve(
        self, phrase: str, author_id: str
    ) -> str | None:  # pragma: no cover - not exercised by L5
        return None

    async def retrieval_rerank(
        self, items: list[str], query: str
    ) -> list[str]:  # pragma: no cover - not exercised by L5
        return list(items)
