"""Core dataset adapter contracts for benchmark runs."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _serialize(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, list):
        return [_serialize(item) for item in value]
    if isinstance(value, dict):
        return {str(key): _serialize(item) for key, item in value.items()}
    return value


@dataclass(frozen=True)
class BenchmarkObservation:
    """A benchmark source item normalized into Fyralis observation shape."""

    observation_id: str
    source: str
    tenant_id: str
    occurred_at: datetime
    content: str
    entities: list[dict[str, Any]] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    trust_tier: str = "benchmark_gold"

    def to_json(self) -> dict[str, Any]:
        return _serialize({
            "observation_id": self.observation_id,
            "source": self.source,
            "tenant_id": self.tenant_id,
            "occurred_at": self.occurred_at,
            "content": self.content,
            "entities": self.entities,
            "metadata": self.metadata,
            "trust_tier": self.trust_tier,
        })


@dataclass(frozen=True)
class BenchmarkQuery:
    """A benchmark question or task normalized into a common query shape."""

    query_id: str
    tenant_id: str
    query_text: str
    query_type: str = "memory_qa"
    constraints: dict[str, Any] = field(default_factory=dict)
    gold_answer: str | None = None
    gold_evidence_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return _serialize({
            "query_id": self.query_id,
            "tenant_id": self.tenant_id,
            "query_text": self.query_text,
            "query_type": self.query_type,
            "constraints": self.constraints,
            "gold_answer": self.gold_answer,
            "gold_evidence_ids": self.gold_evidence_ids,
            "metadata": self.metadata,
        })


@dataclass(frozen=True)
class GoldLabels:
    """Gold labels used by evaluators across public and Fyralis-native suites."""

    answer: str | None = None
    evidence_ids: list[str] = field(default_factory=list)
    counterevidence_ids: list[str] = field(default_factory=list)
    bridge_node_ids: list[str] = field(default_factory=list)
    expected_abstain: bool = False
    state_diff: dict[str, Any] | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_json(self) -> dict[str, Any]:
        return _serialize({
            "answer": self.answer,
            "evidence_ids": self.evidence_ids,
            "counterevidence_ids": self.counterevidence_ids,
            "bridge_node_ids": self.bridge_node_ids,
            "expected_abstain": self.expected_abstain,
            "state_diff": self.state_diff,
            "metadata": self.metadata,
        })


class BenchmarkAdapter(ABC):
    """Interface implemented by each public or Fyralis-native benchmark."""

    benchmark_name: str

    def load_raw(self) -> None:
        """Load raw data if the adapter needs an explicit load step."""

    def preprocess(self) -> None:
        """Prepare processed records if the adapter needs preprocessing."""

    @abstractmethod
    def iter_observations(self) -> Iterable[BenchmarkObservation]:
        """Yield observations to ingest into the benchmark system."""

    @abstractmethod
    def iter_queries(self) -> Iterable[BenchmarkQuery]:
        """Yield benchmark queries after ingestion."""

    @abstractmethod
    def gold(self, query_id: str) -> GoldLabels:
        """Return gold labels for a query or task."""


def observed_at(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
) -> datetime:
    """Convenience for deterministic UTC timestamps in adapters."""
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)


__all__ = [
    "BenchmarkAdapter",
    "BenchmarkObservation",
    "BenchmarkQuery",
    "GoldLabels",
    "observed_at",
]

