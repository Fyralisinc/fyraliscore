"""Sealed deterministic population for the P1 observability exit run."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from hashlib import sha256
import json
from typing import Literal


@dataclass(frozen=True, slots=True)
class SimulatedSignal:
    signal_id: str
    batch_id: str
    source: Literal["chat", "issue", "email"]
    occurred_offset_s: int
    actor_ref: str
    content: str
    expected_disposition: Literal["actionable", "context", "noise"]


@dataclass(frozen=True, slots=True)
class InjectedFault:
    logical_call_ordinal: int
    physical_attempt_ordinal: int
    outcome: Literal["timeout", "invalid_structured_response"]


@dataclass(frozen=True, slots=True)
class P1Population:
    version: str
    batches: tuple[tuple[SimulatedSignal, ...], ...]
    faults: tuple[InjectedFault, ...]

    @property
    def digest(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, separators=(",", ":"))
        return sha256(payload.encode()).hexdigest()


def build_p1_population() -> P1Population:
    """Return two ordinary ten-signal batches with deterministic injected faults.

    Inputs imitate normalized persisted signals, not connector events. Contents
    intentionally avoid benchmark labels, storyline names, scoring hints, and
    expected model text. Expected disposition is evaluator-only metadata and
    must never be passed into production reasoning.
    """

    batch_a = _batch(
        "p1-a",
        (
            ("issue", "a01", "Checkout errors rose after the afternoon release.", "actionable"),
            ("chat", "a02", "Support has five reports from paid accounts.", "actionable"),
            ("email", "a03", "The rollback owner is Priya and the review is at 16:00.", "actionable"),
            ("chat", "a04", "I can reproduce the failure only with saved cards.", "context"),
            ("issue", "a05", "Payment retries are exhausting the gateway timeout.", "actionable"),
            ("chat", "a06", "Lunch delivery is waiting in reception.", "noise"),
            ("email", "a07", "Finance needs an incident impact estimate tomorrow.", "context"),
            ("chat", "a08", "The prior release did not change checkout code.", "context"),
            ("issue", "a09", "Add gateway request IDs to the incident record.", "actionable"),
            ("chat", "a10", "Rollback completed; error rate is declining.", "actionable"),
        ),
    )
    batch_b = _batch(
        "p1-b",
        (
            ("chat", "b01", "The dashboard color looks different today.", "noise"),
            ("chat", "b02", "Three renewal calls mentioned missing audit exports.", "actionable"),
            ("email", "b03", "Northwind will delay renewal unless exports arrive this month.", "actionable"),
            ("chat", "b04", "Does anyone remember the old export prototype?", "context"),
            ("issue", "b05", "Audit export currently omits delegated access events.", "actionable"),
            ("chat", "b06", "A reaction was added to yesterday's announcement.", "noise"),
            ("email", "b07", "Legal confirmed delegated events are required evidence.", "actionable"),
            ("chat", "b08", "The prototype owner moved to another team.", "context"),
            ("issue", "b09", "Scope and estimate delegated-event export work.", "actionable"),
            ("chat", "b10", "Mina will present the estimate at Friday's renewal review.", "actionable"),
        ),
    )
    return P1Population(
        version="p1-observability-population-v1",
        batches=(batch_a, batch_b),
        faults=(
            InjectedFault(1, 1, "timeout"),
            InjectedFault(2, 1, "invalid_structured_response"),
        ),
    )


def production_payload(signal: SimulatedSignal) -> dict[str, object]:
    """Strip evaluator-only labels before a signal reaches production code."""

    return {
        "id": signal.signal_id,
        "source": signal.source,
        "occurred_offset_s": signal.occurred_offset_s,
        "actor_ref": signal.actor_ref,
        "content": signal.content,
    }


def _batch(batch_id: str, rows: tuple[tuple[str, str, str, str], ...]):
    return tuple(
        SimulatedSignal(
            signal_id=signal_id,
            batch_id=batch_id,
            source=source,  # type: ignore[arg-type]
            occurred_offset_s=index * 17,
            actor_ref=f"actor-{(index % 4) + 1}",
            content=content,
            expected_disposition=disposition,  # type: ignore[arg-type]
        )
        for index, (source, signal_id, content, disposition) in enumerate(rows)
    )


__all__ = [
    "InjectedFault",
    "P1Population",
    "SimulatedSignal",
    "build_p1_population",
    "production_payload",
]
