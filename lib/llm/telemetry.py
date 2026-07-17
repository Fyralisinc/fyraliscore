"""Provider-attempt telemetry contracts with no dependency on services.

The sink is deliberately a protocol: core LLM code can emit complete receipts
to memory in tests or to a services-owned durable adapter without importing a
higher architectural layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Literal, Protocol


AttemptOutcome = Literal[
    "success", "timeout", "parse_failure", "provider_error"
]
LogicalCallOutcome = Literal[
    "success", "cache_hit", "timeout", "parse_failure", "provider_error", "exhausted"
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True, slots=True)
class PhysicalAttemptReceipt:
    physical_attempt_id: str
    logical_call_id: str
    parent_attempt_id: str | None
    provider: str
    model: str
    purpose: str
    ordinal: int
    started_at: datetime
    ended_at: datetime
    outcome: AttemptOutcome
    error_class: str | None = None
    error_message: str | None = None
    retry_scheduled: bool = False
    input_tokens: int = 0
    output_tokens: int = 0
    cache_tokens: int = 0
    cost_usd: float = 0.0
    usage_exactness: str = "unavailable"
    pricing_version: str = "fyralis-model-pricing-v1"


@dataclass(frozen=True, slots=True)
class LogicalCallReceipt:
    logical_call_id: str
    provider: str
    model: str
    purpose: str
    schema_name: str
    prompt_digest: str
    started_at: datetime
    ended_at: datetime
    outcome: LogicalCallOutcome
    physical_attempt_count: int
    error_class: str | None = None
    error_message: str | None = None


class LLMReceiptSink(Protocol):
    """Non-blocking contract implemented by in-memory or durable adapters."""

    def record_attempt(self, receipt: PhysicalAttemptReceipt) -> None: ...

    def record_logical_call(self, receipt: LogicalCallReceipt) -> None: ...


@dataclass(slots=True)
class InMemoryLLMReceiptSink:
    """Deterministic sink for tests and bounded in-process diagnostics."""

    attempts: list[PhysicalAttemptReceipt] = field(default_factory=list)
    logical_calls: list[LogicalCallReceipt] = field(default_factory=list)

    def record_attempt(self, receipt: PhysicalAttemptReceipt) -> None:
        self.attempts.append(receipt)

    def record_logical_call(self, receipt: LogicalCallReceipt) -> None:
        self.logical_calls.append(receipt)
