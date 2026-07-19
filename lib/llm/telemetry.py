"""Provider-attempt telemetry contracts with no dependency on services.

The sink is deliberately a protocol: core LLM code can emit complete receipts
to memory in tests or to a services-owned durable adapter without importing a
higher architectural layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import re
from typing import Any, Literal, Protocol


AttemptOutcome = Literal[
    "success", "timeout", "parse_failure", "provider_error"
]
LogicalCallOutcome = Literal[
    "success", "cache_hit", "timeout", "parse_failure", "provider_error", "exhausted"
]
CognitivePurpose = Literal[
    "mention_discovery",
    "entity_resolution",
    "question_planning",
    "main_reconciliation",
    "main_synthesis",
]

_SECRET_KEYS = re.compile(
    r"(authorization|cookie|api[_-]?key|access[_-]?token|refresh[_-]?token|"
    r"password|connection[_-]?(string|url)|provider[_-]?auth)", re.I
)
_GOLD_KEYS = re.compile(
    r"(oracle|gold|expected[_-]?(thesis|mechanism|direction|storyline)|"
    r"(?:scorer|evaluation|gold)[_-]?threshold)",
    re.I,
)
_BEARER = re.compile(r"(?i)bearer\s+[a-z0-9._~+/-]+=*")


class UnsafeCognitionTrace(ValueError):
    """The event contains evaluator authority that runtime may not persist."""


def sanitize_cognition_payload(value: Any, *, path: str = "$") -> Any:
    """Redact credentials and reject evaluator-gold fields recursively."""

    if isinstance(value, dict):
        clean: dict[str, Any] = {}
        for raw_key, item in value.items():
            key = str(raw_key)
            if _GOLD_KEYS.search(key):
                raise UnsafeCognitionTrace(f"evaluator field forbidden at {path}.{key}")
            clean[key] = (
                "[REDACTED]"
                if _SECRET_KEYS.search(key)
                else sanitize_cognition_payload(item, path=f"{path}.{key}")
            )
        return clean
    if isinstance(value, (list, tuple)):
        return [
            sanitize_cognition_payload(item, path=f"{path}[{index}]")
            for index, item in enumerate(value)
        ]
    if isinstance(value, str):
        return _BEARER.sub("Bearer [REDACTED]", value)
    return value


@dataclass(frozen=True, slots=True)
class CognitionTraceEvent:
    """One immutable stage in a logical call's cognition trace."""

    schema_version: str
    event_id: str
    trace_id: str
    logical_call_id: str
    stage: Literal[
        "prompt", "raw_provider_response", "compiler", "validated_command",
        "applied_result",
    ]
    cognitive_purpose: CognitivePurpose
    payload: dict[str, Any]
    content_digest: str
    occurred_at: datetime
    physical_attempt_id: str | None = None


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
    context_digest: str | None = None


class LLMReceiptSink(Protocol):
    """Non-blocking contract implemented by in-memory or durable adapters."""

    def record_attempt(self, receipt: PhysicalAttemptReceipt) -> None: ...

    def record_logical_call(self, receipt: LogicalCallReceipt) -> None: ...

    def record_cognition_event(self, event: CognitionTraceEvent) -> None: ...


@dataclass(slots=True)
class InMemoryLLMReceiptSink:
    """Deterministic sink for tests and bounded in-process diagnostics."""

    attempts: list[PhysicalAttemptReceipt] = field(default_factory=list)
    logical_calls: list[LogicalCallReceipt] = field(default_factory=list)
    cognition_events: list[CognitionTraceEvent] = field(default_factory=list)

    def record_attempt(self, receipt: PhysicalAttemptReceipt) -> None:
        self.attempts.append(receipt)

    def record_logical_call(self, receipt: LogicalCallReceipt) -> None:
        self.logical_calls.append(receipt)

    def record_cognition_event(self, event: CognitionTraceEvent) -> None:
        self.cognition_events.append(event)
