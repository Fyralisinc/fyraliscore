"""Durable, run-scoped persistence for LLM logical-call and attempt receipts."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
import contextvars
from typing import Any
from uuid import UUID

from lib.llm.provider import using_receipt_sink
from lib.llm.telemetry import (
    CognitionTraceEvent,
    LLMReceiptSink,
    LogicalCallReceipt,
    PhysicalAttemptReceipt,
    utc_now,
)
from lib.shared.ids import uuid7


class ReceiptIntegrityError(RuntimeError):
    """Raised when an immutable receipt ID is reused for different facts."""


_CURRENT_COLLECTOR: contextvars.ContextVar[ThinkLLMReceiptCollector | None] = (
    contextvars.ContextVar("think_llm_receipt_collector", default=None)
)


@dataclass(slots=True)
class ThinkLLMReceiptCollector(LLMReceiptSink):
    """Collect provider receipts and attach the owning Think-run coordinates.

    Provider calls are synchronous at the emission boundary, so the collector is
    deliberately in-memory there.  The Think orchestration layer flushes it with
    its async database connection at a durability boundary.
    """

    tenant_id: UUID
    trigger_id: UUID | None = None
    think_run_id: UUID | None = None
    batch_id: str | None = None
    context_digest: str | None = None
    validation_outcome: str | None = None
    apply_outcome: str | None = None
    logical_calls: list[LogicalCallReceipt] = field(default_factory=list)
    attempts: list[PhysicalAttemptReceipt] = field(default_factory=list)
    cognition_events: list[CognitionTraceEvent] = field(default_factory=list)
    terminal_outcomes: dict[str, tuple[str | None, str | None]] = field(
        default_factory=dict
    )

    def record_attempt(self, receipt: PhysicalAttemptReceipt) -> None:
        self.attempts.append(receipt)

    def record_logical_call(self, receipt: LogicalCallReceipt) -> None:
        self.logical_calls.append(receipt)

    def record_cognition_event(self, event: CognitionTraceEvent) -> None:
        self.cognition_events.append(event)

    def capture(self):
        """Install this collector only for the current async task/context."""

        receipt_context = using_receipt_sink(self)

        class _CollectorContext:
            def __enter__(inner_self):
                inner_self.token = _CURRENT_COLLECTOR.set(self)
                return receipt_context.__enter__()

            def __exit__(inner_self, *args):
                try:
                    return receipt_context.__exit__(*args)
                finally:
                    _CURRENT_COLLECTOR.reset(inner_self.token)

        return _CollectorContext()

    def set_terminal_outcomes(
        self,
        *,
        validation_outcome: str | None = None,
        apply_outcome: str | None = None,
    ) -> None:
        self.validation_outcome = validation_outcome
        self.apply_outcome = apply_outcome
        if self.logical_calls:
            self.terminal_outcomes[self.logical_calls[-1].logical_call_id] = (
                validation_outcome, apply_outcome
            )

    def record_pipeline_stage(self, stage: str, payload: Any) -> None:
        if not self.logical_calls or stage not in {
            "compiler", "validated_command", "applied_result"
        }:
            return
        logical = self.logical_calls[-1]
        prompt_event = next(
            (event for event in reversed(self.cognition_events)
             if event.logical_call_id == logical.logical_call_id),
            None,
        )
        if prompt_event is None:
            return
        from lib.contracts.kernel import canonical_sha256
        from lib.llm.telemetry import sanitize_cognition_payload

        safe_payload = sanitize_cognition_payload(payload)
        self.cognition_events.append(CognitionTraceEvent(
            schema_version="think-cognition-trace-v1",
            event_id=str(uuid7()),
            trace_id=prompt_event.trace_id,
            logical_call_id=logical.logical_call_id,
            stage=stage,  # type: ignore[arg-type]
            cognitive_purpose=prompt_event.cognitive_purpose,
            payload=safe_payload,
            content_digest=canonical_sha256(safe_payload),
            occurred_at=utc_now(),
        ))

    async def persist(self, conn: Any) -> None:
        """Atomically persist all collected receipts, preserving immutability.

        Replaying an identical receipt is idempotent. Reusing an ID with any
        changed value returns no row from the guarded upsert and fails closed.
        Logical rows are written before attempts even though the FK is deferred,
        making the ordering explicit for simple adapters and tests.
        """

        self.validate_reconciliation()
        async with conn.transaction():
            for receipt in self.logical_calls:
                persisted = await conn.fetchval(
                    _UPSERT_LOGICAL_RECEIPT,
                    self.tenant_id,
                    receipt.logical_call_id,
                    self.trigger_id,
                    self.think_run_id,
                    self.batch_id,
                    receipt.provider,
                    receipt.model,
                    receipt.purpose,
                    receipt.schema_name,
                    receipt.prompt_digest,
                    receipt.context_digest or self.context_digest,
                    receipt.started_at,
                    receipt.ended_at,
                    receipt.outcome,
                    receipt.physical_attempt_count,
                    self.terminal_outcomes.get(receipt.logical_call_id, (None, None))[0],
                    self.terminal_outcomes.get(receipt.logical_call_id, (None, None))[1],
                    receipt.error_class,
                    receipt.error_message,
                )
                if persisted is None:
                    raise ReceiptIntegrityError(
                        f"logical receipt conflict: {receipt.logical_call_id}"
                    )

            for receipt in self.attempts:
                persisted = await conn.fetchval(
                    _UPSERT_ATTEMPT_RECEIPT,
                    self.tenant_id, receipt.physical_attempt_id,
                    receipt.logical_call_id, receipt.parent_attempt_id,
                    receipt.ordinal, receipt.provider, receipt.model,
                    receipt.purpose, receipt.started_at, receipt.ended_at,
                    receipt.outcome, receipt.error_class, receipt.error_message,
                    receipt.retry_scheduled, receipt.input_tokens,
                    receipt.output_tokens, receipt.cache_tokens, receipt.cost_usd,
                    receipt.usage_exactness, receipt.pricing_version,
                )
                if persisted is None:
                    raise ReceiptIntegrityError(
                        f"physical receipt conflict: {receipt.physical_attempt_id}"
                    )

            for event in self.cognition_events:
                persisted = await conn.fetchval(
                    _UPSERT_COGNITION_EVENT,
                    self.tenant_id, event.event_id, event.trace_id,
                    event.logical_call_id, event.physical_attempt_id,
                    self.trigger_id, self.think_run_id, self.batch_id,
                    event.schema_version, event.stage, event.cognitive_purpose,
                    json.dumps(event.payload, sort_keys=True, separators=(",", ":")),
                    event.content_digest, event.occurred_at,
                )
                if persisted is None:
                    raise ReceiptIntegrityError(
                        f"cognition trace conflict: {event.trace_id}:{event.stage}"
                    )

    def validate_reconciliation(self) -> None:
        logical_by_id = {item.logical_call_id: item for item in self.logical_calls}
        if len(logical_by_id) != len(self.logical_calls):
            raise ReceiptIntegrityError("duplicate logical call receipt")
        attempts_by_call: dict[str, list[PhysicalAttemptReceipt]] = {}
        for attempt in self.attempts:
            attempts_by_call.setdefault(attempt.logical_call_id, []).append(attempt)
        for call_id, logical in logical_by_id.items():
            attempts = sorted(attempts_by_call.get(call_id, []), key=lambda x: x.ordinal)
            if len(attempts) != logical.physical_attempt_count:
                raise ReceiptIntegrityError(f"attempt count mismatch: {call_id}")
            if [item.ordinal for item in attempts] != list(range(1, len(attempts) + 1)):
                raise ReceiptIntegrityError(f"non-contiguous attempts: {call_id}")
            for index, attempt in enumerate(attempts):
                expected_parent = None if index == 0 else attempts[index - 1].physical_attempt_id
                if attempt.parent_attempt_id != expected_parent:
                    raise ReceiptIntegrityError(f"attempt parent mismatch: {call_id}")
                if attempt.provider != logical.provider or attempt.model != logical.model:
                    raise ReceiptIntegrityError(f"attempt identity mismatch: {call_id}")
                if attempt.retry_scheduled != (index < len(attempts) - 1):
                    raise ReceiptIntegrityError(f"retry reconciliation mismatch: {call_id}")
            successes = [item for item in attempts if item.outcome == "success"]
            if len(successes) > 1 or (successes and successes[0] is not attempts[-1]):
                raise ReceiptIntegrityError(f"non-terminal success: {call_id}")
            if logical.outcome == "cache_hit" and attempts:
                raise ReceiptIntegrityError(f"cache hit has attempts: {call_id}")
            if logical.outcome == "success" and (
                len(successes) != 1 or successes[0] is not attempts[-1]
            ):
                raise ReceiptIntegrityError(f"logical success mismatch: {call_id}")
        orphaned = set(attempts_by_call) - set(logical_by_id)
        if orphaned:
            raise ReceiptIntegrityError(f"orphan physical attempts: {sorted(orphaned)}")


_UPSERT_LOGICAL_RECEIPT = """
INSERT INTO llm_logical_call_receipts (
  tenant_id, logical_call_id, trigger_id, think_run_id, batch_id,
  provider, model, purpose, schema_name, prompt_digest, context_digest,
  started_at, ended_at, outcome, physical_attempt_count,
  validation_outcome, apply_outcome, error_class, error_message
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14,
  $15, $16, $17, $18, $19
)
ON CONFLICT (tenant_id, logical_call_id) DO UPDATE
SET logical_call_id = EXCLUDED.logical_call_id
WHERE ROW(
  llm_logical_call_receipts.trigger_id, llm_logical_call_receipts.think_run_id,
  llm_logical_call_receipts.batch_id, llm_logical_call_receipts.provider,
  llm_logical_call_receipts.model, llm_logical_call_receipts.purpose,
  llm_logical_call_receipts.schema_name, llm_logical_call_receipts.prompt_digest,
  llm_logical_call_receipts.context_digest, llm_logical_call_receipts.started_at,
  llm_logical_call_receipts.ended_at, llm_logical_call_receipts.outcome,
  llm_logical_call_receipts.physical_attempt_count,
  llm_logical_call_receipts.validation_outcome,
  llm_logical_call_receipts.apply_outcome, llm_logical_call_receipts.error_class,
  llm_logical_call_receipts.error_message
) IS NOT DISTINCT FROM ROW(
  EXCLUDED.trigger_id, EXCLUDED.think_run_id, EXCLUDED.batch_id,
  EXCLUDED.provider, EXCLUDED.model, EXCLUDED.purpose, EXCLUDED.schema_name,
  EXCLUDED.prompt_digest, EXCLUDED.context_digest, EXCLUDED.started_at,
  EXCLUDED.ended_at, EXCLUDED.outcome, EXCLUDED.physical_attempt_count,
  EXCLUDED.validation_outcome, EXCLUDED.apply_outcome, EXCLUDED.error_class,
  EXCLUDED.error_message
)
RETURNING logical_call_id
"""


_UPSERT_ATTEMPT_RECEIPT = """
INSERT INTO llm_provider_attempt_receipts (
  tenant_id, physical_attempt_id, logical_call_id, parent_attempt_id, ordinal,
  provider, model, purpose, started_at, ended_at, outcome, error_class,
  error_message, retry_scheduled, input_tokens, output_tokens, cache_tokens,
  cost_usd, usage_exactness, pricing_version
) VALUES (
  $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
  $16, $17, $18, $19, $20
)
ON CONFLICT (tenant_id, physical_attempt_id) DO UPDATE
SET physical_attempt_id = EXCLUDED.physical_attempt_id
WHERE ROW(
  llm_provider_attempt_receipts.logical_call_id,
  llm_provider_attempt_receipts.parent_attempt_id,
  llm_provider_attempt_receipts.ordinal, llm_provider_attempt_receipts.provider,
  llm_provider_attempt_receipts.model, llm_provider_attempt_receipts.purpose,
  llm_provider_attempt_receipts.started_at, llm_provider_attempt_receipts.ended_at,
  llm_provider_attempt_receipts.outcome, llm_provider_attempt_receipts.error_class,
  llm_provider_attempt_receipts.error_message,
  llm_provider_attempt_receipts.retry_scheduled,
  llm_provider_attempt_receipts.input_tokens,
  llm_provider_attempt_receipts.output_tokens,
  llm_provider_attempt_receipts.cache_tokens, llm_provider_attempt_receipts.cost_usd,
  llm_provider_attempt_receipts.usage_exactness,
  llm_provider_attempt_receipts.pricing_version
) IS NOT DISTINCT FROM ROW(
  EXCLUDED.logical_call_id, EXCLUDED.parent_attempt_id, EXCLUDED.ordinal,
  EXCLUDED.provider, EXCLUDED.model, EXCLUDED.purpose, EXCLUDED.started_at,
  EXCLUDED.ended_at, EXCLUDED.outcome, EXCLUDED.error_class,
  EXCLUDED.error_message, EXCLUDED.retry_scheduled, EXCLUDED.input_tokens,
  EXCLUDED.output_tokens, EXCLUDED.cache_tokens, EXCLUDED.cost_usd,
  EXCLUDED.usage_exactness, EXCLUDED.pricing_version
)
RETURNING physical_attempt_id
"""

_UPSERT_COGNITION_EVENT = """
INSERT INTO think_cognition_trace_events (
  tenant_id, event_id, trace_id, logical_call_id, physical_attempt_id,
  trigger_id, think_run_id, batch_id,
  schema_version, stage, cognitive_purpose, payload, content_digest, occurred_at
) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14)
ON CONFLICT (tenant_id, event_id) DO UPDATE SET event_id=EXCLUDED.event_id
WHERE ROW(think_cognition_trace_events.logical_call_id,
          think_cognition_trace_events.payload,
          think_cognition_trace_events.content_digest)
 IS NOT DISTINCT FROM ROW(EXCLUDED.logical_call_id, EXCLUDED.payload,
                          EXCLUDED.content_digest)
RETURNING event_id
"""


__all__ = [
    "ReceiptIntegrityError", "ThinkLLMReceiptCollector",
    "record_current_pipeline_stage",
]


def record_current_pipeline_stage(stage: str, payload: Any) -> None:
    collector = _CURRENT_COLLECTOR.get()
    if collector is not None:
        collector.record_pipeline_stage(stage, payload)
