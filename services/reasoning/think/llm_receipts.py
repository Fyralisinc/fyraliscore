"""Durable, run-scoped persistence for LLM logical-call and attempt receipts."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from lib.llm.provider import using_receipt_sink
from lib.llm.telemetry import (
    LLMReceiptSink,
    LogicalCallReceipt,
    PhysicalAttemptReceipt,
)


class ReceiptIntegrityError(RuntimeError):
    """Raised when an immutable receipt ID is reused for different facts."""


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

    def record_attempt(self, receipt: PhysicalAttemptReceipt) -> None:
        self.attempts.append(receipt)

    def record_logical_call(self, receipt: LogicalCallReceipt) -> None:
        self.logical_calls.append(receipt)

    def capture(self):
        """Install this collector only for the current async task/context."""

        return using_receipt_sink(self)

    def set_terminal_outcomes(
        self,
        *,
        validation_outcome: str | None = None,
        apply_outcome: str | None = None,
    ) -> None:
        self.validation_outcome = validation_outcome
        self.apply_outcome = apply_outcome

    async def persist(self, conn: Any) -> None:
        """Atomically persist all collected receipts, preserving immutability.

        Replaying an identical receipt is idempotent. Reusing an ID with any
        changed value returns no row from the guarded upsert and fails closed.
        Logical rows are written before attempts even though the FK is deferred,
        making the ordering explicit for simple adapters and tests.
        """

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
                    self.validation_outcome,
                    self.apply_outcome,
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
                    self.tenant_id,
                    receipt.physical_attempt_id,
                    receipt.logical_call_id,
                    receipt.parent_attempt_id,
                    receipt.ordinal,
                    receipt.provider,
                    receipt.model,
                    receipt.purpose,
                    receipt.started_at,
                    receipt.ended_at,
                    receipt.outcome,
                    receipt.error_class,
                    receipt.error_message,
                    receipt.retry_scheduled,
                    receipt.input_tokens,
                    receipt.output_tokens,
                    receipt.cache_tokens,
                    receipt.cost_usd,
                    receipt.usage_exactness,
                    receipt.pricing_version,
                )
                if persisted is None:
                    raise ReceiptIntegrityError(
                        f"physical receipt conflict: {receipt.physical_attempt_id}"
                    )


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


__all__ = ["ReceiptIntegrityError", "ThinkLLMReceiptCollector"]
