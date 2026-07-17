from __future__ import annotations

from datetime import datetime, timezone
from uuid import uuid4

import pytest

from lib.llm.telemetry import LogicalCallReceipt, PhysicalAttemptReceipt
from services.reasoning.think.llm_receipts import (
    ReceiptIntegrityError,
    ThinkLLMReceiptCollector,
)


class _Transaction:
    def __init__(self, conn):
        self.conn = conn

    async def __aenter__(self):
        self.conn.events.append("begin")

    async def __aexit__(self, exc_type, exc, tb):
        self.conn.events.append("rollback" if exc else "commit")


class _Connection:
    def __init__(self, results=None):
        self.results = iter(results or ["logical-1", "attempt-1"])
        self.calls = []
        self.events = []

    def transaction(self):
        return _Transaction(self)

    async def fetchval(self, sql, *args):
        self.calls.append((sql, args))
        return next(self.results)


def _receipts():
    now = datetime.now(timezone.utc)
    logical = LogicalCallReceipt(
        logical_call_id="logical-1",
        provider="fake",
        model="model",
        purpose="synthesis",
        schema_name="Output",
        prompt_digest="a" * 64,
        started_at=now,
        ended_at=now,
        outcome="success",
        physical_attempt_count=1,
        context_digest="c" * 64,
    )
    attempt = PhysicalAttemptReceipt(
        physical_attempt_id="attempt-1",
        logical_call_id="logical-1",
        parent_attempt_id=None,
        provider="fake",
        model="model",
        purpose="synthesis",
        ordinal=1,
        started_at=now,
        ended_at=now,
        outcome="success",
        input_tokens=10,
        output_tokens=5,
        cost_usd=0.01,
        usage_exactness="reported",
    )
    return logical, attempt


@pytest.mark.asyncio
async def test_persists_logical_before_attempt_in_one_transaction():
    logical, attempt = _receipts()
    tenant_id, trigger_id, run_id = uuid4(), uuid4(), uuid4()
    collector = ThinkLLMReceiptCollector(
        tenant_id=tenant_id,
        trigger_id=trigger_id,
        think_run_id=run_id,
        batch_id="batch-7",
        context_digest="b" * 64,
    )
    collector.record_attempt(attempt)
    collector.record_logical_call(logical)
    collector.set_terminal_outcomes(
        validation_outcome="accepted", apply_outcome="committed"
    )
    conn = _Connection()

    await collector.persist(conn)

    assert conn.events == ["begin", "commit"]
    assert len(conn.calls) == 2
    assert "llm_logical_call_receipts" in conn.calls[0][0]
    assert "llm_provider_attempt_receipts" in conn.calls[1][0]
    logical_args = conn.calls[0][1]
    assert logical_args[:5] == (
        tenant_id,
        "logical-1",
        trigger_id,
        run_id,
        "batch-7",
    )
    assert logical_args[10] == "c" * 64
    assert logical_args[15:17] == ("accepted", "committed")


@pytest.mark.asyncio
async def test_conflicting_immutable_receipt_fails_closed_and_rolls_back():
    logical, attempt = _receipts()
    collector = ThinkLLMReceiptCollector(tenant_id=uuid4())
    collector.record_logical_call(logical)
    collector.record_attempt(attempt)
    conn = _Connection(results=["logical-1", None])

    with pytest.raises(ReceiptIntegrityError, match="attempt-1"):
        await collector.persist(conn)

    assert conn.events == ["begin", "rollback"]


def test_capture_is_task_local_and_collects_provider_receipts():
    logical, attempt = _receipts()
    collector = ThinkLLMReceiptCollector(tenant_id=uuid4())

    with collector.capture() as installed:
        assert installed is collector
        collector.record_attempt(attempt)
        collector.record_logical_call(logical)

    assert collector.attempts == [attempt]
    assert collector.logical_calls == [logical]
