from __future__ import annotations

from datetime import datetime, timezone
import os
from uuid import uuid4

import asyncpg
import pytest

from lib.llm.telemetry import LogicalCallReceipt, PhysicalAttemptReceipt
from services.reasoning.think.llm_receipts import (
    ReceiptIntegrityError,
    ThinkLLMReceiptCollector,
)


pytestmark = pytest.mark.asyncio


async def test_receipt_round_trip_and_conflict_guard_in_postgres():
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        pytest.skip("DATABASE_URL is required for the PostgreSQL receipt proof")

    conn = await asyncpg.connect(dsn)
    outer = conn.transaction()
    await outer.start()
    try:
        tenant_id = await conn.fetchval("SELECT id FROM tenants ORDER BY id LIMIT 1")
        if tenant_id is None:
            tenant_id = uuid4()
            await conn.execute(
                "INSERT INTO tenants (id, name) VALUES ($1, 'p1-receipt-proof')",
                tenant_id,
            )

        now = datetime.now(timezone.utc)
        logical_id = f"p1-pg-logical-{uuid4()}"
        attempt_id = f"p1-pg-attempt-{uuid4()}"
        logical = LogicalCallReceipt(
            logical_call_id=logical_id,
            provider="deterministic",
            model="p1-proof",
            purpose="p1_postgres_proof",
            schema_name="P1Proof",
            prompt_digest="a" * 64,
            started_at=now,
            ended_at=now,
            outcome="success",
            physical_attempt_count=1,
        )
        attempt = PhysicalAttemptReceipt(
            physical_attempt_id=attempt_id,
            logical_call_id=logical_id,
            parent_attempt_id=None,
            provider="deterministic",
            model="p1-proof",
            purpose="p1_postgres_proof",
            ordinal=1,
            started_at=now,
            ended_at=now,
            outcome="success",
            input_tokens=12,
            output_tokens=7,
            cost_usd=0.001,
            usage_exactness="reported",
        )
        collector = ThinkLLMReceiptCollector(
            tenant_id=tenant_id,
            batch_id="p1-postgres-proof",
            context_digest="b" * 64,
        )
        collector.record_logical_call(logical)
        collector.record_attempt(attempt)

        await collector.persist(conn)
        await collector.persist(conn)  # identical replay is idempotent

        stored = await conn.fetchrow(
            """
            SELECT l.physical_attempt_count, a.ordinal, a.input_tokens,
                   a.output_tokens, a.usage_exactness
            FROM llm_logical_call_receipts l
            JOIN llm_provider_attempt_receipts a
              USING (tenant_id, logical_call_id)
            WHERE l.tenant_id = $1 AND l.logical_call_id = $2
            """,
            tenant_id,
            logical_id,
        )
        assert dict(stored) == {
            "physical_attempt_count": 1,
            "ordinal": 1,
            "input_tokens": 12,
            "output_tokens": 7,
            "usage_exactness": "reported",
        }

        conflicting = ThinkLLMReceiptCollector(tenant_id=tenant_id)
        conflicting.record_logical_call(
            LogicalCallReceipt(
                logical_call_id=logical_id,
                provider="deterministic",
                model="changed-model",
                purpose="p1_postgres_proof",
                schema_name="P1Proof",
                prompt_digest="a" * 64,
                started_at=now,
                ended_at=now,
                outcome="success",
                physical_attempt_count=1,
            )
        )
        with pytest.raises(ReceiptIntegrityError):
            await conflicting.persist(conn)
    finally:
        await outer.rollback()
        await conn.close()
