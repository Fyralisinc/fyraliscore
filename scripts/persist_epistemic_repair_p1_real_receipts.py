#!/usr/bin/env python3
"""Persist and reopen a successful P1 provider smoke's exact receipts."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime
from hashlib import sha256
import json
from pathlib import Path
import sys
from uuid import uuid4

import asyncpg

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.llm.telemetry import LogicalCallReceipt, PhysicalAttemptReceipt  # noqa: E402
from services.reasoning.think.llm_receipts import ThinkLLMReceiptCollector  # noqa: E402


def _datetime(value: str) -> datetime:
    return datetime.fromisoformat(value)


async def _run(dsn: str, smoke_path: Path) -> dict[str, object]:
    smoke = json.loads(smoke_path.read_text(encoding="utf-8"))
    if not smoke.get("passed"):
        raise ValueError("real-provider smoke must pass before receipts are persisted")
    logical_rows = smoke.get("logical_history", [])
    attempt_rows = smoke.get("attempt_history", [])
    if len(logical_rows) != 1 or not attempt_rows:
        raise ValueError("smoke must contain one logical receipt and its attempts")

    logical_payload = dict(logical_rows[0])
    logical_payload["started_at"] = _datetime(logical_payload["started_at"])
    logical_payload["ended_at"] = _datetime(logical_payload["ended_at"])
    logical = LogicalCallReceipt(**logical_payload)
    attempts = []
    for row in attempt_rows:
        payload = dict(row)
        payload["started_at"] = _datetime(payload["started_at"])
        payload["ended_at"] = _datetime(payload["ended_at"])
        attempts.append(PhysicalAttemptReceipt(**payload))

    tenant_id = uuid4()
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(
            "INSERT INTO tenants (id,name) VALUES ($1,$2)",
            tenant_id,
            f"p1-codex-receipt-{tenant_id}",
        )
        collector = ThinkLLMReceiptCollector(
            tenant_id=tenant_id,
            batch_id="p1-real-provider-smoke",
            context_digest=logical.context_digest,
            validation_outcome="smoke_schema_valid",
            apply_outcome="telemetry_only",
        )
        collector.record_logical_call(logical)
        for attempt in attempts:
            collector.record_attempt(attempt)
        await collector.persist(conn)
    finally:
        await conn.close()

    # Reopen through a new physical connection, then replay identically to
    # prove durable recovery and idempotence rather than transaction-local read.
    conn = await asyncpg.connect(dsn)
    try:
        collector = ThinkLLMReceiptCollector(
            tenant_id=tenant_id,
            batch_id="p1-real-provider-smoke",
            context_digest=logical.context_digest,
            validation_outcome="smoke_schema_valid",
            apply_outcome="telemetry_only",
        )
        collector.record_logical_call(logical)
        for attempt in attempts:
            collector.record_attempt(attempt)
        await collector.persist(conn)
        rows = await conn.fetch(
            """
            SELECT l.logical_call_id, l.provider, l.model, l.outcome,
                   l.physical_attempt_count, l.context_digest,
                   a.physical_attempt_id, a.ordinal, a.outcome AS attempt_outcome,
                   a.usage_exactness
            FROM llm_logical_call_receipts l
            JOIN llm_provider_attempt_receipts a
              USING (tenant_id, logical_call_id)
            WHERE l.tenant_id=$1 AND l.logical_call_id=$2
            ORDER BY a.ordinal
            """,
            tenant_id,
            logical.logical_call_id,
        )
    finally:
        await conn.close()

    normalized = [dict(row) for row in rows]
    evidence_digest = sha256(
        json.dumps(normalized, sort_keys=True, default=str).encode()
    ).hexdigest()
    passed = (
        len(rows) == len(attempts)
        and rows[0]["physical_attempt_count"] == len(attempts)
        and all(row["provider"] == "codex" for row in rows)
        and all(row["attempt_outcome"] == "success" for row in rows)
    )
    return {
        "schema_version": "epistemic-repair-p1-real-receipt-durability-v1",
        "tenant_id": str(tenant_id),
        "logical_call_id": logical.logical_call_id,
        "logical_rows": 1 if rows else 0,
        "attempt_rows": len(rows),
        "reopened_on_new_connection": bool(rows),
        "identical_replay_idempotent": len(rows) == len(attempts),
        "evidence_digest": evidence_digest,
        "passed": passed,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--smoke", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(_run(args.dsn, args.smoke))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
