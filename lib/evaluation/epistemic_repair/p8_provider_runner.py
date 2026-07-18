"""Real Codex CLI fault probes with durable PostgreSQL attempt receipts."""

from __future__ import annotations

import asyncio
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import json
import os
import signal
from uuid import UUID, uuid4

import asyncpg

from lib.contracts.kernel import canonical_sha256
from lib.evaluation.epistemic_repair.provider_contract import (
    require_codex_cli_environment,
)


PROVIDER_BOUNDARIES = (
    "provider_timeout_before_response",
    "provider_timeout_after_partial_work",
    "invalid_structured_output",
)


@dataclass(frozen=True, slots=True)
class DurableProviderFaultReceipt:
    boundary: str
    duplicate_delivery: bool
    tenant_id: str
    logical_call_id: str
    physical_attempt_id: str
    observed_outcome: str
    stdout_bytes: int
    partial_events: int
    persisted_logical_receipts: int
    persisted_attempt_receipts: int
    queried_receipt_digest: str
    input_tokens: int = 0
    output_tokens: int = 0
    cache_tokens: int = 0
    usage_exactness: str = "unavailable"


@dataclass(frozen=True, slots=True)
class ProviderFaultSlice:
    database_run_id: str
    provider: str
    model: str
    receipts: tuple[DurableProviderFaultReceipt, ...]
    evidence_digest: str


async def _codex_call(boundary: str) -> tuple[str, bytes, int, datetime, datetime]:
    prompts = {
        "provider_timeout_before_response": "Return the JSON object {\"status\":\"ok\"}.",
        "provider_timeout_after_partial_work": (
            "Return one JSON object containing five concise organizational memory failure labels."
        ),
        "invalid_structured_output": (
            "Return exactly this plain sentence and do not use JSON: schema deliberately rejected"
        ),
    }
    limits = {
        "provider_timeout_before_response": 0.05,
        "provider_timeout_after_partial_work": 7.0,
        "invalid_structured_output": 20.0,
    }
    started = datetime.now(timezone.utc)
    process = await asyncio.create_subprocess_exec(
        "codex", "exec", "--ephemeral", "--ignore-rules", "-s", "read-only",
        "-m", "gpt-5.4", "--json", prompts[boundary],
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        start_new_session=True,
    )
    try:
        await asyncio.wait_for(process.wait(), timeout=limits[boundary])
    except TimeoutError:
        os.killpg(process.pid, signal.SIGKILL)
        await process.wait()
        outcome = "timeout"
    else:
        outcome = "success"
    stdout = await process.stdout.read()
    await process.stderr.read()
    ended = datetime.now(timezone.utc)
    events = [line for line in stdout.splitlines() if line.strip()]
    if boundary == "provider_timeout_after_partial_work" and (outcome != "timeout" or not events):
        raise AssertionError("partial-work timeout did not expose partial CLI events before termination")
    if boundary == "invalid_structured_output":
        messages = []
        for line in events:
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                continue
            item = event.get("item") or {}
            if event.get("type") == "item.completed" and item.get("type") == "agent_message":
                messages.append(item.get("text", ""))
        if not messages:
            raise AssertionError("Codex CLI returned no structured-output candidate")
        try:
            json.loads(messages[-1])
        except json.JSONDecodeError:
            outcome = "parse_failure"
        else:
            raise AssertionError("invalid structured-output probe unexpectedly returned JSON")
    return outcome, stdout, len(events), started, ended


async def _persist_and_query(
    dsn: str, *, tenant_id: UUID, boundary: str, outcome: str,
    stdout: bytes, event_count: int, started: datetime, ended: datetime,
) -> tuple[DurableProviderFaultReceipt, DurableProviderFaultReceipt]:
    usage = _reported_usage(stdout)
    input_tokens = int(usage.get("input_tokens", 0))
    output_tokens = int(usage.get("output_tokens", 0))
    cache_tokens = int(usage.get("cached_input_tokens", 0))
    usage_exactness = "reported" if usage else "unavailable"
    logical_id, attempt_id = f"p8:{boundary}:{uuid4()}", f"p8-attempt:{uuid4()}"
    conn = await asyncpg.connect(dsn)
    await conn.execute("INSERT INTO tenants (id,name) VALUES ($1,$2)", tenant_id, f"p8-provider-{tenant_id}")
    tx = conn.transaction()
    await tx.start()
    try:
        await conn.execute(
            """INSERT INTO llm_logical_call_receipts (
                 tenant_id, logical_call_id, provider, model, purpose, schema_name,
                 prompt_digest, context_digest, started_at, ended_at, outcome,
                 physical_attempt_count, validation_outcome, apply_outcome,
                 error_class, error_message
               ) VALUES ($1,$2,'codex-cli','gpt-5.4',$3,'p8_fault_probe',$4,$5,$6,$7,$8,1,$9,'not_applied',$10,$11)""",
            tenant_id, logical_id, f"p8:{boundary}", canonical_sha256({"boundary": boundary}),
            canonical_sha256({"execution": "isolated_codex_cli"}), started, ended,
            outcome, "rejected" if outcome == "parse_failure" else None,
            boundary, f"observed {outcome}",
        )
        await conn.execute(
            """INSERT INTO llm_provider_attempt_receipts (
                 tenant_id, physical_attempt_id, logical_call_id, ordinal, provider,
                 model, purpose, started_at, ended_at, outcome, error_class,
                 error_message, retry_scheduled, input_tokens, output_tokens,
                 cache_tokens, cost_usd, usage_exactness, pricing_version
               ) VALUES ($1,$2,$3,1,'codex-cli','gpt-5.4',$4,$5,$6,$7,$8,$9,false,$10,$11,$12,0,$13,'codex-cli-unpriced')""",
            tenant_id, attempt_id, logical_id, f"p8:{boundary}", started, ended,
            outcome, boundary, f"stdout_bytes={len(stdout)} events={event_count}",
            input_tokens, output_tokens, cache_tokens, usage_exactness,
        )
        await tx.commit()
    finally:
        await conn.close()

    # Reopen twice: second read represents duplicate delivery of the durable
    # terminal receipt and must not issue another physical provider request.
    receipts = []
    for duplicate in (False, True):
        conn = await asyncpg.connect(dsn)
        row = await conn.fetchrow(
            """SELECT l.outcome, count(a.physical_attempt_id)::int AS attempts
               FROM llm_logical_call_receipts l
               JOIN llm_provider_attempt_receipts a USING (tenant_id, logical_call_id)
               WHERE l.tenant_id=$1 AND l.logical_call_id=$2
               GROUP BY l.outcome""",
            tenant_id, logical_id,
        )
        await conn.close()
        state = {
            "tenant_id": str(tenant_id), "logical_call_id": logical_id,
            "physical_attempt_id": attempt_id, "outcome": row["outcome"],
            "attempts": row["attempts"], "duplicate_delivery": duplicate,
        }
        receipts.append(DurableProviderFaultReceipt(
            boundary, duplicate, str(tenant_id), logical_id, attempt_id,
            row["outcome"], len(stdout), event_count, 1, row["attempts"],
            canonical_sha256(state),
            input_tokens, output_tokens, cache_tokens, usage_exactness,
        ))
    return receipts[0], receipts[1]


def _reported_usage(stdout: bytes) -> dict[str, int]:
    """Extract provider-reported usage; never estimate absent fields."""
    for line in reversed(stdout.splitlines()):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        usage = event.get("usage")
        if event.get("type") == "turn.completed" and isinstance(usage, dict):
            return {
                key: int(usage[key])
                for key in ("input_tokens", "output_tokens", "cached_input_tokens")
                if isinstance(usage.get(key), int)
            }
    return {}


async def run_provider_fault_slice(dsn: str) -> ProviderFaultSlice:
    require_codex_cli_environment()
    run_id, rows = str(uuid4()), []
    for boundary in PROVIDER_BOUNDARIES:
        outcome, stdout, events, started, ended = await _codex_call(boundary)
        rows.extend(await _persist_and_query(
            dsn, tenant_id=uuid4(), boundary=boundary, outcome=outcome,
            stdout=stdout, event_count=events, started=started, ended=ended,
        ))
    payload = {"database_run_id": run_id, "provider": "codex-cli", "model": "gpt-5.4", "receipts": [asdict(x) for x in rows]}
    return ProviderFaultSlice(run_id, "codex-cli", "gpt-5.4", tuple(rows), canonical_sha256(payload))
