#!/usr/bin/env python3
"""Run the single bounded real-provider batch required by P1."""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import sys
import time

from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p1_population import (  # noqa: E402
    build_p1_population,
    production_payload,
)
from lib.llm.provider import build_provider  # noqa: E402
from lib.llm.telemetry import InMemoryLLMReceiptSink  # noqa: E402


class _SmokeResult(BaseModel):
    summary: str
    referenced_signal_ids: list[str] = Field(min_length=1)


async def _run() -> dict[str, object]:
    batch = build_p1_population().batches[0]
    payload = [production_payload(signal) for signal in batch]
    user = json.dumps({"signals": payload}, sort_keys=True, separators=(",", ":"))
    provider = build_provider()
    sink = InMemoryLLMReceiptSink()
    provider.set_receipt_sink(sink)
    started_at = datetime.now(timezone.utc)
    started = time.monotonic()
    result: _SmokeResult | None = None
    error: BaseException | None = None
    try:
        result = await provider.structured(
            system=(
                "Summarize this normalized company signal batch. "
                "Return only the requested structure."
            ),
            user=user,
            schema=_SmokeResult,
            max_attempts=3,
            deadline_s=240,
            context_digest=sha256(user.encode()).hexdigest(),
            max_tokens=500,
        )
    except BaseException as exc:  # receipt failures are evidence too
        error = exc

    elapsed_s = time.monotonic() - started
    logical = sink.logical_calls[0] if sink.logical_calls else None
    report: dict[str, object] = {
        "schema_version": "epistemic-repair-p1-real-smoke-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "started_at": started_at.isoformat(),
        "provider": provider.config.provider,
        "model": provider.config.model,
        "input_contract": {
            "batch_count": 1,
            "signal_count": len(batch),
            "individual_signal_calls": 0,
        },
        "elapsed_s": elapsed_s,
        "logical_call_count": len(sink.logical_calls),
        "physical_attempt_count": len(sink.attempts),
        "logical_outcome": logical.outcome if logical else "missing_receipt",
        "attempt_outcomes": [attempt.outcome for attempt in sink.attempts],
        "context_digest_present": bool(logical and logical.context_digest),
        "referenced_signal_count": (
            len(result.referenced_signal_ids) if result is not None else 0
        ),
        "usage_exactness": [attempt.usage_exactness for attempt in sink.attempts],
        "cost_usd": sum(attempt.cost_usd for attempt in sink.attempts),
        "attempt_history": [
            {
                **asdict(attempt),
                "started_at": attempt.started_at.isoformat(),
                "ended_at": attempt.ended_at.isoformat(),
            }
            for attempt in sink.attempts
        ],
        "passed": error is None and result is not None,
        "error": (
            None
            if error is None
            else {"class": type(error).__name__, "message": str(error)[:500]}
        ),
        "proof_boundary": (
            "One clean bounded provider batch; this does not prove semantic quality."
        ),
    }
    return report


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    report = asyncio.run(_run())
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    print(args.output)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
