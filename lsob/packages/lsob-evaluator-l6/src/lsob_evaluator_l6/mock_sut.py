"""Deterministic mock SUT used by Layer 6 tests and the CLI's `--sut mock`.

Returns canned `DiffOp` payloads for triggers known to the fixture. For
unknown triggers it returns a minimal empty diff so the evaluator still has
something to compare.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from lsob_contracts import DiffOp, Trigger


class MockDiffProducingSUT:
    name: str = "mock-diff-producing-sut"
    max_concurrent_ingestion: int = 1

    def __init__(self, canned: dict[str, dict[str, Any]] | None = None) -> None:
        # `canned` maps trigger_id -> raw dict that can validate into DiffOp.
        self._canned: dict[str, dict[str, Any]] = canned or {}

    async def produce_diff_for_trigger(self, trigger: Trigger) -> DiffOp:
        data = self._canned.get(trigger.trigger_id)
        if data is not None:
            payload = {**data, "trigger_id": trigger.trigger_id}
            payload.setdefault("diff_id", f"sut-{trigger.trigger_id}")
            payload.setdefault(
                "produced_at",
                trigger.timestamp.isoformat(),
            )
            return DiffOp.model_validate(payload)
        # Fallback: return an empty diff.
        return DiffOp(
            diff_id=f"sut-empty-{trigger.trigger_id}",
            produced_at=datetime.now(tz=timezone.utc),
            trigger_id=trigger.trigger_id,
            claim_ops=[],
            act_ops=[],
            resource_ops=[],
            rationale="mock-empty",
        )
