"""Provider-failure fate closure without requiring an external database."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import pytest

from services.domain.entity_grounding import mention_fates
from services.domain.entity_grounding.learned_discovery import DISCOVERY_BATCHES


class _PersistedBatchConn:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.heads: set[str] = set()
        self.detections: set[str] = set()

    async def fetch(self, query, *_args):
        assert "FROM observations" in query
        return self.rows

    async def fetchval(self, query, _tenant_id, detection_key):
        assert "entity_mention_detection_heads" in query
        return detection_key in self.heads


class _FailedStructuredProvider:
    def __init__(self, mode: str) -> None:
        self.mode = mode
        self.calls: list[dict] = []

    async def structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.mode == "timeout":
            raise TimeoutError("ten-signal structured turn timed out")
        if self.mode == "schema_failure":
            return kwargs["schema"].model_validate({"mentions": {}})
        signals = json.loads(kwargs["user"])["signals"]
        mentions = []
        for index, signal in enumerate(signals):
            surface = signal["content_text"].split()[0]
            mentions.append({
                "signal_id": signal["signal_id"],
                "surface": surface,
                "span_start": 0,
                "span_end": len(surface),
                "entity_type": (
                    "unsupported_planet" if index == len(signals) - 1 else "resource"
                ),
                "confidence": 0.95,
                "abstain": False,
            })
        return kwargs["schema"].model_validate({"mentions": mentions})


def _rows() -> list[dict]:
    base = datetime(2026, 7, 17, 12, 0, tzinfo=timezone.utc)
    return [{
        "id": uuid4(),
        "occurred_at": base + timedelta(seconds=index),
        "source_channel": "jira:issue",
        "content": {"_unresolved_phrases": [f"CASE-{800 + index}"]},
        "content_text": f"CASE-{800 + index} blocked.",
    } for index in range(10)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure_mode", ("schema_failure", "timeout", "unsupported_sibling")
)
async def test_failed_ten_signal_turn_closes_every_fate_and_replays_once(
    monkeypatch: pytest.MonkeyPatch,
    failure_mode: str,
) -> None:
    rows = _rows()
    conn = _PersistedBatchConn(rows)
    provider = _FailedStructuredProvider(failure_mode)
    tenant_id = uuid4()

    async def apply_context(_self, **_kwargs):
        return None

    async def apply_detection(_self, *, command, **_kwargs):
        # This models the immutable detection + current-head uniqueness contract
        # at the appender boundary. A duplicate call would fail this assertion.
        assert command.detection_key not in conn.heads
        conn.detections.add(str(command.detection.detection_id))
        conn.heads.add(command.detection_key)

    monkeypatch.setattr(
        mention_fates.GroundingAnnotationAppender, "apply_context", apply_context
    )
    monkeypatch.setattr(
        mention_fates.GroundingAnnotationAppender,
        "apply_mention_detection",
        apply_detection,
    )
    learned_before = DISCOVERY_BATCHES.get(mode="learned", outcome="success")
    fallback_before = DISCOVERY_BATCHES.get(
        mode="deterministic_fallback", outcome="provider_error"
    )

    first = await mention_fates.ensure_persisted_observation_mention_fates(
        conn=conn,
        tenant_id=tenant_id,
        observation_ids=[UUID(str(row["id"])) for row in rows],
        discovery_provider=provider,
    )

    assert len(provider.calls) == 1
    assert len(json.loads(provider.calls[0]["user"])["signals"]) == 10
    assert first.discovery_mode == "deterministic_fallback"
    assert first.learned_candidates == 0
    assert first.provider_error is not None
    assert (first.eligible_opportunities, first.committed_fates) == (10, 10)
    assert first.coverage == 1.0
    assert len(conn.heads) == len(conn.detections) == 10
    assert DISCOVERY_BATCHES.get(mode="learned", outcome="success") == learned_before
    assert DISCOVERY_BATCHES.get(
        mode="deterministic_fallback", outcome="provider_error"
    ) == fallback_before + 1

    # Resolver work is keyed by the immutable detection head. It is populated
    # once here to make replay duplication observable without a database.
    downstream_work = set(conn.heads)
    replay = await mention_fates.ensure_persisted_observation_mention_fates(
        conn=conn,
        tenant_id=tenant_id,
        observation_ids=reversed([row["id"] for row in rows]),
        discovery_provider=None,
    )

    assert replay.discovery_mode == "deterministic_fallback"
    assert replay.learned_candidates == 0
    assert replay.provider_error is None
    assert (replay.eligible_opportunities, replay.committed_fates) == (10, 0)
    assert replay.existing_fates == 10
    assert replay.coverage == 1.0
    assert len(conn.heads) == len(conn.detections) == len(downstream_work) == 10
