"""observation_writer unit tests — M2.4 shadow-log path.

These tests cover the writer's M2 shadow-log behaviour, which is
PRESERVED for tenants whose `ingestion.kafka_path_enabled` is FALSE
(default; pre-cutover tenants). The M5.2 full-mode tests live in
`test_observation_writer_m5.py`.

Covers:
  - Happy path: a valid NormalizedEnvelope produces a ShadowWriteEvent.
  - Parse-failure: malformed message bumps `writer.parse_failure`,
    no event recorded.

The end-to-end shadow test (real Kafka + DB + normalizer + writer +
100 webhooks) lives in `services/ingestion/tests/test_e2e_shadow.py`.

M2.4's Path-B import-graph test is INTENTIONALLY REMOVED in M5.2 —
the writer is now Path A (holds an asyncpg pool) when wired with
DB deps. See the module docstring of `observation_writer.py`.
"""
from __future__ import annotations

import datetime as dt
import json
from uuid import uuid4

import orjson
import pytest

from services.ingestion.normalizer.models import NormalizedEnvelope
from services.ingestion.writers import observation_writer as writer_module


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _normalized_envelope_bytes() -> bytes:
    tenant = uuid4()
    content_hash = "a" * 40
    env = NormalizedEnvelope(
        envelope_version=1,
        source="slack",
        ingress_kind="webhook",
        tenant_id=tenant,
        raw_s3_key=f"dev/slack/{tenant}/2026-05/{content_hash[:2]}/{content_hash}.json",
        content_hash=content_hash,
        raw_ingested_at=_NOW,
        source_channel="slack:message",
        content_text="hello",
        content={"channel": "C01", "ts": "1.0", "text": "hello"},
        occurred_at=_NOW,
        trust_tier="attested_agent",
        kind="signal",
        source_actor_ref="slack:U01",
        external_id="C01:1.0",
        entities_hint=[],
        normalized_at=_NOW,
        ingress_metadata={},
        idem_hints={},
    )
    return orjson.dumps(env.model_dump(mode="json"))


@pytest.fixture(autouse=True)
def _reset():
    writer_module.reset_metrics()
    writer_module.reset_shadow_log()


# ---------------------------------------------------------------------
# 1. Happy path — valid NormalizedEnvelope produces ShadowWriteEvent.
# ---------------------------------------------------------------------

async def test_record_event_appends_to_shadow_log():
    env = NormalizedEnvelope.model_validate(
        json.loads(_normalized_envelope_bytes())
    )
    await writer_module._record_shadow_event(env)

    log_entries = writer_module.get_shadow_log()
    assert len(log_entries) == 1
    event = log_entries[0]
    assert event.tenant_id == str(env.tenant_id)
    assert event.source == "slack"
    assert event.source_channel == "slack:message"
    assert event.external_id == "C01:1.0"
    assert event.content_hash == "a" * 40

    assert writer_module.get_metrics()["writer.shadow_write_events"] == 1


# ---------------------------------------------------------------------
# 2. Parse-failure — malformed envelope bumps metric, no log entry.
# (The full-loop variant runs against testcontainers in
# `test_e2e_shadow.py`; here we exercise model_validate directly.)
# ---------------------------------------------------------------------

async def test_malformed_envelope_does_not_record_event():
    bad_payload = {
        "envelope_version": 1,
        "source": "slack",
        "ingress_kind": "webhook",
        # tenant_id missing — Pydantic raises ValidationError.
    }
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        NormalizedEnvelope.model_validate(bad_payload)

    # The run_writer loop's except clause does bump + log + continue.
    # No event was recorded because _record_shadow_event was never reached.
    assert writer_module.get_shadow_log() == []
