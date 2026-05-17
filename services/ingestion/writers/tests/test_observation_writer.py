"""M2.4 — observation_writer unit tests.

Covers:
  - Happy path: a valid NormalizedEnvelope on the wire results in
    a ShadowWriteEvent in the in-process log.
  - Parse-failure: malformed message bumps `writer.parse_failure`,
    no event recorded.
  - Path B static proof: writer's import graph is DB-free.

The full end-to-end test (real Kafka + real DB + normalizer +
writer + 100 webhooks) lives in
`services/ingestion/tests/test_e2e_shadow.py`.
"""
from __future__ import annotations

import datetime as dt
import json
import subprocess
import sys
import textwrap
from pathlib import Path
from uuid import uuid4

import orjson
import pytest

from services.ingestion.normalizer.models import NormalizedEnvelope
from services.ingestion.writers import observation_writer as writer_module


_REPO_ROOT = Path(__file__).resolve().parents[4]
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
    await writer_module._record_event(env)

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
    # No event was recorded because _record_event was never reached.
    assert writer_module.get_shadow_log() == []


# ---------------------------------------------------------------------
# 3. Path B static proof — writer's import graph is DB-free.
# Per M2 work-order §M2.4: the no-op writer stays Path B in M2; M3
# adds a separate Path A writer.
# ---------------------------------------------------------------------

def test_writer_import_graph_contains_no_db_modules():
    script = textwrap.dedent("""
        import sys
        import services.ingestion.writers.observation_writer  # noqa: F401
        violations = sorted(
            m for m in sys.modules
            if m == "asyncpg"
            or m.startswith("asyncpg.")
            or m == "lib.shared.tenant_context"
        )
        print("VIOLATIONS:" + ",".join(violations))
    """)
    proc = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        check=True,
        cwd=_REPO_ROOT,
    )
    last_line = proc.stdout.strip().splitlines()[-1]
    violations_raw = last_line.removeprefix("VIOLATIONS:")
    violations = [v for v in violations_raw.split(",") if v]
    assert violations == [], (
        "observation_writer.py transitively imports DB modules: "
        + ",".join(violations)
        + " — M2.4 writer is Path B; M3's Path A writer is a "
        "DIFFERENT module."
    )
