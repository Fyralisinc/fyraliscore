"""Unit tests for the contract-only normalized observation writer."""
from __future__ import annotations

import datetime as dt
import json
from uuid import uuid4

import orjson
import pytest

from lib.shared.errors import ValidationError
from services.ingest.ingestion.normalizer.models import NormalizedEnvelope
from services.ingest.ingestion.writers import observation_writer as writer_module


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


# ---------------------------------------------------------------------
# 1. Happy path — a valid envelope reconstructs the durable draft.
# ---------------------------------------------------------------------

def test_valid_envelope_reconstructs_draft():
    env = NormalizedEnvelope.model_validate(
        json.loads(_normalized_envelope_bytes())
    )
    draft = writer_module._draft_from_envelope(env)
    assert draft.external_id == "C01:1.0"
    assert draft.content_text == "hello"


def test_full_mode_draft_reconstruction_applies_shared_payload_guards() -> None:
    env = NormalizedEnvelope.model_validate(
        json.loads(_normalized_envelope_bytes())
    ).model_copy(update={"content": {"text": "bad\x00payload"}})
    with pytest.raises(ValidationError, match="NUL byte"):
        writer_module._draft_from_envelope(env)



def test_full_mode_draft_keeps_private_artifact_descriptor_off_content() -> None:
    env = NormalizedEnvelope.model_validate(
        json.loads(_normalized_envelope_bytes())
    ).model_copy(update={
        "content": {"artifacts": [{"blob_id": "blob-1"}]},
        "artifact_descriptors": [{
            "blob_id": "blob-1",
            "bucket": "private-bucket",
            "object_key": "private/key.json",
        }],
    })

    draft = writer_module._draft_from_envelope(env)

    assert draft.content == {"artifacts": [{"blob_id": "blob-1"}]}
    assert draft.artifact_descriptors[0]["bucket"] == "private-bucket"


# ---------------------------------------------------------------------
# 2. Parse-failure — malformed envelope bumps metric, no log entry.
# (The full-loop variant runs against testcontainers in
# `test_e2e_shadow.py`; here we exercise model_validate directly.)
# ---------------------------------------------------------------------

def test_malformed_envelope_is_rejected_before_write():
    bad_payload = {
        "envelope_version": 1,
        "source": "slack",
        "ingress_kind": "webhook",
        # tenant_id missing — Pydantic raises ValidationError.
    }
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        NormalizedEnvelope.model_validate(bad_payload)
