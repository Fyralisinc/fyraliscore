"""Tests for the `ingestion.raw` Kafka envelope Pydantic model (M1.4)."""
from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from services.ingestion.raw_tier.envelope import RawEnvelope


def _minimal_kwargs() -> dict:
    return {
        "source": "slack",
        "tenant_id": UUID("019e34fb-ab3a-7000-9463-5f51662b2be3"),
        "raw_s3_key": "dev/slack/abc/2026-05/12/12abc.json.zst",
        "content_hash": "12abcdef" * 5,  # 40 hex chars (20 bytes)
        "ingested_at": datetime(2026, 5, 17, 12, 0, tzinfo=timezone.utc),
        "ingress_kind": "backfill",
    }


def test_envelope_valid_minimal() -> None:
    env = RawEnvelope(**_minimal_kwargs())
    assert env.envelope_version == 1
    assert env.source == "slack"
    assert env.ingress_kind == "backfill"
    # Optional fields default to empty dicts.
    assert env.ingress_metadata == {}
    assert env.idem_hints == {}


def test_envelope_rejects_unknown_source() -> None:
    bad = _minimal_kwargs()
    bad["source"] = "linkedin"  # not in SourceLiteral
    with pytest.raises(ValidationError):
        RawEnvelope(**bad)


def test_envelope_rejects_unknown_ingress_kind() -> None:
    bad = _minimal_kwargs()
    bad["ingress_kind"] = "stream"  # not in IngressKindLiteral
    with pytest.raises(ValidationError):
        RawEnvelope(**bad)


def test_envelope_version_pinned() -> None:
    bad = _minimal_kwargs()
    bad["envelope_version"] = 2
    with pytest.raises(ValidationError):
        RawEnvelope(**bad)


def test_envelope_round_trips_via_model_dump() -> None:
    """Pydantic v2 model_dump → model_validate round-trip preserves
    semantics including the UUID/datetime types. This is the path
    the producer/consumer take in practice (orjson encodes the dump,
    consumer validates the parse).
    """
    original = RawEnvelope(**_minimal_kwargs())
    dump = original.model_dump(mode="json")
    rebuilt = RawEnvelope.model_validate(dump)
    assert rebuilt == original


def test_envelope_rejects_extra_top_level_fields() -> None:
    """Forward-compat guard: extra="forbid" rejects unknown keys so a
    v2 producer can't land bad data on a v1 consumer.
    """
    bad = _minimal_kwargs()
    bad["v2_only_field"] = "value"
    with pytest.raises(ValidationError):
        RawEnvelope(**bad)


def test_envelope_rejects_empty_raw_s3_key() -> None:
    bad = _minimal_kwargs()
    bad["raw_s3_key"] = ""
    with pytest.raises(ValidationError):
        RawEnvelope(**bad)


def test_envelope_rejects_bad_tenant_uuid() -> None:
    bad = _minimal_kwargs()
    bad["tenant_id"] = "not-a-uuid"
    with pytest.raises(ValidationError):
        RawEnvelope(**bad)
