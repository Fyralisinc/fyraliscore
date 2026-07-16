"""M2.3 — NormalizedEnvelope schema validation.

The envelope is the wire format on `ingestion.normalized` — the
contract between the normalizer and (M2.4) the no-op writer / (M3+)
the real writer. Schema regressions would silently land bad data
on downstream consumers.
"""
from __future__ import annotations

import datetime as dt
import json
from uuid import uuid4

import orjson
import pytest
from pydantic import ValidationError

from services.ingest.ingestion.normalizer.models import NormalizedEnvelope


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _valid_fields() -> dict:
    return {
        "envelope_version": 1,
        "source": "slack",
        "ingress_kind": "webhook",
        "tenant_id": uuid4(),
        "raw_s3_key": "dev/slack/00000000-0000-0000-0000-000000000000/2026-05/aa/key.json",
        "content_hash": "a" * 40,
        "raw_ingested_at": _NOW,
        "source_channel": "slack:message",
        "content_text": "hello",
        "content": {"any": "json"},
        "occurred_at": _NOW,
        "trust_tier": "attested_agent",
        "kind": "signal",
        "source_actor_ref": "slack:U01ALICE",
        "external_id": "C01:1234.567",
        "entities_hint": [{"type": "slack_channel", "id": "C01"}],
        "source_identity_claims": [
            {
                "source_system": "slack",
                "source_native_identifier": (
                    "slack:workspace:T01"
                ),
                "source_surface": "Acme",
                "claim_authority_ref": (
                    "slack-handler:structured-workspace-field-v1"
                ),
            }
        ],
        "normalized_at": _NOW,
        "ingress_metadata": {"delivery_id": "x"},
        "idem_hints": {"hint": "y"},
    }


def test_normalized_envelope_round_trips_through_json():
    env = NormalizedEnvelope(**_valid_fields())
    raw = orjson.dumps(env.model_dump(mode="json"))
    parsed = NormalizedEnvelope.model_validate(json.loads(raw))
    assert parsed == env


def test_envelope_rejects_extra_fields():
    """`extra="forbid"` is load-bearing — a producer that slips an
    extra field through would mask a wire-format bump."""
    fields = _valid_fields()
    fields["unexpected"] = "danger"
    with pytest.raises(ValidationError):
        NormalizedEnvelope(**fields)


def test_envelope_rejects_unknown_source():
    fields = _valid_fields()
    fields["source"] = "twitter"  # not in SourceLiteral
    with pytest.raises(ValidationError):
        NormalizedEnvelope(**fields)


def test_envelope_rejects_unknown_ingress_kind():
    fields = _valid_fields()
    fields["ingress_kind"] = "polling"
    with pytest.raises(ValidationError):
        NormalizedEnvelope(**fields)


def test_envelope_requires_non_empty_raw_s3_key():
    fields = _valid_fields()
    fields["raw_s3_key"] = ""
    with pytest.raises(ValidationError):
        NormalizedEnvelope(**fields)


def test_envelope_rejects_cross_source_identity_claim() -> None:
    fields = _valid_fields()
    fields["source_identity_claims"][0]["source_system"] = "jira"
    fields["source_identity_claims"][0][
        "source_native_identifier"
    ] = "jira:acme.atlassian.net:project:10000"
    with pytest.raises(ValidationError, match="must match envelope source"):
        NormalizedEnvelope(**fields)
