"""M3.1 — DLQ envelope model tests."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import orjson
import pytest
from pydantic import ValidationError

from services.ingestion.dlq.models import DLQEnvelope


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _valid_fields() -> dict:
    return {
        "tenant_id": uuid4(),
        "source": "slack",
        "failure_kind": "normalizer.parse_failure",
        "raw_s3_key": "dev/slack/00000000-0000-0000-0000-000000000000/2026-05/aa/" + "a" * 40 + ".json",
        "error_summary": "json decode failed at byte 42",
        "error_context": {"offset": 42, "partition": 3},
        "failed_at": _NOW,
    }


def test_dlq_envelope_pydantic_round_trip():
    env = DLQEnvelope(**_valid_fields())
    raw = orjson.dumps(env.model_dump(mode="json"))
    parsed = DLQEnvelope.model_validate(orjson.loads(raw))
    assert parsed == env


def test_dlq_envelope_rejects_unknown_failure_kind():
    """failure_kind is a Literal; unknown values must fail validation
    rather than silently land in the DB."""
    fields = _valid_fields()
    fields["failure_kind"] = "writer.disk_full"  # not in WireFailureKind
    with pytest.raises(ValidationError):
        DLQEnvelope(**fields)


def test_dlq_envelope_version_pinned():
    """envelope_version is Literal[1]; rejects v2 producer output until
    a corresponding consumer migration ships."""
    fields = _valid_fields()
    fields["envelope_version"] = 2
    with pytest.raises(ValidationError):
        DLQEnvelope(**fields)
    # Default value is 1 when omitted.
    fields.pop("envelope_version", None)
    assert DLQEnvelope(**fields).envelope_version == 1


def test_dlq_envelope_rejects_extra_field():
    fields = _valid_fields()
    fields["sneaky_extra"] = "no"
    with pytest.raises(ValidationError):
        DLQEnvelope(**fields)


def test_dlq_envelope_rejects_empty_error_summary():
    """error_summary requires min_length=1 — an empty failure record
    is operationally useless."""
    fields = _valid_fields()
    fields["error_summary"] = ""
    with pytest.raises(ValidationError):
        DLQEnvelope(**fields)


def test_dlq_envelope_truncates_huge_error_summary():
    """max_length=500 caps producers from filling the DB with
    multi-MB stack traces."""
    fields = _valid_fields()
    fields["error_summary"] = "x" * 501
    with pytest.raises(ValidationError):
        DLQEnvelope(**fields)


def test_dlq_envelope_allows_null_raw_s3_key():
    """Some failure modes (e.g. byte garbage on Kafka) have no
    upstream S3 reference — raw_s3_key is nullable."""
    fields = _valid_fields()
    fields["raw_s3_key"] = None
    env = DLQEnvelope(**fields)
    assert env.raw_s3_key is None
