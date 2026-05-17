"""M3.2 — EmbeddingEnvelope model tests."""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import orjson
import pytest
from pydantic import ValidationError

from services.ingestion.embedding.models import EmbeddingEnvelope


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _valid_fields() -> dict:
    return {
        "tenant_id": uuid4(),
        "source": "slack",
        "observation_id": uuid4(),
        "enqueued_at": _NOW,
    }


def test_embedding_envelope_pydantic_round_trip():
    env = EmbeddingEnvelope(**_valid_fields())
    raw = orjson.dumps(env.model_dump(mode="json"))
    parsed = EmbeddingEnvelope.model_validate(orjson.loads(raw))
    assert parsed == env


def test_embedding_envelope_rejects_unknown_source():
    """source is a Literal — the inline path filters non-source-family
    channels (internal:*, etc.) BEFORE publish, but if one slips
    through the validation must catch it."""
    fields = _valid_fields()
    fields["source"] = "internal"
    with pytest.raises(ValidationError):
        EmbeddingEnvelope(**fields)


def test_embedding_envelope_version_pinned():
    fields = _valid_fields()
    fields["envelope_version"] = 2
    with pytest.raises(ValidationError):
        EmbeddingEnvelope(**fields)
    fields.pop("envelope_version", None)
    assert EmbeddingEnvelope(**fields).envelope_version == 1


def test_embedding_envelope_rejects_extra_field():
    fields = _valid_fields()
    fields["content_text"] = "no — the worker re-reads this from DB"
    with pytest.raises(ValidationError):
        EmbeddingEnvelope(**fields)


def test_embedding_envelope_requires_observation_id():
    fields = _valid_fields()
    fields.pop("observation_id")
    with pytest.raises(ValidationError):
        EmbeddingEnvelope(**fields)
