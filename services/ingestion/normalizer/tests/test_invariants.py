"""M2.4 — RawEnvelope invariants.

Each invariant gets a positive case (valid envelope passes) and a
negative case (specific violation raises EnvelopeInvariantError).
Catches producer-side regressions even when Pydantic schema
validation would have passed.
"""
from __future__ import annotations

import datetime as dt
from uuid import uuid4

import pytest

from services.ingestion.normalizer.invariants import (
    EnvelopeInvariantError,
    assert_envelope_invariants,
)
from services.ingestion.raw_tier.envelope import RawEnvelope


_NOW = dt.datetime(2026, 5, 17, 12, 0, 0, tzinfo=dt.timezone.utc)


def _good_envelope(**overrides) -> RawEnvelope:
    tenant = uuid4()
    content_hash = "a" * 40
    defaults = dict(
        source="slack",
        tenant_id=tenant,
        raw_s3_key=f"dev/slack/{tenant}/2026-05/aa/{content_hash}.json",
        content_hash=content_hash,
        ingested_at=_NOW,
        ingress_kind="webhook",
        ingress_metadata={},
        idem_hints={},
    )
    defaults.update(overrides)
    return RawEnvelope(**defaults)


def test_valid_envelope_passes():
    assert_envelope_invariants(_good_envelope(), now=_NOW)


def test_short_content_hash_raises():
    env = _good_envelope(content_hash="a" * 39)  # 1 char short
    # Pydantic accepts (min_length=1), but invariant catches.
    with pytest.raises(EnvelopeInvariantError, match="40 lower-hex"):
        assert_envelope_invariants(env, now=_NOW)


def test_uppercase_content_hash_raises():
    env = _good_envelope(content_hash="A" * 40)
    with pytest.raises(EnvelopeInvariantError, match="40 lower-hex"):
        assert_envelope_invariants(env, now=_NOW)


def test_bad_s3_key_shape_raises():
    env = _good_envelope(raw_s3_key="garbage/key/without/sense")
    with pytest.raises(EnvelopeInvariantError, match="LLD .5.1 shape"):
        assert_envelope_invariants(env, now=_NOW)


def test_s3_key_prefix_mismatch_raises():
    """raw_s3_key's second-to-last segment must equal
    content_hash[:2]. Mismatch indicates producer-side path mixup."""
    tenant = uuid4()
    content_hash = "ab" + "0" * 38
    # Build a key that's well-shaped but has wrong prefix segment.
    wrong_prefix = "cd"  # not "ab"
    env = _good_envelope(
        tenant_id=tenant,
        content_hash=content_hash,
        raw_s3_key=f"dev/slack/{tenant}/2026-05/{wrong_prefix}/{content_hash}.json",
    )
    with pytest.raises(EnvelopeInvariantError, match="prefix segment"):
        assert_envelope_invariants(env, now=_NOW)


def test_future_ingested_at_raises():
    env = _good_envelope(
        ingested_at=_NOW + dt.timedelta(hours=2),  # > 1h tolerance
    )
    with pytest.raises(EnvelopeInvariantError, match="in the future"):
        assert_envelope_invariants(env, now=_NOW)


def test_distant_past_ingested_at_raises():
    env = _good_envelope(
        ingested_at=_NOW - dt.timedelta(days=45),  # > 30d window
    )
    with pytest.raises(EnvelopeInvariantError, match="in the past"):
        assert_envelope_invariants(env, now=_NOW)


def test_clock_skew_within_tolerance_passes():
    """1 hour future + 30 days past are both inside the window."""
    assert_envelope_invariants(
        _good_envelope(ingested_at=_NOW + dt.timedelta(seconds=3500)),
        now=_NOW,
    )
    assert_envelope_invariants(
        _good_envelope(ingested_at=_NOW - dt.timedelta(days=29)),
        now=_NOW,
    )


def test_forbidden_raw_guild_id_in_metadata_raises():
    """SC-006: raw guild_id in ingress_metadata is a security
    regression — the Discord helper must hash it first."""
    env = _good_envelope(
        ingress_metadata={"guild_id": "1504477009927999569"},
    )
    with pytest.raises(EnvelopeInvariantError, match="SC-006"):
        assert_envelope_invariants(env, now=_NOW)


def test_short_guild_hash_in_metadata_passes():
    """Hashed identifier is fine; only the raw key is forbidden."""
    env = _good_envelope(
        ingress_metadata={"short_guild_hash": "a1b2c3d4e5f60718"},
    )
    assert_envelope_invariants(env, now=_NOW)


def test_error_inherits_value_error():
    """Defensive: a caller that catches ValueError (e.g. legacy
    parse error handling) still sees EnvelopeInvariantError as a
    recoverable condition."""
    assert issubclass(EnvelopeInvariantError, ValueError)
