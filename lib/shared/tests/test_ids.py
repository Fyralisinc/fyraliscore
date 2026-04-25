"""Tests for lib/shared/ids.py — UUID v7 + tenant context."""
from __future__ import annotations

import asyncio
import os
import time
import uuid

import pytest
from hypothesis import given, strategies as st

from lib.shared.ids import (
    current_tenant,
    extract_timestamp_ms,
    is_uuid7,
    reset_tenant,
    set_tenant,
    tenant_scope,
    uuid7,
)


# ---------------------------------------------------------------------
# UUID v7 — core correctness
# ---------------------------------------------------------------------

def test_uuid7_version_and_variant():
    u = uuid7()
    assert is_uuid7(u)
    assert u.version == 7
    # Variant bits are the top two of byte 8 (bit index 63-62 in a
    # big-endian 128-bit int). RFC 4122 says "10xx".
    variant_bits = (u.int >> 62) & 0b11
    assert variant_bits == 0b10


def test_uuid7_embeds_supplied_timestamp():
    ts = 1_700_000_000_000
    u = uuid7(timestamp_ms=ts)
    assert extract_timestamp_ms(u) == ts


def test_uuid7_timestamp_is_recent_by_default():
    u = uuid7()
    now_ms = int(time.time() * 1000)
    ts = extract_timestamp_ms(u)
    assert abs(now_ms - ts) < 2000


def test_uuid7_monotonic_within_process():
    # 2000 consecutive calls should be strictly non-decreasing by int
    ids = [uuid7() for _ in range(2000)]
    for a, b in zip(ids, ids[1:]):
        assert a.int < b.int, f"monotonicity violated between {a} and {b}"


def test_uuid7_time_sortable():
    # IDs generated across several ms windows must sort by time.
    ids: list[uuid.UUID] = []
    for ms in (1_700_000_000_000, 1_700_000_001_000, 1_700_000_002_000):
        ids.append(uuid7(timestamp_ms=ms))
    assert ids == sorted(ids, key=lambda u: u.int)


def test_uuid7_counter_rollover_bumps_timestamp():
    # Call 4096 times with same timestamp. The 4096th (counter-wraps)
    # must bump to ms+1.
    ts = 1_700_000_000_000
    ids = [uuid7(timestamp_ms=ts) for _ in range(4097)]
    timestamps = [extract_timestamp_ms(u) for u in ids]
    # At least one id must sit at ts+1 because the counter overflowed.
    assert timestamps[-1] >= ts + 1


def test_uuid7_timestamp_bounds():
    with pytest.raises(ValueError):
        uuid7(timestamp_ms=-1)
    with pytest.raises(ValueError):
        uuid7(timestamp_ms=1 << 48)


def test_extract_timestamp_rejects_non_v7():
    v4 = uuid.uuid4()
    with pytest.raises(ValueError):
        extract_timestamp_ms(v4)


def test_is_uuid7_false_on_v4():
    assert not is_uuid7(uuid.uuid4())


def test_is_uuid7_true_on_generated():
    assert is_uuid7(uuid7())


def test_uuid7_high_entropy():
    # 10k ids — every one unique (randomness is enough on top of the counter).
    n = 10_000
    ids = {uuid7() for _ in range(n)}
    assert len(ids) == n


@given(st.integers(min_value=0, max_value=(1 << 48) - 1))
def test_uuid7_roundtrip_property(ts: int):
    u = uuid7(timestamp_ms=ts)
    assert is_uuid7(u)
    assert extract_timestamp_ms(u) == ts


# ---------------------------------------------------------------------
# Tenant context
# ---------------------------------------------------------------------

def test_current_tenant_no_fallback_raises(monkeypatch):
    monkeypatch.delenv("DEFAULT_TENANT_ID", raising=False)
    import contextvars
    # A fresh context never had set_tenant called, so the ContextVar
    # sees its default (None).
    ctx = contextvars.copy_context()
    with pytest.raises(LookupError):
        ctx.run(current_tenant)


def test_current_tenant_honors_env_fallback(monkeypatch):
    fallback = "00000000-0000-0000-0000-000000000001"
    monkeypatch.setenv("DEFAULT_TENANT_ID", fallback)
    # Need a fresh context where no explicit tenant is bound.
    import contextvars
    ctx = contextvars.copy_context()
    assert ctx.run(current_tenant) == uuid.UUID(fallback)


def test_set_tenant_and_reset(monkeypatch):
    monkeypatch.delenv("DEFAULT_TENANT_ID", raising=False)
    u = uuid7()
    token = set_tenant(u)
    try:
        assert current_tenant() == u
    finally:
        reset_tenant(token)


def test_tenant_scope_context_manager(monkeypatch):
    monkeypatch.delenv("DEFAULT_TENANT_ID", raising=False)
    u = uuid7()
    with tenant_scope(u) as bound:
        assert bound == u
        assert current_tenant() == u
    with pytest.raises(LookupError):
        current_tenant()


def test_tenant_scope_restores_on_exception(monkeypatch):
    monkeypatch.delenv("DEFAULT_TENANT_ID", raising=False)
    outer = uuid7()
    inner = uuid7()
    with tenant_scope(outer):
        try:
            with tenant_scope(inner):
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert current_tenant() == outer


def test_tenant_scope_accepts_string():
    s = "123e4567-e89b-12d3-a456-426614174000"
    with tenant_scope(s) as bound:
        assert bound == uuid.UUID(s)
        assert current_tenant() == uuid.UUID(s)


async def test_tenant_is_concurrency_safe():
    """Each asyncio task gets its own tenant binding."""

    async def task(tenant: uuid.UUID) -> uuid.UUID:
        set_tenant(tenant)
        await asyncio.sleep(0)
        return current_tenant()

    tenants = [uuid7() for _ in range(20)]
    results = await asyncio.gather(*(task(t) for t in tenants))
    assert results == tenants
