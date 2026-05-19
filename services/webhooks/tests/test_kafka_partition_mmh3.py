"""M-Load — verify _kafka_partition_for_tenant uses librdkafka-
compatible murmur2 (mmh3) hashing.

The "actual landing partition match" test against a real broker is
deferred to staging deploy (see m-load-runbook.md §3). This file
covers:
  - Determinism (same key → same partition).
  - Algorithm match against the canonical mmh3 + librdkafka formula
    (computed inline).
  - Uniform distribution across many keys (no obvious bucket skew).
"""
from __future__ import annotations

from uuid import uuid4

import mmh3
import pytest

from services.webhooks.router import _kafka_partition_for_tenant


def test_deterministic():
    tid = uuid4()
    a = _kafka_partition_for_tenant(tid, num_partitions=32)
    b = _kafka_partition_for_tenant(tid, num_partitions=32)
    c = _kafka_partition_for_tenant(str(tid), num_partitions=32)
    assert a == b == c


def test_matches_explicit_librdkafka_formula():
    """The settled-decision formula:
       mmh3.hash(tenant_id_bytes, seed=0x9747b28c, signed=False)
       & 0x7fffffff % num_partitions.
    Verify the function matches it for 100 random tenant UUIDs.
    """
    for _ in range(100):
        tid = uuid4()
        key = str(tid).encode("utf-8")
        expected = (
            mmh3.hash(key, seed=0x9747b28c, signed=False) & 0x7fffffff
        ) % 32
        got = _kafka_partition_for_tenant(tid, num_partitions=32)
        assert got == expected, (
            f"Mismatch for {tid}: got={got}, expected={expected}"
        )


def test_distribution_across_partitions():
    """No obvious bucket bias. Stronger than a smoke test; for 1000
    random keys, every partition (out of 32) should appear at least
    once. (Statistical: with uniform distribution, P(any bucket
    missing in 1000 draws) ≈ 32 × (31/32)^1000 ≈ 0; extremely unlikely.)
    """
    counts: dict[int, int] = {}
    for _ in range(1000):
        p = _kafka_partition_for_tenant(uuid4(), num_partitions=32)
        counts[p] = counts.get(p, 0) + 1
    assert len(counts) == 32, (
        f"Some partitions never hit: missing="
        f"{set(range(32)) - set(counts.keys())}"
    )
    # No partition should be wildly over- or under-represented.
    # Expected ~31 per partition with std-dev ~5; allow 4× to be safe.
    for p, c in counts.items():
        assert 5 <= c <= 100, f"partition {p} hit {c} times — distribution likely broken"


def test_num_partitions_changes_output_safely():
    """Different num_partitions produces different distribution but
    same algorithm shape."""
    tid = uuid4()
    p_32 = _kafka_partition_for_tenant(tid, num_partitions=32)
    p_64 = _kafka_partition_for_tenant(tid, num_partitions=64)
    p_8 = _kafka_partition_for_tenant(tid, num_partitions=8)
    # All within range.
    assert 0 <= p_32 < 32
    assert 0 <= p_64 < 64
    assert 0 <= p_8 < 8
    # p_8 must equal p_32 % 8 ... actually only if hash is the same
    # before mod, which it is in our formula.
    key = str(tid).encode("utf-8")
    h_masked = mmh3.hash(key, seed=0x9747b28c, signed=False) & 0x7fffffff
    assert p_32 == h_masked % 32
    assert p_64 == h_masked % 64
    assert p_8 == h_masked % 8
