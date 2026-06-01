"""Phase 2 (A30) — unit tests for the live + cross-path assertions.

Pure-data assertions (attribution, signature gate, replay) run without a
DB; the cross-path twin assertion uses `fresh_db`; the partition-missing
assertion's count→verdict logic is tested by stubbing the Kafka reader.
"""
from __future__ import annotations

import datetime as dt
import uuid

import asyncpg
import pytest

from services.synthetic.validation_runs import assertions as A


pytestmark = pytest.mark.integration


async def _insert_obs(pool, tenant_id, *, channel, external_id, occurred_at):
    if isinstance(occurred_at, str):
        occurred_at = dt.datetime.fromisoformat(occurred_at)
    await pool.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            external_id, content, content_text, trust_tier
        ) VALUES ($1, $2, $3, 'message', $4, $5, '{}'::jsonb, 'x',
                  'trusted')
        """,
        uuid.uuid4(), tenant_id, occurred_at, channel, external_id,
    )


# ---------------------------------------------------------------------
# Cross-path twin dedup (load-bearing).
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cross_path_twins_dedup_passes_for_clean_dedup(
    fresh_db: asyncpg.Pool,
) -> None:
    tid = uuid.uuid4()
    await fresh_db.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)",
                           tid, "t")
    await _insert_obs(fresh_db, tid, channel="slack:message",
                      external_id="C1:1767225600.000001",
                      occurred_at="2026-01-01T00:00:00+00:00")
    n = await A.assert_cross_path_twins_dedup(
        fresh_db, {"slack": "C1:1767225600.000001"},
    )
    assert n == 1


@pytest.mark.asyncio
async def test_cross_path_twins_dedup_detects_duplicate(
    fresh_db: asyncpg.Pool,
) -> None:
    """Two rows sharing an external_id (differing only by occurred_at —
    the dedup-didn't-collapse failure mode) must trip the assertion."""
    tid = uuid.uuid4()
    await fresh_db.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)",
                           tid, "t")
    ext = "I_kwDOtwin"
    await _insert_obs(fresh_db, tid, channel="github:webhook",
                      external_id=ext,
                      occurred_at="2026-01-01T00:00:00+00:00")
    await _insert_obs(fresh_db, tid, channel="github:webhook",
                      external_id=ext,
                      occurred_at="2026-01-02T00:00:00+00:00")
    with pytest.raises(A.PropertyViolation, match="dedup FAILED"):
        await A.assert_cross_path_twins_dedup(fresh_db, {"github": ext})


@pytest.mark.asyncio
async def test_cross_path_twins_dedup_excludes_discord_correctly(
    fresh_db: asyncpg.Pool,
) -> None:
    with pytest.raises(A.PropertyViolation, match="discord"):
        await A.assert_cross_path_twins_dedup(
            fresh_db, {"discord": "discord:msg-y2-1"},
        )


@pytest.mark.asyncio
async def test_cross_path_twins_dedup_rejects_empty(
    fresh_db: asyncpg.Pool,
) -> None:
    with pytest.raises(A.PropertyViolation, match="vacuous"):
        await A.assert_cross_path_twins_dedup(fresh_db, {})


# ---------------------------------------------------------------------
# Partition-missing positive assertion (count→verdict logic).
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_partition_missing_assertion_detects_dlq_entries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_count(*, bootstrap_servers, tenant_ids, poll_timeout_ms):
        return 4
    monkeypatch.setattr(A, "_count_partition_missing", _fake_count)

    # Matches expected → passes.
    assert await A.assert_partition_missing_routes_to_dlq(
        bootstrap_servers="x", expected_count=4,
    ) == 4
    # Mismatch → raises.
    with pytest.raises(A.PropertyViolation, match="expected 2"):
        await A.assert_partition_missing_routes_to_dlq(
            bootstrap_servers="x", expected_count=2,
        )


# ---------------------------------------------------------------------
# Pure-data assertions.
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_live_attribution_passes_and_fails() -> None:
    a = uuid.uuid4()
    b = uuid.uuid4()
    assert await A.assert_live_observations_attributed_correctly(
        {a: 5, b: 5}, {a: 5, b: 5},
    ) == 2
    with pytest.raises(A.PropertyViolation):
        await A.assert_live_observations_attributed_correctly(
            {a: 4, b: 5}, {a: 5, b: 5},
        )


@pytest.mark.asyncio
async def test_signature_gate_scoped_to_hmac_sources() -> None:
    ok = [{"source": "slack", "http_status": 401},
          {"source": "github", "http_status": 401}]
    assert await A.assert_signature_validation_gate_holds_for_hmac_sources(
        ok) == 2
    # Wrong status.
    with pytest.raises(A.PropertyViolation):
        await A.assert_signature_validation_gate_holds_for_hmac_sources(
            [{"source": "slack", "http_status": 200},
             {"source": "github", "http_status": 401}])
    # Wrong source set (gmail must NOT be a signature-gate source).
    with pytest.raises(A.PropertyViolation):
        await A.assert_signature_validation_gate_holds_for_hmac_sources(
            [{"source": "gmail", "http_status": 401}])


@pytest.mark.asyncio
async def test_replay_idempotency_scoped_excludes_discord() -> None:
    assert await A.assert_live_replay_idempotency_holds(
        {"slack": {"dispatched_unique": 1, "observed": 1},
         "github": {"dispatched_unique": 1, "observed": 1},
         "gmail": {"dispatched_unique": 1, "observed": 1}},
    ) == 3
    # Duplicate slipped through.
    with pytest.raises(A.PropertyViolation):
        await A.assert_live_replay_idempotency_holds(
            {"slack": {"dispatched_unique": 1, "observed": 2}})
    # Discord must not be present.
    with pytest.raises(A.PropertyViolation, match="discord"):
        await A.assert_live_replay_idempotency_holds(
            {"discord": {"dispatched_unique": 1, "observed": 1}})
