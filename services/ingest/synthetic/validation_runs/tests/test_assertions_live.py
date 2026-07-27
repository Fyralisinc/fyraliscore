"""Phase 2 (A30) — unit tests for the live + cross-path assertions.

Pure-data assertions (attribution, signature gate, replay) run without a
DB; the cross-path twin assertion uses `fresh_db`; the partition-boundary
assertion's persistence/DLQ verdict logic is tested with controlled readers.
"""

from __future__ import annotations

import datetime as dt
import uuid

import asyncpg
import pytest

from services.ingest.source_contract.catalog import SOURCE_DEFINITIONS
from services.ingest.synthetic.validation_runs import assertions as A


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
        uuid.uuid4(),
        tenant_id,
        occurred_at,
        channel,
        external_id,
    )


# ---------------------------------------------------------------------
# Cross-path twin dedup (load-bearing).
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_cross_path_twins_dedup_passes_for_clean_dedup(
    fresh_db: asyncpg.Pool,
) -> None:
    tid = uuid.uuid4()
    await fresh_db.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tid, "t")
    await _insert_obs(
        fresh_db,
        tid,
        channel="slack:message",
        external_id="C1:1767225600.000001",
        occurred_at="2026-01-01T00:00:00+00:00",
    )
    n = await A.assert_cross_path_twins_dedup(
        fresh_db,
        {"slack": "C1:1767225600.000001"},
    )
    assert n == 1


@pytest.mark.asyncio
async def test_cross_path_twins_dedup_detects_duplicate(
    fresh_db: asyncpg.Pool,
) -> None:
    """Two rows sharing an external_id (differing only by occurred_at —
    the dedup-didn't-collapse failure mode) must trip the assertion."""
    tid = uuid.uuid4()
    await fresh_db.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tid, "t")
    ext = "I_kwDOtwin"
    await _insert_obs(
        fresh_db,
        tid,
        channel="github:webhook",
        external_id=ext,
        occurred_at="2026-01-01T00:00:00+00:00",
    )
    await _insert_obs(
        fresh_db,
        tid,
        channel="github:webhook",
        external_id=ext,
        occurred_at="2026-01-02T00:00:00+00:00",
    )
    with pytest.raises(A.PropertyViolation, match="dedup FAILED"):
        await A.assert_cross_path_twins_dedup(fresh_db, {"github": ext})


@pytest.mark.asyncio
async def test_cross_path_twins_dedup_excludes_discord_correctly(
    fresh_db: asyncpg.Pool,
) -> None:
    with pytest.raises(A.PropertyViolation, match="discord"):
        await A.assert_cross_path_twins_dedup(
            fresh_db,
            {"discord": "discord:msg-y2-1"},
        )


@pytest.mark.asyncio
async def test_cross_path_twins_dedup_rejects_empty(
    fresh_db: asyncpg.Pool,
) -> None:
    with pytest.raises(A.PropertyViolation, match="vacuous"):
        await A.assert_cross_path_twins_dedup(fresh_db, {})


# ---------------------------------------------------------------------
# Observation persistence reaches the T1 trigger boundary.
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_observations_have_exactly_one_same_tenant_t1_trigger(
    fresh_db: asyncpg.Pool,
) -> None:
    tid = uuid.uuid4()
    await fresh_db.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tid, "t1")
    observation_id = uuid.uuid4()
    await fresh_db.execute(
        """
        INSERT INTO observations (
            id, tenant_id, occurred_at, kind, source_channel,
            external_id, content, content_text, trust_tier
        ) VALUES ($1, $2, $3, 'message', 'slack:message', 't1-event',
                  '{}'::jsonb, 'x', 'trusted')
        """,
        observation_id,
        tid,
        dt.datetime.fromisoformat("2026-01-01T00:00:00+00:00"),
    )
    await fresh_db.execute(
        """
        INSERT INTO think_trigger_queue (
            id, tenant_id, trigger_kind, trigger_subkind, observation_id
        ) VALUES ($1, $2, 'T1', 'event_arrival', $3)
        """,
        uuid.uuid4(),
        tid,
        observation_id,
    )

    assert (
        await A.assert_observations_have_exactly_one_t1_trigger(
            fresh_db,
            {tid},
        )
        == 1
    )


@pytest.mark.asyncio
async def test_observations_t1_assertion_rejects_missing_and_duplicate(
    fresh_db: asyncpg.Pool,
) -> None:
    tid = uuid.uuid4()
    await fresh_db.execute("INSERT INTO tenants (id, name) VALUES ($1,$2)", tid, "t1")
    await _insert_obs(
        fresh_db,
        tid,
        channel="github:webhook",
        external_id="missing-t1",
        occurred_at="2026-01-01T00:00:00+00:00",
    )
    with pytest.raises(A.PropertyViolation, match="exactly one"):
        await A.assert_observations_have_exactly_one_t1_trigger(
            fresh_db,
            {tid},
        )

    inserted = await fresh_db.fetchval(
        "SELECT id FROM observations WHERE tenant_id = $1 LIMIT 1",
        tid,
    )
    assert inserted is not None
    for _ in range(2):
        await fresh_db.execute(
            """
            INSERT INTO think_trigger_queue (
                id, tenant_id, trigger_kind, trigger_subkind, observation_id
            ) VALUES ($1, $2, 'T1', 'event_arrival', $3)
            """,
            uuid.uuid4(),
            tid,
            inserted,
        )
    with pytest.raises(A.PropertyViolation, match="exactly one"):
        await A.assert_observations_have_exactly_one_t1_trigger(
            fresh_db,
            {tid},
        )


# ---------------------------------------------------------------------
# Partition-boundary positive assertion (persistence + DLQ verdict logic).
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_partition_missing_reader_subscribes_to_contract_dlq_lanes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[tuple[str, ...]] = []

    class _FakeConsumer:
        def __init__(self, *topics: str, **_kwargs: object) -> None:
            captured.append(topics)

        async def start(self) -> None:
            return None

        async def getmany(self, **_kwargs: object) -> dict[object, list[object]]:
            return {}

        async def stop(self) -> None:
            return None

    monkeypatch.setattr(A, "AIOKafkaConsumer", _FakeConsumer)

    assert (
        await A.assert_zero_partition_missing(
            bootstrap_servers="unused",
            poll_timeout_ms=1,
        )
        == 0
    )
    expected = tuple(
        f"ingestion.dlq.{definition.source_id}" for definition in SOURCE_DEFINITIONS
    )
    assert captured == [expected]
    assert "ingestion.dlq" not in captured[0]


@pytest.mark.asyncio
async def test_partition_boundary_assertion_checks_persistence_and_dlq_reasons(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from types import SimpleNamespace

    tenant_id = uuid.uuid4()
    recovered = {
        definition.source_id: SimpleNamespace(
            tenant_id=tenant_id,
            external_id=f"partition-recovery:{definition.source_id}",
            occurred_at="2020-01-15T12:00:00+00:00",
        )
        for definition in SOURCE_DEFINITIONS
    }
    rejected = {
        definition.source_id: SimpleNamespace(
            tenant_id=tenant_id,
            external_id=f"partition-out-of-bounds:{definition.source_id}",
            occurred_at="2100-01-15T12:00:00+00:00",
        )
        for definition in SOURCE_DEFINITIONS
    }

    class _Pool:
        async def fetchval(self, _query: str, *args: object) -> int:
            external_id = str(args[1])
            return 0 if "out-of-bounds" in external_id else 1

    failures = {
        "partition_missing": [],
        "out_of_bounds_occurred_at": ["rejected"] * len(rejected),
    }

    async def _fake_read(**_kwargs: object) -> dict[str, list[str]]:
        return failures

    monkeypatch.setattr(A, "_read_partition_failures", _fake_read)

    assert (
        await A.assert_partition_boundary_contract(
            pool=_Pool(),  # type: ignore[arg-type]
            bootstrap_servers="x",
            recovered=recovered,
            rejected_out_of_bounds=rejected,
        )
        == len(SOURCE_DEFINITIONS)
    )

    failures["out_of_bounds_occurred_at"].pop()
    with pytest.raises(A.PropertyViolation, match="out_of_bounds_occurred_at"):
        await A.assert_partition_boundary_contract(
            pool=_Pool(),  # type: ignore[arg-type]
            bootstrap_servers="x",
            recovered=recovered,
            rejected_out_of_bounds=rejected,
        )

    failures["out_of_bounds_occurred_at"].append("rejected")
    failures["partition_missing"].append("residual")
    with pytest.raises(A.PropertyViolation, match="residual partition_missing"):
        await A.assert_partition_boundary_contract(
            pool=_Pool(),  # type: ignore[arg-type]
            bootstrap_servers="x",
            recovered=recovered,
            rejected_out_of_bounds=rejected,
        )


# ---------------------------------------------------------------------
# Pure-data assertions.
# ---------------------------------------------------------------------
@pytest.mark.asyncio
async def test_live_attribution_passes_and_fails() -> None:
    a = uuid.uuid4()
    b = uuid.uuid4()
    assert (
        await A.assert_live_observations_attributed_correctly(
            {a: 5, b: 5},
            {a: 5, b: 5},
        )
        == 2
    )
    with pytest.raises(A.PropertyViolation):
        await A.assert_live_observations_attributed_correctly(
            {a: 4, b: 5},
            {a: 5, b: 5},
        )


@pytest.mark.asyncio
async def test_signature_gate_scoped_to_hmac_sources() -> None:
    ok = [
        {"source": "slack", "http_status": 401},
        {"source": "github", "http_status": 401},
    ]
    assert await A.assert_signature_validation_gate_holds_for_hmac_sources(ok) == 2
    assert (
        await A.assert_signature_validation_gate_holds_for_hmac_sources(
            [{"source": "whatsapp", "http_status": 403}],
            expected_sources=("whatsapp",),
        )
        == 1
    )
    # Wrong status.
    with pytest.raises(A.PropertyViolation):
        await A.assert_signature_validation_gate_holds_for_hmac_sources(
            [
                {"source": "slack", "http_status": 200},
                {"source": "github", "http_status": 401},
            ]
        )
    # Wrong source set (gmail must NOT be a signature-gate source).
    with pytest.raises(A.PropertyViolation):
        await A.assert_signature_validation_gate_holds_for_hmac_sources(
            [{"source": "gmail", "http_status": 401}]
        )


@pytest.mark.asyncio
async def test_replay_idempotency_scoped_excludes_discord() -> None:
    assert (
        await A.assert_live_replay_idempotency_holds(
            {
                "slack": {"dispatched_unique": 1, "observed": 1},
                "github": {"dispatched_unique": 1, "observed": 1},
                "gmail": {"dispatched_unique": 1, "observed": 1},
            },
        )
        == 3
    )
    # Duplicate slipped through.
    with pytest.raises(A.PropertyViolation):
        await A.assert_live_replay_idempotency_holds(
            {"slack": {"dispatched_unique": 1, "observed": 2}}
        )
    # Discord must not be present.
    with pytest.raises(A.PropertyViolation, match="discord"):
        await A.assert_live_replay_idempotency_holds(
            {"discord": {"dispatched_unique": 1, "observed": 1}}
        )
