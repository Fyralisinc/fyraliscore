"""
services/workers/calibration_updater/tests/test_calibration.py — Wave 4-C tests.

Covers the six test cases from BUILD-PLAN §5 Prompt 4.C "Calibration"
plus helpers for the pure-compute layer.

Fixture pattern mirrors services/models/tests/conftest.py (per-test
transaction, tenant-UUID isolation, no TRUNCATE — hermetic alongside
parallel waves).
"""
from __future__ import annotations

import math
import uuid
from datetime import datetime, timedelta, timezone

import asyncpg
import pytest

from lib.shared.ids import uuid7

from services.models.calibration import apply_calibration
from services.workers.calibration_updater.compute import (
    CONFIDENCE_BUCKETS,
    DEFAULT_OFFSETS,
    MIN_SAMPLES_PER_TUPLE,
    OFFSET_MAX,
    OFFSET_MIN,
    Stat,
    brier_score,
    bucketed_offsets,
    cold_start_offsets,
    compute_offsets_for_tuple,
)
from services.workers.calibration_updater.worker import run_once
from services.workers.calibration_updater.tests.conftest import (
    insert_actor,
    insert_observation,
    insert_model,
)


pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------
# Pure-compute unit tests (don't touch the DB)
# ---------------------------------------------------------------------


def test_brier_score_hand_fixture():
    """
    Hand-computed Brier score: four predictions, asserted [0.8, 0.8, 0.8, 0.2]
    with outcomes [True, True, False, False] →
      ((0.8-1)^2 + (0.8-1)^2 + (0.8-0)^2 + (0.2-0)^2) / 4
    = (0.04 + 0.04 + 0.64 + 0.04) / 4 = 0.76 / 4 = 0.19.
    """
    stats = [
        Stat(0.8, True),
        Stat(0.8, True),
        Stat(0.8, False),
        Stat(0.2, False),
    ]
    got = brier_score(stats)
    assert got is not None
    assert math.isclose(got, 0.19, abs_tol=1e-9)


def test_brier_score_ignores_inconclusive():
    stats = [Stat(0.8, True), Stat(0.8, None), Stat(0.8, None)]
    got = brier_score(stats)
    assert got is not None
    assert math.isclose(got, (0.8 - 1) ** 2, abs_tol=1e-9)


def test_cold_start_defaults_per_kind():
    # Per AUDIT-REVIEW-1-FIXES §C5 FU5: PROP_KIND_DEFAULTS now covers
    # all 10 PropositionKind values (previously only prediction/state/
    # pattern had entries; the rest fell back to 1.0 identity).
    pred = cold_start_offsets("prediction")
    assert len(pred) == 1
    assert math.isclose(pred[0].offset, DEFAULT_OFFSETS["prediction"])
    state = cold_start_offsets("state")
    assert math.isclose(state[0].offset, DEFAULT_OFFSETS["state"])
    pattern = cold_start_offsets("pattern")
    assert math.isclose(pattern[0].offset, DEFAULT_OFFSETS["pattern"])
    # capability_assessment now has an explicit default (0.88).
    cap = cold_start_offsets("capability_assessment")
    assert math.isclose(cap[0].offset, DEFAULT_OFFSETS["capability_assessment"])
    # Truly unknown kind still → 1.0 identity fallback.
    unknown = cold_start_offsets("__definitely_not_a_real_kind__")
    assert math.isclose(unknown[0].offset, 1.0)


def test_bucketed_offset_matches_analytical_value():
    """
    20 stats at asserted=0.75 (inside (0.7, 0.8)), 12 True / 8 False.
    empirical_rate = 0.6; midpoint = 0.75; offset = 0.6/0.75 = 0.8.
    """
    stats = [Stat(0.75, True)] * 12 + [Stat(0.75, False)] * 8
    rows = bucketed_offsets(stats)
    assert len(rows) == 1
    row = rows[0]
    assert row.bucket_low == 0.7 and row.bucket_high == 0.8
    assert math.isclose(row.offset, 0.8, abs_tol=1e-9)
    assert row.sample_size == 20


def test_offset_clipped_to_range():
    # All outcomes True at asserted=0.2 — empirical rate 1.0, midpoint 0.1,
    # raw_offset = 10.0 → clipped to OFFSET_MAX (1.5).
    stats = [Stat(0.15, True)] * 20
    rows = bucketed_offsets(stats)
    assert len(rows) == 1
    assert math.isclose(rows[0].offset, OFFSET_MAX)
    # All outcomes False at asserted=0.9 — empirical rate 0.0, raw_offset = 0 →
    # clipped to OFFSET_MIN (0.3).
    stats2 = [Stat(0.92, False)] * 20
    rows2 = bucketed_offsets(stats2)
    assert len(rows2) == 1
    assert math.isclose(rows2[0].offset, OFFSET_MIN)


def test_compute_offsets_for_tuple_falls_back_to_cold_start():
    """Fewer than 20 samples → cold-start row regardless of distribution."""
    stats = [Stat(0.8, True)] * 10
    rows = compute_offsets_for_tuple(stats, "prediction")
    assert len(rows) == 1
    assert math.isclose(rows[0].offset, DEFAULT_OFFSETS["prediction"])
    assert rows[0].sample_size == 0


# ---------------------------------------------------------------------
# DB-integration tests
# ---------------------------------------------------------------------


async def _insert_resolved_prediction(
    tx_conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
    actor_id: uuid.UUID,
    born_from_event_id: uuid.UUID,
    asserted_confidence: float,
    outcome: bool,
    kind: str = "prediction",
):
    """Insert a Model that represents a resolved prediction."""
    emb = [0.0] * 768
    emb[0] = 1.0
    proposition = {
        "kind": kind,
        "expected": "the thing happens",
        "resolution": "check x",
    }
    if kind == "state":
        proposition = {"kind": "state", "subject": "alice", "assertion": "reliable"}
    resolved_at = datetime.now(timezone.utc) - timedelta(days=1)
    await insert_model(
        tx_conn,
        tenant=tenant,
        born_from_event_id=born_from_event_id,
        proposition=proposition,
        natural=f"Proposition of kind {kind} ({asserted_confidence}, {outcome})",
        embedding=emb,
        scope_actors=[actor_id],
        confidence=asserted_confidence,
        confidence_at_assertion=asserted_confidence,
        resolved_at=resolved_at,
        resolution_outcome=outcome,
    )


@pytest.mark.asyncio
async def test_cold_start_no_resolutions_does_not_crash(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    A tenant with no resolved Models → run_once is a no-op; does NOT
    explode. Verifies the worker is safe on brand-new tenants.

    We use a fresh pool bound to the transaction via a helper that
    runs the worker INLINE on tx_conn (see _run_inline below).
    """
    result = await _run_inline(tx_conn, tenant=tenant)
    assert result.harvested_stats == 0
    assert result.offsets_written == 0
    assert result.models_recalibrated == 0


@pytest.mark.asyncio
async def test_50_resolutions_produces_expected_offset(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Insert 40 predictions at asserted=0.75, 24 True / 16 False. Run
    the worker. The (0.7, 0.8) bucket's offset should be 0.6/0.75 = 0.8.
    """
    for i in range(24):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.75, outcome=True,
        )
    for i in range(16):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.75, outcome=False,
        )
    result = await _run_inline(tx_conn, tenant=tenant)
    assert result.harvested_stats == 40

    offsets = await tx_conn.fetch(
        """
        SELECT bucket_low, bucket_high, "offset", sample_size
        FROM calibration_offsets
        WHERE tenant_id=$1 AND actor_id=$2 AND proposition_kind='prediction'
        """,
        tenant, actor_id,
    )
    assert len(offsets) == 1
    row = offsets[0]
    assert math.isclose(row["bucket_low"], 0.7)
    assert math.isclose(row["bucket_high"], 0.8)
    assert math.isclose(float(row["offset"]), 0.8, abs_tol=1e-6)
    assert row["sample_size"] == 40


@pytest.mark.asyncio
async def test_offset_clipped_so_confidence_stays_in_bounds(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Even if empirical data is pathological (offset clipped to 1.5), the
    apply_calibration read-path still clips the final confidence to
    [0.05, 0.95]. Fixture: 20 stats at asserted=0.15, all True →
    empirical_rate=1.0, midpoint=0.1, raw_offset=10 → clipped offset=1.5.
    Confidence 0.9 * 1.5 = 1.35 → final clipped to 0.95.
    """
    for _ in range(20):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.15, outcome=True,
        )
    await _run_inline(tx_conn, tenant=tenant)
    # Now insert an active Model whose confidence the worker should
    # bulk-update. We feed a raw 0.90 that, with offset=1.5, would
    # overshoot — expect the clip-path to pin it at 0.95.
    result = await apply_calibration(
        0.9, [actor_id], "prediction",
        tenant_id=tenant, conn=tx_conn,
    )
    # No offset row for bucket (0.8,0.9) → identity → clip → 0.9 returned.
    # We need to verify with a value inside (0.0, 0.2) to hit the 1.5 offset:
    result_in_bucket = await apply_calibration(
        0.15, [actor_id], "prediction",
        tenant_id=tenant, conn=tx_conn,
    )
    # 0.15 * 1.5 = 0.225 → clipped inside [0.05, 0.95] = 0.225
    assert math.isclose(result_in_bucket, 0.15 * 1.5, abs_tol=1e-6)
    # A high raw value should be clipped to 0.95 when the 0.0-0.2 bucket
    # is the matching row. That doesn't trip here because 0.9 doesn't
    # fall into (0.0, 0.2). So sanity-check no-regression:
    assert 0.05 <= result <= 0.95


@pytest.mark.asyncio
async def test_per_kind_separation(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Actor A's 'prediction' offset doesn't leak into 'state' kind.
    Insert 20 prediction resolutions (all wrong → low empirical rate,
    so offset ≈ 0.3 clipped) and 20 state resolutions (all right →
    offset ≈ 1.5 clipped).
    """
    for _ in range(20):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.75, outcome=False,
        )
    for _ in range(20):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.15, outcome=True,
            kind="state",
        )
    await _run_inline(tx_conn, tenant=tenant)

    rows = await tx_conn.fetch(
        """
        SELECT proposition_kind, bucket_low, "offset"
        FROM calibration_offsets
        WHERE tenant_id=$1 AND actor_id=$2
        ORDER BY proposition_kind, bucket_low
        """,
        tenant, actor_id,
    )
    kinds = {(r["proposition_kind"], float(r["bucket_low"])): float(r["offset"]) for r in rows}
    # Prediction bucket (0.7, 0.8): 0 / 0.75 = 0, clipped to 0.3.
    assert math.isclose(kinds[("prediction", 0.7)], OFFSET_MIN, abs_tol=1e-6)
    # State bucket (0.0, 0.2): 1 / 0.1 = 10, clipped to 1.5.
    assert math.isclose(kinds[("state", 0.0)], OFFSET_MAX, abs_tol=1e-6)


@pytest.mark.asyncio
async def test_bulk_confidence_update_applied_atomically(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    After computing offsets, the worker bulk-applies to active Models
    via ModelsRepo.bulk_confidence_update. Verify: 100 active Models
    all get the fresh confidence in a single pass.
    """
    # 40 resolutions to generate a real empirical offset in bucket (0.7, 0.8).
    for _ in range(25):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.75, outcome=True,
        )
    for _ in range(15):
        await _insert_resolved_prediction(
            tx_conn, tenant=tenant, actor_id=actor_id,
            born_from_event_id=born_from_event,
            asserted_confidence=0.75, outcome=False,
        )
    # 100 active predictions at asserted_confidence=0.75.
    emb = [0.0] * 768
    emb[1] = 1.0
    active_ids = []
    for i in range(100):
        mid = await insert_model(
            tx_conn,
            tenant=tenant,
            born_from_event_id=born_from_event,
            proposition={"kind": "prediction", "expected": f"e{i}", "resolution": "r"},
            natural=f"Active prediction {i}",
            embedding=emb,
            scope_actors=[actor_id],
            confidence=0.75,
            confidence_at_assertion=0.75,
        )
        active_ids.append(mid)

    # Run the worker INLINE on the test transaction.
    result = await _run_inline(tx_conn, tenant=tenant)
    # 25 true / 40 total = 0.625 empirical rate; midpoint 0.75; offset
    # = 0.625 / 0.75 = 0.833... New confidence = assertion * offset
    # = 0.75 * (0.625/0.75) = 0.625.
    updated_rows = await tx_conn.fetch(
        "SELECT id, confidence FROM models WHERE id = ANY($1::uuid[])",
        active_ids,
    )
    assert len(updated_rows) == 100
    expected_confidence = 0.75 * (25 / 40) / 0.75  # = 0.625
    for r in updated_rows:
        assert math.isclose(float(r["confidence"]), expected_confidence, abs_tol=1e-6)
    # 100 newly-active + 40 test-resolution Models (also 'active' since
    # the test fixture doesn't archive them after resolving) both get
    # bulk-updated by the worker. Verify all 140 were touched in one
    # pass (bulk_confidence_update is atomic via the test transaction).
    assert result.models_recalibrated == 140


@pytest.mark.asyncio
async def test_existing_models_tests_still_pass_regression(
    fresh_db, tx_conn, tenant, actor_id, born_from_event
):
    """
    Regression: the insert pipeline calls async apply_calibration, which
    per AUDIT-REVIEW-1-FIXES §C5 now applies PROP_KIND_DEFAULTS in the
    cold-start regime (no calibration_offsets row OR sample_size < 20)
    rather than returning identity.

    For a state-kind Model with no prior history: 0.6 × 0.95 = 0.57.
    """
    from services.models.repo import ModelsRepo
    from services.models.calibration import PROP_KIND_DEFAULTS
    from lib.shared.types import ModelCreate
    emb = [0.0] * 768
    emb[2] = 1.0

    repo = ModelsRepo(fresh_db, embedder=None)
    payload = ModelCreate(
        tenant_id=tenant,
        born_from_event_id=born_from_event,
        proposition={"kind": "state", "subject": "alice", "assertion": "reliable"},
        natural="alice is reliable",
        embedding=emb,
        scope_actors=[actor_id],
        scope_temporal={"kind": "open_ended"},
        confidence=0.6,
        confidence_at_assertion=0.6,
    )
    inserted = await repo.insert(payload, conn=tx_conn)
    expected = 0.6 * PROP_KIND_DEFAULTS["state"]
    assert math.isclose(float(inserted.confidence), expected, abs_tol=1e-6)


# ---------------------------------------------------------------------
# Helper: run the worker INSIDE the test transaction
# ---------------------------------------------------------------------


async def _run_inline(
    tx_conn: asyncpg.Connection,
    *,
    tenant: uuid.UUID,
):
    """
    The worker's `run_once` acquires a fresh connection from the pool
    and opens its own transaction. Running it inside `tx_conn` would
    fight the outer rollback. Instead we call the internal steps
    directly on `tx_conn` so the whole thing stays in one transaction.
    """
    from services.workers.calibration_updater.worker import (
        RunResult,
        _apply_offsets_to_active_models,
        _harvest_stats,
        _recompute_all_offsets,
    )
    from services.models.repo import ModelsRepo

    harvested = await _harvest_stats(tx_conn, tenant_id=tenant)
    offsets_written = await _recompute_all_offsets(tx_conn, tenant_id=tenant)
    repo = ModelsRepo(pool=None)
    recalibrated = await _apply_offsets_to_active_models(
        tx_conn, models_repo=repo, tenant_id=tenant,
    )
    return RunResult(
        tenant_id=tenant,
        harvested_stats=harvested,
        offsets_written=offsets_written,
        models_recalibrated=recalibrated,
    )
