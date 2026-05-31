"""tests/unit/sage/test_model_predictions_repo.py — Phase 12 prediction repos.

Two repos backed by migration 0054:
  * ModelPredictionsRepo       — `model_predictions`
  * ModelPredictionErrorsRepo  — `model_prediction_errors`

Plus the pure residual helpers in services/sage/model_predictions/residual.py
(no DB; covered by the bottom half of this file).

The repo tests are marked `pytest.mark.integration` and use the
gateway_pool fixture re-exported via services/gateway/tests/conftest.py.
The residual-helper tests are pure and would run without a DB, but we
group them in the same file so the Phase 12 surface is covered in one
place. The module-level `integration` mark applies to both; the pure
tests will simply skip the DB acquire and run anyway.
"""
from __future__ import annotations

import hashlib
import json
import math
import random
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

import asyncpg
import pytest

from lib.shared.ids import uuid7
from services.sage.model_predictions.repo import (
    ModelPredictionErrorsRepo,
    ModelPredictionsRepo,
)
from services.sage.model_predictions.residual import (
    detect_prediction_error,
    score_residual_impact,
    score_residual_severity,
)
from services.sage.model_predictions.types import (
    ExpectedObservation,
    ModelPrediction,
    ModelPredictionError,
)


# Re-use gateway integration fixtures (per-test pool + fresh DB).
from services.gateway.tests.conftest import (  # noqa: F401
    gateway_pool,
    tenant_id,
)


pytestmark = pytest.mark.integration


# =====================================================================
# Helpers — seed a Model row so FK from model_predictions resolves.
# =====================================================================


def _content_embedding(text: str, dim: int = 768) -> list[float]:
    """Deterministic 768-d unit vector for the models.embedding column."""
    seed = int.from_bytes(hashlib.sha256(text.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    v = [rng.gauss(0.0, 1.0) for _ in range(dim)]
    norm = math.sqrt(sum(x * x for x in v))
    if norm == 0.0:
        return v
    return [x / norm for x in v]


async def _seed_observation(pool: asyncpg.Pool, tenant_id: UUID) -> UUID:
    """Thin wrapper around the shared observation seeder."""
    from tests.unit.sage._seed import seed_observation as _shared_seed_observation
    return await _shared_seed_observation(
        pool, tenant_id=tenant_id, content_text="seed obs",
    )


async def _seed_model(
    pool: asyncpg.Pool,
    tenant_id: UUID,
    *,
    natural: str = "Model under prediction test",
    confidence: float = 0.6,
) -> UUID:
    """Insert a minimal `models` row via the shared seed helper.

    The shared helper handles pgvector binding + confidence_at_assertion
    so this file only owns the per-test parameter shape.
    """
    from tests.unit.sage._seed import seed_model as _shared_seed_model
    obs_id = await _seed_observation(pool, tenant_id)
    return await _shared_seed_model(
        pool,
        tenant_id=tenant_id,
        born_from_event_id=obs_id,
        proposition={"kind": "belief", "subject": "test", "assertion": natural},
        natural=natural,
        confidence=confidence,
        supporting_event_ids=[obs_id],
        embedding=_content_embedding(natural),
    )


def _expected(
    *,
    kind: str = "metric_delta",
    value_constraint: dict | None = None,
    scope_entities: list | None = None,
    scope_actors: list | None = None,
) -> ExpectedObservation:
    return ExpectedObservation(
        kind=kind,
        value_constraint=value_constraint,
        scope_entities=scope_entities or [],
        scope_actors=scope_actors or [],
    )


def _prediction(
    *,
    tenant_id: UUID,
    model_id: UUID,
    prediction: str = "ARR will rise next quarter",
    expected: ExpectedObservation | None = None,
    check_after: datetime | None = None,
    confidence: float | None = 0.7,
    status: str = "active",
) -> ModelPrediction:
    return ModelPrediction(
        tenant_id=tenant_id,
        model_id=model_id,
        prediction=prediction,
        expected_observation=expected or _expected(
            value_constraint={"op": "gt", "value": 0, "field": "delta"},
        ),
        check_after=check_after,
        status=status,  # type: ignore[arg-type]
        confidence=confidence,
    )


# =====================================================================
# ModelPredictionsRepo — DB tests
# =====================================================================


@pytest.mark.asyncio
async def test_predictions_insert_and_get(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """insert returns a hydrated ModelPrediction; get echoes it."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)

    pred = _prediction(tenant_id=tenant_id, model_id=model_id)
    inserted = await repo.insert(pred)

    assert inserted.id is not None
    assert inserted.tenant_id == tenant_id
    assert inserted.model_id == model_id
    assert inserted.prediction == pred.prediction
    assert inserted.expected_observation.kind == "metric_delta"
    assert inserted.expected_observation.value_constraint == {
        "op": "gt", "value": 0, "field": "delta",
    }
    assert inserted.status == "active"
    assert inserted.confidence == pytest.approx(0.7)
    assert inserted.created_at is not None

    fetched = await repo.get(inserted.id)  # type: ignore[arg-type]
    assert fetched is not None
    assert fetched.id == inserted.id
    assert fetched.prediction == inserted.prediction


@pytest.mark.asyncio
async def test_predictions_list_due_filters_active_and_check_after(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """list_due returns only active predictions whose check_after is
    on-or-before the cutoff, ordered oldest-first."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)

    now = datetime.now(timezone.utc)
    past_far = now - timedelta(hours=2)
    past_near = now - timedelta(minutes=10)
    future = now + timedelta(hours=1)

    early = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_id,
        prediction="early due", check_after=past_far,
    ))
    late = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_id,
        prediction="late due", check_after=past_near,
    ))
    not_yet = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_id,
        prediction="not due", check_after=future,
    ))
    no_time = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_id,
        prediction="time-less", check_after=None,
    ))

    # Mark one as already resolved — must not show up.
    resolved = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_id,
        prediction="resolved-already", check_after=past_far,
    ))
    await repo.mark_confirmed(resolved.id, observation_id=None)  # type: ignore[arg-type]

    due = await repo.list_due(now, limit=100)
    due_ids = [p.id for p in due]
    assert early.id in due_ids
    assert late.id in due_ids
    assert not_yet.id not in due_ids
    assert no_time.id not in due_ids
    assert resolved.id not in due_ids
    # oldest first.
    assert due_ids.index(early.id) < due_ids.index(late.id)


@pytest.mark.asyncio
async def test_predictions_mark_confirmed_transitions_status(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """mark_confirmed flips status, sets resolved_at +
    resolved_by_observation_id, and is a no-op once the row leaves
    'active'."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)

    inserted = await repo.insert(
        _prediction(tenant_id=tenant_id, model_id=model_id),
    )
    obs_id = uuid7()
    confirmed = await repo.mark_confirmed(inserted.id, obs_id)  # type: ignore[arg-type]
    assert confirmed is not None
    assert confirmed.status == "confirmed"
    assert confirmed.resolved_at is not None
    assert confirmed.resolved_by_observation_id == obs_id

    # Second call is a no-op (the WHERE clause guards on status='active').
    second = await repo.mark_confirmed(inserted.id, uuid7())  # type: ignore[arg-type]
    assert second is None


@pytest.mark.asyncio
async def test_predictions_mark_falsified_transitions_status(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """mark_falsified mirrors mark_confirmed but writes the
    'falsified' status."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)

    inserted = await repo.insert(
        _prediction(tenant_id=tenant_id, model_id=model_id),
    )
    obs_id = uuid7()
    falsified = await repo.mark_falsified(inserted.id, obs_id)  # type: ignore[arg-type]
    assert falsified is not None
    assert falsified.status == "falsified"
    assert falsified.resolved_at is not None
    assert falsified.resolved_by_observation_id == obs_id


@pytest.mark.asyncio
async def test_predictions_mark_expired_transitions_status(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """mark_expired sets status to 'expired' and leaves
    resolved_by_observation_id as NULL."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)

    inserted = await repo.insert(
        _prediction(tenant_id=tenant_id, model_id=model_id),
    )
    expired = await repo.mark_expired(inserted.id)  # type: ignore[arg-type]
    assert expired is not None
    assert expired.status == "expired"
    assert expired.resolved_at is not None
    assert expired.resolved_by_observation_id is None


@pytest.mark.asyncio
async def test_predictions_list_active_for_model(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """list_active_for_model returns only the active predictions for
    the named model, ignoring resolved ones and predictions for other
    models."""
    model_a = await _seed_model(gateway_pool, tenant_id, natural="model A")
    model_b = await _seed_model(gateway_pool, tenant_id, natural="model B")
    repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)

    keep1 = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_a, prediction="A keep1",
    ))
    keep2 = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_a, prediction="A keep2",
    ))
    drop_resolved = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_a, prediction="A resolved",
    ))
    await repo.mark_confirmed(drop_resolved.id, observation_id=None)  # type: ignore[arg-type]
    other_model = await repo.insert(_prediction(
        tenant_id=tenant_id, model_id=model_b, prediction="B keep",
    ))

    listed = await repo.list_active_for_model(model_a)
    listed_ids = {p.id for p in listed}
    assert keep1.id in listed_ids
    assert keep2.id in listed_ids
    assert drop_resolved.id not in listed_ids
    assert other_model.id not in listed_ids


# =====================================================================
# ModelPredictionErrorsRepo — DB tests
# =====================================================================


def _error(
    *,
    tenant_id: UUID,
    model_id: UUID,
    prediction_id: UUID | None = None,
    severity: float = 0.5,
    impact_score: float = 0.5,
    status: str = "open",
) -> ModelPredictionError:
    return ModelPredictionError(
        tenant_id=tenant_id,
        model_id=model_id,
        prediction_id=prediction_id,
        observed_signal_id=None,
        error_summary="Residual: expected X, observed Y.",
        severity=severity,
        impact_score=impact_score,
        status=status,  # type: ignore[arg-type]
        triage_notes=None,
    )


@pytest.mark.asyncio
async def test_errors_insert_and_get(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """insert returns a hydrated ModelPredictionError; get echoes it."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    pred_repo = ModelPredictionsRepo(gateway_pool, tenant_id=tenant_id)
    err_repo = ModelPredictionErrorsRepo(gateway_pool, tenant_id=tenant_id)

    pred = await pred_repo.insert(
        _prediction(tenant_id=tenant_id, model_id=model_id),
    )

    inserted = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id, prediction_id=pred.id,
        severity=0.42, impact_score=0.7,
    ))
    assert inserted.id is not None
    assert inserted.tenant_id == tenant_id
    assert inserted.model_id == model_id
    assert inserted.prediction_id == pred.id
    assert inserted.severity == pytest.approx(0.42)
    assert inserted.impact_score == pytest.approx(0.7)
    assert inserted.status == "open"
    assert inserted.created_at is not None

    fetched = await err_repo.get(inserted.id)  # type: ignore[arg-type]
    assert fetched is not None
    assert fetched.id == inserted.id


@pytest.mark.asyncio
async def test_errors_list_open_min_impact_filter(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """list_open returns only status='open' rows with
    impact_score >= min_impact, ordered impact DESC."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    err_repo = ModelPredictionErrorsRepo(gateway_pool, tenant_id=tenant_id)

    low = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id, impact_score=0.1,
    ))
    mid = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id, impact_score=0.5,
    ))
    high = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id, impact_score=0.9,
    ))
    triaged = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id,
        impact_score=0.95, status="triaged",
    ))

    # No filter (min_impact=0.0): low, mid, high all show; triaged is not 'open'.
    listed = await err_repo.list_open()
    listed_ids = [r.id for r in listed]
    assert low.id in listed_ids
    assert mid.id in listed_ids
    assert high.id in listed_ids
    assert triaged.id not in listed_ids
    # Ordered by impact_score DESC.
    impact_order = [r.impact_score for r in listed]
    assert impact_order == sorted(impact_order, reverse=True)

    # With min_impact=0.4, only mid + high survive.
    filtered = await err_repo.list_open(min_impact=0.4)
    filtered_ids = {r.id for r in filtered}
    assert low.id not in filtered_ids
    assert mid.id in filtered_ids
    assert high.id in filtered_ids


@pytest.mark.asyncio
async def test_errors_update_status(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """update_status writes a new status; passing triage_notes=None
    preserves the existing notes via COALESCE."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    err_repo = ModelPredictionErrorsRepo(gateway_pool, tenant_id=tenant_id)

    inserted = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id,
    ))

    # First transition: write triage_notes.
    triaged = await err_repo.update_status(
        inserted.id,  # type: ignore[arg-type]
        status="triaged",
        triage_notes={"owner": "alice", "queue": "ops"},
    )
    assert triaged is not None
    assert triaged.status == "triaged"
    assert triaged.triage_notes == {"owner": "alice", "queue": "ops"}

    # Second transition: change status, leave notes untouched.
    resolved = await err_repo.update_status(
        inserted.id,  # type: ignore[arg-type]
        status="resolved",
    )
    assert resolved is not None
    assert resolved.status == "resolved"
    assert resolved.triage_notes == {"owner": "alice", "queue": "ops"}


@pytest.mark.asyncio
async def test_errors_top_by_impact_ordering(
    gateway_pool: asyncpg.Pool, tenant_id: UUID,
):
    """top_by_impact orders by impact_score DESC regardless of status."""
    model_id = await _seed_model(gateway_pool, tenant_id)
    err_repo = ModelPredictionErrorsRepo(gateway_pool, tenant_id=tenant_id)

    a = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id, impact_score=0.3,
    ))
    b = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id,
        impact_score=0.6, status="ignored",
    ))
    c = await err_repo.insert(_error(
        tenant_id=tenant_id, model_id=model_id, impact_score=0.95,
    ))

    top = await err_repo.top_by_impact(limit=10)
    impact_order = [r.impact_score for r in top]
    assert impact_order == sorted(impact_order, reverse=True)
    ids_in_order = [r.id for r in top]
    assert ids_in_order.index(c.id) < ids_in_order.index(b.id)
    assert ids_in_order.index(b.id) < ids_in_order.index(a.id)

    # limit honored.
    top1 = await err_repo.top_by_impact(limit=1)
    assert len(top1) == 1
    assert top1[0].id == c.id


# =====================================================================
# residual.py — pure unit tests (no DB)
# =====================================================================


def _pure_prediction(
    *,
    expected: ExpectedObservation,
    confidence: float | None = 0.7,
    status: str = "active",
) -> ModelPrediction:
    return ModelPrediction(
        id=uuid7(),
        tenant_id=uuid4(),
        model_id=uuid4(),
        prediction="numeric metric must be positive",
        expected_observation=expected,
        status=status,  # type: ignore[arg-type]
        confidence=confidence,
    )


def test_detect_prediction_error_returns_none_when_satisfied():
    """When the observation's value satisfies the value_constraint,
    detect_prediction_error returns None."""
    expected = ExpectedObservation(
        kind="metric_delta",
        value_constraint={"op": "gt", "value": 0, "field": "delta"},
    )
    prediction = _pure_prediction(expected=expected)

    # delta=5 satisfies "gt 0".
    observation = {"id": uuid7(), "kind": "metric", "delta": 5}
    assert detect_prediction_error(prediction, observation) is None


def test_detect_prediction_error_returns_populated_error_on_mismatch():
    """When the observation violates the value_constraint, the helper
    returns a populated (un-inserted) ModelPredictionError carrying
    severity, impact_score, and a status of 'open'."""
    expected = ExpectedObservation(
        kind="metric_delta",
        value_constraint={"op": "gt", "value": 0, "field": "delta"},
    )
    prediction = _pure_prediction(expected=expected, confidence=0.9)

    # delta=-3 violates "gt 0".
    obs_id = uuid7()
    observation = {"id": obs_id, "kind": "metric", "delta": -3}
    err = detect_prediction_error(prediction, observation)

    assert err is not None
    assert isinstance(err, ModelPredictionError)
    assert err.tenant_id == prediction.tenant_id
    assert err.model_id == prediction.model_id
    assert err.prediction_id == prediction.id
    assert err.observed_signal_id == obs_id
    assert err.status == "open"
    assert 0.0 <= err.severity <= 1.0
    assert 0.0 <= err.impact_score <= 1.0
    assert "Residual" in err.error_summary


def test_detect_prediction_error_skips_non_active_predictions():
    """An already-resolved prediction is never re-flagged."""
    expected = ExpectedObservation(
        kind="metric_delta",
        value_constraint={"op": "gt", "value": 0, "field": "delta"},
    )
    prediction = _pure_prediction(expected=expected, status="confirmed")
    observation = {"id": uuid7(), "delta": -3}
    assert detect_prediction_error(prediction, observation) is None


def test_detect_prediction_error_returns_none_for_no_observation():
    expected = ExpectedObservation(
        kind="metric_delta",
        value_constraint={"op": "gt", "value": 0, "field": "delta"},
    )
    prediction = _pure_prediction(expected=expected)
    assert detect_prediction_error(prediction, None) is None


def test_score_residual_severity_is_bounded():
    """Severity is clamped into [0, 1] for both satisfied + violating
    observations, and high-confidence violations score strictly
    higher than low-confidence ones for the same deviation."""
    expected = ExpectedObservation(
        kind="metric_delta",
        value_constraint={"op": "gt", "value": 0, "field": "delta"},
    )

    satisfied_obs = {"id": uuid7(), "delta": 10}
    violating_obs = {"id": uuid7(), "delta": -10}

    low_conf = _pure_prediction(expected=expected, confidence=0.1)
    high_conf = _pure_prediction(expected=expected, confidence=0.9)

    # Satisfied: 0.0.
    sev_ok = score_residual_severity(low_conf, satisfied_obs)
    assert sev_ok == pytest.approx(0.0)
    assert 0.0 <= sev_ok <= 1.0

    # Violations: bounded.
    sev_low = score_residual_severity(low_conf, violating_obs)
    sev_high = score_residual_severity(high_conf, violating_obs)
    assert 0.0 <= sev_low <= 1.0
    assert 0.0 <= sev_high <= 1.0
    # Higher confidence → sharper signal.
    assert sev_high > sev_low

    # None-observation guard.
    assert score_residual_severity(low_conf, None) == 0.0


def test_score_residual_impact_is_bounded_and_weights_by_confidence():
    """Impact is clamped into [0, 1]. With no structural row, the
    fallback is confidence-driven; a high-confidence prediction
    produces a strictly higher fallback than a low-confidence one."""
    expected = ExpectedObservation(kind="state_change")
    low_conf = _pure_prediction(expected=expected, confidence=0.1)
    high_conf = _pure_prediction(expected=expected, confidence=0.9)

    # No model_row → confidence-derived placeholder, bounded.
    fallback_low = score_residual_impact(low_conf, None)
    fallback_high = score_residual_impact(high_conf, None)
    assert 0.0 <= fallback_low <= 1.0
    assert 0.0 <= fallback_high <= 1.0
    assert fallback_high > fallback_low

    # With a populated model_row, the weighted sum is also bounded
    # and reflects the structural inputs.
    model_row = {
        "hub_score": 0.8,
        "bridge_score": 0.6,
        "dependent_count": 12,  # saturates the /10 normaliser.
    }
    impact = score_residual_impact(high_conf, model_row)
    assert 0.0 <= impact <= 1.0
    # With the listed weights (0.4 hub + 0.3 bridge + 0.2 dep_norm +
    # 0.1 conf), this should land north of the no-model fallback.
    assert impact > fallback_high
