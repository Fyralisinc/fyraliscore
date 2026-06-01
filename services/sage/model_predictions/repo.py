"""services.sage.model_predictions.repo — asyncpg repositories.

Schema reference: db/migrations/0054_sage_model_predictions.sql.

Two repos in this module — they share a connection-management pattern
with services.models.repo (acquire-from-pool when no `conn` is passed,
use the caller's `conn` otherwise) but are intentionally separated by
table so a worker that only writes residuals never accidentally
touches the predictions table.

`tenant_id` is bound at construction time. Every SQL statement
filters by `tenant_id` as defense-in-depth alongside the RLS policy
installed by 0054 — so even if a caller forgets to wrap the call in
`tenant_transaction`, the query still scopes correctly.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.sage.model_predictions.types import (
    ExpectedObservation,
    ModelPrediction,
    ModelPredictionError,
    PredictionErrorStatus,
    PredictionStatus,
)


# ---------------------------------------------------------------------
# Column ordering — keep SELECT shape stable so row hydration never
# has to reorder.
# ---------------------------------------------------------------------

_PREDICTION_COLS = (
    "id",
    "tenant_id",
    "model_id",
    "prediction",
    "expected_observation",
    "check_after",
    "status",
    "confidence",
    "created_at",
    "updated_at",
    "resolved_at",
    "resolved_by_observation_id",
)
_PREDICTION_COLS_SQL = ", ".join(_PREDICTION_COLS)

_ERROR_COLS = (
    "id",
    "tenant_id",
    "model_id",
    "prediction_id",
    "observed_signal_id",
    "error_summary",
    "severity",
    "impact_score",
    "status",
    "triage_notes",
    "created_at",
)
_ERROR_COLS_SQL = ", ".join(_ERROR_COLS)


def _jsonb(value: Any) -> str:
    """asyncpg wants a JSON string when the param is cast `::jsonb`."""
    return json.dumps(value, sort_keys=True, default=str)


def _serialize_expected(expected: ExpectedObservation) -> str:
    """Flatten an ExpectedObservation into the JSONB payload shape.

    `extras` is hoisted to the top level so downstream JSONB readers
    don't have to drill through a nested key — this matches the spec
    text which calls `expected_observation` a flat structured shape.
    """
    base = expected.model_dump(mode="json", exclude_none=True)
    extras = base.pop("extras", {}) or {}
    # Caller-declared keys win over `extras` collisions; the declared
    # set is the authoritative surface and `extras` is for extension.
    merged: dict[str, Any] = {}
    if isinstance(extras, dict):
        merged.update(extras)
    merged.update(base)
    return _jsonb(merged)


def _hydrate_prediction(row: asyncpg.Record) -> ModelPrediction:
    raw_expected = row["expected_observation"]
    if isinstance(raw_expected, str):
        raw_expected = json.loads(raw_expected)
    raw_expected = dict(raw_expected or {})
    declared_keys = {
        "kind",
        "scope_entities",
        "scope_actors",
        "value_constraint",
        "falsification_rule",
    }
    declared = {k: raw_expected[k] for k in declared_keys if k in raw_expected}
    extras = {k: v for k, v in raw_expected.items() if k not in declared_keys}
    if "kind" not in declared:
        # Forward compat: rows written by an older path may have
        # skipped `kind`; fill a neutral default so hydration never
        # throws. The residual detector treats unknown kinds as
        # "match anything in scope".
        declared["kind"] = "unspecified"
    declared["extras"] = extras
    expected = ExpectedObservation.model_validate(declared)
    return ModelPrediction(
        id=row["id"],
        tenant_id=row["tenant_id"],
        model_id=row["model_id"],
        prediction=row["prediction"],
        expected_observation=expected,
        check_after=row["check_after"],
        status=row["status"],
        confidence=row["confidence"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        resolved_at=row["resolved_at"],
        resolved_by_observation_id=row["resolved_by_observation_id"],
    )


def _hydrate_error(row: asyncpg.Record) -> ModelPredictionError:
    triage = row["triage_notes"]
    if isinstance(triage, str):
        triage = json.loads(triage)
    return ModelPredictionError(
        id=row["id"],
        tenant_id=row["tenant_id"],
        model_id=row["model_id"],
        prediction_id=row["prediction_id"],
        observed_signal_id=row["observed_signal_id"],
        error_summary=row["error_summary"],
        severity=float(row["severity"]),
        impact_score=float(row["impact_score"]),
        status=row["status"],
        triage_notes=triage,
        created_at=row["created_at"],
    )


# =====================================================================
# ModelPredictionsRepo
# =====================================================================


class ModelPredictionsRepo:
    """Repository for the internal `model_predictions` surface.

    The pool is optional; methods that need a pool when `conn` is None
    raise a clear error.
    """

    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError(
                "ModelPredictionsRepo was constructed without a pool; "
                "callers in conn-only mode must pass conn= on every call"
            )
        return self._pool

    # -----------------------------------------------------------------
    # insert
    # -----------------------------------------------------------------
    async def insert(
        self,
        prediction: ModelPrediction,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPrediction:
        """INSERT a Model-substrate prediction.

        The caller may set `prediction.tenant_id` explicitly — but it
        must match the repo's bound tenant. If `id` is None we assign
        a fresh uuid7. Timestamps default to now() in the DB.
        """
        if prediction.tenant_id != self._tenant_id:
            raise ValueError(
                "ModelPrediction.tenant_id does not match repo's bound tenant"
            )

        pid = prediction.id or uuid7()

        async def _run(c: asyncpg.Connection) -> ModelPrediction:
            row = await c.fetchrow(
                f"""
                INSERT INTO model_predictions (
                    id, tenant_id, model_id,
                    prediction, expected_observation,
                    check_after, status, confidence,
                    resolved_at, resolved_by_observation_id
                ) VALUES (
                    $1, $2, $3,
                    $4, $5::jsonb,
                    $6, $7, $8,
                    $9, $10
                )
                RETURNING {_PREDICTION_COLS_SQL}
                """,
                pid,
                self._tenant_id,
                prediction.model_id,
                prediction.prediction,
                _serialize_expected(prediction.expected_observation),
                prediction.check_after,
                prediction.status,
                prediction.confidence,
                prediction.resolved_at,
                prediction.resolved_by_observation_id,
            )
            assert row is not None
            return _hydrate_prediction(row)

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # get
    # -----------------------------------------------------------------
    async def get(
        self,
        prediction_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPrediction | None:
        async def _run(c: asyncpg.Connection) -> ModelPrediction | None:
            row = await c.fetchrow(
                f"""
                SELECT {_PREDICTION_COLS_SQL}
                FROM model_predictions
                WHERE tenant_id = $1 AND id = $2
                """,
                self._tenant_id,
                prediction_id,
            )
            return _hydrate_prediction(row) if row is not None else None

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # list_due — sweeper feed for the due-prediction evaluator
    # -----------------------------------------------------------------
    async def list_due(
        self,
        before_ts: datetime,
        *,
        limit: int = 100,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelPrediction]:
        """Return active predictions whose `check_after <= before_ts`,
        oldest first."""

        async def _run(c: asyncpg.Connection) -> list[ModelPrediction]:
            rows = await c.fetch(
                f"""
                SELECT {_PREDICTION_COLS_SQL}
                FROM model_predictions
                WHERE tenant_id = $1
                  AND status = 'active'
                  AND check_after IS NOT NULL
                  AND check_after <= $2
                ORDER BY check_after ASC
                LIMIT $3
                """,
                self._tenant_id,
                before_ts,
                limit,
            )
            return [_hydrate_prediction(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # list_active_for_model — used by residual detector when a new
    # observation arrives in a Model's scope.
    # -----------------------------------------------------------------
    async def list_active_for_model(
        self,
        model_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelPrediction]:
        async def _run(c: asyncpg.Connection) -> list[ModelPrediction]:
            rows = await c.fetch(
                f"""
                SELECT {_PREDICTION_COLS_SQL}
                FROM model_predictions
                WHERE tenant_id = $1
                  AND model_id = $2
                  AND status = 'active'
                ORDER BY created_at DESC
                """,
                self._tenant_id,
                model_id,
            )
            return [_hydrate_prediction(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # State transitions: confirmed / falsified / expired
    # -----------------------------------------------------------------
    async def mark_confirmed(
        self,
        prediction_id: UUID,
        observation_id: UUID | None,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPrediction | None:
        return await self._mark_resolved(
            prediction_id, "confirmed", observation_id, conn=conn
        )

    async def mark_falsified(
        self,
        prediction_id: UUID,
        observation_id: UUID | None,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPrediction | None:
        return await self._mark_resolved(
            prediction_id, "falsified", observation_id, conn=conn
        )

    async def mark_expired(
        self,
        prediction_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPrediction | None:
        return await self._mark_resolved(
            prediction_id, "expired", observation_id=None, conn=conn
        )

    async def _mark_resolved(
        self,
        prediction_id: UUID,
        new_status: PredictionStatus,
        observation_id: UUID | None,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPrediction | None:
        async def _run(c: asyncpg.Connection) -> ModelPrediction | None:
            row = await c.fetchrow(
                f"""
                UPDATE model_predictions
                SET status = $3,
                    resolved_at = now(),
                    resolved_by_observation_id = $4,
                    updated_at = now()
                WHERE tenant_id = $1
                  AND id = $2
                  AND status = 'active'
                RETURNING {_PREDICTION_COLS_SQL}
                """,
                self._tenant_id,
                prediction_id,
                new_status,
                observation_id,
            )
            return _hydrate_prediction(row) if row is not None else None

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)


# =====================================================================
# ModelPredictionErrorsRepo
# =====================================================================


class ModelPredictionErrorsRepo:
    """Repository for the `model_prediction_errors` surface."""

    def __init__(
        self,
        pool: asyncpg.Pool | None = None,
        *,
        tenant_id: UUID,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id

    @property
    def tenant_id(self) -> UUID:
        return self._tenant_id

    def _require_pool(self) -> asyncpg.Pool:
        if self._pool is None:
            raise RuntimeError(
                "ModelPredictionErrorsRepo was constructed without a pool; "
                "callers in conn-only mode must pass conn= on every call"
            )
        return self._pool

    # -----------------------------------------------------------------
    # insert
    # -----------------------------------------------------------------
    async def insert(
        self,
        err: ModelPredictionError,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPredictionError:
        if err.tenant_id != self._tenant_id:
            raise ValueError(
                "ModelPredictionError.tenant_id does not match repo's bound tenant"
            )

        eid = err.id or uuid7()
        triage_json = (
            _jsonb(err.triage_notes) if err.triage_notes is not None else None
        )

        async def _run(c: asyncpg.Connection) -> ModelPredictionError:
            row = await c.fetchrow(
                f"""
                INSERT INTO model_prediction_errors (
                    id, tenant_id, model_id,
                    prediction_id, observed_signal_id,
                    error_summary, severity, impact_score,
                    status, triage_notes
                ) VALUES (
                    $1, $2, $3,
                    $4, $5,
                    $6, $7, $8,
                    $9, $10::jsonb
                )
                RETURNING {_ERROR_COLS_SQL}
                """,
                eid,
                self._tenant_id,
                err.model_id,
                err.prediction_id,
                err.observed_signal_id,
                err.error_summary,
                err.severity,
                err.impact_score,
                err.status,
                triage_json,
            )
            assert row is not None
            return _hydrate_error(row)

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # get
    # -----------------------------------------------------------------
    async def get(
        self,
        error_id: UUID,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPredictionError | None:
        async def _run(c: asyncpg.Connection) -> ModelPredictionError | None:
            row = await c.fetchrow(
                f"""
                SELECT {_ERROR_COLS_SQL}
                FROM model_prediction_errors
                WHERE tenant_id = $1 AND id = $2
                """,
                self._tenant_id,
                error_id,
            )
            return _hydrate_error(row) if row is not None else None

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # list_open — triage worker feed
    # -----------------------------------------------------------------
    async def list_open(
        self,
        *,
        limit: int = 50,
        min_impact: float = 0.0,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelPredictionError]:
        async def _run(c: asyncpg.Connection) -> list[ModelPredictionError]:
            rows = await c.fetch(
                f"""
                SELECT {_ERROR_COLS_SQL}
                FROM model_prediction_errors
                WHERE tenant_id = $1
                  AND status = 'open'
                  AND impact_score >= $2
                ORDER BY impact_score DESC, created_at DESC
                LIMIT $3
                """,
                self._tenant_id,
                min_impact,
                limit,
            )
            return [_hydrate_error(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # update_status — triage worker writes status transitions here.
    # -----------------------------------------------------------------
    async def update_status(
        self,
        error_id: UUID,
        status: PredictionErrorStatus,
        triage_notes: Optional[dict[str, Any]] = None,
        *,
        conn: asyncpg.Connection | None = None,
    ) -> ModelPredictionError | None:
        triage_json = _jsonb(triage_notes) if triage_notes is not None else None

        async def _run(c: asyncpg.Connection) -> ModelPredictionError | None:
            # COALESCE so callers can transition status without
            # overwriting existing triage notes.
            row = await c.fetchrow(
                f"""
                UPDATE model_prediction_errors
                SET status = $3,
                    triage_notes = COALESCE($4::jsonb, triage_notes)
                WHERE tenant_id = $1
                  AND id = $2
                RETURNING {_ERROR_COLS_SQL}
                """,
                self._tenant_id,
                error_id,
                status,
                triage_json,
            )
            return _hydrate_error(row) if row is not None else None

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)

    # -----------------------------------------------------------------
    # top_by_impact — leaderboard surface; ignores status to support
    # debug / dashboards.
    # -----------------------------------------------------------------
    async def top_by_impact(
        self,
        *,
        limit: int = 20,
        conn: asyncpg.Connection | None = None,
    ) -> list[ModelPredictionError]:
        async def _run(c: asyncpg.Connection) -> list[ModelPredictionError]:
            rows = await c.fetch(
                f"""
                SELECT {_ERROR_COLS_SQL}
                FROM model_prediction_errors
                WHERE tenant_id = $1
                ORDER BY impact_score DESC, created_at DESC
                LIMIT $2
                """,
                self._tenant_id,
                limit,
            )
            return [_hydrate_error(r) for r in rows]

        if conn is not None:
            return await _run(conn)
        async with self._require_pool().acquire() as owned:
            return await _run(owned)


__all__ = [
    "ModelPredictionErrorsRepo",
    "ModelPredictionsRepo",
]
