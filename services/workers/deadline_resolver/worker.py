"""services/workers/deadline_resolver/worker.py

Wave 4-A. Deadline resolver: poll for overdue prediction Models, run
the kind-specific falsifier evaluator, and enqueue a T2
`prediction_overdue` trigger on `think_trigger_queue` for Think.

The resolver DOES NOT write to `models`. Think's deterministic T2
handler (`services/think/deterministic.py::_handle_t2_prediction`)
owns:
  * confidence deltas on the prediction + its `contributing_models`
  * `resolved_at` / `resolution_outcome` update
  * `confirmed_count` / `contested_count` increments
  * optional archival with `archive_reason='resolved_confirmed'`,
    `'resolved_violated'`, or `'inconclusive'`

Design notes
------------

* Tenant iteration.  `ModelsRepo.get_predictions_due(tenant_id=...)`
  requires a tenant (checked 2026-04-21 in services/models/repo.py
  lines 730-760). BUILD-PLAN §5 Prompt 4.A says "across all tenants,
  or loop per-tenant if the repo requires a tenant_id (check the API;
  document whichever)". We loop per-tenant: one lightweight
  `SELECT DISTINCT tenant_id FROM models WHERE status='active' AND
  evaluate_at <= $now` per cycle, then call `get_predictions_due` for
  each discovered tenant. Documented in BUILD-LOG deviation (a).

* Idempotency.  Two checks before enqueueing:
    (1) `think_trigger_queue` — open (completed_at IS NULL) T2 row
        with matching model_id.
    (2) `applied_triggers` — we look for a recently-applied trigger
        whose payload JSONB `model_id` key equals our prediction id.
  Both checks run with one query each. Rationale: (1) alone would
  let us re-enqueue after Think has consumed the trigger but before
  `evaluate_at` gets updated, generating LLM cost. (2) catches the
  post-consume window. We cap the applied_triggers lookback at 1 hour
  — long enough that re-polling within a single resolver cycle can't
  double-enqueue, short enough that after Think resolves the
  prediction (setting `resolved_at`), a stale re-enqueue isn't
  blocked forever; but since Think sets `resolution_outcome` which
  doesn't affect `evaluate_at`, the ModelsRepo filter (status='active'
  AND evaluate_at IS NOT NULL AND evaluate_at <= now) remains the
  primary gate anyway. Documented in BUILD-LOG deviation (c).

* Dead-letter on context failure.  If evaluator fails (referenced
  commitment doesn't exist → evaluator returns 'inconclusive'; the
  resolver enqueues the trigger anyway so Think can handle it). A
  genuine error (DB connection issue, JSON corruption) is caught and
  logged with an `errors` counter bumped on the run result. The
  resolver does not have its own dead-letter table — model_reeval
  and applied_triggers are sufficient.

* Structured logging.  Every decision logs via `structlog` with
  `prediction_id`, `falsifier_kind`, `provisional_outcome`,
  `tenant_id`. At cycle boundaries we log counts (enqueued, skipped,
  errored).
"""
from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Literal, Sequence
from uuid import UUID

import asyncpg
import structlog

from lib.shared.db import transaction
from lib.shared.ids import uuid7
from services.models.repo import ModelsRepo
from services.workers.deadline_resolver.evaluators import (
    EvaluationContext,
    evaluate_falsifier,
)


log = structlog.get_logger(__name__)


ProvisionalOutcome = Literal["confirmed", "violated", "inconclusive"]


# Public env knob per BUILD-PLAN §5 Prompt 4.A:
#   "loop every 60s (env DEADLINE_POLL_INTERVAL_S, default 60)".
DEFAULT_POLL_INTERVAL_S = 60


# Idempotency lookback for applied_triggers. See module docstring.
_APPLIED_LOOKBACK = timedelta(hours=1)


# Max predictions processed per cycle per tenant. Keeps one slow
# tenant from starving the others.
_MAX_PER_TENANT_PER_CYCLE = 500


# ---------------------------------------------------------------------
# Result payload
# ---------------------------------------------------------------------


@dataclass
class CycleResult:
    """Outcome of one poll cycle.

    * enqueued          — T2 rows created in this cycle
    * skipped_idempotent — already-queued / already-applied predictions
    * errored           — predictions that raised during evaluation
    * by_outcome        — histogram of provisional outcomes enqueued
    """

    enqueued: int = 0
    skipped_idempotent: int = 0
    errored: int = 0
    by_outcome: dict[str, int] = field(default_factory=dict)
    tenants_scanned: int = 0


# ---------------------------------------------------------------------
# DeadlineResolver
# ---------------------------------------------------------------------


class DeadlineResolver:
    """Polls for overdue predictions and enqueues T2 triggers.

    Usage (worker-mode):

        resolver = DeadlineResolver(pool)
        stop = asyncio.Event()
        await resolver.run(stop)

    Usage (test / one-shot):

        result = await resolver.run_once()
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        models_repo: ModelsRepo | None = None,
        poll_interval_s: float | None = None,
        max_per_tenant_per_cycle: int | None = None,
        logger: Any | None = None,
        now_fn=None,
    ) -> None:
        self._pool = pool
        self._models_repo = models_repo or ModelsRepo(pool)
        interval = poll_interval_s
        if interval is None:
            raw = os.environ.get("DEADLINE_POLL_INTERVAL_S")
            try:
                interval = float(raw) if raw else DEFAULT_POLL_INTERVAL_S
            except (TypeError, ValueError):
                interval = DEFAULT_POLL_INTERVAL_S
        self._interval = max(1.0, float(interval))
        self._max_per_tenant = int(
            max_per_tenant_per_cycle
            if max_per_tenant_per_cycle is not None
            else _MAX_PER_TENANT_PER_CYCLE
        )
        self._log = logger or log
        # now_fn indirection for tests — default returns timezone-aware utc.
        self._now = now_fn or (lambda: datetime.now(timezone.utc))

    # -----------------------------------------------------------------
    # Loop
    # -----------------------------------------------------------------

    async def run(self, stop_event: asyncio.Event) -> None:
        """Run the resolver loop until `stop_event` is set.

        The loop sleeps for up to `poll_interval_s` between cycles but
        wakes immediately on stop.
        """
        self._log.info(
            "deadline_resolver.starting",
            poll_interval_s=self._interval,
        )
        while not stop_event.is_set():
            try:
                result = await self.run_once()
                self._log.info(
                    "deadline_resolver.cycle",
                    enqueued=result.enqueued,
                    skipped_idempotent=result.skipped_idempotent,
                    errored=result.errored,
                    tenants_scanned=result.tenants_scanned,
                    by_outcome=result.by_outcome,
                )
            except Exception as exc:   # noqa: BLE001 — loop must stay alive
                self._log.exception(
                    "deadline_resolver.cycle_failed",
                    error=str(exc),
                )
            # Interruptible sleep.
            try:
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._interval
                )
            except asyncio.TimeoutError:
                pass
        self._log.info("deadline_resolver.stopped")

    # -----------------------------------------------------------------
    # One cycle
    # -----------------------------------------------------------------

    async def run_once(self) -> CycleResult:
        """One full pass over all tenants. Returns a CycleResult."""
        result = CycleResult()
        now = self._now()
        tenants = await self._list_active_prediction_tenants(now)
        result.tenants_scanned = len(tenants)
        for tenant_id in tenants:
            predictions = await self._models_repo.get_predictions_due(
                now,
                tenant_id=tenant_id,
                limit=self._max_per_tenant,
            )
            for pred in predictions:
                try:
                    outcome = await self._process_prediction(pred)
                except Exception as exc:   # noqa: BLE001
                    self._log.exception(
                        "deadline_resolver.prediction_failed",
                        prediction_id=str(pred.id),
                        tenant_id=str(pred.tenant_id),
                        error=str(exc),
                    )
                    result.errored += 1
                    continue
                if outcome == "skipped":
                    result.skipped_idempotent += 1
                elif outcome is not None:
                    result.enqueued += 1
                    result.by_outcome[outcome] = (
                        result.by_outcome.get(outcome, 0) + 1
                    )
        return result

    # -----------------------------------------------------------------
    # Per-prediction handler
    # -----------------------------------------------------------------

    async def _process_prediction(self, prediction) -> str | None:
        """Evaluate one prediction. Returns the provisional outcome on
        enqueue, the string 'skipped' when idempotency blocked the
        enqueue, or None on pre-check failure.
        """
        falsifier = prediction.falsifier
        falsifier_kind = (
            falsifier.get("kind") if isinstance(falsifier, dict) else None
        )

        async with self._pool.acquire() as conn:
            # Idempotency checks first — cheap.
            if await self._trigger_already_pending(conn, prediction.id):
                self._log.info(
                    "deadline_resolver.skipped_pending",
                    prediction_id=str(prediction.id),
                    tenant_id=str(prediction.tenant_id),
                    falsifier_kind=falsifier_kind,
                )
                return "skipped"
            if await self._trigger_recently_applied(
                conn, prediction.id, prediction.tenant_id
            ):
                self._log.info(
                    "deadline_resolver.skipped_applied",
                    prediction_id=str(prediction.id),
                    tenant_id=str(prediction.tenant_id),
                    falsifier_kind=falsifier_kind,
                )
                return "skipped"

            # Evaluate.
            ctx = EvaluationContext(
                conn=conn,
                tenant_id=prediction.tenant_id,
                prediction_id=prediction.id,
                prediction_created_at=prediction.created_at,
                now=self._now(),
            )
            provisional = await evaluate_falsifier(
                falsifier if isinstance(falsifier, dict) else None,
                ctx,
            )

            # Enqueue.
            await self._enqueue_t2(
                conn=conn,
                prediction_id=prediction.id,
                tenant_id=prediction.tenant_id,
                provisional_outcome=provisional,
                falsifier_kind=falsifier_kind,
                contributing_models=list(prediction.contributing_models or []),
            )

        self._log.info(
            "deadline_resolver.enqueued",
            prediction_id=str(prediction.id),
            tenant_id=str(prediction.tenant_id),
            falsifier_kind=falsifier_kind,
            provisional_outcome=provisional,
        )
        return provisional

    # -----------------------------------------------------------------
    # Tenant discovery
    # -----------------------------------------------------------------

    async def _list_active_prediction_tenants(
        self, now: datetime
    ) -> list[UUID]:
        """Return every distinct tenant_id that has at least one due
        prediction right now. One query, indexed on
        (status, evaluate_at) via the partial index
        `models_evaluate_idx`."""
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT tenant_id
                FROM models
                WHERE status = 'active'
                  AND evaluate_at IS NOT NULL
                  AND evaluate_at <= $1
                """,
                now,
            )
            return [r["tenant_id"] for r in rows]

    # -----------------------------------------------------------------
    # Idempotency guards
    # -----------------------------------------------------------------

    async def _trigger_already_pending(
        self,
        conn: asyncpg.Connection,
        prediction_id: UUID,
    ) -> bool:
        """Is there an in-flight (or queued) T2 trigger for this
        model?

        "In-flight" means `completed_at IS NULL`. We don't filter on
        kind/subkind because model_id is already unique per
        prediction — any un-completed trigger keyed to this model is
        enough to skip.
        """
        val = await conn.fetchval(
            """
            SELECT 1
            FROM think_trigger_queue
            WHERE model_id = $1
              AND completed_at IS NULL
              AND trigger_kind = 'T2'
              AND trigger_subkind = 'prediction_overdue'
            LIMIT 1
            """,
            prediction_id,
        )
        return val is not None

    async def _trigger_recently_applied(
        self,
        conn: asyncpg.Connection,
        prediction_id: UUID,
        tenant_id: UUID,
    ) -> bool:
        """Was a T2 trigger for this model_id applied in the last hour?

        Uses the payload JSONB's `prediction_id` key which the resolver
        writes at enqueue time. `applied_triggers` is keyed by
        trigger_id, not by model_id, so we look up via the
        `think_trigger_queue.id` → `applied_triggers.trigger_id`
        join on the payload-bound predecessor.
        """
        cutoff = self._now() - _APPLIED_LOOKBACK
        val = await conn.fetchval(
            """
            SELECT 1
            FROM applied_triggers at
            JOIN think_trigger_queue ttq ON ttq.id = at.trigger_id
            WHERE at.tenant_id = $1
              AND at.applied_at >= $2
              AND ttq.model_id = $3
              AND ttq.trigger_kind = 'T2'
              AND ttq.trigger_subkind = 'prediction_overdue'
            LIMIT 1
            """,
            tenant_id,
            cutoff,
            prediction_id,
        )
        return val is not None

    # -----------------------------------------------------------------
    # Enqueue
    # -----------------------------------------------------------------

    async def _enqueue_t2(
        self,
        *,
        conn: asyncpg.Connection,
        prediction_id: UUID,
        tenant_id: UUID,
        provisional_outcome: ProvisionalOutcome,
        falsifier_kind: str | None,
        contributing_models: Sequence[UUID],
    ) -> UUID:
        """Insert one T2 prediction_overdue row into
        `think_trigger_queue`, transactionally.

        Uses lib.shared.db.transaction() if we don't already own a
        transaction. When `conn` is provided and already in a
        transaction, we inherit it.
        """
        trigger_id = uuid7()
        payload = {
            "prediction_id": str(prediction_id),
            "provisional_outcome": provisional_outcome,
            "falsifier_kind": falsifier_kind,
            "contributing_models": [str(m) for m in contributing_models],
        }

        async def _do(c: asyncpg.Connection) -> None:
            await c.execute(
                """
                INSERT INTO think_trigger_queue (
                    id, tenant_id, trigger_kind, trigger_subkind,
                    observation_id, model_id, payload
                ) VALUES (
                    $1, $2, 'T2', 'prediction_overdue',
                    NULL, $3, $4::jsonb
                )
                """,
                trigger_id,
                tenant_id,
                prediction_id,
                json.dumps(payload),
            )

        # If the caller's connection is in a transaction, reuse it;
        # otherwise open one with lib.shared.db.transaction() per the
        # spec directive ("Use lib/shared/db.transaction() for the
        # enqueue").
        if conn.is_in_transaction():
            await _do(conn)
        else:
            async with transaction(pool=self._pool) as tx:
                await _do(tx)
        return trigger_id


__all__ = [
    "DeadlineResolver",
    "CycleResult",
    "DEFAULT_POLL_INTERVAL_S",
    "ProvisionalOutcome",
]
