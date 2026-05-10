"""services/think/worker.py — per-tenant Think worker process.

BUILD-PLAN §4 Prompt 3.B item 1.

Polls:
  * think_trigger_queue  (T1/T2/T3/T4) via FOR UPDATE SKIP LOCKED
  * model_reeval_queue   (W3.Q8 consumer contract) — convert pending
    rows into T4 triggers with subkind='model_reeval'.

Per-tenant concurrency cap via asyncio.Semaphore keyed by tenant_id
(default 4; env `THINK_MAX_CONCURRENCY_PER_TENANT`).

Backpressure: if queue depth > `THINK_QUEUE_BACKPRESSURE_LIMIT`
(default 500), log a warning and slow polling. Newly-enqueued rows
still land; older ones drain first per enqueued_at.

Graceful shutdown: SIGTERM sets a flag; the loop stops polling and
awaits in-flight runs.

Dead-letter policy:
  * Trigger queue failures after 5 attempts → mark completed_at=now()
    + set last_error (no separate DL table for trigger queue; the
    failed row is the dead-letter record since completed_at filters
    it out of polling).
  * model_reeval_queue failures after 5 attempts → move the row to
    `model_reeval_dead_letter` AND set original_row.processed_at=now()
    so the dedup collapses if a new identical row enqueues later.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMProvider
from lib.shared.ids import uuid7

from services.retrieval.primary import TriggerContext

from .observability import METRICS, emit
from .reason import ThinkRunOutcome, think


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Payload → TriggerContext rehydration
# ---------------------------------------------------------------------

def _populate_seed_fields(trigger: TriggerContext, payload: dict) -> None:
    """
    Copy every seed field the enqueuer supplied from the queue row's
    payload onto the TriggerContext. Missing fields leave the context
    defaults intact.

    The enqueuer contract (Wave 2-A ingestion, Wave 4-B anomaly
    processor, entity resolver's T1 re-enqueue, and `model_reeval`
    T4 dispatch) serialises the relevant TriggerContext fields into
    the queue row's `payload` JSONB. Anything in `TriggerContext` that
    the enqueuer might set must be recognised here; anything added to
    `TriggerContext` later needs a matching case below.
    """
    text = payload.get("seed_natural_text")
    if isinstance(text, str):
        trigger.seed_natural_text = text

    entity_ids = payload.get("seed_entity_ids")
    if isinstance(entity_ids, list):
        trigger.seed_entity_ids = [
            e for e in entity_ids if isinstance(e, dict)
        ]

    occurred = payload.get("seed_occurred_at")
    if isinstance(occurred, str):
        try:
            # asyncpg returns UTC ISO-8601 naturally; accept both
            # the explicit Z form and the +00:00 form.
            trigger.seed_occurred_at = datetime.fromisoformat(
                occurred.replace("Z", "+00:00")
            )
        except ValueError:
            pass
    elif isinstance(occurred, datetime):
        trigger.seed_occurred_at = occurred

    scope_actors = payload.get("scope_actors")
    if isinstance(scope_actors, list):
        out = []
        for a in scope_actors:
            if isinstance(a, UUID):
                out.append(a)
            elif isinstance(a, str):
                try:
                    out.append(UUID(a))
                except ValueError:
                    continue
        trigger.scope_actors = out

    region_spec = payload.get("region_spec")
    if isinstance(region_spec, dict):
        trigger.region_spec = region_spec

    # S3 — topology phase event (T6) payload fields. The
    # neighborhood_detector worker writes these into the trigger
    # payload (see services.workers.neighborhood_detector.worker).
    tev_id = payload.get("topology_event_id")
    if isinstance(tev_id, str):
        try:
            trigger.topology_event_id = UUID(tev_id)
        except ValueError:
            pass
    tev_kind = payload.get("topology_event_kind")
    if isinstance(tev_kind, str):
        trigger.topology_event_kind = tev_kind
    nh_id = payload.get("neighborhood_id")
    if isinstance(nh_id, str):
        try:
            trigger.neighborhood_id = UUID(nh_id)
        except ValueError:
            pass
    members = payload.get("member_model_ids")
    if isinstance(members, list):
        out = []
        for m in members:
            if isinstance(m, UUID):
                out.append(m)
            elif isinstance(m, str):
                try:
                    out.append(UUID(m))
                except ValueError:
                    continue
        trigger.member_model_ids = out


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------


@dataclass
class WorkerConfig:
    poll_interval_s: float = 2.0
    poll_batch: int = 10
    max_concurrency_per_tenant: int = 4
    backpressure_limit: int = 500
    trigger_max_attempts: int = 5
    reeval_max_attempts: int = 5
    worker_id: str = "worker"

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        return cls(
            poll_interval_s=float(os.environ.get("THINK_POLL_INTERVAL_S", 2.0)),
            poll_batch=int(os.environ.get("THINK_POLL_BATCH", 10)),
            max_concurrency_per_tenant=int(os.environ.get(
                "THINK_MAX_CONCURRENCY_PER_TENANT", 4
            )),
            backpressure_limit=int(os.environ.get(
                "THINK_QUEUE_BACKPRESSURE_LIMIT", 500
            )),
            trigger_max_attempts=int(os.environ.get(
                "THINK_TRIGGER_MAX_ATTEMPTS", 5
            )),
            reeval_max_attempts=int(os.environ.get(
                "THINK_REEVAL_MAX_ATTEMPTS", 5
            )),
            worker_id=os.environ.get("THINK_WORKER_ID", f"worker-{os.getpid()}"),
        )


# ---------------------------------------------------------------------
# Worker
# ---------------------------------------------------------------------


class ThinkWorker:
    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: WorkerConfig | None = None,
        llm_provider: LLMProvider | None = None,
        embedder: Any | None = None,
    ) -> None:
        self.pool = pool
        self.config = config or WorkerConfig.from_env()
        self.llm_provider = llm_provider
        # Embedder wire-through — enables pathway B (semantic retrieval)
        # and pathway C (temporal) in primary_retrieve. Lazy-constructed
        # default so tests that don't want Ollama can pass None.
        if embedder is None:
            try:
                from lib.embeddings.ollama import OllamaClient
                embedder = OllamaClient()
            except Exception:  # noqa: BLE001
                embedder = None
        self.embedder = embedder
        self._semaphores: dict[UUID, asyncio.Semaphore] = {}
        self._shutdown_event = asyncio.Event()
        self._in_flight: set[asyncio.Task] = set()

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._shutdown_event.set)
            except NotImplementedError:
                # Windows — skip.
                pass

    # -----------------------------------------------------------------
    # Main loop
    # -----------------------------------------------------------------

    async def run(self) -> None:
        emit("think.worker.started", worker_id=self.config.worker_id)
        while not self._shutdown_event.is_set():
            try:
                # 1. Promote pending model_reeval_queue rows to T4 triggers.
                await self._promote_reeval_rows()

                # 2. Poll and dispatch.
                await self._poll_and_dispatch()
            except Exception as e:
                _log.exception("think.worker.loop_error", error=str(e))

            # Backpressure-sensitive sleep.
            depth = await self._queue_depth()
            interval = self.config.poll_interval_s
            if depth > self.config.backpressure_limit:
                interval *= 1.5
                _log.warning(
                    "think.worker.backpressure",
                    depth=depth,
                    limit=self.config.backpressure_limit,
                )
            try:
                await asyncio.wait_for(
                    self._shutdown_event.wait(), timeout=interval
                )
                break
            except asyncio.TimeoutError:
                pass

        # Shutdown — wait for in-flight runs to finish.
        emit("think.worker.shutting_down",
             in_flight=len(self._in_flight))
        if self._in_flight:
            await asyncio.gather(*self._in_flight, return_exceptions=True)
        emit("think.worker.stopped")

    async def stop(self) -> None:
        self._shutdown_event.set()

    # -----------------------------------------------------------------
    # Reeval-queue promotion
    # -----------------------------------------------------------------

    async def _promote_reeval_rows(self) -> None:
        """
        Per W3.Q8 consumer contract: read pending rows, enqueue a T4
        trigger (subkind='model_reeval') into think_trigger_queue. DO
        NOT set processed_at yet — that happens when the T4 trigger
        completes (we wire this by making processed_at-update a part
        of the trigger-completion path, see `_mark_trigger_complete`).
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT id, tenant_id, model_id, cause_model_id, cause_kind
                    FROM model_reeval_queue
                    WHERE processed_at IS NULL
                      AND attempts < $1
                    ORDER BY enqueued_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                    """,
                    self.config.reeval_max_attempts,
                    self.config.poll_batch,
                )
                for r in rows:
                    # Only promote if no trigger is already in flight
                    # for this reeval row. We use the reeval row id as
                    # the trigger's idempotency key in payload.
                    existing = await conn.fetchval(
                        """
                        SELECT 1 FROM think_trigger_queue
                        WHERE trigger_kind = 'T4'
                          AND payload->>'reeval_row_id' = $1
                          AND completed_at IS NULL
                        LIMIT 1
                        """,
                        str(r["id"]),
                    )
                    if existing is not None:
                        continue
                    payload = {
                        "reeval_row_id": str(r["id"]),
                        "cause_model_id": (
                            str(r["cause_model_id"])
                            if r["cause_model_id"] else None
                        ),
                        "cause_kind": r["cause_kind"],
                    }
                    await conn.execute(
                        """
                        INSERT INTO think_trigger_queue
                          (id, tenant_id, trigger_kind, trigger_subkind,
                           model_id, payload)
                        VALUES ($1, $2, 'T4', 'model_reeval', $3, $4::jsonb)
                        """,
                        uuid7(),
                        r["tenant_id"],
                        r["model_id"],
                        json.dumps(payload),
                    )

    # -----------------------------------------------------------------
    # Polling + dispatch
    # -----------------------------------------------------------------

    async def _poll_and_dispatch(self) -> None:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    """
                    SELECT id, tenant_id, trigger_kind, trigger_subkind,
                           observation_id, model_id, payload, attempts
                    FROM think_trigger_queue
                    WHERE completed_at IS NULL
                      AND locked_by IS NULL
                      AND scheduled_for <= now()
                      AND attempts < $1
                    ORDER BY enqueued_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT $2
                    """,
                    self.config.trigger_max_attempts,
                    self.config.poll_batch,
                )
                leased_ids = [r["id"] for r in rows]
                if leased_ids:
                    await conn.execute(
                        """
                        UPDATE think_trigger_queue
                        SET locked_by = $1, locked_at = now()
                        WHERE id = ANY($2::uuid[])
                        """,
                        self.config.worker_id,
                        leased_ids,
                    )
            for r in rows:
                task = asyncio.create_task(
                    self._dispatch_trigger(r)
                )
                self._in_flight.add(task)
                task.add_done_callback(self._in_flight.discard)

    async def _dispatch_trigger(
        self, row: asyncpg.Record
    ) -> None:
        tenant_id = row["tenant_id"]
        sem = self._semaphores.setdefault(
            tenant_id,
            asyncio.Semaphore(self.config.max_concurrency_per_tenant),
        )
        async with sem:
            await self._process_trigger(row)

    async def _process_trigger(self, row: asyncpg.Record) -> None:
        payload = row["payload"] or {}
        if isinstance(payload, (bytes, bytearray)):
            payload = json.loads(payload.decode())
        elif isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}

        # TK-3 — enforce cross-trigger cascade depth bound. If this T1
        # carries a `cascade_depth` that has reached MAX_CASCADE_DEPTH,
        # fail it non-retryable and do NOT dispatch. This catches the
        # case where state_change → T1 chains would otherwise loop
        # indefinitely (e.g. a cycle where two commitments keep
        # unblocking each other across Think cycles).
        from .cascade import MAX_CASCADE_DEPTH
        cascade_depth_raw = payload.get("cascade_depth", 0)
        try:
            cascade_depth = int(cascade_depth_raw)
        except (TypeError, ValueError):
            cascade_depth = 0
        if cascade_depth >= MAX_CASCADE_DEPTH:
            _log.warning(
                "cascade_bound_violation",
                stage="trigger_rejected",
                trigger_id=str(row["id"]),
                trigger_kind=row["trigger_kind"],
                trigger_subkind=row["trigger_subkind"],
                tenant_id=str(row["tenant_id"]),
                cascade_depth=cascade_depth,
                max_cascade_depth=MAX_CASCADE_DEPTH,
            )
            # Non-retryable: mark the row terminal by pushing attempts
            # past the cap so `_mark_trigger_failed` completes it.
            await self._mark_trigger_failed(
                row["id"],
                f"cascade_bound_violation: depth={cascade_depth} >= {MAX_CASCADE_DEPTH}",
                force_terminal=True,
            )
            return

        trigger = TriggerContext(
            kind=row["trigger_kind"],
            tenant_id=row["tenant_id"],
            subkind=row["trigger_subkind"],
            observation_id=row["observation_id"],
            model_id=row["model_id"],
            seed_signature={
                **payload,
                "trigger_id": str(row["id"]),
            },
        )
        # Rehydrate every seed field the enqueuer supplied. Without
        # this, the worker's TriggerContext is missing entity hints
        # that the retrieval region computation needs — the LLM then
        # returns a diff touching un-locked entities and the validator
        # raises OutOfRegionError. (Bug surfaced by the Wave 3-B
        # follow-up agent; blocks Wave 4-B T3 enqueue path.)
        _populate_seed_fields(trigger, payload)
        try:
            outcome = await think(
                trigger,
                self.pool,
                llm_provider=self.llm_provider,
                embedder=self.embedder,
                trigger_kind_subkind=(
                    f"{row['trigger_kind']}:{row['trigger_subkind']}"
                    if row["trigger_subkind"] else row["trigger_kind"]
                ),
            )
        except Exception as e:
            _log.exception(
                "think.worker.unhandled_failure",
                trigger_id=str(row["id"]),
                error=str(e),
            )
            await self._mark_trigger_failed(row["id"], str(e))
            return

        if outcome.succeeded or outcome.skipped_idempotent:
            await self._mark_trigger_complete(
                row["id"], payload=payload
            )
            # POST_COMMIT_HOOK (OP-1): integrated. Post-commit actions are
            # now enqueued in `reason.py::_run_once` inside the apply
            # transaction (atomic with apply_diff) via
            # `services.think.post_commit.enqueue_post_commit_actions`.
            # A separate worker process
            # (`services.think.post_commit.post_commit_worker`) drains the
            # `pending_post_commit_actions` queue with FOR UPDATE SKIP
            # LOCKED dispatch, exponential backoff, and dead-letter after
            # MAX_ATTEMPTS=5. Nothing for the trigger-queue worker to do
            # here anymore; the queue row has already been written atomic
            # with the apply that produced its payload. Marker preserved
            # as documentation of the integration point.
        else:
            await self._mark_trigger_failed(row["id"], outcome.error or "unknown")

    # -----------------------------------------------------------------
    # Trigger lifecycle
    # -----------------------------------------------------------------

    async def _mark_trigger_complete(
        self,
        trigger_id: UUID,
        payload: dict[str, Any] | None = None,
    ) -> None:
        """
        Mark a trigger queue row completed. If the trigger was a
        model_reeval T4 (payload contains reeval_row_id), also stamp
        processed_at on the original model_reeval_queue row in the
        same transaction.
        """
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE think_trigger_queue
                    SET completed_at = now(),
                        locked_by = NULL,
                        locked_at = NULL
                    WHERE id = $1
                    """,
                    trigger_id,
                )
                if payload and "reeval_row_id" in payload:
                    try:
                        rrid = UUID(str(payload["reeval_row_id"]))
                    except (ValueError, TypeError):
                        rrid = None
                    if rrid is not None:
                        await conn.execute(
                            """
                            UPDATE model_reeval_queue
                            SET processed_at = now()
                            WHERE id = $1 AND processed_at IS NULL
                            """,
                            rrid,
                        )

    async def _mark_trigger_failed(
        self,
        trigger_id: UUID,
        error: str,
        *,
        force_terminal: bool = False,
    ) -> None:
        """Mark a trigger failed. `force_terminal=True` flags the row
        as non-retryable (used by TK-3 cascade-bound violations) and
        completes it immediately regardless of attempt count."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    "SELECT attempts, payload FROM think_trigger_queue WHERE id = $1",
                    trigger_id,
                )
                if row is None:
                    return
                attempts = int(row["attempts"] or 0) + 1
                payload = row["payload"] or {}
                if isinstance(payload, (bytes, bytearray)):
                    payload = json.loads(payload.decode())
                elif isinstance(payload, str):
                    try:
                        payload = json.loads(payload)
                    except Exception:
                        payload = {}
                # Increment attempts; if past the limit (or forced), complete (dead letter).
                terminal = force_terminal or attempts >= self.config.trigger_max_attempts
                backoff_seconds = min(300, 10 * (2 ** min(attempts, 4)))
                if terminal:
                    await conn.execute(
                        """
                        UPDATE think_trigger_queue
                        SET attempts = $2,
                            completed_at = now(),
                            locked_by = NULL,
                            locked_at = NULL
                        WHERE id = $1
                        """,
                        trigger_id, attempts,
                    )
                    # For model_reeval, move the original row to dead letter.
                    if "reeval_row_id" in payload:
                        try:
                            rrid = UUID(str(payload["reeval_row_id"]))
                        except (ValueError, TypeError):
                            rrid = None
                        if rrid is not None:
                            await self._dead_letter_reeval(
                                conn, rrid, attempts, error
                            )
                else:
                    await conn.execute(
                        """
                        UPDATE think_trigger_queue
                        SET attempts = $2,
                            locked_by = NULL,
                            locked_at = NULL,
                            scheduled_for = now() + ($3 || ' seconds')::interval
                        WHERE id = $1
                        """,
                        trigger_id, attempts, str(backoff_seconds),
                    )

    async def _dead_letter_reeval(
        self,
        conn: asyncpg.Connection,
        reeval_row_id: UUID,
        attempts: int,
        last_error: str,
    ) -> None:
        """
        Move an exhausted model_reeval_queue row into
        model_reeval_dead_letter AND set processed_at on the original
        so the dedup doesn't collide with a future enqueue.
        """
        row = await conn.fetchrow(
            "SELECT * FROM model_reeval_queue WHERE id = $1",
            reeval_row_id,
        )
        if row is None:
            return
        await conn.execute(
            """
            INSERT INTO model_reeval_dead_letter
              (id, tenant_id, original_queue_id, model_id,
               cause_model_id, cause_kind, attempts, last_error,
               enqueued_at)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
            uuid7(),
            row["tenant_id"],
            row["id"],
            row["model_id"],
            row["cause_model_id"],
            row["cause_kind"],
            attempts,
            last_error,
            row["enqueued_at"],
        )
        await conn.execute(
            """
            UPDATE model_reeval_queue
            SET processed_at = now(),
                attempts = $2,
                last_error = $3
            WHERE id = $1
            """,
            reeval_row_id, attempts, last_error,
        )

    # -----------------------------------------------------------------
    # Queue depth
    # -----------------------------------------------------------------

    async def _queue_depth(self) -> int:
        async with self.pool.acquire() as conn:
            n = await conn.fetchval(
                """
                SELECT COUNT(*) FROM think_trigger_queue
                WHERE completed_at IS NULL
                """
            )
            depth = int(n or 0)
            METRICS.set_queue_depth("all", depth)
            return depth


__all__ = ["ThinkWorker", "WorkerConfig"]
