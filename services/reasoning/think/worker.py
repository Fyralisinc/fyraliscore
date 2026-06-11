"""services/reasoning/think/worker.py — per-tenant Think worker process.

BUILD-PLAN §4 Prompt 3.B item 1.

Polls:
  * think_trigger_queue  (T1/T2/T3/T4) via FOR UPDATE SKIP LOCKED
  * model_reeval_queue   (W3.Q8 consumer contract) — convert pending
    rows into T4 triggers with subkind='model_reeval'.

Per-tenant concurrency cap via asyncio.Semaphore keyed by tenant_id
(default 1; env `THINK_MAX_CONCURRENCY_PER_TENANT`).

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
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg
import structlog

from lib.llm.provider import LLMProvider, build_provider
from lib.observability.health import (
    Heartbeat,
    run_heartbeat_ticker,
    start_health_server,
)
from lib.observability.metrics import render_default
from lib.shared.ids import uuid7

from services.reasoning.retrieval.primary import TriggerContext

from .observability import METRICS, emit, render_prometheus_text
from .reason import think


_log = structlog.get_logger(__name__)
_DEFAULT_T1_BATCH_WINDOW_S = 30.0
_DEFAULT_DOWNSTREAM_BATCH_WINDOW_S = 60.0


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

    observation_ids = payload.get("observation_ids")
    if not isinstance(observation_ids, list):
        observation_ids = payload.get("batch_observation_ids")
    if isinstance(observation_ids, list):
        trigger.observation_ids = _coerce_uuid_list(observation_ids)

    member_trigger_ids = payload.get("member_trigger_ids")
    if not isinstance(member_trigger_ids, list):
        member_trigger_ids = payload.get("batch_member_trigger_ids")
    if isinstance(member_trigger_ids, list):
        trigger.member_trigger_ids = _coerce_uuid_list(member_trigger_ids)

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

    # Legacy T6 topology phase-event payload fields. The accepted-memory
    # neighborhood worker is retired, but old queued rows may still carry
    # this shape and should hydrate cleanly.
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
    model_ids = payload.get("model_ids")
    if not isinstance(model_ids, list):
        model_ids = payload.get("batch_model_ids")
    if isinstance(model_ids, list):
        merged = list(trigger.member_model_ids)
        seen = set(merged)
        for mid in _coerce_uuid_list(model_ids):
            if trigger.model_id is not None and mid == trigger.model_id:
                continue
            if mid not in seen:
                seen.add(mid)
                merged.append(mid)
        trigger.member_model_ids = merged


def _coerce_uuid_list(values: list[Any]) -> list[UUID]:
    out: list[UUID] = []
    seen: set[UUID] = set()
    for value in values:
        if isinstance(value, UUID):
            uid = value
        else:
            try:
                uid = UUID(str(value))
            except (TypeError, ValueError):
                continue
        if uid not in seen:
            seen.add(uid)
            out.append(uid)
    return out


def _payload_uuid_list(payload: dict[str, Any], key: str) -> list[UUID]:
    values = payload.get(key)
    if not isinstance(values, list):
        return []
    return _coerce_uuid_list(values)


def _payload_dict(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            decoded = raw.decode()
        except Exception:
            return {}
        raw = decoded
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except Exception:
            return {}
        return value if isinstance(value, dict) else {}
    return {}


def _timestamp(value: datetime) -> float:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.timestamp()


def _validation_max_attempts_env() -> int | None:
    """Cost-plan §2.4 flag `THINK_VALIDATION_MAX_ATTEMPTS`. When set, validation-
    class failures dead-letter after this many attempts instead of the generic
    `trigger_max_attempts` — stopping the blind same-model resample. Unset (the
    default) → no change in behavior."""
    raw = os.environ.get("THINK_VALIDATION_MAX_ATTEMPTS")
    if raw is None or raw.strip() == "":
        return None
    try:
        return max(1, int(raw))
    except ValueError:
        return None


def _classify_failure(outcome: Any) -> str | None:
    """Cost-plan §2.4: bucket a failed ThinkRunOutcome by exception class so the
    worker can apply a validation-specific retry cap + feedback loop."""
    exc = getattr(outcome, "exception", None)
    if exc is None:
        return None
    name = type(exc).__name__
    if "Validation" in name:
        return "validation"
    if "Reasoning" in name:
        return "reasoning"
    return None


def _daily_budget_enforcement_enabled() -> bool:
    return os.environ.get(
        "THINK_DAILY_BUDGET_ENFORCEMENT", "0"
    ).strip().lower() in {"1", "on", "true", "yes"}


def _escalation_model_env() -> str | None:
    """Cost-plan §2.4: model to escalate to on a validation retry. Unset (the
    default) → no escalation; the retry re-runs on the same model with feedback."""
    raw = os.environ.get("THINK_ESCALATION_MODEL")
    return raw.strip() if raw and raw.strip() else None


def _max_inferential_lineage_depth(hard_max: int) -> int:
    """Cost-plan §3.2: optional tighter cross-trigger lineage bound. Unset →
    the existing `MAX_CASCADE_DEPTH` (`hard_max`, no change). When set, the
    effective bound is `min(hard_max, value)`. The depth field is now threaded
    through T2/T3/T4 (not just T1); observe the lineage distribution before
    tightening this below the hard max."""
    raw = os.environ.get("THINK_MAX_INFERENTIAL_LINEAGE_DEPTH")
    if raw is None or raw.strip() == "":
        return hard_max
    try:
        return min(hard_max, max(1, int(raw)))
    except ValueError:
        return hard_max


def _daily_budget_usd_per_tenant() -> float | None:
    """Cost-plan §3.1 R3 ceiling. None disables (default). Positive value caps
    a tenant's daily LLM spend; over-budget triggers wait in queue (never
    dead-letter) until the day rolls over."""
    raw = os.environ.get("LLM_DAILY_BUDGET_USD_PER_TENANT")
    if raw is None or raw.strip() == "":
        return None
    try:
        value = float(raw)
    except ValueError:
        return None
    return value if value > 0 else None


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------


@dataclass
class WorkerConfig:
    poll_interval_s: float = 2.0
    poll_batch: int = 10
    max_concurrency_per_tenant: int = 1
    backpressure_limit: int = 500
    trigger_max_attempts: int = 5
    reeval_max_attempts: int = 5
    trigger_lock_timeout_s: float = 600.0
    trigger_heartbeat_interval_s: float = 30.0
    run_timeout_s: float = 600.0
    t1_batch_window_s: float = _DEFAULT_T1_BATCH_WINDOW_S
    t1_batch_max_size: int = 30
    t1_batch_min_size: int = 20
    downstream_batch_window_s: float = _DEFAULT_DOWNSTREAM_BATCH_WINDOW_S
    downstream_batch_min_size: int = 2
    t2_batch_max_size: int = 8
    t4_batch_max_size: int = 4
    prune_low_value_downstream_triggers: bool = True
    worker_id: str = "worker"
    tenant_filter: UUID | None = None

    @classmethod
    def from_env(cls) -> "WorkerConfig":
        return cls(
            poll_interval_s=float(os.environ.get("THINK_POLL_INTERVAL_S", 2.0)),
            poll_batch=int(os.environ.get("THINK_POLL_BATCH", 10)),
            max_concurrency_per_tenant=int(os.environ.get(
                "THINK_MAX_CONCURRENCY_PER_TENANT", 1
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
            trigger_lock_timeout_s=float(os.environ.get(
                "THINK_TRIGGER_LOCK_TIMEOUT_S", 600.0
            )),
            trigger_heartbeat_interval_s=float(os.environ.get(
                "THINK_TRIGGER_HEARTBEAT_INTERVAL_S", 30.0
            )),
            run_timeout_s=float(os.environ.get(
                "THINK_RUN_TIMEOUT_S", 600.0
            )),
            t1_batch_window_s=float(os.environ.get(
                "THINK_T1_BATCH_WINDOW_S", _DEFAULT_T1_BATCH_WINDOW_S
            )),
            t1_batch_max_size=int(os.environ.get(
                "THINK_T1_BATCH_MAX_SIZE", 30
            )),
            t1_batch_min_size=int(os.environ.get(
                "THINK_T1_BATCH_MIN_SIZE", 20
            )),
            downstream_batch_window_s=float(os.environ.get(
                "THINK_DOWNSTREAM_BATCH_WINDOW_S",
                _DEFAULT_DOWNSTREAM_BATCH_WINDOW_S,
            )),
            downstream_batch_min_size=int(os.environ.get(
                "THINK_DOWNSTREAM_BATCH_MIN_SIZE", 2
            )),
            t2_batch_max_size=int(os.environ.get(
                "THINK_T2_BATCH_MAX_SIZE", 8
            )),
            t4_batch_max_size=int(os.environ.get(
                "THINK_T4_BATCH_MAX_SIZE", 4
            )),
            prune_low_value_downstream_triggers=(
                os.environ.get(
                    "THINK_PRUNE_LOW_VALUE_DOWNSTREAM_TRIGGERS", "1"
                ).strip().lower()
                not in {"0", "false", "no", "off"}
            ),
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
        # Cost-plan §2.4: lazily-built, cached escalation provider used only on
        # validation retries when THINK_ESCALATION_MODEL is set.
        self._escalation_provider: LLMProvider | None = None

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
        # P2-13: liveness + metrics surface (opt-in via INGESTION_HEALTH_PORT,
        # which the compose x-app-env anchor already sets). /healthz goes 503
        # when the poll loop wedges; /metrics serves the Think families plus
        # the shared lib.observability registry (ollama, db pool, …).
        heartbeat = Heartbeat()
        health = start_health_server(
            worker_name="think_worker",
            render_metrics=lambda: render_prometheus_text() + render_default(),
            heartbeat=heartbeat,
        )
        ticker = asyncio.create_task(
            run_heartbeat_ticker(heartbeat, self._shutdown_event)
        )
        try:
            while not self._shutdown_event.is_set():
                heartbeat.touch()
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
        finally:
            ticker.cancel()
            await asyncio.gather(ticker, return_exceptions=True)
            if health is not None:
                health.shutdown()
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
                if self.config.tenant_filter is None:
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
                else:
                    rows = await conn.fetch(
                        """
                        SELECT id, tenant_id, model_id, cause_model_id, cause_kind
                        FROM model_reeval_queue
                        WHERE processed_at IS NULL
                          AND attempts < $1
                          AND tenant_id = $2
                        ORDER BY enqueued_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT $3
                        """,
                        self.config.reeval_max_attempts,
                        self.config.tenant_filter,
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
        available_slots = max(0, self.config.poll_batch - len(self._in_flight))
        if available_slots <= 0:
            return
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await self._prune_low_value_downstream_rows(conn)
                rows: list[Any] = await self._create_t1_batch_rows(
                    conn,
                    available_slots=available_slots,
                )
                remaining_slots = available_slots - len(rows)
                if remaining_slots > 0:
                    downstream_rows = await self._create_downstream_batch_rows(
                        conn,
                        available_slots=remaining_slots,
                    )
                    rows.extend(downstream_rows)
                    remaining_slots = available_slots - len(rows)
                if remaining_slots <= 0:
                    poll_rows = []
                elif self.config.tenant_filter is None:
                    poll_rows = await conn.fetch(
                        """
                        SELECT id, tenant_id, trigger_kind, trigger_subkind,
                               observation_id, model_id, payload, attempts
                        FROM think_trigger_queue
                        WHERE completed_at IS NULL
                          AND batch_parent_id IS NULL
                          AND (
                            locked_by IS NULL
                            OR locked_at < now() - ($3 || ' seconds')::interval
                          )
                          AND scheduled_for <= now()
                          AND attempts < $1
                          AND (
                            $4 = '0'
                            OR trigger_kind != 'T1'
                            OR trigger_subkind IS DISTINCT FROM 'event_arrival'
                            OR enqueued_at <= now() - ($4 || ' seconds')::interval
                          )
                          AND (
                            $5 = '0'
                            OR NOT (
                              (trigger_kind = 'T2'
                               AND trigger_subkind = 'belief_updated')
                              OR (trigger_kind = 'T4'
                                  AND trigger_subkind = 'latent_relationship_candidate')
                            )
                            OR enqueued_at <= now() - ($5 || ' seconds')::interval
                          )
                        ORDER BY
                          CASE
                            WHEN trigger_kind = 'T4'
                              AND trigger_subkind = 'latent_relationship_candidate'
                              THEN 0
                            WHEN trigger_kind = 'T2' THEN 1
                            WHEN trigger_kind = 'T4' THEN 2
                            WHEN trigger_kind = 'T3' THEN 3
                            ELSE 4
                          END,
                          enqueued_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT $2
                        """,
                        self.config.trigger_max_attempts,
                        remaining_slots,
                        str(self.config.trigger_lock_timeout_s),
                        self._t1_batch_window_arg(),
                        self._downstream_batch_window_arg(),
                    )
                else:
                    poll_rows = await conn.fetch(
                        """
                        SELECT id, tenant_id, trigger_kind, trigger_subkind,
                               observation_id, model_id, payload, attempts
                        FROM think_trigger_queue
                        WHERE completed_at IS NULL
                          AND batch_parent_id IS NULL
                          AND (
                            locked_by IS NULL
                            OR locked_at < now() - ($4 || ' seconds')::interval
                          )
                          AND scheduled_for <= now()
                          AND attempts < $1
                          AND tenant_id = $2
                          AND (
                            $5 = '0'
                            OR trigger_kind != 'T1'
                            OR trigger_subkind IS DISTINCT FROM 'event_arrival'
                            OR enqueued_at <= now() - ($5 || ' seconds')::interval
                          )
                          AND (
                            $6 = '0'
                            OR NOT (
                              (trigger_kind = 'T2'
                               AND trigger_subkind = 'belief_updated')
                              OR (trigger_kind = 'T4'
                                  AND trigger_subkind = 'latent_relationship_candidate')
                            )
                            OR enqueued_at <= now() - ($6 || ' seconds')::interval
                          )
                        ORDER BY
                          CASE
                            WHEN trigger_kind = 'T4'
                              AND trigger_subkind = 'latent_relationship_candidate'
                              THEN 0
                            WHEN trigger_kind = 'T2' THEN 1
                            WHEN trigger_kind = 'T4' THEN 2
                            WHEN trigger_kind = 'T3' THEN 3
                            ELSE 4
                          END,
                          enqueued_at ASC
                        FOR UPDATE SKIP LOCKED
                        LIMIT $3
                        """,
                        self.config.trigger_max_attempts,
                        self.config.tenant_filter,
                        remaining_slots,
                        str(self.config.trigger_lock_timeout_s),
                        self._t1_batch_window_arg(),
                        self._downstream_batch_window_arg(),
                    )
                rows.extend(poll_rows)
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

    async def _prune_low_value_downstream_rows(
        self,
        conn: asyncpg.Connection,
    ) -> None:
        if not self.config.prune_low_value_downstream_triggers:
            return
        tenant_clause = ""
        args: list[Any] = [self.config.worker_id]
        if self.config.tenant_filter is not None:
            args.append(self.config.tenant_filter)
            tenant_clause = f"AND q.tenant_id = ${len(args)}"

        await conn.execute(
            f"""
            UPDATE think_trigger_queue q
            SET completed_at = now(),
                locked_by = NULL,
                locked_at = NULL,
                payload = q.payload || jsonb_build_object(
                  'auto_completed_reason',
                  'non_prediction_belief_updated_noop',
                  'auto_completed_by',
                  $1::text
                )
            FROM models m
            WHERE q.completed_at IS NULL
              AND q.batch_parent_id IS NULL
              AND q.trigger_kind = 'T2'
              AND q.trigger_subkind = 'belief_updated'
              AND q.model_id = m.id
              AND q.tenant_id = m.tenant_id
              {tenant_clause}
              AND COALESCE(m.proposition_kind, 'belief') <> 'prediction'
              AND (m.falsifier IS NULL OR m.falsifier = '{{}}'::jsonb)
              AND m.evaluate_at IS NULL
              AND m.resolution_criteria IS NULL
            """,
            *args,
        )

        await self._aggregate_edge_type_candidates_for_pruning(conn)

        await conn.execute(
            f"""
            UPDATE think_trigger_queue q
            SET completed_at = now(),
                locked_by = NULL,
                locked_at = NULL,
                payload = q.payload || jsonb_build_object(
                  'auto_completed_reason',
                  'missing_model_belief_updated_noop',
                  'auto_completed_by',
                  $1::text
                )
            WHERE q.completed_at IS NULL
              AND q.batch_parent_id IS NULL
              AND q.trigger_kind = 'T2'
              AND q.trigger_subkind = 'belief_updated'
              {tenant_clause}
              AND NOT EXISTS (
                SELECT 1
                FROM models m
                WHERE m.id = q.model_id
                  AND m.tenant_id = q.tenant_id
              )
            """,
            *args,
        )

        await conn.execute(
            f"""
            UPDATE think_trigger_queue q
            SET completed_at = now(),
                locked_by = NULL,
                locked_at = NULL,
                payload = q.payload || jsonb_build_object(
                  'auto_completed_reason',
                  'edge_type_candidate_aggregation_path',
                  'auto_completed_by',
                  $1::text
                )
            FROM relationship_candidates c
            WHERE q.completed_at IS NULL
              AND q.batch_parent_id IS NULL
              AND q.trigger_kind = 'T4'
              AND q.trigger_subkind = 'latent_relationship_candidate'
              {tenant_clause}
              AND q.payload ? 'relationship_candidate_id'
              AND (q.payload->>'relationship_candidate_id') ~*
                '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
              AND c.tenant_id = q.tenant_id
              AND c.id = (q.payload->>'relationship_candidate_id')::uuid
              AND c.candidate_kind = 'edge_type'
            """,
            *args,
        )

    async def _aggregate_edge_type_candidates_for_pruning(
        self,
        conn: asyncpg.Connection,
    ) -> None:
        tenant_clause = ""
        args: list[Any] = []
        if self.config.tenant_filter is not None:
            args.append(self.config.tenant_filter)
            tenant_clause = f"AND q.tenant_id = ${len(args)}"
        try:
            rows = await conn.fetch(
                f"""
                SELECT DISTINCT q.tenant_id
                FROM think_trigger_queue q
                JOIN relationship_candidates c
                  ON c.tenant_id = q.tenant_id
                 AND c.id = CASE
                   WHEN (q.payload->>'relationship_candidate_id') ~*
                     '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                   THEN (q.payload->>'relationship_candidate_id')::uuid
                   ELSE NULL
                 END
                WHERE q.completed_at IS NULL
                  AND q.batch_parent_id IS NULL
                  AND q.trigger_kind = 'T4'
                  AND q.trigger_subkind = 'latent_relationship_candidate'
                  {tenant_clause}
                  AND q.payload ? 'relationship_candidate_id'
                  AND (q.payload->>'relationship_candidate_id') ~*
                    '^[0-9a-f]{{8}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{4}}-[0-9a-f]{{12}}$'
                  AND c.candidate_kind = 'edge_type'
                """,
                *args,
            )
        except (
            asyncpg.UndefinedTableError,
            asyncpg.UndefinedColumnError,
        ):
            return
        if not rows:
            return
        from services.reasoning.relationships import RelationshipOntologyProposalsRepo

        repo = RelationshipOntologyProposalsRepo()
        for row in rows:
            try:
                await repo.aggregate_from_edge_type_candidates(
                    conn,
                    tenant_id=row["tenant_id"],
                )
            except (
                asyncpg.UndefinedTableError,
                asyncpg.UndefinedColumnError,
            ):
                return

    def _t1_batching_enabled(self) -> bool:
        return (
            self.config.t1_batch_window_s > 0
            and self.config.t1_batch_max_size >= 2
            and self.config.t1_batch_min_size >= 2
        )

    def _t1_batch_window_arg(self) -> str:
        if not self._t1_batching_enabled():
            return "0"
        return str(max(0.0, self.config.t1_batch_window_s))

    def _downstream_batching_enabled(self) -> bool:
        return (
            self.config.downstream_batch_window_s > 0
            and self.config.downstream_batch_min_size >= 2
            and (
                self.config.t2_batch_max_size >= 2
                or self.config.t4_batch_max_size >= 2
            )
        )

    def _downstream_batch_window_arg(self) -> str:
        if not self._downstream_batching_enabled():
            return "0"
        return str(max(0.0, self.config.downstream_batch_window_s))

    def _downstream_batch_max_size(self, kind: str, subkind: str | None) -> int:
        if kind == "T2" and subkind == "belief_updated":
            return max(0, self.config.t2_batch_max_size)
        if kind == "T4" and subkind == "latent_relationship_candidate":
            return max(0, self.config.t4_batch_max_size)
        return 0

    async def _create_downstream_batch_rows(
        self,
        conn: asyncpg.Connection,
        *,
        available_slots: int,
    ) -> list[dict[str, Any]]:
        if not self._downstream_batching_enabled() or available_slots <= 0:
            return []
        largest_batch = max(
            self.config.t2_batch_max_size,
            self.config.t4_batch_max_size,
            0,
        )
        if largest_batch < self.config.downstream_batch_min_size:
            return []
        limit = max(largest_batch, largest_batch * available_slots)
        if self.config.tenant_filter is None:
            candidates = await conn.fetch(
                """
                SELECT id, tenant_id, trigger_kind, trigger_subkind,
                       observation_id, model_id, payload, attempts, enqueued_at
                FROM think_trigger_queue
                WHERE completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND (payload->>'unbatched_from') IS NULL
                  AND (
                    locked_by IS NULL
                    OR locked_at < now() - ($2 || ' seconds')::interval
                  )
                  AND scheduled_for <= now()
                  AND attempts < $1
                  AND payload->>'batch' IS DISTINCT FROM 'true'
                  AND (
                    (trigger_kind = 'T2'
                     AND trigger_subkind = 'belief_updated'
                     AND model_id IS NOT NULL)
                    OR (trigger_kind = 'T4'
                        AND trigger_subkind = 'latent_relationship_candidate'
                        AND payload ? 'relationship_candidate_id')
                  )
                ORDER BY enqueued_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $3
                """,
                self.config.trigger_max_attempts,
                str(self.config.trigger_lock_timeout_s),
                limit,
            )
        else:
            candidates = await conn.fetch(
                """
                SELECT id, tenant_id, trigger_kind, trigger_subkind,
                       observation_id, model_id, payload, attempts, enqueued_at
                FROM think_trigger_queue
                WHERE completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND (payload->>'unbatched_from') IS NULL
                  AND (
                    locked_by IS NULL
                    OR locked_at < now() - ($3 || ' seconds')::interval
                  )
                  AND scheduled_for <= now()
                  AND attempts < $1
                  AND tenant_id = $2
                  AND payload->>'batch' IS DISTINCT FROM 'true'
                  AND (
                    (trigger_kind = 'T2'
                     AND trigger_subkind = 'belief_updated'
                     AND model_id IS NOT NULL)
                    OR (trigger_kind = 'T4'
                        AND trigger_subkind = 'latent_relationship_candidate'
                        AND payload ? 'relationship_candidate_id')
                  )
                ORDER BY enqueued_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $4
                """,
                self.config.trigger_max_attempts,
                self.config.tenant_filter,
                str(self.config.trigger_lock_timeout_s),
                limit,
            )
        if not candidates:
            return []

        now = datetime.now(timezone.utc)
        by_group: dict[tuple[UUID, str, str | None], list[asyncpg.Record]] = {}
        for row in candidates:
            key = (row["tenant_id"], row["trigger_kind"], row["trigger_subkind"])
            by_group.setdefault(key, []).append(row)

        batch_rows: list[dict[str, Any]] = []
        for (tenant_id, kind, subkind), rows in by_group.items():
            if len(batch_rows) >= available_slots:
                break
            max_size = self._downstream_batch_max_size(kind, subkind)
            if max_size < self.config.downstream_batch_min_size:
                continue
            rows.sort(key=lambda r: r["enqueued_at"])
            oldest = rows[0]["enqueued_at"]
            if oldest.tzinfo is None:
                oldest = oldest.replace(tzinfo=timezone.utc)
            cutoff = oldest.timestamp() + self.config.downstream_batch_window_s
            in_window = [
                r for r in rows
                if _timestamp(r["enqueued_at"]) <= cutoff
            ]
            window_elapsed = (
                (now - oldest).total_seconds()
                >= self.config.downstream_batch_window_s
            )
            max_size_reached = len(in_window) >= max_size
            if not window_elapsed and not max_size_reached:
                continue
            members = in_window[:max_size]
            if len(members) < self.config.downstream_batch_min_size:
                continue
            batch_row = await self._insert_downstream_batch_row(
                conn,
                tenant_id=tenant_id,
                trigger_kind=kind,
                trigger_subkind=subkind,
                members=members,
            )
            batch_rows.append(batch_row)
        return batch_rows

    async def _create_t1_batch_rows(
        self,
        conn: asyncpg.Connection,
        *,
        available_slots: int,
    ) -> list[dict[str, Any]]:
        if not self._t1_batching_enabled() or available_slots <= 0:
            return []
        limit = max(
            self.config.t1_batch_max_size,
            self.config.t1_batch_max_size * available_slots,
        )
        if self.config.tenant_filter is None:
            candidates = await conn.fetch(
                """
                SELECT id, tenant_id, trigger_kind, trigger_subkind,
                       observation_id, model_id, payload, attempts, enqueued_at
                FROM think_trigger_queue
                WHERE completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND (payload->>'unbatched_from') IS NULL
                  AND (
                    locked_by IS NULL
                    OR locked_at < now() - ($2 || ' seconds')::interval
                  )
                  AND scheduled_for <= now()
                  AND attempts < $1
                  AND trigger_kind = 'T1'
                  AND trigger_subkind = 'event_arrival'
                  AND observation_id IS NOT NULL
                ORDER BY enqueued_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $3
                """,
                self.config.trigger_max_attempts,
                str(self.config.trigger_lock_timeout_s),
                limit,
            )
        else:
            candidates = await conn.fetch(
                """
                SELECT id, tenant_id, trigger_kind, trigger_subkind,
                       observation_id, model_id, payload, attempts, enqueued_at
                FROM think_trigger_queue
                WHERE completed_at IS NULL
                  AND batch_parent_id IS NULL
                  AND (payload->>'unbatched_from') IS NULL
                  AND (
                    locked_by IS NULL
                    OR locked_at < now() - ($3 || ' seconds')::interval
                  )
                  AND scheduled_for <= now()
                  AND attempts < $1
                  AND tenant_id = $2
                  AND trigger_kind = 'T1'
                  AND trigger_subkind = 'event_arrival'
                  AND observation_id IS NOT NULL
                ORDER BY enqueued_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT $4
                """,
                self.config.trigger_max_attempts,
                self.config.tenant_filter,
                str(self.config.trigger_lock_timeout_s),
                limit,
            )
        if not candidates:
            return []

        now = datetime.now(timezone.utc)
        by_tenant: dict[UUID, list[asyncpg.Record]] = {}
        for row in candidates:
            by_tenant.setdefault(row["tenant_id"], []).append(row)

        batch_rows: list[dict[str, Any]] = []
        for tenant_id, rows in by_tenant.items():
            if len(batch_rows) >= available_slots:
                break
            rows.sort(key=lambda r: r["enqueued_at"])
            oldest = rows[0]["enqueued_at"]
            if oldest.tzinfo is None:
                oldest = oldest.replace(tzinfo=timezone.utc)
            cutoff = oldest.timestamp() + self.config.t1_batch_window_s
            in_window = [
                r for r in rows
                if _timestamp(r["enqueued_at"]) <= cutoff
            ]
            window_elapsed = (
                (now - oldest).total_seconds() >= self.config.t1_batch_window_s
            )
            max_size_reached = len(in_window) >= self.config.t1_batch_max_size
            if not window_elapsed and not max_size_reached:
                continue
            members = in_window[:self.config.t1_batch_max_size]
            if len(members) < self.config.t1_batch_min_size:
                continue
            batch_row = await self._insert_t1_batch_row(conn, tenant_id, members)
            batch_rows.append(batch_row)
        return batch_rows

    async def _insert_t1_batch_row(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        members: list[asyncpg.Record],
    ) -> dict[str, Any]:
        batch_id = uuid7()
        member_ids = [m["id"] for m in members]
        observation_ids = [m["observation_id"] for m in members if m["observation_id"]]
        payload = await self._build_t1_batch_payload(
            conn,
            batch_id=batch_id,
            members=members,
            observation_ids=observation_ids,
        )
        primary_observation_id = observation_ids[0] if observation_ids else None
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
              id, tenant_id, trigger_kind, trigger_subkind,
              observation_id, model_id, payload, scheduled_for, locked_by, locked_at
            ) VALUES (
              $1, $2, 'T1', 'event_batch',
              $3, NULL, $4::jsonb, now(), $5, now()
            )
            """,
            batch_id,
            tenant_id,
            primary_observation_id,
            json.dumps(payload, default=str),
            self.config.worker_id,
        )
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET batch_parent_id = $1
            WHERE id = ANY($2::uuid[])
              AND completed_at IS NULL
              AND batch_parent_id IS NULL
            """,
            batch_id,
            member_ids,
        )
        return {
            "id": batch_id,
            "tenant_id": tenant_id,
            "trigger_kind": "T1",
            "trigger_subkind": "event_batch",
            "observation_id": primary_observation_id,
            "model_id": None,
            "payload": payload,
            "attempts": 0,
        }

    async def _build_t1_batch_payload(
        self,
        conn: asyncpg.Connection,
        *,
        batch_id: UUID,
        members: list[asyncpg.Record],
        observation_ids: list[UUID],
    ) -> dict[str, Any]:
        rows = await conn.fetch(
            """
            SELECT id, occurred_at, source_channel, kind, trust_tier,
                   actor_id, content_text, entities_mentioned
            FROM observations
            WHERE id = ANY($1::uuid[])
            ORDER BY occurred_at ASC
            """,
            observation_ids,
        )
        seed_entities: list[dict[str, Any]] = []
        seen_entities: set[tuple[str, str]] = set()
        scope_actors: list[str] = []
        seen_actors: set[str] = set()
        signal_lines: list[str] = []
        earliest: datetime | None = None
        channels: set[str] = set()
        trust_tiers: set[str] = set()
        kinds: set[str] = set()
        for row in rows:
            occurred_at = row["occurred_at"]
            if earliest is None or occurred_at < earliest:
                earliest = occurred_at
            channels.add(str(row["source_channel"]))
            trust_tiers.add(str(row["trust_tier"]))
            kinds.add(str(row["kind"]))
            actor_id = row["actor_id"]
            if actor_id is not None and str(actor_id) not in seen_actors:
                seen_actors.add(str(actor_id))
                scope_actors.append(str(actor_id))
            entities = row["entities_mentioned"] or []
            if isinstance(entities, str):
                try:
                    entities = json.loads(entities)
                except json.JSONDecodeError:
                    entities = []
            if isinstance(entities, list):
                for entity in entities:
                    if not isinstance(entity, dict):
                        continue
                    etype = entity.get("type")
                    eid = entity.get("id")
                    if not etype or eid is None:
                        continue
                    key = (str(etype), str(eid))
                    if key in seen_entities:
                        continue
                    seen_entities.add(key)
                    seed_entities.append({"type": str(etype), "id": str(eid)})
            text = (row["content_text"] or "").strip()
            if text:
                compact = " ".join(text.split())
                if len(compact) > 280:
                    compact = compact[:277].rstrip() + "..."
                signal_lines.append(f"- {row['id']}: {compact}")
        summary = f"Batch of {len(observation_ids)} signals"
        if signal_lines:
            summary = f"{summary}:\n" + "\n".join(signal_lines)
        if len(summary) > 2000:
            summary = summary[:1997].rstrip() + "..."
        return {
            "trigger_id": str(batch_id),
            "batch": True,
            "batch_window_s": self.config.t1_batch_window_s,
            "batch_member_trigger_ids": [str(m["id"]) for m in members],
            "batch_observation_ids": [str(oid) for oid in observation_ids],
            "observation_ids": [str(oid) for oid in observation_ids],
            "member_trigger_ids": [str(m["id"]) for m in members],
            "source_channel": "batch",
            "kind": "signal_batch",
            "observation_kind": "signal_batch",
            "signal_type": "event_batch",
            "trust_tier": "mixed" if len(trust_tiers) != 1 else next(iter(trust_tiers)),
            "source_channels": sorted(channels),
            "observation_kinds": sorted(kinds),
            "trust_tiers": sorted(trust_tiers),
            "seed_occurred_at": earliest.isoformat() if earliest else None,
            "seed_natural_text": summary,
            "seed_entity_ids": seed_entities,
            "scope_actors": scope_actors,
        }

    async def _insert_downstream_batch_row(
        self,
        conn: asyncpg.Connection,
        *,
        tenant_id: UUID,
        trigger_kind: str,
        trigger_subkind: str | None,
        members: list[asyncpg.Record],
    ) -> dict[str, Any]:
        batch_id = uuid7()
        member_ids = [m["id"] for m in members]
        payload = await self._build_downstream_batch_payload(
            conn,
            batch_id=batch_id,
            trigger_kind=trigger_kind,
            trigger_subkind=trigger_subkind,
            members=members,
        )
        primary_model_id = None
        model_ids = _payload_uuid_list(payload, "model_ids")
        if model_ids:
            primary_model_id = model_ids[0]
        primary_observation_id = next(
            (m["observation_id"] for m in members if m["observation_id"]),
            None,
        )
        await conn.execute(
            """
            INSERT INTO think_trigger_queue (
              id, tenant_id, trigger_kind, trigger_subkind,
              observation_id, model_id, payload, scheduled_for, locked_by, locked_at
            ) VALUES (
              $1, $2, $3, $4,
              $5, $6, $7::jsonb, now(), $8, now()
            )
            """,
            batch_id,
            tenant_id,
            trigger_kind,
            trigger_subkind,
            primary_observation_id,
            primary_model_id,
            json.dumps(payload, default=str),
            self.config.worker_id,
        )
        await conn.execute(
            """
            UPDATE think_trigger_queue
            SET batch_parent_id = $1
            WHERE id = ANY($2::uuid[])
              AND completed_at IS NULL
              AND batch_parent_id IS NULL
            """,
            batch_id,
            member_ids,
        )
        return {
            "id": batch_id,
            "tenant_id": tenant_id,
            "trigger_kind": trigger_kind,
            "trigger_subkind": trigger_subkind,
            "observation_id": primary_observation_id,
            "model_id": primary_model_id,
            "payload": payload,
            "attempts": 0,
        }

    async def _build_downstream_batch_payload(
        self,
        conn: asyncpg.Connection,
        *,
        batch_id: UUID,
        trigger_kind: str,
        trigger_subkind: str | None,
        members: list[asyncpg.Record],
    ) -> dict[str, Any]:
        if trigger_kind == "T2" and trigger_subkind == "belief_updated":
            return await self._build_t2_batch_payload(
                conn,
                batch_id=batch_id,
                members=members,
            )
        if (
            trigger_kind == "T4"
            and trigger_subkind == "latent_relationship_candidate"
        ):
            return await self._build_t4_latent_batch_payload(
                conn,
                batch_id=batch_id,
                members=members,
            )
        return {
            "trigger_id": str(batch_id),
            "batch": True,
            "batch_kind": "downstream",
            "batch_member_trigger_ids": [str(m["id"]) for m in members],
            "member_trigger_ids": [str(m["id"]) for m in members],
        }

    async def _build_t2_batch_payload(
        self,
        conn: asyncpg.Connection,
        *,
        batch_id: UUID,
        members: list[asyncpg.Record],
    ) -> dict[str, Any]:
        model_ids = [
            m["model_id"] for m in members
            if m["model_id"] is not None
        ]
        model_ids = list(dict.fromkeys(model_ids))
        rows = await conn.fetch(
            """
            SELECT id, "natural", scope_actors, scope_entities
            FROM models
            WHERE id = ANY($1::uuid[])
            ORDER BY array_position($1::uuid[], id)
            """,
            model_ids,
        ) if model_ids else []
        scope_actors: list[str] = []
        seen_actors: set[str] = set()
        seed_entities: list[dict[str, Any]] = []
        seen_entities: set[tuple[str, str]] = set()
        lines: list[str] = []
        for row in rows:
            natural = (row["natural"] or "").strip()
            if natural:
                compact = " ".join(natural.split())
                if len(compact) > 220:
                    compact = compact[:217].rstrip() + "..."
                lines.append(f"- {row['id']}: {compact}")
            for actor_id in row["scope_actors"] or []:
                sid = str(actor_id)
                if sid not in seen_actors:
                    seen_actors.add(sid)
                    scope_actors.append(sid)
            raw_entities = row["scope_entities"] or []
            if isinstance(raw_entities, str):
                try:
                    raw_entities = json.loads(raw_entities)
                except json.JSONDecodeError:
                    raw_entities = []
            if isinstance(raw_entities, list):
                for entity in raw_entities:
                    if not isinstance(entity, dict):
                        continue
                    etype = entity.get("type")
                    eid = entity.get("id")
                    if not etype or eid is None:
                        continue
                    key = (str(etype), str(eid))
                    if key in seen_entities:
                        continue
                    seen_entities.add(key)
                    seed_entities.append({"type": str(etype), "id": str(eid)})

        observation_ids = [
            m["observation_id"] for m in members
            if m["observation_id"] is not None
        ]
        summary = f"Batch of {len(model_ids)} updated beliefs"
        if lines:
            summary = f"{summary}:\n" + "\n".join(lines)
        if len(summary) > 2000:
            summary = summary[:1997].rstrip() + "..."
        return {
            "trigger_id": str(batch_id),
            "batch": True,
            "batch_kind": "downstream",
            "batch_trigger_kind": "T2",
            "batch_trigger_subkind": "belief_updated",
            "batch_window_s": self.config.downstream_batch_window_s,
            "batch_member_trigger_ids": [str(m["id"]) for m in members],
            "member_trigger_ids": [str(m["id"]) for m in members],
            "batch_model_ids": [str(mid) for mid in model_ids],
            "model_ids": [str(mid) for mid in model_ids],
            "member_model_ids": [str(mid) for mid in model_ids],
            "source_observation_ids": [str(oid) for oid in observation_ids],
            "seed_natural_text": summary,
            "seed_entity_ids": seed_entities,
            "scope_actors": scope_actors,
        }

    async def _build_t4_latent_batch_payload(
        self,
        conn: asyncpg.Connection,
        *,
        batch_id: UUID,
        members: list[asyncpg.Record],
    ) -> dict[str, Any]:
        candidate_ids: list[UUID] = []
        member_model_ids: list[UUID] = []
        seen_members: set[UUID] = set()
        seed_lines: list[str] = []
        for member in members:
            payload = _payload_dict(member["payload"])
            cid_values = payload.get("relationship_candidate_ids")
            if not isinstance(cid_values, list):
                cid_values = [payload.get("relationship_candidate_id")]
            for cid in _coerce_uuid_list(cid_values):
                if cid not in candidate_ids:
                    candidate_ids.append(cid)
            for mid in _coerce_uuid_list(payload.get("member_model_ids") or []):
                if mid not in seen_members:
                    seen_members.add(mid)
                    member_model_ids.append(mid)
            seed_text = payload.get("seed_natural_text")
            if isinstance(seed_text, str) and seed_text.strip():
                compact = " ".join(seed_text.split())
                if len(compact) > 220:
                    compact = compact[:217].rstrip() + "..."
                seed_lines.append(f"- {member['id']}: {compact}")

        rows = await conn.fetch(
            """
            SELECT id, member_model_ids, source_model_id, target_model_id,
                   explanation
            FROM relationship_candidates
            WHERE id = ANY($1::uuid[])
            ORDER BY array_position($1::uuid[], id)
            """,
            candidate_ids,
        ) if candidate_ids else []
        for row in rows:
            for value in row["member_model_ids"] or []:
                if value not in seen_members:
                    seen_members.add(value)
                    member_model_ids.append(value)
            for key in ("source_model_id", "target_model_id"):
                value = row[key]
                if value is not None and value not in seen_members:
                    seen_members.add(value)
                    member_model_ids.append(value)
            explanation = (row["explanation"] or "").strip()
            if explanation:
                compact = " ".join(explanation.split())
                if len(compact) > 220:
                    compact = compact[:217].rstrip() + "..."
                seed_lines.append(f"- candidate {row['id']}: {compact}")

        summary = f"Batch of {len(candidate_ids)} latent relationship candidates"
        if seed_lines:
            summary = f"{summary}:\n" + "\n".join(seed_lines[:8])
        if len(summary) > 2000:
            summary = summary[:1997].rstrip() + "..."
        first_candidate = str(candidate_ids[0]) if candidate_ids else None
        return {
            "trigger_id": str(batch_id),
            "batch": True,
            "batch_kind": "downstream",
            "batch_trigger_kind": "T4",
            "batch_trigger_subkind": "latent_relationship_candidate",
            "batch_window_s": self.config.downstream_batch_window_s,
            "batch_member_trigger_ids": [str(m["id"]) for m in members],
            "member_trigger_ids": [str(m["id"]) for m in members],
            "relationship_candidate_id": first_candidate,
            "relationship_candidate_ids": [
                str(cid) for cid in candidate_ids
            ],
            "batch_relationship_candidate_ids": [
                str(cid) for cid in candidate_ids
            ],
            "member_model_ids": [str(mid) for mid in member_model_ids],
            "seed_natural_text": summary,
            "seed_signature": {
                "kind": "latent_relationship_candidate_batch",
                "candidate_count": len(candidate_ids),
            },
        }

    def _maybe_escalation_provider(
        self, payload: dict[str, Any]
    ) -> LLMProvider | None:
        """Cost-plan §2.4: when this dispatch is a validation retry (the payload
        carries `validation_feedback`, persisted by `_mark_trigger_failed`) and
        `THINK_ESCALATION_MODEL` names a different model, return a cached provider
        configured for that model so the retry escalates. Otherwise None (use the
        default provider)."""
        if self.llm_provider is None or not payload.get("validation_feedback"):
            return None
        model = _escalation_model_env()
        if not model or model == self.llm_provider.config.model:
            return None
        cached = self._escalation_provider
        if cached is not None and cached.config.model == model:
            return cached
        from dataclasses import replace
        self._escalation_provider = build_provider(
            replace(self.llm_provider.config, model=model)
        )
        _log.info(
            "think.validation_escalation",
            from_model=self.llm_provider.config.model,
            to_model=model,
        )
        return self._escalation_provider

    async def _tenant_over_daily_budget(self, tenant_id: UUID) -> bool:
        """Cost-plan §3.1: True when the tenant's today LLM spend (from
        `think_run_costs`) has reached `LLM_DAILY_BUDGET_USD_PER_TENANT`. Returns
        False immediately — no query — when enforcement is off or unset."""
        if not _daily_budget_enforcement_enabled():
            return False
        budget = _daily_budget_usd_per_tenant()
        if budget is None:
            return False
        async with self.pool.acquire() as conn:
            spend = await conn.fetchval(
                """
                SELECT COALESCE(SUM(llm_cost_usd), 0)::float8
                FROM think_run_costs
                WHERE tenant_id = $1
                  AND computed_at >= date_trunc('day', now() AT TIME ZONE 'UTC')
                """,
                tenant_id,
            )
        over = float(spend or 0.0) >= budget
        if over:
            _log.warning(
                "think.daily_budget_exceeded",
                tenant_id=str(tenant_id),
                spend_usd=float(spend or 0.0),
                budget_usd=budget,
            )
        return over

    async def _defer_trigger_for_budget(
        self, trigger_id: UUID, tenant_id: UUID
    ) -> None:
        """Release the lock and reschedule to the next UTC day. Attempts are not
        incremented (a pause, not a failure); the trigger is never dead-lettered."""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE think_trigger_queue
                SET locked_by = NULL,
                    locked_at = NULL,
                    scheduled_for = date_trunc('day', now() AT TIME ZONE 'UTC')
                        + interval '1 day'
                WHERE id = $1 AND locked_by = $2
                """,
                trigger_id,
                self.config.worker_id,
            )
        _log.info(
            "think.trigger_deferred_for_budget",
            trigger_id=str(trigger_id),
            tenant_id=str(tenant_id),
        )

    async def _dispatch_trigger(
        self, row: asyncpg.Record | dict[str, Any]
    ) -> None:
        tenant_id = row["tenant_id"]
        sem = self._semaphores.setdefault(
            tenant_id,
            asyncio.Semaphore(self.config.max_concurrency_per_tenant),
        )
        async with sem:
            await self._process_trigger(row)

    async def _process_trigger(self, row: asyncpg.Record) -> None:
        payload = _payload_dict(row["payload"])

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
        depth_bound = _max_inferential_lineage_depth(MAX_CASCADE_DEPTH)
        if cascade_depth >= depth_bound:
            _log.warning(
                "cascade_bound_violation",
                stage="trigger_rejected",
                trigger_id=str(row["id"]),
                trigger_kind=row["trigger_kind"],
                trigger_subkind=row["trigger_subkind"],
                tenant_id=str(row["tenant_id"]),
                cascade_depth=cascade_depth,
                max_cascade_depth=depth_bound,
            )
            # Non-retryable: mark the row terminal by pushing attempts
            # past the cap so `_mark_trigger_failed` completes it.
            await self._mark_trigger_failed(
                row["id"],
                f"cascade_bound_violation: depth={cascade_depth} >= {depth_bound}",
                force_terminal=True,
            )
            return

        # Cost-plan §3.1: per-tenant daily LLM-spend ceiling. On breach, pause
        # dispatch for this tenant — release the lock and defer to the next day
        # without incrementing attempts. Never dead-letters; resumes when the
        # budget window rolls over. No-op (and no query) unless enabled.
        if await self._tenant_over_daily_budget(row["tenant_id"]):
            await self._defer_trigger_for_budget(row["id"], row["tenant_id"])
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
        # Cost-plan §2.4: escalate to THINK_ESCALATION_MODEL on a validation
        # retry (payload carries validation_feedback); otherwise the default.
        run_provider = self._maybe_escalation_provider(payload) or self.llm_provider
        heartbeat = asyncio.create_task(self._heartbeat_trigger(row["id"]))
        try:
            call = think(
                trigger,
                self.pool,
                llm_provider=run_provider,
                embedder=self.embedder,
                trigger_kind_subkind=(
                    f"{row['trigger_kind']}:{row['trigger_subkind']}"
                    if row["trigger_subkind"] else row["trigger_kind"]
                ),
            )
            if self.config.run_timeout_s > 0:
                outcome = await asyncio.wait_for(
                    call,
                    timeout=self.config.run_timeout_s,
                )
            else:
                outcome = await call
        except asyncio.TimeoutError:
            error = (
                f"think_run_timeout after {self.config.run_timeout_s:.0f}s"
            )
            _log.warning(
                "think.worker.run_timeout",
                trigger_id=str(row["id"]),
                timeout_s=self.config.run_timeout_s,
            )
            try:
                from lib.llm.provider import close_codex_app_server_client
                await asyncio.wait_for(
                    close_codex_app_server_client(),
                    timeout=5.0,
                )
            except Exception:  # noqa: BLE001
                pass
            await self._mark_trigger_failed(row["id"], error)
            return
        except Exception as e:
            _log.exception(
                "think.worker.unhandled_failure",
                trigger_id=str(row["id"]),
                error=str(e),
            )
            await self._mark_trigger_failed(row["id"], str(e))
            return
        finally:
            heartbeat.cancel()
            try:
                await heartbeat
            except asyncio.CancelledError:
                pass

        if outcome.succeeded or outcome.skipped_idempotent:
            await self._mark_trigger_complete(
                row["id"], payload=payload
            )
            # POST_COMMIT_HOOK (OP-1): integrated. Post-commit actions are
            # now enqueued in `reason.py::_run_once` inside the apply
            # transaction (atomic with apply_diff) via
            # `services.reasoning.think.post_commit.enqueue_post_commit_actions`.
            # A separate worker process
            # (`services.reasoning.think.post_commit.post_commit_worker`) drains the
            # `pending_post_commit_actions` queue with FOR UPDATE SKIP
            # LOCKED dispatch, exponential backoff, and dead-letter after
            # MAX_ATTEMPTS=5. Nothing for the trigger-queue worker to do
            # here anymore; the queue row has already been written atomic
            # with the apply that produced its payload. Marker preserved
            # as documentation of the integration point.
        else:
            # Cost-plan §2.4: classify the failure so validation no-survivors
            # get their own (lower) retry cap and a feedback loop, instead of
            # the blind same-model resample up to trigger_max_attempts.
            failure_class = _classify_failure(outcome)
            await self._mark_trigger_failed(
                row["id"], outcome.error or "unknown",
                failure_class=failure_class,
                feedback=(
                    outcome.error if failure_class == "validation" else None
                ),
            )

    async def _heartbeat_trigger(self, trigger_id: UUID) -> None:
        interval = self.config.trigger_heartbeat_interval_s
        if interval <= 0:
            return
        while True:
            try:
                await asyncio.sleep(interval)
                async with self.pool.acquire() as conn:
                    await conn.execute(
                        """
                        UPDATE think_trigger_queue
                        SET locked_at = now()
                        WHERE id = $1
                          AND completed_at IS NULL
                          AND locked_by = $2
                        """,
                        trigger_id,
                        self.config.worker_id,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                _log.warning(
                    "think.worker.heartbeat_failed",
                    trigger_id=str(trigger_id),
                    error=str(e),
                )

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
                      AND locked_by = $2
                    """,
                    trigger_id,
                    self.config.worker_id,
                )
                member_ids = (
                    _payload_uuid_list(payload, "batch_member_trigger_ids")
                    if payload else []
                )
                if member_ids:
                    await conn.execute(
                        """
                        UPDATE think_trigger_queue
                        SET completed_at = now(),
                            locked_by = NULL,
                            locked_at = NULL
                        WHERE id = ANY($1::uuid[])
                          AND batch_parent_id = $2
                          AND completed_at IS NULL
                        """,
                        member_ids,
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
        failure_class: str | None = None,
        feedback: str | None = None,
    ) -> None:
        """Mark a trigger failed. `force_terminal=True` flags the row
        as non-retryable (used by TK-3 cascade-bound violations) and
        completes it immediately regardless of attempt count.

        Cost-plan §2.4: `failure_class='validation'` triggers (a) a
        validation-specific attempt cap (`THINK_VALIDATION_MAX_ATTEMPTS`, when
        set) so the worker stops blindly resampling, and (b) persisting
        `feedback` into the row payload as `validation_feedback`, which the
        retry's prompt appends (and which changes the prompt bytes, making the
        §2.2 response cache safe to reuse — interaction C5)."""
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT attempts, payload
                    FROM think_trigger_queue
                    WHERE id = $1
                      AND completed_at IS NULL
                      AND locked_by = $2
                    FOR UPDATE
                    """,
                    trigger_id,
                    self.config.worker_id,
                )
                if row is None:
                    return
                attempts = int(row["attempts"] or 0) + 1
                payload = _payload_dict(row["payload"])
                # Increment attempts; if past the limit (or forced), complete (dead letter).
                effective_max = self.config.trigger_max_attempts
                validation_cap = _validation_max_attempts_env()
                if failure_class == "validation" and validation_cap is not None:
                    effective_max = min(effective_max, validation_cap)
                terminal = force_terminal or attempts >= effective_max
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
                    member_ids = _payload_uuid_list(
                        payload, "batch_member_trigger_ids"
                    )
                    if member_ids:
                        # Cost-plan §2.3 C7: stamp `unbatched_from` so released
                        # members are excluded from re-batching. Without this a
                        # single failed N-member batch re-enqueues up to N members
                        # × trigger_max_attempts as fresh batch candidates — the
                        # dead-letter unbundle amplifier. Members run as singles.
                        await conn.execute(
                            """
                            UPDATE think_trigger_queue
                            SET batch_parent_id = NULL,
                                locked_by = NULL,
                                locked_at = NULL,
                                scheduled_for = now(),
                                payload = jsonb_set(
                                    payload,
                                    '{unbatched_from}',
                                    to_jsonb($2::text),
                                    true
                                )
                            WHERE id = ANY($1::uuid[])
                              AND batch_parent_id = $2
                              AND completed_at IS NULL
                            """,
                            member_ids,
                            trigger_id,
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
                elif (
                    feedback
                    and failure_class == "validation"
                    and validation_cap is not None
                ):
                    # Cost-plan §2.4: persist validator feedback for the retry's
                    # prompt (also alters the prompt bytes — C5 cache-safety).
                    await conn.execute(
                        """
                        UPDATE think_trigger_queue
                        SET attempts = $2,
                            locked_by = NULL,
                            locked_at = NULL,
                            scheduled_for = now() + ($3 || ' seconds')::interval,
                            payload = jsonb_set(
                                payload, '{validation_feedback}',
                                to_jsonb($4::text), true
                            )
                        WHERE id = $1
                        """,
                        trigger_id, attempts, str(backoff_seconds),
                        feedback[:2000],
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
            if self.config.tenant_filter is None:
                n = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM think_trigger_queue
                    WHERE completed_at IS NULL
                      AND batch_parent_id IS NULL
                    """
                )
            else:
                n = await conn.fetchval(
                    """
                    SELECT COUNT(*) FROM think_trigger_queue
                    WHERE completed_at IS NULL
                      AND batch_parent_id IS NULL
                      AND tenant_id = $1
                    """,
                    self.config.tenant_filter,
                )
            depth = int(n or 0)
            METRICS.set_queue_depth("all", depth)
            return depth


__all__ = ["ThinkWorker", "WorkerConfig"]
