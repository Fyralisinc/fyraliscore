"""services/workers/anomaly_processor/worker.py — poll loop.

BUILD-PLAN §5 Prompt 4.B. Per-cycle flow:

1. Poll observations_new (or, in this implementation, a simple
   cursor-based poll of observations since the last iteration).
2. For each fresh tenant detected in the batch, run all six detectors
   in parallel (asyncio.gather).
3. For each candidate:
   - Compute significance.
   - If >= SIGNIFICANCE_THRESHOLD AND NOT debounced → enqueue T3.
   - If debounced → append to the existing `think_anomalies_raw` row.
   - Else → record sub-threshold signal to signal_memory_fabric.
4. Every Nth cycle, sweep signal_memory_fabric and promote any region
   with > 5 accumulated signals in 7 days. Promoted candidates get
   T3-enqueued without further debounce.
5. Rate-limit T3 enqueues per tenant (token bucket).

Decisions (noted in BUILD-LOG deviations):
- (a) We poll rather than LISTEN. Polling gives us control over
  batch sizing and per-tenant fairness without a dedicated LISTEN
  process. LISTEN can be added later by wiring `events.notify_scope`
  into the loop head; nothing in the code path assumes polling.
- (b) Rate limit is per-tenant (token bucket of N tokens per minute).
  Global rate-limit would starve small tenants when a big tenant
  bursts.
- (c) Debounce reuses `think_anomalies_raw` (Wave 3-B's table).
- (d) T3 enqueue payload ships `region_spec` AND `seed_entity_ids`
  so the Wave 3 worker's `_populate_seed_fields` hydrates correctly.
"""
from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import dataclass
from datetime import timedelta
from uuid import UUID

import asyncpg
import structlog

from lib.shared.ids import uuid7

from .debounce import (
    compute_region_hash,
    decide_debounce,
)
from .detectors import (
    AnomalyCandidate,
    detect_activation_decay_anomaly,
    detect_commitment_drift,
    detect_contestation_cluster,
    detect_external_signal_anomaly,
    detect_resource_overcommit,
    detect_silent_disagreement,
)
from .memory_fabric import (
    list_unpromoted_region_hashes,
    promote_if_accumulated,
    record_subthreshold_signal,
)
from .significance import SIGNIFICANCE_THRESHOLD, compute_significance


_log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------


@dataclass
class AnomalyProcessorConfig:
    poll_interval_s: float = 30.0
    contestation_window_minutes: int = 30
    silent_disagreement_window_days: int = 7
    external_signal_window_minutes: int = 60
    commitment_drift_window_days: int = 28
    debounce_window_minutes: int = 30
    promote_every_n_cycles: int = 10
    t3_budget_per_tenant_per_min: int = 20

    @classmethod
    def from_env(cls) -> "AnomalyProcessorConfig":
        return cls(
            poll_interval_s=float(os.environ.get(
                "ANOMALY_POLL_INTERVAL_S", 30.0
            )),
            t3_budget_per_tenant_per_min=int(os.environ.get(
                "ANOMALY_T3_BUDGET_PER_MIN", 20
            )),
            promote_every_n_cycles=int(os.environ.get(
                "ANOMALY_PROMOTE_EVERY_N_CYCLES", 10
            )),
        )


# ---------------------------------------------------------------------
# Rate limiter (token bucket, per-tenant)
# ---------------------------------------------------------------------


@dataclass
class _Bucket:
    tokens: int
    window_started_at: float


class TenantRateLimiter:
    """
    Simple token bucket with a per-minute refill. Not strictly sliding
    window (window resets every 60s from the first successful
    acquisition) — sufficient for a coarse rate limit that protects
    Think from anomaly-processor bursts.
    """

    def __init__(self, tokens_per_minute: int) -> None:
        self.tokens_per_minute = tokens_per_minute
        self._buckets: dict[UUID, _Bucket] = {}

    def try_acquire(self, tenant_id: UUID) -> bool:
        """Return True if the tenant has budget; consume one token."""
        now = time.monotonic()
        bucket = self._buckets.get(tenant_id)
        if bucket is None or (now - bucket.window_started_at) >= 60.0:
            self._buckets[tenant_id] = _Bucket(
                tokens=self.tokens_per_minute - 1,
                window_started_at=now,
            )
            return True
        if bucket.tokens <= 0:
            return False
        bucket.tokens -= 1
        return True


# ---------------------------------------------------------------------
# T3 enqueue
# ---------------------------------------------------------------------


async def enqueue_t3_trigger(
    candidate: AnomalyCandidate,
    final_significance: float,
    conn: asyncpg.Connection,
    *,
    escalates_anomaly_id: UUID | None = None,
) -> UUID:
    """
    INSERT a T3 row into `think_trigger_queue`. Payload shape:

        {
          "anomaly_kind": str,
          "anomaly_significance": float,
          "region_spec": {"entity_ids": [...]},
          "seed_entity_ids": [...],
          "triggering_observation_ids": [...],
          "candidate_payload": {...},
          "escalates": uuid | None,
        }

    `region_spec` and `seed_entity_ids` are both populated — the Wave 3
    worker's `_populate_seed_fields` hydrates BOTH onto the
    TriggerContext. The region_spec dict shape mirrors what Wave 3-B's
    `TriggerContext.region_spec` expects.

    `escalates_anomaly_id` is set only when this T3 is a
    debounce-escalation of a prior in-window anomaly (per
    ARCHITECTURE-REVIEW-1 §I5 drop-or-escalate semantics).
    """
    entity_ids = candidate.region_entity_ids
    payload = {
        "anomaly_kind": candidate.kind,
        "anomaly_significance": float(final_significance),
        "region_spec": {"entity_ids": entity_ids},
        "seed_entity_ids": entity_ids,
        "triggering_observation_ids": [
            str(o) for o in candidate.triggering_observation_ids
        ],
        "candidate_payload": candidate.payload,
        "escalates": str(escalates_anomaly_id) if escalates_anomaly_id else None,
    }

    trigger_id = uuid7()
    await conn.execute(
        """
        INSERT INTO think_trigger_queue
          (id, tenant_id, trigger_kind, trigger_subkind,
           observation_id, model_id, payload)
        VALUES ($1, $2, 'T3', $3, NULL, $4, $5::jsonb)
        """,
        trigger_id,
        candidate.tenant_id,
        candidate.kind,
        candidate.entity_id if candidate.entity_type == "model" else None,
        json.dumps(payload, default=str),
    )
    return trigger_id


async def write_anomaly_raw(
    candidate: AnomalyCandidate,
    region_hash: str,
    final_significance: float,
    think_run_id: UUID,
    conn: asyncpg.Connection,
) -> UUID:
    """
    Write the anomaly into `think_anomalies_raw` so debounce catches it
    on the next cycle. `think_run_id` is synthetic for anomalies
    produced by the processor (not by Think).
    """
    row_id = uuid7()
    region = {
        "region_hash": region_hash,
        "entity_ids": candidate.region_entity_ids,
    }
    trig_op = {
        "source": "anomaly_processor",
        "triggering_observation_ids": [
            str(o) for o in candidate.triggering_observation_ids
        ],
        "payload": candidate.payload,
    }
    await conn.execute(
        """
        INSERT INTO think_anomalies_raw
          (id, tenant_id, think_run_id, kind, region, significance, triggering_op)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb)
        """,
        row_id,
        candidate.tenant_id,
        think_run_id,
        candidate.kind,
        json.dumps(region, default=str),
        float(final_significance),
        json.dumps(trig_op, default=str),
    )
    return row_id


# ---------------------------------------------------------------------
# AnomalyProcessor
# ---------------------------------------------------------------------


class AnomalyProcessor:
    """
    Poll-loop orchestrator.

    Construction does not start the loop. Call `run(stop_event)` to
    start, or `process_once(tenants)` to do a single cycle (tests do
    the latter).
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        config: AnomalyProcessorConfig | None = None,
    ) -> None:
        self.pool = pool
        self.config = config or AnomalyProcessorConfig.from_env()
        self.rate_limiter = TenantRateLimiter(
            self.config.t3_budget_per_tenant_per_min
        )
        self._cycle_count = 0

    # -----------------------------------------------------------------
    # Single-cycle entry point (for tests and for the scheduler loop)
    # -----------------------------------------------------------------
    async def process_once(
        self,
        tenant_ids: list[UUID],
        *,
        force_promote: bool = False,
    ) -> dict[str, int]:
        """
        Run one cycle over the supplied tenants. Returns a small
        counters dict for observability / tests.

        Counters: detected, enqueued_t3, debounced, subthreshold,
                  promoted, rate_limited.
        """
        counters = {
            "detected": 0,
            "enqueued_t3": 0,
            "debounced": 0,       # kept for back-compat; now ≡ suppressed
            "suppressed": 0,      # new: drop-or-escalate decision = drop
            "escalated": 0,       # new: drop-or-escalate decision = escalate
            "subthreshold": 0,
            "promoted": 0,
            "rate_limited": 0,
        }

        for tenant_id in tenant_ids:
            async with self.pool.acquire() as conn:
                async with conn.transaction():
                    await self._process_tenant(conn, tenant_id, counters)

        # Periodically promote.
        self._cycle_count += 1
        if force_promote or (
            self.config.promote_every_n_cycles > 0
            and self._cycle_count % self.config.promote_every_n_cycles == 0
        ):
            for tenant_id in tenant_ids:
                async with self.pool.acquire() as conn:
                    async with conn.transaction():
                        promoted_count = await self._sweep_promotions(
                            conn, tenant_id
                        )
                        counters["promoted"] += promoted_count
                        counters["enqueued_t3"] += promoted_count

        return counters

    async def _process_tenant(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
        counters: dict[str, int],
    ) -> None:
        # Run all detectors in parallel.
        contestation_window = timedelta(
            minutes=self.config.contestation_window_minutes
        )
        silent_window = timedelta(
            days=self.config.silent_disagreement_window_days
        )
        external_window = timedelta(
            minutes=self.config.external_signal_window_minutes
        )
        commit_window = timedelta(
            days=self.config.commitment_drift_window_days
        )

        # asyncpg connections do NOT support concurrent operations
        # (InterfaceError: "another operation is in progress"). Run
        # detectors sequentially on the shared connection. Parallelism
        # across detectors requires one connection per detector, which
        # isn't worth the pool pressure for a 30s-cadence worker.
        all_candidates: list[AnomalyCandidate] = []
        all_candidates.extend(
            await detect_contestation_cluster(tenant_id, contestation_window, conn)
        )
        all_candidates.extend(
            await detect_silent_disagreement(tenant_id, silent_window, conn)
        )
        all_candidates.extend(
            await detect_activation_decay_anomaly(tenant_id, conn)
        )
        all_candidates.extend(
            await detect_external_signal_anomaly(tenant_id, external_window, conn)
        )
        all_candidates.extend(
            await detect_commitment_drift(tenant_id, commit_window, conn)
        )
        all_candidates.extend(
            await detect_resource_overcommit(tenant_id, conn)
        )

        counters["detected"] += len(all_candidates)

        for candidate in all_candidates:
            region_hash = compute_region_hash(
                candidate.tenant_id,
                candidate.kind,
                candidate.region_entity_ids,
            )
            final_sig = await compute_significance(candidate, conn)

            _log.info(
                "anomaly.detected",
                tenant_id=str(tenant_id),
                kind=candidate.kind,
                entity_id=str(candidate.entity_id),
                region_hash=region_hash,
                significance=final_sig,
            )

            if final_sig < SIGNIFICANCE_THRESHOLD:
                # Sub-threshold → Memory Fabric.
                await record_subthreshold_signal(
                    candidate, region_hash, final_sig, conn
                )
                counters["subthreshold"] += 1
                continue

            # Above threshold — drop-or-escalate decision per
            # ARCHITECTURE-REVIEW-1 §I5. Never mutates prior anomalies.
            decision = await decide_debounce(
                region_hash=region_hash,
                tenant_id=candidate.tenant_id,
                kind=candidate.kind,
                new_significance=final_sig,
                within=timedelta(minutes=self.config.debounce_window_minutes),
                conn=conn,
            )

            if decision.action == "suppress":
                counters["suppressed"] += 1
                counters["debounced"] += 1  # back-compat
                _log.info(
                    "anomaly.debounced_suppressed",
                    tenant_id=str(tenant_id),
                    kind=candidate.kind,
                    prior_anomaly_id=str(decision.prior_anomaly_id),
                    new_significance=final_sig,
                    prior_significance=decision.prior_significance,
                )
                continue

            # publish_new OR escalate — both paths enqueue a fresh T3
            # and write a fresh anomaly_raw row. Rate limit applies to
            # both.
            if not self.rate_limiter.try_acquire(candidate.tenant_id):
                await record_subthreshold_signal(
                    candidate, region_hash, final_sig, conn
                )
                counters["rate_limited"] += 1
                _log.warning(
                    "anomaly.rate_limited",
                    tenant_id=str(tenant_id),
                    kind=candidate.kind,
                )
                continue

            think_run_id = uuid7()
            escalates_id = (
                decision.prior_anomaly_id
                if decision.action == "escalate"
                else None
            )
            await enqueue_t3_trigger(
                candidate, final_sig, conn,
                escalates_anomaly_id=escalates_id,
            )
            await write_anomaly_raw(
                candidate, region_hash, final_sig, think_run_id, conn
            )
            counters["enqueued_t3"] += 1
            if decision.action == "escalate":
                counters["escalated"] += 1
                _log.info(
                    "anomaly.debounced_escalated",
                    tenant_id=str(tenant_id),
                    kind=candidate.kind,
                    significance=final_sig,
                    prior_significance=decision.prior_significance,
                    prior_anomaly_id=str(decision.prior_anomaly_id),
                )
            else:
                _log.info(
                    "anomaly.enqueued_t3",
                    tenant_id=str(tenant_id),
                    kind=candidate.kind,
                    significance=final_sig,
                    entity_ids_count=len(candidate.region_entity_ids),
                )

    async def _sweep_promotions(
        self,
        conn: asyncpg.Connection,
        tenant_id: UUID,
    ) -> int:
        """
        For each distinct region_hash with unpromoted rows for this
        tenant, try promotion. Returns the number of promotions.
        """
        region_hashes = await list_unpromoted_region_hashes(tenant_id, conn)
        promoted = 0
        for region_hash in region_hashes:
            candidate = await promote_if_accumulated(
                tenant_id, region_hash, conn
            )
            if candidate is None:
                continue
            # Promoted candidates bypass the rate limiter — they've
            # already waited via accumulation. But we still debounce
            # against think_anomalies_raw to avoid enqueueing a T3 for
            # a region that Think just emitted an anomaly into. Use the
            # same drop-or-escalate decision as the live path.
            decision = await decide_debounce(
                region_hash=region_hash,
                tenant_id=candidate.tenant_id,
                kind=candidate.kind,
                new_significance=candidate.significance,
                within=timedelta(minutes=self.config.debounce_window_minutes),
                conn=conn,
            )
            if decision.action == "suppress":
                continue
            think_run_id = uuid7()
            escalates_id = (
                decision.prior_anomaly_id
                if decision.action == "escalate"
                else None
            )
            await enqueue_t3_trigger(
                candidate, candidate.significance, conn,
                escalates_anomaly_id=escalates_id,
            )
            await write_anomaly_raw(
                candidate,
                region_hash,
                candidate.significance,
                think_run_id,
                conn,
            )
            promoted += 1
            _log.info(
                "anomaly.promoted_from_fabric",
                tenant_id=str(tenant_id),
                kind=candidate.kind,
                region_hash=region_hash,
                accumulated_count=candidate.payload.get("accumulated_count"),
            )
        return promoted

    # -----------------------------------------------------------------
    # Long-running poll loop
    # -----------------------------------------------------------------
    async def run(self, stop_event: asyncio.Event) -> None:
        _log.info("anomaly.worker.started")
        while not stop_event.is_set():
            try:
                tenants = await self._list_active_tenants()
                if tenants:
                    await self.process_once(tenants)
            except Exception as e:  # pragma: no cover
                _log.exception("anomaly.worker.loop_error", error=str(e))
            try:
                await asyncio.wait_for(
                    stop_event.wait(),
                    timeout=self.config.poll_interval_s,
                )
                break
            except asyncio.TimeoutError:
                continue
        _log.info("anomaly.worker.stopped")

    async def _list_active_tenants(self) -> list[UUID]:
        """
        Distinct tenant_ids we've seen in the last hour. We union
        across observations, models, commitments, resources so we
        don't miss a tenant whose only activity is elsewhere.
        """
        async with self.pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT DISTINCT tenant_id FROM (
                    SELECT tenant_id FROM observations
                      WHERE occurred_at >= now() - interval '1 hour'
                    UNION
                    SELECT tenant_id FROM models WHERE status = 'active'
                    UNION
                    SELECT tenant_id FROM commitments
                      WHERE state NOT IN ('doneverified', 'closed')
                    UNION
                    SELECT tenant_id FROM resources WHERE archived_at IS NULL
                ) t
                """
            )
            return [r["tenant_id"] for r in rows]


__all__ = [
    "AnomalyProcessor",
    "AnomalyProcessorConfig",
    "TenantRateLimiter",
    "enqueue_t3_trigger",
    "write_anomaly_raw",
]
