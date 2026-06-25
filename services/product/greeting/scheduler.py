"""services/product/greeting/scheduler.py — Phase 3 + Phase 4.

Always-on worker that pre-computes greeting / cards / query_grid /
status / close_line and writes them to `view_ceo_cache`. Two refresh
paths:

* **Scheduled** — every `refresh_interval_seconds` (default 15 min,
  configurable per tenant) the full refresh fires.
* **Trigger-driven** — `LISTEN view_ceo_refresh` (Postgres NOTIFY) from
  the post-commit worker, plus a periodic poll of
  `pending_post_commit_actions` to catch actions whose NOTIFY was lost
  on worker restart.

Trigger predicates per COMPANY-OS-UI-BUILD-PLAN §3 Phase 3:
  * new Model with confidence > 0.7
  * Commitment transition (especially to blocked / doneverified)
  * Customer Resource health change
  * Anomaly flagged
  * Time-of-day boundary crossed (6am/10am/2pm/6pm/10pm tenant-local)

Phase 4 — staleness tracking: every refresh logs `latency_ms` and we
log a WARN when a cache key's age exceeds a per-key threshold at refresh
time (indicates the scheduler fell behind).
"""
from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Iterable, Literal
from uuid import UUID

import asyncpg

from lib.shared.db_leases import PostgresLease, default_lease_holder_id
from services.product.greeting.cache import CACHE_KEYS, ViewCeoCacheRepo
from services.product.greeting.rendering_adapter import (
    MockRenderingAdapter,
    RenderingAdapter,
    RenderedCard,
    RenderedCardReasoning,
    RenderedCloseLine,
    RenderedGreeting,
    RenderedQueryGrid,
    build_rendering_adapter,
)
from services.product.greeting.snapshot import (
    ConversationContext,
    FounderContext,
    QueryGridSnapshot,
    SnapshotComposer,
    SubstrateSnapshot,
)


log = logging.getLogger(__name__)

CardKind = Literal["observation", "decision", "question"]


# LISTEN channel: the post-commit worker (OP-1) can NOTIFY this channel
# with a JSON payload {"tenant_id": "<uuid>", "reason": "<str>"} to
# demand a refresh. If the payload is malformed we refresh all active
# tenants — conservative fallback.
VIEW_CEO_REFRESH_CHANNEL = "view_ceo_refresh"

# Staleness thresholds in seconds — WARN when we compute a refresh for
# a cache key that was already older than this. (The scheduler then
# recomputes it, so downstream readers always get fresh content; the
# warning signals the scheduler is falling behind.)
STALENESS_WARN_THRESHOLDS = {
    "greeting": 30 * 60,       # 30 min
    "cards": 5 * 60,           # 5 min on an active day
    "query_grid": 15 * 60,
    "status": 10 * 60,
    "close_line": 15 * 60,
}

# Time-of-day boundaries (tenant-local hours). Crossing these triggers
# a refresh even without a substrate change so the opener moves from
# 'morning' → 'afternoon' etc.
_TOD_BOUNDARY_HOURS = (6, 10, 14, 18, 22)


async def _bounded_render(
    sem: asyncio.Semaphore,
    *,
    label: str,
    render: Callable[[], Any],
    fallback: Callable[[], Any],
) -> Any:
    async with sem:
        try:
            return await render()
        except Exception as exc:  # noqa: BLE001 - partial refresh fallback
            log.warning(
                "grt.render_partial_failure",
                extra={"render_label": label, "error": str(exc)},
            )
            return await fallback()


# =====================================================================
# Listener
# =====================================================================


@dataclass
class _StreamPublisher:
    """Minimal protocol the stream.py manager satisfies. Decoupled here
    so tests can pass a stub without importing stream.py."""

    publish: Callable[[UUID, dict[str, Any]], "asyncio.Future | None"]


# =====================================================================
# Scheduler
# =====================================================================


@dataclass
class SchedulerConfig:
    refresh_interval_seconds: float = 15 * 60
    # Seconds between polls of `pending_post_commit_actions` for
    # trigger-driven refresh (also serves as the fallback when NOTIFY
    # misses).
    post_commit_poll_seconds: float = 30
    # Seconds between time-of-day boundary checks.
    tod_check_seconds: float = 60
    # Max concurrent rendering calls per tenant refresh.
    max_concurrent_renders: int = 6
    # Founder-context default (single-tenant dogfood).
    default_founder: FounderContext | None = None
    # Background loops are singleton across gateway replicas. Manual refreshes
    # stay available on every replica through the API surface.
    leader_election_enabled: bool = True
    leader_lease_name: str = "gateway:greeting_scheduler"
    leader_lease_ttl_seconds: float = 30.0
    leader_lease_refresh_seconds: float = 10.0
    leader_lease_retry_seconds: float = 2.0
    leader_lease_holder_id: str | None = None
    tenant_refresh_lease_enabled: bool = True
    tenant_refresh_lease_ttl_seconds: float = 5 * 60
    tenant_refresh_lease_prefix: str = "gateway:greeting_refresh"


@dataclass(frozen=True)
class _RefreshSnapshots:
    now: datetime
    greeting: SubstrateSnapshot
    query_grid: QueryGridSnapshot
    observation_cards: list[SubstrateSnapshot]
    decision_cards: list[SubstrateSnapshot]
    question_cards: list[SubstrateSnapshot]

    @property
    def card_focuses(self) -> list[SubstrateSnapshot]:
        return (
            list(self.observation_cards)
            + list(self.decision_cards)
            + list(self.question_cards)
        )


@dataclass(frozen=True)
class _RefreshRenderResults:
    greeting: RenderedGreeting
    close_line: RenderedCloseLine
    query_grid: RenderedQueryGrid
    cards: list[RenderedCard]
    card_reasoning: list[RenderedCardReasoning]


@dataclass(frozen=True)
class _RefreshPayloads:
    greeting: dict[str, Any]
    query_grid: dict[str, Any]
    cards: list[dict[str, Any]]
    status: dict[str, Any]
    close_line: dict[str, Any]


class _NoopLease:
    async def release(self) -> bool:
        return True


class GreetingScheduler:
    """One scheduler per process. Manages a set of tenant refreshes.

    Public surface:

      await scheduler.start()
      await scheduler.stop()
      await scheduler.refresh_tenant(tenant_id, reason='manual')
      scheduler.register_tenant(tenant_id, founder)
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
        *,
        cache: ViewCeoCacheRepo | None = None,
        composer: SnapshotComposer | None = None,
        rendering: RenderingAdapter | None = None,
        config: SchedulerConfig | None = None,
        stream_publisher: _StreamPublisher | None = None,
    ):
        self._pool = pool
        self._cache = cache or ViewCeoCacheRepo(pool)
        self._composer = composer or SnapshotComposer(pool)
        self._rendering = rendering or build_rendering_adapter()
        self._config = config or SchedulerConfig()
        self._publisher = stream_publisher

        # Tenant registry: tenant_id -> FounderContext
        self._tenants: dict[UUID, FounderContext] = {}
        # Last refresh timestamp per tenant for TOD-boundary detection.
        self._last_refresh_at: dict[UUID, datetime] = {}
        # Coalesce concurrent triggers per tenant.
        self._tenant_locks: dict[UUID, asyncio.Lock] = {}

        self._tasks: list[asyncio.Task] = []
        self._leader_task: asyncio.Task | None = None
        self._leader_lease: PostgresLease | None = None
        self._lease_holder_id = (
            self._config.leader_lease_holder_id
            or default_lease_holder_id("gateway:greeting_scheduler")
        )
        self._is_leader = False
        self._listen_conn: asyncpg.Connection | None = None
        self._stopped = asyncio.Event()
        self._started = False

    # -----------------------------------------------------------------
    # Tenant registry
    # -----------------------------------------------------------------
    def register_tenant(
        self,
        tenant_id: UUID,
        founder: FounderContext,
    ) -> None:
        self._tenants[tenant_id] = founder
        self._tenant_locks.setdefault(tenant_id, asyncio.Lock())

    def deregister_tenant(self, tenant_id: UUID) -> None:
        self._tenants.pop(tenant_id, None)
        self._tenant_locks.pop(tenant_id, None)
        self._last_refresh_at.pop(tenant_id, None)

    def set_stream_publisher(self, publisher: _StreamPublisher | None) -> None:
        self._publisher = publisher

    # -----------------------------------------------------------------
    # Lifecycle
    # -----------------------------------------------------------------
    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._stopped.clear()
        if self._config.leader_election_enabled:
            self._leader_task = asyncio.create_task(
                self._leader_supervisor_loop(),
                name="grt_leader",
            )
            return
        self._start_worker_tasks()

    async def _leader_supervisor_loop(self) -> None:
        lease = self._get_leader_lease()
        retry_s = max(0.1, float(self._config.leader_lease_retry_seconds))
        refresh_s = max(0.1, float(self._config.leader_lease_refresh_seconds))
        try:
            while not self._stopped.is_set():
                try:
                    acquired = await lease.acquire()
                    if not acquired:
                        await self._sleep_until_stopped(retry_s)
                        continue
                    self._is_leader = True
                    log.info(
                        "grt.leader_acquired",
                        extra={
                            "lease_name": lease.lease_name,
                            "holder_id": lease.holder_id,
                            "ttl_seconds": lease.ttl_seconds,
                        },
                    )
                    self._start_worker_tasks()
                    while not self._stopped.is_set():
                        await self._sleep_until_stopped(refresh_s)
                        if self._stopped.is_set():
                            break
                        if not await lease.refresh():
                            log.warning(
                                "grt.leader_lost",
                                extra={
                                    "lease_name": lease.lease_name,
                                    "holder_id": lease.holder_id,
                                },
                            )
                            break
                    await self._stop_worker_tasks()
                    self._is_leader = False
                    if self._stopped.is_set() and lease.is_held():
                        await lease.release()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("grt.leader_loop_error")
                    await self._stop_worker_tasks()
                    self._is_leader = False
                    await self._sleep_until_stopped(retry_s)
        finally:
            await self._stop_worker_tasks()
            self._is_leader = False
            if lease.is_held():
                with contextlib.suppress(Exception):
                    await lease.release()

    async def _sleep_until_stopped(self, timeout: float) -> None:
        try:
            await asyncio.wait_for(self._stopped.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass

    def _get_leader_lease(self) -> PostgresLease:
        if self._leader_lease is None:
            self._leader_lease = PostgresLease(
                self._pool,
                lease_name=self._config.leader_lease_name,
                holder_id=self._lease_holder_id,
                ttl_seconds=self._config.leader_lease_ttl_seconds,
                metadata={"component": "greeting_scheduler"},
            )
        return self._leader_lease

    def _start_worker_tasks(self) -> None:
        if self._tasks:
            return
        self._tasks = [
            asyncio.create_task(
                self._scheduled_refresh_loop(), name="grt_scheduled"
            ),
            asyncio.create_task(
                self._tod_boundary_loop(), name="grt_tod"
            ),
            asyncio.create_task(
                self._post_commit_listener_loop(), name="grt_listener"
            ),
            asyncio.create_task(
                self._post_commit_poll_loop(), name="grt_poll"
            ),
        ]

    async def _stop_worker_tasks(self) -> None:
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await t
        self._tasks.clear()
        if self._listen_conn is not None:
            with contextlib.suppress(Exception):
                await self._listen_conn.close()
            self._listen_conn = None

    def is_leader(self) -> bool:
        return self._is_leader

    async def stop(self) -> None:
        if self._started:
            self._stopped.set()
            if self._leader_task is not None:
                self._leader_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._leader_task
                self._leader_task = None
            await self._stop_worker_tasks()
            if self._leader_lease is not None and self._leader_lease.is_held():
                with contextlib.suppress(Exception):
                    await self._leader_lease.release()
            self._is_leader = False
            self._started = False
        close_rendering = getattr(self._rendering, "aclose", None)
        if close_rendering is not None:
            with contextlib.suppress(Exception):
                await close_rendering()

    # -----------------------------------------------------------------
    # Public refresh entry points
    # -----------------------------------------------------------------
    async def refresh_all_tenants(self, *, reason: str = "scheduled") -> None:
        for tenant_id in list(self._tenants):
            try:
                await self.refresh_tenant(tenant_id, reason=reason)
            except Exception as e:
                log.exception(
                    "grt.refresh_tenant_failed",
                    extra={"tenant_id": str(tenant_id), "error": str(e)},
                )

    async def refresh_tenant(
        self,
        tenant_id: UUID,
        *,
        reason: str = "scheduled",
    ) -> None:
        """Full refresh of all cache keys for one tenant.

        Coalesces concurrent calls per-tenant via an asyncio.Lock so
        overlapping triggers don't double-render.
        """
        founder = self._tenants.get(tenant_id)
        if founder is None:
            log.warning(
                "grt.refresh_unregistered_tenant",
                extra={"tenant_id": str(tenant_id)},
            )
            return

        lock = self._tenant_locks.setdefault(tenant_id, asyncio.Lock())
        async with lock:
            refresh_lease = await self._acquire_tenant_refresh_lease(
                tenant_id,
                reason=reason,
            )
            if refresh_lease is None:
                return
            started = _now_utc()
            try:
                # Inspect existing cache ages to drive Phase 4 staleness logs.
                prior = await self._cache.get_all(tenant_id)
                await self._refresh_tenant_inner(
                    tenant_id, founder, reason=reason, prior=prior
                )
                self._last_refresh_at[tenant_id] = started
                dur_ms = int((_now_utc() - started).total_seconds() * 1000)
                log.info(
                    "grt.refresh_ok",
                    extra={
                        "tenant_id": str(tenant_id),
                        "reason": reason,
                        "duration_ms": dur_ms,
                    },
                )
            finally:
                with contextlib.suppress(Exception):
                    await refresh_lease.release()

    async def _acquire_tenant_refresh_lease(
        self,
        tenant_id: UUID,
        *,
        reason: str,
    ) -> PostgresLease | _NoopLease | None:
        if not self._config.tenant_refresh_lease_enabled:
            return _NoopLease()
        lease = PostgresLease(
            self._pool,
            lease_name=(
                f"{self._config.tenant_refresh_lease_prefix}:{tenant_id}"
            ),
            holder_id=self._lease_holder_id,
            ttl_seconds=self._config.tenant_refresh_lease_ttl_seconds,
            metadata={
                "component": "greeting_scheduler",
                "tenant_id": str(tenant_id),
                "reason": reason,
            },
        )
        if await lease.acquire():
            return lease
        log.info(
            "grt.refresh_skipped_lease_held",
            extra={
                "tenant_id": str(tenant_id),
                "reason": reason,
                "lease_name": lease.lease_name,
            },
        )
        return None

    # -----------------------------------------------------------------
    # Inner refresh — compose snapshot, render, cache, publish.
    # -----------------------------------------------------------------
    async def _refresh_tenant_inner(
        self,
        tenant_id: UUID,
        founder: FounderContext,
        *,
        reason: str,
        prior: dict[str, Any],
    ) -> None:
        snapshots = await self._compose_refresh_snapshots(tenant_id)
        render_sem = asyncio.Semaphore(
            max(1, int(self._config.max_concurrent_renders))
        )
        fallback_rendering = MockRenderingAdapter()
        rendered = await self._render_refresh_sections(
            founder=founder,
            snapshots=snapshots,
            render_sem=render_sem,
            fallback_rendering=fallback_rendering,
        )
        payloads = self._build_refresh_payloads(snapshots, rendered)
        self._log_stale_prior(tenant_id, prior)
        await self._write_refresh_payloads(
            tenant_id,
            payloads,
            reason=reason,
        )
        await self._publish_refresh_payloads(tenant_id, payloads)

    async def _compose_refresh_snapshots(
        self,
        tenant_id: UUID,
    ) -> _RefreshSnapshots:
        now = _now_utc()
        greeting = await self._composer.compose_greeting_snapshot(
            tenant_id,
            now=now,
            conversation_context=ConversationContext(),
        )
        query_grid = await self._composer.compose_query_grid_snapshot(
            tenant_id,
            now=now,
        )
        observation_cards = await self._composer.compose_card_snapshot(
            tenant_id,
            "observation",
            now=now,
        )
        decision_cards = await self._composer.compose_card_snapshot(
            tenant_id,
            "decision",
            now=now,
        )
        question_cards = await self._composer.compose_card_snapshot(
            tenant_id,
            "question",
            now=now,
        )
        return _RefreshSnapshots(
            now=now,
            greeting=greeting,
            query_grid=query_grid,
            observation_cards=observation_cards,
            decision_cards=decision_cards,
            question_cards=question_cards,
        )

    async def _render_refresh_sections(
        self,
        *,
        founder: FounderContext,
        snapshots: _RefreshSnapshots,
        render_sem: asyncio.Semaphore,
        fallback_rendering: MockRenderingAdapter,
    ) -> _RefreshRenderResults:
        greeting_task = asyncio.create_task(
            _bounded_render(
                render_sem,
                label="greeting",
                render=lambda: self._rendering.render_greeting(
                    snapshots.greeting,
                    founder,
                ),
                fallback=lambda: fallback_rendering.render_greeting(
                    snapshots.greeting,
                    founder,
                ),
            )
        )
        close_task = asyncio.create_task(
            _bounded_render(
                render_sem,
                label="close_line",
                render=lambda: self._rendering.render_close_line(
                    snapshots.greeting,
                    founder,
                ),
                fallback=lambda: fallback_rendering.render_close_line(
                    snapshots.greeting,
                    founder,
                ),
            )
        )
        query_grid_task = asyncio.create_task(
            _bounded_render(
                render_sem,
                label="query_grid",
                render=lambda: self._rendering.render_query_grid(
                    snapshots.query_grid,
                    founder,
                ),
                fallback=lambda: fallback_rendering.render_query_grid(
                    snapshots.query_grid,
                    founder,
                ),
            )
        )
        cards = await self._render_cards(
            founder=founder,
            snapshots=snapshots,
            render_sem=render_sem,
            fallback_rendering=fallback_rendering,
        )
        card_reasoning = await self._render_card_reasoning(
            founder=founder,
            cards=cards,
            card_focuses=snapshots.card_focuses,
            render_sem=render_sem,
            fallback_rendering=fallback_rendering,
        )
        return _RefreshRenderResults(
            greeting=await greeting_task,
            close_line=await close_task,
            query_grid=await query_grid_task,
            cards=cards,
            card_reasoning=card_reasoning,
        )

    async def _render_cards(
        self,
        *,
        founder: FounderContext,
        snapshots: _RefreshSnapshots,
        render_sem: asyncio.Semaphore,
        fallback_rendering: MockRenderingAdapter,
    ) -> list[RenderedCard]:
        tasks: list[asyncio.Task] = []
        card_groups: tuple[tuple[CardKind, list[SubstrateSnapshot]], ...] = (
            ("observation", snapshots.observation_cards),
            ("decision", snapshots.decision_cards),
            ("question", snapshots.question_cards),
        )

        def render_call(
            snap: SubstrateSnapshot,
            kind: CardKind,
        ) -> Callable[[], Any]:
            async def _call() -> RenderedCard:
                return await self._rendering.render_card(snap, founder, kind)

            return _call

        def fallback_call(
            snap: SubstrateSnapshot,
            kind: CardKind,
        ) -> Callable[[], Any]:
            async def _call() -> RenderedCard:
                return await fallback_rendering.render_card(snap, founder, kind)

            return _call

        for kind, focus_snaps in card_groups:
            for snap in focus_snaps:
                tasks.append(
                    asyncio.create_task(
                        _bounded_render(
                            render_sem,
                            label=f"card:{kind}",
                            render=render_call(snap, kind),
                            fallback=fallback_call(snap, kind),
                        )
                    )
                )
        return await asyncio.gather(*tasks) if tasks else []

    async def _render_card_reasoning(
        self,
        *,
        founder: FounderContext,
        cards: list[RenderedCard],
        card_focuses: list[SubstrateSnapshot],
        render_sem: asyncio.Semaphore,
        fallback_rendering: MockRenderingAdapter,
    ) -> list[RenderedCardReasoning]:
        tasks: list[asyncio.Task] = []

        def render_call(
            card: RenderedCard,
            focus_snap: SubstrateSnapshot,
            evidence_refs: list[dict[str, Any]],
        ) -> Callable[[], Any]:
            async def _call() -> RenderedCardReasoning:
                return await self._rendering.render_card_reasoning(
                    focus_snap,
                    founder,
                    card.kind,
                    card_subject=_card_subject_label(card),
                    card_body_context=card.body_html,
                    supporting_evidence=evidence_refs,
                )

            return _call

        def fallback_call(
            card: RenderedCard,
            focus_snap: SubstrateSnapshot,
            evidence_refs: list[dict[str, Any]],
        ) -> Callable[[], Any]:
            async def _call() -> RenderedCardReasoning:
                return await fallback_rendering.render_card_reasoning(
                    focus_snap,
                    founder,
                    card.kind,
                    card_subject=_card_subject_label(card),
                    card_body_context=card.body_html,
                    supporting_evidence=evidence_refs,
                )

            return _call

        for card, focus_snap in zip(cards, card_focuses):
            evidence_refs = _gather_card_evidence(card, focus_snap)
            tasks.append(
                asyncio.create_task(
                    _bounded_render(
                        render_sem,
                        label=f"card_reasoning:{card.kind}",
                        render=render_call(card, focus_snap, evidence_refs),
                        fallback=fallback_call(card, focus_snap, evidence_refs),
                    )
                )
            )
        return await asyncio.gather(*tasks) if tasks else []

    def _build_refresh_payloads(
        self,
        snapshots: _RefreshSnapshots,
        rendered: _RefreshRenderResults,
    ) -> _RefreshPayloads:
        now = snapshots.now
        greeting_payload = {
            "meta": {
                "date_iso": now.date().isoformat(),
                "recomputed_at": now.isoformat(),
                "signals_watched_count": rendered.greeting.signals_watched_count,
            },
            "body_html": rendered.greeting.body_html,
            "cached_at": now.isoformat(),
        }
        query_grid_payload = {
            "queries": rendered.query_grid.queries,
            "cached_at": now.isoformat(),
        }
        return _RefreshPayloads(
            greeting=greeting_payload,
            query_grid=query_grid_payload,
            cards=self._build_cards_payload(rendered.cards, rendered.card_reasoning, now),
            status={
                "substrate_alive": True,
                "calibration_pct": rendered.close_line.calibration_pct,
                "needs_you_count": _needs_you_count(snapshots.greeting),
            },
            close_line={
                "body": rendered.close_line.body,
                "metadata": {
                    "signal_count": rendered.close_line.signal_count,
                    "external_moves": rendered.close_line.external_moves,
                    "calibration_pct": rendered.close_line.calibration_pct,
                },
            },
        )

    def _build_cards_payload(
        self,
        cards: list[RenderedCard],
        reasoning_results: list[RenderedCardReasoning],
        now: datetime,
    ) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for i, card in enumerate(cards):
            if i < len(reasoning_results):
                reasoning = reasoning_results[i]
                reasoning_html = reasoning.reasoning_html or card.reasoning_html
                evidence = reasoning.evidence or card.evidence
            else:
                reasoning_html = card.reasoning_html
                evidence = card.evidence
            payload.append(
                {
                    "id": card.id,
                    "kind": card.kind,
                    "tag_color": card.tag_color,
                    "tag_label": card.tag_label,
                    "meta": card.meta,
                    "body_html": card.body_html,
                    "expanded": {
                        "reasoning_html": reasoning_html,
                        "evidence": evidence,
                        "verbs": card.verbs,
                    },
                    "cached_at": now.isoformat(),
                }
            )
        return payload

    def _log_stale_prior(
        self,
        tenant_id: UUID,
        prior: dict[str, Any],
    ) -> None:
        for key in CACHE_KEYS + ("close_line",):
            if key not in prior:
                continue
            age = prior[key].staleness_seconds
            threshold = STALENESS_WARN_THRESHOLDS.get(key)
            if threshold is not None and age > threshold:
                log.warning(
                    "grt.cache_stale_at_refresh",
                    extra={
                        "tenant_id": str(tenant_id),
                        "cache_key": key,
                        "age_seconds": int(age),
                        "threshold_seconds": threshold,
                    },
                )

    async def _write_refresh_payloads(
        self,
        tenant_id: UUID,
        payloads: _RefreshPayloads,
        *,
        reason: str,
    ) -> None:
        writes: list[tuple[str, dict[str, Any]]] = [
            ("greeting", payloads.greeting),
            ("query_grid", payloads.query_grid),
            ("cards", {"cards": payloads.cards}),
            ("status", payloads.status),
            ("close_line", payloads.close_line),
        ]
        for key, payload in writes:
            await self._cache.set_cached(
                tenant_id,
                key,
                payload,
                reason=reason,
            )

    async def _publish_refresh_payloads(
        self,
        tenant_id: UUID,
        payloads: _RefreshPayloads,
    ) -> None:
        if self._publisher is None:
            return
        await self._publish_many(
            tenant_id,
            [
                {
                    "type": "greeting_updated",
                    "greeting": {
                        **payloads.greeting,
                        "staleness_seconds": 0,
                    },
                },
                {
                    "type": "query_grid_updated",
                    "query_grid": {**payloads.query_grid},
                },
                {"type": "cards_updated", "cards": payloads.cards},
                {"type": "status_updated", "status": payloads.status},
            ],
        )

    async def _publish_many(
        self,
        tenant_id: UUID,
        messages: Iterable[dict[str, Any]],
    ) -> None:
        if self._publisher is None:
            return
        for msg in messages:
            try:
                result = self._publisher.publish(tenant_id, msg)
                if asyncio.iscoroutine(result) or asyncio.isfuture(result):
                    await result
            except Exception as e:
                log.warning(
                    "grt.publish_failed",
                    extra={"tenant_id": str(tenant_id), "error": str(e)},
                )

    # -----------------------------------------------------------------
    # Loops — scheduled / TOD / post-commit LISTEN / post-commit poll
    # -----------------------------------------------------------------
    async def _scheduled_refresh_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                await self.refresh_all_tenants(reason="scheduled")
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=self._config.refresh_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("grt.scheduled_loop_crashed")
            raise

    async def _tod_boundary_loop(self) -> None:
        """Fire a refresh when tenant-local clock crosses a TOD boundary
        hour. Coarse: compares the prior-refresh hour-of-day against the
        current one, in the tenant's tz. Re-fires at most once per
        boundary per tenant per 24h via the `_last_refresh_at` map.
        """
        try:
            while not self._stopped.is_set():
                try:
                    await self._tod_tick()
                except Exception:
                    log.exception("grt.tod_tick_failed")
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=self._config.tod_check_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise

    async def _tod_tick(self) -> None:
        now = _now_utc()
        for tenant_id, founder in list(self._tenants.items()):
            try:
                tenant_now = _in_tenant_tz(now, founder.timezone_name)
            except Exception:
                tenant_now = now
            last = self._last_refresh_at.get(tenant_id)
            if last is None:
                # Defer to the scheduled loop's first refresh.
                continue
            try:
                last_local = _in_tenant_tz(last, founder.timezone_name)
            except Exception:
                last_local = last
            if _crossed_boundary(last_local, tenant_now):
                await self.refresh_tenant(tenant_id, reason="trigger_fired")

    async def _post_commit_listener_loop(self) -> None:
        """LISTEN/NOTIFY subscriber. The post-commit worker may NOTIFY
        `view_ceo_refresh` with a JSON `{tenant_id, reason}` payload
        when an interesting action lands. This loop is best-effort —
        the poll loop is the durable fallback.
        """
        try:
            while not self._stopped.is_set():
                try:
                    conn = await self._pool.acquire()
                    self._listen_conn = conn
                    try:
                        await conn.add_listener(
                            VIEW_CEO_REFRESH_CHANNEL,
                            self._on_notify,
                        )
                        # Park until stopped — notifications fire the
                        # callback out-of-band.
                        try:
                            await asyncio.wait_for(
                                self._stopped.wait(),
                                timeout=None,
                            )
                        except asyncio.TimeoutError:
                            pass
                    finally:
                        with contextlib.suppress(Exception):
                            await conn.remove_listener(
                                VIEW_CEO_REFRESH_CHANNEL,
                                self._on_notify,
                            )
                        with contextlib.suppress(Exception):
                            await self._pool.release(conn)
                        self._listen_conn = None
                except Exception:
                    log.exception("grt.listener_crashed_retrying")
                    # Back off briefly before reopening.
                    try:
                        await asyncio.wait_for(self._stopped.wait(), timeout=5.0)
                    except asyncio.TimeoutError:
                        pass
        except asyncio.CancelledError:
            raise

    def _on_notify(
        self,
        connection: asyncpg.Connection,
        pid: int,
        channel: str,
        payload: str,
    ) -> None:
        # Parse. If bad, log and refresh all tenants conservatively.
        try:
            data = json.loads(payload or "{}")
        except json.JSONDecodeError:
            data = {}
        tenant_raw = data.get("tenant_id")
        reason = data.get("reason") or "trigger_fired"
        if tenant_raw is None:
            asyncio.create_task(self.refresh_all_tenants(reason=reason))
            return
        try:
            tenant_id = UUID(str(tenant_raw))
        except (ValueError, TypeError):
            log.warning(
                "grt.notify_bad_tenant",
                extra={"payload": payload},
            )
            return
        asyncio.create_task(self.refresh_tenant(tenant_id, reason=reason))

    async def _post_commit_poll_loop(self) -> None:
        """Poll `pending_post_commit_actions` for rows that landed since
        our last check, per tenant. Debounce by only peeking at the
        newest `created_at` per tenant. This is the durable fallback for
        LISTEN/NOTIFY losses.

        Triggers we care about (mapped from action_kind):
          * publish_anomalies       → refresh (anomaly flagged)
          * schedule_predictions    → no-op (no user-visible impact)
          * broadcast_realtime      → refresh (substrate state change)
          * invalidate_metrics      → refresh (calibration / health change)
        """
        high_water: dict[UUID, datetime] = {}
        RELEVANT = {"publish_anomalies", "broadcast_realtime", "invalidate_metrics"}
        try:
            while not self._stopped.is_set():
                try:
                    async with self._pool.acquire() as conn:
                        for tenant_id in list(self._tenants):
                            since = high_water.get(tenant_id) or (
                                _now_utc() - timedelta(minutes=10)
                            )
                            rows = await conn.fetch(
                                """
                                SELECT id, action_kind, created_at
                                FROM pending_post_commit_actions
                                WHERE tenant_id = $1
                                  AND created_at > $2
                                  AND action_kind = ANY($3::text[])
                                ORDER BY created_at ASC
                                LIMIT 100
                                """,
                                tenant_id,
                                since,
                                list(RELEVANT),
                            )
                            if rows:
                                high_water[tenant_id] = rows[-1]["created_at"]
                                asyncio.create_task(
                                    self.refresh_tenant(
                                        tenant_id,
                                        reason="trigger_fired",
                                    )
                                )
                except Exception:
                    log.exception("grt.post_commit_poll_failed")
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=self._config.post_commit_poll_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise


# =====================================================================
# Utilities
# =====================================================================


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _card_subject_label(card: Any) -> str:
    """Short subject label for the reasoning prompt. Pulls from
    `card.tag_label` (e.g., 'Observation \u00b7 revenue at risk') stripped
    of the tag prefix, falling back to a generic string."""
    tag = getattr(card, "tag_label", "") or ""
    # Strip the "Observation \u00b7 " / "Decision \u00b7 " prefix if present.
    for sep in (" \u00b7 ", " - ", ": "):
        if sep in tag:
            parts = tag.split(sep, 1)
            if len(parts) == 2 and parts[1].strip():
                return parts[1].strip()
    return tag.strip() or getattr(card, "kind", "") or "this situation"


def _gather_card_evidence(
    card: Any,
    focus_snap: SubstrateSnapshot,
) -> list[dict[str, Any]]:
    """Assemble `supporting_evidence` dict rows from the pinned card
    snapshot's `recent_state_changes` + salient ModelRef / CommitmentRef
    timestamps. Used by the scheduler to feed RND's card-reasoning
    endpoint.

    Returns up to 6 rows, oldest-first, so the rendered evidence list
    reads chronologically.
    """
    rows: list[dict[str, Any]] = []
    # State changes — most concrete signal.
    for sc in focus_snap.recent_state_changes[:4]:
        rows.append(
            {
                "actor": (sc.metadata or {}).get("actor") or sc.kind,
                "channel": (sc.metadata or {}).get("channel") or sc.entity_kind,
                "t": sc.occurred_at,
                "excerpt": (
                    f"{sc.kind.replace('_', ' ')} on "
                    f"{sc.entity_kind or 'entity'}"
                ),
                "cite_id": f"obs-{str(sc.observation_id)[:8]}",
                "kind": "state_change",
            }
        )
    # Top Model shift, if any.
    for m in focus_snap.top_models[:2]:
        if m.last_state_change_at is None:
            continue
        rows.append(
            {
                "actor": "system",
                "channel": "think",
                "t": m.last_state_change_at,
                "excerpt": (
                    f"{m.natural} (confidence "
                    f"{m.confidence_at_assertion:.2f} \u2192 "
                    f"{m.confidence:.2f})"
                ),
                "cite_id": f"m-{str(m.id)[:8]}",
                "kind": "update",
            }
        )
    # Top anomaly.
    for a in focus_snap.anomalies[:1]:
        rows.append(
            {
                "actor": "anomaly detector",
                "channel": "think",
                "t": a.published_at,
                "excerpt": f"{a.kind} (significance {a.significance:.2f})",
                "cite_id": f"a-{str(a.id)[:8]}",
                "kind": a.kind,
            }
        )
    rows.sort(key=lambda r: r.get("t") or datetime.min.replace(tzinfo=timezone.utc))
    return rows[:6]


def _needs_you_count(snap: SubstrateSnapshot) -> int:
    """Heuristic: blocked commitments + critical-path at-risk +
    anomalies with significance >= 0.7."""
    count = 0
    for com in snap.active_commitments:
        if com.state == "blocked":
            count += 1
        elif com.is_critical_path and com.days_to_due is not None and com.days_to_due <= 3:
            count += 1
    for a in snap.anomalies:
        if a.significance >= 0.7:
            count += 1
    return count


def _in_tenant_tz(ts: datetime, tz_name: str) -> datetime:
    """Convert a UTC timestamp to a tenant-local naive hour view. Uses
    zoneinfo when available; falls back to UTC on lookup failure.
    """
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    try:
        from zoneinfo import ZoneInfo

        return ts.astimezone(ZoneInfo(tz_name))
    except Exception:
        return ts.astimezone(timezone.utc)


def _crossed_boundary(prev: datetime, curr: datetime) -> bool:
    """True if curr's time-of-day moved past a boundary hour since prev.

    Boundaries: 06:00, 10:00, 14:00, 18:00, 22:00 local time.
    """
    if prev.date() != curr.date():
        return True
    prev_hour = prev.hour + prev.minute / 60.0
    curr_hour = curr.hour + curr.minute / 60.0
    for boundary in _TOD_BOUNDARY_HOURS:
        if prev_hour < boundary <= curr_hour:
            return True
    return False


__all__ = [
    "GreetingScheduler",
    "SchedulerConfig",
    "VIEW_CEO_REFRESH_CHANNEL",
    "STALENESS_WARN_THRESHOLDS",
]
