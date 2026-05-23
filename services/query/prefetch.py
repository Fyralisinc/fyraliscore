"""
services/query/prefetch.py — pre-execute query-grid chip queries.

Agent-GRT recomputes the query grid periodically (and on triggers).
When the grid changes, we want every chip's response pre-computed so
the UI tap hits the cache instead of paying classify+retrieve+render
latency.

Flow:
  1. Agent-GRT calls `prefetch_query_grid(tenant_id, chips)` after a
     grid recompute.
  2. We run each chip's query through `QueryHandler.answer_query` in
     parallel, tagging each with the chip's `query_id`. The handler
     writes the response to `view_ceo_cache` under the key
     `query_prefetch:<query_id>`.
  3. On tap, the API layer calls `handler.try_serve_from_prefetch`
     which short-circuits to the cache.

Concurrency: we bound parallelism via an asyncio.Semaphore so a 6-chip
grid doesn't stampede the DB. Default 3 concurrent is plenty given
retrieval latency dominates.

Errors in one chip don't block others — each runs in its own task and
writes a `last_error` field into the cache payload on failure so the
API can still return something (even if degraded).
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import Iterable, Optional
from uuid import UUID

from .core import AnswerQueryRequest, QueryHandler

log = logging.getLogger(__name__)


@dataclass
class PrefetchChip:
    """One chip in the query grid.

    `query_id` is the stable id the UI sends back on tap; the response
    is cached under `query_prefetch:<query_id>`.

    `query_text` is the actual natural-language query that goes through
    the handler.

    `context_card_id` is optional: some chips are tied to a specific
    card (e.g. "Draft a reply to Marcus about Acme" might be the
    "Draft" verb on a card).
    """
    query_id: str
    query_text: str
    context_card_id: Optional[UUID] = None
    label: Optional[str] = None   # UI chip label for diagnostics


@dataclass
class PrefetchResult:
    query_id: str
    ok: bool
    latency_ms: int
    error: Optional[str] = None


@dataclass
class PrefetchReport:
    tenant_id: UUID
    total: int
    succeeded: int
    failed: int
    results: list[PrefetchResult] = field(default_factory=list)
    started_at: float = field(default_factory=time.time)
    finished_at: float = 0.0

    @property
    def duration_ms(self) -> int:
        if self.finished_at <= 0:
            return 0
        return int((self.finished_at - self.started_at) * 1000)


class QueryPrefetcher:
    """Runs prefetch for a set of chips.

    Not a singleton — Agent-GRT constructs one bound to a
    QueryHandler (which itself carries the cache adapter). Tests
    inject their own handler + adapter.
    """

    def __init__(
        self,
        handler: QueryHandler,
        *,
        max_concurrency: int = 3,
        per_chip_timeout_s: float = 10.0,
    ) -> None:
        self._handler = handler
        self._sem = asyncio.Semaphore(max_concurrency)
        self._per_chip_timeout_s = per_chip_timeout_s

    async def prefetch(
        self,
        tenant_id: UUID,
        chips: Iterable[PrefetchChip],
    ) -> PrefetchReport:
        chips_list = list(chips)
        report = PrefetchReport(
            tenant_id=tenant_id,
            total=len(chips_list),
            succeeded=0,
            failed=0,
        )
        if not chips_list:
            report.finished_at = time.time()
            return report

        tasks = [
            asyncio.create_task(self._run_chip(tenant_id, chip))
            for chip in chips_list
        ]
        done = await asyncio.gather(*tasks, return_exceptions=False)
        for res in done:
            report.results.append(res)
            if res.ok:
                report.succeeded += 1
            else:
                report.failed += 1
        report.finished_at = time.time()
        log.info(
            "query_prefetch_done tenant=%s total=%d ok=%d fail=%d duration_ms=%d",
            tenant_id, report.total, report.succeeded, report.failed,
            report.duration_ms,
        )
        return report

    async def _run_chip(
        self,
        tenant_id: UUID,
        chip: PrefetchChip,
    ) -> PrefetchResult:
        start = time.perf_counter()
        async with self._sem:
            try:
                req = AnswerQueryRequest(
                    tenant_id=tenant_id,
                    query=chip.query_text,
                    context_card_id=chip.context_card_id,
                    conversation_history=[],
                    query_id=chip.query_id,   # triggers cache write in core
                )
                # Wrap in a timeout so a pathological query can't block
                # the whole prefetch pass.
                await asyncio.wait_for(
                    self._handler.answer_query(req),
                    timeout=self._per_chip_timeout_s,
                )
                latency_ms = int((time.perf_counter() - start) * 1000)
                return PrefetchResult(
                    query_id=chip.query_id,
                    ok=True,
                    latency_ms=latency_ms,
                )
            except asyncio.TimeoutError:
                latency_ms = int((time.perf_counter() - start) * 1000)
                return PrefetchResult(
                    query_id=chip.query_id,
                    ok=False,
                    latency_ms=latency_ms,
                    error="timeout",
                )
            except Exception as exc:  # noqa: BLE001
                latency_ms = int((time.perf_counter() - start) * 1000)
                log.warning(
                    "prefetch_chip_failed query_id=%s error=%s",
                    chip.query_id, exc,
                )
                return PrefetchResult(
                    query_id=chip.query_id,
                    ok=False,
                    latency_ms=latency_ms,
                    error=str(exc),
                )


# ---------------------------------------------------------------------
# Module-level convenience
# ---------------------------------------------------------------------


async def prefetch_query_grid(
    handler: QueryHandler,
    tenant_id: UUID,
    chips: Iterable[PrefetchChip],
    *,
    max_concurrency: int = 3,
) -> PrefetchReport:
    """Convenience entry point for Agent-GRT. Constructs a prefetcher
    scoped to this call and runs it."""
    prefetcher = QueryPrefetcher(handler, max_concurrency=max_concurrency)
    return await prefetcher.prefetch(tenant_id, chips)


__all__ = [
    "PrefetchChip",
    "PrefetchResult",
    "PrefetchReport",
    "QueryPrefetcher",
    "prefetch_query_grid",
]
