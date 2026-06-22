"""WhatsApp reconciliation placeholder.

WhatsApp ingestion is live-webhook only in this repo. Backfill reconciliation is
deferred, but the reconciler registry still needs a per-source module so pool
registration stays uniform across every dispatch source.
"""
from __future__ import annotations

from typing import Any

import asyncpg

from services.ingest.ingestion.reconcilers import (
    RECONCILER_DISPATCH,
    ReconciliationDecision,
)


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.whatsapp: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


async def reconcile_whatsapp(
    shards: list[asyncpg.Record],
    run: asyncpg.Record,
) -> ReconciliationDecision:
    return ReconciliationDecision(
        has_gaps=False,
        message=(
            "WhatsApp backfill reconciliation is deferred; live webhook "
            "ingestion is treated as gap-free for now."
        ),
    )


RECONCILER_DISPATCH["whatsapp"] = reconcile_whatsapp


__all__ = ["reconcile_whatsapp", "set_pool_provider"]
