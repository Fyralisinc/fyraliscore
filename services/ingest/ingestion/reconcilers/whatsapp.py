"""WhatsApp reconciler registration seam.

WhatsApp is currently live-only, so the dispatch table intentionally keeps its
default-clean reconciler until backfill ships.  Reconciler processes still
import every dispatch source at startup to register a shared pool; exposing the
same setter as the implemented modules keeps that registry total without
pretending WhatsApp has a gap-detection algorithm.
"""
from __future__ import annotations

from typing import Any


_pool_provider: Any = None


def set_pool_provider(pool: Any) -> None:
    """Register the workflow pool for interface parity with other sources."""
    global _pool_provider
    _pool_provider = pool


def _get_pool() -> Any:
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.whatsapp: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


__all__ = ["set_pool_provider"]
