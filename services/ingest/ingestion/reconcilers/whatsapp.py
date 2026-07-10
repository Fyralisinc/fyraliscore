"""WhatsApp live-only reconciler shim.

WhatsApp currently has no backfill/poll reconciler; the dispatch entry remains
the default-clean stub from ``reconcilers.__init__``. This module exists so the
shared pool-provider registration can cover every dispatch source uniformly.
"""
from __future__ import annotations

from typing import Any


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool():  # noqa: ANN202
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.whatsapp: pool provider not registered."
        )
    return _pool_provider


__all__ = ["set_pool_provider"]
