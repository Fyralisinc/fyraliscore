"""Pool-registration seam for deferred WhatsApp backfill reconciliation.

WhatsApp is live-ingress only today, so the dispatch table retains its explicit
default-clean compatibility reconciler. This module exists solely so the
shared reconciler workers can register their pool without a source-specific
startup exception before the Phase 3 backfill capability is implemented.
"""

from __future__ import annotations

from typing import Any


_pool_provider: Any = None


def set_pool_provider(provider: Any) -> None:
    global _pool_provider
    _pool_provider = provider


def _get_pool() -> Any:
    if _pool_provider is None:
        raise RuntimeError(
            "reconcilers.whatsapp: pool provider not registered. "
            "Call set_pool_provider(pool) at service startup."
        )
    return _pool_provider


__all__ = ["set_pool_provider"]
