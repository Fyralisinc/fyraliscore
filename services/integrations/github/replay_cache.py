"""services/integrations/github/replay_cache.py — in-process replay LRU.

Defense-in-depth (FR-014, Clarifications Q4): rejects re-deliveries of
the same `(installation_id, X-GitHub-Delivery UUID)` within a 5-minute
window. Cache failures (any internal exception) MUST be swallowed and
logged — observation-layer `(source_channel, external_id)` dedup is
the correctness backstop.

Construction mirrors `services/webhooks/tenant_resolver.py::InstallationCache`
(Constitution §X — copy the bar of existing primitives).
"""
from __future__ import annotations

import time
from collections import OrderedDict
from dataclasses import dataclass


_DEFAULT_MAX_ENTRIES = 4096
_DEFAULT_TTL_SECONDS = 300.0  # 5 minutes


@dataclass(slots=True)
class _Entry:
    inserted_at: float


class ReplayCache:
    """TTL LRU keyed by `(installation_id, delivery_id)` → insertion time.

    Public methods:
      seen(installation_id, delivery_id, now=None) -> bool
        Returns True if the key was inserted within the TTL window
        (idempotent insert-and-check). On internal failure (defensive
        catch-all), increments `_bypass_count` and returns False so the
        caller proceeds without replay protection.

      bypass_count -> int  (test/observability helper)
    """

    def __init__(
        self,
        *,
        max_entries: int = _DEFAULT_MAX_ENTRIES,
        ttl_seconds: float = _DEFAULT_TTL_SECONDS,
    ) -> None:
        if max_entries <= 0:
            raise ValueError("max_entries must be > 0")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be > 0")
        self._max_entries = max_entries
        self._ttl_seconds = ttl_seconds
        self._entries: OrderedDict[tuple[str, str], _Entry] = OrderedDict()
        self._bypass_count = 0

    def seen(
        self,
        installation_id: str | None,
        delivery_id: str | None,
        now: float | None = None,
    ) -> bool:
        """Check-and-insert. Returns True if the key was seen within
        TTL; otherwise inserts and returns False.

        Missing installation_id OR delivery_id → bypass (returns False,
        increments bypass_count).
        """
        try:
            if not installation_id or not delivery_id:
                self._bypass_count += 1
                return False
            key = (str(installation_id), str(delivery_id))
            current = now if now is not None else time.monotonic()
            existing = self._entries.get(key)
            if existing is not None:
                if existing.inserted_at + self._ttl_seconds > current:
                    # Hit within TTL — move to MRU and report hit.
                    self._entries.move_to_end(key)
                    return True
                # Expired — fall through and re-insert.
            self._entries[key] = _Entry(inserted_at=current)
            self._entries.move_to_end(key)
            while len(self._entries) > self._max_entries:
                self._entries.popitem(last=False)
            return False
        except Exception:  # noqa: BLE001 — defense-in-depth, never block
            self._bypass_count += 1
            return False

    @property
    def bypass_count(self) -> int:
        return self._bypass_count

    @property
    def size(self) -> int:
        return len(self._entries)


def make_replay_cache(
    *,
    max_entries: int = _DEFAULT_MAX_ENTRIES,
    ttl_seconds: float = _DEFAULT_TTL_SECONDS,
) -> ReplayCache:
    """Factory matching the IN-08 `build_*` convention. The gateway
    lifespan creates one instance and stores it on
    `request.app.state.github_replay_cache`.
    """
    return ReplayCache(max_entries=max_entries, ttl_seconds=ttl_seconds)


__all__ = ["ReplayCache", "make_replay_cache"]
