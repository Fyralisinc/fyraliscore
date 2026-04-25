"""services/greeting — Agent-GRT: Company OS CEO view pre-compute layer.

Owns the background worker that pre-computes greeting + cards + query
grid + status and writes them to `view_ceo_cache`, plus the HTTP
endpoint (`GET /view/ceo/home`) and WebSocket stream
(`WS /view/ceo/stream`) that serve the UI.

Module map (COMPANY-OS-UI-BUILD-PLAN.md §3):

  cache.py       Phase 1 — Postgres JSONB cache accessors.
  snapshot.py    Phase 2 — SubstrateSnapshot composer; reads Models /
                 Commitments / Resources / state_changes / anomalies.
  scheduler.py   Phase 3 — 15-min loop + LISTEN-based trigger-driven
                 invalidation off `pending_post_commit_actions`.
  stream.py      Phase 5 — ViewCeoStreamManager + FastAPI WS endpoint.
  api.py         Phase 6 — FastAPI routes for GET /view/ceo/home and
                 POST /view/ceo/force-refresh.
  rendering_adapter.py
                 HTTP adapter into services/rendering/ with a
                 deterministic mock for pre-integration development.

Contracts: CONTRACTS.md §1.1 (HTTP shape), §1.4 (WS message shapes),
§2.1 (calls into services/rendering/), §2.3 (SubstrateSnapshot), §3
(cache DDL).
"""

from services.greeting.cache import (
    CachedContent,
    ViewCeoCacheRepo,
    CACHE_KEYS,
)
from services.greeting.snapshot import (
    SubstrateSnapshot,
    FounderContext,
    ConversationContext,
    QueryGridSnapshot,
    SnapshotComposer,
)
from services.greeting.scheduler import GreetingScheduler
from services.greeting.stream import ViewCeoStreamManager
from services.greeting.rendering_adapter import (
    RenderingAdapter,
    MockRenderingAdapter,
)

__all__ = [
    "CachedContent",
    "ViewCeoCacheRepo",
    "CACHE_KEYS",
    "SubstrateSnapshot",
    "FounderContext",
    "ConversationContext",
    "QueryGridSnapshot",
    "SnapshotComposer",
    "GreetingScheduler",
    "ViewCeoStreamManager",
    "RenderingAdapter",
    "MockRenderingAdapter",
]
