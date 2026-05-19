"""Per-source page fetchers. Per ingestion LLD §3.1 + §3.

============================================================
ROLE
============================================================
A fetcher is a per-source function that takes one `(install,
shard_identifier, cursor)` triple and returns one page of records
plus the next cursor (or end-of-data). M6.2a's ShardFetch service
calls these fetchers in a loop, advancing the cursor under the N1
invariant (`state.advance_cursor_atomic_with_kafka_publish`) after
each page.

Contract — codified by M6.2a Phase 2:

    FETCHER_DISPATCH[source](
        install: asyncpg.Record,
        shard_identifier: dict[str, Any],
        cursor: dict[str, Any] | None,
    ) -> FetchResult

Where `FetchResult = (records, next_cursor, end_of_data)`. The
cursor is OPAQUE to ShardFetch — the per-source fetcher owns its
schema. M6.3-M6.6 will each ship a Pydantic model for their
respective cursor shape; M6.2a treats the cursor as `dict | None`.

============================================================
DISPATCH TABLE — STUB ON M6.2a, REAL ON M6.3-M6.6
============================================================
Same shape + same fail-loud pattern as
[services.ingestion.planners.PLANNER_DISPATCH](../planners/__init__.py).
M6.2a Phase 2 ships `NotImplementedError` stubs for every source.
Each per-source M6.x sub-block replaces ONE stub:

    | source   | shard_kind anchor (per A15) | M6.x sub-block |
    |----------|------------------------------|----------------|
    | gmail    | "gmail_mailbox_window"       | M6.3           |
    | github   | "github_repo_events"         | M6.4           |
    | slack    | "slack_channel_window"       | M6.5           |
    | discord  | "discord_channel_window"     | M6.6           |

The dispatch key is `shard.source` (the column in `onboarding_shards`),
NOT `shard_kind` — multiple shard_kinds for the same source share a
fetcher today. If a future per-source sub-block needs to dispatch on
shard_kind too, this dispatch table is the place to do it (extend
to `dict[tuple[str, str], Fetcher]` or use a router).

============================================================
FETCH_RESULT CONTRACT (LOAD-BEARING for N1)
============================================================
The fetcher MUST return:
  - `records`: list of dicts. Each will be serialized into one
    Kafka message on `ingestion.raw`. Empty list is acceptable
    (rate-limited page; no records this round).
  - `next_cursor`: dict | None. The opaque next-page cursor (None
    means "no more pages — use end_of_data=True if this is the
    actual terminal state").
  - `end_of_data`: bool. True signals "stop the fetch loop; mark
    shard 'done'." If False, the loop continues with `next_cursor`.

The fetcher's exceptions are caught by the service:
  - `NotImplementedError` → shard marked 'failed' with the stub
    message; failure is the pre-M6.3 expected steady state.
  - Any other exception → shard marked 'failed' with the exception
    string in `last_error`. The fetcher is responsible for its own
    retry logic (M6.3-M6.6 will use `services.ingestion.workflows.retry`
    helpers for source-specific rate-limit + 5xx retries).

Tests inject test fetchers via `FETCHER_DISPATCH[source] = <test_fn>`
(or `monkeypatch.setitem(FETCHER_DISPATCH, ...)`).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

import asyncpg


# ---------------------------------------------------------------------
# Public types.
# ---------------------------------------------------------------------
@dataclass(frozen=True)
class FetchResult:
    """One page of fetched records + the cursor for the next page.

    `records` are dicts ready to serialize into Kafka messages on the
    `ingestion.raw` topic (LLD §4 envelope). The fetcher owns the
    record shape; M6.2a's ShardFetch treats it as opaque.

    `next_cursor` is dict | None. Per-source fetchers own the cursor
    schema (M6.3-M6.6 will ship per-source Pydantic models). M6.2a
    treats it as opaque and writes it to
    `workflow_states.state_data["cursor"]` per the N1 primitive.

    `end_of_data` is the terminal signal. ShardFetch's fetch loop
    exits when this is True (regardless of next_cursor value).
    """

    records: list[dict[str, Any]] = field(default_factory=list)
    next_cursor: dict[str, Any] | None = None
    end_of_data: bool = False


# `Fetcher` type alias for the dispatch table.
Fetcher = Callable[
    [asyncpg.Record, dict[str, Any], dict[str, Any] | None],
    Awaitable[FetchResult],
]


# ---------------------------------------------------------------------
# NotImplementedError stub factory.
# ---------------------------------------------------------------------
def _not_implemented_fetcher(source: str, milestone: str) -> Fetcher:
    """Build a fetcher stub that raises `NotImplementedError` loudly.

    Same shape as `planners._not_implemented_planner`. The error
    message names the responsible M6.x sub-block so operators
    reading `onboarding_shards.last_error` (or service logs)
    immediately know where the implementation work lives.
    """
    async def stub(
        install: asyncpg.Record,
        shard_identifier: dict[str, Any],
        cursor: dict[str, Any] | None,
    ) -> FetchResult:
        raise NotImplementedError(
            f"Fetcher for source={source!r} is not yet implemented. "
            f"Pending in {milestone} per "
            f"docs/ingestion/04-implementation-plan.md §{milestone}. "
            f"Until then, onboarding_shards for this source will fail "
            f"with this reason; the failure is the expected "
            f"pre-{milestone} steady state, not a regression."
        )
    stub.__name__ = f"_not_implemented_fetcher_{source}"
    return stub


# ---------------------------------------------------------------------
# Dispatch table — M6.2a Phase 2.
# ---------------------------------------------------------------------
# Module-level mutable dict by design: the public API. Tests rebind
# entries via `FETCHER_DISPATCH[source] = test_fn`. Production
# replacements (M6.3-M6.6) overwrite the stub at module-import time.
FETCHER_DISPATCH: dict[str, Fetcher] = {
    "gmail":   _not_implemented_fetcher("gmail",   "M6.3"),
    "github":  _not_implemented_fetcher("github",  "M6.4"),
    "slack":   _not_implemented_fetcher("slack",   "M6.5"),
    "discord": _not_implemented_fetcher("discord", "M6.6"),
}


__all__ = [
    "FETCHER_DISPATCH",
    "FetchResult",
    "Fetcher",
]


# Per-source modules import below — each assigns into FETCHER_DISPATCH
# at module-load time (per A18).
from services.ingestion.fetchers import gmail as _gmail  # noqa: E402,F401
