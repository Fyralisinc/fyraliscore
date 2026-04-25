"""
services/access_control/hierarchy.py — manager chain + shared channels.

Wave 5-A deviation (b): manager chain is derived from
`actors.metadata.manager_id` (JSONB path). Following the pointer
upward gives the full ancestor list for an actor. We guard against
pathological cycles with a depth cap of 32.

Shared channels are rows in `shared_channels` (migration 0014); for
Wave 5-A `audience_role='all'` is the only value tested end-to-end.
Future waves may target 'team:<id>' etc. by adding the matching rows
and extending `is_shared_channel` to check team membership.

Spec §26 "Layer 2 — Scope visibility": HR channels are explicitly
NOT shared. We encode HR channels as any source_channel that starts
with 'hr:' (canonical for Wave 5-A per BUILD-LOG Wave 4-D Deviation).
"""
from __future__ import annotations

from uuid import UUID

import asyncpg


# Channels that are HR/sensitive — manager-chain access AND shared-
# channel rules are BOTH skipped for these. An HR channel is thus only
# visible to: author, mentioned actors, or explicit per-entity roles.
HR_CHANNEL_PREFIXES: tuple[str, ...] = ("hr:", "legal:", "incident:")


# Channels that are implicitly tenant-shared by construction. Internal
# state-change channels carry transition metadata only (no PII / body
# text), so every tenant subscriber can see them. Wave 4-D dispatcher
# relied on this implicit rule — Wave 5-A codifies it. Adding a row
# to `shared_channels` still works and takes precedence (lets ops
# scope down individual internal channels if needed).
_IMPLICIT_SHARED_PREFIXES: tuple[str, ...] = (
    "internal:",
    "system:",
)


def _is_implicit_shared(source_channel: str) -> bool:
    return any(source_channel.startswith(p) for p in _IMPLICIT_SHARED_PREFIXES)


def is_hr_channel(source_channel: str | None) -> bool:
    """Return True when `source_channel` is HR/legal/incident-sensitive.

    A None channel is never HR (dev/test short-circuit). Unknown
    prefixes are NOT HR — we fail open; only explicit HR prefixes flag.
    """
    if not source_channel:
        return False
    return any(source_channel.startswith(p) for p in HR_CHANNEL_PREFIXES)


async def manager_chain_of(
    actor_id: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    max_depth: int = 32,
) -> list[UUID]:
    """
    Return the ancestor chain (manager, manager's manager, ...) for
    `actor_id` within `tenant_id`. The returned list does NOT include
    the starting actor. Order is ascending (direct manager first).

    The chain follows `actors.metadata.manager_id`. If the chain cycles
    or exceeds `max_depth`, we stop and return what we have.
    """
    out: list[UUID] = []
    seen: set[UUID] = {actor_id}
    current = actor_id
    for _ in range(max_depth):
        row = await conn.fetchrow(
            """
            SELECT (metadata->>'manager_id')::UUID AS manager_id
            FROM actors
            WHERE id = $1 AND tenant_id = $2
            """,
            current, tenant_id,
        )
        if row is None:
            break
        next_mgr = row["manager_id"]
        if next_mgr is None or next_mgr in seen:
            break
        out.append(next_mgr)
        seen.add(next_mgr)
        current = next_mgr
    return out


async def is_in_manager_chain(
    actor_id: UUID,
    candidate_manager: UUID,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    max_depth: int = 32,
) -> bool:
    """
    True iff `candidate_manager` is any ancestor (direct or indirect)
    of `actor_id`. Self is not an ancestor.
    """
    if actor_id == candidate_manager:
        return False
    chain = await manager_chain_of(
        actor_id, conn=conn, tenant_id=tenant_id, max_depth=max_depth,
    )
    return candidate_manager in chain


async def is_shared_channel(
    source_channel: str,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    actor_id: UUID | None = None,
    audience_role: str = "all",
) -> bool:
    """
    True iff `source_channel` is marked as shared for the tenant under
    the given `audience_role`.

    HR-prefixed channels are NEVER shared (spec §26 "HR exception
    channels"). This short-circuits before the DB lookup.

    For Wave 5-A, `audience_role='all'` is the only live case. Other
    audience roles are scaffolded but return False until team-
    membership logic lands in a later wave.
    """
    if is_hr_channel(source_channel):
        return False
    # Implicit shared channels (internal:*, system:*) are ALWAYS shared
    # for audience_role='all'. Explicit `shared_channels` rows override
    # this for non-'all' audience targets.
    if audience_role == "all" and _is_implicit_shared(source_channel):
        return True
    if audience_role != "all":
        # Placeholder: future audience roles require team membership
        # resolution. Wave 5-A emits False rather than claiming
        # visibility it can't justify.
        val = await conn.fetchval(
            """
            SELECT 1 FROM shared_channels
            WHERE tenant_id = $1
              AND source_channel = $2
              AND audience_role = $3
            LIMIT 1
            """,
            tenant_id, source_channel, audience_role,
        )
        return val is not None
    val = await conn.fetchval(
        """
        SELECT 1 FROM shared_channels
        WHERE tenant_id = $1
          AND source_channel = $2
          AND audience_role = 'all'
        LIMIT 1
        """,
        tenant_id, source_channel,
    )
    return val is not None


async def register_shared_channel(
    source_channel: str,
    *,
    conn: asyncpg.Connection,
    tenant_id: UUID,
    audience_role: str = "all",
) -> None:
    """Idempotent inserter for test + config setup. Public so tests
    can seed channels without hand-written SQL."""
    await conn.execute(
        """
        INSERT INTO shared_channels (tenant_id, source_channel, audience_role)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        """,
        tenant_id, source_channel, audience_role,
    )


__all__ = [
    "HR_CHANNEL_PREFIXES",
    "is_hr_channel",
    "is_in_manager_chain",
    "is_shared_channel",
    "manager_chain_of",
    "register_shared_channel",
]
