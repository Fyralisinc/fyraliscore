"""services.platform.extensions.substrate_reader — capability-checked reads.

The concrete ``SubstrateReader`` (contract in
``lib.extensions.host_api.v1.substrate``). Bound to one ``(tenant, capabilities)``
pair, it:

  * scopes every query to the tenant via ``app.current_tenant`` + RLS,
  * runs under the restricted ``fyralis_ext_readonly`` Postgres role, so a write
    is denied *structurally* (the role has no write grant) — not on the honour
    system,
  * filters to the granted ``read_channels`` and ``substrate_read`` kinds, and
  * returns frozen view types, never raw rows.

The host builds one of these from an extension's ``extension_grants`` row and
hands it to the extension's read path.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncIterator
from uuid import UUID

from lib.extensions.host_api.v1 import (
    ALL_CHANNELS,
    Capabilities,
    CapabilityError,
    ModelView,
    ObservationView,
)
from lib.shared.tenant_context import tenant_transaction

log = logging.getLogger("extensions.substrate_reader")

EXT_READONLY_ROLE = "fyralis_ext_readonly"


@asynccontextmanager
async def extension_read_scope(
    tenant_id: UUID, *, pool: Any, require_role: bool = True
) -> AsyncIterator[Any]:
    """A tenant-scoped read transaction under the restricted extension role.

    Sets ``app.current_tenant`` (via ``tenant_transaction``) then switches the
    session to ``fyralis_ext_readonly`` for the life of the transaction — reads
    are RLS-scoped and writes are structurally denied. If the role is absent
    (misconfigured DB) and ``require_role`` is False, degrades to tenant-scoped
    reads with a warning (query-layer capability checks still apply).
    """
    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        try:
            await ctx.execute(f"SET LOCAL ROLE {EXT_READONLY_ROLE}")
        except Exception:  # noqa: BLE001
            if require_role:
                raise
            log.warning("ext_readonly_role_unavailable; degrading to tenant scope only")
        yield ctx


class CapabilityScopedReader:
    """Concrete :class:`~lib.extensions.host_api.v1.SubstrateReader`."""

    def __init__(
        self,
        *,
        pool: Any,
        tenant_id: UUID,
        capabilities: Capabilities,
        require_role: bool = True,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id
        self._caps = capabilities
        self._require_role = require_role

    # ---- observations ------------------------------------------------
    async def query_observations(
        self,
        *,
        channel: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[ObservationView]:
        if "observation" not in self._caps.substrate_read:
            raise CapabilityError("grant does not include substrate_read:observation")
        if channel is not None and not self._caps.allows_channel(channel):
            raise CapabilityError(f"channel {channel!r} not in granted read_channels")

        # Restrict to granted channels unless a single (already-checked) channel
        # was requested or the grant is "all".
        granted: list[str] | None
        if channel is not None:
            granted = [channel]
        elif self._caps.read_channels == ALL_CHANNELS:
            granted = None
        else:
            granted = list(self._caps.read_channels)
            if not granted:
                return []

        limit = max(1, min(int(limit), 1000))
        async with extension_read_scope(
            self._tenant_id, pool=self._pool, require_role=self._require_role
        ) as ctx:
            rows = await ctx.fetch(
                """
                SELECT id, tenant_id, occurred_at, kind, source_channel,
                       content, content_text, trust_tier, external_id,
                       entities_mentioned
                FROM observations
                WHERE ($1::text[] IS NULL OR source_channel = ANY($1))
                  AND ($2::timestamptz IS NULL OR occurred_at >= $2)
                ORDER BY occurred_at DESC
                LIMIT $3
                """,
                granted, since, limit,
            )
        return [ObservationView.from_row(r) for r in rows]

    async def get_observation(self, observation_id: UUID) -> ObservationView | None:
        if "observation" not in self._caps.substrate_read:
            raise CapabilityError("grant does not include substrate_read:observation")
        async with extension_read_scope(
            self._tenant_id, pool=self._pool, require_role=self._require_role
        ) as ctx:
            row = await ctx.fetchrow(
                """
                SELECT id, tenant_id, occurred_at, kind, source_channel,
                       content, content_text, trust_tier, external_id,
                       entities_mentioned
                FROM observations WHERE id = $1
                """,
                observation_id,
            )
        if row is None:
            return None
        view = ObservationView.from_row(row)
        # Defense in depth: a granted-channel check on the fetched row.
        if not self._caps.allows_channel(view.source_channel):
            return None
        return view

    # ---- models ------------------------------------------------------
    async def get_model(self, model_id: UUID) -> ModelView | None:
        if "model" not in self._caps.substrate_read:
            raise CapabilityError("grant does not include substrate_read:model")
        async with extension_read_scope(
            self._tenant_id, pool=self._pool, require_role=self._require_role
        ) as ctx:
            row = await ctx.fetchrow(
                """
                SELECT id, tenant_id, proposition_kind, status, confidence,
                       proposition, natural, created_at
                FROM accepted_current_models WHERE id = $1
                """,
                model_id,
            )
        return ModelView.from_row(row) if row is not None else None


__all__ = ["CapabilityScopedReader", "extension_read_scope", "EXT_READONLY_ROLE"]
