"""lib.extensions.host_api.v1.substrate — the capability-checked read contract.

``SubstrateReader`` is the *only* sanctioned way an extension reads core data. It
returns view types (never raw rows), and every implementation is bound to a
single ``(tenant, capabilities)`` pair — the host constructs it with the
extension's grant, so an extension can only ever see what it was granted
(channels it may read, kinds it may read), enforced both in the query layer and
structurally by the ``fyralis_ext_readonly`` Postgres role + RLS.

This module is the *Protocol* (the stable contract). The concrete implementation
lives in ``services.platform.extensions.substrate_reader`` — it must live under
``services`` because it touches ``can_read`` / the DB, and ``lib`` cannot import
``services``.
"""
from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable
from uuid import UUID

from lib.extensions.host_api.v1.views import ModelView, ObservationView


@runtime_checkable
class SubstrateReader(Protocol):
    """Capability-scoped, view-returning read surface for an extension."""

    async def query_observations(
        self,
        *,
        channel: str | None = None,
        since: datetime | None = None,
        limit: int = 100,
    ) -> list[ObservationView]:
        """Observations the extension is granted to read, newest first.

        ``channel`` must be within the grant's ``read_channels`` (or omitted to
        read all granted channels). Raises ``CapabilityError`` if a requested
        channel is not granted."""
        ...

    async def get_observation(self, observation_id: UUID) -> ObservationView | None:
        """A single observation by id (tenant- + capability-scoped), or None."""
        ...

    async def get_model(self, model_id: UUID) -> ModelView | None:
        """A single Model by id, if ``substrate_read`` includes ``model``."""
        ...


__all__ = ["SubstrateReader"]
