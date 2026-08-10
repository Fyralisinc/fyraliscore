"""Context carried by supplemental source backfill planners."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg


@dataclass(frozen=True)
class PlannerContext:
    tenant_id: UUID
    install: asyncpg.Record
    conn: asyncpg.Connection
    source_client: Any | None = None


__all__ = ["PlannerContext"]
