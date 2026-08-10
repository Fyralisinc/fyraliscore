"""Tenant-aware authority switch for direct versus episode reasoning ingress."""

from __future__ import annotations

import os
from typing import Literal
from uuid import UUID

import asyncpg

from lib.shared.errors import ValidationError


ReasoningIngressMode = Literal["direct", "episode"]


def default_reasoning_ingress_mode() -> ReasoningIngressMode:
    value = os.getenv("REASONING_INGRESS_MODE", "direct").strip().lower()
    if value not in {"direct", "episode"}:
        raise ValidationError("REASONING_INGRESS_MODE must be direct or episode")
    return value  # type: ignore[return-value]


async def reasoning_ingress_mode(
    conn: asyncpg.Connection, *, tenant_id: UUID
) -> ReasoningIngressMode:
    value = await conn.fetchval(
        "SELECT mode FROM reasoning_ingress_policies WHERE tenant_id=$1",
        tenant_id,
    )
    if value is None:
        return default_reasoning_ingress_mode()
    if value not in {"direct", "episode"}:
        raise ValidationError("persisted reasoning ingress mode is invalid")
    return value


__all__ = [
    "ReasoningIngressMode", "default_reasoning_ingress_mode",
    "reasoning_ingress_mode",
]
