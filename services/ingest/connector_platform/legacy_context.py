"""Invocation-local bridge state used only by legacy connector adapters."""

from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass
from typing import Any, Iterator, Protocol

from services.ingest.source_contract.capabilities.ingestion import (
    GatewayBatch,
    GatewayOpenRequest,
    GatewayReceiveRequest,
    GatewaySession,
)


class LegacyGatewayDriver(Protocol):
    async def open(self, request: GatewayOpenRequest) -> GatewaySession: ...

    async def receive(self, request: GatewayReceiveRequest) -> GatewayBatch: ...

    async def close(self, session: GatewaySession) -> None: ...


@dataclass(frozen=True)
class LegacyBindingPayload:
    install: Any
    external_installation_id: str
    planner_context: Any | None = None
    reconciliation_shards: list[Any] | None = None
    reconciliation_run: Any | None = None
    poll_shard_identifier: dict[str, Any] | None = None
    gateway_driver: LegacyGatewayDriver | None = None


_CURRENT: ContextVar[LegacyBindingPayload | None] = ContextVar(
    "source_connector_legacy_binding", default=None
)


def require_legacy_binding() -> LegacyBindingPayload:
    payload = _CURRENT.get()
    if payload is None:
        raise RuntimeError(
            "legacy connector binding requires an invocation-local payload"
        )
    return payload


@contextmanager
def legacy_binding_scope(payload: LegacyBindingPayload) -> Iterator[None]:
    token: Token[LegacyBindingPayload | None] = _CURRENT.set(payload)
    try:
        yield
    finally:
        _CURRENT.reset(token)


__all__ = [
    "LegacyBindingPayload",
    "LegacyGatewayDriver",
    "legacy_binding_scope",
    "require_legacy_binding",
]
