"""Health and cleanup capability interfaces."""

from __future__ import annotations

from typing import Literal, Protocol, runtime_checkable

from pydantic import Field

from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.models import ContractModel, HealthReport


class HealthProbeRequest(ContractModel):
    depth: Literal["local", "remote"] = "remote"


class CleanupRequest(ContractModel):
    operation_id: str = Field(min_length=1)
    revoke_remote: bool = True
    force: bool = False


class CleanupResult(ContractModel):
    complete: bool
    remote_revoked: bool = False
    reason_code: str = "complete"
    message: str = ""


@runtime_checkable
class HealthProbeCapability(Protocol):
    async def probe(
        self,
        request: HealthProbeRequest,
        context: OperationContext,
    ) -> HealthReport: ...


@runtime_checkable
class CleanupCapability(Protocol):
    async def cleanup(
        self,
        request: CleanupRequest,
        context: OperationContext,
    ) -> CleanupResult: ...


__all__ = [
    "CleanupCapability",
    "CleanupRequest",
    "CleanupResult",
    "HealthProbeCapability",
    "HealthProbeRequest",
]
