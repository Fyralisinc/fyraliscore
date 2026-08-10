"""Semantic identity and normalization capability interfaces."""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from services.ingest.source_contract.connector import OperationContext
from services.ingest.source_contract.models import (
    IdentityInput,
    NormalizationInput,
    ObservationDraft,
)


@runtime_checkable
class IdentityCapability(Protocol):
    def external_id(self, input: IdentityInput) -> str: ...


@runtime_checkable
class NormalizationCapability(Protocol):
    async def normalize(
        self,
        input: NormalizationInput,
        context: OperationContext,
    ) -> tuple[ObservationDraft, ...]: ...


__all__ = ["IdentityCapability", "NormalizationCapability"]
