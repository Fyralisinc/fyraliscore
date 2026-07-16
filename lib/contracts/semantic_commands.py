"""Neutral command context shared by semantic writers.

The context binds processing authority, writer scope, idempotency and command
time. It is used by company-physics, belief, intent and future agency writers;
using it does not imply task autonomy or consequential execution.
"""

from __future__ import annotations

from datetime import datetime
from typing import Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .kernel import ProcessingAuthorityContext, WriterScopeEpoch


class SemanticWriteContext(BaseModel):
    """Exact authority and idempotency context for one semantic command."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        use_enum_values=False,
    )

    command_id: UUID
    tenant_id: UUID
    processing_authority: ProcessingAuthorityContext
    writer_scope_epoch: WriterScopeEpoch
    idempotency_key: str = Field(min_length=1)
    issued_at: datetime
    expires_at: datetime

    @field_validator("issued_at", "expires_at")
    @classmethod
    def command_times_are_aware(cls, value: datetime, info) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError(f"{info.field_name} must be timezone-aware")
        return value

    @model_validator(mode="after")
    def command_context_is_live_and_tenant_scoped(self) -> Self:
        if self.expires_at <= self.issued_at:
            raise ValueError("semantic command expiry must follow issuance")
        if self.processing_authority.tenant_id != self.tenant_id:
            raise ValueError("semantic command processing authority tenant mismatch")
        if not self.processing_authority.is_live(self.issued_at):
            raise ValueError("semantic command processing authority was not live")
        if self.writer_scope_epoch.tenant_id != self.tenant_id:
            raise ValueError("semantic command writer scope tenant mismatch")
        return self

    def require_writer(self, *, owner: str, responsibility: str) -> None:
        if not self.writer_scope_epoch.permits(
            writer_owner=owner,
            epoch=self.writer_scope_epoch.epoch,
            tenant_id=self.tenant_id,
            semantic_responsibility=responsibility,
            source_partition=str(self.tenant_id),
        ):
            raise ValueError(f"writer scope does not permit {owner}")


__all__ = ["SemanticWriteContext"]
