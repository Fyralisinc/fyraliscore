"""Typed, non-authoritative source-object identity claims from handlers."""

from __future__ import annotations

from typing import Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class StructuredSourceIdentityClaim(BaseModel):
    """A structured source field that may reference a governed binding.

    Claims never create identity. The ingest transaction may attach one only
    when an independently governed binding already exists for the exact native
    identifier.
    """

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )

    source_system: str = Field(min_length=1)
    source_native_identifier: str = Field(min_length=1)
    source_surface: str = Field(min_length=1)
    claim_authority_ref: str = Field(min_length=1)

    @model_validator(mode="after")
    def native_identifier_is_source_namespaced(self) -> Self:
        if not self.source_native_identifier.startswith(
            f"{self.source_system}:"
        ):
            raise ValueError(
                "source-native identifier must be namespaced by source system"
            )
        return self


__all__ = ["StructuredSourceIdentityClaim"]
