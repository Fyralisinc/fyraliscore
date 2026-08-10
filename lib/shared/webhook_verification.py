"""Layer-neutral result of successful webhook authentication."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class VerifiedContext:
    """Verified bytes and safe routing hints produced by webhook ingress."""

    provider: str
    body: bytes
    secret_label: str | None = None
    signed_timestamp: int | None = None
    tenant_hint: dict[str, Any] = field(default_factory=dict)


__all__ = ["VerifiedContext"]
