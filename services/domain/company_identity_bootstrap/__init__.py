"""Founder-authoritative cold-start identity bootstrap."""

from .service import (
    FounderIdentityBootstrapEntry,
    FounderIdentityBootstrapResult,
    apply_founder_identity_bootstrap,
)

__all__ = [
    "FounderIdentityBootstrapEntry",
    "FounderIdentityBootstrapResult",
    "apply_founder_identity_bootstrap",
]
