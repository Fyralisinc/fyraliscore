"""Contract-derived webhook signature verification lookup.

Verifier implementations remain provider-specific modules, while route
ownership lives exclusively in ``services.ingest.source_contract``. Adding a
webhook source therefore requires one provider ingress declaration instead of
mutating a second verifier registry.
"""
from __future__ import annotations

from collections.abc import Callable
from typing import Any, cast

from services.ingest.source_contract import resolve_webhook_verifier


WebhookVerifyCallable = Callable[..., Any]


def verifier_for_provider(provider: str) -> WebhookVerifyCallable | None:
    """Resolve an immutable contract-declared verifier, or ``None``."""

    try:
        return cast(WebhookVerifyCallable, resolve_webhook_verifier(provider))
    except KeyError:
        return None


__all__ = ["WebhookVerifyCallable", "verifier_for_provider"]
