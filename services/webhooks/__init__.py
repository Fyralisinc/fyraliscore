"""services/webhooks — unauthenticated-at-transport webhook ingress.

This module owns the `/webhooks/{provider}/...` ingress that the
gateway exposes. It is parallel to `services/ingestion/` rather than
nested under it because the authentication contract is fundamentally
different: bearer tokens on `/ingest/{channel}` vs. provider-specific
cryptographic signatures here.

Public surface:

- `build_webhooks_router(deps)` — FastAPI router factory mounted by
  the gateway.
- `Verifier`, `WebhookVerificationError`, `VerifiedContext` —
  contract for adding new providers (see `signatures/`).

Constitution alignment (`.specify/memory/constitution.md` v1.0.0):

- Principle III — verified payloads are ingested under
  `tenant_transaction(tenant_id)`; cross-tenant resolution failures
  reject with the `tenant_not_resolved` reason rather than fall back
  to a default tenant.
- Principle VIII — all rejections raise `WebhookVerificationError`
  (a `CompanyOSError` subclass) and surface as
  `{code, message, context}` per `to_dict()`.
"""
from __future__ import annotations

from services.webhooks.verifier import (
    Verifier,
    VerificationReason,
    VerifiedContext,
    WebhookVerificationError,
)

__all__ = [
    "Verifier",
    "VerificationReason",
    "VerifiedContext",
    "WebhookVerificationError",
]
