"""Contract-owned Slack webhook policy.

The shared webhook router verifies Slack's request signature before invoking
either hook declared by the canonical ``WebhookIngressDefinition``:

* URL-verification challenges are acknowledged before tenant-outcome
  rejection because Slack does not include a workspace identifier in that
  bootstrap payload.
* installation lifecycle events run only after exact tenant and installation
  resolution and never enter the Observation ingestion path.
"""

from __future__ import annotations

from typing import Any, Mapping

import structlog
from fastapi import Request
from fastapi.responses import JSONResponse

from services.ingest.integrations.slack import uninstall


log = structlog.get_logger("integrations.slack.webhook_ingress")

_LIFECYCLE_EVENTS = frozenset({"app_uninstalled", "tokens_revoked"})


def _lifecycle_event(payload: Mapping[str, Any] | None) -> str | None:
    if not isinstance(payload, dict):
        return None
    event = payload.get("event")
    if not isinstance(event, dict):
        return None
    event_type = event.get("type")
    return event_type if event_type in _LIFECYCLE_EVENTS else None


async def handle_verified_pre_tenant(
    *,
    request: Request,
    runtime: Any,
    payload: Mapping[str, Any] | None,
) -> JSONResponse | None:
    """Echo a verified Slack URL-verification challenge.

    This hook intentionally runs before unresolved tenant outcomes are
    rejected: the challenge payload has no ``team_id``.  The shared router has
    already loaded the app-level signing secret and verified the signature.
    """

    del request, runtime
    if not isinstance(payload, dict) or payload.get("type") != "url_verification":
        return None
    return JSONResponse(
        {"challenge": payload.get("challenge", "")},
        status_code=200,
    )


async def handle_verified_tenant(
    *,
    request: Request,
    runtime: Any,
    outcome: Any,
    tenant_id: Any,
    payload: Mapping[str, Any] | None,
    verified: Any,
    process_unit: Any | None = None,
) -> JSONResponse | None:
    """Apply a verified lifecycle event to its exact installation."""

    del request, tenant_id, verified, process_unit
    event_type = _lifecycle_event(payload)
    if event_type is None:
        return None
    return await _handle_lifecycle(
        runtime=runtime,
        outcome=outcome,
        payload=payload or {},
        event_type=event_type,
    )


async def _handle_lifecycle(
    *,
    runtime: Any,
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
) -> JSONResponse:
    team_id = payload.get("team_id")
    if not isinstance(team_id, str):
        # Exact tenant resolution normally guarantees this.  Preserve Slack's
        # ACK semantics if a custom resolver returned a malformed outcome.
        return JSONResponse({"handled": event_type}, status_code=200)

    pool = runtime.pool
    secret_store = runtime.secret_store
    tenant_resolver = runtime.tenant_resolver
    if pool is None or secret_store is None or tenant_resolver is None:
        log.error(
            "slack_uninstall_deps_missing",
            has_pool=pool is not None,
            has_secret_store=secret_store is not None,
            has_tenant_resolver=tenant_resolver is not None,
        )
        return JSONResponse({"handled": event_type}, status_code=200)

    handler = (
        uninstall.handle_app_uninstalled
        if event_type == "app_uninstalled"
        else uninstall.handle_tokens_revoked
    )
    await handler(
        pool,
        secret_store,
        tenant_resolver,
        outcome.tenant_id,
        outcome.installation_row_id,
        team_id,
    )
    return JSONResponse({"handled": event_type}, status_code=200)


__all__ = [
    "handle_verified_pre_tenant",
    "handle_verified_tenant",
]
