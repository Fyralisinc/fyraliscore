"""Contract-owned QuickBooks webhook fan-out policy.

Intuit batches multiple ``eventNotifications`` in one signed delivery.  Every
notification owns a ``realmId`` (a connected company and therefore an exact
Fyralis installation) and can contain multiple changed entities.  The shared
webhook router verifies the app-level signature once, then invokes this
contract binding to:

* split the batch into one flat payload per ``(realmId, entity)``;
* resolve every distinct realm independently;
* submit each unit through the router's generic Kafka/inline processor.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import structlog
from fastapi import Request
from fastapi.responses import JSONResponse

from lib.integrations.webhook_verifier import WebhookVerificationError
from lib.shared.errors import CompanyOSError, ValidationError


log = structlog.get_logger("integrations.quickbooks.webhook_ingress")

_PROVIDER = "quickbooks"
ProcessUnit = Callable[..., Awaitable[int]]


def fanout_units(
    payload: Mapping[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    """Return one flat handler payload per valid realm/entity pair."""

    notifications = payload.get("eventNotifications")
    if not isinstance(notifications, list):
        return []

    units: list[tuple[str, dict[str, Any]]] = []
    for notification in notifications:
        if not isinstance(notification, dict):
            continue
        realm_id = str(notification.get("realmId") or "")
        if not realm_id:
            continue
        data_change_event = notification.get("dataChangeEvent")
        entities = (
            data_change_event.get("entities")
            if isinstance(data_change_event, dict)
            else None
        )
        if not isinstance(entities, list):
            continue
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            units.append(
                (
                    realm_id,
                    {
                        "realmId": realm_id,
                        "name": entity.get("name"),
                        "id": entity.get("id"),
                        "operation": entity.get("operation"),
                        "lastUpdated": entity.get("lastUpdated"),
                    },
                )
            )
    return units


def _unknown_installation_response(
    record_failure: Callable[[str, str], None] | None = None,
) -> JSONResponse:
    error = WebhookVerificationError(
        "unknown_installation",  # type: ignore[arg-type]
        "no enabled installation matched any realm in the delivery",
        provider=_PROVIDER,
    )
    if record_failure is not None:
        record_failure(error.provider, error.reason)
    log.info(
        "webhook_verification_failed",
        provider=error.provider,
        reason=error.reason,
        code=error.code,
    )
    return JSONResponse(error.to_dict(), status_code=401)


async def handle_verified_tenant(
    *,
    request: Request,
    runtime: Any,
    outcome: Any,
    tenant_id: Any,
    payload: Mapping[str, Any] | None,
    verified: Any,
    process_unit: ProcessUnit,
) -> JSONResponse | None:
    """Fan a verified Intuit batch across its exact realm installations."""

    del outcome, tenant_id
    if not isinstance(payload, dict) or not isinstance(
        payload.get("eventNotifications"),
        list,
    ):
        return None

    units = fanout_units(payload)
    if not units:
        return JSONResponse(
            {
                "code": "validation_error",
                "message": "quickbooks webhook carried no (realm, entity) units",
                "context": {"provider": _PROVIDER},
            },
            status_code=400,
        )

    resolver = runtime.tenant_resolver
    headers = dict(request.headers)
    statuses: set[int] = set()
    ingested = 0
    unknown_realms = 0
    realm_tenants: dict[str, Any] = {}

    for realm_id, unit_payload in units:
        resolved_tenant_id = realm_tenants.get(realm_id)
        if resolved_tenant_id is None:
            resolution = await resolver.resolve(
                _PROVIDER,
                {"realmId": realm_id},
                headers,
            )
            if (
                getattr(resolution, "outcome", None) != "resolved"
                or getattr(resolution, "tenant_id", None) is None
            ):
                unknown_realms += 1
                # Never log realmId verbatim (tenant-resolver FR-015).
                log.warning("qbo_fanout_unknown_realm", provider=_PROVIDER)
                continue
            resolved_tenant_id = resolution.tenant_id
            realm_tenants[realm_id] = resolved_tenant_id

        try:
            status = await process_unit(
                tenant_id=resolved_tenant_id,
                payload=unit_payload,
            )
        except (ValidationError, CompanyOSError) as exc:
            # One malformed entity must not sink the rest of the signed batch.
            log.warning(
                "qbo_fanout_unit_rejected",
                provider=_PROVIDER,
                code=getattr(exc, "code", "error"),
            )
            continue
        statuses.add(status)
        ingested += 1

    if ingested == 0:
        return _unknown_installation_response(
            getattr(runtime, "record_failure", None),
        )

    status_code = 202 if 202 in statuses else 200
    secret_label = verified.secret_label
    return JSONResponse(
        {
            "status": "accepted",
            "units": ingested,
            "unknown_realms": unknown_realms,
            "secret_label": secret_label,
        },
        status_code=status_code,
        headers={"X-Secret-Label": secret_label or ""},
    )


__all__ = ["fanout_units", "handle_verified_tenant"]
