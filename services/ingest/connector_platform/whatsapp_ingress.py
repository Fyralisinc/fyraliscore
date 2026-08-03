"""Registry-authoritative verification for the WhatsApp batch ingress."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from typing import Any

import httpx

from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.connector_runtime.failures import RuntimeConnectorFailure
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    VerifiedWebhookResult,
)


LegacyVerification = Callable[[], Awaitable[bool]]


async def verify_migrated_whatsapp_webhook(
    *,
    app_state: Any,
    install: Mapping[str, Any],
    body: bytes,
    headers: Mapping[str, str],
    legacy_verify: LegacyVerification,
) -> bool:
    composition = getattr(app_state, "source_connector_runtime", None)
    runtime = getattr(app_state, "integration_runtime", None)
    if composition is None or runtime is None:
        return await legacy_verify()
    try:
        composition.registry.for_source("whatsapp")
    except Exception:
        return await legacy_verify()

    async def legacy_call() -> VerifiedWebhookResult:
        if not await legacy_verify():
            from services.ingest.source_contract.errors import (
                AuthenticationRejectedError,
            )

            raise AuthenticationRejectedError("Meta webhook signature is invalid")
        return VerifiedWebhookResult(events=())

    adapted = {
        "id": install["id"],
        "tenant_id": install["tenant_id"],
        "installation_id": install["phone_number_id"],
        "secret_ref": install.get("app_secret_ref"),
        "enabled": install.get("enabled", True),
    }
    async with httpx.AsyncClient(follow_redirects=False) as client:
        hosts = build_production_host_services_factory(
            ProductionHostBackends(
                pool=runtime.pool,
                secret_store=runtime.secret_store,
                http_client=client,
            )
        )
        router = LegacyExecutionRouter(
            composition,
            hosts,
            authority_repository=PostgresAuthorityRepository(runtime.pool),
            require_durable_authority=True,
        )
        try:
            await router.webhook(
                "whatsapp",
                adapted,
                BoundedWebhookRequest(
                    body=body,
                    headers={str(key): str(value) for key, value in headers.items()},
                    received_at=datetime.now(timezone.utc),
                ),
                legacy_call,
            )
        except RuntimeConnectorFailure as exc:
            if exc.translated.category == "installation" and not exc.retryable:
                return False
            raise
    return True


__all__ = ["verify_migrated_whatsapp_webhook"]
