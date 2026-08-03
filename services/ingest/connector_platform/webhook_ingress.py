"""Registry-authoritative webhook verification bridge for migrated sources."""

from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from datetime import datetime, timezone
from typing import Any

import httpx

from lib.shared.webhook_verification import VerifiedContext
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.execution import LegacyExecutionRouter
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.source_contract.capabilities import WEBHOOK_V1
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
    SourceRecord,
    VerifiedWebhookEvent,
    VerifiedWebhookResult,
)


LegacyVerify = Callable[[], Awaitable[VerifiedContext]]


async def execute_migrated_webhook(
    *,
    app_state: Any,
    provider: str,
    installation_row_id: Any,
    tenant_id: Any,
    body: bytes,
    headers: Mapping[str, str],
    legacy_verify: LegacyVerify,
) -> VerifiedContext:
    composition = getattr(app_state, "source_connector_runtime", None)
    runtime = getattr(app_state, "integration_runtime", None)
    if composition is None or runtime is None:
        return await legacy_verify()
    try:
        registration = composition.registry.for_source(provider)
    except Exception:
        return await legacy_verify()
    if WEBHOOK_V1.ref not in {
        item.ref for item in registration.capability_keys
    }:
        return await legacy_verify()

    install = await runtime.pool.fetchrow(
        """
        SELECT id, tenant_id, provider, installation_id, secret_ref, enabled
          FROM provider_installations
         WHERE id = $1 AND tenant_id = $2 AND provider = $3
        """,
        installation_row_id,
        tenant_id,
        provider,
    )
    if install is None:
        return await legacy_verify()

    legacy_context: VerifiedContext | None = None

    async def legacy_call() -> VerifiedWebhookResult:
        nonlocal legacy_context
        legacy_context = await legacy_verify()
        payload = json.loads(body)
        if payload.get("type") == "url_verification":
            return VerifiedWebhookResult(events=(), response_status_hint=200)
        event = payload.get("event") if isinstance(payload.get("event"), dict) else payload
        return VerifiedWebhookResult(
            events=(
                VerifiedWebhookEvent(
                    external_installation_id=str(install["installation_id"]),
                    native_event_type=str(event.get("type") or "event"),
                    record=SourceRecord(
                        native_type=str(event.get("type") or "event"),
                        payload=payload,
                    ),
                ),
            )
        )

    async with httpx.AsyncClient(follow_redirects=False) as client:
        evidence_sink = getattr(
            app_state, "source_connector_rollout_evidence", None
        )
        host_services = build_production_host_services_factory(
            ProductionHostBackends(
                pool=runtime.pool,
                secret_store=runtime.secret_store,
                http_client=client,
                metric_incrementer=(
                    evidence_sink.increment if evidence_sink is not None else None
                ),
                metric_observer=(
                    evidence_sink.observe if evidence_sink is not None else None
                ),
            )
        )
        router = LegacyExecutionRouter(
            composition,
            host_services,
            shadow_sink=evidence_sink,
            authority_repository=PostgresAuthorityRepository(runtime.pool),
            require_durable_authority=True,
        )
        result = await router.webhook(
            provider,
            install,
            BoundedWebhookRequest(
                body=body,
                headers={str(key): str(value) for key, value in headers.items()},
                received_at=datetime.now(timezone.utc),
            ),
            legacy_call,
        )

    first = result.events[0] if result.events else None
    return VerifiedContext(
        provider=provider,
        body=body,
        secret_label=(legacy_context.secret_label if legacy_context else None),
        signed_timestamp=(
            int(first.signed_at.timestamp())
            if first is not None and first.signed_at is not None
            else None
        ),
        tenant_hint=(
            {"installation_id": first.external_installation_id}
            if first is not None
            else {}
        ),
    )


__all__ = ["execute_migrated_webhook"]
