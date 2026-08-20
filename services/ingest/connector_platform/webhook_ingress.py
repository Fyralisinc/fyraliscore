"""Registry-authoritative webhook verification for contract sources."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

import httpx

from lib.shared.webhook_verification import VerifiedContext
from services.app.webhooks.verifier import WebhookVerificationError
from services.ingest.connector_platform.authority_store import (
    PostgresAuthorityRepository,
)
from services.ingest.connector_platform.execution import ConnectorExecutionRouter
from services.ingest.connector_runtime.failures import RuntimeConnectorFailure
from services.ingest.connector_platform.production_host_services import (
    ProductionHostBackends,
    build_production_host_services_factory,
)
from services.ingest.source_contract.capabilities import WEBHOOK_V1
from services.ingest.source_contract.errors import (
    BindingError,
    AuthenticationRejectedError,
    CapabilityUnavailableError,
    ConnectorNotFoundError,
    SourceUnavailableError,
    PayloadRejectedError,
)
from services.ingest.source_contract.models import (
    BoundedWebhookRequest,
)

async def execute_connector_webhook(
    *,
    app_state: Any,
    provider: str,
    installation_id: Any,
    tenant_id: Any,
    body: bytes,
    headers: Mapping[str, str],
) -> VerifiedContext:
    composition = getattr(app_state, "source_connector_runtime", None)
    runtime = getattr(app_state, "integration_runtime", None)
    if composition is None or runtime is None:
        raise SourceUnavailableError("connector webhook runtime is unavailable")
    try:
        registration = composition.registry.for_source(provider)
    except Exception as exc:
        raise ConnectorNotFoundError(
            "webhook provider is not registered",
            details={"source": provider},
        ) from exc
    if WEBHOOK_V1.ref not in {
        item.ref for item in registration.capability_keys
    }:
        raise CapabilityUnavailableError(
            "connector does not implement webhook verification",
            details={"source": provider, "capability": WEBHOOK_V1.ref},
        )

    install = await runtime.pool.fetchrow(
        """
        SELECT *,
               connector_id = $3 AS enabled
          FROM source_connector_installations
         WHERE id = $1
           AND tenant_id = $2
           AND connector_id = $3
           AND desired_state = 'Ready'
           AND observed_phase IN ('Ready', 'Degraded')
           AND removed_at IS NULL
        """,
        installation_id,
        tenant_id,
        f"fyralis/{provider}",
    )
    if install is None:
        raise BindingError(
            "webhook installation is not registered",
            details={
                "source": provider,
                "installation_id": str(installation_id),
            },
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
                s3_raw_client=getattr(app_state, "s3_raw_client", None),
                kafka_producer=getattr(app_state, "kafka_producer", None),
                metric_incrementer=(
                    evidence_sink.increment if evidence_sink is not None else None
                ),
                metric_observer=(
                    evidence_sink.observe if evidence_sink is not None else None
                ),
            )
        )
        router = ConnectorExecutionRouter(
            composition,
            host_services,
            authority_repository=PostgresAuthorityRepository(runtime.pool),
            require_durable_authority=True,
        )
        try:
            result = await router.webhook_and_emit(
                provider,
                install,
                BoundedWebhookRequest(
                    body=body,
                    headers={str(key): str(value) for key, value in headers.items()},
                    received_at=datetime.now(timezone.utc),
                ),
            )
        except RuntimeConnectorFailure as exc:
            cause = exc.__cause__
            if isinstance(cause, AuthenticationRejectedError):
                raise WebhookVerificationError(
                    "signature_mismatch",
                    "connector rejected webhook authentication",
                    provider=provider,
                ) from exc
            if isinstance(cause, PayloadRejectedError):
                raise WebhookVerificationError(
                    "malformed_body",
                    "connector rejected webhook payload",
                    provider=provider,
                ) from exc
            raise

    first = result.events[0] if result.events else None
    return VerifiedContext(
        provider=provider,
        body=body,
        secret_label=None,
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


__all__ = ["execute_connector_webhook"]
