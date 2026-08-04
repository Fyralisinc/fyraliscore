"""Webhook ingress with the Source Connector contract as sole source owner."""

from __future__ import annotations

import json
import hmac
from typing import Any, Mapping
from uuid import UUID

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
import structlog

from lib.shared.errors import CompanyOSError, ValidationError
from lib.shared.http_headers import safe_headers
from services.app.webhooks import metrics
from services.app.webhooks.secrets import load_secrets
from services.app.webhooks.signatures import VERIFIERS
from services.app.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)
from services.app.webhooks.verifier import WebhookVerificationError
from services.ingest.ingestion.core import MAX_PAYLOAD_BYTES, ingest
from services.ingest.source_contract.source_catalog import source_ids


log = structlog.get_logger("webhooks.router")
_CONTRACT_SOURCES = frozenset(source_ids())
_NON_SOURCE_CHANNELS = {
    "linear": "linear:webhook",
    "stripe": "stripe:webhook",
}


def _error(
    provider: str,
    code: str,
    message: str,
    status_code: int,
) -> JSONResponse:
    headers = {"Retry-After": "30"} if status_code >= 500 else None
    return JSONResponse(
        {"code": code, "message": message, "context": {"provider": provider}},
        status_code=status_code,
        headers=headers,
    )


def _safe_json(raw: bytes) -> dict[str, Any] | None:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _resolver_rejection(provider: str, outcome: Any) -> JSONResponse:
    if isinstance(outcome, PayloadMissing):
        return _error(
            provider,
            "payload_missing",
            "request did not identify an installation",
            400,
        )
    if isinstance(outcome, UnknownInstallation):
        return _error(
            provider,
            "unknown_installation",
            "no ready installation matches this webhook",
            401,
        )
    return _error(provider, "tenant_not_resolved", "tenant resolution failed", 401)


def _verification_error(provider: str, exc: BaseException) -> JSONResponse:
    reason = str(getattr(exc, "reason", "signature_mismatch"))
    metrics.record_failure(provider, reason)
    log.info(
        "webhook_verification_failed",
        provider=provider,
        reason=reason,
        error_type=type(exc).__name__,
    )
    status_code = 400 if reason == "malformed_body" else 401
    return _error(
        provider,
        "webhook_verification_failed",
        "webhook authentication failed",
        status_code,
    )


def _runtime(request: Request) -> Any | None:
    return getattr(request.app.state, "integration_runtime", None)


async def _resolve(
    request: Request,
    provider: str,
    payload: Mapping[str, Any],
    subpath: str,
) -> Resolved | JSONResponse:
    runtime = _runtime(request)
    resolver = getattr(runtime, "tenant_resolver", None)
    if resolver is None:
        return _error(
            provider,
            "service_unavailable",
            "connector tenant resolver is unavailable",
            503,
        )
    outcome = await resolver.resolve(
        provider,
        payload,
        dict(request.headers),
        subpath=subpath,
    )
    return outcome if isinstance(outcome, Resolved) else _resolver_rejection(
        provider, outcome
    )


async def _contract_webhook(
    request: Request,
    *,
    provider: str,
    subpath: str,
    raw: bytes,
    payload: Mapping[str, Any],
) -> JSONResponse:
    if not subpath.startswith("callback/"):
        return _error(
            provider,
            "installation_callback_required",
            "source webhooks require an installation-scoped callback URL",
            404,
        )
    endpoint_id = subpath.removeprefix("callback/").split("/", 1)[0]
    try:
        endpoint_uuid = UUID(endpoint_id)
    except ValueError:
        return _error(provider, "unknown_callback", "callback is unavailable", 401)
    runtime = _runtime(request)
    if runtime is None:
        return _error(provider, "service_unavailable", "callback runtime unavailable", 503)
    callback = await runtime.pool.fetchrow(
        """
        SELECT callback.installation_id, callback.tenant_id,
               callback.nonce_secret_ref, callback.purpose
          FROM source_connector_callbacks AS callback
          JOIN source_connector_installations AS install
            ON install.id = callback.installation_id
         WHERE callback.endpoint_id = $1::uuid
           AND callback.status = 'active'
           AND install.connector_id = $2
           AND install.desired_state = 'Ready'
           AND install.observed_phase IN ('Ready', 'Degraded')
           AND install.removed_at IS NULL
        """,
        endpoint_uuid,
        f"fyralis/{provider}",
    )
    if callback is None:
        return _error(provider, "unknown_callback", "callback is unavailable", 401)
    if callback["purpose"] != "webhook":
        expected = await runtime.secret_store.get(
            callback["nonce_secret_ref"], tenant_id=callback["tenant_id"]
        )
        expected_text = expected.decode() if isinstance(expected, bytes) else str(expected)
        supplied = request.headers.get("x-goog-channel-token", "")
        channel_id = request.headers.get("x-goog-channel-id", "")
        if not hmac.compare_digest(expected_text, supplied) or not hmac.compare_digest(
            endpoint_id, channel_id
        ):
            return _error(provider, "callback_authentication_failed", "callback authentication failed", 401)
        try:
            from services.ingest.connector_platform.push_ingress import (
                execute_connector_push_poll,
            )

            count, has_more = await execute_connector_push_poll(
                app_state=request.app.state,
                source=provider,
                installation_id=callback["installation_id"],
            )
        except Exception as exc:
            log.exception(
                "connector_push_callback_failed",
                provider=provider,
                error_type=type(exc).__name__,
            )
            return _error(provider, "push_processing_unavailable", "push processing unavailable", 503)
        return JSONResponse(
            {"status": "accepted", "records": count, "has_more": has_more},
            status_code=202,
        )
    try:
        from services.ingest.connector_platform.webhook_ingress import (
            execute_connector_webhook,
        )

        verified = await execute_connector_webhook(
            app_state=request.app.state,
            provider=provider,
            installation_id=callback["installation_id"],
            tenant_id=callback["tenant_id"],
            body=raw,
            headers=request.headers,
        )
    except (WebhookVerificationError, CompanyOSError) as exc:
        return _verification_error(provider, exc)
    except Exception as exc:
        log.exception(
            "connector_webhook_failed_closed",
            provider=provider,
            error_type=type(exc).__name__,
        )
        return _error(
            provider,
            "webhook_processing_unavailable",
            "connector webhook processing is unavailable",
            503,
        )

    if provider == "slack" and payload.get("type") == "url_verification":
        return JSONResponse({"challenge": payload.get("challenge", "")})
    if provider == "discord" and payload.get("type") == 1:
        return JSONResponse({"type": 1})
    if provider == "discord" and payload.get("type") == 2:
        return JSONResponse(
            {
                "type": 4,
                "data": {
                    "content": "Accepted by Fyralis.",
                    "flags": 64,
                },
            },
            status_code=200,
        )
    return JSONResponse(
        {"status": "accepted", "secret_label": verified.secret_label},
        status_code=202,
    )


async def _non_source_webhook(
    request: Request,
    *,
    provider: str,
    subpath: str,
    raw: bytes,
    payload: Mapping[str, Any],
) -> JSONResponse:
    verifier = VERIFIERS.get(provider)
    if verifier is None or provider not in _NON_SOURCE_CHANNELS:
        return _error(provider, "unknown_provider", "unknown webhook provider", 404)
    outcome = await _resolve(request, provider, payload, subpath)
    if isinstance(outcome, JSONResponse):
        return outcome
    secrets = await load_secrets(
        provider,
        outcome.tenant_id,
        app_state=request.app.state,
    )
    try:
        await verifier.verify(
            body=raw,
            headers=request.headers,
            secrets=secrets,
        )
    except WebhookVerificationError as exc:
        return _verification_error(provider, exc)
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        return _error(provider, "service_unavailable", "ingestion unavailable", 503)
    try:
        result = await ingest(
            _NON_SOURCE_CHANNELS[provider],
            dict(payload),
            pool=deps.pool,
            tenant_id=outcome.tenant_id,
            actor_repo=deps.actor_repo,
            alias_repo=deps.alias_repo,
            embedder=deps.embedder,
            request_headers=safe_headers(request.headers),
        )
    except ValidationError:
        return _error(provider, "webhook_payload_rejected", "payload rejected", 400)
    except CompanyOSError as exc:
        return _error(
            provider,
            "webhook_processing_unavailable" if exc.recoverable else "webhook_payload_rejected",
            "webhook processing unavailable" if exc.recoverable else "payload rejected",
            503 if exc.recoverable else 400,
        )
    return JSONResponse(
        {
            "observation_id": str(result.observation.id),
            "deduped": result.deduped,
        },
        status_code=200 if result.deduped else 201,
    )


async def _receive_webhook(
    provider: str,
    request: Request,
    *,
    subpath: str,
) -> JSONResponse:
    raw = await request.body()
    if len(raw) > MAX_PAYLOAD_BYTES:
        return _error(provider, "payload_too_large", "payload exceeds 1 MiB", 413)
    is_contract_callback = (
        provider in _CONTRACT_SOURCES and subpath.startswith("callback/")
    )
    payload = {} if is_contract_callback and not raw else _safe_json(raw)
    if payload is None:
        return _error(provider, "webhook_payload_rejected", "JSON object required", 400)
    if provider in _CONTRACT_SOURCES:
        return await _contract_webhook(
            request,
            provider=provider,
            subpath=subpath,
            raw=raw,
            payload=payload,
        )
    return await _non_source_webhook(
        request,
        provider=provider,
        subpath=subpath,
        raw=raw,
        payload=payload,
    )


def build_webhooks_router() -> APIRouter:
    router = APIRouter(prefix="/webhooks", tags=["webhooks"])

    @router.post("/{provider}")
    @router.post("/{provider}/{subpath:path}")
    async def receive(
        provider: str,
        request: Request,
        subpath: str = "",
    ) -> JSONResponse:
        return await _receive_webhook(provider, request, subpath=subpath)

    return router


__all__ = ["build_webhooks_router"]
