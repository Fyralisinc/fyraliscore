"""services/webhooks/router.py — FastAPI router for /webhooks/{provider}/...

Mounted by `services/gateway/main.py`. The Bearer middleware in the
gateway skips this path prefix (see `_PUBLIC_PATH_PREFIXES`), so the
only authentication is the cryptographic signature check below.

Request flow:

    1. Capture raw body bytes (NOT a re-parsed JSON form).
    2. Enforce IN-01 body-size precheck (1 MB).
    3. Look up the per-provider verifier; 404 on unknown provider.
    4. Handle the provider-specific URL-verification handshake
       (Slack `url_verification`, Discord type=1 PING) without
       producing an Observation.
    5. Resolve tenant from the body; 401 with `tenant_not_resolved`
       on miss.
    6. Load active secrets for (provider, tenant); empty → 401 with
       `secret_not_configured`.
    7. Run the verifier; on any `WebhookVerificationError` return 401
       + structured error + metric increment.
    8. On success, decode JSON and run `ingestion.core.ingest()` under
       the resolved tenant. Surface ingestion errors with the same
       error shape used by `/ingest/{channel}`.
"""
from __future__ import annotations

import json
import os
import time
from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from lib.shared.errors import CompanyOSError, ValidationError
from services.ingestion.core import (
    IngestResult,
    MAX_PAYLOAD_BYTES,
    PayloadTooLarge,
    ingest,
)
from services.ingestion.handlers import HandlerNotFound
from services.webhooks import metrics
from services.webhooks.signatures import VERIFIERS
from services.webhooks.secrets import load_secrets
from services.webhooks.tenant_resolution import resolve_tenant
from services.webhooks.verifier import WebhookVerificationError


log = structlog.get_logger("webhooks.router")


# Channels in CHANNEL_TRUST_MAP are keyed differently per provider; the
# router maps from provider → channel name once, here, so the
# verification layer and the ingestion handler registry stay aligned.
_PROVIDER_CHANNEL: dict[str, str] = {
    "slack": "slack:message",
    "github": "github:webhook",
    "linear": "linear:webhook",
    "stripe": "stripe:webhook",
    "discord": "discord:webhook",
}


def _err_response(
    err: WebhookVerificationError,
    status_code: int = 401,
) -> JSONResponse:
    """Render a verification error as a 401 with structured context.

    FR-016: the body and candidate signature are NOT included in the
    response (or in any structured log we emit). The error's
    `to_dict()` shape is `{code, message, context}` with `provider`
    and `reason` always populated.
    """
    metrics.record_failure(err.provider, err.reason)
    log.info(
        "webhook_verification_failed",
        provider=err.provider,
        reason=err.reason,
        code=err.code,
    )
    return JSONResponse(err.to_dict(), status_code=status_code)


def _is_slack_url_verification(body: bytes) -> dict[str, Any] | None:
    """Detect Slack's one-time `url_verification` handshake. Returns
    the parsed payload when matched, else None."""
    try:
        d = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    if isinstance(d, dict) and d.get("type") == "url_verification":
        return d
    return None


def _is_discord_ping(body: bytes) -> bool:
    """Detect Discord's interaction PING (type=1)."""
    try:
        d = json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return False
    return isinstance(d, dict) and d.get("type") == 1


def build_webhooks_router() -> APIRouter:
    """Create the FastAPI router. Mounted at the app root by the
    gateway so paths read as `/webhooks/{provider}/{subpath:path}`.

    The router is stateless — all deps are resolved off `request.app.state`
    so tests can construct the gateway app and exercise the router
    without further wiring.
    """
    router = APIRouter(prefix="/webhooks", tags=["webhooks"])

    @router.post("/{provider}/{subpath:path}")
    async def receive(
        provider: str,
        subpath: str,
        request: Request,
    ) -> JSONResponse:
        verifier = VERIFIERS.get(provider)
        if verifier is None:
            return JSONResponse(
                {
                    "code": "unknown_provider",
                    "message": f"no webhook verifier registered for {provider!r}",
                    "context": {"provider": provider},
                },
                status_code=404,
            )

        # Step 1+2: capture raw body bytes; enforce size precheck.
        raw = await request.body()
        if len(raw) > MAX_PAYLOAD_BYTES:
            return JSONResponse(
                {
                    "code": "payload_too_large",
                    "message": "payload exceeds maximum size",
                    "context": {
                        "provider": provider,
                        "max_bytes": MAX_PAYLOAD_BYTES,
                    },
                },
                status_code=413,
            )

        # Step 3 (Slack URL verification) — handled BEFORE signature
        # checks because Slack signs the handshake too, but emit no
        # Observation. We still verify the signature so a spoofed
        # url_verification cannot reveal challenge values to an
        # attacker.
        slack_uv = (
            _is_slack_url_verification(raw) if provider == "slack" else None
        )

        # Step 4: resolve tenant from body.
        tenant_id_uuid = resolve_tenant(provider, raw)
        if tenant_id_uuid is None and slack_uv is None:
            # The Slack url_verification handshake may arrive before any
            # tenant config is wired (Slack sends it at app install
            # time). We still verify, but defer the tenant_not_resolved
            # rejection until after signature verification so we don't
            # leak which team_ids are configured.
            tenant_id_uuid = None  # explicit: still attempt verification

        # Step 5: load secrets. The verifier itself raises
        # `secret_not_configured` when the list is empty, which keeps
        # the rejection reason consistent.
        secrets = load_secrets(provider, tenant_id_uuid)

        # Step 6: verify.
        try:
            ctx = await verifier.verify(
                body=raw,
                headers=request.headers,
                secrets=secrets,
                now=time.time(),
            )
        except WebhookVerificationError as e:
            return _err_response(e)
        except Exception as e:  # pragma: no cover — defensive
            log.error(
                "webhook_verifier_unexpected_error",
                provider=provider,
                error_type=type(e).__name__,
            )
            metrics.record_failure(provider, "signature_mismatch")
            return JSONResponse(
                {
                    "code": "webhook_verification_failed",
                    "message": "verifier raised unexpected error",
                    "context": {
                        "provider": provider,
                        "reason": "signature_mismatch",
                    },
                },
                status_code=401,
            )

        # Step 7: provider-specific verified-handshake responses.
        if slack_uv is not None:
            challenge = slack_uv.get("challenge", "")
            return JSONResponse({"challenge": challenge}, status_code=200)
        if provider == "discord" and _is_discord_ping(raw):
            # Discord expects type=1 PONG echoed back.
            return JSONResponse({"type": 1}, status_code=200)

        # Now that we've verified the request as authentic, enforce
        # tenant resolution. (Doing this AFTER verification means an
        # attacker who guesses a tenant identifier still sees a
        # signature failure first, not a tenant-resolution leak.)
        if tenant_id_uuid is None:
            # Re-derive in case the handshake path skipped it earlier.
            tenant_id_uuid = resolve_tenant(provider, ctx.body)
        if tenant_id_uuid is None:
            err = WebhookVerificationError(
                "tenant_not_resolved",
                "verified webhook could not be mapped to a tenant",
                provider=provider,
            )
            return _err_response(err)

        # Step 8: ingest.
        channel = _PROVIDER_CHANNEL[provider]
        try:
            payload = json.loads(ctx.body)
        except json.JSONDecodeError:
            return JSONResponse(
                {
                    "code": "invalid_json",
                    "message": "verified body is not valid JSON",
                    "context": {"provider": provider},
                },
                status_code=400,
            )

        deps = _deps(request)
        try:
            result: IngestResult = await ingest(
                channel,
                payload,
                pool=deps.pool,
                tenant_id=tenant_id_uuid,
                actor_repo=deps.actor_repo,
                alias_repo=deps.alias_repo,
                embedder=deps.embedder,
                request_headers=dict(request.headers),
            )
        except HandlerNotFound:
            return JSONResponse(
                {
                    "code": "handler_not_found",
                    "message": f"no ingestion handler for channel {channel!r}",
                    "context": {"provider": provider, "channel": channel},
                },
                status_code=501,
            )
        except PayloadTooLarge:
            return JSONResponse(
                {
                    "code": "payload_too_large",
                    "message": "payload exceeds maximum size",
                    "context": {"provider": provider},
                },
                status_code=413,
            )
        except ValidationError as e:
            return JSONResponse(
                {"code": e.code, "message": e.message, "context": e.context},
                status_code=400,
            )
        except CompanyOSError as e:
            return JSONResponse(
                {"code": e.code, "message": e.message, "context": e.context},
                status_code=400,
            )

        return JSONResponse(
            {
                "observation_id": str(result.observation.id),
                "deduped": result.deduped,
                "trigger_queue_id": (
                    str(result.trigger_queue_id)
                    if result.trigger_queue_id
                    else None
                ),
                "secret_label": ctx.secret_label,
            },
            status_code=200 if result.deduped else 201,
        )

    return router


def _deps(request: Request) -> Any:
    """Resolve gateway deps off the app state.

    Lazy lookup so the router can be mounted before the lifespan
    handler wires deps (the existing gateway pattern).
    """
    deps = getattr(request.app.state, "deps", None)
    if deps is None:
        raise RuntimeError(
            "gateway deps not initialised — webhook router requires "
            "build_app() lifespan to have completed"
        )
    return deps


__all__ = ["build_webhooks_router"]
