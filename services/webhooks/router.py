"""services/webhooks/router.py — FastAPI router for /webhooks/{provider}/...

Mounted by `services/gateway/main.py`. The Bearer middleware in the
gateway skips this path prefix (see `_PUBLIC_PATH_PREFIXES`), so the
only authentication is the cryptographic signature check below.

Request flow:

    1. Capture raw body bytes (NOT a re-parsed JSON form).
    2. Enforce IN-01 body-size precheck (1 MB).
    3. Look up the per-provider verifier; 404 on unknown provider.
    4. Best-effort JSON-parse the body so the tenant resolver and the
       Slack URL-verification handshake have a dict to inspect.
       Malformed JSON does NOT immediately reject — the verifier still
       runs first so an attacker cannot probe the JSON-validity oracle.
    5. Call `request.app.state.tenant_resolver.resolve(provider, payload,
       headers)` to map the (provider, installation_id) pair to a
       tenant. The outcome is captured but the rejection (if any) is
       deferred until AFTER signature verification — same security
       posture as before IN-08: signature failure first, then tenant.
    6. Load secrets via `await load_secrets(provider, tenant_id,
       app_state=request.app.state)`. With IN-08, this resolves
       `provider_installations.secret_ref` through the envelope-
       encrypted secret store; the env-var path is dev-only.
    7. Run the verifier; on any `WebhookVerificationError` return 401
       + structured error + metric increment.
    8. Enforce the resolver outcome: `UnknownInstallation` → 401,
       `PayloadMissing` → 400. On `Resolved`, hand off to
       `ingestion.core.ingest()` under the resolved tenant.
"""
from __future__ import annotations

import json
import time
from typing import Any, Mapping

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
from services.webhooks.tenant_resolver import (
    PayloadMissing,
    Resolved,
    UnknownInstallation,
)
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


def _is_slack_url_verification(payload: Mapping[str, Any] | None) -> dict[str, Any] | None:
    """Detect Slack's one-time `url_verification` handshake. Returns
    the payload when matched, else None."""
    if not isinstance(payload, dict):
        return None
    if payload.get("type") == "url_verification":
        return payload
    return None


def _is_discord_ping(payload: Mapping[str, Any] | None) -> bool:
    """Detect Discord's interaction PING (type=1)."""
    return isinstance(payload, dict) and payload.get("type") == 1


def _slack_lifecycle_event(payload: Mapping[str, Any] | None) -> str | None:
    """Detect Slack installation-lifecycle events. Returns the event
    type string when matched (`'app_uninstalled'` | `'tokens_revoked'`),
    else None. IN-08 US4: these route to the uninstall handler instead
    of ingestion."""
    if not isinstance(payload, dict):
        return None
    event = payload.get("event")
    if isinstance(event, dict):
        t = event.get("type")
        if t in ("app_uninstalled", "tokens_revoked"):
            return t
    return None


async def _handle_slack_lifecycle(
    request: Request,
    outcome: Any,
    payload: Mapping[str, Any],
    event_type: str,
) -> JSONResponse:
    """Run the Slack uninstall flow for a verified, tenant-resolved
    webhook. Returns 200 with `{handled: <event_type>}` so Slack's
    retry budget closes out cleanly."""
    from services.integrations.slack import uninstall as slack_uninstall

    team_id = (
        payload.get("team_id")
        if isinstance(payload, dict)
        else None
    )
    if not isinstance(team_id, str):
        # The resolver already matched the team; this should never
        # happen, but defensively close the request out.
        return JSONResponse({"handled": event_type}, status_code=200)

    pool = getattr(request.app.state, "pool", None)
    secret_store = getattr(request.app.state, "secret_store", None)
    tenant_resolver = getattr(request.app.state, "tenant_resolver", None)
    if pool is None or secret_store is None or tenant_resolver is None:
        log.error(
            "slack_uninstall_deps_missing",
            has_pool=pool is not None,
            has_secret_store=secret_store is not None,
            has_tenant_resolver=tenant_resolver is not None,
        )
        return JSONResponse({"handled": event_type}, status_code=200)

    handler = (
        slack_uninstall.handle_app_uninstalled
        if event_type == "app_uninstalled"
        else slack_uninstall.handle_tokens_revoked
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


def _safe_json_loads(raw: bytes) -> dict[str, Any] | None:
    """Best-effort JSON parse. Returns None for non-JSON or non-object
    bodies; the caller treats `None` as "tenant indeterminate" and
    defers any rejection until after signature verification."""
    try:
        parsed = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def build_webhooks_router() -> APIRouter:
    """Create the FastAPI router. Mounted at the app root by the
    gateway so paths read as `/webhooks/{provider}/{subpath:path}`.

    The router is stateless — all deps are resolved off `request.app.state`
    so tests can construct the gateway app and exercise the router
    without further wiring. Notably, `app.state.tenant_resolver` is
    the IN-07 DB-backed resolver wired by IN-08 (see
    `services/gateway/main.py::_wire_in08_state`).
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

        # Step 3 + 4: best-effort JSON parse so the resolver and the
        # Slack URL-verification handshake have a dict to inspect.
        payload = _safe_json_loads(raw)
        slack_uv = (
            _is_slack_url_verification(payload) if provider == "slack" else None
        )

        # Step 5: resolve tenant via the IN-07 DB-backed resolver.
        # `payload or {}` keeps the API contract clean for Stripe
        # (header-only id extraction) and for malformed bodies.
        tenant_resolver = getattr(request.app.state, "tenant_resolver", None)
        if tenant_resolver is None:
            # Gateway misconfiguration — fail loud rather than silently
            # falling back to the legacy env-var resolver. The
            # `_wire_in08_state` lifespan hook is the single chokepoint
            # that populates this attribute.
            log.error("webhook_router_tenant_resolver_missing", provider=provider)
            return JSONResponse(
                {
                    "code": "service_unavailable",
                    "message": "tenant resolver not initialized",
                    "context": {"provider": provider},
                },
                status_code=503,
            )
        outcome = await tenant_resolver.resolve(
            provider, payload or {}, dict(request.headers),
        )
        tenant_id_uuid = (
            outcome.tenant_id if isinstance(outcome, Resolved) else None
        )

        # Step 6: load secrets — DB-backed via the IN-08 secret store.
        # The verifier itself raises `secret_not_configured` when the
        # list is empty, which keeps the rejection reason consistent.
        secrets = await load_secrets(
            provider, tenant_id_uuid, app_state=request.app.state,
        )

        # Step 7: verify.
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

        # Step 8a: provider-specific verified-handshake responses.
        # These bypass the tenant-rejection enforcement because the
        # handshake itself does not name a tenant.
        if slack_uv is not None:
            challenge = slack_uv.get("challenge", "")
            return JSONResponse({"challenge": challenge}, status_code=200)
        if provider == "discord" and _is_discord_ping(payload):
            return JSONResponse({"type": 1}, status_code=200)

        # Step 8b: enforce resolver outcome — deferred until AFTER
        # signature verification so an attacker probing tenant ids
        # sees signature failures first (FR-023, IN-07 SC-008).
        if isinstance(outcome, UnknownInstallation):
            err = WebhookVerificationError(
                "unknown_installation",
                "no enabled installation matches the supplied identifier",
                provider=outcome.provider,
            )
            return _err_response(err, status_code=401)
        if isinstance(outcome, PayloadMissing):
            # PayloadMissing is a client-side defect (bad request) rather
            # than an auth failure — return 400, matching IN-07 mapping.
            metrics.record_failure(provider, "tenant_not_resolved")
            log.info(
                "webhook_payload_missing_identifier",
                provider=outcome.provider,
            )
            return JSONResponse(
                {
                    "code": "payload_missing",
                    "message": "request did not carry a parseable installation identifier",
                    "context": {"provider": outcome.provider},
                },
                status_code=400,
            )

        # outcome is Resolved at this point — tenant_id_uuid is set.
        if tenant_id_uuid is None:  # pragma: no cover — defensive
            err = WebhookVerificationError(
                "tenant_not_resolved",
                "verified webhook could not be mapped to a tenant",
                provider=provider,
            )
            return _err_response(err)

        # IN-08 US4: dispatch Slack lifecycle events (app_uninstalled /
        # tokens_revoked) to the uninstall handler BEFORE ingestion.
        # These events disable the installation + zeroize secret
        # material; they do NOT produce an Observation.
        if provider == "slack":
            slack_lifecycle = _slack_lifecycle_event(payload)
            if slack_lifecycle is not None:
                return await _handle_slack_lifecycle(
                    request,
                    outcome,
                    payload,
                    slack_lifecycle,
                )

        # Step 9: ingest. Use the already-parsed payload when possible
        # to save a re-decode; fall back to re-parse for paths where
        # the payload didn't reach JSON earlier (shouldn't happen now).
        channel = _PROVIDER_CHANNEL[provider]
        if payload is None:
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
