"""services/app/webhooks/gmail_pubsub.py — Pub/Sub push webhook endpoint.

    POST /webhooks/gmail/pubsub
    Authorization: Bearer <Google-signed OIDC JWT>
    Content-Type: application/json
    {
      "message": {
        "data": "<base64 of {emailAddress, historyId}>",
        "messageId": "...",
        "publishTime": "..."
      },
      "subscription": "projects/.../subscriptions/gmail-{tenant}-sub"
    }

Verification order:
  1. Pull `Authorization: Bearer <jwt>` — required.
  2. Verify the JWT (audience = configured webhook audience, email =
     configured push SA, signed by Google).
  3. Parse the envelope.
  4. Hand off to services.ingest.integrations.gmail.push_handler.handle_push.

ALWAYS returns 200 on transient failures so Pub/Sub doesn't enter a
retry storm — the history poller is the safety net. Returns 401 only
when the OIDC token is missing / invalid.
"""
from __future__ import annotations

import json
import os
from typing import Any
from uuid import UUID

import asyncpg
import structlog
from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse

from services.ingest.ingestion.feature_flags import SHADOW_WRITE_ENABLED
from services.ingest.ingestion.kafka.flush_batcher import coalesced_flush
from services.ingest.ingestion.shadow_write import shadow_write_raw
from services.ingest.integrations.gmail.push_handler import (
    GmailPushError,
    decode_pubsub_message,
    handle_push,
)
from services.ingest.source_contract import dedicated_ingress_definition
from services.app.webhooks.signatures.google_oidc import (
    GoogleOidcError,
    verify_pubsub_oidc_token,
)


log = structlog.get_logger("webhooks.gmail_pubsub")
_INGRESS = dedicated_ingress_definition("gmail_pubsub")


async def _maybe_shadow_write_pubsub(
    request: Request,
    *,
    tenant_id: UUID,
    raw_body: bytes,
    envelope: dict[str, Any],
) -> None:
    """Shadow write for Gmail Pub/Sub. PRIME DIRECTIVE (M2 work
    order §M2.2): a failure here MUST NOT propagate.

    Shadowed body is the Pub/Sub notification payload (small JSON
    envelope with emailAddress + historyId), NOT the messages
    handle_push() subsequently fetches. M6 will shadow the fetched
    messages; M2 stays scoped to the trigger notification per the
    work order.

    Ordering: AFTER handle_push() returns successfully. Same
    reasoning as the webhook router (M2.1): inline is the source
    of truth during M2; observable divergence is "inline fetched,
    shadow missing" which the E2E test asserts against.

    No-ops cleanly when:
      - app.state.kafka_producer or app.state.s3_raw_client is unset.
      - app.state.tenant_flags reports the flag False for this tenant.
    """
    try:
        kafka_producer = getattr(request.app.state, "kafka_producer", None)
        s3_client = getattr(request.app.state, "s3_raw_client", None)
        tenant_flags = getattr(request.app.state, "tenant_flags", None)

        if kafka_producer is None or s3_client is None:
            return

        if tenant_flags is not None:
            enabled = await tenant_flags.get_bool(
                tenant_id, SHADOW_WRITE_ENABLED, default=True,
            )
            if not enabled:
                return

        ingress_metadata: dict[str, Any] = {
            "event_type": "pubsub.notification",
            # The Pub/Sub messageId is a Google-assigned id useful for
            # deduplication / replay debugging — surface in metadata.
            "messageId": (
                envelope.get("message", {}).get("messageId")
                if isinstance(envelope.get("message"), dict)
                else None
            ),
            "subscription": envelope.get("subscription"),
        }

        await shadow_write_raw(
            tenant_id=tenant_id,
            source=_INGRESS.source_id,  # type: ignore[arg-type]
            ingress_kind=_INGRESS.ingress_kind,
            raw_body=raw_body,
            s3_client=s3_client,
            kafka_producer=kafka_producer,
            ingress_metadata=ingress_metadata,
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "shadow_path.failure",
            source="gmail",
            ingress_kind="pubsub",
            error_type=type(exc).__name__,
            error_message=str(exc)[:200],
        )


router = APIRouter(tags=["webhooks", "gmail"])


def _expected_audience() -> str | None:
    return os.environ.get("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE") or os.environ.get(
        "GMAIL_PUBSUB_PUSH_ENDPOINT"
    )


def _expected_email() -> str | None:
    return os.environ.get("GMAIL_PUBSUB_PUSH_OIDC_SA")


def is_pubsub_configured() -> bool:
    """The push ingress can only VERIFY a notification when the OIDC audience
    + push-SA env are set. The route is always mounted (so Google's pushes hit
    a real endpoint instead of a silent 404), but it reports `not_configured`
    rather than verifying garbage when the env is absent."""
    return bool(_expected_audience() and _expected_email())


@router.post(_INGRESS.route_path)
async def gmail_pubsub_push(
    request: Request,
    authorization: str | None = Header(default=None),
) -> JSONResponse:
    audience = _expected_audience()
    email = _expected_email()
    if not audience or not email:
        # Explicit, observable signal — never a silent skip or a 500 crash.
        # 503 so an unconfigured deployment is distinguishable from a bad token
        # (401) and Pub/Sub treats it as retryable until the env is set.
        log.warning(
            "gmail.pubsub.not_configured",
            missing=[
                k for k, v in (
                    ("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE", audience),
                    ("GMAIL_PUBSUB_PUSH_OIDC_SA", email),
                ) if not v
            ],
        )
        return JSONResponse(
            status_code=503,
            content={"status": "not_configured",
                     "reason": "gmail_pubsub_oidc_env_missing"},
        )

    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    token = authorization[len("bearer "):].strip()

    try:
        await verify_pubsub_oidc_token(
            token=token,
            expected_audience=audience,
            expected_email=email,
        )
    except GoogleOidcError as exc:
        log.warning("gmail.pubsub.oidc_invalid", error=str(exc)[:200])
        raise HTTPException(status_code=401, detail="oidc_invalid")

    # Capture the raw bytes before JSON-parsing so the shadow write
    # can hash the exact body Google sent (request.json() would
    # round-trip through dict and lose byte equality).
    raw_body = await request.body()
    try:
        envelope = json.loads(raw_body) if raw_body else None
        if not isinstance(envelope, dict):
            raise ValueError("envelope must be a JSON object")
    except (ValueError, json.JSONDecodeError) as exc:
        log.warning("gmail.pubsub.bad_json", error=str(exc)[:200])
        # 200 to avoid retry storm — Google sent us garbage we can't act on.
        return JSONResponse(content={"status": "skipped", "reason": "bad_json"})

    deps = getattr(request.app.state, "deps", None)
    pool: asyncpg.Pool | None = getattr(deps, "pool", None) if deps else None
    if pool is None:
        log.error("gmail.pubsub.no_pool")
        return JSONResponse(content={"status": "skipped", "reason": "no_pool"})

    try:
        # Cheap sanity decode so a malformed envelope short-circuits
        # before we burn budget on push_handler internals.
        decode_pubsub_message(envelope)
    except GmailPushError as exc:
        log.warning("gmail.pubsub.bad_envelope", error=str(exc)[:200])
        return JSONResponse(content={"status": "skipped", "reason": "bad_envelope"})

    try:
        # Live-via-Kafka cutover deps (sourced from app.state, parallel to
        # the webhook router). When wired + `kafka_path_enabled=TRUE`, the
        # fetched messages publish to `ingestion.raw` instead of inline.
        result = await handle_push(
            pool=pool,
            envelope=envelope,
            s3_raw_client=getattr(request.app.state, "s3_raw_client", None),
            kafka_producer=getattr(request.app.state, "kafka_producer", None),
            tenant_flags=getattr(request.app.state, "tenant_flags", None),
        )
    except Exception as exc:  # noqa: BLE001 — translate to 200 + log
        log.exception("gmail.pubsub.handler_error", error=str(exc)[:200])
        return JSONResponse(content={"status": "error_swallowed"})

    # ---- M2.2 Shadow path ----
    # Fires AFTER handle_push() returns. Only when the tenant was
    # resolvable (handle_push includes "tenant_id" in its result dict
    # for every post-resolved path; absent keys = "unknown_subscription"
    # or "empty_notification" — nothing to shadow).
    tenant_id_str = result.get("tenant_id") if isinstance(result, dict) else None
    if tenant_id_str:
        try:
            tenant_uuid = UUID(tenant_id_str)
        except (ValueError, TypeError):
            tenant_uuid = None
        if tenant_uuid is not None:
            await _maybe_shadow_write_pubsub(
                request,
                tenant_id=tenant_uuid,
                raw_body=raw_body,
                envelope=envelope,
            )

    # CRITICAL (request/response gateway): both handle_push() (fetched
    # messages) and _maybe_shadow_write_pubsub() (the push envelope)
    # only `produce()` into librdkafka's LOCAL buffer. An HTTP handler
    # has nothing driving the delivery queue, so without an explicit
    # flush the produced records sit in the buffer and never land on
    # `ingestion.raw`. Flush so the event is durably on the broker
    # before we ack Pub/Sub. Best-effort: we still return 200 (the
    # Pub/Sub no-retry-storm contract); an incomplete flush is logged
    # for the operator. Same semantic as the webhook router cutover.
    producer = getattr(request.app.state, "kafka_producer", None)
    if producer is not None:
        try:
            remaining = await coalesced_flush(
                producer, timeout_seconds=10.0,
            )
            if remaining:
                log.warning("gmail.pubsub.kafka_flush_incomplete", remaining=remaining)
        except Exception as exc:  # noqa: BLE001 — never break the 200 ack
            log.warning("gmail.pubsub.kafka_flush_error", error=str(exc)[:200])

    # Strip the internal tenant_id key from the response — clients
    # (Pub/Sub) don't need it; keep the public contract minimal.
    if isinstance(result, dict) and "tenant_id" in result:
        result = {k: v for k, v in result.items() if k != "tenant_id"}

    return JSONResponse(content=result)


def build_gmail_pubsub_router() -> APIRouter:
    """Return the router declared by the dedicated ingress contract."""

    return router


__all__ = [
    "build_gmail_pubsub_router",
    "gmail_pubsub_push",
    "is_pubsub_configured",
    "router",
]
