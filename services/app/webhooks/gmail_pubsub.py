"""OIDC-authenticated Gmail Pub/Sub trigger for contract polling."""

from __future__ import annotations

import base64
import json
import os
from typing import Any

from fastapi import APIRouter, Header, HTTPException, Request
from fastapi.responses import JSONResponse
import structlog

from services.app.webhooks.signatures.google_oidc import (
    GoogleOidcError,
    verify_pubsub_oidc_token,
)
from services.ingest.connector_platform.push_ingress import (
    execute_connector_push_poll,
)


log = structlog.get_logger("webhooks.gmail_pubsub")
router = APIRouter(prefix="/webhooks/gmail", tags=["webhooks", "gmail"])


def _expected_audience() -> str | None:
    return os.environ.get("GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE") or os.environ.get(
        "GMAIL_PUBSUB_PUSH_ENDPOINT"
    )


def _expected_email() -> str | None:
    return os.environ.get("GMAIL_PUBSUB_PUSH_OIDC_SA")


def is_pubsub_configured() -> bool:
    return bool(_expected_audience() and _expected_email())


def _notification(envelope: Any) -> tuple[str, str]:
    if not isinstance(envelope, dict) or not isinstance(envelope.get("message"), dict):
        raise ValueError("Pub/Sub envelope is invalid")
    encoded = envelope["message"].get("data")
    if not isinstance(encoded, str):
        raise ValueError("Pub/Sub message data is absent")
    padding = "=" * (-len(encoded) % 4)
    value = json.loads(base64.urlsafe_b64decode(encoded + padding))
    email = value.get("emailAddress") if isinstance(value, dict) else None
    history = value.get("historyId") if isinstance(value, dict) else None
    if not isinstance(email, str) or not email or history in (None, ""):
        raise ValueError("Gmail notification lacks emailAddress/historyId")
    return email, str(history)


async def _execute_poll(request: Request, email: str) -> tuple[int, bool]:
    runtime = getattr(request.app.state, "integration_runtime", None)
    if runtime is None:
        raise RuntimeError("Source Connector runtime is unavailable")
    install = await runtime.pool.fetchrow(
        """
        SELECT install.id
          FROM source_connector_installations AS install
          LEFT JOIN source_connector_installation_data AS watch
            ON watch.installation_id = install.id
           AND watch.namespace = 'google_watch'
         WHERE install.connector_id = 'fyralis/gmail'
           AND install.desired_state = 'Ready'
           AND install.observed_phase IN ('Ready', 'Degraded')
           AND install.removed_at IS NULL
           AND (
             install.external_installation_id = $1
             OR COALESCE(watch.values -> 'email_addresses', '[]'::jsonb) ? $1
           )
         LIMIT 1
        """,
        email,
    )
    if install is None:
        raise LookupError("Gmail notification has no ready connector installation")
    return await execute_connector_push_poll(
        app_state=request.app.state,
        source="gmail",
        installation_id=install["id"],
    )


@router.post("/pubsub")
async def gmail_pubsub_push(
    request: Request,
    authorization: str | None = Header(default=None),
) -> JSONResponse:
    audience = _expected_audience()
    expected_email = _expected_email()
    if not audience or not expected_email:
        return JSONResponse(
            {
                "status": "not_configured",
                "reason": "gmail_pubsub_oidc_env_missing",
            },
            status_code=503,
        )
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="missing bearer token")
    try:
        await verify_pubsub_oidc_token(
            token=authorization[7:].strip(),
            expected_audience=audience,
            expected_email=expected_email,
        )
    except GoogleOidcError as exc:
        log.info("gmail_pubsub_oidc_rejected", error_type=type(exc).__name__)
        raise HTTPException(status_code=401, detail="oidc_invalid") from exc
    try:
        envelope = json.loads(await request.body())
        email, history_id = _notification(envelope)
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        return JSONResponse(
            {"status": "rejected", "reason": "bad_envelope"},
            status_code=400,
        )
    try:
        count, has_more = await _execute_poll(request, email)
    except LookupError:
        return JSONResponse(
            {"status": "rejected", "reason": "unknown_installation"},
            status_code=401,
        )
    except Exception as exc:
        log.exception(
            "gmail_pubsub_contract_poll_failed",
            error_type=type(exc).__name__,
        )
        return JSONResponse(
            {"status": "unavailable", "reason": "connector_poll_failed"},
            status_code=503,
        )
    return JSONResponse(
        {
            "status": "accepted",
            "history_id": history_id,
            "records": count,
            "has_more": has_more,
        },
        status_code=202,
    )


__all__ = ["is_pubsub_configured", "router"]
