"""Provider-bound, single-use OAuth state tokens."""

from __future__ import annotations

import base64
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import secrets
from typing import Any
from uuid import UUID

from lib.shared.env import is_prod
from lib.shared.errors import StateTokenInvalidError
from lib.shared.ids import uuid7
from lib.shared.secrets import load_app_secret_text_from_env


def _encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode()


def _decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _key() -> bytes:
    value = load_app_secret_text_from_env("OAUTH_STATE_HMAC_KEY")
    if not value:
        if is_prod():
            raise StateTokenInvalidError(
                "state_invalid", "OAuth state signing key is unavailable"
            )
        value = "dev-only-source-connector-state-key"
    return value.encode()


async def issue_state_token(
    tenant_id: UUID,
    pool: Any,
    *,
    provider: str,
    ttl_seconds: int = 600,
) -> str:
    nonce = secrets.token_urlsafe(32)
    expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
    await pool.execute(
        """
        INSERT INTO oauth_install_states
            (id, tenant_id, nonce, provider, expires_at)
        VALUES ($1, $2, $3, $4, $5)
        """,
        uuid7(),
        tenant_id,
        nonce,
        provider,
        expires_at,
    )
    payload = _encode(
        json.dumps(
            {
                "tenant_id": str(tenant_id),
                "provider": provider,
                "nonce": nonce,
                "expires_at": expires_at.isoformat(),
            },
            separators=(",", ":"),
        ).encode()
    )
    signature = _encode(hmac.new(_key(), payload.encode(), hashlib.sha256).digest())
    return f"{payload}.{signature}"


async def verify_and_consume_state(
    state: str,
    pool: Any,
    *,
    provider: str,
) -> tuple[UUID, dict[str, Any]]:
    payload_encoded, separator, signature_encoded = state.partition(".")
    if not separator:
        raise StateTokenInvalidError("state_invalid", "state token malformed")
    try:
        expected = hmac.new(
            _key(), payload_encoded.encode(), hashlib.sha256
        ).digest()
        supplied = _decode(signature_encoded)
        payload = json.loads(_decode(payload_encoded))
    except (ValueError, TypeError, json.JSONDecodeError) as exc:
        raise StateTokenInvalidError("state_invalid", "state token unreadable") from exc
    if not hmac.compare_digest(expected, supplied) or not isinstance(payload, dict):
        raise StateTokenInvalidError("state_invalid", "state token is invalid")
    if payload.get("provider") != provider:
        raise StateTokenInvalidError("state_invalid", "state provider mismatch")
    try:
        tenant_id = UUID(str(payload["tenant_id"]))
        nonce = str(payload["nonce"])
    except (KeyError, ValueError) as exc:
        raise StateTokenInvalidError("state_invalid", "state binding is invalid") from exc
    row = await pool.fetchrow(
        """
        UPDATE oauth_install_states
           SET consumed_at = now()
         WHERE nonce = $1
           AND tenant_id = $2
           AND provider = $3
           AND consumed_at IS NULL
           AND expires_at > now()
        RETURNING id
        """,
        nonce,
        tenant_id,
        provider,
    )
    if row is not None:
        return tenant_id, payload
    existing = await pool.fetchrow(
        """
        SELECT consumed_at, expires_at
          FROM oauth_install_states
         WHERE nonce = $1 AND tenant_id = $2 AND provider = $3
        """,
        nonce,
        tenant_id,
        provider,
    )
    if existing is None:
        reason = "state_invalid"
    elif existing["consumed_at"] is not None:
        reason = "state_consumed"
    else:
        reason = "state_expired"
    raise StateTokenInvalidError(reason, "OAuth state token cannot be consumed")


__all__ = ["issue_state_token", "verify_and_consume_state"]
