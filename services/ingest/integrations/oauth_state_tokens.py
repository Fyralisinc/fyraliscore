"""Shared signed, single-use OAuth state token primitives."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import secrets
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import UUID

import asyncpg

from lib.shared.errors import StateTokenInvalidError
from lib.shared.ids import uuid7
from lib.shared.secrets import load_app_secret_text_from_env


def _b64url(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _hmac_key() -> bytes:
    raw = load_app_secret_text_from_env("OAUTH_STATE_HMAC_KEY")
    if not raw:
        from lib.shared.env import is_prod

        if is_prod():
            raise StateTokenInvalidError(
                "state_invalid",
                "OAUTH_STATE_HMAC_KEY not configured in production",
            )
        raw = "dev-only-state-hmac-key-fallback"
    return raw.encode()


async def issue_state_token(
    tenant_id: UUID,
    pool: asyncpg.Pool,
    *,
    ttl_seconds: int = 600,
    provider: str,
    extra_payload: dict[str, Any] | None = None,
) -> str:
    nonce = secrets.token_urlsafe(32)
    expires_at = datetime.now(tz=UTC) + timedelta(seconds=ttl_seconds)
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
    payload: dict[str, Any] = {
        "tenant_id": str(tenant_id),
        "nonce": nonce,
        "expires_at": expires_at.isoformat(),
    }
    if extra_payload:
        payload.update(extra_payload)
    payload_b64 = _b64url(json.dumps(payload, separators=(",", ":")).encode())
    signature = hmac.new(
        _hmac_key(), payload_b64.encode("ascii"), hashlib.sha256
    ).digest()
    return f"{payload_b64}.{_b64url(signature)}"


async def verify_and_consume_state(
    state: str,
    pool: asyncpg.Pool,
    *,
    expected_provider: str | None = None,
) -> tuple[UUID, dict[str, Any]]:
    if not state or "." not in state:
        raise StateTokenInvalidError("state_invalid", "state token malformed")

    payload_b64, _, signature_b64 = state.partition(".")
    try:
        expected = hmac.new(
            _hmac_key(), payload_b64.encode("ascii"), hashlib.sha256
        ).digest()
        provided = _b64url_decode(signature_b64)
    except (TypeError, ValueError) as exc:
        raise StateTokenInvalidError(
            "state_invalid", "state token signature unreadable"
        ) from exc
    if not hmac.compare_digest(expected, provided):
        raise StateTokenInvalidError("state_invalid", "state HMAC mismatch")

    try:
        payload = json.loads(_b64url_decode(payload_b64))
        nonce = payload["nonce"]
        tenant_id = UUID(payload["tenant_id"])
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise StateTokenInvalidError(
            "state_invalid", "state token payload unparseable"
        ) from exc

    row = await pool.fetchrow(
        """
        UPDATE oauth_install_states
           SET consumed_at = now()
         WHERE nonce = $1
           AND consumed_at IS NULL
           AND expires_at > now()
           AND ($2::text IS NULL OR provider = $2)
        RETURNING id, tenant_id, provider
        """,
        nonce,
        expected_provider,
    )
    if row is not None:
        if row["tenant_id"] != tenant_id:
            raise StateTokenInvalidError(
                "state_invalid", "state tenant binding mismatch"
            )
        return tenant_id, payload

    existing = await pool.fetchrow(
        """
        SELECT consumed_at, expires_at, provider
          FROM oauth_install_states
         WHERE nonce = $1
        """,
        nonce,
    )
    if existing is None:
        raise StateTokenInvalidError("state_invalid", "state nonce is unknown")
    if existing["consumed_at"] is not None:
        raise StateTokenInvalidError("state_consumed", "state token already used")
    if expected_provider is not None and existing["provider"] != expected_provider:
        raise StateTokenInvalidError("state_invalid", "state provider mismatch")
    raise StateTokenInvalidError("state_expired", "state token expired")


__all__ = ["issue_state_token", "verify_and_consume_state"]
