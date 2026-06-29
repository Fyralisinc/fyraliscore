"""services/platform/extensions/identity.py — extension OAuth2 identity (DP1.4).

Two concerns, kept separate from the per-tenant capability grant (`grants.py`):

  1. **Client store** (`ExtensionOAuthClientsRepo`) — register / verify / rotate /
     revoke OAuth2 client credentials for an extension, backed by
     `extension_oauth_clients` (migration 0128). The plaintext secret is returned
     once to the operator flow; only a PBKDF2-SHA256 verifier is stored.
  2. **Access tokens** — mint/verify short-lived bearer JWTs (HS256) carrying the
     extension id + environment. The token proves *who is calling*; *what they may
     do* for a given tenant is resolved per-request from `extension_grants`
     (`access.resolve_capabilities`) so a token can never out-live a revoked grant.

The host signs with ``EXTENSION_JWT_SECRET`` (a stable per-deployment secret). If
unset, an ephemeral process secret is generated with a loud warning — fine for
local dev (tokens don't survive a restart), MUST be set in production.
"""
from __future__ import annotations

import hashlib
import hmac
import logging
import os
import secrets
import time
from dataclasses import dataclass
from typing import Any

import jwt

log = logging.getLogger("extensions.identity")

_ALGO = "HS256"
_TTL = int(os.environ.get("EXTENSION_TOKEN_TTL_SECONDS", "3600"))
_PBKDF2_ITER = 240_000
_ephemeral_secret: str | None = None


class IdentityError(Exception):
    """Token/credential verification failed."""


@dataclass(frozen=True)
class ExtensionPrincipal:
    """The authenticated caller behind a verified bearer token."""

    extension_id: str
    environment: str = "production"


# --- signing key ------------------------------------------------------------------
def _signing_key() -> bytes:
    """A 32-byte HS256 key derived (SHA-256) from the configured secret, so any
    operator-chosen secret length is safe (PyJWT rejects <32-byte HMAC keys)."""
    global _ephemeral_secret
    s = os.environ.get("EXTENSION_JWT_SECRET")
    if not s:
        if _ephemeral_secret is None:
            _ephemeral_secret = secrets.token_urlsafe(48)
            log.warning(
                "EXTENSION_JWT_SECRET unset — using an EPHEMERAL signing secret; "
                "extension tokens will not survive a restart. Set it in production."
            )
        s = _ephemeral_secret
    return hashlib.sha256(s.encode()).digest()


# --- secret hashing (PBKDF2-SHA256) -----------------------------------------------
def hash_secret(secret: str, *, salt: bytes | None = None, iterations: int = _PBKDF2_ITER) -> str:
    salt = salt or secrets.token_bytes(16)
    dk = hashlib.pbkdf2_hmac("sha256", secret.encode(), salt, iterations)
    return f"pbkdf2${iterations}${salt.hex()}${dk.hex()}"


def verify_secret(secret: str, stored: str) -> bool:
    try:
        scheme, iter_s, salt_hex, hash_hex = stored.split("$", 3)
        if scheme != "pbkdf2":
            return False
        dk = hashlib.pbkdf2_hmac("sha256", secret.encode(), bytes.fromhex(salt_hex), int(iter_s))
        return hmac.compare_digest(dk.hex(), hash_hex)
    except (ValueError, TypeError):
        return False


# --- access tokens ----------------------------------------------------------------
def mint_access_token(extension_id: str, *, environment: str = "production") -> dict[str, Any]:
    now = int(time.time())
    claims = {
        "sub": extension_id, "env": environment, "typ": "ext",
        "iat": now, "exp": now + _TTL,
    }
    token = jwt.encode(claims, _signing_key(), algorithm=_ALGO)
    return {"access_token": token, "token_type": "Bearer", "expires_in": _TTL}


def verify_access_token(token: str) -> ExtensionPrincipal:
    try:
        claims = jwt.decode(token, _signing_key(), algorithms=[_ALGO])
    except jwt.PyJWTError as exc:  # expired / bad signature / malformed
        raise IdentityError(f"invalid extension token: {exc}") from exc
    if claims.get("typ") != "ext" or not claims.get("sub"):
        raise IdentityError("token is not an extension access token")
    return ExtensionPrincipal(extension_id=claims["sub"], environment=claims.get("env", "production"))


# --- client store -----------------------------------------------------------------
@dataclass(frozen=True)
class RegisteredClient:
    client_id: str
    client_secret: str  # plaintext — returned ONCE, never persisted
    extension_id: str
    environment: str
    webhook_secret: str = ""  # plaintext shared secret for webhook HMAC (stored host-side)


class ExtensionOAuthClientsRepo:
    """CRUD + credential verification over `extension_oauth_clients`."""

    def __init__(self, pool: Any) -> None:
        self.pool = pool

    async def register(
        self, *, extension_id: str, created_by: str, environment: str = "production",
        display_name: str | None = None, callback_url: str | None = None,
    ) -> RegisteredClient:
        client_id = "ext_" + secrets.token_hex(12)
        client_secret = secrets.token_urlsafe(32)
        webhook_secret = "whsec_" + secrets.token_urlsafe(32)
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO extension_oauth_clients "
                "(client_id, extension_id, environment, client_secret_hash, "
                " display_name, callback_url, created_by, webhook_secret) "
                "VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                client_id, extension_id, environment, hash_secret(client_secret),
                display_name, callback_url, created_by, webhook_secret,
            )
        return RegisteredClient(client_id, client_secret, extension_id, environment, webhook_secret)

    async def webhook_target(
        self, extension_id: str, *, environment: str | None = None
    ) -> tuple[str, str] | None:
        """Return (callback_url, webhook_secret) for the extension's most recent
        active client with a callback. ``environment`` optionally narrows the match
        (the egress row doesn't carry one, so delivery looks up by extension)."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT callback_url, webhook_secret FROM extension_oauth_clients "
                "WHERE extension_id=$1 AND ($2::text IS NULL OR environment=$2) "
                "AND callback_url IS NOT NULL AND revoked_at IS NULL "
                "ORDER BY created_at DESC LIMIT 1",
                extension_id, environment,
            )
        if row is None or not row["callback_url"]:
            return None
        return row["callback_url"], row["webhook_secret"] or ""

    async def verify_credentials(self, client_id: str, client_secret: str) -> ExtensionPrincipal | None:
        """Return the principal for valid, non-revoked credentials, else None."""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT extension_id, environment, client_secret_hash, revoked_at "
                "FROM extension_oauth_clients WHERE client_id=$1",
                client_id,
            )
        if row is None or row["revoked_at"] is not None:
            return None
        if not verify_secret(client_secret, row["client_secret_hash"]):
            return None
        # best-effort last-used stamp (separate connection; ignore failure)
        try:
            async with self.pool.acquire() as conn:
                await conn.execute(
                    "UPDATE extension_oauth_clients SET last_token_at=now() WHERE client_id=$1",
                    client_id,
                )
        except Exception:  # noqa: BLE001
            pass
        return ExtensionPrincipal(row["extension_id"], row["environment"])

    async def rotate_secret(self, client_id: str) -> str | None:
        new_secret = secrets.token_urlsafe(32)
        async with self.pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE extension_oauth_clients SET client_secret_hash=$2, rotated_at=now() "
                "WHERE client_id=$1 AND revoked_at IS NULL",
                client_id, hash_secret(new_secret),
            )
        return new_secret if status.endswith("1") else None

    async def revoke(self, client_id: str) -> bool:
        async with self.pool.acquire() as conn:
            status = await conn.execute(
                "UPDATE extension_oauth_clients SET revoked_at=now() "
                "WHERE client_id=$1 AND revoked_at IS NULL",
                client_id,
            )
        return status.endswith("1")

    async def callback_url(self, extension_id: str, *, environment: str = "production") -> str | None:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT callback_url FROM extension_oauth_clients "
                "WHERE extension_id=$1 AND environment=$2 AND revoked_at IS NULL "
                "ORDER BY created_at DESC LIMIT 1",
                extension_id, environment,
            )


__all__ = [
    "IdentityError", "ExtensionPrincipal", "RegisteredClient",
    "ExtensionOAuthClientsRepo", "hash_secret", "verify_secret",
    "mint_access_token", "verify_access_token",
]
