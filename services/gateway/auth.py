"""services/gateway/auth.py — bearer-token session management.

BUILD-PLAN §3 Prompt 2.A:
    "Auth middleware: reads Bearer token, resolves to actor_id via
     actor_sessions table (create this: id, actor_id, token_hash,
     expires_at)."

Design:
- Token is an opaque UUID v7 *string*. Clients never see the hash.
- Storage hashes `token = SHA-256(token_str)` to prevent an attacker
  who gains read-only DB access from impersonating live sessions.
- `validate_token(token_str) -> AuthContext | None` is async and
  used by the auth middleware. Expired / revoked sessions return None.
- `create_session(actor_id, tenant_id, ttl)` mints a UUID v7 token
  and inserts the hash. Returned token string is shown to the client
  exactly once.

Schema refs:
- migrations/0003_actor_sessions.sql (not in SCHEMA-LOCK.md — Gateway
  local; drift check extended accordingly).
- S5.1 `actors` (FK target for actor_sessions.actor_id).
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7


DEFAULT_SESSION_TTL = timedelta(hours=24)


@dataclass(frozen=True)
class AuthContext:
    """Resolved actor + tenant for an authenticated request."""

    session_id: UUID
    actor_id: UUID
    tenant_id: UUID
    expires_at: datetime


def hash_token(token: str) -> str:
    """SHA-256 of the token string, hex-encoded for TEXT storage.

    UUID strings are ASCII; the encoding choice is purely cosmetic.
    """
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def new_token() -> str:
    """Mint an opaque token — a UUID v7 in canonical string form.

    Time-sortable per BUILD-PLAN §0 non-negotiable #7. The client
    only sees this; the DB only stores its SHA-256.
    """
    return str(uuid7())


async def create_session(
    pool: asyncpg.Pool,
    *,
    actor_id: UUID,
    tenant_id: UUID,
    ttl: timedelta | None = None,
    now: datetime | None = None,
) -> tuple[str, AuthContext]:
    """Mint a new session and return (token_str, AuthContext).

    The token string is shown to the caller exactly once; reconstruct
    the AuthContext from the DB via `validate_token` on subsequent
    requests. Expiration defaults to 24 hours.
    """
    ttl = ttl or DEFAULT_SESSION_TTL
    now = now or datetime.now(timezone.utc)
    expires_at = now + ttl
    session_id = uuid7()
    token_str = new_token()
    token_h = hash_token(token_str)

    await pool.execute(
        """
        INSERT INTO actor_sessions (
            id, tenant_id, actor_id, token_hash, expires_at
        ) VALUES ($1, $2, $3, $4, $5)
        """,
        session_id,
        tenant_id,
        actor_id,
        token_h,
        expires_at,
    )
    ctx = AuthContext(
        session_id=session_id,
        actor_id=actor_id,
        tenant_id=tenant_id,
        expires_at=expires_at,
    )
    return token_str, ctx


async def validate_token(
    pool: asyncpg.Pool,
    token_str: str,
    *,
    now: datetime | None = None,
) -> Optional[AuthContext]:
    """Look up `token_str` against `actor_sessions`. Return context
    or None when the token is unknown, expired, or revoked.

    Distinguishing the three cases is deliberately avoided — the
    caller should return the same 401 for all of them (no oracle).
    """
    now = now or datetime.now(timezone.utc)
    token_h = hash_token(token_str)
    row = await pool.fetchrow(
        """
        SELECT id, actor_id, tenant_id, expires_at, revoked_at
        FROM actor_sessions
        WHERE token_hash = $1
        """,
        token_h,
    )
    if row is None:
        return None
    if row["revoked_at"] is not None:
        return None
    if row["expires_at"] <= now:
        return None
    return AuthContext(
        session_id=row["id"],
        actor_id=row["actor_id"],
        tenant_id=row["tenant_id"],
        expires_at=row["expires_at"],
    )


async def revoke_session(pool: asyncpg.Pool, session_id: UUID) -> bool:
    """Revoke a session by id. Returns True if a row was updated."""
    result = await pool.execute(
        """
        UPDATE actor_sessions
        SET revoked_at = now()
        WHERE id = $1 AND revoked_at IS NULL
        """,
        session_id,
    )
    # asyncpg execute returns e.g. "UPDATE 1"
    return result.strip().endswith("1")


__all__ = [
    "AuthContext",
    "DEFAULT_SESSION_TTL",
    "hash_token",
    "new_token",
    "create_session",
    "validate_token",
    "revoke_session",
]
