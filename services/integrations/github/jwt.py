"""services/integrations/github/jwt.py — GitHub App JWT minter.

GitHub App authentication requires a short-lived JWT signed RS256 with
the App's PEM private key, sent as `Authorization: Bearer <jwt>` to
`POST /app/installations/<id>/access_tokens` to obtain a per-installation
access token.

Operator-facing config (FR-020, Clarifications Q3):
  - GITHUB_APP_ID                — numeric App ID (string).
  - GITHUB_APP_PRIVATE_KEY       — multi-line PEM literal, OR
  - GITHUB_APP_PRIVATE_KEY_PATH  — filesystem path to a `.pem` file.
  Exactly one of the two key vars MUST be set under `FYRALIS_ENV=prod`.

The private key is read on EVERY mint (no in-process cache); rotation
is a no-op deploy. The key material is NEVER logged at any level.
"""
from __future__ import annotations

import os
import time
from typing import Any

import jwt as pyjwt  # PyJWT — RS256 via [crypto] extra
from cryptography.hazmat.primitives.serialization import load_pem_private_key

from lib.shared.errors import GithubJWTError


_DEFAULT_TTL_SECONDS = 600  # GitHub enforces <= 10 minutes
_DEFAULT_SKEW_SECONDS = 30   # iat back-dated 30s per GitHub recommendation


def _load_private_key_pem() -> bytes:
    """Read the PEM material from env / file. Returns the raw bytes;
    parsing/validation happens in `mint_app_jwt`.

    Raises:
        GithubJWTError(reason='no_private_key'|'conflicting_keys'|'io_error')
    """
    pem_inline = os.environ.get("GITHUB_APP_PRIVATE_KEY", "")
    pem_path = os.environ.get("GITHUB_APP_PRIVATE_KEY_PATH", "")

    have_inline = bool(pem_inline.strip())
    have_path = bool(pem_path.strip())

    if have_inline and have_path:
        raise GithubJWTError(
            "conflicting_keys",
            "both GITHUB_APP_PRIVATE_KEY and GITHUB_APP_PRIVATE_KEY_PATH "
            "are set; exactly one must be configured",
        )
    if not have_inline and not have_path:
        raise GithubJWTError(
            "no_private_key",
            "neither GITHUB_APP_PRIVATE_KEY nor GITHUB_APP_PRIVATE_KEY_PATH "
            "is configured",
        )

    if have_inline:
        return pem_inline.encode("utf-8")

    try:
        with open(pem_path, "rb") as fh:
            return fh.read()
    except OSError as exc:
        raise GithubJWTError(
            "io_error",
            f"failed to read GITHUB_APP_PRIVATE_KEY_PATH={pem_path!r}",
            error_type=type(exc).__name__,
        ) from exc


def mint_app_jwt(
    *,
    app_id: str | None = None,
    now: float | None = None,
    ttl_seconds: int = _DEFAULT_TTL_SECONDS,
) -> str:
    """Mint a 10-minute App JWT signed RS256 with the App's private key.

    Reads `GITHUB_APP_ID` and the PEM material on every call (no
    in-process cache). The JWT payload follows GitHub's contract:
        {iat: now-30, exp: now+ttl_seconds, iss: app_id}

    The 30-second `iat` back-date is GitHub's documented skew tolerance.
    `ttl_seconds` is capped at GitHub's 600s maximum.

    Raises:
        GithubJWTError on any misconfiguration. The error's `reason`
        discriminates the failure mode for logging.
    """
    resolved_app_id = app_id or os.environ.get("GITHUB_APP_ID", "").strip()
    if not resolved_app_id:
        raise GithubJWTError(
            "no_app_id",
            "GITHUB_APP_ID env var is not set",
        )

    if ttl_seconds <= 0 or ttl_seconds > _DEFAULT_TTL_SECONDS:
        # Silently clamp rather than raising — caller mistakes here are
        # always recoverable, and a hard error would block mint flows.
        ttl_seconds = _DEFAULT_TTL_SECONDS

    pem_bytes = _load_private_key_pem()
    try:
        # Verify the key is parseable RSA. PyJWT accepts the raw PEM in
        # `key=` and parses it internally; we pre-parse here so a malformed
        # key surfaces as GithubJWTError(reason='malformed_key') rather
        # than PyJWT's `InvalidKeyError`.
        load_pem_private_key(pem_bytes, password=None)
    except Exception as exc:  # noqa: BLE001 — wrap any parse failure
        raise GithubJWTError(
            "malformed_key",
            "GITHUB_APP_PRIVATE_KEY (or _PATH) is not a parseable PEM RSA "
            "private key",
            error_type=type(exc).__name__,
        ) from exc

    issued_at = (now if now is not None else time.time()) - _DEFAULT_SKEW_SECONDS
    payload: dict[str, Any] = {
        "iat": int(issued_at),
        "exp": int(issued_at + ttl_seconds + _DEFAULT_SKEW_SECONDS),
        "iss": resolved_app_id,
    }
    return pyjwt.encode(payload, pem_bytes, algorithm="RS256")


__all__ = ["mint_app_jwt"]
