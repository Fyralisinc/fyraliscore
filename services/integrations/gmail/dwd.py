"""services/integrations/gmail/dwd.py — DWD service-account loader + JIT token mint.

A Workspace super-admin grants our service account the right to
impersonate users in their domain at admin-chosen scopes
(Admin Console → Security → API Controls → Domain-wide Delegation).
At ingest time we mint a per-user, scope-bound bearer token via the
JWT-bearer grant flow:

    POST https://oauth2.googleapis.com/token
        grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer
        assertion=<signed JWT>

The signed JWT carries:
    iss = service-account-email
    sub = impersonated-user-email
    scope = space-separated scopes (e.g. "https://…/gmail.metadata")
    aud = https://oauth2.googleapis.com/token
    iat / exp

Properties:
- Service-account private key is loaded from KMS or the local secret
  store ONCE per process and never logged.
- Minted access tokens live in memory only, cached by
  (service_account_email, user_email, frozenset(scopes)) with a TTL a
  bit shorter than Google's ~3600s lifetime.
- Tokens are never persisted to disk or DB.
- A per-(service_account, user) `asyncio.Lock` prevents a thundering
  herd when many concurrent workers ask for the same impersonated token.
"""
from __future__ import annotations

import asyncio
import base64
import json
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx

from lib.shared.errors import CompanyOSError


_TOKEN_URI = "https://oauth2.googleapis.com/token"
_GRANT_TYPE = "urn:ietf:params:oauth:grant-type:jwt-bearer"

# JWT lifetime — Google requires <= 1h. We use 50m to leave headroom.
_JWT_LIFETIME_S = 50 * 60
# Cache tokens for the JWT lifetime minus 5 minutes of clock skew.
_TOKEN_TTL_HEADROOM_S = 5 * 60


class DwdError(CompanyOSError):
    default_code = "gmail_dwd_error"


@dataclass(frozen=True)
class ServiceAccountKey:
    """Subset of the service-account JSON Google issues."""
    client_email: str
    private_key_pem: str
    private_key_id: str
    token_uri: str = _TOKEN_URI

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ServiceAccountKey":
        try:
            return cls(
                client_email=data["client_email"],
                private_key_pem=data["private_key"],
                private_key_id=data.get("private_key_id", ""),
                token_uri=data.get("token_uri", _TOKEN_URI),
            )
        except KeyError as exc:
            raise DwdError(
                f"service-account JSON missing required key {exc.args[0]!r}",
            ) from exc

    @classmethod
    def from_env(cls) -> "ServiceAccountKey":
        """Load the service-account JSON.

        Resolution order:
          1. GMAIL_SERVICE_ACCOUNT_JSON_FILE → path to a JSON file
          2. GMAIL_SERVICE_ACCOUNT_JSON      → JSON inline
          3. raise DwdError

        Production deployments are expected to wire option 1 to a
        KMS-mounted secret path (e.g. /var/run/secrets/gmail-sa.json)
        so the key never appears in process env.
        """
        path = os.environ.get("GMAIL_SERVICE_ACCOUNT_JSON_FILE")
        if path:
            try:
                data = json.loads(Path(path).read_text())
            except OSError as exc:
                raise DwdError(
                    f"could not read GMAIL_SERVICE_ACCOUNT_JSON_FILE: {exc}",
                ) from exc
            except json.JSONDecodeError as exc:
                raise DwdError(
                    f"GMAIL_SERVICE_ACCOUNT_JSON_FILE is not valid JSON: {exc}",
                ) from exc
            return cls.from_dict(data)

        inline = os.environ.get("GMAIL_SERVICE_ACCOUNT_JSON")
        if inline:
            try:
                data = json.loads(inline)
            except json.JSONDecodeError as exc:
                raise DwdError(
                    f"GMAIL_SERVICE_ACCOUNT_JSON is not valid JSON: {exc}",
                ) from exc
            return cls.from_dict(data)

        raise DwdError(
            "no service-account credentials configured; set "
            "GMAIL_SERVICE_ACCOUNT_JSON_FILE or GMAIL_SERVICE_ACCOUNT_JSON",
        )


@dataclass
class _CachedToken:
    access_token: str
    expires_at: float  # epoch seconds


class DwdTokenMinter:
    """Mints impersonated bearer tokens for (service_account, user, scopes).

    Single instance per process; safe to share across asyncio tasks.
    """

    def __init__(
        self,
        key: ServiceAccountKey,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        self._key = key
        self._client = http_client
        self._owns_client = http_client is None
        self._cache: dict[tuple[str, str, frozenset[str]], _CachedToken] = {}
        self._locks: dict[tuple[str, str, frozenset[str]], asyncio.Lock] = {}

    async def __aenter__(self) -> "DwdTokenMinter":
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    @property
    def service_account_email(self) -> str:
        return self._key.client_email

    async def mint(
        self,
        *,
        user_email: str,
        scopes: list[str] | tuple[str, ...],
        now: float | None = None,
    ) -> str:
        """Return a fresh bearer access token impersonating `user_email`."""
        if not user_email:
            raise DwdError("user_email is required")
        if not scopes:
            raise DwdError("at least one scope is required")
        cache_key = (self._key.client_email, user_email, frozenset(scopes))
        t = now if now is not None else time.time()

        cached = self._cache.get(cache_key)
        if cached is not None and cached.expires_at - _TOKEN_TTL_HEADROOM_S > t:
            return cached.access_token

        lock = self._locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            # Re-check after lock — another task may have refreshed while we
            # waited.
            cached = self._cache.get(cache_key)
            t = time.time()
            if cached is not None and cached.expires_at - _TOKEN_TTL_HEADROOM_S > t:
                return cached.access_token

            assertion = self._sign_jwt(user_email=user_email, scopes=scopes, now=t)
            token, expires_in = await self._exchange(assertion)
            self._cache[cache_key] = _CachedToken(
                access_token=token, expires_at=t + expires_in,
            )
            return token

    def invalidate(self, *, user_email: str, scopes: list[str] | tuple[str, ...]) -> None:
        """Drop a cached token. Called on 401 from a downstream Google API."""
        cache_key = (self._key.client_email, user_email, frozenset(scopes))
        self._cache.pop(cache_key, None)

    # -----------------------------------------------------------------
    # JWT signing — RS256 over the service-account private key.
    # -----------------------------------------------------------------
    def _sign_jwt(
        self,
        *,
        user_email: str,
        scopes: list[str] | tuple[str, ...],
        now: float,
    ) -> str:
        header = {"alg": "RS256", "typ": "JWT", "kid": self._key.private_key_id}
        payload = {
            "iss": self._key.client_email,
            "sub": user_email,
            "scope": " ".join(scopes),
            "aud": self._key.token_uri,
            "iat": int(now),
            "exp": int(now + _JWT_LIFETIME_S),
        }
        header_b64 = _b64u(json.dumps(header, separators=(",", ":")).encode())
        payload_b64 = _b64u(json.dumps(payload, separators=(",", ":")).encode())
        signing_input = f"{header_b64}.{payload_b64}".encode()
        signature = _rs256_sign(self._key.private_key_pem, signing_input)
        return f"{header_b64}.{payload_b64}.{_b64u(signature)}"

    async def _exchange(self, assertion: str) -> tuple[str, int]:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=10.0)
        resp = await self._client.post(
            self._key.token_uri,
            data={"grant_type": _GRANT_TYPE, "assertion": assertion},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )
        if resp.status_code != 200:
            # NEVER echo the assertion or any request detail — it
            # contains the impersonated user identity.
            raise DwdError(
                f"token exchange failed: status={resp.status_code} "
                f"body={resp.text[:200]!r}",
                status=resp.status_code,
            )
        body = resp.json()
        token = body.get("access_token")
        ttl = int(body.get("expires_in", _JWT_LIFETIME_S))
        if not token:
            raise DwdError("token endpoint returned no access_token")
        return token, ttl


def _b64u(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _rs256_sign(private_key_pem: str, signing_input: bytes) -> bytes:
    """Sign `signing_input` with the RSA private key in `private_key_pem`.

    Imported lazily so the import cost is paid only by processes that
    actually mint tokens.
    """
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding

    key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"), password=None,
    )
    return key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())


# ---------------------------------------------------------------------
# Process-wide singleton — every code path that needs an impersonated
# token reaches for `get_minter()`.
# ---------------------------------------------------------------------
_MINTER: DwdTokenMinter | None = None


def get_minter() -> DwdTokenMinter:
    global _MINTER
    if _MINTER is None:
        _MINTER = DwdTokenMinter(ServiceAccountKey.from_env())
    return _MINTER


def _reset_minter_for_tests() -> None:
    """Test-only — drop the cached singleton so a fresh key can be loaded."""
    global _MINTER
    _MINTER = None


__all__ = [
    "DwdError",
    "DwdTokenMinter",
    "ServiceAccountKey",
    "get_minter",
]
