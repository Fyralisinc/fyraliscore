"""services/webhooks/signatures/google_oidc.py — verify Google-signed OIDC tokens.

Google Pub/Sub push attaches an OIDC token in the `Authorization`
header. Verification:

    1. Fetch Google's public OAuth2 v3 JWKS.
    2. Parse the JWT header to get `kid`.
    3. Find the matching public key.
    4. Verify the signature (RS256).
    5. Verify claims: iss in {"https://accounts.google.com", "accounts.google.com"},
       aud == our configured audience, exp > now (with skew),
       email == configured push service-account email,
       email_verified == true.

This module ONLY verifies — it does not authorize. The push-handler
maps subscription_name → tenant_id; the OIDC token only proves that
the request came from Google's Pub/Sub on behalf of *some* configured
push identity.
"""
from __future__ import annotations

import base64
import json
import time
from dataclasses import dataclass
from typing import Any

import httpx

from lib.shared.errors import CompanyOSError


_JWKS_URL = "https://www.googleapis.com/oauth2/v3/certs"
# Acceptable issuers per Google docs.
_ALLOWED_ISSUERS = frozenset({
    "accounts.google.com",
    "https://accounts.google.com",
})


class GoogleOidcError(CompanyOSError):
    default_code = "google_oidc_invalid"


@dataclass
class _CachedJwks:
    keys: dict[str, dict[str, Any]]   # kid → JWK
    fetched_at: float
    ttl_s: int = 3600


_JWKS_CACHE: _CachedJwks | None = None


async def _load_jwks(*, http_client: httpx.AsyncClient | None = None) -> dict[str, dict[str, Any]]:
    """Fetch and memoize Google's JWKS. TTL is conservative (1 hour);
    a 401 from verification triggers a forced refresh in callers."""
    global _JWKS_CACHE
    now = time.time()
    if _JWKS_CACHE is not None and (now - _JWKS_CACHE.fetched_at) < _JWKS_CACHE.ttl_s:
        return _JWKS_CACHE.keys
    owns = http_client is None
    client = http_client or httpx.AsyncClient(timeout=10.0)
    try:
        resp = await client.get(_JWKS_URL)
        if resp.status_code != 200:
            raise GoogleOidcError(f"jwks fetch failed: {resp.status_code}")
        body = resp.json()
        keys = {k["kid"]: k for k in body.get("keys", []) if "kid" in k}
        _JWKS_CACHE = _CachedJwks(keys=keys, fetched_at=now)
        return keys
    finally:
        if owns:
            await client.aclose()


def _b64u_decode(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _decode_header_and_payload(token: str) -> tuple[dict[str, Any], dict[str, Any], bytes, bytes]:
    parts = token.split(".")
    if len(parts) != 3:
        raise GoogleOidcError("malformed JWT (expected 3 dot-separated segments)")
    header_b, payload_b, sig_b = parts
    try:
        header = json.loads(_b64u_decode(header_b))
        payload = json.loads(_b64u_decode(payload_b))
        signature = _b64u_decode(sig_b)
    except (ValueError, json.JSONDecodeError) as exc:
        raise GoogleOidcError(f"could not decode JWT: {exc}") from exc
    signing_input = f"{header_b}.{payload_b}".encode("ascii")
    return header, payload, signature, signing_input


def _verify_signature_rs256(jwk: dict[str, Any], signing_input: bytes, signature: bytes) -> None:
    """RS256 verify using cryptography. `jwk` is a Google v3 JWK
    ({kty: RSA, n, e})."""
    from cryptography.hazmat.primitives import hashes
    from cryptography.hazmat.primitives.asymmetric import padding, rsa
    from cryptography.exceptions import InvalidSignature

    n_int = int.from_bytes(_b64u_decode(jwk["n"]), "big")
    e_int = int.from_bytes(_b64u_decode(jwk["e"]), "big")
    pub = rsa.RSAPublicNumbers(e=e_int, n=n_int).public_key()
    try:
        pub.verify(signature, signing_input, padding.PKCS1v15(), hashes.SHA256())
    except InvalidSignature as exc:
        raise GoogleOidcError("JWT signature verification failed") from exc


async def verify_pubsub_oidc_token(
    *,
    token: str,
    expected_audience: str,
    expected_email: str,
    leeway_s: int = 60,
    http_client: httpx.AsyncClient | None = None,
    now: float | None = None,
) -> dict[str, Any]:
    """Validate a Pub/Sub-issued OIDC token. Returns the verified
    payload on success; raises GoogleOidcError otherwise.

    - `expected_audience` must match the configured push audience
      (typically the webhook URL).
    - `expected_email` must match the service account configured on the
      push subscription's oidcToken — proves the push came from a
      subscription we own.
    """
    if not token:
        raise GoogleOidcError("missing JWT")
    header, payload, signature, signing_input = _decode_header_and_payload(token)
    if header.get("alg") != "RS256":
        raise GoogleOidcError(f"unsupported JWT alg: {header.get('alg')!r}")
    kid = header.get("kid")
    if not kid:
        raise GoogleOidcError("JWT header missing kid")

    keys = await _load_jwks(http_client=http_client)
    jwk = keys.get(kid)
    if jwk is None:
        # Forced refresh in case Google rotated keys.
        global _JWKS_CACHE
        _JWKS_CACHE = None
        keys = await _load_jwks(http_client=http_client)
        jwk = keys.get(kid)
    if jwk is None:
        raise GoogleOidcError(f"no JWKS key for kid={kid!r}")

    _verify_signature_rs256(jwk, signing_input, signature)

    # Claims.
    iss = payload.get("iss")
    if iss not in _ALLOWED_ISSUERS:
        raise GoogleOidcError(f"unexpected issuer: {iss!r}")
    aud = payload.get("aud")
    if aud != expected_audience:
        raise GoogleOidcError(f"audience mismatch: got {aud!r}, want {expected_audience!r}")
    t = now if now is not None else time.time()
    exp = payload.get("exp")
    if not isinstance(exp, (int, float)) or exp + leeway_s < t:
        raise GoogleOidcError("token expired")
    iat = payload.get("iat")
    if isinstance(iat, (int, float)) and iat - leeway_s > t:
        raise GoogleOidcError("token issued in the future")
    email = payload.get("email")
    if email != expected_email:
        raise GoogleOidcError(f"email mismatch: got {email!r}, want {expected_email!r}")
    if payload.get("email_verified") is not True:
        raise GoogleOidcError("token email_verified=false")

    return payload


def _clear_jwks_cache_for_tests() -> None:
    global _JWKS_CACHE
    _JWKS_CACHE = None


__all__ = ["GoogleOidcError", "verify_pubsub_oidc_token"]
