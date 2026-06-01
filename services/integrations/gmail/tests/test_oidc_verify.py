"""Tests for the Google OIDC token verifier.

Generates an RSA keypair, signs a JWT with it, then stubs JWKS so the
verifier picks up our key. Exercises the claim-validation paths
(audience, email, exp, iat, issuer) without depending on Google.
"""
from __future__ import annotations

import base64
import json
import time
from typing import Any

import pytest
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa

from services.webhooks.signatures import google_oidc
from services.webhooks.signatures.google_oidc import (
    GoogleOidcError,
    verify_pubsub_oidc_token,
)


def _b64u(b: bytes) -> str:
    return base64.urlsafe_b64encode(b).rstrip(b"=").decode("ascii")


def _gen_key() -> tuple[rsa.RSAPrivateKey, dict[str, Any]]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pub = key.public_key().public_numbers()
    jwk = {
        "kty": "RSA",
        "alg": "RS256",
        "use": "sig",
        "kid": "test-key-1",
        "n": _b64u(pub.n.to_bytes((pub.n.bit_length() + 7) // 8, "big")),
        "e": _b64u(pub.e.to_bytes((pub.e.bit_length() + 7) // 8, "big")),
    }
    return key, jwk


def _sign(key: rsa.RSAPrivateKey, header: dict[str, Any], payload: dict[str, Any]) -> str:
    header_b = _b64u(json.dumps(header, separators=(",", ":")).encode())
    payload_b = _b64u(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{header_b}.{payload_b}".encode("ascii")
    sig = key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())
    return f"{header_b}.{payload_b}.{_b64u(sig)}"


@pytest.fixture
def stub_jwks(monkeypatch: pytest.MonkeyPatch) -> rsa.RSAPrivateKey:
    key, jwk = _gen_key()
    google_oidc._clear_jwks_cache_for_tests()

    async def fake_load_jwks(*, http_client: Any = None) -> dict[str, dict[str, Any]]:
        return {jwk["kid"]: jwk}

    monkeypatch.setattr(google_oidc, "_load_jwks", fake_load_jwks)
    return key


@pytest.mark.asyncio
async def test_valid_token(stub_jwks: rsa.RSAPrivateKey) -> None:
    now = time.time()
    token = _sign(
        stub_jwks,
        {"alg": "RS256", "typ": "JWT", "kid": "test-key-1"},
        {
            "iss": "https://accounts.google.com",
            "aud": "https://gateway.fyralis.app/webhooks/gmail/pubsub",
            "email": "push@fyralis-prod.iam.gserviceaccount.com",
            "email_verified": True,
            "iat": int(now),
            "exp": int(now + 600),
        },
    )
    payload = await verify_pubsub_oidc_token(
        token=token,
        expected_audience="https://gateway.fyralis.app/webhooks/gmail/pubsub",
        expected_email="push@fyralis-prod.iam.gserviceaccount.com",
        now=now,
    )
    assert payload["email_verified"] is True


@pytest.mark.asyncio
async def test_audience_mismatch(stub_jwks: rsa.RSAPrivateKey) -> None:
    now = time.time()
    token = _sign(
        stub_jwks,
        {"alg": "RS256", "typ": "JWT", "kid": "test-key-1"},
        {
            "iss": "accounts.google.com",
            "aud": "https://wrong.example.com/webhook",
            "email": "push@fyralis.iam.gserviceaccount.com",
            "email_verified": True,
            "iat": int(now),
            "exp": int(now + 600),
        },
    )
    with pytest.raises(GoogleOidcError):
        await verify_pubsub_oidc_token(
            token=token,
            expected_audience="https://right.example.com/webhook",
            expected_email="push@fyralis.iam.gserviceaccount.com",
            now=now,
        )


@pytest.mark.asyncio
async def test_email_mismatch(stub_jwks: rsa.RSAPrivateKey) -> None:
    now = time.time()
    token = _sign(
        stub_jwks,
        {"alg": "RS256", "typ": "JWT", "kid": "test-key-1"},
        {
            "iss": "accounts.google.com",
            "aud": "https://right.example.com/webhook",
            "email": "attacker@evil.com",
            "email_verified": True,
            "iat": int(now),
            "exp": int(now + 600),
        },
    )
    with pytest.raises(GoogleOidcError):
        await verify_pubsub_oidc_token(
            token=token,
            expected_audience="https://right.example.com/webhook",
            expected_email="push@fyralis.iam.gserviceaccount.com",
            now=now,
        )


@pytest.mark.asyncio
async def test_expired_token(stub_jwks: rsa.RSAPrivateKey) -> None:
    now = time.time()
    token = _sign(
        stub_jwks,
        {"alg": "RS256", "typ": "JWT", "kid": "test-key-1"},
        {
            "iss": "accounts.google.com",
            "aud": "https://right.example.com/webhook",
            "email": "push@fyralis.iam.gserviceaccount.com",
            "email_verified": True,
            "iat": int(now - 1200),
            "exp": int(now - 600),
        },
    )
    with pytest.raises(GoogleOidcError):
        await verify_pubsub_oidc_token(
            token=token,
            expected_audience="https://right.example.com/webhook",
            expected_email="push@fyralis.iam.gserviceaccount.com",
            now=now,
        )


@pytest.mark.asyncio
async def test_email_not_verified(stub_jwks: rsa.RSAPrivateKey) -> None:
    now = time.time()
    token = _sign(
        stub_jwks,
        {"alg": "RS256", "typ": "JWT", "kid": "test-key-1"},
        {
            "iss": "accounts.google.com",
            "aud": "https://right.example.com/webhook",
            "email": "push@fyralis.iam.gserviceaccount.com",
            "email_verified": False,
            "iat": int(now),
            "exp": int(now + 600),
        },
    )
    with pytest.raises(GoogleOidcError):
        await verify_pubsub_oidc_token(
            token=token,
            expected_audience="https://right.example.com/webhook",
            expected_email="push@fyralis.iam.gserviceaccount.com",
            now=now,
        )


@pytest.mark.asyncio
async def test_unknown_kid(stub_jwks: rsa.RSAPrivateKey) -> None:
    now = time.time()
    token = _sign(
        stub_jwks,
        {"alg": "RS256", "typ": "JWT", "kid": "not-the-right-kid"},
        {
            "iss": "accounts.google.com",
            "aud": "https://right.example.com/webhook",
            "email": "push@fyralis.iam.gserviceaccount.com",
            "email_verified": True,
            "iat": int(now),
            "exp": int(now + 600),
        },
    )
    with pytest.raises(GoogleOidcError):
        await verify_pubsub_oidc_token(
            token=token,
            expected_audience="https://right.example.com/webhook",
            expected_email="push@fyralis.iam.gserviceaccount.com",
            now=now,
        )
