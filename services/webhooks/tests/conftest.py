"""services/webhooks/tests/conftest.py — shared fixtures for IN-06 tests.

Each per-provider test file uses synthetic vendor-shaped payloads
signed with locally-generated keys/secrets. We don't ship recorded
production payloads — they would expire (test secrets rotate) and
they add no signal vs. a payload we sign ourselves with the same
algorithm.

For ed25519 (Discord) we generate a fresh keypair per test session
so verification exercises pynacl in the real path.
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time

import pytest

from services.webhooks import metrics


@pytest.fixture(autouse=True)
def _reset_metrics() -> None:
    """Clear the in-process failure counter between tests so each test
    asserts in isolation."""
    metrics.reset()
    yield
    metrics.reset()


@pytest.fixture
def now() -> float:
    """Stable 'now' for replay-window math. Tests inject this into the
    verifier so frozen-clock behavior is deterministic."""
    return 1_700_000_000.0


@pytest.fixture
def slack_secret() -> str:
    return "test-slack-secret-abcdef"


@pytest.fixture
def github_secret() -> str:
    return "test-github-secret-abcdef"


@pytest.fixture
def linear_secret() -> str:
    return "test-linear-secret-abcdef"


@pytest.fixture
def stripe_secret() -> str:
    return "whsec_test_stripe_secret"


def hmac_sha256_hex(key: str, message: bytes) -> str:
    return hmac.new(key.encode("utf-8"), message, hashlib.sha256).hexdigest()


# Helpers exposed to test files via direct import (not fixtures) for
# clarity.


def slack_sign(secret: str, body: bytes, ts: int) -> str:
    basestring = f"v0:{ts}:{body.decode('utf-8')}".encode("utf-8")
    return "v0=" + hmac_sha256_hex(secret, basestring)


def github_sign(secret: str, body: bytes) -> str:
    return "sha256=" + hmac_sha256_hex(secret, body)


def linear_sign(secret: str, body: bytes) -> str:
    return hmac_sha256_hex(secret, body)


def stripe_sign(secret: str, body: bytes, ts: int) -> str:
    sig = hmac_sha256_hex(secret, f"{ts}.".encode("utf-8") + body)
    return f"t={ts},v1={sig}"


def discord_keypair() -> tuple[str, "SigningKey"]:  # type: ignore[name-defined]
    """Generate an ed25519 keypair. Returns (public_key_hex, signing_key).

    Tests use signing_key.sign(...) to produce a signature; the public
    hex is configured as the 'secret' in the verifier registry.
    """
    from nacl.signing import SigningKey

    sk = SigningKey.generate()
    pub_hex = sk.verify_key.encode().hex()
    return pub_hex, sk


__all__ = [
    "hmac_sha256_hex",
    "slack_sign",
    "github_sign",
    "linear_sign",
    "stripe_sign",
    "discord_keypair",
]
