"""Discord verifier tests — ed25519 path via pynacl.

Spec: US4 / FR-008 / SC-002, SC-003.
"""
from __future__ import annotations

import pytest

pytest.importorskip("nacl", reason="pynacl required for Discord ed25519 path")

from services.webhooks.signatures.discord import verifier
from services.webhooks.tests.conftest import discord_keypair
from services.webhooks.verifier import Secret, WebhookVerificationError


@pytest.mark.asyncio
async def test_happy_path(now: float) -> None:
    pub_hex, sk = discord_keypair()
    body = b'{"type":2,"id":"123","application_id":"a","data":{"name":"ping"}}'
    ts = str(int(now))
    sig = sk.sign(ts.encode("utf-8") + body).signature.hex()

    ctx = await verifier.verify(
        body=body,
        headers={
            "X-Signature-Ed25519": sig,
            "X-Signature-Timestamp": ts,
        },
        secrets=[Secret("discord", pub_hex, label="primary")],
        now=now,
    )
    assert ctx.provider == "discord"
    assert ctx.secret_label == "primary"
    assert ctx.signed_timestamp == int(ts)


@pytest.mark.asyncio
async def test_tampered_body_rejected(now: float) -> None:
    pub_hex, sk = discord_keypair()
    body = b'{"type":2,"id":"123"}'
    ts = str(int(now))
    sig = sk.sign(ts.encode("utf-8") + body).signature.hex()
    tampered = body[:-1] + b" "

    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=tampered,
            headers={
                "X-Signature-Ed25519": sig,
                "X-Signature-Timestamp": ts,
            },
            secrets=[Secret("discord", pub_hex)],
            now=now,
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_tampered_timestamp(now: float) -> None:
    pub_hex, sk = discord_keypair()
    body = b'{"type":2,"id":"123"}'
    ts = str(int(now))
    sig = sk.sign(ts.encode("utf-8") + body).signature.hex()

    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={
                "X-Signature-Ed25519": sig,
                "X-Signature-Timestamp": str(int(now) + 1),  # change ts
            },
            secrets=[Secret("discord", pub_hex)],
            now=now,
        )
    # Either signature_mismatch or expired_timestamp acceptable.
    assert exc.value.reason in ("signature_mismatch", "expired_timestamp")


@pytest.mark.asyncio
async def test_replay_window(now: float) -> None:
    pub_hex, sk = discord_keypair()
    body = b'{"type":2,"id":"123"}'
    ts = str(int(now) - 1000)
    sig = sk.sign(ts.encode("utf-8") + body).signature.hex()

    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=body,
            headers={
                "X-Signature-Ed25519": sig,
                "X-Signature-Timestamp": ts,
            },
            secrets=[Secret("discord", pub_hex)],
            now=now,
        )
    assert exc.value.reason == "expired_timestamp"


@pytest.mark.asyncio
async def test_missing_header(now: float) -> None:
    pub_hex, _ = discord_keypair()
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={"X-Signature-Timestamp": str(int(now))},
            secrets=[Secret("discord", pub_hex)],
            now=now,
        )
    assert exc.value.reason == "missing_signature_header"


@pytest.mark.asyncio
async def test_malformed_hex_signature(now: float) -> None:
    pub_hex, _ = discord_keypair()
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={
                "X-Signature-Ed25519": "not-hex-zzzz",
                "X-Signature-Timestamp": str(int(now)),
            },
            secrets=[Secret("discord", pub_hex)],
            now=now,
        )
    assert exc.value.reason == "malformed_signature_header"


@pytest.mark.asyncio
async def test_no_secret(now: float) -> None:
    with pytest.raises(WebhookVerificationError) as exc:
        await verifier.verify(
            body=b"{}",
            headers={
                "X-Signature-Ed25519": "00" * 64,
                "X-Signature-Timestamp": str(int(now)),
            },
            secrets=[],
            now=now,
        )
    assert exc.value.reason == "secret_not_configured"
