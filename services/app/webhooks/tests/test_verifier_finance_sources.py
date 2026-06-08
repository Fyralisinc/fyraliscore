"""HMAC verifier round-trip tests for the finance/payroll sources
(brex, ramp, gusto, deel — IN-FIN2).

These four verifiers are clones of the Mercury (brex, deel) and QuickBooks
(ramp, gusto) archetypes. Each exposes its signing scheme as module constants so
the *verified* scheme can be dropped in once confirmed against vendor docs (see
the ``TODO(human): confirm ... webhook signature`` markers in each module). The
constant NAMES vary by archetype, so the helper below resolves them defensively
and signs the body exactly the way ``verify`` expects — proving accept-on-valid
and reject-on-tampered/wrong-secret regardless of the (still-unconfirmed) scheme.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import importlib

import pytest

from services.app.webhooks.verifier import Secret, WebhookVerificationError

_SOURCES = ["brex", "ramp", "gusto", "deel"]


def _const(mod, names, default=None):
    for n in names:
        if hasattr(mod, n):
            return getattr(mod, n)
    return default


def _scheme(source):
    mod = importlib.import_module(f"services.app.webhooks.signatures.{source}")
    header = _const(mod, ["_HEADER_NAME", "_SIGNATURE_HEADER", "_HEADER"])
    prefix = _const(mod, ["_PREFIX", "_SIGNATURE_PREFIX"], "")
    encoding = _const(mod, ["_DIGEST_ENCODING"], "hex")
    assert header, f"{source} verifier exposes no recognizable header constant"
    return mod, header, prefix, encoding


def _sign(secret: str, body: bytes, prefix: str, encoding: str) -> str:
    mac = hmac.new(secret.encode("utf-8"), body, hashlib.sha256)
    digest = (
        base64.b64encode(mac.digest()).decode("ascii")
        if str(encoding).lower().startswith("b")
        else mac.hexdigest()
    )
    return prefix + digest


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.asyncio
async def test_happy_path(source):
    mod, header, prefix, encoding = _scheme(source)
    body = b'{"id":"evt_1","type":"test","data":{"x":1}}'
    secret = f"{source}-webhook-secret"
    ctx = await mod.verifier.verify(
        body=body,
        headers={header: _sign(secret, body, prefix, encoding)},
        secrets=[Secret(source, secret, label="primary")],
    )
    assert ctx.provider == source
    assert ctx.secret_label == "primary"


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.asyncio
async def test_tampered_body_rejected(source):
    mod, header, prefix, encoding = _scheme(source)
    body = b'{"id":"evt_1","type":"test"}'
    sig = _sign(f"{source}-webhook-secret", body, prefix, encoding)
    with pytest.raises(WebhookVerificationError) as exc:
        await mod.verifier.verify(
            body=b'{"id":"evt_1","type":"TAMPERED"}',
            headers={header: sig},
            secrets=[Secret(source, f"{source}-webhook-secret")],
        )
    assert exc.value.reason == "signature_mismatch"


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.asyncio
async def test_wrong_secret_rejected(source):
    mod, header, prefix, encoding = _scheme(source)
    body = b"{}"
    with pytest.raises(WebhookVerificationError):
        await mod.verifier.verify(
            body=body,
            headers={header: _sign("wrong-secret", body, prefix, encoding)},
            secrets=[Secret(source, f"{source}-webhook-secret")],
        )


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.asyncio
async def test_missing_header_rejected(source):
    mod, _header, _prefix, _encoding = _scheme(source)
    with pytest.raises(WebhookVerificationError):
        await mod.verifier.verify(
            body=b"{}",
            headers={},
            secrets=[Secret(source, f"{source}-webhook-secret")],
        )


@pytest.mark.parametrize("source", _SOURCES)
@pytest.mark.asyncio
async def test_secret_rotation(source):
    """Verification must succeed when the signing secret is any of several
    active secrets (rotation) — the body is signed with the SECOND secret."""
    mod, header, prefix, encoding = _scheme(source)
    body = b'{"id":"evt_rot","type":"test"}'
    old, new = f"{source}-old", f"{source}-new"
    ctx = await mod.verifier.verify(
        body=body,
        headers={header: _sign(new, body, prefix, encoding)},
        secrets=[Secret(source, old, label="old"), Secret(source, new, label="new")],
    )
    assert ctx.provider == source
