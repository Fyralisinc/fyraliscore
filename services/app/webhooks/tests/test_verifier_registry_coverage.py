from __future__ import annotations

import base64
from collections.abc import Callable
from dataclasses import dataclass
import hashlib
import hmac
import importlib
import json

import pytest

from services.app.webhooks.signatures import VERIFIERS
from services.app.webhooks.verifier import Secret, WebhookVerificationError


@dataclass(frozen=True, slots=True)
class HmacCase:
    provider: str
    module_name: str
    header: str
    prefix: str
    digest_encoding: str
    hash_factory: Callable[[], "hashlib._Hash"]


_HMAC_CASES = (
    HmacCase(
        "mercury",
        "mercury",
        "Mercury-Signature",
        "sha256=",
        "hex",
        hashlib.sha256,
    ),
    HmacCase(
        "quickbooks",
        "quickbooks",
        "intuit-signature",
        "",
        "base64",
        hashlib.sha256,
    ),
    HmacCase(
        "fireflies",
        "fireflies",
        "x-hub-signature",
        "sha256=",
        "hex",
        hashlib.sha256,
    ),
    HmacCase(
        "miro",
        "miro",
        "X-Miro-Signature",
        "sha256=",
        "hex",
        hashlib.sha256,
    ),
    HmacCase(
        "hibob",
        "hibob",
        "Bob-Signature",
        "",
        "base64",
        hashlib.sha512,
    ),
    HmacCase(
        "ashby",
        "ashby",
        "Ashby-Signature",
        "sha256=",
        "hex",
        hashlib.sha256,
    ),
)

_NEGATIVE_COVERED_PROVIDERS = {
    "ashby",
    "brex",
    "deel",
    "discord",
    "figma",
    "fireflies",
    "github",
    "grafana",
    "gusto",
    "hibob",
    "jira",
    "linear",
    "mercury",
    "miro",
    "notion",
    "quickbooks",
    "ramp",
    "slack",
    "stripe",
}


def _sign(
    secret: str,
    body: bytes,
    *,
    prefix: str,
    digest_encoding: str,
    hash_factory: Callable[[], "hashlib._Hash"],
) -> str:
    mac = hmac.new(secret.encode("utf-8"), body, hash_factory)
    digest = (
        base64.b64encode(mac.digest()).decode("ascii")
        if digest_encoding == "base64"
        else mac.hexdigest()
    )
    return prefix + digest


def test_registered_provider_verifiers_have_negative_test_coverage() -> None:
    assert set(VERIFIERS) == _NEGATIVE_COVERED_PROVIDERS


@pytest.mark.parametrize("case", _HMAC_CASES, ids=lambda case: case.provider)
@pytest.mark.asyncio
async def test_hmac_provider_rejects_tampered_body(case: HmacCase) -> None:
    module = importlib.import_module(
        f"services.app.webhooks.signatures.{case.module_name}"
    )
    secret = f"{case.provider}-webhook-secret"
    original = b'{"id":"evt_1","type":"created"}'
    tampered = b'{"id":"evt_1","type":"tampered"}'
    signature = _sign(
        secret,
        original,
        prefix=case.prefix,
        digest_encoding=case.digest_encoding,
        hash_factory=case.hash_factory,
    )

    with pytest.raises(WebhookVerificationError) as exc_info:
        await module.verifier.verify(
            body=tampered,
            headers={case.header: signature},
            secrets=[Secret(case.provider, secret)],
        )

    assert exc_info.value.provider == case.provider
    assert exc_info.value.reason == "signature_mismatch"


@pytest.mark.asyncio
async def test_figma_rejects_wrong_passcode() -> None:
    from services.app.webhooks.signatures import figma

    body = json.dumps({"event_type": "FILE_UPDATE", "passcode": "wrong"}).encode()

    with pytest.raises(WebhookVerificationError) as exc_info:
        await figma.verifier.verify(
            body=body,
            headers={},
            secrets=[Secret("figma", "expected-passcode")],
        )

    assert exc_info.value.provider == "figma"
    assert exc_info.value.reason == "passcode_mismatch"


@pytest.mark.asyncio
async def test_figma_rejects_missing_passcode() -> None:
    from services.app.webhooks.signatures import figma

    with pytest.raises(WebhookVerificationError) as exc_info:
        await figma.verifier.verify(
            body=b'{"event_type":"FILE_UPDATE"}',
            headers={},
            secrets=[Secret("figma", "expected-passcode")],
        )

    assert exc_info.value.provider == "figma"
    assert exc_info.value.reason == "missing_passcode"
