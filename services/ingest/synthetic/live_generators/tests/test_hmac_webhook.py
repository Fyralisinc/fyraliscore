"""Byte-exactness tests for HmacWebhookGenerator.

The generator must reproduce each provider's webhook signature scheme exactly,
so the synthetic live driver passes the SAME production verifier a real provider
delivery would. We assert this against the production `signatures/<provider>.py`
verifier directly (not a re-implementation) — if a verifier's scheme changes,
this test fails and the generator must follow.

Also asserts the per-provider payload carries the tenant-resolution key the
production `tenant_resolver` extracts (so a seeded provider_installations row
resolves), and that a tampered signature is rejected.
"""
from __future__ import annotations

import json

import pytest

from services.app.webhooks.signatures import (
    brex,
    deel,
    grafana,
    gusto,
    jira,
    mercury,
    quickbooks,
    ramp,
)
from services.app.webhooks.tenant_resolver import PROVIDER_EXTRACTORS
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from services.ingest.synthetic.live_generators.hmac_webhook import (
    HMAC_PROVIDERS,
    HmacWebhookGenerator,
)


class _T:
    """Minimal LiveTarget stand-in carrying the per-provider addressing."""
    def __init__(self, source: str) -> None:
        from uuid import uuid4
        self.source = source
        self.slug = "demo0"
        self.tenant_id = uuid4()
        self.jira_site = "demo0.atlassian.net"
        self.mercury_org = "org-demo0"
        self.mercury_account = "acct-demo0"
        self.qbo_realm = "realm-demo0"
        self.qbo_entity = "Invoice"
        self.grafana_instance = "demo0.grafana.net"
        # IN-FIN2 finance sources.
        self.brex_org = "brex-org-demo0"
        self.brex_account = "brex-acct-demo0"
        self.ramp_business = "ramp-biz-demo0"
        self.gusto_company = "gusto-co-demo0"
        self.deel_org = "deel-org-demo0"


_VERIFIERS = {
    "jira": jira.verifier, "mercury": mercury.verifier,
    "quickbooks": quickbooks.verifier, "grafana": grafana.verifier,
    "brex": brex.verifier, "ramp": ramp.verifier,
    "gusto": gusto.verifier, "deel": deel.verifier,
}
_SECRET = "unit-secret"


def _gen(provider: str) -> HmacWebhookGenerator:
    # The generator's signing + payload building need no app/httpx client.
    return HmacWebhookGenerator(app=None, provider=provider, signing_secret=_SECRET)


@pytest.mark.parametrize("provider", HMAC_PROVIDERS)
@pytest.mark.asyncio
async def test_signature_verifies_against_production_verifier(provider: str) -> None:
    gen = _gen(provider)
    payload, _ = gen._build_payload(_T(provider), content="unit")
    body = json.dumps(payload).encode("utf-8")
    signature = gen._sign(body)
    headers = {gen._header_name: signature}
    # The real verifier must accept it (no raise) with the matching secret.
    ctx = await _VERIFIERS[provider].verify(
        body=body, headers=headers, secrets=[Secret(provider=provider, value=_SECRET, label="t")],
    )
    assert ctx.provider == provider


@pytest.mark.parametrize("provider", HMAC_PROVIDERS)
@pytest.mark.asyncio
async def test_tampered_signature_rejected(provider: str) -> None:
    gen = _gen(provider)
    payload, _ = gen._build_payload(_T(provider), content="unit")
    body = json.dumps(payload).encode("utf-8")
    bad = (
        "sha256=" + ("f" * 64)
        if provider in ("jira", "mercury", "brex", "deel")
        else "f" * 64
    )
    with pytest.raises(WebhookVerificationError):
        await _VERIFIERS[provider].verify(
            body=body, headers={gen._header_name: bad},
            secrets=[Secret(provider=provider, value=_SECRET, label="t")],
        )


@pytest.mark.parametrize("provider", HMAC_PROVIDERS)
def test_payload_carries_tenant_resolution_key(provider: str) -> None:
    gen = _gen(provider)
    payload, _ = gen._build_payload(_T(provider), content="unit")
    extractor = PROVIDER_EXTRACTORS[provider]
    resolved = extractor(payload, {})
    assert resolved, f"{provider} payload missing tenant-resolution key"


def test_unknown_provider_rejected() -> None:
    with pytest.raises(ValueError):
        HmacWebhookGenerator(app=None, provider="linkedin", signing_secret="x")
