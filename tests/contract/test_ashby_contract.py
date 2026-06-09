"""Contract test: the Ashby webhook path resolves the tenant from the PER-INSTALL
ENDPOINT URL, not a body field, and verifies the Ashby-Signature.

Guards the Phase-2 architectural fix (finding #28, R3): a real Ashby delivery
carries NO organization/tenant id in the body (`{action, data, ...}`) — the
tenant is named by the receiving endpoint URL (`/webhooks/ashby/{installId}`),
each install configured with a distinct URL + signing secret. The pre-R3 code
resolved by a body `organizationId` (absent in production → live resolution
always failed). The fix threads the URL subpath into `TenantResolver.resolve`
and resolves Ashby from the path segment first; the body read is a legacy
fallback. The verifier (`Ashby-Signature: sha256=<hex>`) was already correct.

Verified against developers.ashbyhq.com webhooks docs.
"""
from __future__ import annotations

import hashlib
import hmac
import json
from uuid import uuid4

import pytest

from services.app.webhooks.signatures.ashby import verifier as ashby_verifier
from services.app.webhooks.tenant_resolver import (
    InstallationCache,
    Resolved,
    TenantResolver,
    TenantResolverDeps,
    _extract_ashby,
    _first_path_segment,
    noop_metrics,
)
from services.app.webhooks.verifier import Secret, WebhookVerificationError
from tests.contract.framework import load_fixture

pytestmark = pytest.mark.contract

_SECRET = "test-ashby-install-secret"


def _fixture():
    return load_fixture("ashby", "webhook", "candidate_event")


def _raw(body: dict) -> bytes:
    return json.dumps(body, separators=(",", ":")).encode("utf-8")


def _sign_hex(raw: bytes) -> str:
    return "sha256=" + hmac.new(_SECRET.encode("utf-8"), raw, hashlib.sha256).hexdigest()


class _FakePool:
    """Captures the installation_id the resolver queries by."""

    def __init__(self, row: dict) -> None:
        self._row = row
        self.queried_installation_id: str | None = None

    async def fetchrow(self, _query: str, _provider: str, installation_id: str):
        self.queried_installation_id = installation_id
        return self._row


def test_ashby_body_carries_no_org_id():
    body = _fixture().body
    # Real Ashby: no organization id anywhere in the delivery body.
    assert _extract_ashby(body, {}) is None
    assert "organizationId" not in body


def test_ashby_install_id_is_the_endpoint_path_segment():
    install_id = _fixture().request["install_id"]
    # `/webhooks/ashby/{installId}` → subpath == installId.
    assert _first_path_segment(install_id) == install_id
    assert _first_path_segment(f"{install_id}/extra") == install_id
    assert _first_path_segment(None) is None


async def test_ashby_resolver_resolves_from_path_not_body():
    fix = _fixture()
    install_id = fix.request["install_id"]
    tenant_id = uuid4()
    pool = _FakePool({"id": uuid4(), "tenant_id": tenant_id, "secret_ref": None})
    resolver = TenantResolver(
        TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=lambda: 0.0,
            metrics=noop_metrics(),
        )
    )
    outcome = await resolver.resolve(
        "ashby", fix.body, {}, subpath=install_id,
    )
    assert isinstance(outcome, Resolved)
    assert outcome.tenant_id == tenant_id
    # Resolution keyed off the URL PATH segment, with nothing read from the body.
    assert pool.queried_installation_id == install_id


async def test_ashby_legacy_body_org_is_fallback_for_bare_endpoint():
    # A bare-endpoint post (no subpath) with the synthetic org-in-body shape
    # still resolves via the legacy body extractor — backward-compat preserved.
    tenant_id = uuid4()
    pool = _FakePool({"id": uuid4(), "tenant_id": tenant_id, "secret_ref": None})
    resolver = TenantResolver(
        TenantResolverDeps(
            pool=pool,
            cache=InstallationCache(),
            clock=lambda: 0.0,
            metrics=noop_metrics(),
        )
    )
    outcome = await resolver.resolve(
        "ashby", {"organizationId": "org-legacy"}, {}, subpath=None,
    )
    assert isinstance(outcome, Resolved)
    assert pool.queried_installation_id == "org-legacy"


async def test_ashby_signature_verifies():
    raw = _raw(_fixture().body)
    ctx = await ashby_verifier.verify(
        body=raw,
        headers={"Ashby-Signature": _sign_hex(raw)},
        secrets=[Secret("ashby", _SECRET)],
    )
    assert ctx.provider == "ashby"


async def test_ashby_tampered_signature_rejected():
    raw = _raw(_fixture().body)
    with pytest.raises(WebhookVerificationError) as exc:
        await ashby_verifier.verify(
            body=raw,
            headers={"Ashby-Signature": _sign_hex(raw)},
            secrets=[Secret("ashby", "wrong-secret")],
        )
    assert exc.value.reason == "signature_mismatch"
