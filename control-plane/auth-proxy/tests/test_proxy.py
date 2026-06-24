"""End-to-end tests for the mTLS auth proxy over real sockets + TLS.

Each test starts the real :class:`AuthProxy` (asyncio mTLS server) in front of a
real echo upstream, then hits it with an ``httpx`` client presenting a chosen
client cert. We assert the EXACT P2 exit-gate behaviors:

* valid acme cert  → upstream receives ``X-Scope-OrgID: acme``;
* client-set ``X-Scope-OrgID: globex`` while presenting acme's cert → overridden
  to ``acme`` (the cert wins, the header is ignored — I4);
* revoked cert     → 403, request NEVER forwarded;
* unknown cert     → 403 (fail-closed);
* no client cert   → connection refused at the TLS layer (handshake fails);
* the proxy faithfully reverse-proxies method/path/body and the upstream status.
"""

from __future__ import annotations

import json
import ssl
import sys
from pathlib import Path

import httpx
import pytest

_AUTH_DIR = Path(__file__).resolve().parent.parent
if str(_AUTH_DIR) not in sys.path:
    sys.path.insert(0, str(_AUTH_DIR))

from tests.conftest import issue_client  # noqa: E402


def _row(tenant_id: str, status: str = "active") -> dict:
    return {
        "tenant_id": tenant_id,
        "issued_at": "2026-06-24T00:00:00Z",
        "status": status,
    }


def _client_verify(ca_fabric) -> ssl.SSLContext:
    """A client-side SSL context that trusts the test CA root (verifies proxy)."""
    ctx = ssl.create_default_context(cafile=str(ca_fabric["root_path"]))
    # The proxy's intermediate must be presented or trusted; our server cert is
    # signed by the intermediate, so also load the full chain as trust.
    ctx.load_verify_locations(cafile=str(ca_fabric["chain_path"]))
    return ctx


@pytest.mark.asyncio
async def test_valid_cert_injects_scope(ca_fabric, proxy_factory, tmp_path):
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({fp: _row("acme")})

    ctx = _client_verify(ca_fabric)
    async with httpx.AsyncClient(
        verify=ctx, cert=(str(crt), str(key))
    ) as client:
        resp = await client.get(f"{base_url}/prometheus/api/v1/query?q=up")
    assert resp.status_code == 200
    echoed = resp.json()
    assert echoed["headers"].get("x-scope-orgid") == "acme"
    assert echoed["path"] == "/prometheus/api/v1/query?q=up"
    assert echoed["method"] == "GET"


@pytest.mark.asyncio
async def test_client_supplied_scope_is_overridden(ca_fabric, proxy_factory, tmp_path):
    """A spoofed X-Scope-OrgID header is stripped + replaced by the cert tenant."""
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({fp: _row("acme")})

    ctx = _client_verify(ca_fabric)
    async with httpx.AsyncClient(verify=ctx, cert=(str(crt), str(key))) as client:
        resp = await client.get(
            f"{base_url}/prometheus/api/v1/query?q=up",
            headers={"X-Scope-OrgID": "globex"},  # the spoof attempt
        )
    assert resp.status_code == 200
    echoed = resp.json()
    # The cert says acme; the spoofed globex header must NOT survive.
    assert echoed["headers"].get("x-scope-orgid") == "acme"
    assert "globex" not in json.dumps(echoed["headers"])


@pytest.mark.asyncio
async def test_case_variant_scope_header_is_stripped(ca_fabric, proxy_factory, tmp_path):
    """Casing tricks (x-SCOPE-orgid / x-scope-org-foo) cannot smuggle scope."""
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({fp: _row("acme")})

    ctx = _client_verify(ca_fabric)
    async with httpx.AsyncClient(verify=ctx, cert=(str(crt), str(key))) as client:
        resp = await client.get(
            f"{base_url}/x",
            headers={
                "x-ScOpE-OrgID": "globex",
                "X-Scope-Org-Extra": "evil",
            },
        )
    assert resp.status_code == 200
    headers = resp.json()["headers"]
    # Exactly one scope header, equal to acme; nothing leaked the prefix.
    assert headers.get("x-scope-orgid") == "acme"
    assert "evil" not in json.dumps(headers)
    assert "globex" not in json.dumps(headers)


@pytest.mark.asyncio
async def test_revoked_cert_gets_403(ca_fabric, proxy_factory, tmp_path):
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({fp: _row("acme", status="revoked")})

    ctx = _client_verify(ca_fabric)
    async with httpx.AsyncClient(verify=ctx, cert=(str(crt), str(key))) as client:
        resp = await client.get(f"{base_url}/prometheus/api/v1/query?q=up")
    assert resp.status_code == 403
    # No upstream JSON echo leaked through — body is the flat 403.
    assert resp.text.strip() == "Forbidden"


@pytest.mark.asyncio
async def test_unknown_cert_gets_403(ca_fabric, proxy_factory, tmp_path):
    """A cert that chains to the CA but has NO registry row is denied (fail-closed)."""
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({})  # empty registry
    ctx = _client_verify(ca_fabric)
    async with httpx.AsyncClient(verify=ctx, cert=(str(crt), str(key))) as client:
        resp = await client.get(f"{base_url}/x")
    assert resp.status_code == 403


@pytest.mark.asyncio
async def test_no_client_cert_is_rejected(ca_fabric, proxy_factory, tmp_path):
    """Without a client cert the request is REJECTED and never forwarded.

    The TLS layer (CERT_REQUIRED) usually aborts the handshake → TransportError.
    Depending on the OpenSSL/TLS1.3 negotiation the connection may instead
    establish with an empty peer cert, in which case the resolver fail-closes to
    403. BOTH are correct security outcomes — an unauthenticated request is never
    proxied. The ONLY failure would be a 200 (i.e. it got forwarded).
    """
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({fp: _row("acme")})

    ctx = _client_verify(ca_fabric)
    try:
        async with httpx.AsyncClient(verify=ctx) as client:
            resp = await client.get(f"{base_url}/x")
        # If the handshake completed, the resolver must have fail-closed to 403.
        assert resp.status_code == 403, (
            "no-cert request must be 403, not forwarded (got %s)" % resp.status_code
        )
    except httpx.TransportError:
        # Handshake rejected outright — also correct (never reached HTTP).
        pass


@pytest.mark.asyncio
async def test_post_body_and_status_round_trip(ca_fabric, proxy_factory, tmp_path):
    """POST with a body is forwarded; upstream method/path are preserved."""
    crt, key, fp, _leaf = issue_client(ca_fabric, tmp_path, "acme")
    base_url, _reg = await proxy_factory({fp: _row("acme")})

    ctx = _client_verify(ca_fabric)
    async with httpx.AsyncClient(verify=ctx, cert=(str(crt), str(key))) as client:
        resp = await client.post(
            f"{base_url}/api/v1/push",
            content=b"metric_data_blob",
            headers={"Content-Type": "application/x-protobuf"},
        )
    assert resp.status_code == 200
    echoed = resp.json()
    assert echoed["method"] == "POST"
    assert echoed["path"] == "/api/v1/push"
    assert echoed["headers"].get("x-scope-orgid") == "acme"


@pytest.mark.asyncio
async def test_two_tenants_are_isolated(ca_fabric, proxy_factory, tmp_path):
    """acme's cert ⇒ acme scope; globex's cert ⇒ globex scope (never crossed)."""
    acme_crt, acme_key, acme_fp, _ = issue_client(ca_fabric, tmp_path, "acme")
    globex_crt, globex_key, globex_fp, _ = issue_client(ca_fabric, tmp_path, "globex")
    base_url, _reg = await proxy_factory(
        {acme_fp: _row("acme"), globex_fp: _row("globex")}
    )
    ctx = _client_verify(ca_fabric)

    async with httpx.AsyncClient(verify=ctx, cert=(str(acme_crt), str(acme_key))) as c:
        r1 = await c.get(f"{base_url}/x")
    async with httpx.AsyncClient(verify=ctx, cert=(str(globex_crt), str(globex_key))) as c:
        r2 = await c.get(f"{base_url}/x")

    assert r1.json()["headers"].get("x-scope-orgid") == "acme"
    assert r2.json()["headers"].get("x-scope-orgid") == "globex"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
