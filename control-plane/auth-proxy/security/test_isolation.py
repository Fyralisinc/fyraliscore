#!/usr/bin/env python3
"""test_isolation.py — ADVERSARIAL tenant-isolation suite for the auth proxy.

This is the GATING security test for Invariant I4 / risk R1: tenant identity at
the auth proxy comes **only** from the verified client-cert SPIFFE SAN, never
from caller-supplied input, and an unauthenticated / revoked / unknown / foreign
cert is **fail-closed** (403, nothing forwarded).

Unlike ``tests/test_proxy.py`` (which checks the happy path and the documented
exit-gate), this suite is written from an attacker's seat. It starts the REAL
:class:`AuthProxy` (asyncio mTLS server) in front of a REAL mock echo upstream
that reflects exactly which headers it received, then mounts concrete attacks:

  A1  acme cert                       -> upstream sees X-Scope-OrgID=acme
  A2  acme cert + forged header       -> upstream STILL sees acme (header stripped)
  A3  revoked cert                    -> 403, upstream NEVER touched
  A4  unknown / unregistered cert     -> 403 (fail-closed)
  A5  no client cert                  -> rejected (handshake fail OR 403)
  A6  cert from a DIFFERENT CA        -> 403 (foreign trust root)
  A7  duplicate / case-variant scope  -> ALL stripped, single cert-derived value
  A8  SAN-forged-but-foreign-CA cert  -> 403 (you can't mint your own identity)
  A9  registry says globex, SAN says acme -> 403 (SAN<->registry mismatch)
  A10 valid acme + smuggled scope via case/prefix tricks all at once -> acme

The load-bearing assertion in every forwarded case is the helper
``assert_upstream_scope_is`` / ``assert_no_foreign_scope``: the upstream must
NEVER observe a cross-tenant or client-controlled org id. If any attack causes a
cross-tenant scope to reach the upstream, that test FAILS LOUDLY — isolation is
broken and the gate must not pass.

Self-contained: an in-process CA + a second adversary CA, a mock echo upstream,
and the proxy on an ephemeral port. No Docker, no external services. Run with::

    python -m pytest auth-proxy/security/test_isolation.py -q
"""

from __future__ import annotations

import asyncio
import datetime as dt
import ipaddress
import json
import socket
import ssl
import sys
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

# --- wire up imports: auth-proxy/ (proxy, config) + ca/ (ca_lib) -------------
_SECURITY_DIR = Path(__file__).resolve().parent
_AUTH_DIR = _SECURITY_DIR.parent
_CA_DIR = _AUTH_DIR.parent / "ca"
for _p in (str(_AUTH_DIR), str(_CA_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ca_lib  # noqa: E402  (control-plane/ca/ca_lib.py — REAL crypto)
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID  # noqa: E402

from config import ProxyConfig  # noqa: E402
from proxy import AuthProxy  # noqa: E402


# ===========================================================================
# Cert helpers
# ===========================================================================

def _ip(addr: str):
    return ipaddress.ip_address(addr)


def _server_cert(intermediate: ca_lib.CertKeyPair) -> ca_lib.CertKeyPair:
    """A serverAuth leaf (localhost/127.0.0.1) the proxy presents to TLS clients."""
    key = ec.generate_private_key(ec.SECP256R1())
    now = dt.datetime.now(dt.timezone.utc)
    cert = (
        x509.CertificateBuilder()
        .subject_name(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "auth-proxy")]))
        .issuer_name(intermediate.cert.subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - dt.timedelta(minutes=5))
        .not_valid_after(now + dt.timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
        .add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH]), critical=False
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName("localhost"), x509.IPAddress(_ip("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(private_key=intermediate.key, algorithm=hashes.SHA256())
    )
    return ca_lib.CertKeyPair(cert=cert, key=key)


def _write(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


# ===========================================================================
# Mock echo upstream (stands in for Mimir): reflects request + records EVERY
# call so a test can prove the upstream was *never* reached on a reject path.
# ===========================================================================

class EchoUpstream:
    """Asyncio HTTP/1.1 echo server. Records every request it serves so a test
    can assert it was NEVER reached on a 403 path (cross-tenant leak guard)."""

    def __init__(self) -> None:
        self.server: asyncio.AbstractServer | None = None
        self.port: int = 0
        self.calls: list[dict] = []  # one entry per request that reached upstream

    async def start(self) -> str:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        return f"http://127.0.0.1:{self.port}"

    async def stop(self) -> None:
        if self.server is not None:
            self.server.close()
            await self.server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            method, target, _ = request_line.decode("latin-1").split(" ", 2)
            # Capture ALL header lines (incl. any duplicate scope headers) so the
            # test sees exactly what the proxy forwarded — duplicates included.
            raw_headers: list[tuple[str, str]] = []
            headers: dict[str, str] = {}
            content_length = 0
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin-1").partition(":")
                name = name.strip().lower()
                value = value.strip()
                raw_headers.append((name, value))
                # last-wins for the dict view; raw_headers preserves dups
                headers[name] = value
                if name == "content-length":
                    content_length = int(value)
            if content_length:
                await reader.readexactly(content_length)
            self.calls.append(
                {"method": method, "path": target, "raw_headers": raw_headers}
            )
            payload = json.dumps(
                {
                    "method": method,
                    "path": target,
                    "headers": headers,
                    "raw_headers": raw_headers,
                }
            ).encode("utf-8")
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(payload)).encode() + b"\r\n"
                b"Connection: close\r\n\r\n" + payload
            )
            await writer.drain()
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except Exception:
                pass

    # --- assertions the tests lean on -------------------------------------

    def scope_values(self) -> list[str]:
        """Every X-Scope-OrgID value the upstream ever saw, across all calls."""
        return [
            v for call in self.calls for (n, v) in call["raw_headers"]
            if n == "x-scope-orgid"
        ]


# ===========================================================================
# Fixtures
# ===========================================================================

@pytest.fixture(scope="session")
def fabric(tmp_path_factory):
    """The legit Fyralis CA + a SECOND adversary CA + the proxy's server cert.

    ``adversary`` is a fully independent root+intermediate the proxy does NOT
    trust — used to mint a foreign cert that even carries a valid-looking SPIFFE
    SAN, proving identity cannot be self-minted off a different trust root.
    """
    base = tmp_path_factory.mktemp("authproxy-sec-ca")

    # Legit Fyralis CA (what the proxy trusts).
    root, intermediate = ca_lib.bootstrap_hierarchy()
    chain_pem = ca_lib.chain_pem(intermediate, root)
    chain_path = _write(base / "ca-chain.crt", chain_pem)
    root_path = _write(base / "root.crt", ca_lib.cert_to_pem(root.cert))

    server = _server_cert(intermediate)
    server_crt = _write(base / "server.crt", ca_lib.cert_to_pem(server.cert))
    server_key = _write(base / "server.key", server.key_pem())

    # Adversary CA — totally separate trust root the proxy must reject.
    adv_root, adv_intermediate = ca_lib.bootstrap_hierarchy()
    adv_chain_path = _write(base / "adv-ca-chain.crt",
                            ca_lib.chain_pem(adv_intermediate, adv_root))

    return {
        "base": base,
        "root": root,
        "intermediate": intermediate,
        "chain_path": chain_path,
        "root_path": root_path,
        "server_crt": server_crt,
        "server_key": server_key,
        "adv_root": adv_root,
        "adv_intermediate": adv_intermediate,
        "adv_chain_path": adv_chain_path,
    }


def issue_legit(fabric, tmp_path: Path, tenant_id: str):
    """Mint a tenant leaf off the LEGIT Fyralis intermediate; return paths + fp."""
    leaf = ca_lib.issue_tenant_cert(tenant_id, fabric["intermediate"])
    fp = leaf.fingerprint_sha256()
    crt = _write(tmp_path / f"{tenant_id}.crt", ca_lib.cert_to_pem(leaf.cert))
    key = _write(tmp_path / f"{tenant_id}.key", leaf.key_pem())
    return crt, key, fp, leaf


def issue_foreign(fabric, tmp_path: Path, tenant_id: str):
    """Mint a leaf with a VALID-looking SPIFFE SAN but signed by the ADVERSARY CA.

    This is the SAN-forgery attack: the attacker controls their own CA and stamps
    ``spiffe://fyralis/tenant/acme`` into the SAN. It must still be rejected
    because it does not chain to the Fyralis root.
    """
    leaf = ca_lib.issue_tenant_cert(tenant_id, fabric["adv_intermediate"])
    fp = leaf.fingerprint_sha256()
    crt = _write(tmp_path / f"foreign-{tenant_id}.crt", ca_lib.cert_to_pem(leaf.cert))
    key = _write(tmp_path / f"foreign-{tenant_id}.key", leaf.key_pem())
    return crt, key, fp, leaf


@pytest_asyncio.fixture
async def echo():
    up = EchoUpstream()
    await up.start()
    try:
        yield up
    finally:
        await up.stop()


@pytest_asyncio.fixture
async def proxy_factory(fabric, echo, tmp_path):
    """Start a proxy for a given {fingerprint: row} registry; tears down after."""
    started: list[AuthProxy] = []

    async def _make(registry_rows: dict):
        reg_path = tmp_path / "tenant_registry.json"
        reg_path.write_text(json.dumps(registry_rows, indent=2), encoding="utf-8")
        port = _free_port()
        cfg = ProxyConfig(
            listen_host="127.0.0.1",
            listen_port=port,
            ca_chain_path=fabric["chain_path"],
            tenant_registry_path=reg_path,
            tls_cert_path=fabric["server_crt"],
            tls_key_path=fabric["server_key"],
            upstream_url=f"http://127.0.0.1:{echo.port}",
        )
        proxy = AuthProxy(cfg)
        await proxy.start()
        started.append(proxy)
        return f"https://127.0.0.1:{port}"

    yield _make
    for proxy in started:
        await proxy.aclose()


def _row(tenant_id: str, status: str = "active") -> dict:
    return {"tenant_id": tenant_id, "issued_at": "2026-06-24T00:00:00Z", "status": status}


def _client_ctx(fabric) -> ssl.SSLContext:
    """Client SSL context that trusts the Fyralis root (so it verifies the proxy)."""
    ctx = ssl.create_default_context(cafile=str(fabric["root_path"]))
    ctx.load_verify_locations(cafile=str(fabric["chain_path"]))
    return ctx


def _mtls_ctx(fabric, crt: Path, key: Path) -> ssl.SSLContext:
    """Client context that ALSO presents a client cert (mTLS)."""
    ctx = _client_ctx(fabric)
    ctx.load_cert_chain(certfile=str(crt), keyfile=str(key))
    return ctx


# ===========================================================================
# Load-bearing assertions — the upstream must NEVER see a cross-tenant or
# client-controlled org id.
# ===========================================================================

def assert_upstream_scope_is(echo: EchoUpstream, expected: str) -> None:
    seen = echo.scope_values()
    assert seen, "upstream received NO X-Scope-OrgID — proxy must inject one"
    assert all(v == expected for v in seen), (
        f"ISOLATION BREACH: upstream saw scope(s) {seen!r}, expected only {expected!r}"
    )


def assert_upstream_never_reached(echo: EchoUpstream) -> None:
    assert echo.calls == [], (
        f"ISOLATION BREACH: upstream WAS reached on a reject path: {echo.calls!r}"
    )


def assert_no_foreign_scope(echo: EchoUpstream, forbidden: str) -> None:
    seen = echo.scope_values()
    assert forbidden not in seen, (
        f"ISOLATION BREACH: forbidden scope {forbidden!r} reached upstream: {seen!r}"
    )


# ===========================================================================
# A1 — acme cert => upstream sees X-Scope-OrgID=acme
# ===========================================================================

@pytest.mark.asyncio
async def test_A1_acme_cert_yields_acme_scope(fabric, proxy_factory, echo, tmp_path):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})
    ctx = _mtls_ctx(fabric, crt, key)
    async with httpx.AsyncClient(verify=ctx) as c:
        r = await c.get(f"{base}/prometheus/api/v1/query?q=up")
    assert r.status_code == 200
    assert r.json()["headers"].get("x-scope-orgid") == "acme"
    assert_upstream_scope_is(echo, "acme")


# ===========================================================================
# A2 — acme cert WITH a forged X-Scope-OrgID=globex header => upstream sees acme
# ===========================================================================

@pytest.mark.asyncio
async def test_A2_forged_scope_header_is_stripped(fabric, proxy_factory, echo, tmp_path):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})
    ctx = _mtls_ctx(fabric, crt, key)
    async with httpx.AsyncClient(verify=ctx) as c:
        r = await c.get(
            f"{base}/x",
            headers={"X-Scope-OrgID": "globex"},  # the spoof
        )
    assert r.status_code == 200
    # Cert wins; the client-supplied globex must not survive anywhere.
    assert r.json()["headers"].get("x-scope-orgid") == "acme"
    assert "globex" not in json.dumps(r.json()["headers"])
    assert_upstream_scope_is(echo, "acme")
    assert_no_foreign_scope(echo, "globex")


# ===========================================================================
# A3 — revoked cert => 403, NOTHING forwarded
# ===========================================================================

@pytest.mark.asyncio
async def test_A3_revoked_cert_403_nothing_forwarded(fabric, proxy_factory, echo, tmp_path):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme", status="revoked")})
    ctx = _mtls_ctx(fabric, crt, key)
    async with httpx.AsyncClient(verify=ctx) as c:
        r = await c.get(f"{base}/prometheus/api/v1/query?q=up")
    assert r.status_code == 403
    assert r.text.strip() == "Forbidden"
    assert_upstream_never_reached(echo)


# ===========================================================================
# A4 — unknown / unregistered cert => 403 (fail-closed)
# ===========================================================================

@pytest.mark.asyncio
async def test_A4_unknown_cert_403_failclosed(fabric, proxy_factory, echo, tmp_path):
    # Cert chains to the Fyralis CA but has NO registry row at all.
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({})  # empty registry
    ctx = _mtls_ctx(fabric, crt, key)
    async with httpx.AsyncClient(verify=ctx) as c:
        r = await c.get(f"{base}/x")
    assert r.status_code == 403
    assert_upstream_never_reached(echo)


# ===========================================================================
# A5 — no client cert => rejected (handshake fail OR fail-closed 403). NEVER 200.
# ===========================================================================

@pytest.mark.asyncio
async def test_A5_no_client_cert_rejected(fabric, proxy_factory, echo, tmp_path):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})
    ctx = _client_ctx(fabric)  # trusts proxy, presents NO client cert
    forwarded = False
    try:
        async with httpx.AsyncClient(verify=ctx) as c:
            r = await c.get(f"{base}/x")
        # If the handshake somehow completed, it MUST be a fail-closed 403.
        assert r.status_code == 403, (
            f"no-cert request must be 403, never forwarded (got {r.status_code})"
        )
        forwarded = r.status_code == 200
    except (httpx.TransportError, ssl.SSLError):
        # Handshake aborted by CERT_REQUIRED — also correct (never reached HTTP).
        pass
    assert not forwarded
    assert_upstream_never_reached(echo)


# ===========================================================================
# A6 — cert from a DIFFERENT CA => 403 (handshake reject OR fail-closed 403)
# ===========================================================================

@pytest.mark.asyncio
async def test_A6_foreign_ca_cert_rejected(fabric, proxy_factory, echo, tmp_path):
    # A leaf minted off the adversary CA. The proxy's TLS layer trusts only the
    # Fyralis CA, so the handshake should fail; if it ever completes, the resolver
    # re-verifies the chain against the Fyralis CA and fail-closes to 403.
    crt, key, fp, _ = issue_foreign(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})  # even WITH a matching row
    ctx = _mtls_ctx(fabric, crt, key)
    forwarded_status = None
    try:
        async with httpx.AsyncClient(verify=ctx) as c:
            r = await c.get(f"{base}/x")
        forwarded_status = r.status_code
        assert r.status_code == 403, (
            f"foreign-CA cert must be 403, never forwarded (got {r.status_code})"
        )
    except (httpx.TransportError, ssl.SSLError):
        pass  # handshake rejected outright — correct
    assert forwarded_status in (None, 403)
    assert_upstream_never_reached(echo)


# ===========================================================================
# A7 — duplicate / case-variant X-Scope-OrgID headers ALL stripped
# ===========================================================================

@pytest.mark.asyncio
async def test_A7_duplicate_and_casevariant_scope_all_stripped(
    fabric, proxy_factory, echo, tmp_path
):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})
    ctx = _mtls_ctx(fabric, crt, key)

    # httpx/h11 won't let us send two identical header names easily, so we use a
    # raw TLS socket + handcrafted request with DUPLICATE + case-variant scope
    # headers and a prefix-variant. All must be stripped; upstream sees one acme.
    loop = asyncio.get_event_loop()
    host = "127.0.0.1"
    port = int(base.rsplit(":", 1)[1])
    raw = (
        f"GET /dup HTTP/1.1\r\n"
        f"Host: {host}\r\n"
        f"X-Scope-OrgID: globex\r\n"
        f"x-scope-orgid: evilcorp\r\n"
        f"X-SCOPE-ORGID: initech\r\n"
        f"X-Scope-Org-Smuggle: sneaky\r\n"
        f"Connection: close\r\n"
        f"Content-Length: 0\r\n"
        f"\r\n"
    ).encode("latin-1")

    reader, writer = await asyncio.open_connection(
        host, port, ssl=ctx, server_hostname="localhost"
    )
    writer.write(raw)
    await writer.drain()
    data = b""
    while True:
        chunk = await reader.read(65536)
        if not chunk:
            break
        data += chunk
    writer.close()
    try:
        await writer.wait_closed()
    except Exception:
        pass

    head, _, body = data.partition(b"\r\n\r\n")
    assert b"200" in head.split(b"\r\n")[0], head
    echoed = json.loads(body.decode("utf-8"))
    # Exactly one scope header reached upstream, and it is the cert tenant.
    scope_seen = [v for (n, v) in echoed["raw_headers"] if n == "x-scope-orgid"]
    assert scope_seen == ["acme"], f"expected exactly ['acme'], got {scope_seen!r}"
    blob = json.dumps(echoed)
    for forbidden in ("globex", "evilcorp", "initech", "sneaky"):
        assert forbidden not in blob, f"smuggled value {forbidden!r} reached upstream"
    assert_upstream_scope_is(echo, "acme")


# ===========================================================================
# A8 — SAN-forged cert off a foreign CA (already covered by A6 path, but assert
#       explicitly that a self-minted 'fyralis' identity is worthless)
# ===========================================================================

@pytest.mark.asyncio
async def test_A8_self_minted_spiffe_identity_is_worthless(
    fabric, proxy_factory, echo, tmp_path
):
    # Attacker mints spiffe://fyralis/tenant/acme off their OWN CA and even adds a
    # registry row keyed on that cert's fingerprint. Must STILL be rejected: chain
    # verification against the Fyralis root fails first.
    crt, key, fp, _ = issue_foreign(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})
    ctx = _mtls_ctx(fabric, crt, key)
    status = None
    try:
        async with httpx.AsyncClient(verify=ctx) as c:
            r = await c.get(f"{base}/x")
        status = r.status_code
        assert status == 403
    except (httpx.TransportError, ssl.SSLError):
        pass
    assert status in (None, 403)
    assert_upstream_never_reached(echo)


# ===========================================================================
# A9 — registry row disagrees with SAN (registry=globex, SAN=acme) => 403
# ===========================================================================

@pytest.mark.asyncio
async def test_A9_san_registry_mismatch_403(fabric, proxy_factory, echo, tmp_path):
    # Legit acme cert, but the registry row for its fingerprint claims 'globex'.
    # C1 requires SAN == registry tenant; a mismatch is rejected so neither tenant
    # is impersonated.
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("globex")})  # deliberate mismatch
    ctx = _mtls_ctx(fabric, crt, key)
    async with httpx.AsyncClient(verify=ctx) as c:
        r = await c.get(f"{base}/x")
    assert r.status_code == 403
    assert_upstream_never_reached(echo)


# ===========================================================================
# A10 — full kitchen-sink smuggle on a VALID acme cert: still scoped acme,
#        and no foreign tenant string leaks anywhere.
# ===========================================================================

@pytest.mark.asyncio
async def test_A10_kitchen_sink_smuggle_still_acme(fabric, proxy_factory, echo, tmp_path):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    base = await proxy_factory({fp: _row("acme")})
    ctx = _mtls_ctx(fabric, crt, key)
    async with httpx.AsyncClient(verify=ctx) as c:
        r = await c.get(
            f"{base}/q",
            headers={
                "X-Scope-OrgID": "globex",
                "X-Scope-Org-Tenant": "initech",
                "X-Scope-Org": "evilcorp",
            },
        )
    assert r.status_code == 200
    blob = json.dumps(r.json()["headers"])
    assert r.json()["headers"].get("x-scope-orgid") == "acme"
    for forbidden in ("globex", "initech", "evilcorp"):
        assert forbidden not in blob
    assert_upstream_scope_is(echo, "acme")


# ===========================================================================
# A11 — cross-tenant isolation under interleaving: acme stays acme, globex stays
#        globex, never crossed.
# ===========================================================================

@pytest.mark.asyncio
async def test_A11_two_tenants_never_cross(fabric, proxy_factory, echo, tmp_path):
    acme_crt, acme_key, acme_fp, _ = issue_legit(fabric, tmp_path, "acme")
    glob_crt, glob_key, glob_fp, _ = issue_legit(fabric, tmp_path, "globex")
    base = await proxy_factory({acme_fp: _row("acme"), glob_fp: _row("globex")})

    actx = _mtls_ctx(fabric, acme_crt, acme_key)
    gctx = _mtls_ctx(fabric, glob_crt, glob_key)
    # globex sends a spoofed acme header; acme sends a spoofed globex header.
    async with httpx.AsyncClient(verify=gctx) as c:
        rg = await c.get(f"{base}/x", headers={"X-Scope-OrgID": "acme"})
    async with httpx.AsyncClient(verify=actx) as c:
        ra = await c.get(f"{base}/x", headers={"X-Scope-OrgID": "globex"})

    assert ra.json()["headers"].get("x-scope-orgid") == "acme"
    assert rg.json()["headers"].get("x-scope-orgid") == "globex"
    # Every scope the upstream saw must be one of the two cert-derived ids, and
    # each request's injected scope matched its CERT, not its spoofed header.
    assert sorted(echo.scope_values()) == ["acme", "globex"]


# ===========================================================================
# A12 — SSRF guard: an absolute-form request target CANNOT re-point the upstream.
#
# This was a REAL finding (see SAST.md / THREAT_MODEL T12): the proxy used to
# forward ``request.target`` verbatim to httpx, so an absolute-form target
# (``GET http://attacker/...``) OVERRODE the pinned base_url host in httpx and a
# *valid acme cert* could make the proxy dial an arbitrary host — an SSRF from
# inside cp-net.
#
# The fix pins the destination to the CONFIGURED upstream and forwards ONLY the
# path+query: an absolute-form ``scheme://authority`` has its scheme+authority
# DISCARDED (path survives, host is ignored), and authority-form/CONNECT is
# rejected. This test now asserts the *fixed* behavior:
#
#   1. an absolute-form ``http://evil-host/metrics`` lands on the CONFIGURED
#      upstream (the mock echo) at path ``/metrics`` — NOT on evil-host, and the
#      "internal" off-upstream server is NEVER reached;
#   2. the upstream Host header is the CONFIGURED upstream's authority, never
#      the attacker-supplied host;
#   3. an authority-form / CONNECT target is rejected (4xx), upstream untouched.
# ===========================================================================

@pytest.mark.asyncio
async def test_A12_absolute_form_target_pins_configured_upstream(
    fabric, proxy_factory, echo, tmp_path
):
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")

    # A second "internal" echo server the attacker WANTS to reach via the
    # absolute-form host — it is NOT the proxy's configured upstream. It must
    # NEVER receive a request.
    internal = EchoUpstream()
    await internal.start()
    try:
        base = await proxy_factory({fp: _row("acme")})
        host = "127.0.0.1"
        port = int(base.rsplit(":", 1)[1])
        ctx = _mtls_ctx(fabric, crt, key)

        # Absolute-form request target naming an attacker host ("evil-host") plus
        # the concrete internal server's authority. Either way, scheme+authority
        # must be DISCARDED and only /metrics forwarded to the CONFIGURED upstream.
        raw = (
            f"GET http://evil-host:{internal.port}/metrics?q=up HTTP/1.1\r\n"
            f"Host: evil-host\r\n"
            f"Connection: close\r\n"
            f"Content-Length: 0\r\n"
            f"\r\n"
        ).encode("latin-1")

        reader, writer = await asyncio.open_connection(
            host, port, ssl=ctx, server_hostname="localhost"
        )
        writer.write(raw)
        await writer.drain()
        data = b""
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            data += chunk
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        head, _, body = data.partition(b"\r\n\r\n")
        status_line = head.split(b"\r\n")[0] if head else b""

        # (1) The off-upstream / attacker server was NEVER reached.
        assert internal.calls == [], (
            "SSRF: absolute-form target reached the off-upstream host: %r"
            % internal.calls
        )

        # (1) The request DID land on the CONFIGURED upstream (the `echo` fixture)
        #     with only the path+query — scheme+authority were discarded.
        assert b"200" in status_line, status_line
        echoed = json.loads(body.decode("utf-8"))
        assert echoed["path"] == "/metrics?q=up", echoed["path"]

        # (2) The upstream Host is the CONFIGURED upstream authority, never the
        #     attacker-controlled "evil-host".
        upstream_host = f"127.0.0.1:{echo.port}"
        seen_hosts = [v for (n, v) in echoed["raw_headers"] if n == "host"]
        assert seen_hosts == [upstream_host], seen_hosts
        assert "evil-host" not in json.dumps(echoed)

        # Scope is still correctly the cert tenant — pinning didn't break I4.
        assert_upstream_scope_is(echo, "acme")
        assert_no_foreign_scope(echo, "evil-host")
    finally:
        await internal.stop()


@pytest.mark.asyncio
async def test_A12b_authority_form_connect_is_rejected(
    fabric, proxy_factory, echo, tmp_path
):
    """An authority-form / CONNECT target (host:port, no leading '/') is rejected
    with a 4xx and never reaches any upstream — the proxy is not a forward/tunnel
    proxy."""
    crt, key, fp, _ = issue_legit(fabric, tmp_path, "acme")
    internal = EchoUpstream()
    await internal.start()
    try:
        base = await proxy_factory({fp: _row("acme")})
        host = "127.0.0.1"
        port = int(base.rsplit(":", 1)[1])
        ctx = _mtls_ctx(fabric, crt, key)

        # authority-form: CONNECT host:port with NO leading '/'.
        raw = (
            f"CONNECT evil-host:{internal.port} HTTP/1.1\r\n"
            f"Host: evil-host:{internal.port}\r\n"
            f"Connection: close\r\n"
            f"Content-Length: 0\r\n"
            f"\r\n"
        ).encode("latin-1")

        reader, writer = await asyncio.open_connection(
            host, port, ssl=ctx, server_hostname="localhost"
        )
        writer.write(raw)
        await writer.drain()
        data = b""
        while True:
            chunk = await reader.read(65536)
            if not chunk:
                break
            data += chunk
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass

        status_line = data.split(b"\r\n", 1)[0] if data else b""
        # Rejected with a 4xx (405 Method Not Allowed); neither the configured
        # upstream nor the attacker host is reached.
        assert b"405" in status_line or b"403" in status_line or b"400" in status_line, (
            "authority-form/CONNECT must be 4xx-rejected, got: %r" % status_line
        )
        assert internal.calls == [], internal.calls
        assert echo.calls == [], echo.calls
    finally:
        await internal.stop()


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
