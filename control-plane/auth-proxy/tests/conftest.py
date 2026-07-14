"""Shared fixtures for the auth-proxy test-suite.

Provides a real, self-contained test fabric:

* an in-memory CA hierarchy (root + intermediate) via ``ca/ca_lib``;
* a **server** leaf cert for the proxy itself (DNS/IP SAN so a TLS client can
  validate the proxy's identity against the CA root);
* per-tenant **client** certs (the data-plane agent identity, SPIFFE SAN);
* a tenant_registry.json on /tmp the proxy reads;
* a tiny asyncio **echo upstream** that reflects the request method/path/headers
  back as JSON so a test can assert which X-Scope-OrgID the proxy injected;
* the running :class:`AuthProxy` on an ephemeral port, plus an ``httpx`` client
  factory that presents a chosen client cert.

Everything is torn down per test. No external services, no Docker.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import json
import socket
import sys
from pathlib import Path

import pytest
import pytest_asyncio

_AUTH_DIR = Path(__file__).resolve().parent.parent
_CA_DIR = _AUTH_DIR.parent / "ca"
for _p in (str(_AUTH_DIR), str(_CA_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ca_lib  # noqa: E402
from cryptography import x509  # noqa: E402
from cryptography.hazmat.primitives import hashes, serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric import ec  # noqa: E402
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID  # noqa: E402

from config import ProxyConfig  # noqa: E402
from proxy import AuthProxy  # noqa: E402


# ---------------------------------------------------------------------------
# Cert helpers
# ---------------------------------------------------------------------------

def _server_cert(intermediate: ca_lib.CertKeyPair) -> ca_lib.CertKeyPair:
    """A serverAuth leaf with localhost/127.0.0.1 SANs, signed by the CA.

    The proxy presents this to TLS clients; the test client validates it against
    the CA root, so we get real server-side verification too (not disabled).
    """
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


def _ip(addr: str):
    import ipaddress

    return ipaddress.ip_address(addr)


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _write(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)
    return path


# ---------------------------------------------------------------------------
# Echo upstream (stands in for Mimir): reflects request as JSON.
# ---------------------------------------------------------------------------

class EchoUpstream:
    """Tiny asyncio HTTP/1.1 server that echoes method/path/headers as JSON."""

    def __init__(self) -> None:
        self.server: asyncio.AbstractServer | None = None
        self.port: int = 0

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
            headers = {}
            content_length = 0
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin-1").partition(":")
                name = name.strip().lower()
                value = value.strip()
                headers[name] = value
                if name == "content-length":
                    content_length = int(value)
            if content_length:
                await reader.readexactly(content_length)
            payload = json.dumps(
                {"method": method, "path": target, "headers": headers}
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def ca_fabric(tmp_path_factory):
    """Root+intermediate, the on-disk CA chain, and a server cert/key on /tmp."""
    base = tmp_path_factory.mktemp("authproxy-ca")
    root, intermediate = ca_lib.bootstrap_hierarchy()
    chain_pem = ca_lib.chain_pem(intermediate, root)
    chain_path = _write(base / "ca-chain.crt", chain_pem)
    root_path = _write(base / "root.crt", ca_lib.cert_to_pem(root.cert))

    server = _server_cert(intermediate)
    server_crt = _write(base / "server.crt", ca_lib.cert_to_pem(server.cert))
    server_key = _write(base / "server.key", server.key_pem())

    return {
        "base": base,
        "root": root,
        "intermediate": intermediate,
        "chain_path": chain_path,
        "root_path": root_path,
        "server_crt": server_crt,
        "server_key": server_key,
    }


def issue_client(ca_fabric, tmp_path: Path, tenant_id: str, *, status: str = "active"):
    """Issue a tenant client cert, write cert/key, and add a registry row.

    Returns (crt_path, key_path, fingerprint). Writes into the per-test
    ``tmp_path`` so registries don't leak between tests.
    """
    leaf = ca_lib.issue_tenant_cert(tenant_id, ca_fabric["intermediate"])
    fp = leaf.fingerprint_sha256()
    crt = _write(tmp_path / f"{tenant_id}.crt", ca_lib.cert_to_pem(leaf.cert))
    key = _write(tmp_path / f"{tenant_id}.key", leaf.key_pem())
    return crt, key, fp, leaf


@pytest_asyncio.fixture
async def echo_upstream():
    up = EchoUpstream()
    url = await up.start()
    try:
        yield url
    finally:
        await up.stop()


@pytest_asyncio.fixture
async def proxy_factory(ca_fabric, echo_upstream, tmp_path):
    """Factory: given a {fingerprint: row} registry, start a proxy; return (base_url, registry_path).

    Tears down every started proxy after the test.
    """
    started: list[AuthProxy] = []

    async def _make(registry_rows: dict):
        reg_path = tmp_path / "tenant_registry.json"
        reg_path.write_text(json.dumps(registry_rows, indent=2), encoding="utf-8")
        port = _free_port()
        cfg = ProxyConfig(
            listen_host="127.0.0.1",
            listen_port=port,
            ca_chain_path=ca_fabric["chain_path"],
            tenant_registry_path=reg_path,
            tls_cert_path=ca_fabric["server_crt"],
            tls_key_path=ca_fabric["server_key"],
            upstream_url=echo_upstream,
        )
        proxy = AuthProxy(cfg)
        await proxy.start()
        started.append(proxy)
        return f"https://127.0.0.1:{port}", reg_path

    yield _make

    for proxy in started:
        await proxy.aclose()
