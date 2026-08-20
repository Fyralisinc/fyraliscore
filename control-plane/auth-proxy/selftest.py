#!/usr/bin/env python3
"""selftest.py — end-to-end self-test of the auth proxy over a real socket.

Bootstraps a throwaway CA under a temp dir, issues a proxy server cert + two
tenant client certs (acme active, badco revoked), starts a real echo upstream
and the real :class:`AuthProxy` on an ephemeral port, then drives it with httpx:

  1. valid acme cert            → upstream sees X-Scope-OrgID: acme
  2. client sets X-Scope-OrgID  → overridden to acme (cert wins, I4)
  3. revoked cert               → 403, never forwarded
  4. unknown cert (no row)      → 403 (fail-closed)
  5. no client cert             → handshake rejected (TransportError)

Exit code 0 ⇒ every assertion held. Writes ONLY under a temp dir; touches no
repo state. Run: ``python auth-proxy/selftest.py``.
"""

from __future__ import annotations

import asyncio
import json
import socket
import ssl
import sys
import tempfile
from pathlib import Path

_HERE = Path(__file__).resolve().parent
_CA_DIR = _HERE.parent / "ca"
for _p in (str(_HERE), str(_CA_DIR)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

import ca_lib  # noqa: E402
import httpx  # noqa: E402

from config import ProxyConfig  # noqa: E402
from proxy import AuthProxy  # noqa: E402

# Reuse the test fixtures' cert + echo helpers so the self-test and the suite
# share one implementation of "make a server cert / echo upstream".
sys.path.insert(0, str(_HERE / "tests"))
from conftest import EchoUpstream, _server_cert  # noqa: E402


def _free_port() -> int:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _write(p: Path, data: bytes) -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(data)
    return p


async def _run() -> int:
    failures: list[str] = []

    def check(cond: bool, msg: str) -> None:
        if cond:
            print(f"  PASS {msg}")
        else:
            failures.append(msg)
            print(f"  FAIL {msg}")

    with tempfile.TemporaryDirectory(prefix="authproxy-selftest-") as d:
        base = Path(d)
        root, inter = ca_lib.bootstrap_hierarchy()
        chain_path = _write(base / "ca-chain.crt", ca_lib.chain_pem(inter, root))
        root_path = _write(base / "root.crt", ca_lib.cert_to_pem(root.cert))

        server = _server_cert(inter)
        server_crt = _write(base / "server.crt", ca_lib.cert_to_pem(server.cert))
        server_key = _write(base / "server.key", server.key_pem())

        # Tenant client certs.
        acme = ca_lib.issue_tenant_cert("acme", inter)
        acme_crt = _write(base / "acme.crt", ca_lib.cert_to_pem(acme.cert))
        acme_key = _write(base / "acme.key", acme.key_pem())
        badco = ca_lib.issue_tenant_cert("badco", inter)
        badco_crt = _write(base / "badco.crt", ca_lib.cert_to_pem(badco.cert))
        badco_key = _write(base / "badco.key", badco.key_pem())
        unknown = ca_lib.issue_tenant_cert("ghost", inter)  # chains, no registry row
        unknown_crt = _write(base / "ghost.crt", ca_lib.cert_to_pem(unknown.cert))
        unknown_key = _write(base / "ghost.key", unknown.key_pem())

        # Registry: acme active, badco revoked, ghost absent.
        registry = {
            acme.fingerprint_sha256(): {
                "tenant_id": "acme",
                "issued_at": "2026-06-24T00:00:00Z",
                "status": "active",
            },
            badco.fingerprint_sha256(): {
                "tenant_id": "badco",
                "issued_at": "2026-06-24T00:00:00Z",
                "status": "revoked",
            },
        }
        reg_path = _write(
            base / "tenant_registry.json", json.dumps(registry, indent=2).encode()
        )

        upstream = EchoUpstream()
        upstream_url = await upstream.start()

        port = _free_port()
        cfg = ProxyConfig(
            listen_host="127.0.0.1",
            listen_port=port,
            ca_chain_path=chain_path,
            tenant_registry_path=reg_path,
            tls_cert_path=server_crt,
            tls_key_path=server_key,
            upstream_url=upstream_url,
        )
        proxy = AuthProxy(cfg)
        await proxy.start()
        base_url = f"https://127.0.0.1:{port}"

        client_ctx = ssl.create_default_context(cafile=str(root_path))
        client_ctx.load_verify_locations(cafile=str(chain_path))

        try:
            # 1. valid acme → upstream sees acme
            async with httpx.AsyncClient(
                verify=client_ctx, cert=(str(acme_crt), str(acme_key))
            ) as c:
                r = await c.get(f"{base_url}/api/v1/query?q=up")
            check(r.status_code == 200, "valid acme cert → 200")
            check(
                r.json()["headers"].get("x-scope-orgid") == "acme",
                "upstream received X-Scope-OrgID: acme",
            )

            # 2. spoofed header → overridden to acme
            async with httpx.AsyncClient(
                verify=client_ctx, cert=(str(acme_crt), str(acme_key))
            ) as c:
                r = await c.get(
                    f"{base_url}/x", headers={"X-Scope-OrgID": "globex"}
                )
            hdrs = r.json()["headers"]
            check(
                hdrs.get("x-scope-orgid") == "acme",
                "client-set X-Scope-OrgID: globex overridden to acme",
            )
            check("globex" not in json.dumps(hdrs), "spoofed 'globex' did not leak")

            # 3. revoked cert → 403
            async with httpx.AsyncClient(
                verify=client_ctx, cert=(str(badco_crt), str(badco_key))
            ) as c:
                r = await c.get(f"{base_url}/x")
            check(r.status_code == 403, "revoked cert → 403")
            check("x-scope" not in r.text.lower(), "revoked cert NOT forwarded")

            # 4. unknown cert → 403 (fail-closed)
            async with httpx.AsyncClient(
                verify=client_ctx, cert=(str(unknown_crt), str(unknown_key))
            ) as c:
                r = await c.get(f"{base_url}/x")
            check(r.status_code == 403, "unknown cert → 403 (fail-closed)")

            # 5. no client cert → REJECTED, never forwarded. The TLS stack may
            #    abort the handshake (TransportError) OR — depending on the
            #    OpenSSL/TLS1.3 negotiation — let the connection establish with an
            #    empty peer cert, in which case the resolver fail-closes to 403.
            #    BOTH are correct: the unauthenticated request is never proxied.
            #    A 200 (proxied) would be the only failure.
            try:
                async with httpx.AsyncClient(verify=client_ctx) as c:
                    r = await c.get(f"{base_url}/x")
                check(
                    r.status_code == 403,
                    "no client cert → 403 (resolver fail-closed; never forwarded)",
                )
            except httpx.TransportError:
                check(True, "no client cert → handshake rejected (never forwarded)")
        finally:
            await proxy.aclose()
            await upstream.stop()

    print()
    if failures:
        print(f"{len(failures)} FAILED: {failures}")
        return 1
    print("ALL AUTH-PROXY SELF-TESTS PASSED")
    return 0


def main() -> int:
    return asyncio.run(_run())


if __name__ == "__main__":
    raise SystemExit(main())
