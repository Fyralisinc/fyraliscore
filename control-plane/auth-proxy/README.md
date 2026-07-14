# auth-proxy — the Fyralis tenant auth proxy (P2, Invariant I4)

The **single most security-critical component** in the control plane. An
**mTLS-terminating reverse proxy** that sits in front of central
**Mimir / Loki / Grafana** and is the *only* place a request's tenant identity is
established — **server-side, from the verified client certificate**, never from a
request header.

> If you change one thing here, change it knowing this is the gate that keeps
> tenant A from reading tenant B's metrics.

---

## What it does (exact behavior)

For every request:

1. **Terminate client mTLS.** The TLS server context **requires** a client cert
   (`ssl.CERT_REQUIRED`) that chains to the **Fyralis CA** (`ca/pki/ca-chain.crt`,
   loaded via `load_verify_locations`). A client with **no cert**, or a cert that
   does **not chain** to the CA, **fails the handshake** — it never reaches HTTP.
2. **Extract `tenant_id` from the VERIFIED cert's URI SAN only.** After the
   handshake the proxy pulls the verified leaf (DER) straight off the SSL object
   and reads `tenant_id` out of its SPIFFE SAN
   `spiffe://fyralis/tenant/<id>` using **`ca/ca_lib.extract_tenant_from_cert`**.
   It **NEVER** trusts a `tenant_id` from a header, query, or body (I4).
3. **Revocation check (fail-closed).** It computes the leaf's SHA-256 fingerprint
   and consults the registry via **`ca/registry.is_revoked`** (already
   fail-closed): a **revoked OR unknown** fingerprint ⇒ **403**. A revoked cert's
   chain stays cryptographically valid, so this post-verification check is
   **mandatory**. It also asserts the registry row's `tenant_id` **equals** the
   SAN-derived one (C1) — a mismatch is 403.
4. **Strip + inject the scope header.** Every inbound `X-Scope-OrgID` (and any
   `x-scope-org*` casing/variant) is **stripped**, then a single
   `X-Scope-OrgID: <tenant_id>` derived from the cert is **injected**, and the
   sanitized request is **reverse-proxied** to the configured upstream (default
   Mimir).
5. **Never leak, never fail-open.** Missing cert, invalid chain, missing/invalid
   SAN, revoked/unknown fingerprint, SAN↔registry mismatch, or an unreadable
   registry all collapse to a flat **403** (no 5xx detail leak). An
   unauthenticated request is **never forwarded**.

```
   agent (mTLS client cert)            auth-proxy                 upstream (Mimir)
   ───────────────────────►  TLS: require+verify client cert  ──►
                             verify chain → extract SAN tenant
                             fingerprint → registry (fail-closed)
                             strip client X-Scope-OrgID
                             inject X-Scope-OrgID: <tenant_id>  ──► metrics, scoped
```

---

## Files

| File | Role |
|------|------|
| `proxy.py` | The server: asyncio + `h11` mTLS-terminating reverse proxy. Builds the `CERT_REQUIRED` SSL context, pulls the verified peer cert, runs the resolver, sanitizes headers, forwards via `httpx`. Run it directly: `python proxy.py`. |
| `tenant_resolver.py` | The fail-closed security core: **verify → extract → revoke → SAN↔registry agree**. Reuses `ca/verify_chain`, `ca/ca_lib`, `ca/registry`. Raises `TenantResolutionError` (→ 403) on every rejection path. |
| `config.py` | `ProxyConfig` + `load_config()` — ports, upstream URL, CA chain path, registry path, TLS server cert/key. Env-driven with safe defaults. |
| `gen_server_cert.py` | Mints the proxy's **own** server (serverAuth) cert from the CA so the handshake completes and clients can verify the proxy. |
| `run.sh` | Convenience launcher; resolves CA chain / registry / TLS material from env or repo defaults and `exec`s `proxy.py`. |
| `selftest.py` | Out-of-process end-to-end self-test over a real socket (the five behaviors below). Exit 0 = all held. |
| `Dockerfile` | Container image (build context = `control-plane/` root, so `ca/` is included). |
| `tests/` | `test_tenant_resolver.py` (pure resolver, no sockets) + `test_proxy.py` (real mTLS over sockets vs an echo upstream) + `conftest.py` fixtures. |

**Why not uvicorn/FastAPI?** A security proxy needs *direct* access to the
verified DER peer cert and *byte-level* control over which headers cross the
trust boundary. asyncio's SSL transport exposes the peer cert via
`getpeercert(binary_form=True)`; `h11` is a vetted sans-IO HTTP/1.1 state machine
(no hand-rolled parsing). This keeps the security-critical path small and
auditable. `hypercorn` was not available in the target environment.

---

## Configuration (env)

| Env var | Default | Meaning |
|---------|---------|---------|
| `AUTH_PROXY_LISTEN_HOST` | `0.0.0.0` | bind host |
| `AUTH_PROXY_LISTEN_PORT` | `8443` | mTLS listen port |
| `AUTH_PROXY_CA_CHAIN` | `../ca/pki/ca-chain.crt` | CA chain that **verifies client certs** (C1) |
| `AUTH_PROXY_TENANT_REGISTRY` | `../ca/tenant_registry.json` | fingerprint→tenant revocation registry (C1) |
| `AUTH_PROXY_TLS_CERT` | *(required)* | the proxy's **own** server cert |
| `AUTH_PROXY_TLS_KEY` | *(required)* | the proxy's own server key |
| `AUTH_PROXY_UPSTREAM_URL` | `http://mimir:9009` | reverse-proxy target (C5) |
| `AUTH_PROXY_UPSTREAM_TIMEOUT` | `30` | upstream timeout (s) |

The CA chain + registry are **bind-mounted from the live `ca/`** in compose so a
revocation written by WS-CA takes effect without a rebuild. The registry is
re-read fresh on every request by default (correctness over a tiny `stat()`).

---

## How to run (local)

```bash
# 0. one-time: create the CA + issue a tenant cert (WS-CA tooling)
cd control-plane/ca
python bootstrap_ca.py
python issue_cert.py issue acme            # writes a registry row, status=active

# 1. mint the proxy's own server cert from that CA
cd ../auth-proxy
python gen_server_cert.py --san localhost --san 127.0.0.1 --san auth-proxy

# 2. start the proxy
AUTH_PROXY_TLS_CERT=./tls/proxy-server.crt \
AUTH_PROXY_TLS_KEY=./tls/proxy-server.key \
AUTH_PROXY_UPSTREAM_URL=http://localhost:9009 \
./run.sh

# 3. hit it WITH the tenant client cert (note: --cert presents acme's identity)
curl --cacert ../ca/pki/ca-chain.crt \
     --cert ../ca/pki/tenants/acme/acme.crt \
     --key  ../ca/pki/tenants/acme/acme.key \
     https://localhost:8443/prometheus/api/v1/query?q=up
# → upstream receives the request with header  X-Scope-OrgID: acme
```

---

## How to test

```bash
cd control-plane/auth-proxy

# unit + E2E suite (18 tests: resolver logic + real mTLS over sockets)
python -m pytest tests/ -q

# out-of-process end-to-end self-test (real socket, real TLS, echo upstream)
python selftest.py
```

The self-test asserts, against a throwaway CA on /tmp:

1. valid **acme** cert → upstream sees `X-Scope-OrgID: acme`;
2. client-set `X-Scope-OrgID: globex` → **overridden to acme** (the cert wins, I4);
3. **revoked** cert → **403**, never forwarded;
4. **unknown** cert (chains but no registry row) → **403** (fail-closed);
5. **no** client cert → rejected (handshake abort *or* 403 — never proxied).

### Related lib reconciliation test

This work also made `lib/tenant.py`'s `TenantRegistry.is_revoked()` **fail-closed**
to match `ca/registry.is_revoked` (see *Caveats*). That regression test lives in
`lib/` and is run from the **control-plane root** so `lib` imports as a package:

```bash
cd control-plane
python -m pytest lib/test_tenant_failclosed.py -q --import-mode=importlib
# or the standalone runner:
python -m lib.test_tenant_failclosed
```

---

## Docker / compose

The image build context is the **control-plane root** (so `ca/` is included):

```bash
cd control-plane
docker build -f auth-proxy/Dockerfile -t fyralis/auth-proxy .
```

`docker-compose.control-plane.yml` already carries a commented `auth-proxy`
service stub (port `8443:8443`, on `cp-net`, `depends_on: mimir`). When the
scaffold owner enables it, set the build to use this Dockerfile from the root
context and mount the live CA material + the proxy server cert:

```yaml
auth-proxy:
  build:
    context: .
    dockerfile: auth-proxy/Dockerfile
  ports: ["8443:8443"]
  environment:
    AUTH_PROXY_UPSTREAM_URL: http://mimir:9009
    AUTH_PROXY_TLS_CERT: /tls/proxy-server.crt
    AUTH_PROXY_TLS_KEY: /tls/proxy-server.key
  volumes:
    - ./ca/pki/ca-chain.crt:/app/ca/pki/ca-chain.crt:ro
    - ./ca/tenant_registry.json:/app/ca/tenant_registry.json:ro   # live revocation
    - ./auth-proxy/tls:/tls:ro
  networks: [cp-net]
  depends_on: [mimir]
```

---

## Security notes & caveats

* **Defense in depth on identity.** The TLS layer already requires + verifies the
  client cert against the CA, but the resolver **re-verifies the chain itself**
  (`ca/verify_chain.verify_chain`) before trusting the SAN. The security decision
  never depends solely on the TLS stack's configuration being correct.
* **No-cert behavior is OpenSSL/TLS-version-dependent.** With TLS 1.2 the server
  aborts the handshake on a missing client cert; with TLS 1.3 some stacks let the
  connection establish with an empty peer cert. Either way the request is
  **rejected** — the resolver fail-closes a certless request to 403 and **never
  forwards** it. (The test suite accepts both outcomes; a `200` would be the only
  failure.)
* **Revocation latency.** The registry is consulted per request and (by default)
  re-read fresh each time, so a `revoked` flip is effective immediately. There is
  **no CRL/OCSP** — revocation is registry-lookup only (matches WS-CA's design).
* **Fail-closed everywhere.** An unreadable registry, an unparseable cert, a
  missing/duplicate SAN, or a SAN↔registry tenant mismatch all **deny**. There is
  no code path that returns a tenant id from caller-supplied input.
* **Hop-by-hop hygiene.** Standard hop-by-hop headers (RFC 7230 §6.1) are not
  forwarded; `Host`/`Content-Length` are recomputed by the upstream client.
* **HTTP/1.1 only.** The proxy speaks HTTP/1.1 (h11). Mimir/Loki ingest + query
  are HTTP/1.1-compatible; if an upstream needs HTTP/2 streaming, front it
  accordingly.
* **Reconciled inconsistency (lib/tenant.py).** Before this work, two
  fingerprint-status readers disagreed: `ca/registry.is_revoked` was **fail-closed**
  (unknown ⇒ revoked) but `lib/tenant.py`'s `TenantRegistry.is_revoked()` was
  **fail-OPEN** (unknown ⇒ `False`), so a gate `if reg.is_revoked(fp): reject()`
  using the lib reader would have **let an unregistered cert through**. `is_revoked()`
  is now a **fail-closed deny predicate** matching `ca/registry`; the precise-reason
  surface (`tenant_for_fingerprint`, which distinguishes unknown from revoked) is
  unchanged. The proxy itself uses `ca/registry`, but the two readers now agree so
  no future caller inherits the unsafe default.
