# auth-proxy/security/ — WS-AUTHPROXY-SEC

Gating security review of the Fyralis auth proxy (Invariant **I4**, risk **R1**):
the proxy is the *only* place tenant identity is established, and it must come
**only** from the verified client-cert SPIFFE SAN — never from a caller-supplied
header. This directory is the disjoint, write-only output of that review.

## What's here

| File | What |
|------|------|
| `THREAT_MODEL.md` | STRIDE threat model (T1–T13): each threat, the in-code control with `file:line` citations, and residual risk. Includes the gate verdict. |
| `test_isolation.py` | **Adversarial** pytest suite. Starts the REAL proxy + a mock echo upstream and runs 12 attacks (A1–A12). Every forwarded case asserts the upstream NEVER sees a cross-tenant or client-controlled org id; every reject case asserts the upstream is NEVER reached. |
| `SAST.md` | SAST pass: `bandit` results + a manual injection/SSRF/error-leak/fail-open checklist, with findings F1–F5. |
| `bandit_report.json` | Raw bandit JSON. |

## How to test

From `control-plane/auth-proxy/` (so `proxy`/`config` and `../ca/*` import):

```bash
PYBIN=/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python

# Adversarial isolation suite (asyncio_mode=auto is inherited from pytest.ini):
$PYBIN -m pytest security/test_isolation.py -rxs

# SAST (bandit is installed in that venv):
$PYBIN -m bandit -r proxy.py tenant_resolver.py config.py gen_server_cert.py selftest.py
```

The suite is self-contained: it builds an in-process Fyralis CA **and** a second
adversary CA, a mock echo upstream that records every request it serves, and the
proxy on an ephemeral port. No Docker, no external services.

## The 12 attacks (A1–A12)

| # | Attack | Expected |
|---|--------|----------|
| A1 | acme cert | upstream sees `X-Scope-OrgID=acme` |
| A2 | acme cert + forged `X-Scope-OrgID: globex` | upstream still sees `acme` (header stripped) |
| A3 | revoked cert | 403, upstream never reached |
| A4 | unknown/unregistered cert | 403 (fail-closed) |
| A5 | no client cert | handshake-reject OR fail-closed 403; never 200 |
| A6 | cert from a different CA | handshake-reject OR 403; never forwarded |
| A7 | duplicate + case-variant + prefix scope headers (raw socket) | all stripped; upstream sees exactly `['acme']` |
| A8 | self-minted `spiffe://fyralis/...` off a foreign CA + matching registry row | 403 (chain re-verify wins) |
| A9 | registry says globex, SAN says acme | 403 (SAN↔registry mismatch) |
| A10 | valid acme + kitchen-sink smuggle headers | scoped acme; no foreign string leaks |
| A11 | two tenants interleaved, each spoofing the other's id | acme↔acme, globex↔globex, never crossed |
| A12 | **SSRF**: absolute-form request target → off-upstream host | **xfail — confirms the live exploit (F1)** |

## Results (last run)

```
11 passed, 1 xfailed   (security/test_isolation.py)
bandit: 0 High, 1 Medium (intended 0.0.0.0 bind), 3 Low
```

## Verdict & caveats (READ THIS)

- **Tenant isolation (I4) holds and is proven.** Under A1–A11 the upstream never
  receives a cross-tenant or client-controlled org id; spoofed/duplicate/
  case-variant headers, revoked/unknown/foreign/no certs, and SAN↔registry
  mismatches are all fail-closed with nothing forwarded.
- **GATING DEFECT — SSRF (HIGH).** Test **A12** confirms a live **server-side
  request forgery**: a valid-cert client can re-point the proxy's upstream to an
  arbitrary host (e.g. the cloud metadata endpoint) via an **absolute-form
  request target**, because `proxy.py:270` forwards `request.target` verbatim and
  httpx lets an absolute target override the pinned `base_url` host. This is
  **not** a cross-tenant scope leak, but it **is** an attacker-controlled egress
  from inside `cp-net`. **Must be fixed** (reject non-origin-form targets — see
  `SAST.md` F1 / `THREAT_MODEL.md` T12) before this gate is fully clean.
- A12 is `xfail(strict=False)` on purpose: it documents and regression-guards the
  exploit while keeping the suite green. When the host-pin fix lands, change A12
  to assert the internal host is never reached / a 403 is returned; it will then
  XPASS.
- Revocation is **registry-lookup only** (no CRL/OCSP); the registry is re-read
  per request and leaves are short (90d), bounding the miss window, but a stolen
  *unrevoked* cert is valid until its row is flipped or it expires.
- This review did not exercise a live clock-warp expiry test; expiry is covered
  structurally by `verify_chain` (`_within_validity`) and the TLS stack.
