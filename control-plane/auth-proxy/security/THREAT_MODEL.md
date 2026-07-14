# Auth Proxy Threat Model (STRIDE) — WS-AUTHPROXY-SEC

**Component:** `control-plane/auth-proxy/` — the mTLS-terminating reverse proxy in
front of central Mimir/Loki/Grafana. It is the **sole** establishment point for
tenant identity in the control plane (Invariant **I4**, risk **R1**).

**Trust boundary:** the data-plane agent (untrusted network) on the outside; the
`cp-net` Docker network (trusted, Mimir trusts `X-Scope-OrgID` only because it
arrives from inside `cp-net` behind this proxy — C5) on the inside. Everything
the proxy decides hinges on **the verified client-cert SPIFFE SAN, never a
header** (C1).

Code citations are `file:line` against the reviewed tree (`proxy.py`,
`tenant_resolver.py`, `config.py`, and the reused `ca/` library).

**Scope of guarantees.** The single contractual guarantee under review is:
*the org id forwarded to the upstream is derived only from a verified, active,
registry-agreed client cert, and is never influenced by caller-supplied input.*
Every threat below is scored against that guarantee. Verdicts: **MITIGATED** (a
real control exists and is exercised by `security/test_isolation.py`),
**PARTIAL** (control exists but has a residual gap worth tracking),
**OUT-OF-SCOPE-BUT-NOTED** (handled by a neighboring component / deployment).

---

## STRIDE summary table

| # | STRIDE | Threat | Verdict | Control (code) | Adversarial test |
|---|--------|--------|---------|----------------|------------------|
| T1 | Spoofing | Tenant spoofing via `X-Scope-OrgID` header | **MITIGATED** | `proxy.py:300-327` strip+inject | A2, A10, A11 |
| T2 | Spoofing | SAN forgery (self-minted `spiffe://fyralis/...`) | **MITIGATED** | `tenant_resolver.py:166-175` chain re-verify | A6, A8 |
| T3 | Spoofing | Cert from a different / untrusted CA | **MITIGATED** | TLS `CERT_REQUIRED` `proxy.py:94-95` + re-verify | A6 |
| T4 | Spoofing | No client cert (fail-open?) | **MITIGATED** | `proxy.py:95` + `tenant_resolver.py:150-153` | A5 |
| T5 | Tampering | Revoked / expired cert accepted | **MITIGATED** | `tenant_resolver.py:186-202` fail-closed registry; validity in `verify_chain` | A3 |
| T6 | Tampering | Unknown (unregistered) cert fail-open | **MITIGATED** | `registry.is_revoked` unknown⇒True `registry.py:144-154` | A4 |
| T7 | Tampering | Header smuggling / duplicate / case-variant | **MITIGATED** | `proxy.py:300-327` lower-case + prefix strip | A7, A10 |
| T8 | Repudiation | No audit of who was scoped / rejected | **PARTIAL** | structured reasons `tenant_resolver.py:80-86`, logged `proxy.py:223` | (n/a) |
| T9 | Info-disclosure | Error message leaks tenant / cert internals | **MITIGATED** | flat 403 body `proxy.py:224`; reason kept server-side | A3 |
| T10 | Info-disclosure | SAN↔registry mismatch lets wrong tenant through | **MITIGATED** | `tenant_resolver.py:204-213` | A9 |
| T11 | DoS | Oversized body / connection exhaustion | **PARTIAL** | 64 MiB body cap `proxy.py:74,353-354`; upstream timeout | (n/a) |
| T12 | Elevation | Upstream SSRF via **absolute-form** request target | **VULNERABLE (confirmed)** | fixed `base_url` is bypassed by httpx URL join `proxy.py:269-279` | A12 (xfail, confirms exploit) |
| T13 | Elevation | TLS downgrade / weak protocol | **PARTIAL** | `minimum_version = TLSv1_2` `proxy.py:99` | (n/a) |

---

## T1 — Spoofing: tenant identity via the `X-Scope-OrgID` header

**Attack.** A data-plane agent (or anyone who obtains *a* valid Fyralis cert)
sends `X-Scope-OrgID: globex` while presenting acme's cert, hoping to read or
write another tenant's metrics in Mimir.

**Control.** `_sanitize_headers` (`proxy.py:300-327`) strips, **case-
insensitively**, the exact scope header (`lname == scope_lower`, line 312) and
**any** header whose lowercased name starts with a configured prefix
(`x-scope-org`, `config.py:113-115`; matched at `proxy.py:314`). Only after all
client copies are removed is a single `X-Scope-OrgID: <tenant_id>` appended
(line 326), where `tenant_id` comes exclusively from
`ResolvedTenant.tenant_id` — i.e. the verified cert SAN. There is no code path
that copies a request header into the injected scope.

**Residual risk.** Low. The strip is keyed on `config.scope_header.lower()` and
the `strip_header_prefixes` tuple; if an operator *narrowed* the prefix list to
`()` via misconfiguration, a `X-Scope-Org-Foo` variant could pass through to the
upstream as an *extra* header — but it would still not become the injected
`X-Scope-OrgID` (Mimir keys only on the canonical header, which is always
stripped at line 312). Defense-in-depth recommendation: keep the
`x-scope-org` prefix and consider an allowlist (forward only known-safe headers)
rather than a denylist. **Tested:** A2, A10, A11.

## T2 — Spoofing: SAN forgery (self-minted SPIFFE identity)

**Attack.** Attacker stands up their *own* CA, mints a leaf carrying
`spiffe://fyralis/tenant/acme`, and even pre-seeds a registry row keyed on its
fingerprint.

**Control.** Two independent gates. (1) The TLS server context trusts only the
Fyralis CA chain (`load_verify_locations(ca-chain.crt)` + `CERT_REQUIRED`,
`proxy.py:94-95`), so a foreign-CA leaf fails the handshake. (2) Even if a future
TLS-stack quirk let it through, `TenantResolver.resolve` re-verifies the chain
against the Fyralis CA *in process* (`verify_chain` at `tenant_resolver.py:166-
175`) before reading the SAN (line 179). The SAN is read only from an
already-chain-verified leaf. Identity therefore can never be self-asserted off a
foreign trust root.

**Residual risk.** Low. The re-verify uses `cryptography`'s native
`ClientVerifier` when available (`verify_chain.py:128-156`) which enforces path,
signatures, validity and the `clientAuth` EKU; a manual fallback exists for old
`cryptography` (`verify_chain.py:163-218`). The installed version is 48.0.1 →
native path is used. **Tested:** A6, A8.

## T3 — Spoofing: cert from a different / untrusted CA

**Attack.** Present a well-formed leaf chaining to *some* public/other CA.

**Control.** Same as T2 gate (1): `CERT_REQUIRED` + `load_verify_locations`
restricted to the Fyralis chain (`proxy.py:94-95`) ⇒ TLS handshake aborts.

**Residual risk.** Low. **Tested:** A6.

## T4 — Spoofing: no client cert (fail-open check)

**Attack.** Connect without presenting any client cert and hope the proxy
defaults to *some* tenant.

**Control.** `verify_mode = ssl.CERT_REQUIRED` (`proxy.py:95`) makes the TLS
handshake fail when no client cert is offered. Belt-and-suspenders:
`_peer_cert_der` can return `None` (`proxy.py:186-199`) and `resolve` raises
`REASON_NO_CERT` on a falsy DER (`tenant_resolver.py:150-153`), which the proxy
collapses to a flat 403 (`proxy.py:221-228`). There is **no** default tenant.

**Residual risk.** Low. With TLS 1.3 some stacks complete the handshake and only
surface the missing cert at app layer; the resolver covers that exact case
fail-closed. **Tested:** A5.

## T5 — Tampering: revoked or expired cert accepted

**Attack.** Use a cert that once was valid but has been revoked, or whose
validity window has passed.

**Control.** Revocation: every request computes the leaf SHA-256 fingerprint
(`tenant_resolver.py:188`) and calls `registry.is_revoked` (line 190), which
returns `True` for a `status != "active"` row (`registry.py:144-154`); a `True`
raises 403 (lines 198-202). The registry is re-read on every request
(`registry.load_registry` opens the file each call; `config.registry_fresh_
every_request = True`, `config.py:108`) so a revocation takes effect
immediately — no cache to poison. Expiry: validity window is enforced both by
the TLS stack and by `verify_chain` (`_within_validity`, `verify_chain.py:278-
281`, and the native verifier's time policy).

**Residual risk.** Medium-low. There is **no CRL/OCSP** — revocation is a
registry-lookup only (documented caveat in `ca/ca_lib.py:67-70`). This is
acceptable because the registry is read fresh per request and leaves are short
(90 days, `ca_lib.py:71`), bounding the miss window, but a stolen *unrevoked*
cert is valid until its row is flipped or it expires. Recommendation: short leaf
TTL + prompt registry revocation remain the operational controls. **Tested:** A3
(revoked); expiry is covered structurally by `verify_chain` (no live
clock-warp test here — see caveats).

## T6 — Tampering: unknown (unregistered) cert fail-open

**Attack.** A cert that legitimately chains to the Fyralis CA but for which no
registry row exists (e.g. issued-then-never-registered, or a registry-write
race).

**Control.** `registry.is_revoked` returns `True` when the row is **absent**
(`registry.py:151-153`) — unknown is treated as revoked. Fail-closed by default,
inherited by every caller. The proxy turns that into 403.

**Residual risk.** Low. If the registry **file** is unreadable/corrupt,
`load_registry` raises, `resolve` catches it and raises `REASON_REGISTRY_ERROR`
(`tenant_resolver.py:189-197`) ⇒ 403 — "deny everything" when the source of
truth is unreadable, never fail-open. **Tested:** A4.

## T7 — Tampering: header smuggling / duplication / case variants

**Attack.** Send multiple `X-Scope-OrgID` headers, mixed-case
(`x-ScOpE-OrgID`), or prefix variants (`X-Scope-Org-Smuggle`) to get one copy
past the strip.

**Control.** The strip in `_sanitize_headers` iterates **every** inbound header
pair (`proxy.py:310`), lowercases each name (line 311), and drops it if it equals
the scope header (line 312) **or** starts with any strip prefix (line 314).
Because h11 surfaces duplicate headers as separate `(name, value)` pairs, **all**
copies are removed regardless of count or casing. Hop-by-hop and `Host`/
`Content-Length` are also normalized (lines 316-323).

**Residual risk.** Low. The denylist approach means a brand-new header the
attacker invents (not matching `x-scope-org`) is forwarded verbatim — harmless
for tenant scoping (Mimir ignores it) but see T1's allowlist recommendation.
**Tested:** A7 (raw socket with 3 duplicate/case-variant + 1 prefix variant),
A10.

## T8 — Repudiation: insufficient audit trail

**Attack.** After an incident, an operator cannot prove which tenant a request
was scoped to or why a cert was rejected.

**Control.** Rejections log a closed-set machine `reason`
(`tenant_resolver.py:80-86`) at WARNING (`proxy.py:223`). Reasons are greppable
and do not leak cert internals.

**Residual risk.** **PARTIAL.** Successful forwards are **not** access-logged
(no per-request success line with tenant + path), and there is no structured
audit sink (the SPRINT_PLAN `audit/` component is P5, not wired here). For a
gating I4 control, an append-only access log keyed on `tenant_id` +
`fingerprint` is recommended. Not exploitable for isolation breach; tracked as a
follow-up.

## T9 — Information disclosure: error messages leak tenant / cert internals

**Attack.** Probe the proxy with bad certs to extract tenant ids, fingerprints,
or chain-failure detail from error bodies.

**Control.** Every rejection returns a **flat** `Forbidden\n` body with status
403 (`_send_simple`, `proxy.py:224,369-387`). The descriptive `reason`/`detail`
in `TenantResolutionError` is **never** put on the wire — only logged
server-side (`tenant_resolver.py:65-76`). Upstream errors return a flat
`502 Bad Gateway` (`proxy.py:235`). No upstream JSON echo can leak through on a
reject path because the reject happens **before** `_proxy_upstream` is called.

**Residual risk.** Low. Timing side-channels (handshake-fail vs app-403) could in
principle distinguish "untrusted CA" from "unknown fingerprint", but neither
reveals another tenant's data. **Tested:** A3 asserts body is exactly
`Forbidden`; A3/A4/A8/A9 assert the upstream is never reached.

## T10 — Information disclosure: SAN↔registry mismatch

**Attack.** A cert whose SAN says `acme` but whose registry row says `globex`
(misissuance, registry tampering, or fingerprint reuse) could scope a request to
the wrong tenant.

**Control.** After revocation passes, the resolver fetches the row and requires
`registry_tenant == san_tenant` (`tenant_resolver.py:204-213`); any disagreement
raises `REASON_SAN_REGISTRY_MISMATCH` ⇒ 403. Neither the SAN tenant nor the
registry tenant is forwarded on a mismatch — the request is dropped entirely.

**Residual risk.** Low. **Tested:** A9.

## T11 — DoS: oversized body / resource exhaustion

**Attack.** Stream an unbounded request body or many slow connections.

**Control.** A 64 MiB body cap (`_MAX_REQUEST_BODY`, `proxy.py:74`) raises
`request_body_too_large` ⇒ 403 (`proxy.py:353-354`). Upstream calls have a
configurable timeout (`config.py:103`, applied at `proxy.py:128-130`). Per-
connection handlers are isolated and a handler bug cannot crash the accept loop
(`proxy.py:176-177`).

**Residual risk.** **PARTIAL.** No per-IP connection cap, no slow-loris read
timeout on the *client* read (`reader.read` has no deadline), no concurrency
limit. In production the proxy sits behind `cp-net` and should be fronted by a
network-level rate limiter / the orchestrator's resource limits. Not an
isolation breach. Tracked as a follow-up.

## T12 — Elevation: upstream SSRF via request target

**Attack.** Send a request whose target is an absolute URL
(`GET http://169.254.169.254/ HTTP/1.1`) or `..`-traversal hoping the proxy
forwards to an attacker-chosen host.

**Control (intended).** The upstream host is pinned at construction to `base_url`
(`config.upstream_url`, `proxy.py:127-130`); the per-request call is expected to
pass only the **path-and-query** `request.target` to `httpx` (`proxy.py:269-
279`). For a normal `/`-leading target this works — `GET /api/v1/query` resolves
against `base_url` to the pinned upstream.

**Residual risk — CONFIRMED VULNERABLE (gating).** The proxy forwards
`request.target` **verbatim** (`proxy.py:270`, `target = request.target.decode
(...)`). h11 will happily parse an **absolute-form** request line
(`GET http://169.254.169.254/latest/ HTTP/1.1` → `request.target ==
b'http://169.254.169.254/latest/'`, verified), and httpx's URL-join treats an
absolute target as a **full override of the `base_url` host**:

```
base_url=http://mimir:9009 , target='http://169.254.169.254/latest/'
  -> request URL = http://169.254.169.254/latest/   # host re-pointed!
```

So **any holder of a valid, active tenant cert** can make the proxy issue
requests to an **arbitrary host/port reachable from inside `cp-net`** — internal
control-plane services, or the cloud **metadata endpoint** (169.254.169.254).
This is a server-side request forgery (SSRF) and an isolation/elevation concern
even though the injected `X-Scope-OrgID` is still the *correct* tenant (acme):
it is **not** a cross-tenant scope leak, but it **is** an attacker-controlled
egress from the proxy's network position. **Proven live by test A12**
(`security/test_isolation.py`), which reaches a second off-upstream echo server
through the proxy (the test is `xfail` so it documents + regression-guards the
exploit while keeping the suite green; a fix flips it to XPASS).

**Remediation (required before this gate is fully clean):** before forwarding,
reject any `request.target` that is not origin-form (does not start with `/`) —
i.e. reject absolute-form and authority-form targets — and/or rebuild the
upstream URL explicitly from `base_url` + a sanitized path, ignoring any
scheme/host in the target. See **SAST.md F1**.

## T13 — TLS downgrade / weak protocol

**Attack.** Force a downgrade to TLS 1.0/1.1 or a weak cipher to attack the
channel.

**Control.** `ctx.minimum_version = ssl.TLSVersion.TLSv1_2` (`proxy.py:99`) on a
context built from `ssl.create_default_context` (modern cipher defaults, server
side). `check_hostname=False` is deliberate (identity is the SPIFFE SAN, not a
DNS name — `proxy.py:96-97`) and does **not** weaken client-cert verification,
which is governed by `CERT_REQUIRED` + the trust store.

**Residual risk.** **PARTIAL.** No explicit `minimum_version = TLSv1_3` floor and
no explicit cipher-suite pinning; TLS 1.2 with default ciphers is the floor.
For a control-plane ingress, consider raising the floor to TLS 1.3. Not an
isolation breach. Tracked as a follow-up.

---

## Residual-risk register (rolled up)

| Risk | Severity | Status | Owner action |
|------|----------|--------|--------------|
| No CRL/OCSP — revocation is registry-only (T5) | Medium-low | Accepted (short TTL + fresh read) | Keep leaf TTL ≤ 90d; prompt revocation |
| Header forwarding is a denylist, not allowlist (T1/T7) | Low | Open | Switch to allowlist of forwarded headers |
| No success access-log / audit sink (T8) | Low | Open (P5 `audit/`) | Append-only per-request tenant log |
| **SSRF: absolute-form request target re-points upstream host (T12)** | **HIGH (confirmed exploit, A12)** | **OPEN — must fix** | Reject non-origin-form targets; rebuild URL from base_url+path |
| TLS floor is 1.2, no cipher pin (T13) | Low | Open | Consider TLS 1.3 floor |
| No client-side read deadline / conn cap (T11) | Low | Open | Front with rate limiter; add read timeout |

**Gate verdict.** The **core I4 tenant-isolation guarantee is MITIGATED and
proven**: the upstream is never observed receiving a **cross-tenant or
client-controlled org id** under any of the 11 modeled attacks (A1–A11 all
green). A spoofed/duplicated/case-variant header, a revoked/unknown/foreign cert,
a no-cert connection, and a SAN↔registry mismatch are all fail-closed with
nothing forwarded.

**HOWEVER** — a **HIGH-severity SSRF (T12 / SAST F1) is CONFIRMED EXPLOITABLE**
(test A12): a valid-cert client can re-point the proxy's upstream to an arbitrary
host via an absolute-form request target. This does **not** break tenant
*scoping*, but it does break the proxy's network containment and **must be fixed
before WS-AUTHPROXY-SEC can be signed off as fully clean.** The remaining items
in the register are lower-severity hardening follow-ups.
