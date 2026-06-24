# SAST Pass — Auth Proxy (WS-AUTHPROXY-SEC)

Static analysis of `control-plane/auth-proxy/` source. Two layers:

1. **Automated:** `bandit` (installed into the project venv) over the proxy
   source. Raw JSON in `security/bandit_report.json`.
2. **Manual** injection / SSRF / error-leak / fail-open review against the
   security-critical request path.

Tooling:
```
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python -m bandit \
    -r proxy.py tenant_resolver.py config.py gen_server_cert.py selftest.py
# bandit 1.9.4 ; 872 LOC scanned
```

---

## 1. Bandit results

| Severity | Count |
|----------|-------|
| High | 0 |
| Medium | 1 |
| Low | 3 |

All findings are **low-risk / false-positive in this context**. Detail:

### B104 — `hardcoded_bind_all_interfaces` (Medium/Medium) — `config.py:51`
`DEFAULT_LISTEN_HOST = "0.0.0.0"`. **Accepted, by design.** The proxy is a
network ingress and binds all interfaces inside its container/network namespace;
the listen host is overridable via `AUTH_PROXY_LISTEN_HOST` (`config.py:168`) and
the test fabric binds `127.0.0.1`. Exposure is governed by the `cp-net` network
boundary and the orchestrator's published-port policy (C5), not by this default.
Not a vulnerability on its own.

### B110 — `try_except_pass` (Low/High) — `proxy.py:182`, `proxy.py:384`
Both are in **teardown / send-failure** paths (close a writer; abort a response
we can no longer send). Swallowing here is correct: the security decision has
already been made (an unauthenticated request was *never* forwarded), and the
only thing left is to drop the connection cleanly. **Accepted.** Note: these
broad `except Exception` blocks should not be copied into the *decision* path —
and they are not; the decision path raises typed `TenantResolutionError` and
fail-closes (`tenant_resolver.py`).

### B101 — `assert_used` (Low/High) — `proxy.py:273`
`assert self._client is not None` before forwarding. Under `python -O` asserts
are stripped, which would turn this into an `AttributeError` on `None` if
`_proxy_upstream` were somehow called before `start()`. **Low risk** (it cannot
happen via the public lifecycle — `start()` always sets `_client`), but for a
security component running possibly with `-O`, prefer an explicit
`if self._client is None: raise RuntimeError(...)`. **Minor hardening,
non-blocking.**

**Bandit verdict:** no High findings; the one Medium is an intended bind; the
Lows are teardown/`assert` patterns. Bandit does **not** model the SSRF (F1
below) because it cannot see the httpx URL-join semantics — found by manual
review.

---

## 2. Manual review checklist

| Class | Check | Result |
|-------|-------|--------|
| **Identity injection** | Is `tenant_id` ever sourced from a request header/body/query? | **NO** — only from `ResolvedTenant.tenant_id` (verified SAN). `proxy.py:300-327`. |
| **Header smuggling** | Are all scope-header casings/duplicates/prefixes stripped before inject? | **YES** — lowercased compare + prefix denylist, every pair. `proxy.py:310-327`. Tested A7/A10. |
| **Fail-open: no cert** | Can a no-cert request be forwarded? | **NO** — `CERT_REQUIRED` + `REASON_NO_CERT` 403. Tested A5. |
| **Fail-open: unknown cert** | Is an unregistered-but-CA-valid cert allowed? | **NO** — `is_revoked` unknown⇒True. `registry.py:151-153`. Tested A4. |
| **Fail-open: registry unreadable** | What if the registry file is corrupt? | **DENY-ALL** — `REASON_REGISTRY_ERROR` 403. `tenant_resolver.py:189-197`. |
| **Revocation** | Does a flipped `status:revoked` take effect immediately? | **YES** — registry re-read per request (no cache). `config.py:108`, `registry.py:45-53`. Tested A3. |
| **Foreign CA** | Can a leaf off another CA be accepted? | **NO** — TLS trust store + in-proc re-verify. Tested A6/A8. |
| **SAN↔registry** | Is a SAN/registry tenant mismatch rejected? | **YES** — `tenant_resolver.py:204-213`. Tested A9. |
| **Error leak** | Do reject responses leak cert/tenant detail? | **NO** — flat `Forbidden\n` body; reason logged server-side only. `proxy.py:224,369-387`. Tested A3. |
| **Upstream error leak** | Does an upstream 5xx echo upstream internals? | **NO** — flat `502 Bad Gateway`. `proxy.py:235`. |
| **Body DoS** | Is request body size bounded? | **YES** — 64 MiB cap ⇒ 403. `proxy.py:74,353-354`. |
| **SSRF** | Can the request target re-point the upstream host? | **YES — VULNERABLE.** See **F1** below. Tested A12. |
| **Header forward policy** | Allowlist or denylist for forwarded headers? | Denylist (hop-by-hop + scope + host/content-length). See **F2**. |
| **TLS floor** | Minimum protocol version? | TLS 1.2 (`proxy.py:99`). See **F3**. |
| **Secret handling** | Are keys/certs logged? | **NO** — only paths + closed-set reasons are logged. |

---

## Findings

### F1 — **HIGH: SSRF via absolute-form request target** (`proxy.py:269-279`)

**The single material SAST finding.** The proxy forwards `request.target`
verbatim:

```python
target = request.target.decode("latin-1")          # proxy.py:270
...
upstream = await self._client.request(method, target, ...)   # proxy.py:274
```

h11 parses an **absolute-form** request line into `request.target` unchanged
(verified: `GET http://169.254.169.254/latest/ HTTP/1.1` →
`b'http://169.254.169.254/latest/'`), and httpx's URL join treats an absolute
target as a **full override** of the client `base_url` host (verified:
`base_url=http://mimir:9009`, `target='http://169.254.169.254/latest/'` →
request URL `http://169.254.169.254/latest/`).

**Impact.** Any holder of a **valid, active tenant cert** can make the proxy
issue HTTP requests to **any host/port reachable from `cp-net`** — internal
control-plane services or the cloud **metadata endpoint** (169.254.169.254,
potential credential theft). The injected `X-Scope-OrgID` remains the correct
tenant, so this is **not a cross-tenant scope leak**, but it **is** SSRF /
network-containment break from a trusted position.

**Proof.** `security/test_isolation.py::test_A12_absolute_form_target_is_ssrf`
reaches a second off-upstream echo server through the proxy (currently `xfail`,
documenting the live exploit; a fix turns it XPASS).

**Remediation (required to fully clear this gate):** reject any non-origin-form
target before forwarding, e.g. at the top of `_proxy_upstream`:

```python
if not target.startswith("/"):
    raise _Http403("non_origin_form_target")
```

and/or build the upstream URL explicitly so the target's scheme/host can never
take effect:

```python
from urllib.parse import urlsplit
parts = urlsplit(target)
if parts.scheme or parts.netloc:
    raise _Http403("absolute_form_target")
path = target  # origin-form only
```

After the fix, flip A12 to assert a 403 / that the internal host is never
reached.

### F2 — LOW: header forwarding is a denylist, not an allowlist
(`proxy.py:300-327`)

Only hop-by-hop, the scope header, `x-scope-org*`, `host`, and `content-length`
are removed; **all other** client headers are forwarded to the upstream. For
tenant scoping this is safe (Mimir keys only on the canonical scope header, which
is always stripped+reinjected), but a defense-in-depth allowlist (forward only
the small set Mimir/Loki need) would shrink the surface. Non-blocking.

### F3 — LOW: TLS floor is 1.2, no explicit cipher pin (`proxy.py:99`)

`minimum_version = TLSv1_2`. For a control-plane ingress consider raising to TLS
1.3. Channel-only concern; does not affect client-cert verification. Non-blocking.

### F4 — INFO: `assert` in the forward path (`proxy.py:273`) — see Bandit B101.

### F5 — INFO: no success access-log / audit sink. Rejections log a closed-set
reason; successful forwards are not access-logged with `tenant_id`+`fingerprint`.
Recommend an append-only access log (ties into the P5 `audit/` component). Not an
isolation breach.

---

## SAST verdict

- **Automated (bandit):** 0 High, 1 Medium (intended bind), 3 Low (teardown /
  assert). Clean.
- **Manual:** the tenant-isolation core is sound and fail-closed on every modeled
  path. **One HIGH finding (F1, SSRF)** is confirmed exploitable and must be
  remediated; it does **not** compromise tenant *scoping* but does break upstream
  network containment. F2–F5 are non-blocking hardening items.
