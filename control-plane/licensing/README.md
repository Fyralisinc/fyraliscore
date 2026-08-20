# WS-LICENSE — signed, expiring licenses + a local fail-closed validator

This component issues **signed, expiring licenses** and gives the data-plane agent a
**local validator** that decides — fail-closed — whether the agent is allowed to operate.
A license is the cryptographic grant that says *"this deployment, for this tenant, on this
plan, with these features, may run until this instant."* The agent refuses to operate when
the license is missing, tampered, expired, for the wrong tenant/deployment, or revoked.

It is built on the control plane's existing primitives — it does **not** re-implement crypto:

| Reused from | What for |
|---|---|
| `control-plane/signing` (`sign_bundle`, `verify_bundle`, `signing_lib`) | ed25519 sign + verify against the trust root (C2 / I6) |
| `control-plane/lib` (`DeploymentRecord`) | the identity (`tenant_id` / `deployment_id`) the license is bound to (C4 / P4) |

---

## The license contract (the LICENSE JSON)

A license is a JSON document with the P4 shape (owned by `license_model.License`):

```json
{
  "tenant_id":     "acme",
  "deployment_id": "acme-use1-7f3a",
  "plan":          "enterprise",
  "issued_at":     "2026-06-24T00:00:00Z",
  "expires_at":    "2026-06-25T00:00:00Z",
  "features":      ["telemetry_t3", "byoc"],
  "license_id":    "lic-acme-3f9c1a2b",
  "version":       1
}
```

* `tenant_id` / `deployment_id` — the deployment this license is **bound to** (matched at
  validate time so a license cannot be reused on another deployment).
* `plan` — open enum; `trial | standard | pro | enterprise` are recognised, any non-empty
  string is accepted.
* `issued_at` / `expires_at` — RFC-3339 UTC (`...Z`), same grammar as `DeploymentRecord`.
* `features` — granted feature flags (list of strings).
* `license_id` — `lic-<tenant>-<8 hex>`; the precise handle the revocation list keys on.

### The signed *bundle* on disk

Signing produces the C2 **detached-signature trio** (the agent reads the directory):

```
license.json                 # the document above
license.json.sig             # base64 ed25519 signature over the canonical JSON bytes
license.json.manifest.json   # {artifact:"license", version, sha256, key_id, algo, signed_at}
```

The signed quantity is the **canonical compact JSON** of the document (sorted keys, no
whitespace), so the signature is independent of key ordering / formatting. Tampering with any
field changes those bytes and breaks the signature.

---

## How the agent validates (fail-closed)

`validator.validate()` is the single decision point. **All four gates must pass for ALLOW;
anything unprovable → DENY.** The signature is checked *first and unconditionally* — fields
are only trusted after the bytes that carry them are verified (verify-before-use, I6).

| # | Gate | Deny code | What it catches |
|---|------|-----------|-----------------|
| 1 | **Signature** (`verify_bundle` vs the trust root) | `deny_bad_signature` | tampered field, wrong/unknown/retired key, missing/corrupt sig, `sha256` mismatch |
| 2 | **Expiry** (`now < expires_at`, boundary = expired; `issued_at` not future) | `deny_expired` / `deny_not_yet_valid` | an expired (or not-yet-valid) license |
| 3 | **Identity** (license `tenant_id`/`deployment_id` == this deployment) | `deny_tenant_mismatch` / `deny_deployment_mismatch` | a license minted for another tenant/deployment (lateral reuse) |
| 4 | **Revocation** (not on the revocation list) | `deny_revoked` | a still-signed, still-unexpired license that was pulled early (FR-F) |

It returns a `Decision(allow, reason, code, license, checks)` — a clear allow/deny plus a
single-sentence human reason and a stable machine `code`. It **never raises for a bad
license** (every failure is a DENY); a malformed JSON, an unreadable revocation list, or a
wrong-artifact-kind bundle all fail *closed* to DENY.

```python
from validator import validate
d = validate(license_dir="/etc/fyralis/license",
             expected_tenant_id="acme", expected_deployment_id="acme-use1-7f3a")
if not d.allow:
    log.error("license denied: %s", d.reason)   # refuse to operate
```

`validate_for_deployment(record, ...)` binds the identity gate directly from a C4
`DeploymentRecord` dict, so the agent never skips it:

```python
from validator import validate_for_deployment
d = validate_for_deployment(deployment_record, license_dir="/etc/fyralis/license")
```

---

## Quick start (CLI)

```bash
PY=/path/to/.venv/bin/python      # any venv with `cryptography` (+ fastapi for the service)

# 0. One-time: the control plane must have a signing key (owned by WS-SIGNING).
#    python ../signing/keygen.py --activate      # mints signing/keys + trust_root.json

# 1. Issue a 1-day enterprise license for acme's deployment
$PY issue_license.py \
    --tenant-id acme --deployment-id acme-use1-7f3a \
    --plan enterprise --duration-days 1 \
    --feature telemetry_t3 --feature byoc \
    --out /etc/fyralis/license          # writes license.json + .sig + .manifest.json

# 2. Validate it (exit 0 = ALLOW, 1 = DENY)
$PY validator.py validate /etc/fyralis/license \
    --tenant-id acme --deployment-id acme-use1-7f3a
# ALLOW [allow]: license valid for tenant 'acme' deployment 'acme-use1-7f3a' ...

# 3. Revoke it before expiry (FR-F) — validate now DENYs
$PY revoke.py add --license-id lic-acme-3f9c1a2b --reason "key compromise"
$PY validator.py validate /etc/fyralis/license --tenant-id acme --deployment-id acme-use1-7f3a
# DENY [deny_revoked]: license revoked: matched license_id='lic-acme-3f9c1a2b' ...
```

Expiry can be set three ways (exactly one): `--duration-days`, `--duration-seconds`, or an
explicit `--expires-at 2027-06-24T00:00:00Z`. A **negative** duration mints an
already-expired license (used by the self-test to prove the expiry gate).

---

## Revocation & the revocation list (FR-F)

A signature is immortal — once signed, a license verifies forever. The **only** way to pull a
still-valid, still-signed license (tenant churn, plan downgrade, key compromise) is an
out-of-band **deny list** the validator consults on every `validate()`. That list is
`revocations.json` (path overridable via `--path` or the `REVOCATIONS_PATH` env var):

```json
{ "version": 1, "updated_at": "...", "revocations": [
  {"type": "license_id",    "value": "lic-acme-3f9c1a2b", "reason": "...", "revoked_at": "..."},
  {"type": "deployment_id", "value": "acme-use1-7f3a",    "reason": "...", "revoked_at": "..."},
  {"type": "tenant_id",     "value": "acme",              "reason": "...", "revoked_at": "..."}
]}
```

Revoke by **license_id** (precise), **deployment_id** (one deployment), or **tenant_id** (all
that tenant's licenses); a license is denied if it matches *any* entry. This file is the
revocation source of truth the validator reads — analogous to `ca/tenant_registry.json` for
certs — and is shipped to the agent the way config is. A revocation takes effect on the next
`validate()`; no re-issue needed.

```bash
$PY revoke.py add    --tenant-id acme --reason "churned"      # revoke everything for acme
$PY revoke.py remove --license-id lic-acme-3f9c1a2b           # un-revoke (mistake)
$PY revoke.py check  --license-id lic-acme-3f9c1a2b           # exit 0 = revoked
$PY revoke.py list
```

`from revoke import is_revoked` / `revocation_match` is what `validator.validate` imports.

> Fail-closed nuance: a **missing** revocation list means "nothing revoked" (empty); a
> present-but-**corrupt** list is *not* treated as empty — the validator denies
> (`deny_revocation_list_unreadable`) so a damaged file can never silently un-revoke.

---

## Key rotation

Rotation is handled entirely by `control-plane/signing` (the keyring + trust root) — this
component just rides it:

* A license is signed by the trust root's **active** key (`issue_license` calls
  `sign_bundle.sign_file`); pin a specific signer with `--key-id`.
* `validator.validate` verifies via `verify_bundle`, which resolves `manifest.key_id` in the
  trust root. An **unknown** key → DENY. A **retired** key → DENY for new applies by default
  (pass `allow_retired_key=True` only for a deliberate back-verify during rotation).
* Because retired public keys stay in the trust root, licenses signed before a rotation keep
  verifying until they expire — rotation is non-breaking. Rotate the signing key, re-issue
  going forward; old licenses age out naturally.

---

## Optional HTTP service

The CLI + validator are the core; `service.py` (FastAPI) is a thin control-plane-side
convenience for the console/onboarding flow. **The agent never calls it** — the agent
validates locally (outbound-only, I2). Endpoints: `POST /api/v1/licenses` (issue),
`POST /api/v1/licenses/validate`, `GET/POST/DELETE /api/v1/revocations`, `GET /healthz`.

```bash
$PY -m uvicorn service:app --port 8088        # or: docker compose ... license-service
```

`service.compose.yml` is the standalone fragment the integrate step merges into the master
`docker-compose.control-plane.yml` (build context is the control-plane **root** so the image
includes `signing/`; the private key is mounted read-only, never baked into the image).

---

## Self-test

```bash
$PY selftest.py        # exit 0 = all green
```

Proves end-to-end, through the **real** signing lib (against an isolated throwaway trust root
under a temp dir, so it never touches the repo's signing state):

1. issue a 1-day license for acme → `validate()` **ALLOW**
2. tamper a field in the signed `license.json` → **DENY** (`deny_bad_signature`)
3. issue an already-expired license → **DENY** (`deny_expired`)
4. revoke a still-valid license → **DENY** (`deny_revoked`)
5–7. negative-space: wrong tenant → DENY, wrong deployment → DENY, signed by an unknown key
   (different trust root) → DENY
8. `validate_for_deployment` binds identity from a `DeploymentRecord` → ALLOW

Current status: **11/11 checks pass.**

---

## Files

| File | Role |
|---|---|
| `license_model.py` | the LICENSE document contract + canonical bytes (no crypto) |
| `issue_license.py` | mint + sign a license bundle (delegates to `signing/sign_bundle`) |
| `validator.py` | `validate()` — fail-closed sig + expiry + identity + revocation |
| `revoke.py` | the revocation list (FR-F) the validator consults + the operator CLI |
| `service.py` | optional FastAPI issue/validate/revoke endpoint |
| `selftest.py` | the end-to-end proof (issue/tamper/expire/revoke) |
| `Dockerfile` / `service.compose.yml` | containerized service + integrate-step fragment |
| `revocations.json` | committed empty seed of the revocation list |

---

## Caveats / non-goals

* **Trust root is the root of trust.** Validation is only as strong as the trust root the
  agent ships; protecting `signing/keys/` (the private signing key, gitignored) is WS-SIGNING's
  job. Without a minted key, `issue_license` exits non-zero (it will not produce an unsigned
  "license").
* **Clock-dependent.** Expiry is wall-clock; a deployment with a badly wrong clock can
  mis-judge expiry. `skew_seconds` grants a small grace for clock drift; it does not defend
  against a deliberately back-dated clock (that is a host-integrity problem, out of scope).
* **Revocation is pull-based, not instantaneous.** The agent enforces revocation when it
  next validates against the list shipped to it; propagation latency = how often the agent
  refreshes the list. There is no online OCSP-style callback (by design — the agent is
  outbound-only and must keep working offline).
* **License binds identity, not authorization policy.** It carries `plan` + `features`; what
  those *enable* is enforced by the consuming subsystems, not here.
* **No license storage/DB.** Issuing writes a bundle to disk (or returns it over the optional
  API); persistence/distribution to the agent is the onboarding/config-dist path, not this
  component.
* The HTTP service is an operator convenience and assumes it sits **behind the auth-proxy** on
  `cp-net`; it does no auth of its own.
