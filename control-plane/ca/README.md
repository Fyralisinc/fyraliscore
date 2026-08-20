# `ca/` — Fyralis private CA + per-tenant mTLS identity (WS-CA / Phase P1)

This component is the **trust root** of the Fyralis BYOC control plane. It mints
a private CA hierarchy and issues a **per-tenant mTLS client certificate** for
each customer data plane. The cert carries the tenant's identity in a URI SAN;
the auth proxy (P2) terminates mTLS, verifies the cert chains to this CA, and
extracts the tenant id **server-side from the verified SAN** — never from a
caller-supplied header (Invariant **I4**).

There are two paths, both honoring the same contract:

| Path | What | Where |
|------|------|-------|
| **Python** (this dir) | deterministic, dependency-light CA for local dev, CI, and the proxy test-suite. **No external `step` binary required.** | `ca_lib.py` + CLIs |
| **step-ca** (prod) | production CA server (ACME/JWK provisioners, badger DB). | `config/ca.json` + `config/templates/tenant-leaf.tpl` |

Both stamp the **identical** leaf shape: clientAuth-only, no-CA, single URI SAN.

---

## The cert → tenant contract (C1)

Every tenant leaf carries exactly one URI Subject Alternative Name:

```
spiffe://fyralis/tenant/<tenant_id>
```

* `extract_tenant_from_cert(cert_pem)` reads `<tenant_id>` back out of that SAN.
  It is strict: exactly one SPIFFE URI under the `fyralis` trust domain must be
  present, or it raises.
* `fingerprint_sha256(cert)` is the **lowercase-hex SHA-256 of the cert's DER**.
  This is the key the revocation registry is keyed on and the value the proxy
  computes from the presented leaf. Issuance, registry, and the proxy all use
  this one function so the digest never drifts.

The revocation registry lives at `ca/tenant_registry.json` (the contract path):

```json
{
  "<fingerprint_sha256_hex>": {
    "tenant_id": "acme",
    "issued_at": "2026-06-24T00:00:00Z",
    "status": "active"
  }
}
```

`status` ∈ `active | revoked`. On every request the proxy computes the leaf
fingerprint, looks it up, and **rejects (403)** if the row is **missing** or
**revoked**, or if the registry `tenant_id` disagrees with the SAN-derived one.
`is_revoked(fp)` is **fail-closed**: an unknown fingerprint is treated as
revoked.

---

## What's here

| File | Role |
|------|------|
| `ca_lib.py` | Core library. `generate_root_ca()`, `generate_intermediate(root)`, `issue_tenant_cert(tenant_id, intermediate)`, `extract_tenant_from_cert(cert_pem)`, `fingerprint_sha256(cert)`, plus PEM (de)serialization and `chain_pem()`. |
| `verify_chain.py` | `verify_chain(leaf, chain)` → validates a leaf chains to the root (clientAuth + validity). Uses cryptography's native `ClientVerifier` when present; ships a real manual fallback otherwise. Used by the proxy and tests. |
| `registry.py` | Read/write surface for `tenant_registry.json` (atomic writes). `add_entry`, `set_status`, `get_entry`, `find_by_tenant`, `is_revoked`, `is_active`. |
| `bootstrap_ca.py` | CLI: create root + intermediate into `ca/pki/` (keys under `ca/pki/keys/`, gitignored). |
| `issue_cert.py` | CLI: `issue <tenant_id>` → writes the tenant cert+key+bundle and **adds a registry row**. |
| `revoke.py` | CLI: `revoke <fingerprint\|tenant_id>` → flips registry status to `revoked`. Re-exports `is_revoked`. |
| `config/ca.json` | step-ca **production** config (the prod path). |
| `config/templates/tenant-leaf.tpl` | step-ca x509 leaf template — mirrors `issue_tenant_cert()`. |
| `selftest.py` | Zero-dependency end-to-end smoke (bootstrap → issue acme → extract → verify → revoke + negatives). |
| `test_ca.py` | pytest suite (18 tests) — the P1 exit gate. |

### CA hierarchy

```
Fyralis Root CA  (self-signed, P-256, path_len=1, OFFLINE in prod)
   └── Fyralis Intermediate CA  (path_len=0, the ONLINE signer)
          └── tenant leaf  (clientAuth-only, no-CA, URI SAN spiffe://fyralis/tenant/<id>)
```

The **intermediate** signs leaves so the root key can stay offline. Verification
walks leaf → intermediate → root.

---

## Quickstart

```bash
# from control-plane/ca/  (use the project venv that has `cryptography`)
PY=/path/to/.venv/bin/python

# 1) create the CA (root + intermediate) under ./pki  (keys under ./pki/keys, gitignored)
$PY bootstrap_ca.py
#   add --force to rotate the trust anchor, --key-password env:CA_KEY_PASSWORD to encrypt keys

# 2) issue a tenant cert -> writes ./pki/tenants/acme/{acme.crt,acme.key,acme.bundle.crt}
#    and adds a row to ./tenant_registry.json
$PY issue_cert.py issue acme

# 3) inspect / revoke
$PY revoke.py list
$PY revoke.py revoke acme          # by tenant id (revokes all that tenant's certs)
$PY revoke.py revoke <fingerprint> # by exact fingerprint
$PY revoke.py status acme
```

### Use from the auth proxy (the authorization predicate)

```python
import ca_lib, verify_chain, registry

def authorize(leaf_pem: bytes, ca_chain_pem: bytes, registry_path: str) -> str:
    # 1) cert must chain to our CA (clientAuth, in-validity)
    if not verify_chain.verify_chain(leaf_pem, ca_chain_pem).ok:
        raise PermissionError("untrusted client cert")
    # 2) identity comes ONLY from the verified SAN (I4)
    tenant = ca_lib.extract_tenant_from_cert(leaf_pem)
    # 3) registry check, fail-closed
    fp = ca_lib.fingerprint_sha256(leaf_pem)
    if registry.is_revoked(fp, path=registry_path):
        raise PermissionError("revoked or unknown cert")
    if registry.get_entry(fp, path=registry_path)["tenant_id"] != tenant:
        raise PermissionError("SAN/registry tenant mismatch")
    return tenant  # -> inject as X-Scope-OrgID downstream (C5)
```

---

## Testing

```bash
# zero-dependency smoke (leaves no artifacts; runs in a temp dir)
$PY selftest.py

# full pytest suite (P1 exit gate)
$PY -m pytest test_ca.py -q
```

`selftest.py` proves the P1 exit gate: it bootstraps a CA, issues a cert for
tenant `acme`, asserts `extract_tenant_from_cert(...) == "acme"`, asserts the
chain verifies, round-trips the registry (including a `revoked` entry), and runs
negative checks (a foreign CA's cert does **not** verify; an unknown fingerprint
is fail-closed).

---

## Production path (step-ca)

`config/ca.json` is the production CA config. Differences vs. the Python path:

* step-ca runs as a CA **server** (`:9000`) with ACME + JWK provisioners; the
  onboarding service (P4) drives issuance, mapping the enrollment to the tenant
  id that the leaf template stamps into the URI SAN.
* an x509 **policy** hard-restricts leaves to `spiffe://fyralis/tenant/*` URIs
  with no DNS/IP/email SANs, so the proxy's SAN parse stays unambiguous.
* step-ca keeps its own serial DB; we still mirror cert → tenant into
  `tenant_registry.json` because the proxy authorizes on **fingerprint + SAN**,
  which is the portable contract both paths share.

Either way the leaf shape (clientAuth-only, no-CA, single SPIFFE URI SAN) and the
registry contract are identical, so the proxy and agent don't care which path
issued a cert.

---

## Caveats / non-goals

* **No CRL / OCSP.** Revocation is a **registry-lookup** at the proxy
  (`tenant_registry.json`), not a CRL distribution point or OCSP responder. This
  is deliberate: the control plane already sees every request at the proxy, so a
  central registry flip is simpler and effective. Leaves are short-lived (90d
  default) so even without revocation propagation the blast radius is bounded.
* **Single registry file.** `tenant_registry.json` is a JSON file with atomic
  writes; fine for the control-plane scale (one row per cert). If the fleet
  grows past comfortable file size, back the same `registry.py` API with a DB —
  the contract (fingerprint → `{tenant_id, issued_at, status}`) is unchanged.
* **Key storage.** CA private keys and tenant keys are written under
  `pki/keys/` and `pki/tenants/.../<id>.key`, mode `0600`, and gitignored. They
  are **unencrypted by default** for local dev; pass `--key-password env:VAR` to
  encrypt. In prod the intermediate key lives in step-ca's secret store and the
  root key is offline.
* **EC P-256** is used throughout (small certs/keys, fast handshakes). RSA is not
  issued by this path, though `verify_chain.py` can verify RSA/ed25519 issuers if
  a future hierarchy uses them.
* **Tenant id charset** is restricted to `[A-Za-z0-9._-]` (no slashes/spaces) so
  the SPIFFE URI round-trips exactly and cannot be made ambiguous.
