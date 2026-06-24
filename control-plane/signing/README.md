# `signing/` — ed25519 supply-chain signing (Contract C2 / Invariant I6)

This is the **supply-chain trust root** for the Fyralis BYOC control plane. It implements
[Shared Contract **C2**](../SPRINT_PLAN.md#c2--signing-i6-ed25519-sign--verify-everything-shipped)
and [Invariant **I6**](../SPRINT_PLAN.md#invariants-must-hold-across-all-phases):

> **Everything the control plane ships to a data plane is signed** — release tarballs,
> license JSON, and config JSON — using **ed25519** with a **detached signature** plus a
> **manifest**. Agents ship the public keyring and **verify before apply**: an artifact whose
> signature fails, or whose `key_id` is unknown/retired, is **never applied**.

Pure Python, depends only on [`cryptography`](https://cryptography.io) (ed25519). Self-contained
(no import of the sibling `lib/`), so it can be tested in isolation.

---

## What is signed, and how

| Artifact kind | Example file | Canonical signed bytes |
|---------------|--------------|------------------------|
| `release`     | `release-1.4.2.tar.gz` | the **exact file bytes** (the tarball) |
| `license`     | `license-acme.json`    | **compact-canonical UTF-8 JSON** (sorted keys, no whitespace) |
| `config`      | `agent-config.json`    | **compact-canonical UTF-8 JSON** (sorted keys, no whitespace) |

For JSON artifacts we canonicalize first, so a verifier accepts a license/config that was merely
**re-pretty-printed or had its keys re-ordered** but rejects any change to the actual content.
(Tarballs are canonicalized byte-for-byte.) The `sha256` in the manifest is the digest of those
canonical bytes.

### Manifest binding — what the signature actually covers (I6, `sig_binding: v2`)

The ed25519 signature does **not** cover the raw canonical artifact bytes directly. Doing so left a
**relabel gap**: because the manifest's `version` / `artifact` / `key_id` were *outside* the signed
quantity, a signed bundle could be **relabeled** — e.g. a release manifest swapped to claim a
different version or artifact-kind — and still verify, because the artifact bytes (and therefore the
old signature) were unchanged.

To close this, the signature now covers a **canonical binding** of the manifest identity:

```
signed_payload = canonical_json({
  "binding": "v2",
  "algo": "ed25519",
  "artifact": <kind>,            # release | license | config
  "version": <version>,
  "key_id": <key_id>,
  "artifact_sha256": sha256(canonical artifact bytes)   # binds the content, indirectly
})
```

(see `signing_lib.signing_payload` / `signed_payload_for`). A verifier recomputes this binding from
**(artifact + manifest)** and ed25519-verifies it, so:

* tampering the **artifact** changes `artifact_sha256` → reject;
* relabeling the **version**, **artifact-kind**, or **key_id** changes the binding → reject;
* re-pretty-printing / key-reordering a JSON artifact is still accepted (canonicalized).

`sha256` in the manifest remains a redundant cross-check (and is the value fed into the binding).

**Schema version & back-compat.** Manifests now carry `"sig_binding": "v2"`. A manifest **without**
`sig_binding` is treated as **legacy v1** (signature over raw canonical bytes — relabel-vulnerable)
and is still accepted by `verify_file` for backward compatibility; pass `allow_legacy_v1=False`
(CLI `--require-binding`) to reject it and require the bound v2 form. A legacy v1 bundle cannot be
"upgraded" by stamping a `v2` marker onto its manifest — its signature won't match the v2 payload.

For each signed `<file>` we emit two siblings:

```
<file>.sig             # base64 of the raw 64-byte ed25519 signature (detached, over the v2 binding)
<file>.manifest.json   # { artifact, version, sha256, key_id, algo:"ed25519", sig_binding:"v2", signed_at }
```

Manifest shape (C2):

```json
{
  "artifact": "config",
  "version": "7",
  "sha256": "267070fb…",
  "key_id": "cp-signing-2026-06",
  "algo": "ed25519",
  "sig_binding": "v2",
  "signed_at": "2026-06-24T11:38:42Z"
}
```

---

## Files

| File | Role |
|------|------|
| `signing_lib.py`  | Library: ed25519 `generate_keypair` / `sign` / `verify`; key (de)serialization; the **`Keyring`** abstraction (multiple keys by `key_id`, one **active** signer + retained **retired** verifiers, rotation); manifest + canonical-bytes helpers. |
| `keygen.py`       | CLI: generate a signing keypair into `keys/` (private **gitignored**) and emit the **public** trust root to `trust_root.json` (`key_id → pubkey`). |
| `sign_bundle.py`  | CLI: `sign <file>` → writes `<file>.sig` + `<file>.manifest.json`. Works for release tarballs, license JSON, config JSON. |
| `verify_bundle.py`| CLI: `verify <file>` → checks the signature against `trust_root.json`; **non-zero exit + clear message** on tamper/unknown-key. **This is the function agents call before apply.** Also importable: `verify_file(path) -> VerifyResult`. |
| `rotation.py`     | Demonstrates rotating to a new `key_id` while old signatures still verify against retained pubkeys. |
| `selftest.py`     | End-to-end functional self-test (keygen → sign → verify OK → tamper → FAIL → rotate → old+new verify). Runnable directly or under `pytest`. |

---

## Key custody layout

```
signing/
├── keys/                                  # GITIGNORED (repo rule **/keys/) — NEVER commit
│   ├── cp-signing-2026-06.private.pem      #   PKCS#8 PEM, mode 0600 — the private signing key
│   └── cp-signing-2026-06.public.b64       #   convenience public-key sidecar (also in trust root)
└── trust_root.json                        # PUBLIC keyring — committed/shipped to agents
```

- **Private keys live only in `keys/`** and only on the control-plane signer host. The repo
  `.gitignore` (`**/keys/`, `**/*.key`) keeps them out of git. `keygen.py` writes them `0600`.
- **`trust_root.json` is the public keyring** (`{key_id: {pubkey, algo, status}}` + `active_key_id`).
  It contains **no private material** and is the artifact agents ship to verify-before-apply.
  It is **not** committed by this build step (an operational artifact); `keygen.py` regenerates it.

---

## Quick start

```bash
PY=python   # any interpreter with `cryptography` installed

# 1. Mint the active signing key + public trust root.
$PY keygen.py --key-id cp-signing-2026-06 --activate

# 2. Sign artifacts (kind inferred from filename, or pass --kind).
$PY sign_bundle.py sign release-1.4.2.tar.gz --version 1.4.2
$PY sign_bundle.py sign license-acme.json   --kind license --version 2027-06-24
$PY sign_bundle.py sign agent-config.json   --kind config  --version 7

# 3. Verify-before-apply (exit 0 = apply; non-zero = REFUSE).
$PY verify_bundle.py verify agent-config.json && echo APPLY || echo REFUSE

# 4. Prove rotation (old signatures still verify under the retained key).
$PY rotation.py

# 5. Full functional self-test.
$PY selftest.py            # or: pytest selftest.py
```

### How an agent uses this (verify-before-apply, I6)

```python
from verify_bundle import verify_file
res = verify_file("agent-config.json")          # checks bound sig (v2) + key_id policy + sha256
if not res.ok:
    audit_log.record("rejected", reason=res.reason)   # NEVER apply
    raise RuntimeError(res.reason)
apply_config("agent-config.json")               # only reached on a verified artifact
```

---

## Rotation

`key_id` rotation is **non-breaking**: rotating to a new key retires the previous key but
**retains its public key** in the trust root, so artifacts signed before the rotation still
verify. Add the next key with `keygen.py`:

```bash
$PY keygen.py --key-id cp-signing-2026-09 --activate
# trust_root.json now has cp-signing-2026-06 (retired) + cp-signing-2026-09 (active)
```

**Apply policy vs. cryptographic validity** — `verify_bundle.py` distinguishes the two:

- An artifact signed by the **active** key → `VERIFY OK`, apply.
- An artifact signed by a **retired** key → **rejected by default** (`VERIFY FAILED: … RETIRED`),
  because new applies should use the current key. Pass `--allow-retired` to **back-verify** (the
  signature is still cryptographically valid against the retained pubkey) — used by tooling that
  audits historical artifacts.
- An artifact whose `key_id` is **unknown** (not in the trust root at all) → always rejected.

`rotation.py` and `selftest.py` prove all three branches.

---

## Caveats (read before production)

- **Keys-on-disk is for local/dev/single-host only.** `keys/*.private.pem` is an unencrypted
  PKCS#8 file protected by filesystem perms (`0600`) and `.gitignore`. **The production custody
  path is a KMS/HSM** (e.g. AWS KMS, GCP KMS, or a PKCS#11 HSM): the private key never leaves the
  HSM, signing is an API call, and `key_id` maps to a KMS key ARN. `signing_lib` is structured so
  the `Keyring` could hold a "remote signer" entry (sign via KMS) without changing the manifest or
  `verify_bundle` — only `sign_bundle`'s key loading would swap to a KMS client. **Do not ship
  on-disk private keys to a real signer host.**
- **Trust-root distribution is itself a trust decision.** Agents must obtain `trust_root.json`
  over a trusted channel (baked into the signed installer image / pinned at enrollment), otherwise
  an attacker who can swap the trust root defeats verification. In this design the agent ships with
  the public keys; rotation adds keys to a trust root the agent already trusts.
- **No on-chain transparency / no revocation list for signatures here.** We support key
  *retirement* (a retired key no longer signs new artifacts, and the default apply-policy rejects
  its artifacts), but there is no per-artifact revocation. If a signing key is **compromised**,
  the response is: rotate the active key, and **remove** the compromised `key_id` from the trust
  root entirely (so its signatures stop verifying) rather than merely retiring it.
- **ed25519 only.** `algo` is pinned to `ed25519`; a manifest with any other `algo` is rejected.
  This is deliberate (one strong, modern, deterministic scheme) — no algorithm-agility downgrade
  surface.
- **`signed_at` is informational.** It is recorded in the manifest but not enforced (no expiry on
  signatures). Time-bounded validity for **licenses** is handled by the licensing layer's
  `license_expiry` (C4), not by the signature.
