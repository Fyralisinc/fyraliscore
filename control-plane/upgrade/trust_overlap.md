# CA Trust-Chain Overlap — non-disruptive CA rotation / revocation (FR-A5)

This document explains the **trust-overlap** technique the control-plane upgrade
uses so that **rotating or replacing the issuing CA never breaks an in-flight
agent's mTLS**. It is the detail behind §4 of [`UPGRADE_RUNBOOK.md`](./UPGRADE_RUNBOOK.md).

## The problem

Every customer data plane authenticates to the control plane with a **per-tenant
mTLS client certificate** that chains to the **Fyralis CA** (C1). The auth-proxy
(`auth-proxy/proxy.py`) terminates that mTLS and **requires** a client cert that
verifies against its trust bundle:

```python
ctx.load_verify_locations(cafile=AUTH_PROXY_CA_CHAIN)   # ca/pki/ca-chain.crt
ctx.verify_mode = ssl.CERT_REQUIRED
```

If we **rotate the CA** — issue future certs from a new CA, or revoke + replace a
compromised old one — there is a transition window where **some agents still hold
old-CA certs and some hold new-CA certs**. If at any instant the proxy trusts only
*one* of those CAs, every agent on the *other* CA is locked out: its TLS handshake
fails before HTTP, the heartbeat/telemetry push fails, and (only because of I3) the
data plane keeps working but goes blind to the control plane. That is the
disruption FR-A5 forbids.

## The fix: trust BOTH CAs during the cutover

`ssl.load_verify_locations` (and the sibling `ca/verify_chain.py`, which the
proxy's `TenantResolver` uses to re-verify) accept a **concatenated PEM bundle
with multiple trust anchors**. OpenSSL — and the `cryptography`
`x509.verification` path in `verify_chain.py` — build a chain from the presented
leaf to **any** root in the bundle. So a bundle that contains

```
old_intermediate
old_root
new_intermediate
new_root
```

trusts leaves from **both** CAs **simultaneously**. That overlapping window is the
whole technique:

```
            bundle = {old}              bundle = {old, new}            bundle = {new}
            ───────────────  add new   ──────────────────  remove old ───────────────
old-CA leaf:  TRUSTED          ───►        TRUSTED            ───►        rejected
new-CA leaf:  rejected         ───►        TRUSTED            ───►        TRUSTED

                              ▲ overlap window: BOTH trusted ▲
                              every agent keeps working here, whichever CA it is on
```

While the overlap window is open, agents rotate to new-CA certs **at their own
pace**; none is ever locked out, because whichever CA signed its current cert is
trusted.

## The ordering (this is the part you must not get wrong)

```
1. ADD    the new CA to the bundle      => bundle = {old, new}
2. RELOAD the auth-proxy (rolling)      => the new trust is live, BOTH CAs trusted
3. ISSUE  new-CA leaves; rotate agents  => each at its own pace, none locked out
4. REMOVE the old CA from the bundle    => bundle = {new}, only AFTER all rotated
5. RELOAD the auth-proxy again          => old CA fully retired
```

Two rules make or break it:

- **ADD-before-ISSUE.** You must `add` the new CA **and reload the proxy** *before*
  you issue/rotate any agent to a new-CA cert. Issue-first means an agent could
  present a new-CA leaf to a proxy that does not yet trust it → 403.
- **REMOVE-after-ROTATE.** You must `remove` the old CA **only after** every active
  agent presents a new-CA leaf. Remove-first locks out every agent still on the old
  CA. Use `console:8080/api/v1/deployments` (and your issuance records) to confirm
  the fleet has fully rotated before removing.

Doing it in the wrong order (remove-then-add, or issue-before-add) is *exactly*
the disruption this tooling exists to prevent. The helper enforces the one
backstop it safely can: `remove` **refuses to leave the bundle empty** (it never
lets you strand the proxy with zero trust anchors).

## What about revoking a single tenant (not the whole CA)?

That does **not** touch the trust bundle at all. Per-tenant revocation flips the
cert's SHA-256 fingerprint to `status: "revoked"` in `ca/tenant_registry.json`.
The proxy re-reads the registry **fresh on every request**
(`registry_fresh_every_request=True`), so a revocation takes effect **immediately**
and is inherently non-disruptive to *every other* tenant. The trust-overlap dance
is only for rotating the **CA itself** (the signing authority), which is the rarer,
heavier event.

## The helper

`trust_bundle.py` performs the bundle surgery; `trust_overlap.sh` is the operator
front-end that also reloads the proxy at the right moment. Both **reuse the
committed siblings** — `ca/verify_chain.py` for verification and `signing/` for
signing the bundle (I6) — and never re-implement crypto.

### `trust_bundle.py` commands

```bash
# inspect every trust anchor in the bundle
python upgrade/trust_bundle.py list ca/pki/ca-chain.crt

# ADD a new CA (idempotent) and re-sign the bundle (I6)
python upgrade/trust_bundle.py add ca/pki/ca-chain.crt \
    --add-ca ca/pki-new/ca-chain.crt --sign

# PROVE the overlap: an existing (old-CA) agent leaf still verifies
python upgrade/trust_bundle.py verify ca/pki/ca-chain.crt \
    --leaf /path/to/existing-agent-leaf.crt

# REMOVE a retired CA by its root subject CN, re-sign
python upgrade/trust_bundle.py remove ca/pki/ca-chain.crt \
    --match-root-cn "Fyralis Root CA" --sign

# verify the bundle's own ed25519 signature before the proxy loads it (I6)
python upgrade/trust_bundle.py verify ca/pki/ca-chain.crt --require-signature
```

Key properties:

- **`add` is idempotent** — re-adding a CA already in the bundle is a no-op (it
  matches by SHA-256 fingerprint), so re-running the upgrade is safe.
- **`add` / `remove` are atomic** — temp-file + `os.replace`, and each write leaves
  a timestamped `.bak` for one-`mv` rollback.
- **`remove` drops a whole CA together** — the matched root *and* the
  intermediate(s) it issued leave the trust set as a unit.
- **`verify --leaf`** uses the *same* `ca/verify_chain.py` the proxy resolver uses,
  so a green `verify` is a real guarantee the proxy will accept that leaf, not a
  toy check.
- **`--sign` / `--require-signature`** integrate the control-plane keyring: the
  trust bundle is itself a signed artifact, so the upgrade never loads a bundle it
  did not produce + sign (I6).

### `trust_overlap.sh` (operator front-end)

```bash
# step 1+2: add new CA, sign, and roll JUST the auth-proxy so it loads the bundle
./upgrade/trust_overlap.sh add --new-ca ca/pki-new/ca-chain.crt

# prove the overlap held for an un-rotated agent
./upgrade/trust_overlap.sh verify --leaf /tmp/acme-leaf.crt

# step 4+5: retire the old CA (only after the fleet has rotated) and roll again
./upgrade/trust_overlap.sh remove --root-cn "Fyralis Root CA"
```

The `add`/`remove` subcommands auto-invoke `rolling_upgrade.sh` for **just the
`auth-proxy`** at the right moment (pass `--no-reload` to defer the reload to your
own change window). That reload is itself a **single, health-gated rolling restart
of one stateless service** — so loading the new trust bundle is zero-disruption by
the same mechanism as the rest of the upgrade.

## Self-test

`selftest.py` proves the whole lifecycle with two real, independently-minted CA
hierarchies and the committed verifier:

- old-only bundle → verifies the old leaf, **rejects** the new leaf;
- after `add` → the overlap bundle **verifies both**;
- `add` is idempotent;
- after `remove` → verifies the new leaf, **rejects** the old leaf;
- `remove` **refuses** to empty the bundle;
- the bundle signs + verifies (and a tampered bundle fails) via `signing/`.

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python upgrade/selftest.py
```
