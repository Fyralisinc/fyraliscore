# onboarding/ — atomic per-tenant onboarding (WS-ONBOARD, FR-E)

The **all-or-nothing tenant enrollment transaction** for the Fyralis BYOC control
plane. One command takes a tenant from nothing to a deployable *agent bundle* —
issuing its mTLS identity, minting its license, wiring it into the auth-proxy and
the console — and, on **any** failure, rolls every side effect back so no
half-onboarded state remains (**FR-E**).

```
onboard  --tenant acme --region us-east --plan standard
offboard --tenant acme --deployment acme-use1-7f3a
```

This area only *reuses* the committed P1–P3 primitives; it never re-implements
crypto or identity:

| Primitive | Used for | Module |
|-----------|----------|--------|
| `ca/issue_cert` + `ca/registry` | issue the tenant mTLS cert, write the active registry row (the **proxy binding**) | `ca/` |
| `ca/revoke` + `ca/registry` | revoke + delete the row on rollback / offboard | `ca/` |
| `signing/sign_bundle` + `verify_bundle` | ed25519-sign the license + agent config; verify-before-use (C2/I6) | `signing/` |
| `lib/deployment` (`DeploymentRecord`) | the C4 fleet record pushed to the console | `lib/` |
| `lib/primitives`, `lib/tiers`, `lib/errors` | RFC-3339 time, telemetry tiers, typed errors | `lib/` |

---

## The flow (6 steps)

`onboard --tenant acme --region us-east --plan standard` runs, in order, with each
successful step registering an **undo** on a LIFO rollback ledger:

1. **REGISTER** — `POST /api/v1/register {tenant_id?, region, plan}` to the console
   to mint a `deployment_id` (and `tenant_id` if not supplied). With `--local-ids`
   the ids are minted locally and no console is contacted. *(undo: none — no side
   effect on the CP yet.)*
2. **ISSUE CERT** — `ca/issue_cert.issue(tenant)` mints a per-tenant mTLS client
   cert whose URI SAN is `spiffe://fyralis/tenant/acme`, and **adds an `active` row
   to `ca/tenant_registry.json`** keyed by the leaf's SHA-256 fingerprint. The
   auth-proxy reads that registry on every request, so *issuing the cert is the
   proxy binding* — the proxy now accepts this cert and rejects everything else
   (fail-closed). *(undo: revoke the cert **and** delete its registry row **and**
   remove the partial bundle dir.)*
3. **MINT + SIGN LICENSE** — build the P4 license JSON
   `{tenant_id, deployment_id, plan, issued_at, expires_at, features[]}` and
   ed25519-sign it via `signing/` (detached `.sig` + `.manifest.json`). The agent
   **verifies before use** (I6) and refuses to operate once expired. Minting is
   delegated to the canonical `control-plane/licensing` minter when that area is
   importable, and falls back to an equivalent self-contained signer otherwise —
   both produce the identical signed-license trio (onboarding sets the plan→features
   entitlements either way). *(undo: covered by removing the bundle dir.)*
4. **ASSEMBLE THE AGENT BUNDLE** — write the deployable directory the customer
   ships into their VPC (see *Bundle layout* below), including a **signed agent
   config** that points the outbound-only agent (I2) at the console, plus the
   **public trust root** the agent verifies the license/config against. *(undo:
   delete the bundle dir.)*
5. **SEED HEARTBEAT** — push an initial `DeploymentRecord` (C4) via
   `POST /api/v1/heartbeat` so the deployment shows up in the console immediately;
   the agent re-heartbeats from the data plane thereafter. *(undo: best-effort
   remove the deployment from the console.)*
6. **CONFIRM** — `GET /api/v1/deployments/{deployment_id}`; assert the console
   lists it. A missing deployment fails the transaction (and rolls back).

On success the operator gets a bundle dir + an `OnboardResult` (`--json` prints
it). On failure the ledger unwinds newest-first and the command exits non-zero.

### Bundle layout

```
bundles/<deployment_id>/
  cert/
    <tenant>.crt           # tenant leaf cert (SAN spiffe://fyralis/tenant/<tenant>)
    <tenant>.key           # tenant PRIVATE KEY (0600; stays in the customer VPC)
    <tenant>.bundle.crt    # leaf + intermediate (+root) chain for the mTLS handshake
  <tenant>.license.json            # signed license (verify-before-use)
  <tenant>.license.json.sig        #   detached ed25519 signature (base64)
  <tenant>.license.json.manifest.json
  agent-config.json                # signed config: console_url, mtls paths, tier, I2/I3/I6 flags
  agent-config.json.sig
  agent-config.json.manifest.json
  trust_root.json          # PUBLIC keys the agent verifies license + config with
  BUNDLE.json              # human/automation manifest of everything above
```

> The bundle contains a private key — it is git-ignored (`bundles/`) and must be
> delivered to the customer over a secure channel, not committed or logged.

---

## Atomicity & rollback (FR-E)

Onboarding is a transaction implemented with an in-process **rollback ledger**
(`RollbackLedger`): each successful side effect pushes a `(label, undo)` pair, and
any exception triggers `roll_back()`, which runs the undos **newest-first**. Key
properties:

- **Revoke-then-delete.** The cert undo *revokes first* (so even if the row-delete
  failed, the proxy already 403s the cert — fail-closed) and *then deletes* the
  registry row, leaving the registry exactly as it was before onboarding.
- **No orphan bundle.** Issuing the cert is what first creates the bundle dir
  (it writes `cert/`). That same first undo also `rmtree`s the bundle dir, so a
  failure **anywhere** after the cert step leaves no partial bundle on disk.
- **Best-effort, non-masking.** A failing undo is logged and the remaining undos
  still run; the *original* failure is the error that propagates (an injected or
  genuine step error is never hidden behind a rollback error).
- **Console deregister.** If a console was used, a late failure also removes the
  seeded deployment. Against the in-process/embedded console this is a real delete;
  against a *real* console there is **no DELETE verb** in the P4 contract, so the
  record is left to age to `red`/expired and be reaped by the console operator
  (see *Caveats*).

`offboard.py` is the inverse for an **already-committed** onboarding: it **revokes**
every active cert for the tenant (proxy binding severed), optionally `--purge-registry`
(delete the rows for no trace), best-effort deregisters from the console, and
optionally `--purge-bundle` removes the local bundle.

---

## Running it

All commands run from `control-plane/`. The CA and signing material must be
bootstrapped first (P1):

```bash
python ca/bootstrap_ca.py                       # root + intermediate CA -> ca/pki/
python signing/keygen.py --activate             # ed25519 trust root -> signing/
```

### Against a real console

```bash
python onboarding/onboard.py \
  --tenant acme --region us-east --plan standard \
  --console-url http://console:8080 --json
```

### Local ids (no console)

```bash
python onboarding/onboard.py --tenant acme --region us-east --plan standard --local-ids
```

### Embedded console (dev/demo, no server process)

```bash
python onboarding/onboard.py --tenant acme --region us-east --plan standard --embedded-console
```

### Offboard

```bash
python onboarding/offboard.py --tenant acme --deployment acme-use1-7f3a \
  --console-url http://console:8080 --purge-bundle
```

### Via Docker (the `ops`-profile one-shot)

`service.compose.yml` defines an on-demand `onboarding` container (gated behind the
`ops` profile, so a plain `up` never starts it). At integrate time it merges into
`docker-compose.control-plane.yml`:

```bash
docker compose -f docker-compose.control-plane.yml \
  run --rm onboarding onboard --tenant acme --region us-east --plan standard
```

It mounts `ca/tenant_registry.json` **read-write** (the registry the auth-proxy
reads **read-only**), `ca/pki` and `signing/` read-only, and `onboarding/bundles`
read-write.

### Plans → features

| plan | features |
|------|----------|
| `trial` | metrics |
| `standard` | metrics, logs, fleet-dashboards |
| `enterprise` | metrics, logs, traces, fleet-dashboards, sso, audit-export |

(Plan features are coarse product entitlements in the license; **telemetry tiers**
`T1/T2/T3` independently gate what may egress the VPC — see `lib/tiers.py`.)

---

## Self-test

```bash
python onboarding/selftest.py
```

Bootstraps a **throwaway** CA hierarchy + ed25519 trust root + tenant registry in
a temp dir (never touching the committed `ca/pki`, `signing/`, or
`ca/tenant_registry.json`) and runs against an in-process console honoring the P4
REST contract. It asserts:

- **Happy path** — a bundle is produced with a valid cert (`SAN == acme`) and a
  valid (signature-verifies + unexpired) license; the registry has an *active*
  `acme` row keyed by the issued fingerprint; and the console lists the deployment.
- **Rollback (late failure)** — a failure after the heartbeat step rolls back the
  cert/registry row, the bundle, and the console deployment.
- **Rollback (early failure)** — a failure right after the cert step leaves **no
  orphan bundle dir** (regression guard).
- **Rollback (genuine failure)** — with signing material unavailable, the *real*
  license-signing failure rolls back the cert/registry row.
- **Offboard** — revokes the cert (row → `revoked`), deregisters from the console,
  and purges the bundle.

Exit 0 with all assertions green; non-zero otherwise.

---

## Caveats

- **No DELETE on a real console.** The P4 REST contract is upsert-only
  (`register` + `heartbeat`), so onboarding cannot *delete* a deployment from a
  *real* console during rollback/offboard — the record is left to age to
  `red`/expired and be reaped by the console operator. The embedded/in-process
  console used by the self-test *does* support removal, so rollback there is exact.
  If the console later adds a DELETE/decommission verb, wire it into
  `_best_effort_console_remove` / `offboard`.
- **Revocation = registry status, not CRL/OCSP.** Like `ca/revoke`, the proxy
  binding is the `tenant_registry.json` lookup; there is no CRL/OCSP. Offboarding
  flips the row to `revoked` (and `--purge-registry` deletes it).
- **License vs. tier.** The license carries product *features*; it does **not**
  enforce telemetry egress. The customer-configured `telemetry_tier` (default T1,
  zero PII per I1) is enforced at the **boundary** collector (`boundary/`), not here.
- **Registry write contention.** `ca/registry` writes atomically (temp file +
  `os.replace`), but it is not multi-writer-locked. Run one onboarding/offboarding
  against a given registry at a time (the operator CLI is inherently serial).
- **Signing key custody.** `onboard` needs the signing **private** key to mint the
  license/config; run it on a host that has `signing/keys/`. The agent only ever
  needs the **public** `trust_root.json` shipped in the bundle.
- **Base image CVEs.** The `Dockerfile` uses `python:3.12-slim`; its base layer may
  carry upstream CVEs (flagged by the image scanner). Pin a patched digest / rebuild
  on the org's hardened base at integrate/release time.
- **`fail_after` is test-only.** The `--fail-after` flag injects a deliberate
  failure to exercise rollback; never pass it in production.
