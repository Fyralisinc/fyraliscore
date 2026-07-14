# `control-plane/upgrade/` — zero-disruption CP upgrade/migration (WS-CP-UPGRADE, NFR-6)

The procedure + tooling to **upgrade or migrate the Fyralis BYOC control plane
without disrupting the fleet**: roll the stateless services one-at-a-time, migrate
the stateful stores (Mimir/Loki) on shared object storage without dropping
remote-write, and rotate the CA with **trust-chain overlap** so in-flight agent
mTLS never breaks (FR-A5). It leans on **I3** — the agent buffers/retries, so a
brief control-plane gap is invisible to the customer's data plane.

> Built per the WS-CP-UPGRADE rules: everything lives **only** in this directory;
> it **imports** the committed siblings (`ca/`, `signing/`, the master compose) and
> edits none of them; all signing/verify reuses `control-plane/signing/` (I6).

## Files

| File | What it is |
|------|-----------|
| **`UPGRADE_RUNBOOK.md`** | The procedure. Stateless rolling, stateful blue-green/rolling, trust-overlap, the I3 buffering guarantee, verification + rollback. **Start here.** |
| **`trust_overlap.md`** | Deep dive on CA trust-overlap + the add-before-rotate / remove-after ordering. |
| **`trust_bundle.py`** | Helper to `add` / `remove` / `list` / `verify` / `sign` a CA trust bundle. Reuses `ca/verify_chain.py` (the proxy's own verifier) + `signing/`. |
| **`trust_overlap.sh`** | Operator front-end for the overlap dance; auto-reloads just the auth-proxy. |
| **`rolling_upgrade.sh`** | Health-gated, one-at-a-time rolling restart of the **stateless** CP services (`auth-proxy` → `config-dist` → `console`). Refuses stateful services. |
| **`service.compose.yml`** | On-demand `cp-upgrade-tools` operator container (compose overlay; does not edit the master compose). |
| **`selftest.py`** | Proves trust-overlap end-to-end + that docs/scripts/YAML hold. |

## Quick start

```bash
cd control-plane

# Stateless rolling upgrade (dry-run first, then real), health-gated:
DRY_RUN=1 ./upgrade/rolling_upgrade.sh
./upgrade/rolling_upgrade.sh

# CA rotation with trust overlap (FR-A5) — add the new CA BEFORE rotating agents:
./upgrade/trust_overlap.sh add    --new-ca ca/pki-new/ca-chain.crt
./upgrade/trust_overlap.sh verify --leaf  /path/to/existing-agent-leaf.crt
# ... rotate agents at their own pace; both CAs are trusted ...
./upgrade/trust_overlap.sh remove --root-cn "Fyralis Root CA"   # after all rotated

# Stateful Mimir/Loki upgrade -> follow UPGRADE_RUNBOOK.md §3 (blue-green / rolling
# on shared object storage, with the remote-write cut-over ordering).
```

## Self-test

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python upgrade/selftest.py
```

It asserts:

- **trust-overlap** with two real CAs (old-only rejects new; overlap trusts both;
  idempotent add; post-retire rejects old; refuse-empty; sign+verify, tamper fails)
  — using the committed `ca/verify_chain.py` verifier;
- **doc coverage** — the runbook covers stateless-rolling + stateful-migration
  (blue-green + shared object storage + remote-write ordering) + trust-overlap +
  the I3 buffering guarantee + health-gating; `trust_overlap.md` covers the
  ordering;
- **scripts** pass `bash -n` (and `shellcheck` if installed);
- **YAML/compose** — `service.compose.yml` is valid YAML and `docker compose
  config` parses it (when docker is present).

## How it maps to the requirements

- **NFR-6 (zero-disruption upgrade/migration)** — the runbook + the two scripts.
- **FR-A5 (non-disruptive CA rotation/revocation)** — `trust_bundle.py` /
  `trust_overlap.sh` + `trust_overlap.md`.
- **I3 (data plane survives CP outage)** — the buffering guarantee documented in
  the runbook is *why* each rolling/blue-green gap is invisible to the fleet; it
  rides on the committed `agent/buffer.py`.
- **I6 (sign + verify everything)** — the trust bundle is itself signed/verified
  via `signing/`; releases/configs are signed before deploy and verified by the
  agent before apply.

## Caveats

- **`shellcheck` is not installed in this environment**, so the script self-test
  uses `bash -n` (syntax check). `shellcheck` is run automatically *if present*;
  installing it (`apt-get install shellcheck`) upgrades the lint. The scripts are
  written `set -euo pipefail` and pass `bash -n`.
- **`rolling_upgrade.sh` drives `docker compose` against a running stack.** It is a
  real rolling-restart tool, exercised here in `DRY_RUN` and `bash -n`; a live
  end-to-end roll needs the full control-plane stack up
  (`docker compose -f docker-compose.control-plane.yml up`). The health-gating,
  state inspection, stateful-denylist, and rollback logic are all real code paths.
- **`config-dist` is not yet wired into the master compose** (its dir is a
  scaffold). `rolling_upgrade.sh` handles this gracefully: a service not defined in
  the compose file is **skipped with a warning**, not an error — so the script is
  correct today and picks up `config-dist` automatically once it lands.
- **Blue-green for Mimir/Loki assumes shared object storage** (S3/GCS) in
  production. The dev single-host stack uses local named volumes
  (`mimir-data`/`loki-data`); on that backend prefer the **rolling** stateful
  variant (runbook §3.3) — true two-writer blue-green needs the shared bucket.
- **`trust_bundle.py --sign` needs the CP keyring** (`signing/trust_root.json` +
  the private key under `signing/keys/<key_id>.key`). If you have not run
  `signing/keygen.py`, run the overlap steps without `--sign` (or via
  `SIGN=0 trust_overlap.sh ...`) and sign later. The self-test signs with a
  throwaway in-memory keyring, so it never depends on repo keyring state.
- **The trust-overlap ordering is operator-enforced.** The helper enforces the one
  safe backstop (never empty the bundle) but cannot know whether your *fleet* has
  fully rotated before you `remove` — confirm via the console first (runbook §4).
