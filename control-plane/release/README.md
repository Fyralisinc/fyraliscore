# WS-RELEASE — signed release bundles + canary→fleet rollout

This directory is the Fyralis BYOC control plane's **ship machinery** (SPRINT_PLAN
P5, FR-D, invariant **I6**): it **builds + signs** data-plane releases, **publishes**
them to a versioned registry the fleet pulls from, and **rolls them out** canary→fleet
with **halt-on-drift** and **rollback**.

Everything the control plane ships is ed25519-signed and **verified before apply** by
the agent (C2/I6). This module **reuses `control-plane/signing` for all signing and
verification** — it never re-implements the crypto, the manifest shape, or the
canonical-bytes rule. A bundle built here is byte-for-byte what the agent's
`config_pull` / `verify_bundle` already accepts.

```
  build_release.py            publish.py                 rollout.py
  ───────────────             ──────────                 ──────────
  src tree ──▶ tarball ──▶    registry/<version>/   ──▶  console GET /api/v1/deployments
            + SIGN (ed25519)  (verify-before-publish)    pick CANARY ──▶ promote ──▶ WATCH
            + release.json    serve /releases/<v>/…       healthy? ──▶ promote FLEET
                                  ▲                       drifted? ──▶ HALT (+ rollback)
                                  │ agent pulls + VERIFIES (I6)
                              data-plane agent
```

## Components

| File | What it is |
|------|------------|
| `build_release.py` | Package a source tree into a **deterministic versioned tarball** (`fyralis-release-<v>.tar.gz`) + a human/CD `*.release.json` manifest, then **SIGN** the tarball (detached `.sig` + the C2 signing manifest) via the signing context. |
| `publish.py` | The **release registry**: store signed bundles by version on disk (`<registry>/<version>/…` + `index.json` with a `latest` pointer), **re-verifying before publish** (refuse unsigned/tampered — I6). Also an **HTTP server** that serves bundles in exactly the layout the agent's `config_pull` consumes. |
| `rollout.py` | The **canary→fleet rollout controller**. Reads deployment versions/health from the console (`GET /api/v1/deployments`), promotes a release to a **canary subset** first, **watches** their health rollup, **HALTS** the rollout on canary drift before fleet-wide promotion, and supports **rollback** to the prior version. |
| `signing_ctx.py` | A thin, *configurable* wrapper over `control-plane/signing` so the builder/registry can sign/verify against a **chosen** trust root + key store (the production CP key, or a hermetic CI/self-test key store) without writing into the committed `signing/` tree. |
| `_bootstrap.py` | Puts `control-plane/` (for `import lib…`) and `control-plane/signing/` (for `import sign_bundle`/`verify_bundle`/`signing_lib`) on `sys.path` — the same convention `agent/_bootstrap.py` uses. |
| `selftest.py` | End-to-end proof (see below). |
| `service.compose.yml` + `Dockerfile` | Overlay to run the registry HTTP server (`release-registry`, port 8090) on `cp-net`. |

## How signing is wired (reuse, not reinvention)

* **Sign** goes through `signing_lib` exactly as `signing/sign_bundle.py` does
  (canonical bytes for the artifact kind → ed25519 detached sig → C2 manifest).
* **Verify** goes through `signing/verify_bundle.verify_file` — **the very function
  the agent calls before apply**. So "it verifies in the registry" ⇒ "the agent will
  apply it".
* The only thing `signing_ctx` adds is a **configurable trust-root path**, because
  `sign_bundle` resolves its key store from fixed paths and the builder/self-test need
  to point at a chosen one. Canonical bytes for a release tarball are the **exact
  tarball bytes** (C2: tarballs are signed as opaque blobs).

## Quick start

```bash
PY=/path/to/.venv/bin/python   # the host venv with cryptography/fastapi

# 0. (production) mint a CP signing key once — owned by control-plane/signing:
$PY control-plane/signing/keygen.py --activate

# 1. BUILD + SIGN a release from a data-plane source tree
$PY control-plane/release/build_release.py build \
    --src ./dataplane --version 1.4.3 --out ./_dist
#   -> _dist/fyralis-release-1.4.3.tar.gz{,.sig,.manifest.json,.release.json}

# 2. VERIFY (round-trip; this is what the agent does before apply)
$PY control-plane/release/build_release.py verify ./_dist/fyralis-release-1.4.3.tar.gz

# 3. PUBLISH into the registry (verify-before-publish; moves `latest`)
$PY control-plane/release/publish.py publish \
    ./_dist/fyralis-release-1.4.3.tar.gz --registry ./_registry
$PY control-plane/release/publish.py list --registry ./_registry

# 4. SERVE the registry so agents can pull + verify
$PY control-plane/release/publish.py serve --registry ./_registry --port 8090
#   GET /releases/latest                       -> JSON pointer
#   GET /releases/1.4.3/fyralis-release-1.4.3.tar.gz[.sig|.manifest.json]

# 5. ROLL OUT canary -> fleet against the live console (port 8080)
$PY control-plane/release/rollout.py promote \
    --console http://localhost:8080 --version 1.4.3 \
    --canary-count 1 --watch-seconds 30 --poll-seconds 3
#   healthy canary -> fleet promoted (exit 0)
#   drifted canary -> HALT, fleet untouched, canary rolled back (exit 1)

# inspect / roll back
$PY control-plane/release/rollout.py status   --console http://localhost:8080
$PY control-plane/release/rollout.py rollback --console http://localhost:8080 --to 1.4.2
```

## The rollout controller in detail

1. **Read** the fleet via `GET /api/v1/deployments` (C4 records; the console derives
   `health` on read from heartbeat freshness + SLI burn — the controller trusts the
   console's derived health, never the wire field).
2. **Select** a canary deterministically: eligible = deployments **not already on the
   target**; sort by `deployment_id`; take `--canary-count` (or `--canary-fraction`,
   default 1). Always leaves ≥1 deployment in the fleet remainder so the canary
   actually gates (unless there is only one eligible deployment).
3. **Record** each deployment's **prior version** (for rollback).
4. **Promote** the canary to the target version (via the injected `Promoter`).
5. **Watch** the canary health rollup (poll the console):
   * any canary **non-green** (drift) → **HALT immediately**: fleet **not** promoted,
     and (default) the canary is **rolled back** to its prior version;
   * a canary **missing** from the registry → halt;
   * window expires without canaries adopting the target version → halt;
   * all canaries **green on the target version** → proceed.
6. **Promote the fleet** remainder only after a clean canary watch.

Health policy is configurable: by default **only `green`** counts as healthy
(strict). `--tolerate-yellow` accepts a stale-but-not-dead canary.

### What "promote" actually does (and what it does not)

The controller owns the **decision** (who is canary, watch, halt/proceed, rollback);
moving bytes is the agent's job. Promotion is expressed through an injected
`Promoter(deployment_id, version)` so the controller stays decoupled and testable:

* the **default** promoter (`--registry …`) moves the release registry's `latest`
  pointer to the target the first time it's seen, and the controller logs the
  per-deployment intent; the agent then pulls the new signed bundle, **verifies**
  it (I6), applies it, and heartbeats its new version — which the console reflects;
* the **self-test** promoter drives a fake fleet's heartbeats directly so it can model
  a good build (green) vs a bad build (red drift) deterministically;
* wiring a **per-deployment** signed config that pins each deployment's target version
  is `config-dist`'s job (a sibling) — the rollout controller calls into it via the
  same `Promoter` seam when that lands.

A promotion can never bypass signing: the only thing a deployment adopts is a release
whose bundle the agent verified. The controller operates on **versions**; the agent's
verify-before-apply is the gate on the **bytes** (I6).

## Self-test

```bash
/path/to/.venv/bin/python control-plane/release/selftest.py
```

Runs against the **real** committed siblings (no mock crypto, no mock console — it
drives the actual `console/app.py` FastAPI app over a `TestClient`):

* **T1** build + sign a release → `verify_bundle` **accepts**; tarball is
  deterministic (stable sha256) and a `*.private.pem` secret is excluded.
* **T2** tamper the tarball → `verify_bundle` **rejects**; a foreign trust root
  (unknown key) also rejects.
* **T3** publish (verify-before-publish); refuse to publish a tampered bundle; the
  published on-disk bundle re-verifies (what an agent pulls).
* **T4** rollout with a **healthy canary** → canary green on target → **fleet promoted**.
* **T5** rollout with an **unhealthy canary** → **halt-on-drift** → **fleet NOT
  promoted**, canary rolled back to its prior version.
* **T6** rollback the whole fleet to the prior version.

All 20 checks pass.

## Compose

The registry HTTP server runs as an overlay (not merged into the master compose):

```bash
docker compose \
  -f docker-compose.control-plane.yml \
  -f release/service.compose.yml \
  up release-registry          # FastAPI on :8090, on cp-net
```

`build_release.py` and `rollout.py` are operator/CD **CLIs**, not long-running
services, so they run on demand (a CI job or operator shell / `docker run … python
release/rollout.py …`), not as compose services.

## Caveats

See `## Caveats` at the bottom of this file — but in short: promotion's byte-delivery
half (per-deployment signed config) is delegated to `config-dist` via the `Promoter`
seam; the controller here owns the safe-rollout decision logic and the registry. The
self-test/CI sign with an **ephemeral** key store so they never touch the committed
`signing/` tree; production signs with the real CP key minted by `signing/keygen.py`.

### Caveats

* **No production CP key is committed.** `build_release`/`publish` default to the
  committed `control-plane/signing` trust root, which is empty until someone runs
  `signing/keygen.py --activate`. The self-test and CI use `--signing-root` /
  `SigningContext.ephemeral` to mint a throwaway key store so they are hermetic and
  write-disjoint from `signing/`.
* **Promotion delivers a version, not bytes.** The controller decides *who* gets the
  release and *when*, and moves the registry `latest` pointer; the actual per-deployment
  signed-config delivery (so each agent pulls + verifies + applies its pinned version)
  is `config-dist`'s responsibility and is invoked through the `Promoter` callback. The
  default CLI promoter is therefore a registry-pointer + intent-log; swap in a
  config-dist promoter when that lands.
* **Health source is the console's derived health.** The controller is only as good as
  the console's NFR-5 freshness thresholds (stale >90 s → yellow, missing >300 s → red)
  and any fleet-SLI burn flag the console folds in. A bad release that stays *green*
  (e.g. crashes only on real traffic) won't be caught by heartbeat-freshness alone — pair
  rollout with fleet-SLI burn alerts for behavioural canary signal.
* **Deterministic canary = first-by-id.** Canary selection is the lowest
  `deployment_id`s among eligible, for reproducibility; it is not (yet) region/criticality
  aware beyond an optional `--region` filter. `--canary-count` / `--canary-fraction`
  size it.
* **The registry server is unauthenticated on its own.** In a real BYOC deploy it sits
  behind the auth-proxy / a TLS vendor endpoint; the bundles are still ed25519-signed, so
  authenticity does not depend on transport — but availability/listing does. Path-traversal
  on artifact fetch is blocked.
* **Tarball determinism** pins mtimes/uids and sorts entries, so the same source tree +
  version yields the same sha256. It does **not** canonicalize file *contents* (e.g. it
  won't strip a build timestamp your source baked in) — give it a clean source tree.
