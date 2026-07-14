# Fyralis BYOC — Signed Config Distribution (WS-CONFIG)

`config-dist` is the control-plane service that serves each data plane its **signed
per-deployment config bundle** (FR-C3 / FR-C4 / FR-D4):

* **feature flags** the data plane reads,
* the **`telemetry_tier`** (C3: `T1|T2|T3`),
* the **token-rotation schedule** (FR-D4).

Every published version is **ed25519-signed via `control-plane/signing`** (C2 / I6). The
outbound-only **agent** points `AGENT_CONFIG_URL` at this service, `GET`s the bundle, and
**verifies-before-apply** — which `agent/config_pull.py` already does. A **tier change or a
flag flip is a brand-new signed version, no redeploy**: the agent simply pulls and the
version bumps.

```
customer VPC                         vendor control plane (cp-net)
┌────────────────────────┐  outbound  ┌──────────────────────────────────────┐
│ fyralis-agent          │   mTLS     │ auth-proxy ──▶ config-dist (this)      │
│  AGENT_CONFIG_URL  ─────┼──GET──────▶│   GET /config/<deployment_id>          │
│  config_pull.py        │  (I2: out) │   + .sig + .manifest.json              │
│  verify-before-apply ◀─┼────────────│   ed25519-signed (control-plane/signing)│
│  (I6)                  │            └──────────────────────────────────────┘
└────────────────────────┘
```

The service **never dials into a customer VPC** (I2). In production the agent reaches it
only **through the auth-proxy** on `cp-net`, which terminates the data plane's mTLS and
extracts `tenant_id` from the verified client-cert SAN (C1).

---

## The agent-pull contract (set `AGENT_CONFIG_URL` here)

The agent's fetcher (`agent/config_pull.py:http_fetcher`) GETs **three** URLs derived
from one base `<config_url>`:

| Request | Served by | Response read as |
|---------|-----------|------------------|
| `GET <config_url>` | the config JSON (the signed document) | `response.content` (bytes) |
| `GET <config_url>.sig` | base64 detached ed25519 signature | `response.text` |
| `GET <config_url>.manifest.json` | the C2 manifest JSON | `response.content` (bytes) |

So set the agent's **`AGENT_CONFIG_URL`** to the deployment's HEAD config endpoint:

```
AGENT_CONFIG_URL = http://config-dist:8090/config/<deployment_id>
```

(behind the auth-proxy in production, e.g. `https://<proxy>/config/<deployment_id>`).

The agent then runs `verify_bundle.verify_file` over the trio against its **shipped trust
root** and applies the config **only if**:

1. the **ed25519 signature verifies** over the canonical config bytes, **and**
2. the manifest `key_id` is **known and not retired** in the trust root, **and**
3. the manifest `artifact == "config"`.

Any tampered byte (config, sig, or manifest), an unknown/retired `key_id`, or a non-config
artifact ⇒ **rejected, previous config kept** (I6). The data plane keeps running on its
last-good config if a pull fails (I3).

### The served document shape

`GET /config/<deployment_id>` returns the **whole signed document** (the agent applies it
verbatim; `config_pull.load_applied_config` re-reads it):

```jsonc
{
  "schema": "fyralis.config/v1",
  "tenant_id": "acme",
  "deployment_id": "acme-use1-7f3a",
  "version": 2,                       // per-deployment monotonic integer
  "created": "2026-06-24T00:00:00Z",
  "config": {                         // the FR-C3/D4 surface the data plane reads
    "flags": { "ingestion_enabled": true, "anomaly_detection_enabled": false },
    "telemetry_tier": "T2",           // C3 tier; a change ⇒ a new version
    "token_rotation": {               // FR-D4 schedule
      "enabled": true, "interval_hours": 12,
      "next_rotation_at": null, "grace_seconds": 3600
    }
  }
}
```

The **signed bytes** are the canonical JSON of this document (C2), so the agent's verifier
(which recomputes canonical bytes for `kind="config"`) checks the exact bytes that were
signed regardless of transport formatting.

### Pinned versions / rollback

The agent normally pulls HEAD. For operator pinning or rollback the same trio is available
per version:

```
GET /config/<deployment_id>/v<N>
GET /config/<deployment_id>/v<N>.sig
GET /config/<deployment_id>/v<N>.manifest.json
```

Old versions are **immutable** and keep verifying.

---

## Endpoints

### Agent pull surface (no operator auth at the service; tenant-scoped at the proxy)
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/config/{deployment_id}` | HEAD config bytes (the signed document) |
| `GET` | `/config/{deployment_id}.sig` | HEAD detached signature (base64 text) |
| `GET` | `/config/{deployment_id}.manifest.json` | HEAD C2 manifest |
| `GET` | `/config/{deployment_id}/v{n}[.sig\|.manifest.json]` | a pinned version's trio |
| `GET` | `/trust_root.json` | the **public** verifier keyring (installer convenience) |

### Operator / publish surface (behind the auth-proxy; never agent-inbound)
| Method | Path | Purpose |
|--------|------|---------|
| `GET` | `/healthz` | liveness + `active_key_id` + deployment count |
| `POST` | `/api/v1/config/{deployment_id}` | publish a new signed version |
| `GET` | `/api/v1/config/{deployment_id}` | describe HEAD (version, tier, flags, manifest) |
| `GET` | `/api/v1/config/{deployment_id}/versions` | list versions + HEAD |
| `GET` | `/api/v1/deployments` | list deployments |

**Publish** accepts either a whole `config` body, or individual pieces layered onto the
deployment's current config (or a default for a brand-new deployment):

```sh
curl -XPOST localhost:8090/api/v1/config/acme-use1-7f3a -H content-type:application/json -d '{
  "tenant_id": "acme",
  "telemetry_tier": "T2",
  "flags": { "anomaly_detection_enabled": true },
  "token_rotation": { "interval_hours": 12 }
}'
```

---

## CLI — `publish_config.py`

Set a deployment's config → mint a new signed version (each publish self-verifies via I6
before reporting success):

```sh
PY=/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python

# Flip a flag (layers onto the current config; v -> v+1)
$PY publish_config.py acme-use1-7f3a --tenant-id acme --flag anomaly_detection_enabled=true

# Change the telemetry tier (T1 -> T2) — a new signed version, no redeploy
$PY publish_config.py acme-use1-7f3a --tenant-id acme --tier T2

# Set a token-rotation field (FR-D4)
$PY publish_config.py acme-use1-7f3a --tenant-id acme --rotation interval_hours=12

# Replace the whole config body from a JSON file
$PY publish_config.py acme-use1-7f3a --tenant-id acme --config-file body.json

# Inspect
$PY publish_config.py acme-use1-7f3a --list
$PY publish_config.py --list-deployments
```

Flag/rotation values are parsed as JSON scalars (`true`/`false`/`null`, numbers, JSON, else
string). Store + signing home default to `config-dist/_data/` (override with
`CONFIG_DIST_STORE_ROOT` / `CONFIG_DIST_SIGNING_HOME` or `--store-root` / `--signing-home`).

---

## Signing & the trust root (C2 / I6)

This service signs configs with an **ed25519** key held in a **config-dist signing home**
(`<signing-home>/trust_root.json` + `<signing-home>/keys/<key_id>.private.pem`). All
signing/verifying is delegated to the committed `control-plane/signing` package
(`sign_bundle` → `signing_lib`, `verify_bundle`) — no crypto is re-implemented here; the
write-disjoint rule is honored by **retargeting** those CLIs' storage paths at the
config-dist home for the duration of a call (the code paths and wire formats are identical
to the committed signer).

* The **private key never leaves** the signing home; it's gitignored (`_data/`,
  `*.private.pem`, and the repo-wide `**/keys/`).
* The **public** trust root is exported at `GET /trust_root.json` so an installer can pin
  the verifier keys into the agent. The agent ships these public keys and verifies before
  apply.
* **Rotation by `key_id`** is supported by the underlying keyring: a new active key signs
  new versions while retired keys keep verifying old ones (configs signed by a *retired*
  key are rejected for new applies per `verify_bundle`).

---

## Run

### Locally
```sh
PY=/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python
$PY -m uvicorn config_service:app --host 0.0.0.0 --port 8090
# (the signing key is bootstrapped into config-dist/_data/signing-home on first start)
```

### Self-test (the spec scenario — TestClient, no external services)
```sh
$PY selftest.py
```
Asserts: publish a config → GET it → **verify with `control-plane/signing` (valid)** →
**tamper the served bytes → verify fails (I6)** → **a tier change produces a new version**
→ and drives the **real `agent/config_pull.ConfigPuller`** over the served trio to prove
verify-before-apply applies the good bundle and rejects a tampered one.

### Compose
`service.compose.yml` runs uvicorn on **port 8090** on **`cp-net`**. Build context is the
control-plane **root** so the image includes `signing/` + `lib/`. A named volume
(`config-dist-data`) persists the store + signing home across restarts. The integrate step
merges this fragment into the master `docker-compose.control-plane.yml` (do not edit the
master directly).

---

## Caveats

* **Service-local trust root by default.** Out of the box this service mints its **own**
  ed25519 signing key (a config-dist signing home) rather than the shared CP key, because
  `signing/keys/` is gitignored and not guaranteed present at build time. The agent must
  therefore pin **this** service's public trust root (fetch `GET /trust_root.json`). In
  production, to chain config/license/release to **one** CP trust root, mount a keystore
  that already has `trust_root.json` + the active private PEM and set
  `CONFIG_DIST_SIGNING_HOME` / `CONFIG_DIST_KEY_ID` accordingly — then the same trust root
  the agent already ships verifies configs too.
* **The signing key is a secret.** It lives under the persisted `/data` volume
  (`config-dist/_data/signing-home` locally). Back it up and protect it like any signing
  secret; losing it means re-pinning the agent trust root. It is gitignored.
* **No authn/authz at the service itself.** Tenant isolation and operator authentication
  are enforced **at the auth-proxy** (C1/I4), not here. Do not expose `8090` publicly; the
  publish endpoints especially must sit behind the proxy. The host `ports:` mapping is for
  local/dev inspection only.
* **No per-tenant scoping of the pull path yet.** A caller that reaches the service
  directly can request any `deployment_id`. The config bundle is signed but not encrypted
  and (by design) carries no secrets/PII — it's flags + tier + a rotation schedule (I1).
  Cross-tenant *fetch* prevention is the proxy's job (match the cert SAN's `tenant_id` to
  the requested deployment's tenant); a future hardening can also assert the requested
  deployment belongs to the proxied tenant inside this service.
* **Publishing is serialized per-process** (a `threading.Lock`) so the version counter
  can't race; multiple service replicas writing the **same** deployment to a **shared**
  store concurrently is not coordinated — run a single writer, or front publishes through
  one instance. Reads scale freely.
* **HEAD advances after the trio is durable**, so a crash mid-publish leaves HEAD on the
  previous good version (the half-written `v<N>/` dir is simply unreferenced and can be
  swept).
* **`token_rotation` is a schedule the data plane/agent honor**, not an actor here — this
  service distributes the schedule (FR-D4); the actual credential rotation is performed in
  the data plane / by the onboarding-issued credentials, not by config-dist.
