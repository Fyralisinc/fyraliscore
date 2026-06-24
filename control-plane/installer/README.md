# WS-INSTALLER — minimal local installer for ONE tenant deployment

The **installer** is the customer-VPC bootstrap tool. Given a single **agent
bundle** (tenant cert + signed config + signed license + trust root), it stands
up — on one host — a complete Fyralis BYOC *data-plane deployment*:

```
   customer host (one tenant)                           vendor control plane
  ┌───────────────────────────────────────┐           ┌────────────────────────┐
  │ data plane  ── exporters ──┐           │  mTLS,    │  auth-proxy :8443      │
  │ (postgres/redis/kafka …)   │           │  OUTBOUND │   (cert SAN→tenant→     │
  │                        boundary OTel ──┼─remote_write──▶ X-Scope-OrgID) ▶ Mimir
  │                        collector (I1)  │  only     │                        │
  │                            agent ──────┼─heartbeat──▶ console :8080         │
  │                       (outbound-only,  │  (C4)     │   /api/v1/heartbeat    │
  │                        verify-before-  │           │                        │
  │                        apply I6, I3)   │           └────────────────────────┘
  └───────────────────────────────────────┘
```

> **This is the MINIMAL LOCAL installer (single host, docker compose).** The
> production path is **Helm/Terraform** — see [Production path](#production-path).

---

## What it stands up

`deployment.compose.yml` is a compose overlay that brings up, for **one tenant**:

| Component | What | Source |
|---|---|---|
| **Data plane** | a runnable *subset* of the repo-root `docker-compose.yml`: `postgres` (+`postgres-exporter`), `redis` (+`redis-exporter`), `kafka` (+`kafka-exporter`). Image tags mirror the root compose. | repo-root `docker-compose.yml` |
| **boundary** | the OTel Collector from `control-plane/boundary` — scrapes the data-plane exporters, enforces the telemetry tier (Invariant **I1**: zero PII at T1), stamps deployment identity (C4), and **remote-writes filtered metrics OUTBOUND** to the vendor auth-proxy over mTLS. | `control-plane/boundary/otel-collector-config.yaml` |
| **agent** | the **outbound-only** dial-home agent from `control-plane/agent` — pulls/verifies signed config & license (**I6**), refuses to operate on an expired license, and heartbeats the C4 `DeploymentRecord` to the console, **buffering on console outage** (**I3**). Opens **no inbound listener** (**I2**). | `control-plane/agent` |

Everything tenant-specific is parameterized via `${...}` variables and the bundle
mounts — nothing in the overlay is hard-coded to a tenant. `install.sh` renders
those variables from the bundle manifest.

---

## The bundle it consumes

An **agent bundle** is one directory (minted by the control plane during
onboarding/licensing; locally you can mint a real sample with
`make_sample_bundle.py`). Contract lives in `bundle_lib.py`:

```
<bundle-dir>/
  bundle.json                 # identity manifest the overlay is parameterized by
  ca.crt                      # Fyralis CA chain (boundary trusts proxy server cert)
  client.crt                  # per-tenant mTLS client cert (SAN spiffe://fyralis/tenant/<id>)
  client.key                  # client private key — STAYS in the customer VPC (0600)
  trust_root.json             # ed25519 public keyring the agent VERIFIES against (I6)
  license.json{,.sig,.manifest.json}   # signed license (C2: tenant/plan/expires_at/features)
  config.json{,.sig,.manifest.json}    # signed agent config (C2)
```

`bundle.json` pins the deployment identity:

```json
{
  "tenant_id": "acme",
  "deployment_id": "acme-use1-7f3a",
  "region": "us-east-1",
  "version": "1.4.2",
  "telemetry_tier": "T1",
  "auth_proxy_url": "https://auth-proxy.fyralis.example:8443",
  "console_url": "http://console:8080",
  "license_expiry": "2027-06-24T00:00:00Z"
}
```

**Validation (`bundle_lib.validate_bundle`, fail-closed)** — the installer refuses
to launch unless ALL hold:

1. every required file present and non-empty;
2. `bundle.json` parses, has all keys, valid `telemetry_tier ∈ {T1,T2,T3}`;
3. **C1** — the `client.crt` URI SAN round-trips to `bundle.json.tenant_id`
   (wrong cert for the bundle ⇒ reject);
4. `trust_root.json` is a non-empty ed25519 keyring;
5. **I6** — `license.json` and `config.json` each verify against the bundle's own
   trust root; the license `tenant_id` matches; and the license is **not expired**.

The cert/SAN (`ca/ca_lib`) and signature (`signing/verify_bundle`) checks reuse the
committed P1 primitives — they are not re-implemented here.

---

## Run it

```bash
# 0) (local only) mint a REAL sample bundle (ephemeral CA + signing key)
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python make_sample_bundle.py ./sample-bundle

# 1) validate ONLY (no render, no launch) — this is the safe first move
./install.sh --dry-run ./sample-bundle

# 2) full install: validate → render .env → register (best-effort) → launch
./install.sh ./sample-bundle

# tear down (keeps volumes; add --volumes to wipe buffered state + DB)
./uninstall.sh ./sample-bundle
```

`install.sh` steps: **validate → render `.deployment.env` → register → launch →
print next steps**.

- **Register** is best-effort: it `POST`s `{tenant_id, region, plan}` to
  `${console_url}/api/v1/register`. If the console is unreachable the install
  proceeds anyway — the agent registers/heartbeats when it dials home (**I3**:
  the data plane never depends on the control plane being up). Skip with
  `--no-register`.
- **Launch** runs `docker compose -f deployment.compose.yml --env-file
  .deployment.env up -d`. Skip with `--no-up` to render-only.

---

## Data-plane coupling

- The boundary collector **scrapes data-plane services by name** on the shared
  `dp-net` network. The committed boundary config
  (`control-plane/boundary/otel-collector-config.yaml`) targets the exporters
  `postgres-exporter:9187`, `kafka-exporter:9308`, `redis-exporter:9121` — which
  this overlay provides — **plus** the full worker fleet (`normalizer:9300`,
  `gateway:8000`, `minio:9000`, …) which this MINIMAL subset does **not** run.
  Those absent targets surface as **`up == 0`** in the fleet view, which is the
  intended "coded-but-not-running" signal (boundary design §12 G5) — not an
  error. To exercise the full fleet, bring up the repo-root
  `docker-compose.yml` on the same `dp-net` (or rename `dp-net` to the root
  compose's default network) so the worker targets resolve.
- The overlay references the **root compose's image tags** (`pgvector/pgvector:pg16`,
  `apache/kafka:4.0.2`, `redis:7-alpine`, and the three exporters) so the local
  data plane matches production images.
- The boundary's egress is **outbound only** to `${auth_proxy_url}` over the host
  route; there is **no inbound** network path from the control plane into this
  stack (**I2**).

---

## Self-test

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python selftest.py
```

Checks (no containers needed for the core): overlay declares boundary + agent +
the data-plane subset + the bundle mounts; a freshly-minted sample bundle
validates (cert SAN C1, trust-root, **I6** signature verify, fresh license);
`install.sh --dry-run` exits 0 and reports `VALID`; **negatives** are rejected
fail-closed (expired license, tampered config, wrong-tenant cert); and — when
docker is present — a real `docker compose -f deployment.compose.yml config`
parse with a rendered env. **42/42 pass** here (incl. the real compose parse).

---

## Files

| File | Purpose |
|---|---|
| `deployment.compose.yml` | the per-tenant overlay (data plane + boundary + agent), parameterized by bundle env/volumes |
| `install.sh` | `install [--dry-run] [--no-register] [--no-up] <bundle-dir>` |
| `uninstall.sh` | `uninstall [--volumes] <bundle-dir>` (or `--project <name>`) |
| `bundle_lib.py` | the bundle contract + `validate_bundle()` + `manifest_to_env()` |
| `validate_bundle.py` | CLI over `bundle_lib` (used by `install.sh`; `--print-env` renders the overlay env) |
| `make_sample_bundle.py` | mint a REAL self-contained sample bundle (ephemeral CA + ed25519 keyring) |
| `selftest.py` | the full self-test suite |
| `service.compose.yml` | integrate-step fragment (opt-in `installer-selftest` one-shot; the installer is not a CP service) |

---

## Caveats

- **Minimal subset, not the full data plane.** The overlay runs the stateful core
  + the three exporters the boundary scrapes, not all 45 root-compose services.
  Run the root `docker-compose.yml` for the complete worker fleet (see
  [Data-plane coupling](#data-plane-coupling)).
- **Sample bundle uses an EPHEMERAL CA + signing key.** `make_sample_bundle.py`
  is a fixture: it mints its own root/intermediate CA and ed25519 keyring so the
  self-test is hermetic. A production bundle is minted by the control plane with
  the **real fleet CA** (`control-plane/ca`) and the **real signing key**
  (`control-plane/signing`) during onboarding/licensing — the *shape* is
  identical, only the trust roots differ.
- **`client.key` is a secret.** The bundle holds the tenant private key (written
  `0600`); it must **stay in the customer VPC** and never be committed (the dir's
  `.gitignore` excludes `*-bundle/`, and the repo `.gitignore` excludes `*.key`).
- **Agent/console/onboarding are sibling builds.** This installer wires to their
  P4 contracts (agent command `python -m agent.run`; console
  `POST /api/v1/register`). If the `agent` package isn't present yet, its
  container logs a clear message and idles rather than crash-looping; override
  the entrypoint via `FYRALIS_AGENT_CMD` / `FYRALIS_AGENT_IMAGE` if the agent
  build ships a different one. Registration degrades gracefully when the console
  is down (I3).
- **One tenant per invocation.** The compose project name is derived from
  `deployment_id` (`fyralis-dp-<slug>`) so two bundles never collide on one host;
  run `install.sh` once per tenant.

---

## Production path

This installer is for **local single-host dev/demo**. In production a customer
deployment is provisioned with **Helm** (Kubernetes) or **Terraform** (VM/managed
infra), driven by the same agent-bundle contract:

- the bundle's `client.crt`/`client.key`/`ca.crt` become a **Kubernetes Secret**
  (or a Vault-backed secret), mounted into the boundary collector + agent pods;
- `bundle.json` → Helm `values.yaml` (tenant_id, deployment_id, region, tier,
  auth_proxy_url, console_url);
- the boundary collector ships as a sidecar/DaemonSet scraping the real
  data-plane service discovery; the agent runs as an outbound-only Deployment;
- `license.json`/`config.json` (+ their `.sig`/manifest) ride as a mounted
  ConfigMap that the agent **verifies before apply** exactly as here.

The validation, identity, tier-enforcement, and verify-before-apply semantics are
identical; only the orchestrator changes.
