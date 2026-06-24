# Fyralis BYOC Control Plane

This directory holds the **Fyralis BYOC (Bring-Your-Own-Cloud) control plane** — the
vendor-owned half of a two-plane deployment model.

## The model: vendor control plane + customer-VPC data plane

Fyralis can run entirely inside a customer's own cloud account. We split the system into two planes:

- **Control plane (this directory) — vendor-owned.** Runs in Fyralis-operated infrastructure. It holds
  the trust roots (CA + signing keys), the fleet registry of every deployment, cross-tenant
  observability (Mimir / Loki / Grafana), the operator console, and the onboarding / licensing /
  release / config-distribution machinery.
- **Data plane — customer-owned, in the customer's VPC.** The full Fyralis application stack
  (ingestion, reasoning, product, workers, Postgres) processing the customer's data. The customer's
  raw data never leaves their VPC.

The two planes are connected by a single, **outbound-only agent** that lives in the data plane and
**dials home** to the control plane over **mutually-authenticated TLS**:

```
   VENDOR control plane  ◀── mTLS, outbound-only ──  CUSTOMER VPC data plane
   (CA, fleet, Mimir/Loki/Grafana, console,            (agent → boundary collector →
    onboarding, licensing, release, config-dist)         Fyralis app stack + Postgres)
```

The control plane **never** opens an inbound connection into a customer VPC. The agent is the only
initiator: it pulls **signed** config/license artifacts (and verifies them before applying) and pushes
**aggregated, PII-free, tiered** telemetry. See `SPRINT_PLAN.md` for the normative architecture
diagram.

## Core guarantees (invariants)

These hold across every component — full statements and rationale are in
[`SPRINT_PLAN.md`](./SPRINT_PLAN.md#invariants-must-hold-across-all-phases):

- **I1 — No PII at the default telemetry tier (T1).** Aggregated metrics only.
- **I2 — No inbound connections to the customer VPC.** The agent is outbound-only.
- **I3 — The data plane survives a control-plane outage.** It keeps processing the customer's data.
- **I4 — Tenant isolation is enforced server-side at the auth proxy** from the verified client-cert
  SAN, never from a request header.
- **I6 — Everything shipped is ed25519-signed and verified before apply** (releases, licenses,
  configs).

## Telemetry tiers (customer-configurable)

Tiers are cumulative and enforced at the **boundary OTel Collector inside the customer VPC**:

| Tier | Adds | Default |
|------|------|---------|
| `T1` | Aggregated metrics only, zero PII | ✅ on by default |
| `T2` | + redacted logs | opt-in |
| `T3` | + sampled traces | opt-in |

## Layout

```
control-plane/
├── README.md                          ← you are here
├── SPRINT_PLAN.md                     ← authoritative plan + shared CONTRACTS (read this first)
├── BUILD_LOG.md                       ← append-only build journal
├── docker-compose.control-plane.yml   ← one-command bring-up (filled in per phase)
├── requirements.txt                   ← Python deps
├── ca/            trust roots: CA, per-tenant cert issuance, tenant_registry.json
├── signing/       ed25519 keyring, detached-signature signer, manifests
├── lib/           shared primitives (fingerprinting, canonical JSON, time)
├── auth-proxy/    mTLS termination, cert→tenant, X-Scope-OrgID injection
├── boundary/      customer-VPC OTel Collector + telemetry-tier enforcement
├── agent/         outbound-only dial-home agent (verify config, push telemetry, heartbeat)
├── mimir/  loki/  grafana/   cross-tenant observability stack
├── fleet-sli/     golden SLIs + recording/alerting rules across the fleet
├── console/       operator UI/API over the fleet registry
├── onboarding/    tenant enrollment → cert issuance → registry row
├── licensing/     signed license mint + expiry
├── installer/     customer-VPC bootstrap of the data plane + agent
├── release/       build + sign release tarballs, CD
├── config-dist/   signed config publication + agent rollout
├── metering/      usage aggregation from T1 metrics for billing
├── audit/         append-only audit log of signed-artifact applies, cert events
├── tests/         integration + end-to-end tests
└── docs/          architecture, runbook, security model
```

## QUICKSTART — the one-command bring-up

The whole control plane comes up with a single first-run command. It mints the
trust roots, onboards a demo tenant (`acme`), and brings up every service:

```bash
cd control-plane
./bootstrap.sh           # CA + signing keys + demo-tenant onboard, then `up`
```

`bootstrap.sh` is **idempotent** (safe to re-run) and does, in order:

1. generate the **CA** (`ca/bootstrap_ca.py`) → `ca/pki/*`
2. generate **+ activate** the CP **signing key** (`signing/keygen.py --activate`)
   → `signing/trust_root.json` (the private key stays gitignored)
3. mint the **auth-proxy server cert** (`auth-proxy/gen_server_cert.py`)
4. **onboard the demo tenant `acme`** → a signed bundle (cert + license +
   agent-config), staging the runtime material the compose mounts into
   `./_runtime/` (gitignored) and writing a `.env`
5. `docker compose up -d`, wait for health, print the URLs

Then look here:

| Surface | URL | What you see |
|---------|-----|--------------|
| **Operator Console** | http://localhost:8080 | fleet registry + per-deployment health (derived from heartbeats) |
| **Grafana** | http://localhost:3000 | **Fleet** + **Per-Customer** dashboards (golden-12 SLIs from the demo tenant), and the **Control-Plane** folder = the self-observability watchdog. Login `admin` / `fyralis-operator` |

> **Trust boundary (I4).** Only the three operator/tenant entrypoints above
> publish host ports — `auth-proxy` (:8443 tenant mTLS ingest), `console` (:8080),
> `grafana` (:3000). All INTERNAL stores (Mimir, Loki, the CP self-obs
> Prometheus, config-dist, release-registry, …) are **cp-net-only** with **no host
> ports**, so nothing on the host can reach them directly with a forged
> `X-Scope-OrgID`. Reach the CP self-obs watchdog and Mimir/Loki **through
> Grafana** (provisioned datasources) or the **auth-proxy**, not a host port.

Common operations (see the `Makefile`):

```bash
make up                 # bring the stack up (build + -d)
make smoke              # end-to-end smoke (tests/e2e_smoke.py — no docker needed)
make onboard TENANT=globex REGION=eu-west   # onboard another tenant
make logs               # tail the stack
make down               # stop the stack
make clean              # full reset (down -v + wipe generated CA/keys/runtime)
```

### Offline / CI path (no docker)

```bash
./bootstrap.sh --no-docker   # CA + signing + demo onboard + the python e2e smoke
```

This exercises the real control-plane code paths (CA chain, ed25519 signing,
onboarding, the agent license/config-verify gates, and the auth-proxy mTLS
tenant-isolation contract) with Mimir mocked — no containers required.

### Data flow (what the demo exercises)

```
  CUSTOMER VPC (demo)                         VENDOR CONTROL PLANE
  ──────────────────                          ────────────────────
  demo-dataplane :9300  ── scrape ─▶  boundary OTel Collector
   (golden-12 fyralis_* SLIs)          │  allowlist-filter + drop PII labels (I1)
                                       │  stamp tenant/deployment/region (C4)
                                       ▼
                         auth-proxy :8443  (mTLS termination; X-Scope-OrgID
                         ◀── mTLS ───────   injected from the verified client-cert
                          (acme client cert) SAN — never from a header, I4)
                                       ▼
                                     Mimir  (multi-tenant store)
                                       ▼
                                   Grafana  (Fleet + Per-Customer views, fleet-sli rules)

  fyralis-agent  ── outbound heartbeat (I2) ─▶  Console  (fleet health, derived on read)
   (verifies signed license + config before apply, I6)

  cp-self-obs-exporter ─ probes every CP service ─▶ cp-prometheus (independent watchdog, NFR-10)
```

> The `demo-dataplane` is a **stub** so the bring-up is testable without the real
> Fyralis stack. In **production** the installer (`installer/`) stands up the real
> data plane in the customer VPC and the boundary collector scrapes the **real**
> targets. See `demo-dataplane/README.md`.

Python tooling (CA, signing, agent, console, metering) runs in a virtualenv:

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
```

## Documentation

Full docs live in [`docs/`](./docs/) — architecture, operator runbook, security model, and the
telemetry-tier policy. The plan of record and shared contracts are in
[`SPRINT_PLAN.md`](./SPRINT_PLAN.md); the build journal is in [`BUILD_LOG.md`](./BUILD_LOG.md).
