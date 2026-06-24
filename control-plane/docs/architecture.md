# Fyralis BYOC Control Plane — Architecture

> Authoritative architecture reference for the Fyralis **Bring-Your-Own-Cloud (BYOC)**
> control plane. Grounded in the committed tree under
> `control-plane/`. For per-feature usage and caveats see
> [`reference.md`](./reference.md); for the operator runbooks see
> [`operations.md`](./operations.md). The normative plan and shared contracts are
> in [`../SPRINT_PLAN.md`](../SPRINT_PLAN.md); the build journal is in
> [`../BUILD_LOG.md`](../BUILD_LOG.md).

---

## 1. The two-plane model

Fyralis runs as **two planes** with a single, **outbound-only** link between them:

```
   ┌──────────────────────── VENDOR control plane ──────────────────────────┐
   │  auth-proxy (mTLS termination, cert→tenant, X-Scope-OrgID inject)       │
   │      │            │             │            │           │              │
   │   Mimir         Loki        Grafana      console     fleet registry     │
   │  (metrics)     (logs)      (dashboards)  (operator)   (deployments)     │
   │   release · config-dist · licensing · onboarding · metering · audit     │
   │   self-obs (independent watchdog: "silence != health")                  │
   └──────────────▲──────────────────────────────────┬─────────────────────┘
                  │  mTLS, OUTBOUND-ONLY from DP      │ signed config/license
                  │  (telemetry push + config pull)   ▼
   ┌──────────────┴──────── CUSTOMER VPC data plane ───────────────────────┐
   │  agent (dials home) → boundary OTel Collector (tier enforcement, I1)    │
   │  Fyralis app stack (ingestion, reasoning, product, workers, postgres)   │
   └────────────────────────────────────────────────────────────────────────┘
```

- **Control plane (CP) — vendor-owned.** Runs in Fyralis-operated infrastructure. It
  holds the **trust roots** (CA + ed25519 signing keys), the **fleet registry** of every
  deployment, cross-tenant **observability** (Mimir / Loki / Grafana), the **operator
  console**, and the **onboarding / licensing / release / config-distribution / metering /
  audit / upgrade** machinery, plus an independent **self-observability** watchdog.
- **Data plane (DP) — customer-owned, in the customer's VPC.** The full Fyralis
  application stack processing the customer's data. The customer's raw data **never
  leaves their VPC**.

The two planes are connected only by the **agent** (`agent/`), which lives in the data
plane and **dials home** to the control plane over **mutually-authenticated TLS**. The
control plane **never** opens an inbound connection into a customer VPC (Invariant I2).
The agent is the only initiator: it **pulls** signed config/license artifacts (and
**verifies them before applying**, I6) and **pushes** aggregated, PII-free, tiered
telemetry.

---

## 2. The end-to-end data flow

There are **two distinct paths** into the central stores, with **two different trust
boundaries**. Confusing them is the single most important mistake to avoid.

### 2.1 Agent ingest path (untrusted edge — PUSH — trust boundary = mTLS cert)

```
  CUSTOMER VPC                                       VENDOR CONTROL PLANE
  ──────────────────                                 ────────────────────
  data plane :9300/:8000/exporters
        │  (golden-12 fyralis_* SLIs; real targets in prod,
        │   demo-dataplane stub in the testable bring-up)
        ▼  scrape
  boundary OTel Collector  (boundary/)
        │  GATE 1: family allowlist  (default-deny; keep golden-12 + G1–G7)   ─┐
        │  GATE 2: label drop        (strip every id/email/url/free-text)      ├─ I1
        │  stamp C4 identity (tenant_id/deployment_id/region/telemetry_tier)  ─┘
        │  does NOT set X-Scope-OrgID
        ▼  remote_write over mTLS (OUTBOUND ONLY, I2)
  auth-proxy :8443  (auth-proxy/)
        │  terminate client mTLS (CERT_REQUIRED, chains to Fyralis CA)
        │  re-verify chain → extract tenant_id from the VERIFIED SPIFFE SAN ONLY
        │  fingerprint → registry revocation check (fail-closed, 403)
        │  STRIP any caller X-Scope-OrgID → INJECT X-Scope-OrgID:<tenant_id>  (I4)
        ▼  http
  Mimir :9009  (metrics)   /   Loki :3100  (T2 logs)
        ▼
  fleet-sli rules evaluate centrally in the Mimir ruler under tenant __fleet__
        ▼
  Grafana :3000  (Fleet + Per-Customer dashboards)
```

The boundary collector deliberately **does not** set `X-Scope-OrgID`; the proxy injects
it server-side from the verified client-cert SAN. Tenant scoping is therefore a property
of the **certificate**, not of any header the data plane could forge.

### 2.2 Operator query path (trusted, internal — trust boundary = the cp-net network)

```
  operator Grafana ──HTTP + X-Scope-OrgID header──► mimir:9009 / loki:3100  (DIRECT)
```

Grafana sets `X-Scope-OrgID` **itself**, per datasource (per-customer datasources are
templated by the `${tenant_scope}` variable; `Mimir (fleet)` / `Loki (fleet)` use the
admin org `__fleet__`). The operator side holds **no per-tenant client cert**, so these
datasources do **not** go through the mTLS proxy — they reach Mimir/Loki directly over
`cp-net`. Mimir/Loki trust the header **only because** it arrives from inside `cp-net`
behind the network boundary (Contract C5). Datasources use `access: proxy` so the header
is attached server-side and never reaches the browser (`grafana/`).

### 2.3 Fleet-health path (heartbeat — derived on read)

```
  agent ──outbound https POST /api/v1/heartbeat──► console :8080
        (C4 DeploymentRecord; buffers + retries on console outage, I3)
  console derives health on read (fresh≤90s green / ≤300s yellow / >300s red / expired→red)
```

### 2.4 Self-observability path (independent watchdog)

```
  cp-self-obs-exporter :9110 ── probes every CP service each scrape ──► cp-prometheus :9091
        (auth-proxy via TLS handshake; mimir/loki /ready; grafana /api/health;
         console/config-dist/release /healthz; ingest-path-alive synthetic)
```

`cp-prometheus` is **independent of** the Mimir/fleet pipeline so it can still page when
the thing that broke *is* the fleet pipeline. Every "down" alert has a `absent()` silence
twin so the *absence* of a signal pages as loudly as an explicit failure (NFR-10).

---

## 3. The six invariants (where they live)

The shared contracts (`SPRINT_PLAN.md` §Shared Contracts) carry the normative
statements; the per-component READMEs and code mechanize them. The first-class
invariant set is **I1–I6** (the scaffold and `BUILD_LOG.md` enumerate "invariants
I1–I6"); `SPRINT_PLAN.md`'s invariant block names I1, I2, I3, I4, I6 explicitly, and
**I5 (break-glass)** is stated normatively in `audit/README.md` and implemented in
`audit/`.

| Inv | Statement | Mechanized in |
|-----|-----------|---------------|
| **I1** | **No PII at the default tier (T1).** Aggregated metrics only; zero PII/payload bytes. | `boundary/` two gates (family allowlist + label drop); `redaction_allowlist.md`; `lib/tiers.py` (`carries_pii_risk()` False only for T1) |
| **I2** | **No inbound to the customer VPC.** The agent is outbound-only; the CP never dials the DP in prod. | `agent/` (no listen host/port, no server framework; proven 3 ways in `agent/tests/test_no_listener.py`); every CP service that touches a DP does so only by serving the agent's *outbound* call |
| **I3** | **Data plane survives a CP outage.** It keeps processing; the agent buffers/retries telemetry and config polling. | `agent/buffer.py` (durable bounded JSONL queue), `agent/agent.py` (never-crash loop, capped backoff); `installer/` persists the buffer volume; `upgrade/` leans on it for zero-disruption |
| **I4** | **Tenant isolation server-side at the proxy.** Identity comes from the verified client-cert SAN, never from caller input. | `auth-proxy/tenant_resolver.py` (verify → extract SAN → fingerprint → registry, all fail-closed); `auth-proxy/security/` adversarial proof (A1–A12) |
| **I5** | **Break-glass access is customer-granted, scoped, time-boxed, and audit-logged.** | `audit/breakglass.py` (state machine: request→approve→check), every transition appended to the hash-chained log |
| **I6** | **Sign + verify everything shipped.** Releases, licenses, configs are ed25519-signed and verified before apply. | `signing/` (keyring + detached signer + `verify_bundle`); consumed by `agent/config_pull.py`, `licensing/validator.py`, `release/`, `config-dist/`, `metering/`, `audit/` |

---

## 4. The trust model

### 4.1 Identity: cert → tenant (Contract C1)

Each data plane is issued a **per-tenant mTLS client certificate** carrying its identity
in a **URI Subject Alternative Name**:

```
spiffe://fyralis/tenant/<tenant_id>
```

The CA hierarchy (`ca/`) is **root → intermediate → tenant leaf** (P-256, the intermediate
signs leaves so the root can stay offline). Leaves are `clientAuth`-only, no-CA, with
exactly one SPIFFE URI SAN.

The auth proxy is the **only** place tenant identity is established. Per request it:

1. **Terminates client mTLS** with `ssl.CERT_REQUIRED` against the Fyralis CA chain — a
   certless or non-chaining cert fails the handshake.
2. **Re-verifies the chain itself** (`ca/verify_chain.py`) — the decision never depends
   solely on the TLS stack being configured correctly (defense in depth).
3. **Extracts `tenant_id` from the verified SPIFFE SAN only** (`ca/ca_lib.extract_tenant_from_cert`).
4. **Computes the leaf's SHA-256 fingerprint** and consults the **revocation registry**.
5. Asserts the registry row's `tenant_id` **equals** the SAN-derived one.
6. **Strips** any caller `X-Scope-OrgID` (all casing/prefix variants) and **injects**
   `X-Scope-OrgID: <tenant_id>` before reverse-proxying.

### 4.2 Fail-closed revocation

The revocation registry is `ca/tenant_registry.json`, keyed by **fingerprint → `{tenant_id,
issued_at, status}`** where `status ∈ {active, revoked}`. The proxy rejects with a flat
**403** if the row is **missing** *or* **revoked**, or on a SAN↔registry mismatch.
`is_revoked()` is **fail-closed**: an unknown fingerprint is treated as revoked. The
registry is re-read fresh per request, so a revocation flip is effective immediately. There
is **no CRL/OCSP** — revocation is a central registry lookup, which is sufficient because the
proxy already sees every request, and leaves are short-lived (90d default) to bound the miss
window for a stolen-but-unrevoked cert. A reconciliation during the build made
`lib/tenant.py`'s `is_revoked()` fail-closed too, so the two readers agree.

### 4.3 Signing: ed25519, verify-before-apply (Contract C2 / I6)

Everything shipped to a data plane — **release tarballs, license JSON, config JSON** — is
**ed25519-signed** with a **detached signature** plus a **manifest**:

```
<file>.sig             # base64 of the raw 64-byte ed25519 signature (detached)
<file>.manifest.json   # { artifact:"release|license|config", version, sha256, key_id, algo:"ed25519", signed_at }
```

The **signed quantity** is the ed25519 signature over the canonical bytes: the **exact
tarball bytes** for a release, or the **compact-canonical UTF-8 JSON** (sorted keys, no
whitespace) for a license/config. The `sha256` in the manifest is a redundant integrity
check, not the signed quantity. A **keyring** maps `key_id → public key` (and, CP-side
only, the private key) to support **rotation by key id**: a new active key signs new
artifacts while **retired** keys are retained so old artifacts still verify; an artifact
signed by a retired key is rejected for new applies by default, and an **unknown** key is
always rejected. Agents ship only the **public** trust root (`signing/trust_root.json`) and
**verify before apply** — a bad signature, unknown/retired key, or wrong artifact kind is
**never applied** and is logged.

### 4.4 The two trust boundaries, side by side

| Path | Direction | Who sets the tenant | Trust boundary | Cert? |
|------|-----------|---------------------|----------------|-------|
| **Agent ingest** | DP → CP push | the **auth-proxy**, from the verified cert SAN | the **mTLS client cert** | yes, per-tenant |
| **Operator query** | Grafana → stores | **Grafana**, per datasource header | the **`cp-net` network** | no |

Mimir (`multitenancy_enabled: true`) and Loki (`auth_enabled: true`) **require**
`X-Scope-OrgID` on every request and reject anonymous ones (401). They trust the header
**only because** it can reach them solely from inside `cp-net` (behind the proxy on the
ingest path, from Grafana on the query path). Their host ports are published for
operator/dev convenience only; in production they are not directly reachable.

---

## 5. Telemetry tiers (Contract C3, customer-configurable)

Tiers are **cumulative** and enforced **at the boundary collector inside the customer
VPC**, by what is *wired* — a higher signal class has no receiver/exporter unless its
pipeline block is present, so it **physically cannot egress** ("C3 by absence").

| Tier | Adds | PII / payload | Default |
|------|------|---------------|---------|
| `T1` | aggregated **metrics only** | **ZERO** (I1) | ✅ on by default |
| `T2` | + **redacted logs** (PII-masked, body replaced with `[redacted-T2]`) → Loki | logs redacted at the boundary before egress | opt-in |
| `T3` | + **sampled traces** (probabilistic sampling + SQL/URL/body stripping) → OTLP | traces sampled + redacted | opt-in |

A tier change is **config-only** (`boundary/tier_policy.yaml` increment blocks) and a
new signed config version (`config-dist/`) — no redeploy. The agent advertises its tier
in its heartbeat (C4 `telemetry_tier`).

---

## 6. The deployment record (Contract C4)

The fleet registry stores exactly **one row per deployment** (`lib/deployment.py`,
`DeploymentRecord`):

```json
{
  "tenant_id": "acme",
  "deployment_id": "acme-use1-7f3a",
  "version": "1.4.2",
  "region": "us-east-1",
  "last_heartbeat_ts": "2026-06-24T00:00:00Z",
  "health": "green",
  "license_expiry": "2027-06-24T00:00:00Z",
  "telemetry_tier": "T1"
}
```

`health ∈ {green, yellow, red}`, `telemetry_tier ∈ {T1, T2, T3}`, timestamps RFC-3339 UTC.
Health is **never trusted off the wire** — it is **re-derived on read** from heartbeat
freshness (`derive_health`): fresh (≤90s) = green, stale (≤300s) = yellow, missing (>300s)
= red; an SLI breach degrades green→yellow; an expired license forces red; a future
heartbeat is clamped (clock-skew safe). The same labels stamp every metric series so the
fleet-sli rules can compute one SLI series **per deployment**.

---

## 7. Compose & networking (Contract C5)

- All CP services share the docker network **`cp-net`**. Some also attach to the
  **`dataplane-net`** (external) network for the single-host demo scrape.
- The one-command bring-up is `docker-compose.control-plane.yml` (16 services), minted by
  `bootstrap.sh`. Published host ports (unique): auth-proxy `8443`, Mimir `9009`, Loki
  `3100`, Grafana `3000`, demo-dataplane `9300`, console `8080`, config-dist `8090`,
  release-registry `8091→8090`, cp-self-obs-exporter `9110`, cp-prometheus `9091→9090`.
  Operator/one-shot tools (`onboarding`, `licensing`, `metering`, `audit`,
  `cp-upgrade-tools`) are profile-gated or `restart: no` and run via `docker compose run`.
- The agent (`fyralis-agent`) has **no `ports:`/EXPOSE** by design (I2).

See [`reference.md`](./reference.md) for every component's config, usage, and caveats, and
[`operations.md`](./operations.md) for the lifecycle runbooks.
