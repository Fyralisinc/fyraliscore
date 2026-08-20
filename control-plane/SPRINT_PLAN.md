# BYOC Control Plane — Sprint Plan

This is the **authoritative** plan and contract for the Fyralis BYOC (Bring-Your-Own-Cloud)
control plane. Every component in `control-plane/` MUST honor the [Shared Contracts](#shared-contracts)
section below. When in doubt, this document wins.

## Architecture in one paragraph

Fyralis ships as a **vendor-owned control plane** (CP) and a **customer-VPC-resident data plane** (DP).
The data plane is the full Fyralis application stack running inside the customer's own cloud account;
it processes the customer's data and never sends raw payloads out. A small **outbound-only agent** in
the data plane dials home to the control plane over **mutually-authenticated TLS** to receive signed
configuration/licenses and to push **aggregated, PII-free telemetry** (tiered). The control plane runs
the fleet registry, Mimir/Loki/Grafana for cross-tenant observability, the operator console, and the
onboarding/licensing/release machinery. **No service in the control plane ever opens an inbound
connection into a customer VPC** — the agent is the only initiator (Invariant I2).

```
   ┌──────────────────────── VENDOR (control plane) ────────────────────────┐
   │  auth-proxy (mTLS termination, tenant extraction, X-Scope-OrgID inject) │
   │      │            │             │            │           │              │
   │   Mimir         Loki        Grafana      console     fleet registry     │
   │  (metrics)     (logs)      (dashboards)  (operator)   (deployments)     │
   │   release-CD · config-dist · licensing · onboarding · metering · audit  │
   └──────────────▲──────────────────────────────────┬─────────────────────┘
                  │  mTLS, OUTBOUND-ONLY from DP      │ signed config/license
                  │  (telemetry push + config pull)   ▼
   ┌──────────────┴──────── CUSTOMER VPC (data plane) ──────────────────────┐
   │  agent (dials home) → boundary OTel Collector (tier enforcement)        │
   │  Fyralis app stack (ingestion, reasoning, product, workers, postgres)   │
   └────────────────────────────────────────────────────────────────────────┘
```

---

## Shared Contracts

> **These are normative.** All build agents read this section first and implement against it
> exactly. Field names, enum values, paths, and header names are part of the contract — do not
> rename them.

### C1 — Identity: cert → tenant (server-side, never a header)

- Each customer data plane is issued a **per-tenant mTLS client certificate**. The cert carries the
  tenant identity in a **URI Subject Alternative Name**:

  ```
  spiffe://fyralis/tenant/<tenant_id>
  ```

- The **auth proxy** terminates mTLS, **verifies** the client cert chains to the Fyralis CA, then
  **extracts `tenant_id` from the verified client-cert SAN server-side**. The proxy **MUST NEVER**
  trust a `tenant_id` (or any identity) supplied in a request header, query string, or body. A
  spoofed header is ignored; the cert SAN is the sole source of truth (Invariant I4).
- **Revocation registry** at `control-plane/ca/tenant_registry.json` maps cert fingerprint → tenant:

  ```json
  {
    "<cert_fingerprint_sha256_hex>": {
      "tenant_id": "acme",
      "issued_at": "2026-06-24T00:00:00Z",
      "status": "active"
    }
  }
  ```

  `status` is one of `active | revoked`. On every request the proxy computes the SHA-256 fingerprint
  (lowercase hex) of the presented leaf cert, looks it up, and rejects (`403`) if the entry is missing
  or `status == "revoked"`. The `tenant_id` from the registry row MUST equal the `tenant_id` parsed
  from the SAN; a mismatch is rejected.

### C2 — Signing (I6): ed25519 sign + verify everything shipped

- **Everything the control plane ships to a data plane is signed**: release tarballs, license JSON,
  and config JSON. Signing uses **ed25519** with a **detached signature** plus a **manifest**.
- Manifest shape (JSON):

  ```json
  {
    "artifact": "release|license|config",
    "version": "1.4.2",
    "sha256": "<hex digest of the signed bytes>",
    "key_id": "cp-signing-2026-06",
    "signed_at": "2026-06-24T00:00:00Z"
  }
  ```

- The **detached signature** is the ed25519 signature (raw 64 bytes, distributed base64) over the
  **canonical signed bytes** = the exact artifact bytes (tarball bytes, or the UTF-8 bytes of the
  compact-JSON license/config). The `sha256` in the manifest is a redundant integrity check, not the
  signed quantity — verifiers MUST verify the ed25519 signature over the bytes, and MAY additionally
  check `sha256`.
- A **keyring** maps `key_id → public_key` (and, on the CP side only, the private key) to support
  **rotation by key id**. Agents ship with the keyring's public keys and **VERIFY before apply**: an
  artifact whose signature fails verification, or whose `key_id` is unknown/retired, is **never
  applied** and is logged to the audit trail.

### C3 — Telemetry tiers (customer-configurable, enforced at the boundary)

Tiers are **cumulative** and enforced by the **boundary OTel Collector inside the customer VPC** so
that nothing higher than the configured tier ever leaves the VPC:

| Tier | Contents                                   | PII / payload |
|------|--------------------------------------------|---------------|
| `T1` | Aggregated **metrics only** (default)      | **ZERO** PII/payload (Invariant I1) |
| `T2` | `T1` + **redacted logs**                   | logs are redacted at the boundary before egress |
| `T3` | `T1` + `T2` + **sampled traces**           | traces sampled + redacted |

The agent advertises its tier in its heartbeat; the boundary collector is configured to the same tier
and **drops** any signal class above the configured tier. T1 is the default and the only tier that is
on unless the customer opts up.

### C4 — Deployment record (the fleet registry row)

The fleet registry stores exactly one row per deployment:

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

- `health` ∈ `green | yellow | red`. `telemetry_tier` ∈ `T1 | T2 | T3`. Timestamps are RFC 3339 UTC.
- Health is derived from heartbeat freshness (and, later, fleet-SLI burn): fresh = `green`, stale =
  `yellow`, missing/expired = `red`.

### C5 — Compose & networking

- Control-plane services share a docker network **`cp-net`**.
- The control plane **also attaches to the data-plane network** locally so it can scrape the local
  data plane during single-host dev/demo (in production the agent pushes; local dev may scrape).
- **Mimir multi-tenancy** is keyed by the **`X-Scope-OrgID`** header, which is injected by the
  **auth proxy** from the verified `tenant_id` (C1). Downstream Mimir/Loki components trust this
  header **only because** it arrives from inside `cp-net` behind the proxy — it is never accepted
  from outside.

### Invariants (must hold across all phases)

- **I1 — No PII at T1.** The default tier emits aggregated metrics only; zero PII or payload bytes.
- **I2 — No inbound to customer VPC.** The agent is outbound-only; the CP never dials the DP in prod.
- **I3 — Data plane survives CP outage.** If the control plane is unreachable, the data plane keeps
  processing the customer's data; the agent buffers/retries telemetry and config polling.
- **I4 — Tenant isolation server-side at the proxy.** Tenant identity comes from the verified client
  cert SAN, enforced at the auth proxy; never from caller-supplied input.
- **I6 — Sign + verify everything shipped.** Releases, licenses, and configs are ed25519-signed and
  verified by the agent before apply.

---

## Phases

Each phase has an owner area, a deliverable, and an exit gate. Build agents append progress to
`BUILD_LOG.md` as phases complete.

### P1 — Trust roots
- **Build:** `ca/` (Fyralis root + intermediate CA, per-tenant cert issuance with the
  `spiffe://fyralis/tenant/<id>` URI SAN, `tenant_registry.json` writer), `signing/` (ed25519
  keyring, detached-signature signer, manifest builder), shared `lib/` primitives (fingerprinting,
  canonical JSON, RFC-3339 time).
- **Exit:** can issue a tenant cert with the correct SAN; can sign + verify an artifact; registry
  round-trips fingerprint → tenant lookups including a `revoked` entry.

### P2 — Auth proxy + boundary + egress
- **Build:** `auth-proxy/` (mTLS termination, C1 cert→tenant extraction, registry/revocation check,
  `X-Scope-OrgID` injection, upstream routing to Mimir/Loki), `boundary/` (OTel Collector config +
  tier-enforcement processor implementing C3 inside the customer VPC).
- **Exit:** a request with a valid tenant cert is routed with the right `X-Scope-OrgID`; a request
  with a spoofed `tenant_id` header is ignored; a revoked cert gets `403`; the boundary drops
  above-tier signals.

### P3 — Mimir / Loki / Grafana + fleet-SLI
- **Build:** `mimir/`, `loki/`, `grafana/` (provisioning, datasources behind the proxy, tenant-scoped
  dashboards), `fleet-sli/` (the golden-12 SLIs + recording/alerting rules across the fleet).
- **Exit:** per-tenant metrics/logs are queryable through the proxy; fleet-SLI rules load and burn-rate
  alerts evaluate.

### P4 — Agent + console + onboarding + licensing + installer
- **Build:** `agent/` (outbound-only dial-home: pull signed config/license, verify per C2, push tiered
  telemetry, heartbeat with the C4 record), `console/` (operator UI/API over the fleet registry),
  `onboarding/` (tenant enrollment → cert issuance → registry row), `licensing/` (signed license mint
  + expiry), `installer/` (customer-VPC bootstrap of the DP + agent).
- **Exit:** an onboarded tenant gets a cert + license + registry row; the agent dials home, verifies a
  signed config, and heartbeats a valid deployment record; console lists the fleet.

### P5 — Release / CD + config-dist + metering + audit + CP-upgrade
- **Build:** `release/` (build + ed25519-sign release tarballs, CD pipeline), `config-dist/` (signed
  config publication + agent rollout), `metering/` (usage aggregation from T1 metrics for billing),
  `audit/` (append-only audit log of every signed-artifact apply, cert issuance, revocation), plus the
  control-plane self-upgrade path.
- **Exit:** a release is built+signed+published; an agent picks up a new signed config and applies it
  only after verification; metering produces a per-tenant usage rollup; audit captures the events.

### P6 — Self-observability + integration + one-command bring-up
- **Build:** CP self-monitoring (the CP scrapes itself into Mimir), end-to-end integration tests
  spanning onboarding → cert → agent dial-home → telemetry → console, and the **one-command compose
  bring-up** wiring every phase's service into `docker-compose.control-plane.yml`.
- **Exit:** `docker compose -f docker-compose.control-plane.yml up` brings the whole CP online; the
  integration suite passes against it.

### P7 — Docs + test guide + review
- **Build:** `docs/` (architecture, operator runbook, security model, tier policy), a test guide, and a
  final security/architecture review pass against the invariants.
- **Exit:** docs link from `README.md`; reviewer signs off that I1–I6 hold.

---

## Ownership map (write-disjoint)

| Area | Owner files / dirs |
|------|--------------------|
| Scaffold + contracts | `README.md`, `SPRINT_PLAN.md`, `BUILD_LOG.md`, `docker-compose.control-plane.yml`, `requirements.txt`, `.gitignore` (this agent) |
| Trust roots | `ca/`, `signing/`, `lib/` |
| Auth/egress | `auth-proxy/`, `boundary/` |
| Observability | `mimir/`, `loki/`, `grafana/`, `fleet-sli/` |
| Fleet/lifecycle | `agent/`, `console/`, `onboarding/`, `licensing/`, `installer/` |
| Ship/operate | `release/`, `config-dist/`, `metering/`, `audit/` |
| Docs/tests | `docs/`, `tests/` |
