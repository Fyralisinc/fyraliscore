# WS-METER — signed Tier-1 usage metering / billing rollup (FR-F2 / FR-F3)

The metering job turns the control plane's **aggregate Tier-1 metrics** into a
**per-tenant, signed, tamper-evident usage rollup** for billing. It reads the central
**Mimir** one tenant at a time (`X-Scope-OrgID: <tenant>`, SPRINT_PLAN.md C5), computes
usage over a billing period, **signs** the rollup with `control-plane/signing` (ed25519,
FR-F2 / C2 / I6), and exports the signed rollups for billing (CSV/JSON).

> **PII posture (Invariant I1):** metering reads **only aggregate counters** — observation
> *counts* per source, reasoning *run counts*, and a *USD spend* number. It never reads a
> payload, a raw event, or any label that could carry PII, and nothing above tier T1. The
> rollup carries integers/floats and a tenant id — no customer data.

## Files

| File | What it is |
|------|------------|
| `mimir_client.py` | A thin **per-tenant Mimir query client** (`httpx`). Sets `X-Scope-OrgID` per call; runs `increase(<counter>[<period>])` instant queries and parses the Prometheus `vector` result. Transport-only; the `httpx` transport is injectable so tests run against a mock Mimir. |
| `rollup.py` | Computes the per-tenant **usage rollup** from the three Tier-1 counters and **signs** it (reuses `signing/sign_bundle` → `rollup.json` + `.sig` + `.manifest.json`). `verify_rollup` re-checks the signature before any usage is trusted (verify-before-apply, I6). Also a CLI. |
| `export.py` | **Exports signed rollups for billing** (CSV/JSON). Verifies every bundle before export (fail-closed: an unverifiable rollup is never billed), carries a signature receipt per row, and round-trips JSON back to the rollup documents. Also a CLI. |
| `selftest.py` | End-to-end proof against a **mock Mimir** (`httpx.MockTransport`) + the **real** signing lib: compute → sign → verify (valid) → tamper → verify (invalid) → export round-trip → fail-closed guards. |
| `Dockerfile` / `service.compose.yml` | Batch-job image + standalone compose fragment (on `cp-net`, signing key mounted read-only). |

## The Tier-1 counters it reads

All three are series the data plane already emits (`lib/observability`) and the fleet-sli
recording rules are built from — metering reads the raw cumulative counters and takes the
**delta over the period** (`increase(...[period])`):

| Usage | Counter | In the rollup |
|-------|---------|---------------|
| obs-per-source / ingestion volume | `writer_full_mode_writes_total{source=...}` | `metrics.obs_per_source.<source>`, `metrics.ingestion_volume` |
| reasoning runs | `think_runs_total` | `metrics.think_runs` |
| LLM / think spend (`cost_usd`) | `think_cost_recent_usd_total` | `metrics.think_cost_usd` |

A missing series (a source with no writes, a tenant that never ran `think`) is recorded as
`0` — never an error. `increase()` edge-extrapolation that dips slightly negative is clamped
to `0` (usage cannot be negative).

## Rollup document schema (the signed bytes)

```json
{
  "tenant_id": "acme",
  "period": { "start": "2026-06-01T00:00:00Z", "end": "2026-07-01T00:00:00Z", "label": "2026-06" },
  "metrics": {
    "obs_per_source":   { "github": 1234.0, "jira": 42.0, "slack": 88.0 },
    "ingestion_volume": 1364.0,
    "think_runs":       57.0,
    "think_cost_usd":   3.141592
  },
  "totals": { "observations": 1364.0, "think_runs": 57.0, "cost_usd": 3.141592 },
  "schema_version": 1,
  "metric_source": "mimir-tier1",
  "generated_at": "2026-06-24T00:00:00Z"
}
```

A **signed rollup on disk** is the trio (same shape as a signed license/config, C2):

```
<out-dir>/rollup.json                 # the document above
<out-dir>/rollup.json.sig             # base64 ed25519 detached signature over the canonical JSON
<out-dir>/rollup.json.manifest.json   # { artifact:"config", version:<period>, sha256, key_id, algo, signed_at }
```

The **signed quantity** is the ed25519 signature over the **canonical (sorted-keys, compact)
JSON** of `rollup.json` — so signing is independent of formatting, and **any later edit to a
usage number breaks verification** (FR-F2 tamper-evidence). `manifest.sha256` is a redundant
integrity check.

## Billing export schema

**JSON** (system-of-record; round-trips exactly):

```json
{
  "export_version": 1,
  "generated_at": "2026-06-24T00:00:00Z",
  "rollups": [
    { "rollup": { <the rollup document> },
      "receipt": { "key_id": "cp-signing-2026-06", "sha256": "...", "signature": "<b64>", "verified": true } }
  ]
}
```

**CSV** (spreadsheet / ERP): one row per `(tenant, period, source)` plus a `__TOTAL__` row
per period, with the signature receipt on every row:

```
tenant_id,period_label,period_start,period_end,source,observations,think_runs,cost_usd,key_id,sha256
acme,2026-06,2026-06-01T00:00:00Z,2026-07-01T00:00:00Z,github,1234.0,57.0,3.141592,cp-signing-2026-06,<hex>
acme,2026-06,...,slack,88.0,57.0,3.141592,...
acme,2026-06,...,__TOTAL__,1364.0,57.0,3.141592,...
```

## Run it

### Self-test (no Docker, no Mimir, no network)

```bash
/home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
  /home/prajwal-adhikari/Desktop/v2/fyralis-control-plane/control-plane/metering/selftest.py
```

Proves: compute from a mock Mimir → sign → verify (VALID) → **tamper a usage number →
verify FAILS** → JSON export round-trips → CSV carries sources + receipt → a zero-activity
tenant rolls up to 0 → export **refuses** an unverifiable bundle → an unknown-key signature
fails. Exit 0 = all green (17/17).

### Compute + sign a rollup for a real tenant (CLI)

```bash
cd /home/prajwal-adhikari/Desktop/v2/fyralis-control-plane/control-plane
PYTHONPATH=metering:signing /home/prajwal-adhikari/Desktop/v2/fyraliscore/.venv/bin/python \
  metering/rollup.py acme --month 2026-06 \
  --mimir-url http://localhost:9009 \
  --out-dir /tmp/billing/acme-2026-06 --verify
```

(Needs a reachable Mimir with `acme` metrics and the active signing key under
`signing/keys/`. Use `--range START END` for an arbitrary RFC-3339 window.)

### Export signed rollups for billing (CLI)

```bash
PYTHONPATH=metering:signing python metering/export.py \
  /tmp/billing/acme-2026-06 /tmp/billing/globex-2026-06 \
  --format csv --out /tmp/billing/2026-06.csv
# JSON (system-of-record): --format json
```

Export **verifies each bundle first** and refuses any whose signature does not validate
against the trust root (`--skip-verify` only for offline re-formatting of trusted data).

### As a compose batch job

```bash
cd /home/prajwal-adhikari/Desktop/v2/fyralis-control-plane/control-plane
docker compose --project-directory . -f metering/service.compose.yml up --build
```

Default command runs the offline self-test (proves the image). Override `command:` to run
`rollup.py` for real tenants (see the fragment header).

## How it wires into the control plane

```
metering job ──query + X-Scope-OrgID:<tenant>──▶ auth-proxy / mimir:9009   (reads T1 counters)
     │
     ├─ compute per-tenant rollup over [start,end]
     ├─ SIGN via control-plane/signing (ed25519, detached sig + manifest)   ← FR-F2 / C2 / I6
     └─ export.py ─▶ billing (CSV/JSON, each row carries a signature receipt)
```

- It REUSES `control-plane/signing` for all signing/verify — no crypto is reimplemented.
- It is **outbound-read-only against Mimir**; it never dials a data plane (I2).
- Verify-before-trust (I6) is enforced on **both** sides: the export refuses unverifiable
  rollups, and a billing consumer can re-verify from the receipt in the export.

## Schema summary (what integrate / downstream can rely on)

```yaml
module:            control-plane/metering            # WS-METER (FR-F2/F3)
reads_from:        mimir (X-Scope-OrgID per tenant)  # aggregate T1 counters only (I1)
counters:
  obs_per_source:  writer_full_mode_writes_total{source}
  think_runs:      think_runs_total
  think_cost_usd:  think_cost_recent_usd_total
period_math:       increase(<counter>[<period>])  evaluated at period end
rollup_artifact:   rollup.json + rollup.json.sig + rollup.json.manifest.json
signed_bytes:      canonical (sorted-keys, compact) JSON of rollup.json  (ed25519)
manifest_kind:     config            # signing artifact kind for JSON canonical signing
signs_with:        control-plane/signing  (active key from signing/trust_root.json)
export_formats:    json (system-of-record, round-trips) | csv (per-source + __TOTAL__)
export_policy:     verify-before-export; unverifiable bundle => REFUSED (fail-closed)
selftest:          metering/selftest.py   (17/17, mock Mimir + real signing)
```

## Caveats

- **Aggregate Tier-1 only — no PII (I1).** Metering reads observation *counts* per source,
  run *counts*, and a USD spend *number*. It deliberately does not read any payload, raw
  event, or PII-bearing label. The `source` label (e.g. `github`, `slack`) is a connector
  name, not customer data. If a future counter you add to this list carries identifying
  labels, it does **not** belong here.
- **The metering job sets `X-Scope-OrgID` itself.** It is a trusted CP-internal reader, so
  the header it sets is the verified tenant id it is billing — it does not come from a data
  plane. The contract that `X-Scope-OrgID` is "never trusted from outside `cp-net`" is about
  *external* callers; this job runs *inside* `cp-net`. For defence-in-depth, point `MIMIR_URL`
  at the auth-proxy and let the proxy inject the header.
- **Counter resets / deploy gaps.** `increase()` over a long window handles single counter
  resets (a restart) correctly, but a data plane that was **down for part of the period**
  under-counts that gap — usage is only as complete as the metrics the agent pushed. A
  deployment that never reported at all yields an all-zero rollup (which is a valid bill of
  zero, not an error); cross-check against the fleet registry heartbeat (C4) before trusting
  a zero rollup as "no usage" vs. "no telemetry".
- **Cost is a metric, not an invoice.** `think_cost_usd` is the data plane's *self-reported*
  LLM spend gauge. It is suitable for usage-based billing inputs but is not an accounting
  source of truth; reconcile against the provider's billing if the contract requires it.
- **Floats, not decimals.** Usage numbers are JSON floats rounded for tidiness (counts to 3
  dp, cost to 6 dp). For currency-exact billing, the downstream system should treat
  `cost_usd` as cents-precise and apply its own rounding policy.
- **One signing key per rollup.** Each rollup is signed by the trust-root *active* key at
  rollup time; key rotation is transparent (the receipt records `key_id`, and `verify_bundle`
  resolves it in the trust root, accepting retired keys only with `--allow-retired`).
- **No `obs_per_source` cap.** A tenant with hundreds of sources produces a large
  `obs_per_source` map and many CSV rows; this is bounded by the connector count, not by any
  cardinality budget — it is fine for billing but is not a place to dump high-cardinality
  series.
```
