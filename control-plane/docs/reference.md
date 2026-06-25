# Fyralis BYOC Control Plane — Reference

> The authoritative **feature reference**: what every component does, its key
> config/env, how to use it, and its caveats. Grounded in the committed tree under
> `control-plane/`. Read [`architecture.md`](./architecture.md) first for the two-plane
> model, the data flow, the six invariants, and the trust model; see
> [`operations.md`](./operations.md) for the lifecycle runbooks. The normative contracts
> are in [`../SPRINT_PLAN.md`](../SPRINT_PLAN.md); the build journal is in
> [`../BUILD_LOG.md`](../BUILD_LOG.md). For the **forward-looking** plan of what can
> be added to the operator Console (turning it from a read-only dashboard into a
> control surface), see [`console-roadmap.md`](./console-roadmap.md).

All paths below are relative to `control-plane/`. Python tooling runs in a virtualenv with
`requirements.txt` installed (`cryptography`, `fastapi`, `uvicorn`, `httpx`, `h11`,
`requests`, `pydantic`, `structlog`, `pyyaml`, `bandit`).

---

## Quickstart

```bash
cd control-plane
./bootstrap.sh            # CA + signing keys + demo-tenant onboard, then `docker compose up -d`
./bootstrap.sh --no-docker   # the same, but runs the python e2e smoke instead of containers
```

`bootstrap.sh` is **idempotent** and, in order: (1) generates the CA → `ca/pki/*`; (2)
generates **+ activates** the CP signing key → `signing/trust_root.json` (private key
gitignored); (3) mints the auth-proxy server cert; (4) **onboards the demo tenant `acme`**
via an embedded console → a signed bundle, staging the runtime material into `./_runtime/`
(gitignored) and writing `.env` with the real minted `deployment_id`; (5) `docker compose
up -d`, waits for health, prints the URLs. The `Makefile` wraps common ops: `make up`,
`make smoke`, `make onboard TENANT=globex REGION=eu-west`, `make logs`, `make down`,
`make clean`.

| Surface | URL | What you see |
|---------|-----|--------------|
| Operator Console | http://localhost:8080 | fleet registry + per-deployment health (from heartbeats) |
| Grafana | http://localhost:3000 | Fleet + Per-Customer dashboards + the Control-Plane self-obs folder. Login `admin` / `<see control-plane/.env>` |
| CP self-obs Prometheus | http://localhost:9091 | the independent "silence != health" watchdog |

---

## Trust-root components

### `ca/` — private CA + per-tenant mTLS identity (P1, Contract C1 / I4)

**What it does.** Mints a private CA hierarchy (root → intermediate → tenant leaf, P-256)
and issues a per-tenant mTLS client cert whose URI SAN is
`spiffe://fyralis/tenant/<tenant_id>`. Owns the fail-closed **revocation registry**
`tenant_registry.json` (fingerprint → `{tenant_id, issued_at, status}`).

**Key files.** `ca_lib.py` (`generate_root_ca`, `generate_intermediate`,
`issue_tenant_cert`, `extract_tenant_from_cert`, `fingerprint_sha256`), `verify_chain.py`
(`verify_chain(leaf, chain)` — native `ClientVerifier` with a manual fallback),
`registry.py` (atomic read/write, `is_revoked` fail-closed), CLIs `bootstrap_ca.py` /
`issue_cert.py` / `revoke.py`. Production path is **step-ca** (`config/ca.json` +
`config/templates/tenant-leaf.tpl`), which stamps the identical leaf shape.

**Use.**
```bash
python ca/bootstrap_ca.py                 # root + intermediate → ca/pki/
python ca/issue_cert.py issue acme        # tenant cert + an active registry row
python ca/revoke.py revoke acme           # revoke by tenant (or by exact fingerprint)
python ca/revoke.py list
```

**Caveats.** No CRL/OCSP — revocation is a registry lookup at the proxy. Single registry
JSON file (atomic writes, not multi-writer-locked; back with a DB at scale). CA/tenant
keys are `0600` and gitignored, **unencrypted by default for dev** (`--key-password
env:VAR` to encrypt; prod keeps the intermediate in step-ca's secret store and the root
offline). P-256 throughout. Tenant id charset is `[A-Za-z0-9._-]` so the SPIFFE URI
round-trips unambiguously.

### `signing/` — ed25519 supply-chain signing (P1, Contract C2 / I6)

**What it does.** The supply-chain trust root: signs everything shipped to a data plane
(release tarballs, license JSON, config JSON) with **ed25519 detached signatures + a
manifest**, and gives agents `verify_bundle` to **verify before apply**.

**Key files.** `signing_lib.py` (keypair gen, sign/verify, the `Keyring` with one active
signer + retained retired verifiers, rotation, canonical bytes), CLIs `keygen.py`
(`--activate` writes `keys/` 0600 + the public `trust_root.json`), `sign_bundle.py`,
`verify_bundle.py` (`verify_file(path) → VerifyResult` — **the function agents call**),
`rotation.py`.

**Use.**
```bash
python signing/keygen.py --key-id cp-signing-2026-06 --activate
python signing/sign_bundle.py sign agent-config.json --kind config --version 7
python signing/verify_bundle.py verify agent-config.json && echo APPLY || echo REFUSE
```

**Verify-before-apply policy.** Active key → OK. Retired key → rejected for new applies
(`--allow-retired` to back-verify historical artifacts). Unknown key → always rejected.
`algo` is pinned to `ed25519` (no algorithm-agility downgrade surface).

**Caveats.** **Keys-on-disk is dev-only** — the production custody path is a KMS/HSM
(`Keyring` is structured to hold a remote signer without changing the manifest or
verifier). Trust-root distribution is itself a trust decision (bake it into the signed
installer / pin at enrollment). No per-artifact signature revocation — a *compromised*
key is handled by removing its `key_id` from the trust root entirely (not merely
retiring). `signed_at` is informational (license expiry is the licensing layer's job, not
the signature's).

### `lib/` — shared models + readers (P1, Contracts C1–C5)

**What it does.** The cross-cutting primitives every component imports, so "tenant",
"tier", "deployment record", and "signed bytes" mean exactly one thing fleet-wide.

**Modules.** `tenant.py` (C1: read-only `TenantRegistry`, fail-closed, mtime/size cache so
out-of-band revocations are picked up live), `tiers.py` (C3: `TelemetryTier` T1/T2/T3 +
cumulative `TierPolicy`; `carries_pii_risk()` False only for T1), `deployment.py` (C4:
`DeploymentRecord` + `derive_health`), `config.py` (`ControlPlaneConfig` from `CP_*` env),
`primitives.py` (RFC-3339 time, `canonical_json_bytes` = the C2 signed-bytes definition,
DER/PEM SHA-256 fingerprints), `errors.py`, `logging.py`.

**Use.** Import as the `lib` package from the `control-plane/` root
(`python -m lib._selftest`). Key env: `CP_TENANT_REGISTRY`, `CP_MIMIR_URL`, `CP_ROOT`,
`CP_HEARTBEAT_YELLOW_AFTER_S` (90), `CP_HEARTBEAT_RED_AFTER_S` (300).

**Caveats.** Read-only with respect to the registry/keyring (writers live in `ca/` /
`signing/`). `derive_health` is wall-clock relative; a future heartbeat is clamped to age
0. `fingerprint_pem` imports `cryptography` lazily so the models import with just
`pydantic`/`structlog` present.

---

## Auth / egress components

### `auth-proxy/` — the tenant auth proxy (P2, I4)

**What it does.** The single most security-critical component: an **mTLS-terminating
reverse proxy** in front of Mimir/Loki/Grafana. It is the **only** place a request's
tenant identity is established — server-side, from the verified client cert, never from a
header. See [`architecture.md` §4](./architecture.md) for the full per-request flow.

**Key files.** `proxy.py` (asyncio + `h11` server, builds the `CERT_REQUIRED` SSL context,
pulls the verified DER peer cert via `getpeercert(binary_form=True)`, sanitizes headers,
forwards via `httpx`), `tenant_resolver.py` (the fail-closed core: verify → extract SAN →
fingerprint → registry → SAN↔registry agree, reusing the `ca/` primitives), `config.py`,
`gen_server_cert.py` (mints the proxy's own serverAuth cert), `security/` (the gating
review).

**Config (env).**

| Env | Default | Meaning |
|-----|---------|---------|
| `AUTH_PROXY_LISTEN_HOST` / `_PORT` | `0.0.0.0` / `8443` | bind |
| `AUTH_PROXY_CA_CHAIN` | `../ca/pki/ca-chain.crt` | CA chain that verifies client certs |
| `AUTH_PROXY_TENANT_REGISTRY` | `../ca/tenant_registry.json` | fingerprint→tenant registry (live-mounted) |
| `AUTH_PROXY_TLS_CERT` / `_KEY` | *(required)* | the proxy's own server cert/key |
| `AUTH_PROXY_UPSTREAM_URL` | `http://mimir:9009` | reverse-proxy target |

The CA chain + registry are bind-mounted from the live `ca/` so a revocation lands without
a rebuild; the registry is re-read fresh per request.

**Use.**
```bash
curl --cacert ../ca/pki/ca-chain.crt \
     --cert ../ca/pki/tenants/acme/acme.crt --key ../ca/pki/tenants/acme/acme.key \
     https://localhost:8443/prometheus/api/v1/query?q=up
# upstream receives X-Scope-OrgID: acme (derived from the cert, not the request)
```

**Security posture (verified).** The adversarial suite `security/test_isolation.py`
(A1–A12) plus `tests/` are **green** — the proxy injects `acme` from a valid cert; strips
forged/duplicate/case-variant/prefix `X-Scope-OrgID` headers; 403s revoked, unknown,
foreign-CA, no-cert, and SAN↔registry-mismatch requests, never forwarding them; and keeps
two tenants from ever crossing. The **SSRF defect** that the build flagged (A12 / SAST F1 /
threat T12 — an absolute-form request target re-pointing the upstream) is **FIXED** in the
committed `proxy.py`: `_safe_upstream_path()` reduces every client target to origin-form
`path?query` and discards any scheme/authority, so an absolute-form target lands on the
**configured** upstream (never the attacker host) and authority-form/CONNECT is rejected;
A12 now asserts the off-upstream host is never reached and passes. (Note: the `loki/` and
`grafana/` READMEs were written before the fix and still describe the SSRF as open — the
code and the test suite supersede them.)

**Other caveats.** No-cert behavior is TLS-version-dependent (handshake abort on 1.2,
fail-closed 403 on 1.3) — either way never a 200. No CRL/OCSP (registry-lookup
revocation). HTTP/1.1 only (h11). Why not uvicorn/FastAPI: a security proxy needs direct
verified-DER-cert access and byte-level header control.

### `boundary/` — boundary OTel Collector (P2, Contract C3 / I1)

**What it does.** The egress chokepoint **inside the customer VPC**. Scrapes the data-plane
metrics, enforces **I1 (zero PII at T1)** with two gates, stamps C4 identity, and
**remote-writes filtered metrics through the auth-proxy over mTLS**.

**The two I1 gates.** Gate 1 = **family allowlist** (`filter/allowlist`, OTTL default-deny;
only the golden-12 + G1–G7 families survive). Gate 2 = **label drop**
(`transform/redact-labels`, deletes ~70 id/email/url/free-text labels, keeps only bounded
enums). The `resource` processor adds `tenant_id/deployment_id/region/telemetry_tier` from
env. The collector **never sets `X-Scope-OrgID`** (the proxy injects it).

**Key files.** `otel-collector-config.yaml` (the validated T1 config), `tier_policy.yaml`
(cumulative T1/T2/T3 increment blocks — a tier change is config-only),
`redaction_allowlist.md` (the auditable I1 artifact), `dataplane_remote_write.md` +
`prometheus_remote_write_overlay.yml` (the alternative direct Prometheus path,
reproducing both gates via `write_relabel_configs`).

**Config (env).** `FYRALIS_TENANT_ID` / `_DEPLOYMENT_ID` / `_REGION` (C4 identity),
`FYRALIS_TELEMETRY_TIER` (`T1`/`T2`/`T3` — must match the active pipeline set),
`FYRALIS_AUTH_PROXY_URL` (HTTPS push base), `FYRALIS_AUTH_PROXY_GRPC` (T3 OTLP traces).
mTLS material mounts at `/etc/fyralis/ca/ca.crt` and `/etc/fyralis/agent/client.{crt,key}`.

**Switch tiers (config-only).** Set `FYRALIS_TELEMETRY_TIER=T2` (or `T3`) and merge the
`t2_increment.*` (or `t3_increment.*`) blocks from `tier_policy.yaml`. A higher signal
class has no receiver/exporter unless its block is present, so it physically cannot egress.

**Caveats.** Scrape targets are data-plane service names — a real deploy swaps them for its
own service discovery; the redaction/identity/egress stays fixed. Default-deny is
intentional (a new metric family is dropped until added to the allowlist in **both**
config files — keep them in sync). `anomaly-processor`/`deadline-resolver` are scraped, so
if absent their `up` series is `0` — the intended G5 "coded-but-not-running" signal, not an
error. The single-host compose mounts `./boundary/config.yaml`; symlink/copy
`otel-collector-config.yaml` → `config.yaml` at deploy time.

---

## Observability components

### `mimir/` — central multi-tenant metrics store (P3)

**What it does.** Grafana Mimir 2.13.0, all tenants in one cluster isolated by
`X-Scope-OrgID` (injected by the proxy from the cert SAN). `multitenancy_enabled: true` —
a request without the header is rejected **401**. `target: all` (monolithic); filesystem
blocks + ruler under `/data`; remote-write receive at `POST /api/v1/push`; HTTP on **9009**
(the proxy upstream).

**Per-tenant cardinality budgets.** Defaults in `mimir.yaml > limits`
(`max_global_series_per_user` 150000, `ingestion_rate`/`burst` 25000/50000,
`max_label_names_per_series` 30). Per-tenant overrides in `runtime_overrides.yaml`
**hot-reloaded every 15s** (worked examples: acme 500k, globex 50k, the `__fleet__` ruler
tenant). `cardinality.md` is the MEASURE-then-ENFORCE method.

**Rules loading.** The fleet-sli rules are loaded via the **`mimir-ruler-loader`** one-shot
(`mimirtool rules load` → ruler API, tenant `__fleet__`). Mimir's filesystem ruler does
**not** auto-discover a directory of multi-group YAML — a plain `cp` yields "no rule groups
found".

**Caveats.** Local filesystem storage is **dev-only** (switch `blocks_storage`/etc. to
`s3`/`gcs` for prod; all-in-one RF=1 has no HA). The `grafana/mimir` image is **distroless**
(no shell/curl) so there is no in-container healthcheck — readiness is `GET /ready` (a ~15s
post-start grace returns 503, which is normal). `auth_enabled` is **not** a valid Mimir key
(`multitenancy_enabled` is its successor). Exemplars off + `usage_stats` disabled to keep
I1. The fleet view needs `tenant_federation.enabled: true` for the `__fleet__` cross-tenant
read.

### `loki/` — Tier-2 central log store (P3, Contract C3)

**What it does.** Grafana Loki 3.4.2, the logs analogue of Mimir for **T2 redacted logs**.
`auth_enabled: true` (C5 — `X-Scope-OrgID` required on every request). Filesystem storage
under `/data` (TSDB v13), compactor-owned retention **744h (31d)**, per-tenant ingestion
limits and `runtime_config` hot-reload of `overrides/loki-overrides.yaml`.

**Trust boundary.** **Loki is the sink, not the redactor** — by the time a log reaches it,
the boundary collector has already (inside the VPC) dropped non-allowlisted attributes,
masked PII/secret patterns, and replaced the body with `[redacted-T2]` (I1). Loki performs
no redaction.

**Caveats.** Single-binary, single-instance, RF=1 (split read/write/backend + object store
for HA). Compactor-driven retention deletes lag the policy by the compaction interval (10m)
+ delete delay (2h). A request without `X-Scope-OrgID` gets a 4xx (intentional fail-closed
tenancy). Tenant isolation depends entirely on the proxy being the only ingress and
stripping client-supplied values — do not publish 3100 to an untrusted network. Pinned to
3.x-only keys (`tsdb`, `allow_structured_metadata`) — re-validate before a major bump.

### `grafana/` — operator Grafana (P3)

**What it does.** The operator-facing Grafana (11.1.0). Two dashboards: **Fyralis Fleet —
Overview** (`uid fyralis-fleet-overview`: green/yellow/red census, worst heartbeat age, a
health-colored deployments table, golden-12 fleet panels) and **Per-Customer — Drill-down**
(`uid fyralis-tenant-drilldown`: templated by the `tenant_scope` variable, which *is* the
`X-Scope-OrgID` value, hard-scoping every panel to one customer + a Loki logs panel).

**The query path (critical).** Grafana datasources point **directly** at `mimir:9009` /
`loki:3100` over cp-net (the **trusted operator query path**, distinct from the **agent
mTLS ingest path** through the proxy). Grafana sets `X-Scope-OrgID` itself per datasource —
per-customer datasources are templated by `${tenant_scope}`; `Mimir (fleet)` / `Loki
(fleet)` use `__fleet__`. `access: proxy` so the header attaches server-side and never
reaches the browser. Health is derived **at query time** from
`worker_heartbeat_age_seconds` (green ≤90s / yellow ≤300s / red >300s) + SLI flags.

**Caveats.** `${tenant_scope}` only resolves on the per-customer dashboard — for ad-hoc
Explore use the `(fleet)` datasources. Do **not** switch datasources to `direct`/browser
access (it would leak the scope header). Fleet view assumes Mimir tenant-federation. Loki
panels are empty for T1 (metrics-only) tenants. Health thresholds live in the panels (not
yet recording rules). Default admin creds (`admin`/`<see control-plane/.env>`) are demo-only —
set `GF_ADMIN_USER`/`GF_ADMIN_PASSWORD` or wire SSO.

### `fleet-sli/` — fleet SLI / alert / SLO rules (P3)

**What it does.** Turns the golden-12 SLIs into a per-deployment 🟢/🟡/🔴 view and a paging
contract for the whole fleet. The rules load into the **Mimir ruler** and evaluate
**centrally, once**, under the synthetic `__fleet__` tenant over every tenant's
remote-written metrics — so a customer VPC runs no ruler.

**Files (88 rules total, promtool-clean).** `recording_rules.yml` (58 rules: golden-12 SLIs
both per-deployment `fyralis:*` and fleet-wide `fleet:*`, plus `fyralis:health_code` 0/1/2
matching `lib/deployment.derive_health`). `alert_rules.yml` (17: the 13 deployment alerts
ported to fleet scope + shadow-drop + llm-breaker-open + the G1/G2/G3/G5 gap-metric
alerts). `slo_burnrate_rules.yml` (11) + `slo.md` (NFR-5 SLOs as Google-SRE multi-window
multi-burn-rate alerts: availability 99.5%, fast 14.4× page / slow 6× ticket; liveness
heartbeat). `fleet_sli.rules.yaml` (the Mimir cardinality watchdog).

**Caveats.** Some base names are **gap-metrics** (G1 `fyralis_schema_version`, G2
`fyralis_oauth_token_*`, G3 `fyralis_llm_breaker_state`, G5 worker-present/running) that
must be wired in the data plane before those alerts produce data; `or vector(0)`/`or`
fallbacks keep the recording rules from erroring until then. Several rules `or` two
candidate metric spellings (`_total`/`fyralis_` prefix disagreements). Thresholds are
design-doc defaults — tune per-tenant SLA once baselines exist. 30-day SLO windows warm up
over time on a fresh CP.

---

## Fleet / lifecycle components

### `agent/` — outbound-only data-plane agent (P4, I2 / I3 / I6)

**What it does.** The **outbound-only** agent in the customer VPC. Each tick it collects a
C4 `DeploymentRecord` (version, region, tier, identity, license_expiry from the **verified**
license, health derived from heartbeat freshness folded with a local `/healthz` SLI probe +
license expiry), POSTs it to `<console>/api/v1/heartbeat` over outbound HTTPS, and — license
gated — can pull a signed config bundle and **verify-before-apply (I6)**.

**Invariants it owns.** I2 (no listener: no `EXPOSE`/`ports:`/server framework — proven 3
ways: a `socket.listen` trap, a `/proc/net/tcp` LISTEN diff, and a source forbidden-
primitive scan). I3 (console outage never crashes or blocks: durable bounded JSONL
`buffer.py`, flush oldest-first on reconnect, capped exponential backoff, never-crash
loop). I6 (`config_pull.py` delegates to `signing/verify_bundle` before atomic apply).
License gate (`license_check.py` re-verifies every call; refuses privileged actions when
unlicensed but **still heartbeats** so the console shows a red, unlicensed deployment).

**Config (env).** `AGENT_CONSOLE_URL`, `AGENT_TENANT_ID`, `AGENT_DEPLOYMENT_ID`,
`AGENT_REGION`, `AGENT_TELEMETRY_TIER`, `AGENT_LICENSE_PATH`, `AGENT_TRUST_ROOT`,
`AGENT_HEALTHZ_URL`, `AGENT_INTERVAL_S`, `AGENT_BUFFER_PATH`, `AGENT_BACKOFF_BASE_S/_MAX_S`.
**No listen host/port — by design.**

**Caveats.** License/config **issuance is upstream** — the agent only consumes signed
bundles and ships only the **public** trust root. `register` is not driven by the loop
(deployment_id/tenant_id are pre-provisioned by onboarding/installer). The SLI probe is
single-source (one local `/healthz`; the hook is injectable). Applied config is file-drop
(restart re-reads; no in-memory hot-swap yet). The buffer drops **oldest** past
`AGENT_BUFFER_MAX_RECORDS` (default 10k). Outbound TLS uses `requests`' default trust store
— pin the console cert via `REQUESTS_CA_BUNDLE` for hardening. Single-process buffer
ownership.

### `console/` — fleet console / registry (P4)

**What it does.** A FastAPI service (uvicorn :8080, cp-net) over the fleet registry — the
one-row-per-deployment store of C4 records — and the place where health is **derived on
read** so a silent deployment visibly degrades.

**REST contract.** `POST /api/v1/register {tenant_id?, region, plan}` → mints
`<tenant>-<region>-<rand>` + a tenant_id when absent, stamps an initial green heartbeat;
`POST /api/v1/heartbeat {DeploymentRecord}` → upsert by deployment_id, recompute health
(malformed/extra-field/bad-tier bodies → **422**, never 500); `GET /api/v1/deployments`
(worst-health-first, derived on read); `GET /api/v1/deployments/{id}` (404 if unknown);
`GET /` (HTML fleet rollup); `GET /healthz`.

**Config (env).** `CP_CONSOLE_HOST`/`_PORT` (0.0.0.0/8080), `CP_CONSOLE_DATA_DIR`
(`console/data/`), `CP_HEARTBEAT_YELLOW_AFTER_S`/`_RED_AFTER_S` (90/300).

**Caveats.** `plan` is a register **hint**, not a C4 field (the signed license is
authoritative; the agent's first heartbeat corrects `license_expiry`). The console does
**not** verify license signatures (that is the agent's job — treat the shown expiry as
advisory). **No auth on the console API** — it sits behind the cp-net perimeter; the agent
reaches it **outbound** (I2). Do not publish :8080 to an untrusted network. Persistence is
best-effort single-node JSON (temp+`os.replace`; corrupt/missing → empty, never crashes;
swap for a shared DB past MVP). Health is wall-clock relative (future heartbeat clamped to
age 0). `deployment_id` minting is console-side for the MVP.

### `onboarding/` — atomic per-tenant onboarding (P4, FR-E)

**What it does.** The **all-or-nothing** tenant enrollment transaction. `onboard.py` runs a
6-step transaction, each registering an undo on a LIFO `RollbackLedger`: (1) **register**
via console; (2) **issue cert** via `ca/issue_cert` (the active registry row **is** the
proxy binding); (3) **mint + sign license** (delegates to `licensing.issue_license`, else a
self-contained ed25519 signer); (4) **assemble bundle** (`bundles/<deployment_id>/` with
cert/, signed license, signed agent-config pointing the agent at the console, the public
trust root, `BUNDLE.json`); (5) **seed heartbeat**; (6) **confirm listed**. On any failure
the ledger unwinds newest-first (revoke+delete the registry row, rmtree the partial bundle,
best-effort console remove) — no half-onboarded state. `offboard.py` revokes every active
cert for the tenant (+ optional `--purge-registry`/`--purge-bundle`).

**Use.**
```bash
python onboarding/onboard.py --tenant acme --region us-east --plan standard \
  --console-url http://console:8080 --json
python onboarding/onboard.py ... --local-ids        # no console
python onboarding/onboard.py ... --embedded-console # dev/demo, no server process
python onboarding/offboard.py --tenant acme --deployment acme-use1-7f3a --purge-bundle
```
Plans → features: `trial`=metrics; `standard`=metrics,logs,fleet-dashboards;
`enterprise`=+traces,sso,audit-export. (Plan features ≠ telemetry tiers — tiers gate
egress at the boundary.)

**Caveats.** The P4 console contract has **no DELETE verb**, so against a *real* console
rollback can't delete a seeded deployment (left to age to red; the embedded console
supports removal so the self-test is exact). Needs the signing **private** key to mint.
Bundles carry a tenant private key (gitignored; deliver over a secure channel). The
on-demand `onboarding` compose service is `ops`-profile-gated (`docker compose run --rm
onboarding ...`). `--fail-after` is test-only.

### `licensing/` — signed expiring licenses + fail-closed validator (P4, FR-F)

**What it does.** Issues **signed, expiring licenses** and gives the agent a **local
fail-closed validator**. A license is the grant *"this deployment, this tenant, this plan,
these features, until this instant."*

**License contract.** `{tenant_id, deployment_id, plan, issued_at, expires_at, features[],
license_id, version}`, signed as the C2 detached trio. The signed bytes are the canonical
compact JSON.

**Validation — four gates, ALL required for ALLOW (signature checked first).** (1)
**signature** via `verify_bundle` (`deny_bad_signature`); (2) **expiry** `now < expires_at`
+ not-yet-valid guard (`deny_expired`/`deny_not_yet_valid`); (3) **identity** tenant_id +
deployment_id match this deployment (`deny_tenant_mismatch`/`deny_deployment_mismatch` —
lateral-reuse guard); (4) **revocation** not on the deny list (`deny_revoked`). It **never
raises** for a bad license (every failure is a DENY); a corrupt revocation list fails
closed (`deny_revocation_list_unreadable`, never silently un-revoking).
`validate_for_deployment(record)` binds identity from a C4 record so the identity gate is
never skipped.

**Revocation (FR-F).** `revocations.json` (overridable via `REVOCATIONS_PATH`) revokes by
`license_id` (precise), `deployment_id`, or `tenant_id` (all that tenant's licenses). A
missing list = nothing revoked; a corrupt list = fail-closed deny.

**Use.**
```bash
python licensing/issue_license.py --tenant-id acme --deployment-id acme-use1-7f3a \
  --plan enterprise --duration-days 365 --feature telemetry_t3 --out /etc/fyralis/license
python licensing/validator.py validate /etc/fyralis/license --tenant-id acme --deployment-id acme-use1-7f3a
python licensing/revoke.py add --license-id lic-acme-3f9c1a2b --reason "key compromise"
```

**Caveats.** Validation is only as strong as the shipped trust root. Clock-dependent
(small `skew_seconds` grace; no defense against a back-dated host clock). Revocation is
**pull-based** (propagation = the agent's list-refresh cadence — by design, the agent is
offline-capable). The license binds identity, not authorization policy (consuming
subsystems enforce features). The optional FastAPI `service.py` is an operator convenience
the agent never calls (it validates locally, I2).

### `installer/` — single-tenant provisioning bundle/overlay (P4)

**What it does.** The customer-VPC bootstrap tool (single-host, docker compose).
`deployment.compose.yml` brings up, for one tenant on a shared `dp-net`: a **minimal**
data-plane subset (postgres+exporter, redis+exporter, kafka+exporter), the **boundary**
collector (mounting the committed `otel-collector-config.yaml` + the bundle's mTLS
material), and the **agent** (persistent buffer volume, **no inbound ports**). `bundle_lib.py`
is the agent-bundle contract + fail-closed `validate_bundle()` (required files, manifest
keys, tier enum, C1 cert-SAN round-trip via `ca/ca_lib`, trust-root parse, **I6** signature
verify of license + config via `signing/verify_bundle`, expiry, tenant-binding).

**Use.**
```bash
python installer/make_sample_bundle.py ./sample-bundle   # local: real ephemeral-CA sample
./installer/install.sh --dry-run ./sample-bundle         # validate only
./installer/install.sh ./sample-bundle                   # validate → render → register → up
./installer/uninstall.sh ./sample-bundle
```

**Caveats.** Minimal subset, not the full data plane — absent worker targets surface as
`up==0` (the G5 signal); run the root `docker-compose.yml` on `dp-net` for the full fleet.
The **sample bundle uses an ephemeral CA + signing key** (a fixture); a production bundle is
minted by onboarding with the real fleet CA + signing key (same shape, different trust
roots). `client.key` is a secret that stays in the VPC. **Production path is Helm/Terraform**
driven by the same bundle contract (cert/key → K8s Secret, `bundle.json` → `values.yaml`,
boundary as sidecar/DaemonSet, license/config as a verified ConfigMap).

---

## Ship / operate components

### `release/` — signed release bundles + canary→fleet rollout (P5, FR-D / I6)

**What it does.** The ship machinery. `build_release.py` packages a source tree into a
**deterministic** versioned tarball (`fyralis-release-<v>.tar.gz` — sorted entries, pinned
mtime/uid, stable sha256), emits a `*.release.json` manifest, and **signs** the tarball
(detached `.sig` + C2 manifest, `artifact=release`), **excluding** `*.private.pem`/keys/
`*.pyc` so a release never ships a key. `publish.py` is the on-disk release registry
(`<registry>/<version>/` + `index.json` with a `latest` pointer) that **re-verifies before
publish** and a FastAPI server (`serve`, container 8090 → host **8091**) exposing bundles
in exactly the layout the agent's `config_pull`/`verify_bundle` consumes. `rollout.py` is
the **canary→fleet controller**: reads the fleet from the console, deterministically selects
a canary subset (lowest deployment_id first, always leaving a gating remainder), promotes
it, **watches** its health, **halts on any non-green drift / window expiry** and rolls the
canary back, and promotes the fleet only after a clean watch.

**Use.**
```bash
python release/build_release.py build --src ./dataplane --version 1.4.3 --out ./_dist
python release/build_release.py verify ./_dist/fyralis-release-1.4.3.tar.gz
python release/publish.py publish ./_dist/fyralis-release-1.4.3.tar.gz --registry ./_registry
python release/rollout.py promote --console http://localhost:8080 --version 1.4.3 \
  --canary-count 1 --watch-seconds 30 --poll-seconds 3
```

**Caveats.** The production CP signing key is empty until `signing/keygen.py --activate`
(the self-test/CI sign with an ephemeral key store, write-disjoint from `signing/`).
Promotion delivers a **version** + moves `latest`; the per-deployment signed-config
**byte** delivery is config-dist's job via the injected `Promoter` seam (signing is never
bypassed). Heartbeat-freshness alone won't catch a release that stays green but misbehaves
under real traffic — pair with fleet-SLI burn. Deterministic canary = first-by-id. The
registry server is unauthenticated on its own (bundles are signed, so authenticity does not
depend on transport; front availability with the proxy; path traversal is blocked).

### `config-dist/` — signed config distribution (P5, FR-C3/C4/D4)

**What it does.** Serves each data plane its **signed per-deployment config bundle**
(feature flags + `telemetry_tier` (C3) + token-rotation schedule (FR-D4)). FastAPI/uvicorn
on **8090**, cp-net. `store.py` is per-deployment, versioned, ed25519-signed persistence:
publishing a tier change / flag flip / rotation edit **appends a new immutable signed
version and advances HEAD — no redeploy**. All signing delegates to `control-plane/signing`
via a write-disjoint `SigningHome`.

**Agent-pull contract.** Point the agent's `AGENT_CONFIG_URL` at
`http://config-dist:8090/config/<deployment_id>` (behind the proxy in prod). The agent GETs
the trio (`<url>`, `<url>.sig`, `<url>.manifest.json`) and applies only if the ed25519
signature verifies, the `key_id` is known and not retired, and `artifact == "config"`. Old
versions are immutable and keep verifying (`/config/<id>/v<N>` for pinning/rollback).
`GET /trust_root.json` exports the public verifier keyring.

**Use.**
```bash
python config-dist/publish_config.py acme-use1-7f3a --tenant-id acme --tier T2     # T1→T2, new signed version
python config-dist/publish_config.py acme-use1-7f3a --tenant-id acme --flag anomaly_detection_enabled=true
python config-dist/publish_config.py acme-use1-7f3a --tenant-id acme --rotation interval_hours=12
```
Env: `CONFIG_DIST_STORE_ROOT`, `CONFIG_DIST_SIGNING_HOME`, `CONFIG_DIST_KEY_ID`.

**Caveats.** **Service-local trust root by default** (mints its own key into a config-dist
signing home; the agent must pin **this** service's `GET /trust_root.json`). To chain to one
CP trust root, mount a shared keystore + set `CONFIG_DIST_SIGNING_HOME`/`_KEY_ID`. No
authn/authz at the service (proxy's job; do not expose 8090 publicly). No per-tenant scoping
of the pull path yet (the config carries no secrets/PII by design — flags + tier + rotation
schedule; cross-tenant fetch prevention is the proxy's job). Publishing is serialized per
process (one writer). `token_rotation` is a **schedule** distributed to the agent, not the
rotation itself.

### `metering/` — signed Tier-1 usage metering / billing rollup (P5, FR-F2/F3)

**What it does.** Turns aggregate **Tier-1** metrics into a per-tenant, **signed,
tamper-evident** usage rollup for billing. Reads central Mimir one tenant at a time
(`X-Scope-OrgID: <tenant>`), computes period deltas via `increase()`, **signs** the rollup
(ed25519), and exports it (CSV/JSON).

**The three T1 counters (I1: aggregate only).** `writer_full_mode_writes_total{source}` →
obs-per-source + ingestion_volume; `think_runs_total` → think_runs;
`think_cost_recent_usd_total` → think_cost_usd. A missing series → 0 (a valid zero bill);
negative `increase()` extrapolation clamped to 0. The signed rollup is the C2 trio
(`rollup.json{,.sig,.manifest.json}`) — any later edit to a usage number breaks
verification (FR-F2). `export.py` verifies every bundle first and **refuses** (fail-closed)
any whose signature doesn't validate.

**Use.**
```bash
PYTHONPATH=metering:signing python metering/rollup.py acme --month 2026-06 \
  --mimir-url http://localhost:9009 --out-dir /tmp/billing/acme-2026-06 --verify
PYTHONPATH=metering:signing python metering/export.py /tmp/billing/acme-2026-06 \
  --format csv --out /tmp/billing/2026-06.csv
```

**Caveats.** Aggregate T1 only — no PII (the `source` label is a connector name). The job
sets `X-Scope-OrgID` itself (a trusted CP-internal reader; point `MIMIR_URL` at the proxy
for defense in depth). A deployment down for part of the period under-counts; a
never-reporting deployment yields a valid all-zero bill (cross-check the C4 heartbeat to
distinguish "no usage" from "no telemetry"). `think_cost_usd` is the data plane's
**self-reported** spend (reconcile against the provider bill if required). JSON floats —
apply cents-precise rounding downstream.

### `audit/` — append-only hash-chained audit log + break-glass (P5, FR-G / I5)

**What it does.** The tamper-evident record of who did what, plus the **break-glass**
emergency-access workflow.

**Audit log.** `audit_log.py` is a JSONL file where every entry carries `prev_hash` +
`entry_hash = sha256(canonical_json(body))`; the genesis `prev_hash` is `"GENESIS"`.
`append()` only ever `O_APPEND`-writes one fsync'd line. **Two tamper layers:** (1) the
hash chain detects an edited past entry and a broken link (`verify_chain()` returns
`bad_seq`); (2) a **signed ed25519 checkpoint** over the chain-head hash
(`<log>.checkpoint.json`) defeats a whole-file rewrite (the attacker can't re-sign the new
head without the private key). Tamper-**EVIDENT**, not tamper-PROOF.

**Break-glass (I5: customer-granted, scoped, time-boxed, audit-logged).**
`breakglass.py` is a state machine: `request_grant` (**INERT**) → `approve_grant` (the
distinct **customer** step that starts the time-box, `expires_at = approval + ttl`) →
`check_access` (honors only approved + unexpired + in-scope grants; exact or opt-in
`tenant:x/*` wildcard) + deny/revoke/sweep. Every transition (request/approve/deny/**use**/
expire/revoke/denied-check) is appended to the hash-chained log; expiry is lazy +
idempotent (exactly one `expire` event per grant).

**Use.**
```bash
python audit/cli.py audit append --actor ops@fyralis --action config.apply --target acme --meta '{"v":7}'
python audit/cli.py audit verify                     # exit 0 = chain OK, 1 = tampered (prints bad seq)
python audit/cli.py breakglass request --actor sre@fyralis --scope tenant:acme/logs:read --ttl 900 --reason inc-1
python audit/cli.py breakglass approve --grant-id bg-xxxx --approved-by acme-admin@acme.com
python audit/cli.py breakglass check   --actor sre@fyralis --scope tenant:acme/logs:read
```

**Caveats.** Whole-file evidence needs the active CP private key mounted read-only
(chain-only mode reports `signature_ok=None`). The checkpoint signs the **current head
only** — truncation-to-a-prior-signed-head needs an external monotonic witness (a v2 item).
Single-writer. Wall-clock expiry with no skew grace (fails toward expiring sooner).
**Approver identity is NOT authenticated in the MVP.** `approve_grant` accepts any
`approved_by` string and does not verify that the approver is the customer principal
owning the tenant in the grant's scope, so the audit trail records *who-claimed-approval*,
not *who-actually-approved*. The grant is genuinely scoped + time-boxed + audit-logged, but
authenticating the approver and **binding approver identity to the grant's tenant** is a
documented **next-sprint** item (delegated to the console / auth-proxy, which do not yet
exist for this plane) — see `LIMITATIONS.md` L-11.

### `upgrade/` — zero-disruption CP upgrade tooling (P5/P6, NFR-6 / FR-A5)

**What it does.** The procedure + tooling to upgrade/migrate the control plane **without
disrupting the fleet**, leaning on **I3** (the agent buffers, so a brief CP gap is invisible
to the DP). `trust_bundle.py` is the load-bearing CA trust-**overlap** helper
(`add`/`remove`/`list`/`verify`/`sign` the proxy's `ca/pki/ca-chain.crt`): `add` appends a
new CA so the proxy trusts {old,new} simultaneously during a cutover (idempotent by SHA-256
fp, atomic write + timestamped `.bak`); `remove` refuses to empty the bundle; `verify
--leaf` proves a leaf chains using the same committed `ca/verify_chain.py` the proxy uses;
`--sign`/`--require-signature` reuse `signing/` (I6). `rolling_upgrade.sh` does a
health-gated, one-at-a-time rolling restart of the **stateless** CP services (pre-gate →
recreate → post-gate poll → auto-rollback; `DRY_RUN`/`NO_PULL`/`NO_ROLLBACK`; gracefully
skips not-yet-wired services; **refuses** stateful mimir/loki/grafana with exit 2 →
blue-green). `trust_overlap.sh` wraps the add-before-rotate / remove-after dance.
`UPGRADE_RUNBOOK.md` is the full procedure.

**Use.**
```bash
DRY_RUN=1 ./upgrade/rolling_upgrade.sh        # then without DRY_RUN for the real roll
./upgrade/trust_overlap.sh add --new-ca ca/pki-new/ca-chain.crt
./upgrade/trust_overlap.sh verify --leaf /path/to/existing-agent-leaf.crt
./upgrade/trust_overlap.sh remove --root-cn "Fyralis Root CA"   # after all agents rotated
```

**Caveats.** `shellcheck` absent in the build env (fell back to `bash -n`). A live roll
needs the full stack up. Blue-green for Mimir/Loki assumes **shared object storage** in prod
(on dev local volumes prefer the rolling stateful variant). `--sign` needs the CP keyring.
The trust-overlap *ordering* is operator-enforced (the helper only enforces the
never-empty-the-bundle backstop) — confirm the fleet has fully rotated via the console
before `remove`.

### `self-obs/` — control-plane self-observability (P6, NFR-10)

**What it does.** The inside-out watchdog: **"the control plane monitors itself; silence !=
health."** `cp_exporter.py` actively probes every CP service each scrape — **auth-proxy via
a TLS handshake** (it is mTLS-only with no unauthenticated `/healthz`), mimir/loki via
`/ready`, grafana `/api/health`, console/config-dist/release `/healthz` — and exposes
`cp_service_up`, probe latency, last-success timestamps, the **ingest-path-alive synthetic**,
and a **scrape heartbeat** so silence itself is alarmable. A **dedicated, independent**
`cp-prometheus` (separate from the fleet/Mimir path so it survives a fleet-pipeline outage)
loads `cp_rules.yml` (25 rules incl. `absent()`-based silence twins). An 11-panel
`cp_self.json` Grafana dashboard renders it.

**Surfaces.** Exporter `http://localhost:9110/metrics`; CP Prometheus
`http://localhost:9091`. The ingest synthetic runs `structural` by default (confirms both
ingest endpoints alive without pushing a byte) or `fullpush` when a tenant client cert is
mounted (a real authenticated request **through** the proxy proving the complete control
path).

**Caveats.** auth-proxy is **intentionally not** a direct Prometheus target (mTLS-only) —
its liveness comes solely from the exporter probe; do not add an `auth-proxy:8443` scrape
job. The TLS handshake probe is a *liveness* probe, not a cert-identity check (a handshake
that fails because the proxy demanded a client cert is correctly read as **up**). Probes
run on each scrape, serially (bounded by `scrape_timeout`). Short 15d retention (a watchdog,
not a warehouse).

---

## Demo & test components

### `demo-dataplane/` — golden-12 SLI metrics stub (P6)

A stdlib-only HTTP server exposing the golden-12 `fyralis_*` families on `:9300` so the
**testable** bring-up has a realistic boundary scrape target without the real data plane.
`DEMO_DP_SCENARIO=healthy|degraded` flips a few SLIs into the yellow/red band to demo the
fleet roll-up colour + alerts. **Demo fixture only** — do not ship to a customer; production
points the boundary at the real targets.

### `tests/` — end-to-end smoke + integration suite (P6)

The **CTO smoke**: `make smoke` wires the **committed** components in-process (no stubs) and
asserts the full BYOC path in seven steps — bootstrap a throwaway CA + trust root in /tmp;
onboard → bundle + registry row + console listing; agent green + heartbeat → console GREEN;
push a metric **as acme** through the **real** auth-proxy over a genuine mTLS socket →
MockMimir (proxy injects `X-Scope-OrgID:acme` from the cert SAN); **isolation** (query as
globex → 0 series; a client-set `X-Scope-OrgID` is overridden, I4); license tamper → agent
refuses the privileged pull (I6); config-dist signs → agent verifies-before-apply, rejects a
tampered config (I6). Result: `e2e_smoke.py` **52 passed / 0 failed / 3 docker-only skips**;
`test_e2e.py` **6 passed / 1 live-docker skip**. Mimir is the only no-docker substitution
(MockMimir reproduces the exact `X-Scope-OrgID` multitenancy contract). `make live` /
`--live-docker` runs the metric round-trip against the real Mimir image.

---

## Feature catalog — FR / invariant → where it lives → status

Status legend: **done** = built + self-test/suite green in this checkout; **partial** =
built with a documented caveat / dev-only path; **next-sprint** = a deliberate v2 hook.

| FR / Invariant / NFR | Capability | Lives in | Status |
|---|---|---|---|
| **C1 / I4** | cert→tenant from verified SPIFFE SAN, server-side | `ca/`, `auth-proxy/tenant_resolver.py` | **done** (adversarial A1–A12 green) |
| **C1** | fail-closed fingerprint revocation registry | `ca/registry.py`, `ca/tenant_registry.json` | **done** (no CRL/OCSP — registry lookup) |
| **C2 / I6** | ed25519 sign + verify-before-apply, rotation by key_id | `signing/` | **done** (keys-on-disk dev-only; KMS = prod) |
| **C3 / I1** | telemetry tiers, two-gate boundary redaction (zero PII at T1) | `boundary/`, `lib/tiers.py` | **done** (T1 validated vs real collector; T2/T3 config-only) |
| **C4** | one-row-per-deployment registry + derive-health | `lib/deployment.py`, `console/` | **done** |
| **C5** | cp-net networking + `X-Scope-OrgID` multitenancy | `mimir/`, `loki/`, `grafana/`, compose | **done** |
| **I2** | outbound-only agent, no inbound to the VPC | `agent/` | **done** (no-listener proven 3 ways) |
| **I3** | data plane survives a CP outage (buffer + retry) | `agent/buffer.py`, `installer/`, `upgrade/` | **done** |
| **I5** | break-glass: customer-granted, scoped, time-boxed, audited | `audit/breakglass.py` | **done** |
| **R1 SSRF (T12)** | upstream-pin / origin-form-only request target | `auth-proxy/proxy.py:_safe_upstream_path` | **done** (was a gating defect; **fixed**, A12 asserts host never reached) |
| **FR-A5 / NFR-6** | non-disruptive CA rotation + zero-disruption CP upgrade | `upgrade/` | **partial** (stateful blue-green assumes prod object storage) |
| **FR-C3/C4/D4** | signed per-deployment config dist; tier/flag/rotation as new version | `config-dist/` | **done** (service-local trust root by default — pin it) |
| **FR-D / I6** | signed deterministic releases + canary→fleet rollout w/ halt-on-drift | `release/` | **partial** (byte-delivery via the config-dist `Promoter` seam) |
| **FR-E** | atomic onboard transaction + rollback; offboard | `onboarding/` | **partial** (no DELETE verb on a *real* console → reap-on-age) |
| **FR-F** | signed expiring licenses + fail-closed validator + revocation | `licensing/` | **done** (revocation is pull-based by design) |
| **FR-F2/F3** | signed Tier-1 usage metering + billing export | `metering/` | **done** (self-reported cost; reconcile w/ provider) |
| **FR-G / I5** | append-only hash-chained + signed-checkpoint audit log | `audit/` | **done** (tamper-EVIDENT, not -PROOF — WORM sink = prod) |
| **NFR-5** | derived health (fresh/stale/missing) + SLO burn alerts | `console/`, `fleet-sli/` | **done** |
| **golden-12 SLIs** | per-deployment + fleet recording/alert/SLO rules | `fleet-sli/` | **partial** (G1/G2/G3/G5 gap-metrics need DP wiring) |
| **NFR-10** | CP self-observability, "silence != health" | `self-obs/` | **done** |
| **P6** | one-command compose bring-up + e2e smoke | `bootstrap.sh`, `docker-compose.control-plane.yml`, `tests/` | **done** (52/0/3) |
| **installer / prod** | single-host installer (Helm/Terraform = prod) | `installer/` | **partial** (minimal DP subset; Helm/TF is next-sprint) |
| **config hot-reload** | agent applies verified config without restart | `agent/config_pull.py` | **next-sprint** (file-drop today; reload hook stubbed) |
| **trust-root unification** | one CP trust root across release/license/config | `signing/`, `config-dist/` | **next-sprint** (config-dist mints its own by default) |
| **signing custody** | KMS/HSM signing | `signing/` | **next-sprint** (Keyring structured for a remote signer) |
| **audit anti-truncation** | external monotonic witness for the head | `audit/` | **next-sprint** |
