# BYOC Control Plane — Build Log

Append-only journal of build progress. Each integrate step appends one or more entries under the
relevant phase heading. Keep entries terse and dated; newest within a phase goes at the bottom.

## Phase 1 — Trust roots

- 2026-06-24 — Scaffold complete. Created `control-plane/` structure (20 component dirs each with a
  `.gitkeep`), the authoritative `SPRINT_PLAN.md` (with the shared CONTRACTS section: cert→tenant SAN,
  ed25519 signing, telemetry tiers, deployment record, compose networking, invariants I1–I6), this
  `BUILD_LOG.md`, the `docker-compose.control-plane.yml` skeleton (`cp-net` network + commented
  per-phase service placeholders), `requirements.txt`, `.gitignore`, and `README.md`. Trust-root
  implementation (`ca/`, `signing/`, `lib/`) is owned by the P1 build agent and lands next.

- 2026-06-24 — **WS-CA** — Private CA + per-tenant mTLS identity (`control-plane/ca/`). Real working
  CA on the `cryptography` lib (no `step` binary needed for the testable path): P-256 root→intermediate
  →clientAuth-only tenant leaf carrying the SPIFFE URI SAN `spiffe://fyralis/tenant/<id>` (Contract C1),
  `extract_tenant_from_cert` derives tenant id server-side from the verified SAN (Invariant I4, never a
  header). Fingerprint-keyed, fail-closed revocation registry. Chain verifier prefers cryptography's
  native `ClientVerifier` with a real manual fallback. CLIs: `bootstrap_ca`, `issue_cert`, `revoke`.
  Key files: `ca_lib.py`, `verify_chain.py`, `registry.py`, `bootstrap_ca.py`, `issue_cert.py`,
  `revoke.py`. Verified: `selftest.py` all gates pass + `test_ca.py` 18 passed. step-ca production
  config documented in `config/ca.json`.

- 2026-06-24 — **WS-SIGNKEYS** — ed25519 supply-chain signing (`control-plane/signing/`). Implements
  Contract C2 / Invariant I6 (verify-before-apply). `signing_lib.py` core: keypair gen, detached
  sign/verify, a `Keyring` with exactly one active signer + retained retired verifiers, rotation by
  key_id, and canonical-JSON signed-bytes. CLIs: `keygen` (writes `keys/` 0600 + public `trust_root.json`),
  `sign_bundle` (`<file>.sig` + `<file>.manifest.json`), `verify_bundle` (rejects retired keys by default).
  `rotation.py` demonstrates back-verify of old artifacts under a retained key. Verified: `selftest.py`
  10/10 pass, `rotation.py` 8/8 pass. Custody note: keys-on-disk is dev-only; KMS/HSM is the prod path
  (Keyring structured to swap in a remote signer without changing the manifest or verifier).

- 2026-06-24 — **shared-lib** — Tenant/tier/deployment models (`control-plane/lib/`, pydantic v2 +
  structlog). `tenant.py` (C1): read-only `TenantRegistry` over `ca/tenant_registry.json`, fails closed
  with typed errors (TenantNotFound/Revoked/Inactive) + mtime/size cache so out-of-band revocations are
  picked up live. `tiers.py` (C3): cumulative T1/T2/T3 `TelemetryTier` policy table (T1 = zero PII, I1).
  `deployment.py` (C4): `DeploymentRecord` on the exact wire shape + `derive_health` (green/yellow/red).
  `config.py` frozen `ControlPlaneConfig` from `CP_*` env. `primitives.py` (P1): RFC-3339 time,
  `canonical_json_bytes` (the C2 signed-bytes definition), DER/PEM SHA-256 fingerprints. `errors.py`,
  `logging.py`. Verified: `python -m lib._selftest` all 40 assertions green. **Note: added `pydantic`
  to `requirements.txt`** (lib's runtime dep was missing).

## Phase 2 — Linchpin (auth proxy + boundary)

- 2026-06-24 — **WS-AUTHPROXY** — Tenant auth proxy (`control-plane/auth-proxy/`, Invariant I4 / risk
  R1). REAL asyncio + h11 HTTP/1.1 mTLS-terminating reverse proxy (chosen over uvicorn/FastAPI for
  direct verified-DER-peer-cert access via `getpeercert(binary_form=True)` and byte-level header
  control; httpx forwards upstream). `proxy.py` builds an `ssl.SSLContext` with `CERT_REQUIRED` +
  the Fyralis CA chain so the handshake itself demands a chaining client cert; per request it STRIPS
  any inbound `X-Scope-OrgID` (all casing/prefix variants) + hop-by-hop headers and INJECTS the
  server-derived `X-Scope-OrgID=<tenant_id>` before reverse-proxying to Mimir (default
  `http://mimir:9009`); any rejection → flat 403, never forwarded, no 5xx leak. `tenant_resolver.py`
  is the fail-closed core (verify → extract → revoke → SAN-vs-registry agreement) and REUSES the
  WS-CA primitives exactly: `verify_chain`, `extract_tenant_from_cert` (tenant id read ONLY from the
  verified SPIFFE URI SAN), `fingerprint_sha256`, `is_revoked` (revoked OR unknown → 403). `config.py`
  loads `AUTH_PROXY_*` env and `require_files()` fails loudly at boot; the registry is re-read fresh
  per request so revocation is immediate. Reconciled the known inconsistency: `lib/tenant.py`
  `TenantRegistry.is_revoked()` was fail-OPEN (unknown → False) — made it fail-CLOSED to match
  `ca/registry.is_revoked` (unknown OR non-active → True), updated the stale assertion in
  `lib/_selftest.py`, and added `lib/test_tenant_failclosed.py` cross-checking that `lib.tenant` and
  `ca.registry` now give identical deny answers. Verified: `tests/` 18 passed, out-of-process
  `selftest.py` all green (9 checks incl. header-smuggle strip + two-tenant isolation),
  `lib/test_tenant_failclosed.py` 6 passed, `lib/_selftest` regression green. **Note: added `h11`
  to `requirements.txt`** (proxy runtime dep). Compose follow-up: the auth-proxy build context must
  be the control-plane ROOT (so `ca/` is in the image) — `build.context: .` +
  `build.dockerfile: auth-proxy/Dockerfile` (documented in `auth-proxy/README.md`).

- 2026-06-24 — **WS-AUTHPROXY-SEC** — Gating security review of the auth proxy
  (`control-plane/auth-proxy/security/`, Invariant I4 / risk R1). `THREAT_MODEL.md`: STRIDE T1–T13
  (header tenant spoofing, revoked/expired cert, SAN forgery, unknown-cert fail-open, header
  smuggling/duplication, upstream SSRF, error-leak, TLS downgrade, SAN↔registry mismatch,
  registry-unreadable), each citing the in-code control by file:line + residual risk, with a
  roll-up register and gate verdict. `test_isolation.py`: REAL adversarial suite (boots the actual
  AuthProxy + a recording echo upstream + an in-process Fyralis CA AND an adversary CA), 12 attacks
  A1–A12 incl. all 7 required isolation attacks (valid→scope, forged-header override, revoked,
  unknown, no-cert, foreign-CA, duplicate/case/prefix smuggle) plus foreign-CA SPIFFE forgery,
  SAN↔registry mismatch, kitchen-sink smuggle, two-tenant interleave, SSRF. `SAST.md` + raw
  `bandit_report.json`: bandit 1.9.4 over the proxy source (0 High / 1 Medium intended `0.0.0.0`
  bind / 3 Low) plus a manual injection/SSRF/error-leak/fail-open checklist (F1–F5). Verified:
  `security/test_isolation.py` 11 passed + 1 xfailed (A1–A11 green — upstream never sees a
  cross-tenant or client-controlled org id), `tests/` still 18 passed. **Note: added `bandit` to
  `requirements.txt`** (SAST tooling). **GATING DEFECT carried forward: HIGH-severity SSRF (A12,
  SAST F1, T12) is confirmed exploitable** — a holder of any valid active tenant cert can re-point
  the upstream to an arbitrary host (e.g. `169.254.169.254`) via an absolute-form request target
  (`proxy.py` forwards `request.target` verbatim; httpx lets an absolute target override the pinned
  base_url). Tenant SCOPING is intact; network containment is NOT. A12 is `xfail(strict=False)` so
  it regression-guards the live exploit. FIX REQUIRED before sign-off: reject non-origin-form targets
  (must start with `/`) or rebuild the upstream URL from base_url + sanitized path, then flip A12 to
  assert the internal host is never reached.

- 2026-06-24 — **WS-BOUNDARY** — Boundary OTel Collector (`control-plane/boundary/`, Invariant I1).
  Runs inside the customer VPC, enforces zero PII/payload egress at Tier 1, and does metrics
  remote-write through the auth proxy. `otel-collector-config.yaml` (T1): a `prometheus` receiver
  scraping the data-plane targets (workers :9300, gateway :8000, pg/kafka/redis/minio exporters,
  incl. anomaly-processor/deadline-resolver so the G5 coded-but-not-running gap surfaces as up==0);
  TWO redaction gates — `filter/allowlist` (OTTL default-deny keeping ONLY the golden-12 + G1–G7
  fleet families) and `transform/redact-labels` (OTTL delete_key dropping ~70 high-cardinality/PII
  labels, keeping only bounded enums); a `resource` processor adding C4 deployment identity
  (tenant_id/deployment_id/region/telemetry_tier from env); and a `prometheusremotewrite` exporter to
  the proxy `/api/v1/push` over mTLS that deliberately does NOT set `X-Scope-OrgID` (the proxy injects
  it from the verified cert SAN per C1/I4). `tier_policy.yaml`: cumulative T1/T2/T3 increments
  (config-only pipeline-block swaps; above-tier signals have no receiver/exporter so they physically
  cannot egress — C3 by absence). `redaction_allowlist.md`: the auditable I1 artifact (Gate-1 keep
  list → golden-12/G1–G7, Gate-2 drop list + enum allowlist, worked auditor example).
  `dataplane_remote_write.md` + `prometheus_remote_write_overlay.yml`: the direct data-plane
  Prometheus `remote_write` alternative reproducing both gates via `write_relabel_configs`. Verified:
  `selftest.py` 55/55 (incl. a REAL `otelcol-contrib validate` of the T1 config via docker, exit 0);
  a merged cumulative T3 also validates clean against the real collector. The real-collector run
  caught + fixed a genuine bug: the T3 traces OTLP endpoint had a URL-plus-extra-port → split into a
  dedicated `${FYRALIS_AUTH_PROXY_GRPC}` host:port var. Compose follow-up: the compose mounts
  `./boundary/config.yaml` but the deliverable is `otel-collector-config.yaml` — symlink/copy or
  update the mount at deploy time (documented in `boundary/README.md`).

## Phase 3 — Central stores + fleet SLIs

- 2026-06-24 — **WS-MIMIR + WS-MIMIR-CARD** — Central multi-tenant metrics store (`control-plane/mimir/`,
  Grafana Mimir 2.13.0). `mimir.yaml`: `multitenancy_enabled: true` (every request needs X-Scope-OrgID;
  anonymous → 401), `target: all` (monolithic distributor/ingester/querier/query-frontend/ruler/
  compactor/store-gateway), filesystem blocks + ruler + alertmanager under `/data`, remote-write RECEIVE
  via the distributor (`POST /api/v1/push` + OTLP), HTTP on 9009 (the auth-proxy upstream). `limits`
  holds the per-tenant cardinality budget DEFAULTS (max_global_series_per_user 150000, ingestion_rate
  25000/burst 50000, max_label_names_per_series 30 + per-metric/query guardrails); `runtime_config`
  hot-reloads `runtime_overrides.yaml` every 15s for per-tenant overrides (worked examples: acme 500k,
  globex 50k, the `__fleet__` ruler tenant). `cardinality.md` is the MEASURE-then-ENFORCE method
  (cardinality analysis APIs + 4xx series-cap / 429 rate backpressure). Verified: `validate.py` exit 0,
  real `grafana/mimir:2.13.0` boot clean (caught two config bugs the binary rejects: `auth_enabled` is
  not a Mimir key — `multitenancy_enabled` is its successor; per-tenant override config belongs under
  `runtime_config`, not `limits`), and an 8/8 contract suite against a live cluster (no header → 401,
  with header → 200; runtime overrides applied; ruler evaluating all fleet-sli groups under `__fleet__`).

- 2026-06-24 — **WS-LOKI-T2** — Tier-2 central log store (`control-plane/loki/`, Grafana Loki 3.4.2,
  `-target=all`). `loki.yaml`: `auth_enabled: true` (C5 — X-Scope-OrgID required on every request),
  filesystem storage local under `/data` (TSDB schema v13, chunks/index/compactor/ruler WAL on
  `loki-data`), compactor-owned retention 744h (31d), per-tenant `limits_config` (ingestion 8MB/16MB,
  per_stream 3MB/8MB, max_streams 10000, max_label_names 30, max_line 256KiB, reject_old 168h),
  `runtime_config` hook for hot-reloaded per-tenant overrides (`overrides/loki-overrides.yaml`, empty by
  default), analytics reporting off. Trust boundary (README): T2 logs arrive ALREADY REDACTED — the
  boundary OTel Collector strips PII before egress (I1); Loki is the sink, not the redactor. Verified:
  yaml.safe_load assertions green, real `grafana/loki:3.4.2 -verify-config` → "config is valid" exit 0,
  `docker compose config` exit 0.

- 2026-06-24 — **central Grafana** — Operator Grafana provisioning (`control-plane/grafana/`,
  grafana 11.1.0). 4 provisioned datasources (`provisioning/datasources/datasources.yaml`): `Mimir`
  (prometheus type, `http://mimir:9009/prometheus`, isDefault) + `Loki` (`http://loki:3100`) carry
  `X-Scope-OrgID: ${tenant_scope}` (templated per-customer scope) and `Mimir (fleet)` + `Loki (fleet)`
  carry `${FYRALIS_FLEET_ORG_ID:__fleet__}` for cross-fleet reads — `access: proxy` so the scope header
  attaches inside cp-net and never reaches the browser. Datasources point DIRECTLY at Mimir/Loki over
  cp-net (the trusted OPERATOR QUERY PATH, distinct from the agent mTLS INGEST PATH through the
  auth-proxy — the operator side has no per-tenant client cert). Two dashboard providers →
  `fleet/fleet-overview.json` (uid fyralis-fleet-overview: green/yellow/red deployment census + worst
  heartbeat age + a health-colored deployments table + golden-12 fleet panels) and
  `tenant/tenant-drilldown.json` (uid fyralis-tenant-drilldown: templated by the `tenant_scope`
  variable = the X-Scope-OrgID value, hard-scoping every panel to one customer + a Loki logs panel).
  Health derived at query time from `worker_heartbeat_age_seconds` (green ≤90s/yellow ≤300s/red >300s)
  + SLI flags, using only boundary-allowlist metric names with C4 labels. Verified: `validate.py`
  ("ALL CHECKS PASSED") asserts X-Scope-OrgID on all 4 datasources, the per-customer dashboard declares
  `tenant_scope`, dashboards reference only provisioned DS uids, and the fragment exposes :3000 on
  cp-net+dataplane-net / depends_on mimir+loki.

- 2026-06-24 — **WS-FLEETSLI** — Fleet-level SLI/alert/SLO rules (`control-plane/fleet-sli/`), evaluated
  CENTRALLY in the Mimir ruler under the synthetic `__fleet__` tenant over every tenant's remote-written
  metrics; every series is per-deployment via the C4 `tenant_id`/`deployment_id`/`region` labels.
  `recording_rules.yml` (58 rules, 10 groups): golden-12 SLIs both PER-DEPLOYMENT (`fyralis:*`) and
  FLEET-WIDE (`fleet:*` roll-ups) — worker up/heartbeat, kafka lag, DLQ/dead-letter, ingest/backfill,
  shadow-drop silent-loss, think queue/failure, embedding backlog/failure, LLM breaker + $/hr, DB pool +
  schema version + partition coverage, OAuth/webhook/gateway 5xx — plus `fyralis:health_code` (0/1/2
  matching `lib/deployment.py derive_health`) and a fleet red/yellow census. `alert_rules.yml` (17): all
  13 deployment alerts ported to fleet scope (per-deployment annotations) + shadow-drop page +
  llm-breaker-open, with G1/G2/G3/G5 gap-metric alerts (schema drift, OAuth refresh, breaker, worker
  missing). `slo_burnrate_rules.yml` (11) + `slo.md`: NFR-5 SLOs as Google-SRE multi-window
  multi-burn-rate alerts (availability 99.5%/0.5% budget — fast 14.4x@5m+1h page within 2 min, slow
  6x@30m+6h ticket; liveness heartbeat>90s/30s page). Verified: `promtool check rules --lint=all`
  exit 0 (88 rules across the 4 files), a real Prometheus loaded all 22 groups and evaluated every rule
  with health==ok / zero lastError (catches eval-time join/bool errors a static parse misses).

- 2026-06-24 — **integrate** — Merged each dir's `service.compose.yml` into
  `docker-compose.control-plane.yml`: filled in the `mimir`, `loki`, `grafana` services (replacing the
  Phase-3 placeholder comments) on `cp-net`, wired the `mimir-data`/`loki-data`/`grafana-data` named
  volumes and `cp-net`+`dataplane-net` networks (already top-level-declared), grafana `depends_on`
  mimir+loki and attaches to both networks. The fleet-sli rules are loaded by the **`mimir-ruler-loader`
  one-shot (mimirtool `rules load` → ruler API, tenant `__fleet__`)** mounting `./fleet-sli -> /rules`:
  this is the authoritative ruler path — Mimir's filesystem ruler backend does NOT auto-discover a
  directory of multi-group YAML, so the WS-FLEETSLI `fleet-sli-ruler-bootstrap` busybox disk-copy was
  intentionally NOT carried over (its copies would yield "no rule groups found"). The mimirtool loader
  globs BOTH `*.yml` and `*.yaml`, so it covers the WS-FLEETSLI `*.yml` deliverables and resolves the
  glob-mismatch caveat the fleet-sli agent flagged. Auth-proxy / boundary / console placeholders left
  intact (still commented). No new Python deps (all Phase-3 components are container images; the
  `validate.py` scripts use only stdlib + already-present `pyyaml`) — `requirements.txt` unchanged.
  Verified: `docker compose -f docker-compose.control-plane.yml config` exit 0 with all 3 new services
  + the ruler loader present; a yaml.safe_load assertion pass over the rendered config (mimir/loki on
  cp-net with config+data mounts, grafana on cp-net+dataplane-net depends_on mimir+loki, ruler-loader
  depends_on mimir mounting fleet-sli→/rules, all named volumes + networks declared).

## Phase 4 — Deployment surface (agent/console/onboarding/licensing/installer)

- 2026-06-24 — **WS-AGENT** — Outbound-only data-plane agent (`control-plane/agent/`, runs in the
  customer VPC; I2/I3/I6). `agent.py` daemon loop: every `AGENT_INTERVAL_S` it collect()s a C4
  `DeploymentRecord` (version from `VERSION`, region/tier/tenant/deployment from config, license_expiry
  from the VERIFIED license, health derived from heartbeat freshness folded with a local /healthz SLI
  probe + license expiry) and POSTs it to `<console>/api/v1/heartbeat` over an OUTBOUND https call.
  `deliver()` flushes any backlog oldest-first then sends live; on an unreachable console it parks the
  record in a durable bounded JSONL `buffer.py` (I3) and retries with capped exponential backoff;
  `run_forever()` is SIGINT/SIGTERM-aware with a try/except backstop so no tick crashes the daemon.
  `config_pull.py` GETs a signed config bundle and runs `signing/verify_bundle.verify_file`
  (ed25519 + key-id policy + sha256) BEFORE atomic apply (I6) — tampered/unknown-key/wrong-kind →
  REJECTED, old config untouched. `license_check.py` verifies the local signed license (sig + kind ==
  `license` + issued_at ≤ now < expires_at), `is_licensed()` re-evaluated every call; the agent refuses
  privileged actions when unlicensed but still heartbeats so the console shows a red, unlicensed
  deployment. `config.py` deliberately has NO listen host/port (I2); no server framework is imported
  anywhere and `requests` is imported lazily. Reuses the committed `lib` (DeploymentRecord/tiers) +
  `signing` via a sys.path `_bootstrap`. Verified: `pytest -q` 37 passed (license verify/expiry/tamper/
  unknown-key, config verify-before-apply, buffer durability/ordering/bounding, agent tick/SLI-yellow/
  expired-red/buffer+flush+backoff/license-gated-pull, a REAL loopback HTTPServer round-trip with
  kill+recover, and the I2 no-listener guard 3 ways — socket.listen trap + /proc/net/tcp LISTEN diff +
  source forbidden-primitive scan); `selftest.py` 11/11, exit 0. Caveats: license/config ISSUANCE is
  upstream (signing + WS-LICENSE) — the agent only CONSUMES signed bundles and ships only the PUBLIC
  trust root; POST /api/v1/register is NOT driven by the loop (deployment_id/tenant_id are pre-provisioned
  by onboarding/installer); health folds ONE local SLI today (the probe is injectable); buffer cap drops
  OLDEST past `AGENT_BUFFER_MAX_RECORDS` (default 10k). **No new dep beyond `requests`** (added at
  integrate).

- 2026-06-24 — **WS-CONSOLE** — Fleet console/registry (`control-plane/console/`, FastAPI/uvicorn on
  8080, cp-net). Implements the P4 CONSOLE REST contract exactly over the committed `lib` C4
  `DeploymentRecord` + `derive_health` (no record redefinition): `POST /api/v1/register {tenant_id?,
  region, plan}` mints `<tenant>-<region>-<rand>` + a tenant_id when absent and stamps an initial green
  heartbeat; `POST /api/v1/heartbeat {DeploymentRecord}` upserts by deployment_id + recomputes health
  (malformed/extra-field/bad-tier/non-object bodies → 422, never 500); `GET /api/v1/deployments`
  (worst-health-first, health derived ON READ) + `GET /api/v1/deployments/{id}` (404 if unknown);
  `GET /` a self-contained HTML fleet rollup (color health badges, last-heartbeat age, license expiry
  with EXPIRED/soon flags, green/yellow/red/total counts); `GET /healthz`. `store.py` is an in-memory
  registry with atomic JSON-file persistence under `console/data/` (`CP_CONSOLE_DATA_DIR`, temp+os.replace,
  corrupt/missing → empty, RLock). Health is ALWAYS re-derived on read (NFR-5: stale >90s → yellow,
  missing >300s → red, expired-license → red, SLI burn → green→yellow, future heartbeat clamped to age 0)
  — the wire `health` is never trusted. Verified: `pytest` 15 passed (register/upsert/404/malformed/
  health-derivation fresh→green/stale→yellow/missing→red/expired→red/HTML-rollup/persistence round-trip);
  spec self-test via TestClient + a REAL uvicorn boot on 8080 (exact C4 wire shape, RFC-3339 Z timestamps);
  `docker compose config` validates; OpenAPI exposes exactly the 5 contract routes + /healthz. **Bug
  found+fixed:** a bad telemetry_tier raised a typed `TierError` from lib's validator (not wrapped into a
  pydantic ValidationError in this pydantic version) → would have 500'd; the heartbeat handler now catches
  any record-construction error and returns 422 (+2 regression tests). Caveats: no auth on the console API
  (sits behind the CP perimeter on cp-net; do not publish :8080 to an untrusted network); `plan` is a
  register hint, not a C4 field (the signed license is authoritative, corrected by the agent's first
  heartbeat); persistence is best-effort single-node JSON (swap for a shared DB past MVP). Deps already
  present (fastapi/uvicorn/pydantic).

- 2026-06-24 — **WS-ONBOARD** — Atomic per-tenant onboarding (`control-plane/onboarding/`, FR-E).
  `onboard.py` CLI runs a 6-step transaction, each registering an undo on a LIFO `RollbackLedger`:
  (1) REGISTER via console `POST /api/v1/register` (or `--local-ids` / `--embedded-console`); (2) ISSUE
  CERT via `ca/issue_cert` — per-tenant mTLS cert with SAN `spiffe://fyralis/tenant/<tenant>` + the active
  row written to `ca/tenant_registry.json` (that row IS the auth-proxy binding); (3) MINT+SIGN LICENSE —
  delegates to `control-plane/licensing.issue_license` when importable, else a self-contained ed25519
  signer via `control-plane/signing`, producing `{tenant_id,deployment_id,plan,issued_at,expires_at,
  features[]}` + detached `.sig` + `.manifest.json` (verify-before-use, I6); (4) ASSEMBLE BUNDLE —
  `bundles/<deployment_id>/` with cert/ (crt+key+chain), the signed license, a signed agent-config.json
  pointing the outbound-only agent (I2) at the console, the PUBLIC `trust_root.json`, and `BUNDLE.json`;
  (5) SEED HEARTBEAT so it appears in the console now; (6) CONFIRM listed. On ANY failure the ledger
  unwinds newest-first (revoke+delete the registry row, rmtree the partial bundle, best-effort console
  remove) — no half-onboarded state. `offboard.py` revokes every active cert for the tenant (proxy 403s
  it) + optional `--purge-registry`/`--purge-bundle`. Reuses committed `ca`/`signing`/`lib` — does not
  redefine DeploymentRecord/signing/CA; the license is wire-compatible with sibling `licensing`. Verified:
  `selftest.py` 31/31 across 5 scenarios against a throwaway CA/signing/registry in tmp (committed
  ca/tenant_registry.json verified still `{}`): happy path (valid SAN==acme cert + sig-verifying unexpired
  license + signed outbound config + shipped public trust root + exactly one ACTIVE acme row keyed by the
  fingerprint + console-listed); rollback on a late and an early forced failure (no orphan bundle/row);
  rollback on a genuine signing-material-absent failure; offboard (cert revoked, deregistered, bundle
  purged); both license paths (delegation + fallback) verify; `docker compose config` exit 0. **Operator
  CLI / one-shot — NOT wired as an always-on service** (its `service.compose.yml` is `ops`-profile-gated;
  run via `docker compose run --rm onboarding ...`). Caveats: the P4 console contract has no DELETE verb
  so against a REAL console rollback can't delete a seeded deployment (left to age to red; embedded/fake
  console supports removal so self-test rollback is exact); needs the signing PRIVATE key to mint;
  assembled bundles carry a tenant private key (git-ignored, deliver over a secure channel).

- 2026-06-24 — **WS-LICENSE** — Signed expiring licenses + a fail-closed validator
  (`control-plane/licensing/`), built entirely on the committed `signing` ed25519 lib (no crypto
  re-implemented). `license_model.py`: the LICENSE JSON contract `{tenant_id, deployment_id, plan,
  issued_at, expires_at, features[], license_id, version}`; `License.mint()` supports a duration or an
  explicit `expires_at`; canonical compact-JSON bytes match `signing_lib` so what we hash is what we sign.
  `issue_license.py` (CLI + lib) builds + signs the doc as a `license` artifact via `signing/sign_bundle`,
  producing the C2 detached trio (license.json + .sig + .manifest.json); exits non-zero if no signing
  key/trust root (never emits an unsigned license). `validator.py` `validate()` is FAIL-CLOSED with four
  gates ALL required for ALLOW: (1) SIGNATURE via `signing/verify_bundle` (verify-before-use I6 — denies
  tampered/unknown-key/retired-key/wrong-artifact), (2) EXPIRY now<expires_at + not-yet-valid guard,
  (3) IDENTITY tenant_id+deployment_id match this deployment (lateral-reuse guard), (4) REVOCATION not on
  the deny list; stable deny codes; corrupt revocation list fails closed (does not silently un-revoke).
  `validate_for_deployment(record)` binds identity from a C4 record so the identity gate is never skipped.
  `revoke.py` is the revocation list (FR-F — revoke a still-signed/unexpired license) with add/remove/
  check/list CLIs over `revocations.json` (`REVOCATIONS_PATH` override). Verified: `selftest.py` 11/11
  ALL GREEN against an isolated throwaway trust root (issue→ALLOW all 4 gates; tamper→deny_bad_signature;
  expired→deny_expired; revoke→deny_revoked; wrong-tenant/wrong-deployment/unknown-key→DENY;
  validate_for_deployment(matching)→ALLOW); end-to-end issue→validate→revoke via CLI; the optional FastAPI
  service via TestClient. **Optional HTTP service — `ops`-style fragment, NOT wired into the master
  compose** (the agent validates LOCALLY and never calls it). **Bug found+fixed:** `revoke.py` bound
  `path=DEFAULT_REVOCATIONS_PATH` as a default arg (frozen at import) so a monkeypatched path was ignored
  on writes while honored on reads → refactored to resolve the path at call time; reset `revocations.json`
  to a clean empty seed. Caveats: revocation is pull-based (propagation = the agent's list-refresh cadence,
  by design — agent is outbound-only/offline-capable); no trust root exists in the repo yet (issuance needs
  `signing/keygen.py --activate` first).

- 2026-06-24 — **WS-INSTALLER** — Single-tenant provisioning bundle/overlay
  (`control-plane/installer/`, customer-VPC tool — NOT a vendor CP service). `deployment.compose.yml` is a
  per-tenant overlay (project `fyralis-dp-<slug>`) bringing up three layers on a shared `dp-net`: a MINIMAL
  runnable subset of the repo-root data plane (postgres+exporter, redis+exporter, kafka+exporter), the
  boundary OTel Collector mounting the committed `otel-collector-config.yaml` + the bundle's mTLS material
  into the EXACT paths the config expects, and the `control-plane/agent` on the mounted control-plane tree
  with a persistent agent-buffer volume (I3) and NO inbound ports (I2) — all tenant-specifics `${...}`-
  parameterized from the bundle. `bundle_lib.py` is the agent-bundle contract + fail-closed
  `validate_bundle()`: required files, manifest keys, telemetry_tier enum, C1 cert-SAN round-trip (via
  committed `ca/ca_lib`), trust-root parse, and I6 signature verification of license.json+config.json (via
  committed `signing/verify_bundle`) + expiry + tenant-binding. `install.sh "[--dry-run] [--no-register]
  [--no-up] <bundle-dir>"` validates → renders `.deployment.env` → best-effort `POST /api/v1/register`
  (degrades on outage, I3) → `docker compose up -d`; `uninstall.sh` tears down by project.
  `make_sample_bundle.py` mints a REAL self-contained sample bundle (ephemeral CA + leaf cert with spiffe
  SAN + ephemeral keyring + real signed license+config) for a hermetic self-test. Verified: `selftest.py`
  42 passed, 0 failed (overlay declares boundary+agent+6 data-plane services + cert/key/ca mounts + agent
  buffer + ZERO inbound ports + dp-net; sample bundle validates with C1 SAN round-trip + I6 sig verify
  actually executing; `install.sh --dry-run` exits 0 RESULT: VALID launching nothing; negatives rejected
  fail-closed — expired license, tampered config, wrong-tenant cert; a REAL `docker compose config` parses
  with cert mounts resolving; `bash -n` clean on both scripts). **Contributes NO long-running service to the
  master compose** (its `service.compose.yml` is just a `tools`-profile-gated `installer-selftest` marker).
  Caveats: MINIMAL data-plane subset (the boundary config also targets the worker fleet which this subset
  does not run → those targets surface as up==0, the intended G5 coded-but-not-running signal — run the
  root `docker-compose.yml` on dp-net for the full fleet); production path is Helm/Terraform (same bundle
  contract); the sample bundle uses an EPHEMERAL CA/key (production bundles are minted by onboarding with
  the real fleet CA + signing key).

- 2026-06-24 — **integrate** — Merged the **WS-CONSOLE** + **WS-AGENT** `service.compose.yml` fragments
  into `docker-compose.control-plane.yml` (replacing the commented Phase-4 `console:` stub): `console` on
  cp-net (8080, `console-data` volume, stdlib /healthz healthcheck, build.context = control-plane ROOT so
  the image copies `lib/`) and `fyralis-agent` on cp-net+dataplane-net with NO `ports:`/EXPOSE (I2), the
  durable `agent-state` buffer volume (I3), `depends_on: [console]`, and build.context = control-plane ROOT
  so the image includes `signing/`+`lib/`. Added the `console-data` + `agent-state` named volumes to the
  top-level `volumes:` block. WS-ONBOARD / WS-LICENSE / WS-INSTALLER are on-demand operator/one-shot tools
  (profile-gated fragments), intentionally NOT wired as always-on services — run their fragments directly.
  **New dep: `requests`** (the WS-AGENT outbound https client) added to `requirements.txt`; console + the
  agent's crypto/pydantic were already present. Verified: `docker compose -f docker-compose.control-plane.yml
  config` exit 0; a yaml.safe_load assertion pass over the rendered config (console on cp-net publishing
  8080 with console-data; fyralis-agent with ZERO ports on cp-net+dataplane-net with agent-state;
  console-data + agent-state in the top-level volumes; dataplane-net still external).

## Phase 5 — Operate the fleet

- 2026-06-24 — **WS-RELEASE** — Signed release bundles + canary→fleet rollout (`control-plane/release/`,
  FR-D / I6). `build_release.py` packages a source tree into a DETERMINISTIC versioned tarball
  (`fyralis-release-<v>.tar.gz` — sorted entries, pinned mtime/uid, stable sha256), emits a CD
  `*.release.json` manifest, and SIGNS the tarball (detached `.sig` + C2 manifest, `artifact=release`),
  EXCLUDING `*.private.pem`/keys/`*.pyc` so a release never ships a key (`build`/`verify` subcommands).
  `publish.py` is the on-disk release registry (`<registry>/<version>/` + `index.json` with a `latest`
  pointer) that RE-VERIFIES before publish (refuses unsigned/tampered/non-release/silent-overwrite) and a
  FastAPI server (`serve`, 8090) exposing bundles in EXACTLY the layout the agent's
  config_pull/verify_bundle consumes (`/releases/<v>/<tarball>[.sig|.manifest.json]`, `/releases/latest`,
  `/index.json`, `/healthz`, path-traversal-blocked). `rollout.py` is the canary→fleet CONTROLLER: reads
  the fleet from the console (`GET /api/v1/deployments`, health derived on read), deterministically
  selects a CANARY subset (lowest deployment_id first, always leaves a gating remainder), promotes the
  canary, WATCHES its health rollup, HALTS on any non-green drift / window expiry and ROLLS the canary
  back, and promotes the FLEET only after a clean watch (`promote`/`status`/`rollback`; transport- and
  promoter-injected so the decision logic is testable). `signing_ctx.py` is a thin REUSE wrapper over the
  committed `control-plane/signing` (signing_lib + verify_bundle) with a configurable trust-root so the
  builder/registry/self-test can sign against the production CP key OR a hermetic ephemeral store WITHOUT
  writing into the committed `signing/` tree (never re-implements ed25519). Verified: `selftest.py`
  **20/20 GREEN** driving the REAL siblings (real ed25519, real console via TestClient): deterministic
  build+sign → verify_bundle ACCEPTS, `*.private.pem` excluded; tamper / foreign trust root → REJECT;
  verify-before-publish; HEALTHY canary → FLEET PROMOTED; UNHEALTHY canary → HALT-ON-DRIFT, fleet NOT
  promoted + canary rolled back; an agent pulling the trio over the HTTP server VERIFIES (ACCEPT). Caveats:
  the production CP signing key is empty until `signing/keygen.py --activate`; promotion delivers a VERSION
  + moves `latest` (the per-deployment signed-config delivery is config-dist's job via the injected
  Promoter seam — signing is never bypassed); heartbeat-freshness alone won't catch a release that stays
  green but misbehaves under real traffic (pair with SLI burn). **No new dep** (cryptography/fastapi/
  uvicorn/httpx already present).

- 2026-06-24 — **WS-CONFIG** — Signed config distribution (`control-plane/config-dist/`, FastAPI/uvicorn on
  8090, cp-net; FR-C3/C4/D4). `store.py` `ConfigStore` is per-deployment, versioned, ed25519-SIGNED config
  persistence: publishing a tier change / flag flip / rotation-schedule edit APPENDS a new immutable signed
  version and advances HEAD (no redeploy) — on-disk `deployments/<id>/HEAD + v<N>/config.json{,.sig,
  .manifest.json}`; the signed body wraps flags + telemetry_tier (C3) + token_rotation (FR-D4) with
  identity+version+created. All signing/verifying is delegated to the committed `control-plane/signing`
  (sign_bundle/verify_bundle), write-disjoint via a `SigningHome` that retargets the CLIs' KEYS_DIR/
  TRUST_ROOT at a config-dist-owned home (identical crypto, storage moved out of `signing/`). `config_service.py`
  (`config_service:app`) implements the EXACT agent-pull contract (`agent/config_pull.py`): `GET
  /config/{deployment_id}[.sig|.manifest.json]` + pinned `/config/{id}/v{n}` + `GET /trust_root.json`, plus
  the operator surface (`POST/GET /api/v1/config/{id}`, `/versions`, `/deployments`, `/healthz`); publishes
  serialized per-process so the version counter can't race. `publish_config.py` CLI sets a config → new
  signed version (`--flag`/`--tier`/`--rotation`/`--config-file`), self-verifying (I6) before reporting
  success. Verified: `selftest.py` **15/15 GREEN** via TestClient — publish → v1 (T1); GET the trio + VERIFY
  via `verify_bundle.verify_file` → VALID `artifact='config'`; tamper config bytes / signature → FAILS
  (I6); a tier change T1→T2 → NEW v2 verifies, HEAD serves v2, v1 still immutably served; additionally drove
  the REAL committed `agent/config_pull.ConfigPuller` over the served trio (good applied, tampered rejected).
  Caveats: service-local trust root by default (mints its own key into a config-dist signing home — the
  agent must pin `GET /trust_root.json`; mount a shared CP keystore + set CONFIG_DIST_SIGNING_HOME/KEY_ID to
  chain to one trust root); no authn at the service (tenant isolation + operator auth are the auth-proxy's
  job — do not expose 8090 publicly); single-writer (publishes serialized via a Lock; run one writer);
  token_rotation is a SCHEDULE distributed to the agent, not the rotation itself. **No new dep** (fastapi/
  uvicorn/cryptography/pydantic already present).

- 2026-06-24 — **WS-METER** — Signed Tier-1 usage metering/billing rollup (`control-plane/metering/`,
  FR-F2/F3). `mimir_client.py` is a per-tenant Mimir query client (httpx) that sets X-Scope-OrgID per call
  (C5), runs `increase(<counter>[<period>])` instant queries against `/prometheus/api/v1/query`, and parses
  the Prometheus vector — transport-only with an injectable httpx transport (the self-test feeds a
  MockTransport so the REAL request-build + parse path runs against a canned Mimir), fail-loud on HTTP/
  protocol errors (a metering job must not silently bill zero on a 500). `rollup.py` computes the per-tenant
  rollup from the three Tier-1 counters harvested from `fleet-sli/recording_rules.yml`
  (`writer_full_mode_writes_total{source}`, `think_runs_total`, `think_cost_recent_usd_total`): period
  delta via `increase()` at period end, missing series → 0 (valid zero bill), negative-extrapolation
  clamped; emits `{tenant_id, period, metrics{obs_per_source, ingestion_volume, think_runs, think_cost_usd},
  totals, schema_version, …}` and SIGNS it by REUSING `control-plane/signing` (artifact=config so the
  canonical sorted-keys JSON is the signed quantity), so any later edit to a usage number breaks
  verification (FR-F2); `verify_rollup()` delegates to `verify_bundle.verify_file` (I6). `export.py` emits
  signed rollups for billing in JSON (round-trips exactly, per-row signature receipt) and CSV (per source +
  a `__TOTAL__` row); `collect_signed_rollups` verifies every bundle first and REFUSES (fail-closed) any
  whose signature doesn't validate. Verified: `selftest.py` **17/17 GREEN** against a mock Mimir
  (httpx.MockTransport) + REAL ed25519 (throwaway trust root): per-source obs (github=1234/slack=88/
  jira=42), ingestion_volume=1364, think_runs=57, cost_usd=3.141592; every query sends X-Scope-OrgID==acme;
  SIGN → verify VALID; tamper cost / obs → INVALID; JSON round-trips; CSV per-source + TOTAL with receipt;
  zero-activity tenant → all-zeros without error; export REFUSES a tampered bundle; unknown-key rollup
  fails verify. Caveats: I1 — reads ONLY aggregate counters (no payloads/PII); the job sets X-Scope-OrgID
  itself (trusted CP-internal reader; point MIMIR_URL at the auth-proxy for defence-in-depth); period math
  under-counts a data-plane gap and a never-reporting deployment yields a valid all-zero bill (cross-check
  the C4 heartbeat to distinguish 'no usage' from 'no telemetry'); think_cost_usd is the data plane's
  self-reported spend (reconcile against the provider bill if the contract requires); JSON floats — apply
  cents-precise rounding downstream. **No new dep** (httpx/cryptography already present).

- 2026-06-24 — **WS-AUDIT** — Append-only hash-chained audit log + break-glass (`control-plane/audit/`,
  FR-G / I5). `audit_log.py` `AuditLog` over an append-only JSONL file: every entry carries
  `seq/ts/actor/action/target/metadata/prev_hash/entry_hash` where `entry_hash = sha256(canonical_json
  (body))` and `prev_hash` links to the previous entry (genesis prev = `GENESIS`); `append()` only ever
  O_APPEND-writes one fsync'd line (never truncates/rewrites); `verify_chain()` walks the chain and detects
  both a tampered past entry (recomputed hash mismatch) and a broken link, returning `bad_seq`. A SECOND
  tamper layer reuses `control-plane/signing` — each append re-signs a detached ed25519 checkpoint over the
  chain-head hash (`<log>.checkpoint.json`), so even a whole-file rewrite (all hashes recomputed) is caught
  because the attacker can't re-sign the new head; canonical_json/sha256/rfc3339 all come from `signing_lib`
  so byte representations match the rest of the CP. `breakglass.py` is a state machine over the log
  implementing all four I5 clauses: `request_grant` (INERT) → `approve_grant` (the distinct customer step
  that starts the time-box, `expires_at = approval + ttl`) → `check_access` (honors ONLY approved +
  unexpired + in-scope grants, exact + opt-in `tenant:x/*` wildcard) + deny/revoke/sweep; every transition
  (request/approve/deny/USE/expire/revoke/denied-check) is appended to the chain, expiry lazy + idempotent.
  `cli.py` exposes `audit append|verify|list` + `breakglass request|approve|deny|revoke|check|sweep|list`,
  auto-loading the active signing key (chain-only if absent). Verified: `selftest.py` **23/23 GREEN** (mints
  a REAL throwaway ed25519 key): append 4 → verify OK + signed checkpoint valid; flip a past field → DETECTS
  + pinpoints bad_seq=1, whole-file rewrite re-links the chain but FAILS the signed checkpoint; customer-
  approve a 1s grant → ALLOWED in window, DENIED after expiry + wrong-scope DENIED; each transition audited
  (expiry exactly once), chain still valid; CLI end-to-end with correct exit codes. Caveats: tamper-EVIDENT
  not tamper-PROOF (detection via hash chain + signed checkpoint; for prevention ship to a WORM/remote
  sink); whole-file evidence needs the active CP private key mounted read-only (chain-only mode reports
  `signature_ok=None`); the checkpoint signs the current head only (truncation-to-a-prior-signed-head needs
  an external monotonic witness — a v2 item); single-writer; wall-clock expiry with no skew grace (fails
  toward expiring sooner); **approver identity is NOT authenticated in the MVP** — `approve_grant` accepts
  any `approved_by` string and does not bind the approver to the grant's tenant, so the trail records
  who-CLAIMED-approval, not who-actually-approved (the grant is genuinely scoped + time-boxed + audit-logged;
  authenticating the approver and binding identity-to-tenant is a documented next-sprint item delegated to the
  console/auth-proxy — see `docs/LIMITATIONS.md` L-11). **No new dep** (cryptography/signing already present).

- 2026-06-24 — **WS-CP-UPGRADE** — Zero-disruption control-plane upgrade tooling (`control-plane/upgrade/`,
  NFR-6). `trust_bundle.py` is the load-bearing CA trust-OVERLAP helper — `add`/`remove`/`list`/`verify`/
  `sign` a CA trust bundle (the proxy's `ca/pki/ca-chain.crt`): `add` appends a new CA's intermediate+root
  so the auth-proxy trusts {old,new} simultaneously during cutover (idempotent by SHA-256 fp, atomic write
  + timestamped `.bak`); `remove` drops a retired CA and REFUSES to empty the bundle; `verify --leaf` proves
  a leaf chains using the SAME committed `ca/verify_chain.py` the proxy uses; `--sign`/`--require-signature`
  reuse `control-plane/signing` (ed25519, C2/I6) so the bundle is itself signed/verified before load.
  `rolling_upgrade.sh` does a real health-gated, one-at-a-time rolling restart of the STATELESS CP services
  (`docker compose up -d --no-deps <svc>` with pre-gate → recreate → post-gate poll → auto-rollback +
  stop-on-failure; DRY_RUN/NO_PULL/NO_ROLLBACK; gracefully skips not-yet-wired services; REFUSES stateful
  mimir/loki/grafana with exit 2 → the blue-green procedure). `trust_overlap.sh` wraps the add-before-rotate
  / remove-after-rotate dance, reloading JUST the auth-proxy at the right moment. `UPGRADE_RUNBOOK.md`
  documents the full procedure (stateless rolling; stateful Mimir/Loki blue-green on SHARED OBJECT STORAGE
  with the remote-write cut-over ordering that never drops a sample; trust-overlap; the I3 buffering safety
  net; rollback + I1–I6 mapping). Verified: `selftest.py` **31 passed, 0 failed** — two real CAs (old-only
  rejects new leaf / overlap trusts BOTH / idempotent add / post-retire rejects old / refuse-empty / sign+
  verify+tamper-fails) via the committed `ca/verify_chain.py`; runbook + trust_overlap.md coverage; scripts
  pass `bash -n`; `service.compose.yml` is valid YAML and `docker compose config` parses the overlay;
  `rolling_upgrade.sh` DRY_RUN against the master compose rolls `console`, skips not-yet-wired `auth-proxy`,
  rejects `mimir` exit 2. Wired into the master compose as the on-demand `cp-upgrade-tools` container
  (`python:3.12-slim` + cryptography/pyyaml, NO ports, `restart: no`, `.:/app` RW so the operator edits the
  trust bundle in place; run via `docker compose run --rm cp-upgrade-tools …`). Caveats: shellcheck absent
  (fell back to `bash -n`); a live roll needs the full stack up; auth-proxy/config-dist not yet in the
  master compose are gracefully skipped, mimir/loki correctly rejected as stateful; `--sign` needs the CP
  keyring; blue-green assumes shared object storage in prod (dev local volumes → use the rolling stateful
  variant). **No new dep** (cryptography/pyyaml already present).

- 2026-06-24 — **INTEGRATE** — Wired the four Phase-5 always-/on-demand services + the upgrade tool into
  `docker-compose.control-plane.yml` (replacing the commented Phase-5 stub), all on cp-net with
  build.context = control-plane ROOT (`.`) + per-dir dockerfile so each image copies `signing/`+`lib/`:
  `release-registry` (publish.py serve, container 8090) HOST-mapped to **8091** to avoid colliding with
  `config-dist` (uvicorn config_service:app, host 8090) — both keep container 8090 so the agent reaches
  them by service name (config-dist:8090 / release-registry:8090) on cp-net unchanged; `metering` (one-shot
  signed-rollup batch job, default command runs the offline self-test, `restart: no`, signing keys mounted
  read-only, `metering-out` volume); `audit` (on-demand CLI image, `restart: no`, `audit-data` volume +
  read-only signing keys); `cp-upgrade-tools` (on-demand operator container, no ports). Added the
  `release-registry-data` / `config-dist-data` / `metering-out` named volumes (audit reuses the existing
  `audit-data`). Verified: `docker compose -f docker-compose.control-plane.yml config` exit 0; all five new
  services render and every published host port is UNIQUE (3000/3100/8080/**8090 config-dist**/**8091
  release-registry**/9009). **No new requirements**: every dep used across the five workstreams
  (cryptography/fastapi/uvicorn/httpx/pydantic/requests/pyyaml) was already in `requirements.txt`.


## Phase 6 — Integration + one-command bring-up

- 2026-06-24 — **WS-SELFOBS** — Control-plane self-observability (`control-plane/self-obs/`, NFR-10,
  "silence != health"): an ACTIVE prober (`cp_exporter.py`) that probes every CP service on each scrape
  (auth-proxy via a TLS HANDSHAKE since it is mTLS-only with no unauthenticated /healthz; mimir/loki via
  /ready; grafana /api/health; console/config-dist/release via /healthz) and exposes `cp_service_up`,
  probe latency, last-success timestamps, the ingest-path-alive synthetic, and a scrape heartbeat so the
  silence itself is alarmable; a DEDICATED, INDEPENDENT `cp-prometheus` (separate from the fleet/Mimir
  path so it survives a fleet-pipeline outage) loading `cp_rules.yml` (25 rules incl. absent()-based
  silence alerts); and an 11-panel `cp_self.json` Grafana dashboard. Self-test 16 passed / 1 skipped
  (promtool absent); rules+config validated out-of-band via the prom/prometheus image (25 rules).

- 2026-06-24 — **WS-SMOKE / tests** — End-to-end SMOKE + pytest integration suite (`control-plane/tests/`)
  that wires the COMMITTED CP components in-process (no stubs) and asserts the full BYOC path against REAL
  code: bootstrap a throwaway CA + ed25519 trust root in /tmp; onboard → bundle + registry row + console
  listing; agent green + heartbeat → console health GREEN; push a metric AS acme through the REAL
  auth-proxy over a genuine mTLS socket → MockMimir, proxy injects X-Scope-OrgID:acme from the cert SAN;
  ISOLATION (query as globex → 0 series; a client-set X-Scope-OrgID is overridden, I4); license tamper →
  agent refuses the privileged pull (I6); config-dist signs → agent verifies-before-apply, rejects a
  tampered config (I6). `e2e_smoke.py` = 52 passed / 0 failed / 3 docker-only skips; `test_e2e.py` =
  6 passed / 1 live-docker skip. Mimir is the only no-docker substitution (MockMimir reproduces the
  X-Scope-OrgID multitenancy contract); `make smoke` runs it with no containers.

- 2026-06-24 — **demo-dataplane** — A tiny stdlib-only golden-12 SLI metrics stub
  (`control-plane/demo-dataplane/metrics_stub.py`) exposing the `fyralis_*` families on :9300 — the exact
  names the boundary allowlist keeps and the fleet-sli rules aggregate — so the boundary OTel Collector
  has a realistic scrape target for the TESTABLE bring-up WITHOUT the real ingestion/reasoning/Postgres
  data plane. `healthy|degraded` scenarios flip a few SLIs into the yellow/red band to demo the fleet
  roll-up colour + alerts. Ships `service.compose.yml` + `Dockerfile` (non-root, stdlib healthcheck) +
  `boundary-collector.demo.yaml` (the same allowlist→redact→identity→mTLS-remote-write shape as the prod
  boundary config, but scraping ONLY the stub). DEMO FIXTURE ONLY — production points the boundary
  collector at the real targets (documented in demo-dataplane/README.md). Verified serving /metrics +
  /healthz.

- 2026-06-24 — **ASSEMBLE / one-command bring-up** — Finalized the master
  `docker-compose.control-plane.yml`: removed the `cp-scaffold` busybox marker and wired EVERY service
  onto cp-net + dataplane-net — **auth-proxy** (mTLS termination, build context = CP root so it carries
  ca/; server cert minted by bootstrap, CA chain + registry bind-mounted live so revocations land without
  a rebuild; NO healthcheck because mTLS-only liveness is observed by the self-obs prober), **boundary-
  collector** (otel-contrib 0.105.0, demo config + the acme tenant client cert from `_runtime/`, remote-
  writes through https://auth-proxy:8443), **demo-dataplane**, **cp-self-obs-exporter** + **cp-prometheus**
  (the WS-SELFOBS fragment), alongside the already-wired mimir/loki/grafana/console/agent/release/config-
  dist/metering/audit/upgrade. Fixed the agent license mounts to the bootstrap-staged `_runtime/agent/`
  trio; added the `cp-self-obs-data` volume; provisioned a Grafana "CP Self-Obs (Prometheus)" datasource
  (uid `fyralis-cp-prometheus`) + a Control-Plane dashboard provider, dropping a provisioned copy of
  `cp_self.json` (DS_CP rebound to the uid, __inputs stripped) under grafana/dashboards/control-plane/.
  16 services, every published host port unique (3000/3100/8080/8090/8091/8443/9009/9091/9110/9300);
  `docker compose -f docker-compose.control-plane.yml config` → exit 0.

  Added **bootstrap.sh** (idempotent first-run entry: CA → signing keygen --activate → auth-proxy server
  cert → onboard demo tenant `acme` via the embedded console → stage the bundle's license trio + tenant
  client cert + CA chain into the gitignored `_runtime/` + write `.env` with the real minted
  deployment_id → create the external dataplane-net → `docker compose up -d --build` → wait-for-health →
  print Console/Grafana/CP-Prom URLs; `--no-docker` does CA/keys/onboard + the python e2e smoke).
  Verified: `./bootstrap.sh --no-docker` from a CLEAN state → CA+keys+onboard+staging all succeed and the
  smoke passes 52/0/3; a second run is fully idempotent (every step reports "already present"). Added a
  **Makefile** (up/down/bootstrap/onboard/smoke/logs/clean/config) and rewrote the **README QUICKSTART**
  to the real one-command flow + a data-flow diagram + where to look. `.gitignore` extended for
  `signing/trust_root.json` (generated public artifact) + `_runtime/` (per-deploy onboarding material) so
  no secret or per-deploy state is ever committed. **No new requirements.**

## Phase 7 — Docs + review; SPRINT COMPLETE

- 2026-06-24 — **DOCS** — Authored the four operator/handoff docs under `control-plane/docs/`
  grounded entirely in the committed tree: `architecture.md` (the trust model + data-flow +
  invariants I1–I6 mapped to where each lives), `reference.md` (per-component config / ports /
  contracts / caveats), `operations.md` (the 10 lifecycle runbooks — onboard/offboard, tier
  change, release+canary, config push, metering export, break-glass, zero-disruption upgrade,
  self-obs), and `TEST_GUIDE.md` (the CTO-as-operator walkthrough from the committed tree:
  `make smoke` 52/0/3, the auth-proxy/audit(23/23)/metering/licensing/config-dist/self-obs/
  onboarding selftests, `test_e2e.py` 6/1-skip, and the live `docker compose` bring-up path).

- 2026-06-24 — **REVIEW** — Three independent CTO-handoff review lenses run against the
  committed tree: **security-e2e** (verdict: concerns — the crypto trust core is genuinely
  strong + fail-closed, but the shipped one-command compose publishes Mimir/Loki/worker-metrics/
  console to the host, bypassing the C5/I4 `X-Scope-OrgID`-trust-behind-the-proxy premise; the
  console registry write API is unauthenticated), **completeness-vs-design** (verdict: concerns —
  every P1–P6 workstream is real wired code, the security crux is faithful, but the C4 heartbeat
  plane is unauthenticated and `FleetSLOLivenessDeploymentSilent`'s `up==0` cannot fire on total
  silence), and **operability** (verdict: concerns — the TEST_GUIDE is mostly truthful-and-runnable
  and `make smoke` matched the documented 52/0/3, but `bootstrap.sh`'s idempotency guard ignores
  the persistent `ca/tenant_registry.json` → a reproducible silent-403 trap, plus the I5
  customer-approve guarantee is oversold in the docs). **Net: no CRITICAL open at the wire level
  in the demo (metrics-plane isolation I4 verified clean); three HIGH items gate the handoff** —
  (L-1) compose host-port publish, (L-2) unauthenticated console registry/heartbeat plane, and
  (L-3) the bootstrap registry-consistency trap — two of which are one-line "do-not-publish-to-host"
  compose changes matching the team's own caveats and one a cheap bootstrap-guard fix.

- 2026-06-24 — **LIMITATIONS** — Consolidated EVERY known caveat (the per-phase build-log notes,
  the component README caveats, and all three review findings) into the single triaged
  `control-plane/docs/LIMITATIONS.md` — each item tagged status (`done`/`partial`/`next-sprint`)
  + severity, with §1 the three HIGH fix-before-handoff items, §2/§3 the medium/low next-sprint
  hardening, and §4 the explicitly-intentional out-of-scope set (live cloud/AWS deploy, formal
  SOC-2 audit, P2 PrivateLink/air-gap/multi-cloud, RLS-if-CP-goes-single-DB-multi-tenant,
  operator-Grafana authn, object-storage for Mimir/Loki, KMS/HSM signing). §5 is the at-a-glance
  triage table + handoff verdict.

- 2026-06-24 — **SPRINT COMPLETE** — Phase rollup: P1 trust roots (CA + ed25519 signing + shared
  lib) → P2 linchpin (mTLS auth-proxy I4 + SSRF fix + boundary OTel T1 redaction I1) → P3 central
  stores + fleet SLIs (multi-tenant Mimir/Loki, operator Grafana, golden-12 + 17 alerts + 4 SLO
  burn-rate rules) → P4 deployment surface (outbound-only agent I2/I3/I6, console registry, atomic
  onboarding FR-E, signed expiring licenses, installer overlay) → P5 operate-the-fleet (signed
  releases + canary, signed config distribution, signed metering rollup, hash-chained audit +
  break-glass I5, zero-disruption upgrade NFR-6) → P6 integration (self-obs NFR-10, e2e smoke
  52/0/3, demo data-plane, one-command `bootstrap.sh` + Makefile) → P7 docs + review +
  LIMITATIONS. All six invariants I1–I6 implemented + the security crux (cert→tenant→`X-Scope-OrgID`
  fail-closed isolation) adversarially verified. Commits `9216d0e … 8875dbd` on
  `feat/byoc-control-plane-mvp`; docs + LIMITATIONS + this entry land as the final two commits.
  Handoff state: ship-ready for an internal/CTO demo once the three HIGH §1 items are closed (all
  network-exposure / bootstrap-consistency, none a wire-level breach in the demo).
