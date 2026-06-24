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
