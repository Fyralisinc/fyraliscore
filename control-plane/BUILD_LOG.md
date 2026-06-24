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
