# `control-plane/lib/` — shared control-plane library

The cross-cutting primitives and models every Fyralis BYOC control-plane
component depends on, implemented exactly against the [`SPRINT_PLAN.md`](../SPRINT_PLAN.md)
shared contracts (C1–C5 and invariants I1/I4/I6). Other workstreams (auth proxy,
agent, console, onboarding, licensing, boundary collector) import from here so
that "tenant identity", "telemetry tier", "deployment record", and "signed bytes"
mean exactly one thing fleet-wide.

> **Scope of this WS:** the shared models + readers only. This library is
> **read-only** with respect to the registry and keyring — WS-CA owns *writing*
> `ca/tenant_registry.json` and the signing keyring; we only **read** them.

## Modules

| File | Contract | What it provides |
|------|----------|------------------|
| `tenant.py` | **C1** | `TenantId` type + `TenantRegistry` — a read-only reader over `ca/tenant_registry.json` answering `tenant_for_fingerprint()`, `is_active()`, `is_revoked()`. |
| `tiers.py` | **C3** | `TelemetryTier` enum (`T1`/`T2`/`T3`) + cumulative `TierPolicy` (`permits()` / `requires_redaction()`), consumed by the boundary collector. |
| `deployment.py` | **C4** | `DeploymentRecord` (the fleet-registry row) + `derive_health()` (`green`/`yellow`/`red` from heartbeat age, SLI flags, license expiry). |
| `config.py` | C5 | `ControlPlaneConfig` loaded from `CP_*` env vars with dev-sane defaults (ports, Mimir/Loki URLs, trust-root + registry paths, `X-Scope-OrgID`). |
| `primitives.py` | C1/C2 | fingerprinting (SHA-256 over cert DER), canonical JSON (the bytes signed in C2), RFC-3339 UTC time. |
| `errors.py` | — | shared error hierarchy (`ControlPlaneError` + typed subclasses). |
| `logging.py` | — | structlog setup (`configure_logging()` / `get_logger()`), JSON by default. |
| `__init__.py` | — | the public export surface — import from `control_plane.lib` / `lib`, not the submodules. |

## Public schema (the load-bearing models)

### `TenantRegistry` (C1, read-only)

```python
TenantRegistry(registry_path=None, *, config=None, cache=True)
  .tenant_for_fingerprint(fp) -> TenantId   # raises on unknown/revoked/inactive
  .is_active(fp)  -> bool                    # present AND status == "active"
  .is_revoked(fp) -> bool                    # present AND status == "revoked"
  .record_for_fingerprint(fp) -> TenantRecord  # raw row, no status gate
  .fingerprints() -> list[str]
  .reload()                                  # drop cache, re-read next access
```

Registry file format read (written by WS-CA), keyed by lowercase-hex SHA-256 of
the **leaf cert's DER**:

```json
{
  "<cert_fingerprint_sha256_hex>": {
    "tenant_id": "acme",
    "issued_at": "2026-06-24T00:00:00Z",
    "status": "active"
  }
}
```

`TenantRecord` fields: `tenant_id: str`, `issued_at: str` (RFC-3339), `status:
"active" | "revoked"`.

### `DeploymentRecord` (C4)

```python
DeploymentRecord(
  tenant_id: str,
  deployment_id: str,
  version: str,
  region: str,
  last_heartbeat_ts: datetime,     # accepts RFC-3339 str; serializes to "...Z"
  health: Health = "green",        # green | yellow | red
  license_expiry: datetime,        # accepts RFC-3339 str
  telemetry_tier: TelemetryTier = "T1",   # T1 | T2 | T3
)
  .to_registry_dict() -> dict      # exact C4 JSON wire shape
  .derived_health(...) -> Health
  .with_derived_health(...) -> DeploymentRecord
DeploymentRecord.heartbeat(...)    # build at heartbeat time, health derived
```

`derive_health(last_heartbeat_ts, *, now, yellow_after_s=90, red_after_s=300,
sli_breached=False, license_expiry=None) -> Health` — fresh→`green`,
stale→`yellow`, missing→`red`; an SLI breach degrades `green`→`yellow`; an
expired license forces `red`; a future heartbeat is clamped (clock-skew safe).
Worst applicable condition wins.

### `TelemetryTier` / `TierPolicy` (C3)

```python
TelemetryTier.T1 | T2 | T3                  # str enum, JSON value "T1"...
TelemetryTier.parse("t2") -> TelemetryTier  # raises TierError on garbage
tier_policy("T2") -> TierPolicy
  .permits(SignalClass.LOGS) -> bool
  .requires_redaction(SignalClass.LOGS) -> bool
  .carries_pii_risk() -> bool               # False only for T1 (Invariant I1)
```

Cumulative table: T1 = metrics; T2 = +redacted logs; T3 = +redacted/sampled
traces.

### `ControlPlaneConfig` (env-driven)

`load_config(**overrides)` reads `CP_*` env vars with defaults. Key fields:
`tenant_registry_path` (→ `ca/tenant_registry.json`), `trust_root_path`,
`signing_keyring_path`, `mimir_url`/`loki_url`/`grafana_url` (cp-net service
names), `scope_org_header` (`X-Scope-OrgID`), `auth_proxy_port` (8443),
`heartbeat_yellow_after_s`/`heartbeat_red_after_s` (90/300). All overridable;
e.g. `CP_TENANT_REGISTRY`, `CP_MIMIR_URL`, `CP_ROOT`.

## Run / test

The package uses relative imports, so run it with the **`control-plane/` root on
`sys.path`** (i.e. import it as the `lib` package):

```bash
# from control-plane/
python -m lib._selftest        # constructs a DeploymentRecord, exercises health
                               # derivation, and round-trips TenantRegistry
                               # against a tiny sample registry under /tmp
```

`py_compile` only (no deps needed beyond pydantic):

```bash
python -m py_compile lib/*.py
```

The self-test writes its sample registry under `/tmp` (never into `ca/`) and
exits non-zero if any assertion fails.

## Design notes / caveats

- **Read-only by design.** `TenantRegistry` never writes; the signing keyring is
  referenced by path only. Writers live in WS-CA / WS-signing.
- **Fingerprint canon.** A cert fingerprint is the lowercase-hex SHA-256 over the
  cert's **DER** bytes — verified in the self-test to match
  `cryptography`'s `cert.fingerprint(SHA256)` and OpenSSL's
  `-fingerprint -sha256`. The reader normalizes presented fingerprints
  (strips colons/spaces, lowercases, drops a `sha256:` prefix) so a lookup never
  misses on cosmetic formatting.
- **Fail closed.** Any status other than the literal `active`/`revoked` is a
  `RegistryFormatError` (an unknown/empty status is **not** treated as active),
  and `tenant_for_fingerprint()` rejects unknown, revoked, and inactive
  distinctly so the proxy can 403 with the right reason (I4).
- **Prompt revocation.** The reader caches by file `mtime+size` and re-reads when
  the file changes, so a revocation written out-of-band by WS-CA takes effect on
  the next call without a process restart. Pass `cache=False` to always read
  fresh, or call `.reload()`.
- **`canonical_json_bytes`** is the C2 "signed bytes" definition (sorted keys,
  no whitespace, UTF-8, non-ASCII preserved). The actual ed25519 sign/verify
  lives in WS-signing; this module only provides the canonical-bytes primitive.
- **`fingerprint_pem`** is the only function that imports `cryptography`, and it
  does so lazily — the rest of `lib` imports with just `pydantic`/`structlog`
  present, so importing the models never fails if the crypto stack is absent.
- **Health re-derivation:** the agent stamps health at heartbeat time via
  `DeploymentRecord.heartbeat(...)`; the console should call
  `with_derived_health(...)` on read to catch a deployment that went silent
  *after* its last heartbeat (its stored `health` would otherwise be stale).
- **`_selftest.py`** is a runnable verification harness kept inside this dir; it
  is not part of the public API.
