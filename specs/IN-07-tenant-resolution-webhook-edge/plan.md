# Implementation Plan: IN-07 — Tenant Resolution at Webhook Edge

**Branch**: `feat/IN-07-tenant-resolution-webhook-edge` | **Date**: 2026-05-13 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/IN-07-tenant-resolution-webhook-edge/spec.md`

## Summary

Replace the IN-06 stub `services/webhooks/tenant_resolution.py` (env-var
lookup MVP) with a DB-backed tenant resolver that consults a new
`provider_installations` registry table, caches lookups in an
in-process TTL LRU, exposes register / disable / re-enable /
update-secret-ref admin actions, and returns a discriminated-union
outcome (`Resolved | UnknownInstallation | PayloadMissing`) that the
IN-06 webhook router maps to HTTP 200 / 401 / 400. Disabled and
never-registered installations are externally indistinguishable
(FR-005, SC-003). Hot-path latency target: 2 ms p95 hit, 25 ms p95
miss (SC-009). Cache hit rate ≥ 95% after warmup (SC-004).

The module is renamed from the IN-06-introduced `tenant_resolution.py`
to `tenant_resolver.py` per `source.md` "Files relevant"; one import
site in IN-06's `services/webhooks/router.py` updates with the
rename (see Research item R1).

## Technical Context

**Language/Version**: Python 3.12 (project `requires-python = ">=3.11"`; `.venv` is 3.12).
**Primary Dependencies**: FastAPI ≥0.110 (admin route mounting, not used in IN-07 — see R4), asyncpg ≥0.29 (DB), Pydantic v2 (outcome types + admin request model), structlog (logs), `prometheus_client` (metrics — already used by `services/webhooks/metrics.py`). Stdlib `collections.OrderedDict` + `time.monotonic` for the in-process LRU (no new dependency for the cache tier).
**Storage**: One new table `provider_installations` (idempotent migration `0039_provider_installations.sql`). No pgvector. No partitions.
**Testing**: pytest + pytest-asyncio. Real Postgres via `db_pool` / `fresh_db` fixtures from root `conftest.py`. No mocks of Postgres. The `integration` marker is required for tests that exercise the table.
**Target Platform**: Linux containers under the existing `docker-compose.yml`. Resolver runs in-process inside the gateway / webhook-receiver process; no separate worker.
**Project Type**: Web service module (FastAPI). No frontend change.
**Performance Goals**: SC-009 — cache-hit ≤ 2 ms p95, cache-miss ≤ 25 ms p95 (one Postgres round-trip plus a cache write). SC-004 — ≥ 95% hit rate after warmup.
**Constraints**: Constitution §III (FK + RLS + tenant-prefixed indexes on `provider_installations`); §II (idempotent migration, next-free number 0039); §VII (uuid7 for the row id); §VIII (CompanyOSError-derived errors for any error path that does escape the discriminated-union result); §X (YAGNI on a Redis L2 tier — single in-process LRU until SC-004 is demonstrated insufficient).
**Scale/Scope**: At launch, expected installation count is bounded by tenants × providers — low thousands max for the foreseeable horizon. The whole table fits comfortably in memory; the LRU is sized for everything.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

Evaluating each principle from `.specify/memory/constitution.md` v1.0.0:

| # | Principle | Status | Notes |
|---|-----------|--------|-------|
| I | Four Foundations are epistemically distinct | **PASS (with substrate-exemption note)** | This feature produces NO Observation / Model / Act / Resource. `provider_installations` is a per-feature side table for tenant routing — explicitly permitted ("Per-feature side tables for cross-cutting concerns ... are allowed and encouraged — they are not new foundations"). The Universal Flow Rule does not apply directly; downstream Observations produced by IN-06 carry the resolved `tenant_id` and continue to obey the rule. Recorded in spec §"Context & Substrate Alignment". |
| II | Schema is append-only, migrations idempotent | **PASS** | New migration `0039_provider_installations.sql` uses `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX IF NOT EXISTS`, `DO $$ ... $$` for the RLS policy attach (matching the migration 0036 / 0037 pattern). Re-running against an existing DB is a no-op. The task body's literal "0041" is non-authoritative; the next-free number is 0039 (recorded in spec Assumption A1). |
| III | Tenant isolation is structural | **PASS** | `provider_installations` carries `tenant_id UUID NOT NULL REFERENCES tenants(id) DEFERRABLE INITIALLY IMMEDIATE`, `ENABLE ROW LEVEL SECURITY`, `FORCE ROW LEVEL SECURITY`, the migration 0036 `tenant_isolation` permissive policy attached, plus indexes: the `UNIQUE (provider, installation_id)` from source.md (lookup path) and a new `(tenant_id, provider)` index for the admin enumeration path. All resolver SQL uses `WHERE tenant_id = ...` explicitly where applicable; the lookup path is keyed by `(provider, installation_id)` plus `enabled = true` and returns the row's `tenant_id` — no cross-tenant join, no leak. |
| IV | Integration tests use a real database | **PASS** | The integration test suite (`services/webhooks/tests/test_tenant_resolver_*.py`) uses the existing `db_pool` / `fresh_db` fixtures. No Postgres mocks. Cache eviction tests use a tiny TTL injected through the deps factory — no mock cache. |
| V | Reasoning is separated from rendering | **N/A** | No Think, no Rendering. |
| VI | Trust, confidence, falsifiers are first-class | **N/A** | No Observation, Model, or Commitment written here. |
| VII | Determinism, idempotency, audit trails | **PASS** | Row primary key uses `uuid7()` from `lib/shared/ids.py`. Resolver is a pure function of `(provider, payload, headers, time)` given fixed DB state. No queue introduced; no `FOR UPDATE SKIP LOCKED` needed. No audit table — installation lifecycle events are deliberately out of scope (spec A10). |
| VIII | Errors carry structured context | **PASS** | Resolver outcomes are values (Pydantic discriminated union), not exceptions, because all three are business outcomes the caller branches on. Admin-path errors (`InstallationConflictError`, `InstallationNotFoundError`) extend `CompanyOSError`, carry stable `code` strings (`installation_conflict`, `installation_not_found`), and serialize as `{code, message, context}` per the existing contract. |
| IX | Substrate changes are dual-write until proven | **N/A** | Not a substrate-shape change. The IN-06 stub `tenant_resolution.py` is a write-once placeholder (no production callers depend on its current behavior on this branch), so the rename + replacement is a single-step swap, not a dual-write. Documented as Research item R1. |
| X | Simplicity, YAGNI, no premature abstraction | **PASS** | Single in-process TTL LRU cache (no Redis L2 until measurement shows we need it — see R3). Admin interface is a Python service function plus a thin CLI wrapper, NOT a FastAPI admin endpoint (no second caller today — see R4). One outcome type (`ResolverOutcome` discriminated union) rather than three subclasses (no callsite needs to branch on type vs. value — see R5). |

**Stack-constraint check**:

- New deps in `pyproject.toml`: **none**. The LRU uses stdlib `OrderedDict` + `time.monotonic`. `prometheus_client` is already a dependency.
- `from __future__ import annotations`, full type hints, Pydantic v2 with `extra="forbid"` on the admin request model.
- `asyncpg` via the existing `db_pool` dependency wired in `services/gateway/main.py`.
- No `print()`. No `uuid.uuid4()`. No module-level globals for the pool, cache, or clock — `build_tenant_resolver(deps)` factory.
- Metrics registered in `services/webhooks/metrics.py` at module import (Prometheus client is process-global by design); the resolver receives the metric *instances* as deps so tests can pass throwaway counters.

**Review-gate check** (from constitution §Workflow):

- Edits an already-applied migration: **NO** (0039 is new).
- Substrate write path bypassing audit chain / dual-write / region lock: **N/A** (no substrate write).
- Model write missing `born_from_event_id`: **N/A**.
- New tenant-scoped table missing FK + RLS + tenant-prefixed index: **NO** (all three present — see Principle III above).
- Tenant-scoped query missing `WHERE tenant_id`: **NO** (admin queries scope by tenant; resolver-side lookup is keyed by `(provider, installation_id)` plus an explicit `WHERE enabled = true` and returns the row's `tenant_id` — no cross-tenant join, no leak).
- `uuid.uuid4()` for a substrate row, `print()` in service code, mocked Postgres in integration test: **NO** for all three.
- Voice rules exceeded: **N/A** (no rendering).

**Outcome**: PASS. No constitution exceptions are required.

## Project Structure

### Documentation (this feature)

```text
specs/IN-07-tenant-resolution-webhook-edge/
├── source.md            # Original ClickUp body (verbatim)
├── spec.md              # Feature spec (Phase 1 output, gated)
├── plan.md              # This file (Phase 3 output)
├── research.md          # Phase 0 — five plan-phase decisions (R1..R5)
├── checklists/
│   └── requirements.md  # Spec quality checklist (Phase 1)
└── tasks.md             # Phase 4 output (NOT created here)
```

A separate `data-model.md` / `contracts/` / `quickstart.md` are
omitted — the table is small enough to live inline (§"Data Model"
below), the only external interface is a CLI command (§"Admin
Interface"), and the feature is plumbing without a developer-onboarding
story. Constitution §X (no premature abstraction) cuts against
generating boilerplate files for a 200-line module.

### Source Code (repository root)

```text
services/webhooks/
├── tenant_resolver.py             # NEW (renamed from tenant_resolution.py).
│                                  #   Discriminated-union ResolverOutcome,
│                                  #   build_tenant_resolver(deps) factory,
│                                  #   per-provider extractor table,
│                                  #   admin actions (register/disable/re-enable/update-secret-ref).
├── tenant_resolution.py           # REMOVED (rename target; one IN-06 import site updates).
├── metrics.py                     # MODIFIED — add three resolver metrics (FR-018).
├── router.py                      # MODIFIED — single import-line update (tenant_resolution → tenant_resolver),
│                                  #   no behavior change to the IN-06 router.
└── tests/
    ├── test_tenant_resolver_extract.py   # NEW — per-provider id extraction (pure unit, no DB).
    ├── test_tenant_resolver_lookup.py    # NEW — @integration, DB-backed lookup + RLS.
    ├── test_tenant_resolver_cache.py     # NEW — TTL/eviction/invalidation; mostly unit.
    ├── test_tenant_resolver_admin.py     # NEW — @integration, register/disable/update flows.
    └── test_tenant_resolver_security.py  # NEW — @integration, indistinguishability (US-3 / SC-003).

db/migrations/
└── 0039_provider_installations.sql       # NEW.

scripts/
└── webhook_install.py             # NEW — CLI wrapper over the admin actions (FR-007).

services/gateway/main.py           # NO CHANGE — resolver is wired via the existing IN-06 router build path.
pyproject.toml                     # NO CHANGE — no new dependencies.
```

**Structure decision**: The resolver and its admin actions live in
one module (`services/webhooks/tenant_resolver.py`) rather than
splitting "resolver" and "admin" into separate files. Justification:
both surfaces share the same `provider_installations` SQL, the same
cache-invalidation rules, the same metric labels, and the same error
classes. Splitting them would force a circular import or a third
shared module. Constitution §X: don't add layers for the sake of
symmetry.

Test files are split by concern (extraction, lookup, cache, admin,
security) rather than by surface, because the test failure modes
differ — a broken extractor reads as a per-provider unit-test
failure, a broken RLS policy reads as a security regression, a
broken cache reads as a latency regression.

## Data Model

The whole substrate-shape change is one table; reproducing it
inline:

```sql
-- db/migrations/0039_provider_installations.sql

-- Idempotent: this file is re-run as a no-op against any DB where the
-- objects already exist.

CREATE TABLE IF NOT EXISTS provider_installations (
    id              UUID         PRIMARY KEY,                          -- uuid7() generated app-side
    tenant_id       UUID         NOT NULL
                                 REFERENCES tenants(id)
                                 DEFERRABLE INITIALLY IMMEDIATE,       -- migration 0037 pattern
    provider        TEXT         NOT NULL,
    installation_id TEXT         NOT NULL,
    secret_ref      TEXT,                                              -- pointer into external secrets manager; NULLable
    enabled         BOOLEAN      NOT NULL DEFAULT true,
    installed_at    TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (provider, installation_id)                                 -- lookup uniqueness from source.md
);

CREATE INDEX IF NOT EXISTS idx_provider_installations_tenant_provider
    ON provider_installations (tenant_id, provider);                   -- admin enumeration; tenant-prefixed per §III

-- RLS: same pattern as migration 0036.
ALTER TABLE provider_installations ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_installations FORCE ROW LEVEL SECURITY;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_policies
        WHERE schemaname = 'public'
          AND tablename = 'provider_installations'
          AND policyname = 'tenant_isolation'
    ) THEN
        EXECUTE $POLICY$
            CREATE POLICY tenant_isolation ON provider_installations
            USING (
                current_setting('app.current_tenant', true) IS NULL
                OR tenant_id = current_setting('app.current_tenant', true)::uuid
            )
            WITH CHECK (
                current_setting('app.current_tenant', true) IS NULL
                OR tenant_id = current_setting('app.current_tenant', true)::uuid
            )
        $POLICY$;
    END IF;
END $$;
```

Notes:

- The `id` column is **not** `DEFAULT gen_random_uuid()` — application
  code allocates it via `lib.shared.ids.uuid7()` (Constitution §VII).
  Letting the DB default to `uuid4` is a Review-Gate violation.
- The `provider` column is `TEXT`, not an `ENUM`. Justification: the
  five supported providers are validated at the application boundary
  (`ResolverProvider = Literal["slack", "github", "linear", "stripe",
  "discord"]` in Python). A DB enum would require a migration every
  time we add a sixth provider; the Python literal is sufficient.
- `secret_ref` is `NULLable` because the resolver itself does not
  require a secret to do its job — signature verification (IN-06) does.
  A row can be registered first and have its secret_ref attached
  later via the update-secret-ref admin action.

### Python entity types (sketch — full impl in Phase 6)

```python
# services/webhooks/tenant_resolver.py

from typing import Literal, Annotated
from uuid import UUID
from datetime import datetime
from pydantic import BaseModel, Field

ResolverProvider = Literal["slack", "github", "linear", "stripe", "discord"]

class Installation(BaseModel):
    model_config = {"extra": "forbid"}
    id: UUID
    tenant_id: UUID
    provider: ResolverProvider
    installation_id: str
    secret_ref: str | None
    enabled: bool
    installed_at: datetime

class Resolved(BaseModel):
    model_config = {"extra": "forbid"}
    outcome: Literal["resolved"] = "resolved"
    tenant_id: UUID
    installation_row_id: UUID          # the row id, NOT the provider-native string
    secret_ref: str | None

class UnknownInstallation(BaseModel):
    model_config = {"extra": "forbid"}
    outcome: Literal["unknown_installation"] = "unknown_installation"
    provider: ResolverProvider         # installation_id deliberately absent — FR-014

class PayloadMissing(BaseModel):
    model_config = {"extra": "forbid"}
    outcome: Literal["payload_missing"] = "payload_missing"
    provider: ResolverProvider

ResolverOutcome = Annotated[
    Resolved | UnknownInstallation | PayloadMissing,
    Field(discriminator="outcome"),
]
```

## Resolver Module API

```python
# services/webhooks/tenant_resolver.py — public surface, signatures only.

from collections.abc import Mapping, Callable
from typing import NamedTuple
import asyncpg

class TenantResolverDeps(NamedTuple):
    pool: asyncpg.Pool
    cache: "InstallationCache"      # see "Cache shape" below
    clock: Callable[[], float]      # time.monotonic by default; injectable for tests
    metrics: "ResolverMetrics"      # the three FR-018 metric instances

def build_tenant_resolver(deps: TenantResolverDeps) -> "TenantResolver": ...

class TenantResolver:
    """Pure-function-ish resolver. No module-level state."""
    async def resolve(
        self,
        provider: ResolverProvider,
        payload: Mapping[str, Any],
        headers: Mapping[str, str],
    ) -> ResolverOutcome: ...

    # Admin actions — all guarded by operator-auth at the caller (CLI / future endpoint).
    async def register_installation(self, req: RegisterInstallationRequest) -> Installation: ...
    async def disable_installation(self, installation_row_id: UUID) -> None: ...
    async def enable_installation(self, installation_row_id: UUID) -> None: ...
    async def update_secret_ref(self, installation_row_id: UUID, new_secret_ref: str | None) -> None: ...
```

The factory pattern matches the existing `build_*_router(deps)`
convention in the codebase (Constitution stack constraints). Deps
include the cache (so tests can pass a tiny LRU) and the clock (so
tests can advance time deterministically).

Per-provider id extraction is a static table:

```python
PROVIDER_EXTRACTORS: dict[ResolverProvider, Callable[[Mapping, Mapping], str | None]] = {
    "slack":   lambda p, h: _str_or_none(p.get("team_id")),
    "github":  lambda p, h: _str_or_none((p.get("installation") or {}).get("id")),
    "linear":  lambda p, h: _str_or_none(p.get("organizationId")),
    "stripe":  lambda p, h: _str_or_none(h.get("Stripe-Account") or h.get("stripe-account")),
    "discord": lambda p, h: _str_or_none(p.get("guild_id") or p.get("application_id")),
}
```

`_str_or_none` returns `None` if the value is absent, an empty
string, or a non-stringifiable type — that drives the
`PayloadMissing` outcome (FR-006). A `None` here is distinct from a
"present but unknown" id (which drives `UnknownInstallation`).

## Cache Shape

**Choice**: single in-process TTL LRU. No Redis L2.

Implementation:

```python
class InstallationCache:
    """TTL LRU keyed by (provider, installation_id) → CacheHit | CacheNegative.

    CacheNegative is a sentinel for "looked up, not found / disabled" so we
    cache misses too — without it, an attacker can drive infinite DB
    queries by repeating unknown ids.
    """
    def __init__(self, *, max_entries: int = 4096, ttl_seconds: float = 300.0): ...
    def get(self, key: tuple[str, str], now: float) -> "CacheHit | CacheNegative | None": ...
    def put(self, key: tuple[str, str], value: "CacheHit | CacheNegative", now: float) -> None: ...
    def invalidate(self, key: tuple[str, str]) -> None: ...
    def stats(self) -> "CacheStats": ...
```

Key decisions:

- **TTL = 300 s** matches source.md "Redis with 5min TTL." This is the
  upper bound on time-to-consistency for an admin action that is
  *not* paired with explicit invalidation; SC-006 (5-second
  consistency) is met by the explicit invalidation path, not by TTL.
- **Negative caching** is mandatory. Without it, an attacker
  enumerating random ids drives one DB query per request and turns
  SC-007 (cache-backend-unavailable degradation) into a DoS path.
- **`max_entries = 4096`** — comfortably exceeds the expected
  population (low thousands of installations) by 1–2 OOM. Sized so
  no eviction happens in steady state; eviction is only a defense
  against pathological probing.
- **Eviction policy = LRU** via `OrderedDict.move_to_end`. Simple,
  zero deps, p99 cost is a hash + a linked-list pointer swap.
- **Thread safety**: asyncio is single-threaded, no lock needed.
- **Two-tier Redis L2 deferred** — see Research item R3.

Invalidation:

- `register_installation` → `cache.invalidate((provider, installation_id))` so the prior negative entry (if any) is cleared.
- `disable_installation` / `enable_installation` / `update_secret_ref`
  → look up the row to get its `(provider, installation_id)`, then
  invalidate.
- The 5-second consistency target in SC-006 is met by the
  invalidation, NOT by TTL.

## Admin Interface

**Choice**: a Python service function plus a thin CLI wrapper at
`scripts/webhook_install.py`. No FastAPI admin endpoint.

```bash
$ python scripts/webhook_install.py register --provider slack \
        --installation-id T_ACME_123 --tenant-id 11111111-... \
        --secret-ref arn:secrets:slack/acme/v1
{"id":"01923f...","outcome":"registered"}

$ python scripts/webhook_install.py disable --id 01923f...
{"outcome":"disabled"}

$ python scripts/webhook_install.py update-secret-ref --id 01923f... \
        --secret-ref arn:secrets:slack/acme/v2
{"outcome":"secret_ref_updated"}
```

Justification (Constitution §X, YAGNI):

- The OAuth callback flow that would call this from in-process Python
  is OUT OF SCOPE (spec A10). The only callers today are operators.
- A CLI is operator-only by virtue of shell access — no auth-on-the-
  wire needed (FR-017 is satisfied by the shell).
- An HTTP admin endpoint adds auth middleware, route registration,
  Pydantic request models for the wire format, and an operator-token
  management story. None of that is required today.
- When the OAuth callback lands, it will call
  `TenantResolver.register_installation()` directly in-process —
  the CLI is just one of two callers, but the only shipping caller
  in IN-07.

The CLI exits non-zero on conflict (`InstallationConflictError`) and
on not-found (`InstallationNotFoundError`), with the structured
error body printed to stderr.

## Error Taxonomy

**Choice**: discriminated-union value type for resolver outcomes
(`ResolverOutcome`), `CompanyOSError` subclasses for admin
exceptional paths.

| Surface | Form | Why |
|---|---|---|
| Resolver: `resolve(...)` returns | `ResolverOutcome = Resolved \| UnknownInstallation \| PayloadMissing` | All three outcomes are routine business cases the caller (IN-06 router) branches on. Per §VIII, an exception is the right shape when a call site needs to branch on **type**; here the call site branches on **value** (`outcome.outcome == "resolved"`). Pydantic discriminated union keeps the JSON wire shape stable (the field name `outcome` is the discriminator) and gives mypy exhaustiveness checking via `assert_never`. |
| Admin: `register_installation` conflict | `class InstallationConflictError(CompanyOSError)`, code `"installation_conflict"` | Conflict is exceptional — the call site is the CLI / OAuth callback, and the right behavior is "bubble to the operator." Exception path is correct per §VIII (a). |
| Admin: `disable_installation` / `update_secret_ref` not found | `class InstallationNotFoundError(CompanyOSError)`, code `"installation_not_found"` | Same justification. |

Both new error classes extend `CompanyOSError`, fill `context = {"provider": ..., "installation_id": ...}` for admin paths (admin callers are trusted and the id is in the input), and serialize to `{code, message, context}` per the existing contract.

Critically, the **resolver path** never includes the
`installation_id` in any error or log line (FR-015) — the
`UnknownInstallation` outcome carries only the provider, exactly as
called out in the discriminated union sketch above.

## Metric Registration

Three metrics added to `services/webhooks/metrics.py` at module
import time (Prometheus client is process-global by design; metric
objects are singletons):

```python
# services/webhooks/metrics.py — added entries.

from prometheus_client import Counter, Histogram

webhook_resolver_outcomes_total = Counter(
    "webhook_resolver_outcomes_total",
    "Tenant resolver outcome distribution.",
    labelnames=("provider", "outcome"),
)
webhook_resolver_cache_total = Counter(
    "webhook_resolver_cache_total",
    "Tenant resolver cache result distribution.",
    labelnames=("provider", "result"),
)
webhook_resolver_duration_seconds = Histogram(
    "webhook_resolver_duration_seconds",
    "End-to-end tenant resolver latency, seconds.",
    labelnames=("provider",),
    buckets=(0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250),
)
```

Histogram buckets are explicit: 0.5 ms → 250 ms, chosen so that the
2 ms hit SLO (SC-009) and the 25 ms miss SLO (SC-009) both fall on
bucket boundaries, making p95 readout from the histogram exact at
the SLO threshold.

The resolver factory accepts the **instances** as a dep:

```python
class ResolverMetrics(NamedTuple):
    outcomes: Counter
    cache: Counter
    duration: Histogram
```

This lets tests pass in throwaway counters (zeroed per test, no
cross-test pollution) without monkeypatching the module-level
globals. Production code wires the singletons in
`build_tenant_resolver` at gateway startup.

## Workflow Phases

### Phase 0 — Research (output: `research.md`)

Five decisions resolved (R1..R5) — see [research.md](./research.md).

### Phase 1 — Design

Captured inline in this plan (no separate `data-model.md` /
`contracts/` / `quickstart.md`). See:

- §"Data Model" for the table + Python entity types.
- §"Resolver Module API" for the public surface.
- §"Cache Shape", §"Admin Interface", §"Error Taxonomy" for the
  remaining design decisions.

### Phase 2 — Tasks (output: `tasks.md`)

Out of scope here — produced by Phase 4 of the SDD pipeline. The
task ordering MUST be: migration first → resolver core + outcome
types → cache → admin actions + CLI → metrics wiring → tests
(per Constitution §IX "migrations land first, dual-write/sidecar
writers second, reader cutover and tests last" — adapted: this
isn't a dual-write feature, but the migration-first rule still
applies).

## Test Strategy

Aligned with Constitution §IV (real DB, not mocks). Five test
modules, all under `services/webhooks/tests/`:

| File | Marker | Concern | Notable assertions |
|---|---|---|---|
| `test_tenant_resolver_extract.py` | unit | Per-provider id extraction from payload/headers. | Each of the 5 providers produces the right key from a recorded vendor-sample payload; malformed payloads yield `PayloadMissing` (FR-006). |
| `test_tenant_resolver_lookup.py` | `integration` | DB-backed lookup correctness + cache cold path. | Real Postgres; `db_pool` fixture; assert `Resolved` for an enabled row, `UnknownInstallation` for a never-registered row, `UnknownInstallation` (indistinguishable response shape) for a disabled row. |
| `test_tenant_resolver_cache.py` | unit | LRU TTL, eviction, invalidation, negative caching. | Inject a small `max_entries=4` and a fast clock; assert eviction order, post-TTL re-read, invalidation idempotency, negative-entry behavior. |
| `test_tenant_resolver_admin.py` | `integration` | Register / disable / re-enable / update-secret-ref. | Conflict on duplicate `(provider, installation_id)` regardless of tenant; not-found on disable of a missing row; SC-006 (5-second consistency) verified by asserting the next `resolve` after `disable` returns `UnknownInstallation` within a poll loop bounded by 5 s. |
| `test_tenant_resolver_security.py` | `integration` | Indistinguishability + RLS + log hygiene. | Hash response/log body for the disabled and never-registered cases and assert equality (SC-003); assert RLS prevents cross-tenant reads (US-6 / SC-006-RLS); scan the test-emitted log records for any installation_id value and assert zero hits (SC-008). |

Latency assertion (SC-009) lives in `test_tenant_resolver_lookup.py`
as a marker-gated `@pytest.mark.slow` run-once test that hits the
histogram and reads the p95 bucket; flaky-ness budget is one
allowed retry (test-level), because per-process p95 noise on a
shared CI host is unavoidable.

## Migration & Coexistence Plan

This feature **renames** an unmerged sibling file (`tenant_resolution.py`
→ `tenant_resolver.py`) and **replaces** its env-var implementation
with the DB-backed one. The rename is safe because:

- The existing `tenant_resolution.py` is on the same branch base as
  this work; no upstream merge has consumed it.
- The only import site is `services/webhooks/router.py` (a single
  line). Updating it is part of this feature's diff, not a dual-
  write coordination problem.
- Test suites that reference the old name (if any) update in the
  same commit.

| Stage | Work | Gate |
|---|---|---|
| A | Migration 0039 lands (`provider_installations` table + RLS + index). | `python scripts/check_schema_drift.py` exits zero. |
| B | `tenant_resolver.py` lands with `resolve` + admin actions; cache wired; metrics registered. | All 5 unit test files pass; `services/webhooks/test_tenant_resolver_extract.py` and `_cache.py` green. |
| C | `tenant_resolution.py` removed; `router.py` import updated. | `services/webhooks/tests/test_router_paths.py` (IN-06 owned) still green. |
| D | Integration tests for lookup / admin / security pass against real Postgres. | `pytest -m integration services/webhooks/` green. |
| E | (Out of scope for IN-07) IN-06 cuts the dogfood Slack app over to use this resolver against a real `provider_installations` row. | IN-06's own gate. |

## Complexity Tracking

> Fill ONLY if Constitution Check has violations that must be justified.

No constitution violations require justification.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _(none)_ | _(n/a)_ | _(n/a)_ |
