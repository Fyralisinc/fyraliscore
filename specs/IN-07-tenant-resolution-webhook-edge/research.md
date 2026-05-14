# Research: IN-07 — Tenant Resolution at Webhook Edge

Five plan-phase decisions, each with a chosen path, the rationale,
and the alternatives that were rejected. Linked from
[plan.md](./plan.md) §"Constitution Check" and §"Cache Shape".

---

## R1 — Module name & coexistence with IN-06's stub

**Decision**: Rename `services/webhooks/tenant_resolution.py` → `services/webhooks/tenant_resolver.py` in the same commit that replaces its env-var MVP with the DB-backed implementation. Update the single import site in `services/webhooks/router.py`.

**Rationale**: `source.md` "Files relevant" names `tenant_resolver.py`. IN-06's sibling branch wrote `tenant_resolution.py` as a write-once stub on the same branch base. Both names coexisting is the worst of both worlds; keeping the IN-06 name silently overrides `source.md`. Renaming costs one commit-line in `router.py`.

**Alternatives considered**:

- **Keep `tenant_resolution.py`, update spec Assumption A2 to say IN-06's name wins.** Rejected: this would mean the recorded resolution in the spec ("task body wins") gets flipped at plan time without strong cause — the cost of the rename is one line.
- **Keep both files, with `tenant_resolver.py` re-exporting from `tenant_resolution.py`.** Rejected: two files, two import paths, no functional gain — exactly the kind of premature indirection Constitution §X forbids.

---

## R2 — Migration number

**Decision**: `db/migrations/0039_provider_installations.sql`. NOT `0041` as named in `source.md`.

**Rationale**: Constitution §II.1 — "Migrations are numbered, applied in filename order, never edited after merge." The next free number on this branch is 0039 (latest applied: `0038_signal_readings_sidecar.sql`). The literal "0041" in `source.md` was a guess by the task author; renumbering before merge is exactly the case where the constitution wins over the task body. Spec Assumption A1 explicitly recorded that the number was deferred to plan time.

**Alternatives considered**:

- **Reserve 0040 for an in-flight IN-06 migration.** Rejected: IN-06's current plan (`specs/IN-06-webhook-gateway-router/plan.md`) does not introduce a migration ("No new migrations" — IN-06 plan §"Constitution Check / II"). 0040 is therefore unreserved; using 0039 keeps the sequence dense.
- **Use 0041 to match `source.md` literally.** Rejected: leaves 0039 and 0040 as gaps, which the schema-drift script would not flag but which mislead future readers about the merge order.

---

## R3 — Cache architecture (single-tier vs two-tier)

**Decision**: Single in-process TTL LRU (`collections.OrderedDict` + `time.monotonic`), TTL = 300 s, `max_entries = 4096`, with negative caching of `UnknownInstallation` outcomes.

**Rationale**: SC-009 sets a 2 ms p95 hit-path target. Redis RTT alone in a containerized deployment is typically 0.5–2 ms before any network jitter, so a Redis-only design cannot reliably meet 2 ms. A two-tier (in-process L1 + Redis L2) design *could* meet the hit-path target on L1 hits but adds: a new runtime dep, a new failure mode (Redis unavailability), an additional invalidation surface (process-local invalidation no longer suffices — every process needs notified). Constitution §X is explicit: "Don't introduce a config knob, a feature flag, or a plugin point without a current second caller or a written reason." A single in-process tier per process meets the hit-path target on its own; SC-004 (95% hit rate after warmup) is achievable per-process because the installation count is small (low thousands) and the cache is sized to fit the entire working set.

**Alternatives considered**:

- **Redis L2 tier in addition to in-process.** Rejected for IN-07; reconsider if SC-004 is missed in production by ≥5 percentage points or if installation count grows past ~10× current expectation. The upgrade path is straightforward: wrap the existing LRU's miss path with a Redis check before going to Postgres.
- **No cache at all; rely on connection-pooled async Postgres latency.** Rejected: the 2 ms hit target is not survivable with one Postgres round-trip per webhook even on a perfectly tuned local cluster, and Slack delivers retries aggressively. SC-009 is unmeetable without caching.
- **`functools.lru_cache` on a wrapper function.** Rejected: no TTL support, no explicit invalidation. Both are hard requirements (SC-006 needs ≤5 s consistency from admin action).

---

## R4 — Admin interface form factor

**Decision**: One Python service function (`TenantResolver.register_installation` etc.) plus a CLI script (`scripts/webhook_install.py`) that wraps it. No FastAPI admin endpoint.

**Rationale**: FR-007 says "CLI **or** admin HTTP endpoint." Today there is exactly one caller — a Company OS operator at a shell. Constitution §X: "Don't introduce a layer of indirection ... when a direct call works." An admin HTTP endpoint requires: an auth middleware path that ties into the existing operator-auth (which uses the demo-config / privileged-caller pattern), Pydantic request models for the wire format, route registration in `services/gateway/main.py`, and an integration test for the new route. None of that is needed for FR-007 today; a CLI satisfies FR-017's "operator-level authorization" by virtue of shell access. The OAuth-callback caller noted in spec A10 is explicitly out of scope; when it lands, it will call the service function directly in-process (not via HTTP), so the function form is the load-bearing one.

**Alternatives considered**:

- **Admin HTTP endpoint mounted on the gateway, behind operator auth.** Rejected for IN-07. Re-evaluate when (a) the OAuth callback flow lands AND wants to call this from another service, OR (b) self-serve tenant admin lands and needs an authenticated wire format.
- **Both CLI and HTTP endpoint at once.** Rejected: textbook §X violation. One caller, one surface.

---

## R5 — Error taxonomy (value vs exception)

**Decision**: Resolver outcomes are a Pydantic **discriminated union** (`ResolverOutcome = Resolved | UnknownInstallation | PayloadMissing`). Admin-path errors are `CompanyOSError` **subclasses** (`InstallationConflictError`, `InstallationNotFoundError`).

**Rationale**: Constitution §VIII gives the rule: "Add a new error class when (a) call sites need to branch on type, not message, OR (b) a structured code will be read by an external consumer."

- For the resolver, the caller (IN-06's router) branches on **value** (the discriminator field), not on Python type. The three outcomes are routine — there is nothing exceptional about an unknown installation. Modeling them as exceptions would force the router to write `try/except` for the normal path, which is the canonical anti-pattern §VIII is trying to prevent. The discriminated union also gives the JSON wire shape for free (Pydantic serializes the discriminator) and enables `assert_never` exhaustiveness in mypy.

- For the admin path, the call site is the CLI (or future OAuth callback). The two failure modes — conflict and not-found — are genuinely exceptional from the caller's perspective; they want to bubble out to the operator with a stable `code`. §VIII case (a) applies: the caller branches on type (`except InstallationConflictError`), not on a returned value.

**Alternatives considered**:

- **All-exception taxonomy** (six classes — one per resolver outcome + two admin). Rejected: forces try/except on the normal path, violates §VIII's intent.
- **All-value taxonomy** (resolver returns a union, admin also returns `Result[OK, ConflictReason | NotFoundReason]`). Rejected: admin path call sites in Python idiomatically expect exceptions for "I asked you to do a thing and you couldn't"; forcing them through a Result type adds friction without a payoff.
- **One outcome class with a `reason: Literal[...]` field** (instead of a discriminated union). Rejected on a tie-break: the discriminated union gives mypy exhaustiveness checks via `assert_never`, which a single-class-with-literal does not. Both shapes serialize the same; the union shape is one extra import-line and otherwise identical.

---

## Open questions deferred to implementation

None. The five questions above cover the entire plan-phase decision space. Phase 6 is mechanical from here.
