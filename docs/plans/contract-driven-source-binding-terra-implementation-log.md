# Engineering Implementation Log

## Milestone

R0 — Reproducible, scoped baseline and inventory (complete)

---

## Objective

Establish an isolated implementation worktree from the contract-driven source
binding checkpoint, preserve the user's unrelated working-tree changes, and
record baseline validation before production changes begin.

---

## Tasks Completed

- Read the complete Engineering Handoff Document and Terra implementation
  brief.
- Recorded the documented baseline commit:
  `9a55c1d44e4fe84c550eb61a1df56a8d91e39d97`.
- Created the isolated implementation worktree
  `/tmp/fyralis-contract-driven-source-binding` on branch
  `terra/contract-driven-source-binding`.
- Confirmed the original worktree contains unrelated modified and untracked
  files listed in the handoff; no original file has been reset, staged, or
  changed by this milestone.
- Repaired the classified certification-surface determinism defect without
  changing runtime fixture recency. The surface generator now pins only the
  AWS, Grafana, and HiBob golden-fixture anchors, and a regression test proves
  that their checked-in artifacts do not vary with the runtime clock.
- Regenerated the three affected source-certification surface, evidence, and
  executable-binding artifacts.
- Created the path-level legacy-selector inventory at
  `docs/plans/contract-driven-source-binding-terra-legacy-inventory.md`.
- Ran the complete R0 catalog, Provider Lab, certification, attribution, and
  exact-installation/scheduler gate set from the isolated worktree.

---

## Decisions Made

- Use an isolated Git worktree for production changes. This preserves the
  user-owned dirty worktree while allowing baseline checks and implementation
  to be reproducible from the checkpoint SHA.
- Treat the handoff at
  `/home/prajwal-adhikari/Desktop/v2/fyraliscore/docs/plans/contract-driven-source-binding-terra-handoff.md`
  as immutable architecture input. It is intentionally not edited here.
- Pin checked-in golden fixture inputs in the generator rather than changing
  runtime fixture defaults. Runtime fixtures must remain recent so Provider
  Lab backfill windows exercise active data; generated certification surfaces
  must instead be reproducible at every calendar date.
- Use a dedicated disposable PostgreSQL instance at `127.0.0.1:55446` for
  migration-backed tests because the handoff's example port was unavailable
  and the existing development stack must not be truncated by test fixtures.

---

## Files Added

- `docs/plans/contract-driven-source-binding-terra-implementation-log.md` —
  chronological Terra implementation journal.
- `docs/plans/contract-driven-source-binding-terra-legacy-inventory.md` —
  frozen R0 inventory of source-binding selectors and their contract owners.

---

## Files Modified

- `scripts/generate_source_certification_surfaces.py` — adds fixed generation
  parameters for the three time-windowed golden fixtures.
- `services/ingest/source_certification/tests/test_surface_artifacts.py` —
  adds a regression test for clock-independent golden fixture generation.
- Generated AWS, Grafana, and HiBob certification surface, evidence, and
  execution-binding JSON artifacts — refreshed to match the deterministic
  generator output.

---

## Tests Executed

- Source catalog generation check: passed.
- Source certification surface generation check: passed after the generator
  fix.
- Source certification execution-binding generation check: passed after the
  generated bindings were refreshed.
- Source architecture ratchet (`--no-baseline`): passed with zero findings.
- Source-contract suite: `192 passed`.
- Provider Lab suite: `95 passed` when run with loopback socket permission.
- Fixture installation-isolation suite: `31 passed`.
- Attribution unit suite: `23 passed`.
- Source-certification suite: `241 passed, 1 skipped` after the deterministic
  surface fix.
- Ruff on the modified generator and regression test: passed.
- Disposable PostgreSQL exact-installation, scheduler, reconciliation, shard,
  and event-attribution migration gate: `67 passed in 469.99s`.
- Certification readiness command: expected non-zero result with
  `evidence_ready_sources=0`, `required_sources=27`, and state `blocked`.
- The isolated worktree has no local `.venv`; baseline commands use the
  project virtual environment by absolute path while source imports remain
  scoped to the isolated worktree.

---

## Problems Encountered

- The primary worktree is intentionally dirty with unrelated user work, so it
  cannot supply a reproducible implementation baseline directly.
- Checked-in AWS, Grafana, and HiBob certification surfaces were
  nondeterministic across calendar days. The surface generator called fixture
  factories with empty parameters; their runtime defaults intentionally use a
  recent current timestamp, so generated golden fixtures and their pinned
  evidence hashes drifted daily.
- Provider Lab tests require loopback sockets and cannot run inside the
  restricted execution sandbox. They pass when run with the required loopback
  permission.
- The migration-backed fixture performs a full schema application for each
  isolated test. The R0 database gate is therefore intentionally slow
  (7m50s), but it completed without a failure.

---

## Resolution

- Created the isolated worktree from the documented checkpoint. The original
  worktree remains untouched.
- Kept runtime fixture recency unchanged. Applied a generator-local fixed
  anchor only for the three checked-in golden fixtures, added a regression
  test, and regenerated the associated surface/evidence/binding artifacts.
  This preserves runtime time-window behavior and avoids a 27-source
  implementation-digest churn.
- Use the approved loopback-capable test execution only for tests that create
  local sockets.
- Ran database integration tests only against an isolated container started
  for this work; no shared service or user-owned data was changed.

---

## Assumptions

- The user intends implementation to proceed on the isolated
  `terra/contract-driven-source-binding` branch and expects the final commits
  and artifacts to be handed back without absorbing unrelated work.

---

## Deviations

No deviations.

---

## Next Planned Work

R1 — make typed executable operations the active execution-plan authority:
audit the existing certification execution driver, load search, and pipeline
runner; then implement contract-owned typed operation selection, declared
absence/non-applicability handling, and receipt requirements without creating
a parallel mutable source registry.

---

## Milestone

R1 — Complete typed executable-load integration (complete)

---

## Objective

Make typed executable load declarations, rather than the legacy
`operation_mix` projection, the authoritative source for source-certification
load scheduling, evidence, evaluation, and stage-artifact validation.

---

## Tasks Completed

- Made `LoadSuite.execution_workload_dict()` the canonical, hash-pinned typed
  workload declaration. It contains executable data/control operations,
  contract absences, and explicit non-applicability; `operation_mix` is now an
  optional read-only compatibility projection.
- Enforced typed suite invariants: applicable workloads require per-item data,
  historical operations require positive raw/normalized/Observation
  cardinalities, quota mappings, and cursor consistency, and combined renewal
  semantics are derived from typed operations or explicit absences.
- Added the pipeline-runner conversion from `LoadSuite` and a shared
  non-promoting configuration projection. The execution driver and stage
  verifier use the same topology, timing, search, and release settings.
- Changed the load driver to invoke `run_pipeline_load()` for every declared
  suite in both `provider_safe` and `fyralis_ceiling` modes. In R1, the absent
  R3 exact adapter produces sealed blocked artifacts; WhatsApp historical
  produces only its declared neutral `not_applicable` artifacts.
- Kept Provider Lab request-load measurements separate and explicitly
  non-promoting. They can diagnose strict route behavior but cannot substitute
  for typed callable invocation or raw-to-T1 receipts.
- Updated the evaluator to require typed workload binding plus full executable
  and control-operation coverage for a passing suite. Declared
  non-applicability is neutral, while undeclared non-applicability fails.
- Upgraded stage-artifact v3 validation to require the full six-artifact typed
  pipeline matrix, exact workload/configuration binding, and rejection of
  self-hashed promotion-eligible nested artifacts.
- Regenerated all 27 execution bindings and updated the load-runner and
  execution-binding documentation.
- Added all-catalog coverage proving that each of the 27 source definitions
  emits six typed pipeline artifacts through the R1 runner.

---

## Decisions Made

- Do not let Provider Lab HTTP traffic stand in for a data-plane receipt. It
  remains a diagnostic-only boundary until R3 supplies an exact pipeline
  adapter.
- Preserve compatibility labels for old readers, but allow the projection to
  be empty and exclude it from typed workload identity, callable selection,
  coverage, and promotion.
- Fail closed on any load artifact whose typed workload, configuration,
  topology, timing, or promotion state differs from the R1 contract.
- Keep R1 non-promoting even when a structurally valid self-hashed pipeline
  artifact claims promotion. A future release-capable schema must independently
  authorize that claim.

---

## Files Added

None.

---

## Files Modified

- `services/ingest/source_certification/models.py` — typed workload,
  non-applicability, historical-fetch, and compatibility invariants.
- `services/ingest/source_certification/pipeline_load_runner.py` — canonical
  suite conversion and shared R1 configuration projection.
- `services/ingest/source_certification/execution_driver.py` — six typed
  pipeline-runner invocations and separate Provider Lab diagnostics.
- `services/ingest/source_certification/load_search.py` — v3 typed diagnostic
  artifacts and typed coverage accounting.
- `services/ingest/source_certification/evaluator.py` and
  `stage_artifacts.py` — typed neutral-state, configuration, and promotion
  validation.
- Source-certification unit tests, all 27 generated execution bindings, and
  the two R1 documentation files.

---

## Tests Executed

- Focused typed-load suite: `122 passed, 1 skipped` (the skip requires the
  opt-in real Redis diagnostic).
- Full source-certification suite: `258 passed, 1 skipped`.
- Source-contract suite: `192 passed`.
- Provider Lab suite: `95 passed` with loopback socket permission.
- Source catalog, certification-surface, and execution-binding generator
  checks: passed.
- Source architecture ratchet (`--no-baseline`): passed with zero findings.
- Ruff over every changed Python module and test: passed.

---

## Problems Encountered

- The restricted execution sandbox cannot create the local TCP sockets used by
  Provider Lab transport tests.
- The repository has no R3 exact-pipeline adapter yet, so a truthful R1 load
  run cannot demonstrate end-to-end raw evidence, Kafka, Observation, and T1
  throughput.
- A read-only audit found that nested pipeline artifacts were not initially
  bound to the source suite's R1 configuration; it also found that a rehashed
  promotion-shaped artifact needed an explicit v3 rejection test.

---

## Resolution

- Ran Provider Lab tests with the required loopback-only execution permission;
  they passed without contacting an external provider.
- Kept the missing adapter explicit: pipeline artifacts remain blocked rather
  than falling back to Provider Lab or a synthetic pass path.
- Added shared configuration projection, exact nested-configuration validation,
  historical declaration checks, and self-hashed promotion-claim rejection
  tests before accepting the R1 milestone.

---

## Assumptions

- R3 will provide the exact `PipelineBoundaryAdapter`; R1 must not fabricate
  that capability.
- R5 will supply verified, source-specific quota configuration for promotable
  provider-safe runs.
- The eight renewal gaps remain explicit typed contract absences until R2
  supplies bounded callables.

---

## Deviations

No deviations.

---

## Next Planned Work

R2 — add bounded, exact-installation renewal executables for Gmail, Google
Calendar, Google Drive, QuickBooks, Ramp, Gusto, Carta, and LinkedIn; wire
their provider calls through `ProviderTransport`; prove persisted renewal,
retry/reauthorization behavior, and removal of the combined-suite renewal
blocker in Provider Lab.

---

## Milestone

R2a — Renewal catalog and certification wiring (complete)

---

## Objective

Make the eight contract-declared bounded renewal invokers resolvable at
runtime and executable as typed periodic controls in combined certification
workloads, without claiming that the complete durable R2 lifecycle milestone
has passed.

---

## Tasks Completed

- Added a public runtime resolver for contract-declared renewal invokers and a
  typed error for sources that intentionally declare no renewal.
- Included renewal invokers in the cached startup binding guard, so a missing
  module or renamed callable fails before the runtime accepts work.
- Added exactly one periodic
  `<source>.token_or_watch_renewal` combined control for Gmail, Google
  Calendar, Google Drive, QuickBooks, Ramp, Gusto, Carta, and LinkedIn.
- Bound each periodic control to its source-owned invoker, declared cadence,
  exact provider operation/quota mapping, and the required
  `binding_invocation`, `quota_mapping`, `renewal_state`, and
  `secret_redacted` receipt proofs.
- Removed the blocking renewal absence for those eight sources while
  preserving one explicit nonblocking absence for every source whose contract
  declares no renewal semantic.
- Added renewal invokers to the execution driver's source-owned callable
  inventory and exact execution-plan hash.
- Added focused runtime, startup-failure, catalog, quota-mapping, absence, and
  execution-plan tests.
- Regenerated all contract-hash-affected certification surfaces, evidence
  packs, and execution bindings with the repository generators.

---

## Decisions Made

- `SourceDefinition.renewal` is the only production selector. Certification
  derives periodic controls from that declaration rather than introducing a
  second source-to-invoker map.
- The certification operation ID remains the stable logical
  `<source>.token_or_watch_renewal`; quota evidence names the exact underlying
  provider operation such as `watch.create`, `events.watch`,
  `changes.watch`, `oauth.token.refresh`, or `oauth.token.mint`.
- Renewal is a periodic control and never contributes selection weight or
  offered data rate.
- A source without a renewal declaration keeps an explicit nonblocking
  absence. Absence is not relabeled as execution or success.

---

## Files Modified

- `services/ingest/source_contract/runtime.py` and
  `services/ingest/source_contract/__init__.py` — renewal resolution, startup
  validation, errors, and public exports.
- `services/ingest/source_certification/catalog.py` — eight exact periodic
  combined controls and nonblocking absence behavior.
- `services/ingest/source_certification/execution_driver.py` — renewal
  callable inventory and execution-plan binding.
- Focused source-contract and source-certification tests.
- Generated source-certification surfaces, evidence packs, and execution
  bindings.

---

## Tests Executed

- Focused runtime/certification/driver set: `121 passed, 1 skipped`; the skip
  is the existing opt-in real-Redis diagnostic.
- Full source-contract plus source-certification suites:
  `454 passed, 1 skipped`.
- Source-catalog, certification-surface, and execution-binding generator
  checks: passed.
- Ruff check and format validation over the R2a Python changes: passed.

---

## Problems Encountered

- The first focused run found all three Google renewal bindings unresolved
  while their source-specific invokers were still being added in parallel.
  This produced five fail-closed test failures and no false pass.

---

## Resolution

- Reran the exact focus set after the Gmail, Google Calendar, and Google Drive
  invokers landed. Runtime startup validation and every source-isolated
  callable probe then passed.
- Regenerated the hash-pinned artifacts only after all eight declared
  invokers resolved from the shared source state.

---

## Assumptions

- Full R2 completion remains gated on durable renewal jobs, source-specific
  ProviderTransport execution, lifecycle/fault evidence, Provider Lab
  behavior, and combined validation owned by the remaining R2 work.

---

## Deviations

No deviations.

---

## Next Planned Work

Complete and validate the remaining R2 durable job, provider invoker,
Provider Lab lifecycle, retry/reauthorization, and combined execution work
before marking R2 itself complete.

---

## Milestone

R2b — Source-neutral durable renewal-job substrate (complete)

---

## Objective

Provide the small, durable database boundary needed by every bounded watch or
credential renewal without introducing a second source registry or storing
provider credentials in scheduling state.

---

## Tasks Completed

- Added `source_renewal_jobs`, keyed by the exact source, tenant,
  installation, and non-secret target selector.
- Added durable next-attempt scheduling, attempt/success timestamps, bounded
  controlled error codes, explicit reauthorization state, known expiry
  metadata, and no secret/payload/detail fields.
- Added source-independent claim, heartbeat, complete, defer, reauthorization,
  and exact-get operations. Each operation opens only a short RLS-bound
  transaction; provider work happens outside the substrate.
- Implemented owner/version lease generations, recovery after expiry, and
  strict live-lease fencing for complete/defer/reauthorization writes. A stale
  worker cannot commit after a takeover.
- Added a durable due/fairness index and a lease-expiry recovery index for the
  future periodic runner.
- Added strict tenant RLS with no unbound-context bypass.
- Added focused migration and integration coverage for idempotence, RLS,
  durable no-hot-loop scheduling, heartbeat, retry, reauthorization, and
  stale-generation rejection.

---

## Decisions Made

- The job table stores only a constrained Fyralis error code. It deliberately
  has no free-form failure text, provider response, secret reference, or token
  column.
- `target_key` belongs in the primary key because a resource-scoped Google
  watch and an installation-scoped OAuth refresh must never compete for a
  lease merely because they share a tenant/install pair.
- Heartbeat uses the current owner/version fence so a worker can recover a
  narrowly expired lease only when no replacement has won. Terminal writes
  additionally require an unexpired lease, which fails closed if the provider
  call outruns its heartbeat.
- Reauthorization clears `next_attempt_at`; it cannot silently re-enter a
  retry loop until the owning installation is repaired and explicitly
  reintroduced by its lifecycle path.

---

## Files Added

- `db/migrations/0200_source_renewal_jobs.sql` — exact, RLS-protected durable
  renewal metadata and lease schema.
- `services/ingest/ingestion/renewal_jobs.py` — source-neutral async durable
  job operations.
- `services/ingest/ingestion/tests/test_renewal_jobs.py` — focused lifecycle
  and migration coverage.

---

## Tests Executed

- Ruff and Python compile checks for the new module and tests: passed.
- Focused isolated PostgreSQL integration suite:
  `4 passed in 6.31s` against the disposable Terra database on port `55446`.

---

## Problems Encountered

- The previously suggested local PostgreSQL port `55434` was not listening.
  The first restricted test attempt also could not create local TCP sockets.

---

## Resolution

- Located the existing isolated `fyralis-terra-r0-db` container on port
  `55446` and ran the focused suite there with loopback permission. No shared
  development database or source provider was used.

---

## Assumptions

- Source-specific lifecycle invokers pass only an exact `RenewalJobKey` and a
  declared controlled error code to this substrate.
- Installation tables remain the sole authority for credential references and
  provider-specific watch state; a later source lifecycle path handles an
  administrator's successful reauthorization.

---

## R2b Addendum — Provider Lab renewal lifecycle (complete subtask)

### Objective

Make the local Provider Lab capable of deterministic before-expiry, renewal,
and after-expiry behavior for the eight R2 renewal sources while preserving
the static default fixtures used by existing client-conformance tests.

### Tasks Completed

- Added an opt-in `renewal_lifecycle` fixture state. It is inactive unless a
  source test explicitly sets `enabled: true`.
- Passed the Lab's existing virtual clock into every provider request, so the
  lifecycle implementation never reads wall-clock time.
- Added virtual-time/scope-derived DWD access-token responses and dynamic
  watch expiry/resource metadata for Gmail, Google Calendar, and Google Drive.
  Their default token and year-2100 watch fixtures remain unchanged.
- Added virtual-time/scope-derived credential responses for QuickBooks, Ramp,
  Gusto, Carta, and LinkedIn. QuickBooks, Gusto, and LinkedIn validate a live
  configured or previously renewed refresh credential; Ramp and Carta validate
  the client-credentials grant.
- Added live-access validation on the declared resource routes in lifecycle
  mode. An expired or wrong-scope lifecycle access credential receives a 401;
  a renewed credential succeeds.
- Used opaque, self-validating Lab-only identifiers with bounded expiry rather
  than persisting provider-like secrets or adding a mutable token registry.
- Added focused coverage for all eight sources, including scope variance,
  watch re-renewal after virtual-time advance, expired old OAuth refresh and
  access credentials, recovery using a renewed refresh credential, and
  client-credential re-minting.

### Files Added

- `services/ingest/synthetic/provider_lab/renewal_lifecycle.py` — opt-in
  virtual-clock lifecycle helper.
- `services/ingest/synthetic/provider_lab/tests/test_renewal_lifecycle.py` —
  eight-source lifecycle coverage.

### Files Modified

- `services/ingest/synthetic/provider_lab/protocol.py` and `app.py` — carry
  deterministic virtual time into adapters.
- `services/ingest/synthetic/provider_lab/adapters.py`, `wave_b.py`, and
  `wave_cd.py` — activate lifecycle behavior only for the eight declared R2
  sources.

### Tests Executed

- Focused lifecycle suite: `9 passed in 0.64s`.
- Full local Provider Lab suite with loopback transport: `104 passed in 5.79s`.
- Ruff and Python compile checks across the Provider Lab lifecycle changes:
  passed.

### Scope Boundary

This is a Provider Lab subtask only. It does not mark R2 complete: final R2
still requires source-invoker lifecycle integration evidence, combined
certification execution, and broader failure/recovery gates.

---

## Milestone

R2 — Bounded renewal executables for eight sources (complete)

---

## Objective

Finish the handoff-defined, contract-referenced renewal path for Gmail,
Google Calendar, Google Drive, QuickBooks, Ramp, Gusto, Carta, and LinkedIn.
The path must use exact tenant and installation identity, the universal
provider transport, durable schedule/lease state, and a deterministic Provider
Lab lifecycle without exposing provider secrets.

---

## Tasks Completed

- Declared one `RenewalDefinition` per R2 source in the canonical source
  catalog, including cadence, lease scope, primary operation, runtime invoker,
  and Google renewal child operations.
- Added source-neutral durable renewal jobs keyed by source, tenant,
  installation, and non-secret target key, with RLS, lease generations,
  heartbeats, retry scheduling, reauthorization, and explicit manual
  reconciliation for ambiguous unsafe outcomes.
- Bound every watch and credential renewal invoker to the exact installation;
  no renewal path selects a latest or arbitrary active installation.
- Routed each actual renewal request through `ProviderTransport`, including
  Google DWD token exchange and Calendar/Drive `channels.stop` cleanup.
- Added fair watch and credential scheduler selection. Future cooldowns and
  terminal jobs do not occupy the first candidate batch or starve a later
  tenant.
- Implemented Provider Lab lifecycle behavior and source-invoker integration
  checks for all eight sources: before expiry, renewal, and after expiry.
- Mapped definite DWD authorization rejections (`400`, `401`, `403`) to a
  durable exact-installation reauthorization state before an unsafe watch
  create can occur. DWD response bodies remain excluded from errors and
  durable state.
- Added stale-heartbeat/takeover protection: an expired lease that crossed an
  unsafe provider boundary becomes manual reconciliation rather than being
  replayed by another worker.
- Generated and checked catalog, certification-surface, and execution-binding
  artifacts. The combined certification declarations no longer have a missing
  renewal semantic for any R2 source.

---

## Decisions Made

- Kept the durable job table source-neutral. Provider credentials, refresh
  references, watch metadata, and source-specific resource state remain in
  their existing installation/source tables.
- Treat a known authorization rejection before an unsafe operation as
  reauthorization-required. Treat an unknown outcome after an unsafe token
  rotation or watch creation as manual reconciliation, rather than guessing it
  is safe to retry.
- Allow a lease owner to recover a narrowly expired lease only when no other
  worker has acted; a replacement observation after an unsafe marker
  terminalizes the job. This preserves fencing without turning normal
  scheduler jitter into duplicate work.
- Test real production source invokers against the strict used API surface of
  Provider Lab. The only injected boundary is the intended local test
  transport, not a source-specific renewal shortcut.

---

## Files Added

- `db/migrations/0200_source_renewal_jobs.sql` — durable exact-identity
  renewal schedule and fencing schema.
- `services/ingest/ingestion/renewal_jobs.py` — source-neutral job/lease API.
- `services/ingest/ingestion/tests/test_renewal_jobs.py` — migration, RLS,
  durability, fencing, and terminal-state coverage.
- `services/ingest/integrations/bounded_renewal.py` — common bounded renewal
  envelope for source-owned invokers.
- `services/ingest/integrations/oauth_renewal.py` — five catalog-bound
  credential-renewal wrappers.
- `services/ingest/ingestion/workflows/credential_renewal_scheduler.py` and
  `scripts/run_credential_renewal_scheduler.py` — fair durable credential
  scheduler and launcher.
- `services/ingest/synthetic/provider_lab/renewal_lifecycle.py` — deterministic
  opt-in Provider Lab lifecycle state.
- Focused renewal, scheduler, and Provider Lab test modules for the above.

---

## Files Modified

- Source-contract models, catalog, runtime resolution, generated artifacts,
  and certification catalog/driver — make renewal a validated contract-owned
  executable rather than a side registry.
- Google/Gmail watch implementations, Gmail DWD/client handling, and OAuth
  refresh handling — enforce bounded renewal, exact identity, transport use,
  redaction, and durable outcomes.
- Provider Lab adapters/runtime/protocol — provide lifecycle behavior on the
  exact source APIs exercised by production clients.
- Process manifest, Compose, Prometheus, Google/Gmail launch scripts, and
  pgbouncer wiring checks — launch the declared renewal workers with the
  required provider transport runtime.
- Generated evidence, surfaces, and execution bindings — reflect the renewal
  operations from the canonical catalog.

---

## Tests Executed

- Durable renewal-job PostgreSQL suite, split only to stay within the local
  runner's command window: `10 passed`.
- Source-invoker Provider Lab lifecycle checks: `3 passed` for Gmail/Calendar/
  Drive and `5 passed` for QuickBooks/Ramp/Gusto/Carta/LinkedIn.
- DWD authorization-rejection integration coverage: `3 passed`.
- All-eight-source durable `RetryLater` coverage: `3 + 3 + 2 passed`.
- All-eight-source contract lease-concurrency coverage: `3 + 3 + 2 passed`.
- Actual credential-renewal concurrency coverage: `3 + 2 passed`.
- Gmail/Calendar/Drive sibling-installation isolation: `3 passed`.
- Unsafe-provider terminal behavior: `3 + 3 + 2 passed`; stale-heartbeat
  cancellation and replay blocking: `1 passed`.
- Watch fairness/exact-resource checks: `4 passed` and `2 passed`.
- Google push ingress/registration suite: `8 passed`.
- Full local Provider Lab suite: `110 passed`.
- OAuth refresh and finance transport units: `22 passed`.
- Catalog, runtime, certification, execution-driver, scheduler, and launcher
  checks: `192 passed, 1 skipped` (the expected real-Redis diagnostic without
  `FYRALIS_TEST_REDIS_URL`).
- Generator round-trip checks, Ruff, Python compile checks, and
  `git diff --check`: passed.

---

## Problems Encountered

- A single broad database test command can exceed the local command runner's
  reporting window even though individual test cases are bounded.
- The first stale-heartbeat test modeled an expired lease without a replacement
  observation. That is intentionally recoverable by the same owner and did
  not prove the unsafe takeover path.

---

## Resolution

- Ran the database checks in bounded, named groups; each group has a recorded
  final result above.
- Changed the heartbeat test to model the real failure sequence: mark an unsafe
  provider boundary, expire the lease, let a prospective replacement observe
  it, require manual reconciliation, and then prove the stale attempt is
  cancelled.

---

## Assumptions

- Provider Lab remains the permitted saturation/lifecycle environment. This
  milestone does not claim a real-provider canary or external load result.
- An operator or source lifecycle repair explicitly resumes a terminal renewal
  job only after the provider-side state is reconciled.

---

## Deviations

No deviations.

---

## Next Planned Work

Begin R3 exactly as specified in the handoff: implement the long-lived
exact-pipeline trial supervisor and adapter, with durable stage evidence and
no synthetic source-dispatch path.
