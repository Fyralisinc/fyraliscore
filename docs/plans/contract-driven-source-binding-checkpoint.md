# Contract-Driven Source Binding Checkpoint

Date: 2026-07-27

The commit containing this document is the checkpoint. Resolve it with:

```bash
git log -1 --format=%H -- docs/plans/contract-driven-source-binding-checkpoint.md
```

## Goal

Make one validated `SourceDefinition` / `ProviderDefinition` catalog own every
Fyralis source binding, remove parallel registries and source switches, and
certify all 27 canonical sources through Provider Lab, full ingestion,
provider-safe throughput tests, and low-rate real-provider canaries.

## Current Outcome

The contract-driven local runtime and its exact raw-to-T1 pipeline proof are
implemented for all 27 sources. The release milestone is **not complete**:

- Local pipeline boundary: **27/27 passed**.
- Applicable pipeline scenarios: **133 passed**.
- Remaining source-specific scenario ledger: **225 blocked, 0 failed**.
- Transport enforcement: **27/27**.
- Verified provider-evidence readiness: **0/27**.
- Real-provider canaries: **0/27** in this environment.
- Quota-aware throughput/soak certification: not runnable without verified
  provider quotas and the concrete exact-pipeline load adapter.

No blocked item has been converted into a synthetic pass.

The 225 blocked rows comprise 198 local harness/observability gaps and 27
provider-issued credential-expiry/refresh scenarios. The local rows are 27
each for pagination/resume, lifecycle, out-of-order delivery, distributed
429 handling, timeout/disconnect recovery, and hydration-failure cursor
invariance; 34 named source-specific scenarios; and two WhatsApp live-worker
attribution scenarios.

## Completed at This Checkpoint

### One contract-owned source runtime

- The catalog contains exactly 27 stable canonical IDs. Twenty-six sources
  support historical ingestion; WhatsApp explicitly declares `history=None`.
- The catalog owns aliases, capabilities, normalizers, idempotency builders,
  installation adapters, planners, fetchers, reconcilers, live ingress,
  request policies, endpoint resolution, secrets, runtime launchers,
  onboarding, browser recipes, and certification bindings.
- Provider Lab and the catalog agree on all 27 sources and 166 outbound
  operation policies.
- Every source has one generated certification surface, evidence pack, and
  execution binding. CI runs 27 single-source shards, not a parallel hand-kept
  source list.
- Generated catalog, surface, evidence, and execution-binding artifacts are
  included in package data and pass their `--check` gates.
- The strict source-architecture ratchet reports zero legacy findings.

### Exact installation identity and durable scheduling

- Migration `0196_exact_installation_scope_uniqueness.sql` closes ambiguous
  sibling-installation scopes.
- Onboarding, history planning, retry scheduling, reconciliation, synthetic
  seeding, and tenant resolution preserve the exact tenant and installation
  identity.
- The harness proves two tenants × two exact installations × two replicas for
  every historical source and records durable per-replica participation.
- Replica startup is fenced until every workflow process has published its
  durable heartbeat. A dead required process fails immediately.
- Tenant onboarding safely consumes completion signals whose run or source row
  was deleted, preventing a shared-inbox crash loop.
- Durable retries use explicit next-attempt state; fair selection and
  owner/version-aware leases are covered by PostgreSQL workflow tests.

### Required-data commit boundaries

- The normalizer flushes normalized output to Kafka before committing its raw
  input offset.
- A Kafka generation change after normalized output or Observation persistence
  is a replayable at-least-once handoff, not a data-loss window or shutdown
  crash.
- Replay certification selects one deterministic raw delivery for every
  unique, successfully normalized S3 parent. It verifies exact topic growth
  while Observation and T1 identity sets remain unchanged.
- Figma certification provisions both its raw-evidence and durable-artifact
  buckets.
- Gmail history-token expiry recovers without advancing past missing data.

### Facebook Pages token lifecycle

- Migration `0198_facebook_pages_token_lifecycle.sql` adds exact-installation
  connection state, durable recovery scheduling, lease state, User-token
  expiry, and controlled error metadata.
- Recovery follows the supported Meta flow: a still-valid long-lived User
  token re-derives the exact Page token through `/me/accounts`.
- Missing or expired User credentials transition to
  `reauthorization_required`; Fyralis does not invent a Page-token refresh
  grant.
- Public installation-status keys remain compatible while the database avoids
  plaintext credential-like metadata names.

### Provider Lab and certification harness

- Production clients run against Provider Lab through explicit loopback
  endpoint overrides.
- The lab covers the used REST, GraphQL, OAuth, webhook, WebSocket/protocol,
  pagination, cursor, quota, fault, and request-ledger surfaces.
- Calibration requires both the configured time window and minimum sample
  count, removing a short-window false pass.
- `pipeline_load_runner.py` provides the typed offered-load search framework
  and fails closed with `exact_pipeline_adapter_absent` when no concrete
  pipeline adapter is supplied. Provider Lab request throughput is not
  mislabeled as end-to-end Fyralis throughput.
- An exact adapter cannot be added as a thin factory against the current
  protocol. Catalog load mixes contain semantic control/downstream operations
  such as planning, verification, persistence, reconciliation, and watch
  renewal, while the adapter currently requires every accepted item to emit
  one raw S3 object and one raw Kafka record. The protocol first needs typed
  executable operation bindings, operation-specific output/cursor semantics,
  verifiable receipts, quota-bucket mappings, a long-lived trial supervisor,
  and durable event-to-replica attribution.

## Final Local Pipeline Matrix

Run 5 executed every generated local-correctness binding sequentially against
dedicated PostgreSQL, Kafka, moto/S3, Redis, and Provider Lab services:

- 27/27 source probes passed.
- 133 applicable pipeline scenarios passed.
- 2,370 expected Observations were persisted and identity-checked.
- Twenty-six historical sources passed five scenarios with two replicas.
- WhatsApp passed its three applicable live-only scenarios with no fabricated
  historical claim.
- Zero pipeline errors and zero cleanup failures were reported.

Detailed per-source counts and timings are in
`docs/validation/path_i/run5_contract_pipeline_report.md`.

## Validation Gates

| Gate | Result |
|---|---|
| Run 1 — all-source backfill + live | Historical snapshot: 54 tenants; 77.6s |
| Run 2 — transient faults + partition bounds | Historical snapshot: 54 tenants; 27 self-heals + 27 DLQs; 69.7s |
| Run 3 — sibling installs + two replicas | Historical snapshot: 52 tenants / 104 installs; 79.9s |
| Run 4 — production clients + Provider Lab + Kafka live | Historical snapshot: 1,454 Observations; zero duplicates; 68.7s |
| Run 5 — 27 generated execution bindings | 27/27 probes; 133 scenarios; 2,370 expected Observations |
| Source contract | 192 passed |
| Provider Lab | 95 passed |
| Source certification | 220 passed |
| Exact-installation / scheduling / migration database gate | 108 passed |
| Validation / backfill harness / fixtures | 194 passed; 7 opt-in E2E tests skipped because Run 5 exercised the stronger all-source path |
| General and source architecture ratchets | passed; source ratchet 0 findings |
| Import contracts | 7 kept, 0 broken |
| Catalog / surface / execution generation | current |
| MkDocs strict + YAML + TOML | passed |
| Ruff + diff hygiene | passed |

Canonical historical reports:

- `docs/validation/path_i/run1_report.md`
- `docs/validation/path_i/run2_report.md`
- `docs/validation/path_i/run3_report.md`
- `docs/validation/path_i/run4_report.md`
- `docs/validation/path_i/run5_contract_pipeline_report.md`

Runs 1–4 predate the final broker-ack, replay, startup-barrier, and Facebook
lifecycle changes. Run 5 is the current working-tree pipeline proof. It is not
a signed clean-commit release artifact.

## Remaining Local Engineering

1. Redesign the load contract so its semantic operation mix is executable and
   operation-aware, then implement the concrete `PipelineBoundaryAdapter`.
   Direct raw injection or echoing an operation ID cannot truthfully stand in
   for historical planning, live verification, reconciliation, persistence,
   or renewal.
2. Produce accepted artifacts for the remaining 225 source-specific scenarios,
   including auth expiry, pagination/delta expiry, webhook replay/order,
   source-specific fault recovery, renewals, and protocol behaviors.
3. Run provider-safe, quota-disabled ceiling, burst/recovery, combined
   live/backfill, 15-minute stable, and 60-minute soak workloads after verified
   quota configuration exists.
4. Confirm the Provider Lab remains at least 2× faster than each offered load
   and below the required client-timeout p99 ratio during those suites.
5. Retire the remaining development-only compatibility router and any
   contract-replaced test fallback only after the external certification gates
   pass. Do not perform final P9 deletion earlier.

## External State Required

- Evidence readiness is **0/27**.
- There are **81 unverified facts**: one `used_api_surface`, `quota_policy`,
  and `fyralis_runtime_contract` assertion for each source.
- This environment contains no configured per-source canary credential set.
- `FYRALIS_PROVIDER_QUOTAS_JSON` is absent.
- The 27 canaries declare 239 operations. Of those, 84 remain unclassified
  for mutation/cleanup semantics across 26 sources, so credentials alone
  would not make those canaries promotable.
- Thirty-eight source/quota-bucket pairs still need verified quota
  configuration.
- Dedicated disposable provider accounts, secrets, partner approvals, and
  entitlements are required for all 27 low-rate canaries.
- Saturation against live providers remains prohibited unless the provider's
  sandbox terms explicitly permit it.

Do not guess quotas, substitute generic development tokens, mark local lab
results as live evidence, waive an inaccessible source, or declare the
27-source release milestone complete while these conditions remain.

## Final P9 Sequence

After all local scenario/load gates and all external evidence/canary gates
pass:

1. Run the complete contract-only matrix from one clean commit.
2. Delete compatibility facades, contract-replaced fallback registries,
   development source switches, and obsolete mock paths.
3. Expand the architecture ratchet over every retired surface.
4. Rerun the local correctness matrix, provider-safe/ceiling/fault/soak suites,
   and all 27 low-rate canaries from the deletion commit.
5. Produce, verify, evaluate, and sign the exact-commit release manifest.

## Resume Commands

The dedicated local services used by this checkpoint are:

- Pipeline certification: PostgreSQL `127.0.0.1:55444`, Kafka
  `127.0.0.1:59092`, Redis `127.0.0.1:56379`, moto/S3
  `http://127.0.0.1:5601`.
- Database/unit gates: PostgreSQL `127.0.0.1:55445`, Kafka
  `127.0.0.1:59093`, Redis `127.0.0.1:56380`.

Start the next session with:

```bash
git show --stat --oneline HEAD
git log -1 --format=%H -- docs/plans/contract-driven-source-binding-checkpoint.md
COMPANY_OS_ENV=test PYTHONPATH=. .venv/bin/python \
  -m services.ingest.source_certification inventory --require-ready
COMPANY_OS_ENV=test PYTHONPATH=. .venv/bin/python \
  scripts/generate_source_catalog_artifacts.py --check
COMPANY_OS_ENV=test PYTHONPATH=. .venv/bin/python \
  scripts/generate_source_certification_surfaces.py --check
COMPANY_OS_ENV=test PYTHONPATH=. .venv/bin/python \
  scripts/generate_source_certification_execution_bindings.py --check
PYTHONPATH=. .venv/bin/python \
  scripts/check_source_architecture_ratchet.py --no-baseline
```

The expected inventory result remains blocked at 0/27 until verified provider
evidence, quota configuration, credentials, and live canary receipts are
supplied.

## Worktree Scope Kept Out of This Checkpoint

The checkpoint commit must not absorb unrelated pre-existing edits, including:

- `services/ingest/graphify-out/`
- `docs/diagrams/`
- the old source-connection and ingestion-wide audit documents
- unrelated source-resource RLS migrations/tests
- unrelated browser-storage ratchet edits
- unrelated Figma operations-document edits

`mkdocs.yml` and the BYOC onboarding router/tests require partial staging so
only the source-contract portions enter this checkpoint.
