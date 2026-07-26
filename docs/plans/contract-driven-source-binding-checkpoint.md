# Contract-Driven Source Binding Checkpoint

Date: 2026-07-26

The commit containing this document is the checkpoint. Resolve it with:

```bash
git log -1 --format=%H -- docs/plans/contract-driven-source-binding-checkpoint.md
```

## Goal

Make one validated `SourceDefinition` / `ProviderDefinition` catalog own every
Fyralis source binding, remove legacy registries and source switches, and
certify all 27 canonical sources through Provider Lab, full ingestion, safe
throughput tests, and low-rate real-provider canaries.

Completion still means 27/27 sources certified and a final contract-only run
after all compatibility paths have been deleted.

## Where We Are

- The canonical catalog covers all 27 sources. Twenty-six have historical
  ingestion; WhatsApp is explicitly live-only.
- Historical certification has source-owned fixture factories, exact
  Observation-count oracles, and installation seeders for all 26 sources.
- Validation Runs 1–4 derive their source matrix from the catalog. Run 4
  models 52 historical installations plus two WhatsApp live targets.
- Installation lookup, operational status, retries, and synthetic attribution
  use exact tenant + installation identities. Ambiguous sibling binding fails
  closed.
- Durable shard scheduling supports fair waves, leases, and `RetryLater`
  persistence instead of hot loops.
- Provider requests use `ProviderTransport`; CI now detects raw outbound
  bypasses. The AWS AssumeRole fallback found by this rule was removed.
- Quota configuration is linked to 165 contract-owned opaque operation
  references and an exact catalog hash. Deployment still supplies verified
  limits and evidence; no provider quotas were guessed.
- Webhook ingress metadata and GitHub normalizer header projection are
  contract-owned. Embedding terminal failures derive DLQ attribution from all
  27 source contracts.
- Versioned evidence packs exist for every source, but intentionally remain
  unverified. Release readiness is therefore **0/27**, not certified.
- Current checkpoint gates passed:
  - 461 contract, certification, normalizer, transport, validation, and
    architecture tests; four Docker-dependent tests skipped.
  - 76 Postgres onboarding, scheduling, reconciliation, lease, and
    installation-management tests.
  - 238 source-contract and Provider Lab tests.
  - 27 webhook cutover tests and 15 backfill-harness integration tests.
  - Strict architecture, generated-catalog, lifecycle, Ruff, and diff checks.

## Remaining Work

1. Lock documented/observed evidence for every source. All 27 packs still need
   verified dates, schema hashes, quota evidence, credentials, and approvals.
2. Build the real `Source Certification Evidence` workflow. It must execute
   correctness, load, soak, recovery, and canary bindings and emit all 27 input
   artifacts; a synthetic or fabricated producer is not acceptable.
3. Upgrade the certification harness to prove two tenants, multiple
   installations per tenant, and at least two Fyralis replicas. Setup still
   seeds installation rows directly instead of exercising every OAuth/auth
   boundary.
4. Run the complete Kafka + S3 + Postgres chain for Runs 1–4. The local Kafka
   service was unhealthy/not host-exposed, so the full concurrent E2E run and
   Docker-dependent cases have not been certified.
5. Execute provider-safe load search, quota-disabled Fyralis ceilings,
   15-minute stable runs, 60-minute soaks, burst/recovery tests, and combined
   live/backfill workloads for every source.
6. Run all 27 low-rate real-provider canaries using dedicated accounts. Missing
   credentials or partner entitlement remains a blocker, not a waiver.
7. Move remaining provider-specific gateway behavior behind contract callables:
   GitHub ping/replay and lifecycle filtering, Slack lifecycle/URL
   verification, QuickBooks realm fanout, provider secret selection, and
   remaining BYOC/fetcher client switches.
8. Only after the above passes in contract-only mode, delete compatibility
   facades, old registries/mock paths, migration flags, and all remaining
   legacy source dispatch; rerun the complete matrix from the final commit.

## Recommended Next Session

Start by reading this file, then:

1. Bring up an isolated healthy Kafka and moto/S3 endpoint accessible to the
   host test runner.
2. Run the full contract-derived Run 4 and record the first real failure.
3. Extend the harness for same-tenant sibling installations and two replicas.
4. Define executable correctness/load/canary bindings for one Wave-A reference
   source, then generalize that evidence-producing path across all 27 sources.
