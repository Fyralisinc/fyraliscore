# ADR 0002 — Temporal: Cloud for Production, Local Dev Server for Development

**Status:** Accepted
**Date:** 2026-05-17
**Context:** Ingestion implementation-plan Q2; ingestion LLD §2; HLD open question 1
**Related work:** M2 (Temporal worker bring-up)

## Context

The ingestion pipeline relies on Temporal for durable orchestration:
the outbox poller workflow (LLD §2.2), TenantOnboardingWorkflow
(§2.3), SourceOnboardingWorkflow, ShardFetchWorkflow (§2.4), and
the FeelsOnboardedMonitorWorkflow (§2.6). M2 will introduce the
first worker registration; M3 onward fans out.

Two paths exist:

1. **Temporal Cloud** (managed by Temporal Technologies Inc.): a
   hosted, multi-tenant deployment with per-action billing. mTLS
   client certs; namespaces map to environments.
2. **Self-hosted Temporal Server:** run the matching, history,
   frontend, and worker services ourselves; bring our own
   Cassandra / Postgres backing store; bring our own Elasticsearch
   for advanced visibility.

The decision lands now because M2 needs a target endpoint for
worker registration code.

## Decision

- **Production:** Temporal Cloud.
- **Staging:** Temporal Cloud (separate namespace).
- **Development:** Local Temporal dev server (`temporal server
  start-dev`).

## Rationale

- **SRE burden.** Self-hosted Temporal is a sustained operational
  load. The control plane has five service components (matching,
  history, frontend, worker, web UI) plus a backing store and
  Elasticsearch. Operating that competently means on-call rotation
  for it, upgrade cadence (Temporal ships frequently), version
  matrix testing between server and SDK, certificate rotation,
  capacity planning. At our scale none of that buys business value
  the way the per-action Cloud price does.
- **Time-to-first-workflow.** Cloud namespace provisioning is
  hours. Self-hosted from-scratch is weeks (and that's before
  hardening). M2 cannot wait on infra.
- **Cost economics at our scale.**
  - Self-hosted Cassandra cluster + worker pool + ES, even
    conservatively sized, costs ~$1.5k/mo in cloud infrastructure
    before any engineer time.
  - Temporal Cloud's per-action pricing scales with actual
    workflow volume; projected production cost for the first
    paid tenants is <$100/month. Crossover with self-hosted
    happens far above current planned scale.
- **Vendor lock-in is mitigated by SDK portability.** The
  workflow code (`@workflow.defn`, `@activity.defn`) is the same
  whether it talks to Cloud or self-hosted. If a future migration
  is needed, it is a config change at worker startup, not a
  rewrite. We commit to NOT using Cloud-exclusive features (e.g.
  Cloud's experimental scheduling extensions); all features used
  must be part of the open-source server's surface.

## Cost Reality

| Environment | Temporal target | Cost |
|---|---|---|
| Dev (every laptop) | `temporal server start-dev` (local) | $0 |
| CI | `temporal server start-dev` (ephemeral, in-test) | $0 (compute already paid) |
| Staging | Temporal Cloud — own namespace | Free/starter tier (verify current terms at temporal.io/cloud/pricing) |
| Production | Temporal Cloud — own namespace | Per-action pricing; projected <$100/mo at M5 cutover scale |

## Configuration

- **Namespaces:**
  - `fyralis-dev` (local; per-developer)
  - `fyralis-ci` (CI; ephemeral)
  - `fyralis-staging`
  - `fyralis-prod`
- **Auth:**
  - Dev: insecure local connection on `localhost:7233`.
  - Staging/Prod: mTLS client certs (Cloud's standard). Certs
    rotated via the same secret-rotation runbook used for other
    service credentials.
- **Connection string:** environment variable `TEMPORAL_ADDRESS`
  (and `TEMPORAL_NAMESPACE`, `TEMPORAL_TLS_CERT`,
  `TEMPORAL_TLS_KEY`). Code reads from env; no hardcoded
  endpoints. Same binary targets dev/staging/prod by config.
- **Workflow history retention:** 30 days. Sufficient for
  post-mortem on a multi-day backfill; aligns with raw-tier S3
  retention (LLD §5.1).
- **Region:** same region as primary Postgres + Kafka. Cross-region
  latency between worker and Temporal is the dominant cost in a
  hot workflow loop.
- **SDK pin:** `temporalio>=1.5` (M1.4); upgrade quarterly.

## Trade-offs Accepted

- **Per-action pricing scales with volume.** A pathological
  workflow that loops without progress could rack up Cloud
  costs. Mitigation: every workflow has a sane retry policy and
  bounded child-workflow fan-out (LLD §2.4). Cloud billing
  alerts fire if cost exceeds threshold.
- **Network dependency on a third party.** Cloud outages would
  pause new workflow starts and signal delivery. Mitigation:
  webhook + Gateway hot paths remain inline-decoupled from
  Temporal (LLD §11 feature flag); a Cloud outage degrades
  backfill / observation-write progress, not the inbound edge.
- **Data residency.** Workflow inputs/outputs and history live in
  Cloud. Inputs are tenant-scoped pointers (e.g., outbox row
  IDs) rather than tenant content; this is acceptable for the
  current customer base. Revisit if a compliance requirement
  arises.

## When to Revisit

- **Monthly Cloud bill exceeds ~$10k/mo.** Crossover with
  self-hosted infrastructure approaches; do the analysis
  formally then.
- **Customer contract requires data residency in a region Cloud
  does not serve.** Self-hosted in that region becomes the only
  option for that customer. Could mean a two-deployment posture:
  Cloud for most, self-hosted for the residency-constrained
  customer.
- **Workflow latency requirements emerge that Cloud cannot meet.**
  E.g., a sub-100ms p99 worker-to-server round-trip would push
  toward co-located self-hosted. Not in scope for v1.

## Out of Scope

- Provisioning Cloud accounts, namespaces, billing setup,
  TLS-cert minting. That's deployment / SRE work, not application
  work.
- Migration tooling between Cloud and self-hosted. Not needed
  unless we revisit.
