# Source Connector Completion Phase 2 report

## Outcome

Completion Phases 1 and 2 are implemented as an incremental, merge-safe source
connector platform slice. The change is suitable to present as a pull request
for the contract/runtime foundation and three native pilots. It must not be
represented as completion of the original seven-phase architecture: 23 source
families remain compatibility-backed and Completion Phase 3 remains required.

All 26 sources default to legacy execution. Slack, Notion, and WhatsApp can be
promoted only by a validated, durable routing revision naming an explicit
cohort. Artifact quarantine overrides routing. A production process that cannot
consume durable signed admission and installation authority is forced to
legacy, so the current merge boundary fails closed.

## Phase 1 — release foundation

Phase 1 is committed as `56a10f6d` (`feat(connectors): harden legacy-safe
release foundation`). It established:

- declarative, independently inspectable manifests and resolvable factories;
- independent checked-in structural release evidence for all 26 candidates;
- manifest-scoped least authority and measured running-artifact admission;
- production-required signed provenance and explicit quarantine;
- executable inventory, evidence, packaging, environment, and import gates;
- legacy routing as the truthful default, with 23 compatibility candidates
  preserved.

## Phase 2 — native pilots

Slack, Notion, and WhatsApp now have connector-local implementations. Their
native modules do not import legacy ingestion, integration, application, or
connector-platform implementations. Provider HTTP, DTO mapping, durable
checkpoint projection, identity, normalization, reconciliation, webhook
verification, OAuth/health, and idempotent cleanup live behind declared
capability facets.

Slack declares both bot and user OAuth token slots and the DM scopes needed for
conversation completeness. Fetch results distinguish a continuation cursor
from the durable terminal checkpoint. Raw envelopes carry the actual connector
installation ID through backfill and live ingress so semantic resolution does
not fabricate installation identity.

## Admission and behavioral proof

Release evidence schema v2 binds each native pilot to:

- its exact declarative manifest;
- every declared implementation module measured from the running checkout;
- structural conformance;
- capability-specific behavioral fixtures and their fingerprint.

The behavioral gate exercises the applicable pagination/checkpoint, stable
identity, reconciliation, webhook rejection/acceptance, semantic
normalization, lifecycle, health, cleanup, retry, and state behavior for each
pilot. Registration requires the combined structural and behavioral approval;
evidence is not synthesized from the candidate being admitted.

## Closed-loop rollout and lifecycle

Rollout stages are materialized into routing policy with validation: canary is
exactly one tenant, cohort is non-empty and bounded, and shadow/full do not
carry a tenant cohort. Database-backed owners continuously refresh active
routing and artifact admission.

Execution, duration, parity, lifecycle, and DLQ observations are appended to
`source_connector_rollout_events` without tenant, installation, or execution
identifiers. The rollout reader computes bounded error, p95, parity, lifecycle,
and DLQ rates from the same evidence stream used for promotion and automatic
rollback decisions. Evidence is pinned to the revision active when it is
scheduled.

The lifecycle controller is deployed in `docker-compose.yml`. It reconciles
desired/observed state through durable authority, health, fenced writes,
credential retirement, cleanup, and removal. OAuth completion validates the
exact granted scopes and declared credential slots before persisting Ready.

## Database and dependency repair

Migration 0187 now seeds pilot installation data and authority, uses
`granted_slot_names`, stores secret references rather than secret values, adds
bounded rollout events, and makes connector control-plane RLS fail closed when
tenant context is absent. Migration 0188 repairs the earlier BYOC access-grant
RLS drift with the same fail-closed rule.

Ingestion no longer imports application-owned provider-installation and product
workflow helpers. The shared observability event definition and
ingestion-owned installation repository restore the intended dependency
direction without expanding an allowlist.

## Recorded drills and verification

The following behaviors are covered by executable release or test evidence:

| Drill | Evidence |
| --- | --- |
| Shadow and parity | Canonical comparison plus durable parity-event tests |
| Canary and cohort | Stage/cohort policy validation and materialization tests |
| Automatic rollback | Threshold assessment and newer global-legacy revision tests |
| Replay/checkpoint | Continuation-versus-checkpoint and raw-envelope replay tests |
| Refresh | OAuth lifecycle and exact scope/slot persistence tests |
| Reconciliation | Native pilot reconciliation fixtures and workflow bridge tests |
| Uninstall | Idempotent connector cleanup and lifecycle removal tests |
| Continuous propagation | Gateway/workflow refresh and quarantine tests |

Verification completed for this delivery:

- source connector release gate: 26 candidates, structural plus behavioral
  evidence, measured implementation modules, legacy-safe defaults;
- connector-focused suite: 86 passed;
- architecture ratchets: passed;
- import-linter: 10 contracts kept, 0 broken;
- blocking Ruff rules and changed-file compilation: passed;
- fresh pgvector/PostgreSQL 16: all 188 migrations applied and schema lock
  clean;
- connector control-plane integration: 4 passed, including fail-closed RLS and
  rollout writer-to-reader evidence;
- production-like Kafka/S3/PostgreSQL shadow soak: 100 records, passed;
- Docker Compose configuration and package-asset checks: passed.

Repository-wide CI cleanup is intentionally not claimed as a Phase 2 exit gate
in this report. The connector-specific, architecture, packaging, migration,
and production-like gates above are the evidence accepted for this boundary;
the final repository-wide result remains a later PR verification concern.

## Phase boundary and remaining work

This commit completes the legacy-safe connector foundation plus native Slack,
Notion, and WhatsApp pilot operations. Native execution remains explicit,
cohort-scoped, observable, reversible, and fail-closed.

It is not a 10/10 completion of the original roadmap. Completion Phase 3 still
owns:

- native migration and soak evidence for the remaining 23 source families;
- a signed control-plane distribution mechanism for stateless workers before
  native promotion there;
- manifest/catalog generation of remaining central ingress and deployment
  wiring;
- connector, contract, envelope, and state upgrade/downgrade certification;
- evidence-based retirement of compatibility candidates and legacy dispatch;
- fleet chaos, SLO, capacity, multi-region, and disaster-recovery hardening;
- stable-v1 review and any separately approved out-of-process or marketplace
  model.

No branch push, pull request creation, or merge is part of this run.
