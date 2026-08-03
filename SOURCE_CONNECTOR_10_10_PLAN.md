# Source Connector 10/10 completion plan

Status: **All three repository implementation phases complete**<br>
Baseline: principal-engineering audit at `a4fd22d4` (5.8/10)<br>
Rule: each phase is independently verified and committed; no later phase starts
until the prior phase has been reviewed.

Phase 3 completion means the checked-in runtime, migration, release controls,
tests, and operational contract are ready for review and controlled rollout.
Production soak windows, live-provider certification, multi-region drills, and
final deletion of the emergency legacy surface are deployment evidence, not
claims fabricated by a source change. They remain mandatory gates before the
corresponding production retirement records may be accepted.

## Completion Phase 1 — merge-safe default posture

Goal: make the connector subsystem truthful and disabled by default, without
claiming that native migration or production rollout is complete. This phase
does not make the branch merge-ready while required repository gates remain
red.

- Keep every connector in legacy execution by default. Connector mode requires
  an explicit routing revision.
- Move the three native manifests into checked-in declarative JSON and resolve
  their factory paths only during deliberate activation.
- Recompute conformance independently and compare all 26 candidates with a
  checked-in release-evidence catalog; never derive host approvals from the
  candidates being admitted.
- Intersect durable grants with manifest permissions before constructing host
  services, and reject broad binding contexts.
- Bind signed provenance to a digest measured from the running implementation
  module plus exact manifest. Make signed admission mandatory in production.
- Correct native control-plane seed versions, package manifest/evidence assets,
  add an executable release gate, and ratchet connector implementation imports.
- Correct architecture, migration, development, operations, and historical
  implementation-report status claims.
- Preserve the 23 compatibility candidates and all legacy paths as explicit
  rollback/production behavior.

Exit gate: manifest discovery and factories are executable; all 26 fingerprints
match independent evidence; default routing is legacy; authority and artifact
negative tests pass; focused connector suites and production environment checks
pass. Repository-wide pre-existing architecture/migration ratchet failures are
recorded rather than disguised as connector completion.

## Completion Phase 2 — native pilots and closed-loop operations

Goal: make Slack, Notion, and WhatsApp genuinely native and eligible for a
measured, reversible production rollout.

- Remove `legacy_binding_scope`/`require_legacy_binding` and all imports from
  native connector implementations into legacy planners, fetchers, handlers,
  reconcilers, integration helpers, and platform runtime modules.
- Introduce connector-local provider clients, pagination/state DTOs, identity,
  normalization, reconciliation, webhook verification, and OAuth facets.
- Make normalizer, backfill/poll, webhook, OAuth, and lifecycle owners resolve
  the same admitted registry artifact and durable installation authority.
- Ship capability-specific behavioral conformance fixtures and make their
  evidence a release-admission input alongside structural evidence.
- Persist shadow reports and bounded execution/latency/error/DLQ/lifecycle
  windows; translate rollout stages and tenant cohorts into routing policy.
- Continuously refresh artifact admission and routing across every execution
  owner. Deploy the lifecycle controller with health, alerts, and runbooks.
- Run migration 0187, schema/RLS/constraint checks, and production-like
  PostgreSQL/Kafka/S3/secret-store tests. Repair existing architecture and
  migration ratchet failures so required CI is fully green.
- Perform and record shadow, canary, bounded-cohort, rollback, replay, refresh,
  reconciliation, and uninstall drills for all three pilots.

Exit gate: a no-ambient-context test proves each pilot is native; behavioral and
artifact admission is green; evidence writers feed rollout readers; every
runtime owner honors quarantine/authority; production-like integration and CI
gates pass. Defaults remain legacy until an audited routing revision promotes a
specific cohort.

Implementation boundary: the three pilot packages are connector-local and the
database-backed execution owners continuously consume durable admission,
authority, routing, and rollout evidence. Stateless owners that cannot obtain
that durable control-plane state are explicitly forced to legacy in production
when signed artifacts are required. This is a fail-closed merge boundary, not a
claim that the remaining fleet or stateless artifact distribution is complete.

## Completion Phase 3 — complete fleet and stable v1

Goal: finish the original roadmap rather than merely operating a migration
framework.

- Migrate the remaining 23 source families by archetype, with complete ingress,
  installation, lifecycle, behavioral parity, rollout, rollback, and soak
  evidence for each.
- Make the connector manifest/catalog generate or validate central source
  identity, ingress coverage, channel/handler wiring, installation factories,
  worker/deployment configuration, and documentation.
- Add declared available/configured capability semantics, connector/contract/
  envelope/state version upgrade rules, explicit state migrations, mixed-version
  worker compatibility, downgrade policy, and replay certification.
- Delete compatibility candidates, dispatch-map authority, central source
  wiring, `SourceLiteral` architectural ownership, obsolete lifecycle storage,
  and provider-specific duplicate control-plane state only when retirement
  evidence permits it.
- Run failure injection, provider throttling/outage, lease loss, cancellation,
  secret rotation, credential revocation, poison payload, multi-region and
  disaster-recovery tests. Establish SLOs, dashboards, alerts, capacity and
  rollback ownership.
- Stabilize the contract as v1 only after compatibility, extension, security,
  and operations review. Out-of-process/marketplace support remains optional
  unless separately approved.

Exit gate: all 26 sources execute through the connector runtime, no legacy
registration/dispatch authority remains, version and replay upgrades are
proven, production SLO/chaos evidence is accepted, and the architecture report
can truthfully state that the original roadmap is complete.

### Implemented outcome

- All 26 checked-in manifests are stable `sources.fyralis.io/v1` definitions
  and resolve connector-local first-party factories. The catalog is derived
  from those manifests and validated against the generated source index and
  ingress channel/handler wiring.
- The default fleet policy is connector execution. Signed-artifact admission,
  authority binding, and quarantine remain higher-precedence fail-closed gates.
- Capability declarations distinguish implementation availability from
  installation configuration, so ungranted secret-backed facets are withheld.
- Explicit state-schema migrations, mixed-worker compatibility, downgrade
  policy, replay certification fields, resilience evidence, and retirement
  evidence are implemented.
- Structural and deterministic behavioral release evidence covers all 26
  sources. Throttling, outage, credential rejection, poison payload, replay,
  cancellation, lease, rotation, failover, and disaster-recovery scenarios have
  an enforceable evidence contract.
- Migration `0189_source_connector_stable_v1.sql`, fleet dashboards, recording
  rules, alerts, SLOs, ownership, and rollback procedures complete the
  repository-side operational surface.
- Compatibility candidate generation and central source-identity ownership are
  retired. Legacy execution code remains callable only for signed-admission
  quarantine and emergency rollback; its eventual deletion requires durable
  `source_connector_retirement_evidence` after live rollout acceptance.

Verification and residual operational gates are recorded in
`SOURCE_CONNECTOR_FINAL_SUMMARY.md` and `PHASE_3_IMPLEMENTATION_REPORT.md`.
