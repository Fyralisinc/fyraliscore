# Fyralis Source Connector Contract — Principal Engineering Audit

**Audit date:** 2026-08-03<br>
**Branch:** feature/source-connector-contract<br>
**Audited revision:** a4fd22d4<br>
**Decision:** **Do not merge under the claim that the source-connector roadmap is complete.**

> **Remediation status:** Completion Phases 1 and 2 address the truthful-scope,
> legacy-safe default, executable manifest, independent structural and
> behavioral evidence, least-authority, measured-artifact, native-pilot,
> closed-loop rollout, deployment, migration, and production-like proof
> blockers identified below. The remaining 23 migrations, stateless signed
> control-plane distribution, legacy retirement, stable-v1 evolution, and
> fleet hardening remain Phase 3 work. The audit evidence below intentionally
> describes revision `a4fd22d4`; see `SOURCE_CONNECTOR_10_10_PLAN.md` and the
> completion reports for the current remediation boundary.

## Executive Summary

The branch establishes a strong architectural foundation: a dependency-light
source contract, typed capability facets, immutable registry snapshots,
installation-scoped binding, host-service ports, routing modes, a common
control-plane schema, lifecycle and rollout models, artifact attestations, and
good unit-level coverage. Those are substantial improvements over the original
collection of unrelated source maps.

It does **not**, however, fully implement the architecture in
[source-connector-contract.md](docs/architecture/source-connector-contract.md).
The research document defines seven roadmap phases. The implementation reports
describe three delivery phases that cover parts of research phases 1–4 and some
scaffolding from phases 5–7. Treating those three reports as completion of the
original roadmap is the central acceptance error.

The current runtime contains 26 registry entries, but only Slack, Notion, and
WhatsApp are marked native. The other 23 are compatibility candidates whose
production behavior remains legacy. Even the native Slack and Notion pull,
poll, and reconciliation facets require an ambient legacy ContextVar carrying
legacy installation rows and planner/reconciliation context. The normalizer
still resolves the central channel map and handler registry before connector
execution. A new source therefore cannot be added solely through a connector
package and manifest; it still requires edits to central source identity,
catalog, installation loading/client construction, channel/handler routing,
and ingress ownership.

There are also release-control gaps that make the default connector routing for
the three pilots premature:

- behavioral conformance exists only as a library tested with synthetic test
  fixtures; no shipped connector runs it as an admission gate;
- native fingerprints are approved by constructing the approval set from the
  candidates being admitted;
- shadow reports and connector metrics have no production sink/writer;
- the rollout metric table has a reader but no writer, so automatic rollback
  has no evidence stream;
- rollout stage and tenant cohort are stored as metadata but are not translated
  into or validated against routing policy;
- artifact signatures attest to a claimed artifact digest, but admission never
  hashes the running artifact and compares it with that claim;
- artifact admission is startup-time, not continuously propagated;
- the stateless normalizer bypasses durable artifact admission and durable
  authority;
- migration 0187 and database/Kafka/S3 integration gates were not executed;
- the lifecycle worker exists as a module but has no checked-in deployment
  definition.

The branch is best described as a **credible connector-platform foundation with
three transitional pilot adapters**, not a completed source-connector
platform. Overall architectural score: **5.8/10**.

## Audit Basis and Evidence

The audit cross-referenced:

1. [source-connector-contract.md](docs/architecture/source-connector-contract.md)
2. [PHASE_1_IMPLEMENTATION_REPORT.md](PHASE_1_IMPLEMENTATION_REPORT.md)
3. [PHASE_2_IMPLEMENTATION_REPORT.md](PHASE_2_IMPLEMENTATION_REPORT.md)
4. [PHASE_3_IMPLEMENTATION_REPORT.md](PHASE_3_IMPLEMENTATION_REPORT.md)
5. the source-contract, runtime, conformance, platform, connector, workflow,
   gateway, migration, deployment, and documentation code in the repository.

Focused executable evidence:

- runtime candidates: **26**
- native candidates: **3**
- compatibility candidates: **23**
- immutable registry entries: **26**
- default connector-routed sources: Slack, Notion, WhatsApp
- native structural fingerprints recomputed: **3/3 match**
- manifest implementation targets: **3/3 native targets unresolved**
- focused connector runtime/conformance/platform tests: **77 passed**
- import-linter: **8 contracts kept, 1 repository-wide contract broken**
- connector-specific source-contract and runtime boundaries: **kept**
- database/Docker production evidence: **not run**, consistent with the Phase 3
  report

The unrelated untracked research document already present in the worktree was
not modified.

## Severity Model

| Severity | Meaning |
| --- | --- |
| Blocker | Must be resolved, or the merge scope/default behavior must be changed, before main |
| High | Material architecture or production-control weakness |
| Medium | Important debt that impairs extensibility, safety, or maintainability |
| Low | Naming, documentation, or hardening work |

## Roadmap Completeness Matrix

The status is measured against the **seven research phases**, not the three
implementation-report labels.

| Research objective | Status | Evidence and assessment |
| --- | --- | --- |
| Shared vocabulary and ownership model | ✓ | The research and new packages distinguish connector, installation, binding, capability, execution, and host ownership clearly. |
| Complete machine-readable baseline of all existing source behavior | △ | CONNECTOR_CATALOG inventories source and ingress kinds, but not the full install/auth/planner/fetcher/reconciler/handler/lifecycle matrix requested by research Phase 1. |
| Characterization of existing delivery invariants | △ | Existing ingestion tests preserve several invariants; there is no connector-wide release matrix proving S3-before-Kafka, cursor-after-ack, DLQ lineage, trust capping, and parity per source/capability. |
| Source-contract dependency floor | ✓ | source_contract is dependency-light and the import-linter boundary passes. |
| Connector implementation dependency boundary | ✗ | No CI rule forbids services.ingest.connectors from importing runtime/platform/ingestion. native.py currently imports connector_platform, connector_runtime, and legacy ingestion types. |
| Connector IDs, versions, DTOs, and typed errors | ✓ | Implemented with Pydantic/dataclass contract models and semantic version/range support. |
| Small root connector plus capability facets | ✓ | SourceConnector and BoundConnector remain small; optional behavior is represented through versioned keys. |
| Capability protocol catalog | ✓ | OAuth, configuration, rotation, pull, poll, webhook, subscription, gateway, reconciliation, identity, normalization, cleanup, and health shapes exist. |
| Host-service ports | ✓ | Secrets, governed HTTP, state, installation data, raw emission, callbacks, clock, cancellation, metrics, logging, and lease ports exist. |
| Serializable/infrastructure-neutral logical contract | △ | Contract DTOs are clean, but native pull/reconciliation execution still depends on ambient legacy rows/context outside the DTO boundary. |
| Declarative manifests inspectable without implementation import | ✗ | Manifests are Python objects built while importing candidate/source modules. There are no standalone manifest resources or separate discovery loader. |
| Valid factory loading from manifest implementation | ✗ | All three native paths point to functions absent from services.ingest.connectors.native. Compatibility paths resolve to one function returning the whole candidate tuple, not a per-manifest SourceConnector factory. The registry never uses or validates these paths. |
| One truthful manifest per source | △ | Twenty-six manifest-shaped candidates exist, but 23 are synthesized compatibility descriptions. Webhook/gateway ingress kinds often have no matching webhook/gateway capability. |
| Deterministic immutable registry | ✓ | Candidate ordering, duplicate checks, compatibility negotiation, activation, immutable indexes, diagnostics, and fingerprinting are implemented. |
| Strict duplicate/source/alias/capability validation | ✓ | The implemented checks are deterministic and fail registry construction. |
| Permission and trust policy validation at registration/bind | △ | Required manifest permissions are checked as a subset of the grant, but extra granted authority is passed through unchanged; trust tiers are untyped and not ordered/enforced. |
| Independent behavioral conformance as registration gate | ✗ | BehavioralConformanceSuite is not invoked for shipped candidates. Registry fingerprints prove structural build/bind/facet resolution only. |
| Reproducible approved conformance evidence | △ | Current native structural fingerprints reproduce, but the startup approval set is derived from the candidates themselves, so approval is not an independent release policy. |
| Catalog generates/validates source wire values and deployment metadata | △ | A test checks catalog equality with SourceLiteral. SourceLiteral remains the generator for topics/compose, and progress types, channel maps, source loaders, and docs remain separately maintained. |
| Installation-scoped binding | △ | The binding identity and durable authority repository are present. Native pull/poll/reconciliation still require legacy ambient binding data, and bound connector version/configured capabilities are not used for selection. |
| Least-authority host services | △ | Ports enforce the supplied grant, but the runtime does not compute manifest ∩ environment policy ∩ installation grant. Extra grant entries remain usable. |
| Production host adapters | △ | PostgreSQL, secret store, governed HTTP, CAS, S3/Kafka raw emission, callback, and lease adapters exist. Metrics default to no-op and generic raw emission hard-codes ingress_kind=gateway. Real infrastructure gates were skipped. |
| Runtime-owned orchestration and delivery invariants | △ | Existing workflows retain publication/checkpoint ownership. They still know source-specific installation loaders, clients, dispatch maps, retry/breaker behavior, and legacy DTOs. |
| Planner/fetch runtime through registry | △ | Side-by-side bridges exist. Three sources default connector; 23 use legacy, and the native planners/fetchers still consume legacy context. |
| Normalization through connector-local routing | ✗ | The worker resolves the central channel map and handler before it invokes the connector router. A new native source still needs central channel and handler registration. |
| Generic webhook edge and host tenant resolution | △ | Slack and WhatsApp have integration bridges. There is no implemented generic callback endpoint driven by connector callback records for arbitrary sources. |
| Gateway supervision/lease integration | ✗ | A protocol and legacy adapter shape exist, but no persistent gateway pilot is registry-routed end to end. |
| Typed failure translation | ✓ | Contract failures are translated into stable runtime actions/categories. |
| Hierarchical retry/rate/breaker policy by connector/capability | △ | Existing host workflows retain rate/retry/breaker behavior; the connector platform does not expose the complete hierarchy or availability gate described by the research. |
| Implemented/configured/available capability views | ✗ | Registry implements the static view. enabled_capabilities is written but not read; breaker/conditions do not gate individual capability availability. |
| Desired/observed installation lifecycle | △ | A controller, durable repository, fencing, health, cleanup, and removal exist. Validation and initialization are assumed true; configure/subscription facets are not invoked; Syncing, Pausing, and Upgrading are absent. |
| Lifecycle worker production deployment | ✗ | The runnable module exists, but no compose/deployment/service entry was found. |
| OAuth through connector capabilities | △ | Slack/Notion begin/complete/revoke are integrated. No host scheduler invokes OAuth lifecycle refresh, and the general secret-rotation state machine is not integrated. |
| State migration and upgrade/rollback | △ | StateMigrationRunner is implemented and unit-tested but has no runtime caller, lifecycle transition, connector migration set, or bound-version upgrade flow. |
| Feature flags and per-scope routing | ✓ | Global, connector, capability, tenant, and combined overrides with atomic revisions and quarantine precedence are present. |
| Shadow execution | △ | Safe shadow execution and comparison models exist. Production wiring never supplies a shadow sink, so evidence is discarded. |
| Durable staged rollout | △ | Revision persistence, propagation, assessment, audit, and rollback models exist. Stage/cohort are not enforced, metric windows have no writer, and only the gateway is wired with the metric reader. |
| Automatic rollback | ✗ | The algorithm is tested, but the production evidence loop is open. With no metric writer it cannot make an evidence-based rollback decision. |
| Signed artifact admission and quarantine | △ | Ed25519 verification, manifest/fingerprint identity, builder/signer policy, and quarantine exist. Running artifact bytes are not hashed; signing is optional by default; admission changes are not watched continuously. |
| Operational telemetry and bounded cardinality | △ | Structured fields are present, but production callbacks are unwired and metric attributes include execution_id and installation_id, creating unbounded/high-cardinality series. |
| Common installation/authority migration | △ | Migration 0187 provides a common header and seeds Slack, Notion, and WhatsApp only. The remaining sources and provider lifecycle owners are not migrated. |
| Migrate representative pilot diversity | △ | Slack, Notion, and WhatsApp cover useful cases, but no Gmail domain/PubSub pilot, gateway pilot, or Grafana multi-channel pilot validates those archetypes. |
| Migrate all 26 source families | ✗ | 3 are transitional native candidates; 23 intentionally remain compatibility/legacy. |
| Record connector/capability/state versions on execution and DLQ | △ | Connector telemetry records versions; existing execution/DLQ persistence and replay do not consistently pin the original connector/capability/state version. |
| Remove legacy registration and dispatch | ✗ | Planner/fetcher/reconciler dictionaries, handler globals, channel map, source-specific loaders, and provider maps remain production-active. |
| Registry as sole runtime authority | ✗ | Registry, SourceLiteral, CONNECTOR_CATALOG, dispatch maps, handler registry, central channel map, and ingress/provider owners coexist. |
| Chaos, upgrade, rollback, and compatibility matrix tests | ✗ | Unit tests cover local algorithms; the Phase 7 chaos/evolution matrix is absent. |
| Dashboards, SLOs, and operator capability views | △ | A sanitized health snapshot and runbooks exist. No production metric writers, dashboards, alerts, state-migration view, or per-capability availability view are implemented. |
| Author/security/migration/operator documentation | △ | Seven focused guides are useful, but architecture status text and broader source-count/source-authority documents drift. |
| Stable v1 promotion | ✗ | Contract remains v1alpha1, correctly so given incomplete pilot diversity, migration, and deprecation proof. |
| Third-party/out-of-process security decision | ✗ | Logical DTOs anticipate RPC and signing, but there is no separate approved security architecture or execution implementation. |

### Roadmap phase verdict

| Research phase | Verdict |
| --- | --- |
| Phase 1 — Architectural foundations | **Mostly complete**, except full machine-readable inventory and connector import boundary |
| Phase 2 — Core connector contract | **Substantially complete** at the logical/API level |
| Phase 3 — Registry | **Partially complete**; immutable registry is strong, manifest discovery/loading and truthful/generative catalog are not |
| Phase 4 — Runtime integration | **Partially complete**; selected bridges exist, normalizer/gateway/generic ingress and capability availability remain |
| Phase 5 — Migration of existing connectors | **Not complete**; 3/26 transitional native, no production evidence window |
| Phase 6 — Removal of legacy registration | **Not started as a retirement phase**; legacy is still production-authoritative |
| Phase 7 — Product hardening | **Partially scaffolded**; docs and models exist, operational evidence/chaos/SLO/evolution gates do not |

## Phase Consistency Review

### Implementation Phase 1

Phase 1 correctly introduced the inward dependency floor, contract models,
registry validation, a legacy adapter, and structural conformance without
changing production behavior. This is the strongest and most internally
consistent phase.

Two items did not mature later as the report anticipated:

1. the conformance skeleton did not become per-connector behavioral admission;
2. the proposed connector-implementation import boundary was never encoded.

The report correctly warned that its fingerprint proved the harness ran against
an artifact shape, not behavioral correctness. Phase 3 later overstates this
evidence.

### Implementation Phase 2

Phase 2 correctly built a side-by-side runtime with explicit legacy, shadow,
and connector modes. It preserved host-owned delivery semantics and avoided a
big-bang migration. That sequencing matches the research.

The main consistency debt is the ambient LegacyBindingPayload/ContextVar bridge.
It allowed old worker DTOs to survive, but it became a hidden prerequisite of
facets later labelled native. The direct function calls avoid mutable map lookup
but do not establish a self-contained connector boundary.

Phase 2 also added WorkflowStateLifecycleRepository. Phase 3 introduced a new
PostgresInstallationLifecycleRepository and no production caller uses the
Phase 2 repository. The older repository is now an abandoned duplicate unless
it is explicitly retained as rollback/migration tooling.

### Implementation Phase 3

Phase 3 correctly adds durable authority, production host adapters, OAuth and
webhook bridges, common lifecycle models, rollout/artifact models, a 26-entry
registry, and operational documentation.

Its report is commendably explicit that 23 sources remain legacy, the database
migration was not run, metric writers and rollout evidence are absent, signed
artifacts are optional, and stateless normalizer admission remains open.

The report is nevertheless internally inconsistent in several claims:

- “static and behavioral conformance before registration” is false for shipped
  candidates; only structural conformance is registered;
- “host services enforce the intersection” is false; they enforce the complete
  durable grant passed to them;
- “automatic rollback” describes an implemented algorithm, not an operational
  closed loop;
- “native” Slack/Notion pull and reconciliation still require ambient legacy
  objects;
- “catalog source authority” means equality validation, not generator
  authority;
- the architecture document header says Phase 3 is implemented while its
  original status line still says Phase 1 is awaiting review.

### Cross-phase conclusion

The code evolution is directionally coherent, but the phase naming obscures
scope. The three implementation phases do not correspond to completion of the
seven-phase source-of-truth roadmap. Transitional mechanisms were promoted in
terminology before their removal criteria were met.

## Architecture Conformance Review

| Principle | Conformance | Deviation disposition |
| --- | --- | --- |
| Dependency inversion | Partial | Contract/runtime direction is good. Native connectors importing platform/runtime/ingestion must be corrected or explicitly isolated as migration adapters. |
| Capability-based architecture | Strong at type level | Keep. Add configured/available enforcement and truthful compatibility declarations. |
| Explicit deterministic registration | Strong | Keep. Replace direct candidate assembly with actual manifest discovery/factory loading. |
| Manifest-driven connectors | Weak | Correct. The implementation field is currently non-functional metadata. |
| Least authority | Weak | Correct before production native routing. Compute and pass an explicit intersection; type and enforce trust tiers. |
| Connector/source orchestration separation | Partial | Existing source-specific loaders and ambient rows are acceptable only as named, time-bounded adapters. They are not the final architecture. |
| Control plane/data plane separation | Partial | Models are separated, but rollout/admission state does not continuously control every execution owner. |
| Versioning/compatibility negotiation | Partial | Contract/capability majors negotiate. Connector-version selection, install version constraints, state upgrade, replay pinning, and adjacent-major adapters are absent. |
| Artifact lifecycle ownership | Partial | Runtime-global artifact state exists, but byte identity and continuous propagation are incomplete. |
| Installation lifecycle ownership | Partial | Common state exists for three sources; provider-specific ownership remains, and validation/initialization are not real reconciled operations. |
| Runtime ownership of durability/checkpoints | Good through inherited workflows | Acceptable transition. Add connector-native end-to-end proof before removing legacy. |
| Connector ownership of source semantics | Partial | Identity/webhook/OAuth facets move correctly; native pull/normalization still wrap legacy source functions/types. |
| Unsupported capability is absent | Good for WhatsApp | Compatibility manifests are less truthful because declared ingress does not correspond to complete facets. |
| Conformance is behavioral | Non-conformant | Must be corrected; it is a stated design principle and release gate. |
| Migration is reversible and observable | Reversible, not observable | Routing fallback exists. Production parity/native-vs-legacy telemetry is not connected. |

### Least-authority defect in detail

RegisteredConnector verifies that every permission requested by the manifest is
present in the granted authority. HostServicesFactory then builds ports from
the entire grant. If a durable grant contains an undeclared secret slot or
outbound host, connector code can use it. This is the inverse of the promised
manifest-maximum/intersection model.

The correct binding result should be based on:

**manifest request ∩ environment policy ∩ active installation grant ∩
capability-specific need**

Required capability grants can fail binding; optional capability grants should
remove only that capability from the configured/available view.

### Artifact-integrity defect in detail

ArtifactDeploymentPolicy verifies a signature over artifact_sha256 and checks
manifest and conformance values. It does not receive or compute the digest of
the code/package/image being executed. A valid signature therefore proves that
someone signed a claim, not that the running connector bytes match that claim.
Before third-party or independent artifact loading, admission must bind the
attestation to an independently measured artifact or immutable image/package
identity.

## Legacy Migration Audit

### Intentionally retained and still required

- the 23 compatibility candidates;
- planner/fetcher/reconciler dispatch maps for non-migrated sources;
- handler registry and channel map for non-migrated normalization;
- provider-specific installation tables as typed extension storage;
- legacy fallback for Slack, Notion, and WhatsApp until production rollback
  windows close.

Deleting these now would violate the research retirement criteria.

### Active legacy paths inconsistent with “migration complete”

- PLANNER_DISPATCH remains referenced by 32 non-test ingestion modules;
- FETCHER_DISPATCH remains referenced by 32 non-test ingestion modules;
- RECONCILER_DISPATCH remains referenced by 30 non-test ingestion modules;
- source_onboarding, shard_fetch, reconciler, and periodic_reconciler retain
  direct map paths;
- periodic_reconciler bypasses connector routing entirely;
- the normalizer always resolves the central channel map and handler first;
- source_onboarding still loads installations and constructs clients through
  source-specific branches;
- native Slack/Notion pull/poll/reconciliation facets depend on
  require_legacy_binding;
- NativeSlackWebhook and native normalization call legacy ingestion modules;
- provider webhook maps and source-specific lifecycle/install owners remain.

### Multiple registration/identity authorities

The following mechanisms coexist:

1. ConnectorRegistry
2. CONNECTOR_CATALOG
3. RawEnvelope SourceLiteral
4. PLANNER_DISPATCH
5. FETCHER_DISPATCH
6. RECONCILER_DISPATCH
7. handler decorator registry
8. central channel mapping
9. progress-event Source literal
10. provider/webhook and source-installation branches

Some duplication is necessary during migration. It must be treated as a tracked
retirement program with owners, deadlines, and invocation telemetry. At
present, the catalog only proves equality with SourceLiteral; it does not make
the other authorities derived.

### SourceLiteral verdict

SourceLiteral still has architectural responsibility: it generates Kafka topic
names, worker compose fragments, schema validation, and several derived lists.
That is explicitly contrary to the final target where connector catalog
registration generates or validates every such concern and a 27th source
requires no source allowlist/topic edits.

## Connector Platform Assessment

### Does it behave as a true platform?

**Not yet.** It behaves as a platform kernel plus migration facade.

The most objective test is the research success condition: add Stripe as the
27th source. With the current code, a genuinely native Stripe connector would
still require central changes to:

- RawEnvelope SourceLiteral;
- CONNECTOR_CATALOG;
- the central candidate composition/build functions;
- source installation loading and likely source client construction;
- normalizer channel mapping and handler registry, because the worker rejects
  unknown channels before calling the connector;
- webhook/provider routing or a new ingress owner;
- common installation/authority migration;
- deployment/topology generation and associated source metadata validation.

That is materially better than the pre-contract architecture because the
contract and registry are available, but it does not satisfy the connector-style
extension goal.

### Strengths

- small, coherent root contract;
- capability composition rather than inheritance;
- strict immutable registry and useful diagnostics;
- typed contract DTOs and failures;
- installation identity and authority validation;
- host-owned I/O ports and preserved durable publication/checkpoint model;
- explicit coexistence modes and rollback precedence;
- common control-plane schema with RLS and generation fences;
- good local unit testability;
- clear separation of first-party native and compatibility candidate origins;
- WhatsApp correctly demonstrates absence of historical capabilities.

### Weaknesses

- no real manifest loader/package boundary;
- connector onboarding still needs engine edits;
- only REST/webhook-like pilot diversity;
- no operational behavioral conformance;
- no closed-loop rollout evidence;
- no per-capability configured/available state;
- no real connector version selection/upgrade;
- no generic ingress/gateway architecture;
- no completed source migration or retirement;
- no enforced connector implementation import boundary;
- in-process code is trusted by convention, not sandboxed.

### Operational maturity

The control-plane models are thoughtful, but operational maturity is below the
report’s wording. A production platform needs writers, exporters, dashboards,
alerting, propagation watches, deployable controllers, and rehearsed rollback.
The current branch mostly supplies readers, algorithms, and runbooks.

## Code Quality Assessment

### Positive observations

- source-contract models are compact and cohesive;
- registry construction cleanly separates static validation from activation;
- immutable mappings and deterministic ordering reduce startup ambiguity;
- host ports are narrow and testable;
- errors are normalized at boundaries;
- routing precedence and quarantine behavior are easy to reason about;
- state migration and rollout algorithms are small and unit-testable;
- current structural conformance fingerprints reproduce exactly.

### High-impact code smells

1. **Ambient execution dependency.** require_legacy_binding hides required
   inputs from capability signatures and makes native facets unusable in a pure
   contract harness.
2. **Non-executable manifest metadata.** implementation strings are neither
   loaded nor checked; the native values are invalid.
3. **Circular conformance approval.** build_pilot_composition builds the
   approved fingerprint set from the same candidates it then validates.
4. **Leaky connector boundary.** native.py imports platform/runtime and legacy
   ingestion DTOs.
5. **Duplicate lifecycle repositories.** lifecycle_store.py is test-only after
   the new common-table repository was introduced.
6. **Misleading composition names.** build_pilot_composition builds all 26
   candidates; its docstring says both native definitions despite three native
   and 23 compatibility definitions.
7. **Open-loop rollout.** the metric-window table has no writer and shadow
   reports have no production sink.
8. **High-cardinality metrics.** BoundedMetrics does not bound attribute names
   or cardinality, while telemetry attaches execution and installation IDs.
9. **Per-request clients.** migrated webhook bridges create a new AsyncClient
   for each request rather than using a lifecycle-owned pooled client.
10. **Hard-coded raw ingress kind.** the generic raw publisher always writes
    gateway, limiting the correctness of the host port for other emitting
    capabilities.
11. **Unused persisted fields.** enabled_capabilities and
    bound_connector_version are written but not consulted for resolution.
12. **Migration version mismatch.** migration 0187 seeds Slack/Notion
    bound_connector_version as 0.1.0 while their native manifests are 1.0.0.
13. **Documentation drift.** the research document contains conflicting
    implementation-status lines, and broader ingestion docs still disagree on
    source counts/authority.

### Runtime inefficiency

The registry itself is not a meaningful performance concern relative to source,
S3, Kafka, and database I/O. The more relevant inefficiencies are repeated
composition/conformance construction in processes, per-request HTTP clients,
and parallel maintenance of multiple runtime registries and DTO translations.

## Future Readiness Assessment

| Future capability | Readiness | Missing foundation |
| --- | --- | --- |
| Dynamically loaded first-party connectors | Low | Real manifest discovery, resolvable factory contract, package identity, multi-version selection |
| Out-of-process connectors | Low–medium logical, low operational | RPC schemas/adapters, process supervisor, enforced egress/filesystem/resource policy, measured artifacts |
| Connector marketplace | Low | Artifact repository/API, publisher identity, review policy, revocation, compatibility/deprecation, billing/support model |
| Third-party connectors | Low | Separate security approval, sandbox, least-authority enforcement, tenant-safe conformance, incident/quarantine process |
| Connector signing | Medium model, low assurance | Bind signature to measured running bytes and make policy environment-safe/continuously propagated |
| Sandboxing | Missing | Process/container isolation and enforceable host capability transport |
| Remote execution | Missing | Versioned RPC, authentication, lease/cancellation propagation, fault and retry semantics |
| Distributed connector workers | Partial | Registry/routing revision distribution, artifact propagation, durable metrics, connector/capability breaker coordination |
| Control-plane/data-plane separation | Partial | Every execution owner must consume durable revisions/admission; stateless workers need a signed distribution channel |
| Multi-region deployment | Low | Regional ownership/fencing, replicated routing/artifact state, installation lease locality, callback routing, state consistency and failover |

The logical DTO discipline is a useful future asset. It should not be mistaken
for operational plugin readiness.

## Prioritized Risks

### Blockers

**B1 — Acceptance/scope mismatch.** The branch is presented as completion of all
phases while research phases 5–7 and parts of 3–4 are incomplete.

**B2 — Default cutover without production evidence.** Slack, Notion, and
WhatsApp default to connector mode even though migration 0187, real
infrastructure, metric windows, and production soak/rollback evidence are
absent. Either provide the required evidence or keep the merge default legacy.

**B3 — Release gates are not real gates.** Behavioral conformance is unused,
fingerprint approval is self-derived, and manifest factory targets are invalid
or source-agnostic.

**B4 — Least-authority contract is not enforced.** Extra durable authority is
available to in-process connector code and trust tier is not validated.

**B5 — Artifact assurance is incomplete.** A signed claimed digest is not tied
to the executing artifact; signed admission is optional and not continuously
propagated to all owners.

**B6 — Rollout safety loop is non-operational.** No shadow sink, metric writer,
or enforced cohort mapping exists; automatic rollback cannot work as described.

**B7 — Required release integration is unverified.** Database migration, RLS,
transaction fencing, S3/Kafka ordering, lifecycle, and fleet propagation were
not exercised with real infrastructure; the lifecycle controller is not
deployed by checked-in topology.

### High risks

- Native pilot terminology masks ambient legacy dependencies.
- The stateless normalizer can execute connector code without durable artifact
  admission or lifecycle authority.
- Periodic reconciliation bypasses routing and creates behavior inconsistency
  between reconciliation owners.
- Configured and available capability state is ignored.
- Connector version/state upgrade and replay pinning are absent.
- No gateway/domain-install/multi-channel pilot tests the most divergent source
  archetypes.
- Metrics are no-op in production wiring and would have excessive cardinality
  if naively connected.

### Medium risks

- SourceLiteral/catalog/progress/docs may drift.
- Compatibility declarations can imply ingress support without corresponding
  facets.
- Artifact quarantine is process-local by connector ID rather than continuously
  version-aware across a fleet.
- Sequential lifecycle reconciliation can be stopped by an unexpected
  repository/save error.
- Database child rows do not enforce tenant/connector equality with the common
  installation header through composite foreign keys.
- Provider-specific lifecycle code and the common lifecycle controller may
  diverge while both remain active.

## Recommendations

### Required before merge to main

1. **Correct the acceptance statement.** Recast this branch as connector
   foundation plus three pilot migrations, or finish the original seven-phase
   roadmap. Update conflicting architecture/report status language.
2. **Choose a safe cutover posture.** Until release evidence exists, make
   absent configuration resolve fully legacy, including the three pilots. A
   durable, audited rollout revision should enable connector mode.
3. **Make manifests executable and independently inspectable.** Store/discover
   manifests without importing implementations, validate every implementation
   target, and require a per-manifest factory that returns exactly that
   connector.
4. **Make behavioral conformance a release/admission gate.** Supply real
   fixtures for every declared native capability and combine structural and
   behavioral evidence into an independently approved release fingerprint.
5. **Enforce least authority.** Compute the explicit intersection, enforce
   typed trust ordering, and remove unavailable optional capabilities rather
   than passing broad grants through.
6. **Close artifact assurance.** Measure the executing package/image artifact,
   bind its immutable digest to the attestation, require signing in production,
   and propagate admission revisions continuously to gateway, workflows,
   normalizer, and lifecycle workers.
7. **Close rollout telemetry.** Persist shadow comparisons and metric windows,
   export bounded connector metrics, validate stage/cohort against the applied
   routing policy, and exercise automatic rollback.
8. **Run release-environment gates.** Apply/rollback migration 0187 on a
   production-like database and test RLS, cross-tenant integrity, authority
   backfill, S3-before-Kafka, cursor-after-ack, duplicate publication,
   lifecycle fencing, rollout propagation, and rollback.
9. **Deploy the lifecycle owner** or do not describe it as production
   authoritative.
10. **Either integrate all execution owners or document exclusions.** At
    minimum, periodic_reconciler and stateless normalizer must obey the same
    routing/admission rules.

### Required to declare the roadmap complete

1. Remove the ambient legacy dependency from native facets.
2. Replace central normalizer/channel/handler routing with connector-local event
   routing.
3. Implement the generic webhook/callback edge and gateway supervision.
4. Migrate the representative Gmail, gateway, and Grafana archetypes before
   freezing v1.
5. Migrate the remaining 23 sources with behavioral, parity, lifecycle,
   uninstall, and production evidence.
6. Make catalog registration generate or strictly validate all wire values,
   topics, deployment selectors, lifecycle coverage, docs, and supported-source
   metadata.
7. Implement configured/available capability views, state upgrades, compatible
   connector-version selection, and replay version pinning.
8. Prove zero legacy invocation for the agreed window, exercise rollback, then
   retire maps and adapters.
9. Complete chaos/evolution/SLO/operator-view work and only then promote the
   contract from alpha.

### Technical debt that can safely wait after a correctly scoped, legacy-default merge

- RPC/out-of-process execution;
- marketplace and third-party publishing;
- dynamic Python entry points;
- multi-region active/active ownership;
- provider-specific extension table consolidation;
- per-connector rather than global automatic rollback;
- performance optimization of registry composition;
- cosmetic package reorganization after dependency direction is enforced.

These can wait because the original research explicitly places first-party
in-process connectors first. They cannot be cited as completed capabilities.

### If starting again

1. Keep the successful source-contract and immutable registry designs.
2. Define the release artifact as manifest + factory + behavioral evidence from
   the beginning, and make the registry consume only that artifact.
3. Migrate one complete vertical slice before adding rollout scaffolding:
   install → bind → ingress → normalize → publish → checkpoint → reconcile →
   cleanup, with no ambient legacy context.
4. Make Stripe-like onboarding a compile/test acceptance scenario: a fixture
   connector must join the pipeline without editing engine source files.
5. Select pilots by architecture immediately: OAuth webhook/pull, domain
   PubSub, persistent gateway, live-only, and multi-channel.
6. Build the rollout evidence writer and artifact distributor alongside their
   readers/controllers so no control-plane algorithm is merged open-loop.
7. Keep all routing legacy until production evidence explicitly activates a
   revision.

## Merge Readiness

### Current verdict

**No, not as currently represented or default-routed.**

The branch should not merge to main under a “completed all phases” acceptance
criterion. It also should not silently make the three pilots connector-default
before the release controls and infrastructure evidence exist.

### Conditional merge path

An incremental merge could be reasonable if all of the following are true:

- scope is renamed to foundation/registry/runtime scaffolding plus transitional
  pilots;
- default production routing remains legacy until an audited rollout revision;
- the manifest, conformance, least-authority, and artifact-integrity blockers
  are fixed;
- migration and production-like integration gates pass;
- incomplete runtime owners and lifecycle deployment are explicitly gated;
- remaining 23 migrations and legacy retirement remain tracked, required work.

The presence of legacy code is not itself the blocker. The blockers are the
mismatch between claimed completion and actual state, and the fact that release
controls do not yet make the default native route safe and observable.

## Final Verdict

1. **Is the original architecture fully implemented?**
   **No.** Research phases 5 and 6 are incomplete, Phase 7 is partial, and
   important Phase 3/4 objectives are still absent.

2. **Is any planned work still missing?**
   **Yes.** Twenty-three migrations, legacy retirement, connector-local
   normalizer routing, generic webhook/gateway runtime, behavioral admission,
   configured/available capabilities, version/state upgrades, closed-loop
   rollout, production evidence, chaos/SLO work, and stable-v1 promotion.

3. **Is any implementation inconsistent with the research document?**
   **Yes.** Invalid/non-loaded manifest factories, connector imports of runtime
   infrastructure, broad grant pass-through, central source routing,
   non-behavioral conformance, catalog not being generator authority, and
   open-loop operational controls are direct deviations.

4. **Are any phases internally inconsistent?**
   **Yes.** The implementation sequence is directionally consistent, but Phase
   3 overstates behavioral conformance, authority intersection, automatic
   rollback, native independence, and catalog authority. Phase 2 lifecycle
   persistence is superseded but remains.

5. **Can the feature branch be merged into main?**
   **Not under its current completion claim and default production posture.**

6. **If not, what blockers remain?**
   B1–B7 above: truthful scope, safe cutover, real manifest/conformance gates,
   least authority, measured artifact identity, closed-loop rollout, and
   production-like integration/deployment proof.

7. **What technical debt should be addressed before merge?**
   Invalid implementation paths, self-approved fingerprints, ambient native
   context, authority intersection, artifact byte binding, rollout writers and
   shadow sink, normalizer/periodic-owner admission consistency, lifecycle
   deployment, migration version mismatch, and documentation status.

8. **What technical debt can safely wait?**
   Out-of-process RPC, marketplace, third-party connectors, dynamic loading,
   multi-region, storage consolidation, and performance/cosmetic refinements,
   provided they remain explicitly future work.

9. **What would be improved if starting again?**
   Start with a complete no-ambient-context vertical slice and an executable
   manifest/release artifact; use a no-engine-edits fixture connector as the
   platform acceptance test; connect evidence writers at the same time as
   rollout/admission readers.

10. **Overall architectural score:**
    **5.8/10.** Contract and registry foundations are approximately 8/10;
    migration completeness, production enforcement, and operational evidence
    are approximately 3–5/10. The weighted result reflects a promising
    architecture that has not yet crossed from migration framework to finished
    platform.
