# Phase 1 Source Connector Contract Implementation Report

## Executive summary

Phase 1 establishes the Fyralis Source Connector Contract as a dependency-light,
capability-based architectural boundary without changing any production
ingestion path. It introduces immutable connector manifests and DTOs, typed
capability protocols, least-authority host service ports, deterministic registry
construction, installation-scoped binding, structured diagnostics, snapshot
health/fingerprints, a coexistence adapter, and a reusable conformance harness.

No existing source was migrated. No legacy dispatch registry, source mapping,
worker path, webhook route, planner, fetcher, reconciler, normalizer, lifecycle
command, S3/Kafka path, or cursor flow was changed. Existing ingestion therefore
continues to operate exactly as it did before this branch.

## Objectives completed

- Added the `services.ingest.source_contract` package with stable connector,
  source, capability, secret-slot, installation, and operation identities.
- Added a strict frozen `sources.fyralis.io/v1alpha1` manifest model with source
  aliases, semantic connector versions, contract ranges, capability
  declarations, permissions, trust metadata, ingress kinds, and runtime
  isolation metadata.
- Added all 14 initial versioned capability facets described by the architecture:
  configuration, OAuth2, secret rotation, resource discovery, historical pull,
  incremental poll, webhook, push subscription, gateway stream,
  reconciliation, identity, normalization, cleanup, and health probe.
- Added transport-neutral request/result/state/health DTOs with strict field
  validation and frozen top-level values.
- Added a typed and redaction-aware connector error taxonomy.
- Added least-authority host service protocols for secrets, governed HTTP,
  read-only state, installation storage, raw emission, callback allocation,
  clock, cancellation, metrics, logging, and leases.
- Added explicit `SourceConnector` and `BoundConnector` contracts plus a typed
  capability-key resolver and immutable static binding implementation.
- Added a deterministic two-stage registry builder. Static identity,
  compatibility, capability, interface, conformance-evidence, isolation, and
  policy validation completes before any factory is activated.
- Added strict duplicate connector ID, source wire value, and alias rejection.
- Added exact capability-major compatibility checks; unsupported required
  capabilities fail registration while unsupported optional capabilities are
  omitted with a warning.
- Added installation authority validation before connector binding and verified
  that each negotiated capability resolves to its declared runtime protocol.
- Added immutable registry snapshots queryable by connector ID, source/alias,
  and capability, with deterministic descriptions, health, and SHA-256 snapshot
  fingerprints.
- Added opt-in host enforcement for approved connector conformance fingerprints.
- Added a `LegacyConnectorAdapter` that composes explicitly injected capability
  providers without importing or mutating any legacy registry.
- Added deterministic fake host services and a conformance harness covering
  manifest/registry validation, factory activation, installation binding, typed
  capability resolution, reports, assertions, and artifact fingerprints.
- Added import-linter contracts enforcing that the source contract cannot import
  connector runtime, conformance, existing source implementations, ingestion
  runtime, or application routes, and that connector runtime cannot import
  source implementations or application routes.
- Added and linked the complete architecture research document in the MkDocs
  navigation.

## Files added

### Architecture and reporting

- `docs/architecture/source-connector-contract.md`
- `PHASE_1_IMPLEMENTATION_REPORT.md`

### Source contract

- `services/ingest/source_contract/__init__.py`
- `services/ingest/source_contract/connector.py`
- `services/ingest/source_contract/errors.py`
- `services/ingest/source_contract/host_services.py`
- `services/ingest/source_contract/identity.py`
- `services/ingest/source_contract/manifest.py`
- `services/ingest/source_contract/models.py`
- `services/ingest/source_contract/versioning.py`
- `services/ingest/source_contract/capabilities/__init__.py`
- `services/ingest/source_contract/capabilities/ingestion.py`
- `services/ingest/source_contract/capabilities/installation.py`
- `services/ingest/source_contract/capabilities/lifecycle.py`
- `services/ingest/source_contract/capabilities/semantic.py`
- `services/ingest/source_contract/tests/__init__.py`
- `services/ingest/source_contract/tests/conftest.py`
- `services/ingest/source_contract/tests/test_errors_and_secrets.py`
- `services/ingest/source_contract/tests/test_identity.py`
- `services/ingest/source_contract/tests/test_manifest.py`
- `services/ingest/source_contract/tests/test_models.py`
- `services/ingest/source_contract/tests/test_versioning.py`

### Connector runtime foundation

- `services/ingest/connector_runtime/__init__.py`
- `services/ingest/connector_runtime/binding.py`
- `services/ingest/connector_runtime/definitions.py`
- `services/ingest/connector_runtime/diagnostics.py`
- `services/ingest/connector_runtime/health.py`
- `services/ingest/connector_runtime/legacy.py`
- `services/ingest/connector_runtime/registry.py`
- `services/ingest/connector_runtime/validation.py`
- `services/ingest/connector_runtime/tests/__init__.py`
- `services/ingest/connector_runtime/tests/helpers.py`
- `services/ingest/connector_runtime/tests/test_legacy.py`
- `services/ingest/connector_runtime/tests/test_registry.py`

### Conformance kit

- `services/ingest/connector_conformance/__init__.py`
- `services/ingest/connector_conformance/fakes.py`
- `services/ingest/connector_conformance/models.py`
- `services/ingest/connector_conformance/suite.py`
- `services/ingest/connector_conformance/tests/__init__.py`
- `services/ingest/connector_conformance/tests/test_suite.py`

## Files modified

- `mkdocs.yml` — added the Source Connector Contract to architecture navigation.
- `pyproject.toml` — added enforceable source-contract and connector-runtime
  dependency boundaries.

## Architectural decisions

### Capability composition over inheritance

The connector root has only `manifest` and `bind`. Optional behavior is expressed
through separately versioned, runtime-checkable capability protocols. A source
implements only capabilities it actually supports; no base class supplies fake
or optional no-op methods.

### Definition, installation, and execution remain separate

`ConnectorCandidate` and `ConnectorManifest` describe an artifact. `BindingContext`
binds that artifact to one tenant installation and one explicit authority grant.
`OperationContext` scopes an invocation. The immutable registry contains no
tenant state.

### Explicit construction with no import-time registration

Registry builders accept explicitly supplied candidates and own their mutable
construction state locally. They freeze registrations into a new snapshot.
There is no module scanning, global registry mutation, or last-writer-wins
behavior.

### Inspect before activation

The builder performs all side-effect-free checks before invoking any connector
factory. Duplicate identity, incompatible contract/isolation, capability drift,
interface mismatches, policy rejection, and missing conformance evidence cannot
activate connector code.

### Least authority at binding

Manifest permission requests and installation grants are distinct. Binding is
rejected before connector code runs if required secret slots, outbound hosts, or
scopes are absent. Connectors receive narrow host ports instead of raw database,
Kafka, S3, Redis, FastAPI, Temporal, or provider-global clients.

### Host-owned data-plane guarantees

Contract capabilities return records, drafts, state proposals, health, and
reconciliation decisions. They do not receive APIs for raw Kafka publication,
direct S3 namespace writes, durable cursor advancement, tenant resolution, or
final trust assignment. This preserves S3-first durability, Kafka ordering,
idempotency, cursor-after-ack semantics, DLQ ownership, and trust capping as host
responsibilities.

### Strict startup semantics with observable diagnostics

The Phase 1 builder returns a structured result for inspection and refuses to
publish a partial registry when validation or factory activation fails. Optional
capability incompatibility produces a degraded but usable snapshot. Per-artifact
quarantine and deployment enablement policy remain runtime-integration work.

### Compatibility evidence is enforceable policy

The conformance suite creates a deterministic SHA-256 fingerprint tied to the
suite version, manifest, declared interfaces, and check results. Hosts may allow
unattested candidates during development or require an approved fingerprint in
strict deployments.

### Backward compatibility through explicit composition

The legacy adapter accepts capability providers through dependency injection and
creates the same `ConnectorCandidate` shape as a first-class implementation. It
does not know existing dispatch-map locations, so migration can occur source by
source without making the new boundary depend on legacy globals.

## Deviations

There are no intentional deviations from the architecture's core ownership,
capability, binding, versioning, registry, or dependency-direction decisions.
The following are deliberate phase boundaries rather than redesigns:

- The architecture document has a more granular internal roadmap, while the
  implementation directive groups the logical contract and registry foundation
  into user-defined Phase 1. The user-defined three-phase boundary was followed.
- Registry discovery currently begins from explicit in-process
  `ConnectorCandidate` values. Checked-in source catalogs, entry-point discovery,
  implementation-path loading, signature verification, and out-of-process RPC
  discovery are intentionally not activated in Phase 1.
- The conformance kit validates the common artifact/registry/binding contract.
  Capability-specific behavioral fixtures for pull, push, lifecycle,
  normalization, reconciliation, and state migration will be added as sources
  enter the new path.
- Strict builds fail atomically. Operational quarantine/enable/disable policy is
  deferred until there is a runtime control-plane integration to own it.

## Compatibility considerations

- No existing production module imports the new packages.
- No existing runtime module was edited.
- No source connector was registered, adapted, or migrated.
- No database schema or migration was added.
- No source literal, Kafka topic, Docker/Compose service, lifecycle coverage map,
  webhook provider table, planner/fetcher/reconciler dispatch, handler map, or
  central channel mapping was changed.
- The legacy adapter is inert until explicitly constructed and registered by a
  future composition root.
- New manifest aliases are local to immutable registry snapshots and do not
  change existing source wire values.
- The source-contract package contains no imports of runtime/source/app code;
  the connector runtime contains no imports of existing sources or app routes.
- Secrets redact `str` and `repr`; unexpected factory, validator, and binding
  exceptions are normalized without exposing raw exception text in diagnostics.

## Migration notes

When Phase 2 is approved, migration should begin with a composition root that
constructs a registry snapshot alongside—not in place of—the current global
dispatch structures. A representative connector should receive a manifest and
small adapter facets that delegate to existing source logic. Shadow comparison
should prove identity, normalization, state, and publication parity before any
tenant cohort is routed through registry-resolved capabilities.

The initial pilot should exercise more than one archetype only if needed to
validate the boundary: an OAuth + webhook + backfill source and a materially
different stream or polling source are better evidence than several similar HTTP
connectors. Feature flags and immediate fallback to the legacy path must remain
available throughout Phase 2.

## Risks

- The v1alpha1 DTO and protocol surface has not yet been exercised against a
  real Fyralis source adapter. Pilot implementation may reveal source-specific
  state, callback, lifecycle, or pagination fields that should be generalized
  before the API stabilizes.
- Capability declarations currently identify an exact major version. A future
  connector that implements multiple majors needs explicit selection semantics
  and behavioral compatibility fixtures before this contract leaves alpha.
- Frozen Pydantic models prevent field replacement, but nested native payload
  dictionaries remain mutable Python values by contract convention. RPC or
  third-party boundaries should introduce canonical serialization and payload
  size enforcement.
- The authority model validates named grants but deliberately does not encode a
  global ordering for trust-tier strings. Platform trust policy must supply that
  ordering and final cap during runtime integration.
- A conformance fingerprint proves this harness ran against an artifact shape;
  artifact signing, provenance, and binary/package checksums are separate future
  supply-chain controls.
- Full capability behavior is not proven by structural runtime protocols. Each
  migrated connector still needs semantic fixtures for bounded pagination,
  path-independent identity, cursor monotonicity, retries, webhook verification,
  normalization, reconciliation, and cleanup.
- Repository-wide test execution on this checkout is not fully green for
  pre-existing reasons: 4,080 tests passed and 2,390 skipped, while 22 unrelated
  tests failed and 13 subprocess tests errored because the optional `moto`
  dependency is absent. None of those failures reference files added or modified
  by Phase 1. The repository-wide import-linter run also reports pre-existing
  ingest-to-app edges; both newly added boundary contracts pass independently.

## Validation performed

- `pytest -q services/ingest/source_contract/tests services/ingest/connector_runtime/tests services/ingest/connector_conformance/tests`
  — **57 passed**.
- `mypy --explicit-package-bases services/ingest/source_contract services/ingest/connector_runtime services/ingest/connector_conformance`
  — **success across 38 source files**.
- `lint-imports --contract source-contract-boundary --contract connector-runtime-boundary`
  — **2 contracts kept, 0 broken**.
- `git diff --check` — **clean**.
- `mkdocs build --strict` — run as the final documentation gate for this report.
- Broad `pytest -q` — **4,080 passed, 2,390 skipped, 22 failed, 13 errors**;
  failures are recorded under Risks and are outside Phase 1 paths.

## Remaining work

Phase 1 intentionally leaves all production integration and migration work
undone:

- registry composition at service startup;
- installation lifecycle integration and persisted desired/observed status;
- platform implementations of host service ports;
- worker/orchestrator capability resolution;
- compatibility-backed planner, fetcher, webhook, gateway, normalizer, and
  reconciler execution;
- per-source manifests and factories;
- representative connector adapters and parity fixtures;
- operational enable/disable/quarantine policy and telemetry;
- state codec/migration execution;
- registry-generated source/topic/deployment metadata;
- staged cutover, rollback, and legacy dispatch retirement.

## Suggested review checklist

- [ ] Confirm the connector root remains intentionally small and capability
      facets match Fyralis source archetypes.
- [ ] Confirm manifest identity, aliases, compatibility axes, requested
      permissions, trust, and isolation metadata are sufficient for pilots.
- [ ] Review every DTO for infrastructure leakage and source-specific coupling.
- [ ] Review host ports for least authority and verify none permits bypassing
      S3-first/Kafka/checkpoint/trust invariants.
- [ ] Review typed error categories and redaction behavior.
- [ ] Confirm static validation runs before factory activation and duplicate
      definitions cannot produce a snapshot.
- [ ] Confirm required/optional capability behavior and interface identity
      checks are correct.
- [ ] Confirm binding is installation-scoped and rejects insufficient grants
      before connector code executes.
- [ ] Confirm registry snapshots and their indexes/health/fingerprints are
      deterministic and immutable.
- [ ] Confirm the legacy adapter has no import-time or global-registry coupling.
- [ ] Confirm the conformance skeleton is the right foundation for behavioral
      source suites.
- [ ] Confirm the new import-boundary gates express the intended dependency
      direction.
- [ ] Confirm no existing runtime or source path changed in this phase.
- [ ] Decide whether the documented alpha risks should be resolved before the
      first Phase 2 pilot or during that pilot.

## Recommended next phase

After explicit Phase 1 approval, proceed to Phase 2 runtime integration. Build a
single composition root and host-service adapters, keep the existing runtime as
the default, and migrate the smallest representative connector cohort needed to
prove installation binding, registry-driven execution, data-plane parity,
observability, feature-flagged cutover, and immediate rollback. Do not begin
bulk source migration or delete any legacy registry in Phase 2.
