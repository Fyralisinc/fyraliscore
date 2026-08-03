# Source connector development guide

This guide describes how to add a first-class Fyralis source connector. The
normative contract is the [Source Connector Contract](../../architecture/source-connector-contract.md);
this document is the repository-level implementation path.

## What a connector is

A connector is an immutable `SourceConnector` definition with:

- a manifest identifying one canonical `fyralis/<source>` connector;
- factories for the versioned capability facets it implements;
- installation-scoped binding through `BindingContext`;
- access only to host services granted by its manifest and durable authority;
- a conformance fingerprint independently approved before registry registration.

Connectors decode provider behavior. The host still owns tenant selection,
durable raw publication, Kafka acknowledgement, checkpoints, retry policy,
circuit breaking, DLQ routing, metrics, leases, and process lifecycle.

## Add a source

Using Stripe as an example:

1. Add `stripe` to the canonical raw-envelope source literal and to
   `CONNECTOR_CATALOG`, with its real ingress kinds (`webhook`, `backfill`, or
   `poll`). Do not add a second source registry.
2. Add a JSON manifest under `services/ingest/connectors/manifests` with ID
   `fyralis/stripe`, semantic connector version,
   supported contract range, declared capabilities, secret slots, OAuth scopes,
   outbound hosts, trust ceiling, and isolation profile.
3. Implement a root `SourceConnector`. Its `bind()` method must return facets
   created for the supplied installation and host services; it must not retain
   process-global tenant or credential state.
4. Implement only the capabilities Stripe needs. A likely initial set is OAuth,
   OAuth lifecycle, historical pull, incremental poll, webhook, reconciliation,
   identity, normalization, health, and cleanup.
5. Put OAuth credentials into named secret slots. Return `SecretCandidate`
   values from OAuth completion or refresh; never persist or log token values in
   connector code.
6. Emit `SourceRecord` values or verified webhook events. Do not write S3,
   publish Kafka, advance checkpoints, or acknowledge provider cursors inside
   the connector.
7. Define stable external identity and normalization behavior. Preserve the
   existing `RawEnvelope` and `NormalizedEnvelope` semantics.
8. Run structural and behavioral conformance, review the result, and add its
   independently approved fingerprint to `release-evidence.json`. Registration
   fails if computed and approved evidence differ.
9. Add signed artifact provenance and enable the artifact record only after its
   measured implementation digest, manifest digest, conformance fingerprint,
   builder, and signature are valid.
10. Keep the legacy Stripe path available while running shadow, canary, cohort,
    and full rollout. Remove it only after every ingress surface has production
    evidence and a rollback drill has passed.

## Manifest shape

```yaml
apiVersion: sources.fyralis.io/v1alpha1
kind: SourceConnector
metadata:
  id: fyralis/stripe
  source: stripe
  displayName: Stripe
  version: 0.1.0
  owner: ingestion
spec:
  contract: ">=1.0,<2.0"
  implementation: services.ingest.connectors.stripe:build_connector
  maturity: preview
  capabilities:
    - id: installation.oauth2
      version: 1
    - id: ingestion.webhook
      version: 1
    - id: semantic.identity
      version: 1
    - id: semantic.normalization
      version: 1
    - id: health.probe
      version: 1
    - id: lifecycle.cleanup
      version: 1
  ingressKinds: [webhook]
  permissions:
    secretSlots: [oauth_access_token, webhook_signing_secret]
    outboundHosts: [api.stripe.com, connect.stripe.com]
    requestedScopes: []
  trust:
    maximumTier: attested_agent
  runtime:
    isolation: in_process_trusted
```

The example is illustrative. Requested capabilities and authority must reflect
the actual product behavior and provider authorization model.

## Implementation rules

- Use capability DTOs from `services.ingest.source_contract`; do not introduce
  provider SDK objects into the contract layer.
- Treat cursors as opaque, versioned state. A successful page may propose a
  cursor, but the host advances it only after durable publication.
- Webhook verification must operate on the bounded raw request and must fail
  before decoding untrusted events as authoritative.
- Identity must be deterministic for the same native entity.
- Normalize into observation drafts without mutating domain storage.
- Use governed HTTP so host allowlists, deadlines, cancellation, and telemetry
  remain enforceable.
- Classify provider failures with the typed source-contract error taxonomy.
- Cleanup, refresh, revocation, and state migration operations must be
  idempotent under retry.

## Required tests

Add unit tests for each facet, behavioral conformance tests, an installation
binding test, ingress parity tests, and a rollback test. Pagination tests must
cover terminal pages and monotonic cursors. Webhook tests must cover valid,
invalid, replayed, oversized, and multi-event requests. OAuth tests must cover
state validation, insufficient scopes, refresh rotation, revocation, and secret
redaction.

The minimum repository gate is:

```bash
.venv/bin/pytest -q \
  services/ingest/source_contract/tests \
  services/ingest/connector_conformance/tests \
  services/ingest/connector_runtime/tests \
  services/ingest/connector_platform/tests
```

Also run `python scripts/check_source_connector_release_gate.py`; it verifies
the complete inventory, factories, independent evidence, measurable artifacts,
and legacy-safe bootstrap policy.

## Review checklist

- One canonical source and connector ID exist.
- The manifest requests least authority.
- No raw credentials appear in state, logs, metrics, or errors.
- All declared capabilities are implemented and all providers are declared.
- Conformance evidence is reproducible.
- Raw durability and checkpoint ordering stay host-owned.
- Tenant isolation is covered by binding and persistence tests.
- Shadow comparison and rollback are ready before connector routing is enabled.
