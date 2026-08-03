# Source connector migration guide

This guide moves an existing source from direct dispatch to native connector
execution without weakening ingestion guarantees.

## Current migration boundary

The manifest-derived immutable catalog contains all 26 source families. Every
entry is a stable-v1 connector-local first-party candidate with structural and
behavioral release evidence. Connector execution is the fleet default, subject
to durable authority and signed-artifact admission. Compatibility candidate
generation no longer exists.

Native catalog authority is not permission to delete rollback code. Physical
legacy removal remains source-scoped and evidence-gated even after every
planner, fetcher, poller, webhook/gateway, reconciler, normalizer, handler,
installation, and lifecycle owner resolves through the connector runtime.

## Migration sequence

1. Inventory every ingress owner and dispatch entry for the source. Record
   planner, fetcher, reconciler, live ingress, normalizer, OAuth, installation,
   cleanup, metrics, retry, breaker, and DLQ behavior.
2. Capture parity fixtures and operational baselines: output identities,
   pagination, checkpoints, retry classifications, p95 latency, error and DLQ
   rates, lifecycle failures, and reconciliation repairs.
3. Add or verify the source in `source-index.json` and its stable-v1 manifest.
   The catalog is derived and must not be edited as a second registry.
4. Implement a native connector root and native capabilities. Existing source
   clients may be reused behind capability facets; do not call mutable dispatch
   maps from a native candidate.
5. Backfill the common installation, authority, credential-reference, and
   provenance records. Keep provider-specific tables only as extension storage
   where they still carry provider-specific data.
6. Route installation and OAuth through connector capabilities. Validate
   granted scopes against the manifest before persisting current credentials.
7. Integrate each ingress owner through registry resolution and the capability
   executor. Keep S3-first publication, Kafka ordering, acknowledgements, and
   checkpoints in the host.
8. Pass fleet wiring, static conformance, and behavioral conformance. Update the
   independently reviewed release-evidence record. Sign the measured artifact
   and enable its attestation. Startup and continuous quarantine must be clear.
9. Roll out in stages: shadow, canary tenants, bounded cohort, then full.
10. Hold full routing while gathering production evidence across backfill,
    incremental/live ingress, reconciliation, lifecycle, and uninstall.
11. Exercise configuration-only rollback and artifact quarantine.
12. Record one `source_connector_retirement_evidence` row for every legacy
    surface, then remove those dispatch entries only after the retirement
    criteria below are satisfied.

## Invariants to compare

| Area | Required parity evidence |
| --- | --- |
| Envelopes | Equal source, tenant, installation, native identity, and metadata semantics |
| Durability | S3 acknowledgement precedes Kafka publication and checkpoint advancement |
| Pagination | Same terminal behavior; cursor is monotonic and advances after durable success |
| Idempotency | Stable keys and replay behavior under duplicate delivery |
| Reconciliation | Equivalent missing-shard detection and repair requests |
| Failure policy | Equivalent retryability, throttling, breaker, timeout, cancellation, and DLQ behavior |
| Trust | Same or stricter trust ceiling and content classification |
| Lifecycle | Install, health degradation/recovery, pause, maintenance, cleanup, and removal parity |
| Tenancy | No cross-tenant authority, secret, state, callback, or publication access |
| Telemetry | Connector labels added without losing existing operational signals |

## Rollback points

- A routing revision can restore global or connector-scoped `legacy` mode.
- Threshold breaches create and activate a durable legacy revision.
- Artifact admission quarantine overrides any routing revision in process.
- The legacy implementation remains callable until retirement evidence is
  accepted.

Rollback does not undo already acknowledged raw publication. Resume from the
host-owned durable checkpoint and preserve idempotency keys.

## Legacy retirement criteria

Retire a source path only when all are true:

- every declared ingress kind is native and registry-resolved;
- installation and lifecycle authority are in the common control plane;
- OAuth/credential rotation and cleanup are connector-owned where applicable;
- conformance and parity suites are green;
- signed artifact admission is green in the target environment;
- production metrics meet thresholds for the agreed observation window;
- rollback has been exercised without checkpoint or publication drift;
- the owning team accepts removal of the source-specific dispatch entries.
- resilience evidence is current for the exact connector version and target
  region, including disaster-recovery replay and multi-region failover;
- every retired surface has a durable evidence reference and named rollback
  owner.

Until then, legacy code is intentional rollback infrastructure, not definition
or registration authority and not dead code.
