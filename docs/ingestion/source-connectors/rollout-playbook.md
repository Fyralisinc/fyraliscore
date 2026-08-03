# Source connector rollout playbook

## Goal

Move one complete connector surface to native execution with measurable parity,
bounded blast radius, automatic rollback, and retained legacy recovery until
production evidence supports retirement.

## Preconditions

- Native capability and ingress tests pass.
- Behavioral conformance passes and its fingerprint is attached to the
  candidate.
- The released artifact has valid signed provenance and is enabled.
- Installation/authority backfill is complete for the rollout cohort.
- Baseline legacy error, latency, lifecycle, parity, and DLQ metrics exist.
- The operator has identified the exact rollback revision and owner.

## Stages

### 1. Shadow

Keep legacy authoritative. Duplicate only explicitly shadow-safe operations and
record canonical comparisons. Webhook acknowledgements, mutations, remote
subscription changes, cleanup, and other unsafe effects must not be duplicated.
Require sufficient executions and zero unexplained identity, cursor,
publication, normalization, or state mismatches.

### 2. Canary

Route a small, named tenant cohort to connector mode. Verify all ingress kinds,
reconciliation, health, refresh, and uninstall behavior represented by that
source. Keep the observation window long enough to include provider throttling
and scheduled reconciliation.

### 3. Cohort

Increase tenant coverage in bounded revisions. Avoid combining a connector code
change with a broad cohort expansion. Confirm every gateway and workflow process
has propagated the active revision.

### 4. Full

Route the connector to native mode for all installations. Continue collecting
parity where safe and retain legacy recovery through the agreed soak window.

## Default thresholds

The runtime model defaults to at least 100 executions, error rate at most 2%,
parity mismatch rate at most 0.1%, p95 regression ratio at most 1.25, zero
lifecycle failures, and DLQ-rate increase at most 0.1 percentage points. Source
owners may adopt stricter values. Looser values require an explicit risk review.

Any threshold breach requires rollback. Insufficient evidence blocks promotion
but does not itself trigger rollback.

## Revision policy

Every staged policy receives a monotonically increasing revision, stage,
tenant cohort, thresholds, creator, and audit event. Activation atomically
supersedes the prior active revision. Processes watch the active revision and
record propagation. Metric evaluation uses the recent rollout windows. A breach
atomically marks the failed revision rolled back and creates a newer active
global-legacy revision.

## Promotion checklist

- Required execution count reached.
- Error and retry classifications match baseline.
- No unexplained parity mismatch remains.
- p95 latency remains within threshold.
- No lifecycle failure occurred.
- DLQ delta remains within threshold.
- Checkpoint and S3/Kafka ordering were sampled.
- Artifact and runtime quarantine counts are zero for the connector.
- Propagation audit covers all expected process owners.
- On-call and connector owner approve the next stage.

## Rollback drill

Before full rollout, activate a legacy revision and confirm:

- process-local decisions change without deployment;
- in-flight work completes or retries under the host's existing semantics;
- no checkpoint advances ahead of durable publication;
- duplicate delivery remains idempotent;
- lifecycle and authority records remain valid;
- the audit trail identifies actor, reason, revision, and metric snapshot.

Return to connector mode only with a newer reviewed revision.

## Retirement

Full routing is not legacy retirement. Remove source-specific dispatch and
compatibility code only after the migration guide's full criteria and the
agreed production soak. If any ingress owner still calls a source-specific map
directly, the source is not eligible for retirement.

