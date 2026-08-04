# Source connector rollout playbook

## Model

Every rollout revision is contract-only. Stages (`canary`, `cohort`, `full`)
select an admitted connector artifact revision and evidence cohort; they cannot
select a source-local implementation. Policy must resolve to
`{"global":"connector"}` and may carry only its monotonic revision.

## Before rollout

- Apply all migrations through `0190_source_connector_contract_only.sql` on a
  staging clone and inspect common installation/authority/credential rows.
- Reauthorize or reconfigure all intended installations left in `Maintenance`.
- Verify the exact manifest and implementation fingerprint with the release
  gate.
- Enable a signed measured artifact for the connector version.
- Confirm S3, Kafka, callback base URL, secret store, database pool and the
  connector's generic worker owner are configured.
- Exercise install, health, pause/resume, credential rotation and removal in a
  provider sandbox.

## Stages

1. Canary: one tenant/installation on the new artifact revision.
2. Cohort: a bounded set of tenants with representative data volume and provider
   permissions.
3. Full: all eligible Ready installations for that connector version.

At each stage evaluate execution count, error rate, p95 duration, lifecycle
failures and connector DLQ rate. Insufficient evidence holds the stage. A
threshold breach rolls back to the previous admitted artifact revision; the
execution path remains the connector contract.

## Quarantine

An invalid signature, digest mismatch, disabled artifact, or operator quarantine
removes the connector from the admitted set. New execution fails closed and the
lifecycle controller reports artifact quarantine. Restore availability by
admitting a known-good artifact revision or repairing the admission record.

## Installation controls

Use `scripts/manage_source_installations.py` for status, pause, resume,
maintenance and uninstall. Each mutation bumps the installation generation,
aligns active authority fencing, schedules immediate reconciliation and writes
an operator audit event.

## Rollback limits

Artifact rollback does not undo acknowledged raw publication. Preserve raw
objects, Kafka envelopes, connector state and idempotency keys. If the schema or
manifest changed, use only declared compatible state migrations. A database
rollback across migration `0190` requires restoring the pre-cutover database
backup and matching application revision; it is not an online source-routing
switch.
