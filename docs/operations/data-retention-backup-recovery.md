# Data Retention, Backup, And Recovery Policy

Owner: Platform Engineering.
Last reviewed: 2026-06-24.

This policy defines the production default for Fyralis core data. Customer
contracts may set stricter retention, but production code must not retain data
longer than the class below unless a customer-specific exception is recorded.

## Retention Classes

| Data class | Examples | Default retention | Current enforcement |
| --- | --- | ---: | --- |
| Customer substrate | `observations`, `models`, acts, resources, relation tables | Customer lifetime plus contractual deletion window | Not auto-deleted. Requires explicit deletion/export workflow before GA. |
| Raw ingestion tier | Raw webhook/event objects in S3/MinIO/Kafka raw lanes | 30 days | Raw S3 writes are tagged with `fyralis-data-class=raw-ingestion` and `fyralis-retention-days`; normalizer reads verify `content_hash` before parsing. Object-store lifecycle must consume those tags. |
| Large object blobs/chunks | Drive files, attachments, extracted chunks, blob metadata | Customer lifetime or source uninstall plus deletion window | Policy only. Lifecycle automation remains open. |
| Think debug artifacts | `think_run_artifacts` prompt/response/validation payloads | 30 days | Enforced by `think_run_artifact_retention` housekeeper job. |
| Quality and cost telemetry | `think_runs`, `think_run_costs`, aggregate metrics | 13 months aggregated, 30-90 days high-cardinality detail | Partial. Metrics retention depends on Prometheus/remote storage config. |
| Audit trails | `audit_events`, `access_override_log`, `operator_action_log`, install audit | 7 years unless contract requires longer | Policy only. Must not be deleted by routine cleanup. |
| Dead-letter/operator queues | Post-commit DLQ, model re-eval DLQ, exhausted Think triggers | Until triaged, then 90 days after retry/quarantine | Operator surfaces exist; expiry after resolution remains open. |
| Application logs | Gateway/worker logs, ingress summaries, operational errors | 30 days hot, 1 year security-relevant archive | Policy only. Logs must be redacted before export/archive. |

## Enforced Retention Job

The housekeeper worker runs `think_run_artifact_retention` by default.

Configuration:

```env
THINK_RUN_ARTIFACT_RETENTION_DAYS=30
THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE=5000
THINK_RUN_ARTIFACT_RETENTION_DRY_RUN=0
HOUSEKEEPER_THINK_ARTIFACT_RETENTION_INTERVAL_S=86400
```

Operational metrics:

```text
housekeeper_retention_rows_total{table="think_run_artifacts",mode="delete|dry_run"}
housekeeper_retention_eligible_rows{table="think_run_artifacts"}
housekeeper_retention_last_run_timestamp_seconds{table="think_run_artifacts",status="ok"}
```

Run dry-run mode before changing TTLs in production. A dry run reports the
bounded batch size that would be affected and does not delete rows.

## Backup Requirements

Postgres:

- Enable continuous WAL archiving and point-in-time recovery for every
  production database.
- Take at least one full base backup daily.
- Retain restorable backups for at least 35 days, or longer if the customer
  contract requires it.
- Verify that the backup includes extensions required by schema drift checks,
  especially `vector`, `pg_trgm`, and `btree_gin`.

Object storage:

- Enable bucket versioning for raw payload and blob buckets.
- Enable lifecycle rules that match the retention classes above. Raw payload
  rules must expire objects tagged `fyralis-data-class=raw-ingestion` after
  the configured `S3_RAW_RETENTION_DAYS` value.
- Emit a daily inventory or equivalent manifest so missing/corrupt objects can
  be detected without reading every object.

Kafka/broker state:

- Kafka is replay/transport state, not the system of record. Retain topics long
  enough for operational replay windows, but restore from Postgres/object store
  first.

Secrets:

- Secrets are restored from the secret manager, not from database backups.
- `MASTER_KEK` or equivalent wrapping keys must be available before restoring
  encrypted source credentials.

## Backup Status Reporting

External backup automation and restore rehearsals must report their latest
status into `backup_recovery_status`. Fyralis core treats this table as the
deployment-local readiness contract: the backup engine can be RDS snapshots,
Cloud SQL backups, Velero, native object-store replication, or customer-owned
automation, but the application must expose one bounded status shape.

Record status from a backup job:

```bash
python scripts/record_backup_recovery_status.py \
  --component postgres \
  --check backup \
  --status ok \
  --occurred-at 2026-06-24T01:00:00Z \
  --details-json '{"provider":"rds","job":"daily-base-backup"}'
```

Record a restore rehearsal:

```bash
python scripts/record_backup_recovery_status.py \
  --component object_store \
  --check restore_test \
  --status ok \
  --freshness-slo-seconds 3024000 \
  --details-json '{"provider":"s3","scope":"sample-prefix"}'
```

Supported dimensions are intentionally bounded:

| Field | Allowed values |
| --- | --- |
| `component` | `postgres`, `object_store`, `broker`, `secrets`, `application_config` |
| `check_name` | `backup`, `restore_test`, `inventory` |
| `status` | `ok`, `failed`, `unknown` |

Production SLO defaults:

```env
BACKUP_POSTGRES_FRESHNESS_SLO_SECONDS=129600
BACKUP_OBJECT_STORE_FRESHNESS_SLO_SECONDS=129600
RESTORE_TEST_FRESHNESS_SLO_SECONDS=3024000
HOUSEKEEPER_BACKUP_RECOVERY_METRICS_INTERVAL_S=300
```

The housekeeper worker runs `backup_recovery_metrics` by default and emits:

```text
backup_recovery_last_success_timestamp_seconds{component,check}
backup_recovery_last_attempt_timestamp_seconds{component,check}
backup_recovery_last_success_age_seconds{component,check}
backup_recovery_health_status{component,check,state="fresh|stale|missing|failed"}
```

The `details` JSON must stay small and non-secret. It may include provider,
job name, restore scope, or runbook reference, but it must not contain object
keys, payload samples, credentials, tokens, customer identifiers, or PII.

## Restore Order

Use this order for a full environment restore:

1. Restore infrastructure primitives: network, Postgres, object buckets,
   Kafka/Redis, secret manager access, and service identities.
2. Restore secrets and wrapping keys. Do not start application workers until
   secret resolution succeeds.
3. Restore Postgres to the target point in time.
4. Apply pending migrations if restoring into a newer application version.
5. Run `scripts/check_schema_drift.py` against the restored database.
6. Restore object buckets or verify replicated object versions and inventories.
7. Start gateway in read-only or maintenance mode.
8. Start non-mutating health checks and verify tenant isolation probes.
9. Start workers in this order: post-commit, housekeeper, source schedulers,
   ingestion writers, Think workers.
10. Reconcile queue depths and DLQ counts before allowing customer traffic.

## Promotion Blockers

Production promotion is blocked when any of the following is true:

- No successful restore rehearsal exists for the target release window.
- `backup_recovery_health_status` reports `stale`, `missing`, or `failed` for
  required Postgres/object-store backup or restore-test checks.
- Schema drift fails on the restored database.
- Object-store inventory is stale or missing.
- Retention dry-run reports a destructive batch outside the approved class.
- Rollback instructions require deleting customer data.

## Customer Export Workflow

Customer data export must be tenant-scoped, approved, and auditable.

1. Confirm requester identity, tenant, contractual right to export, and scope.
2. Record the export request in the support or compliance tracker.
3. Freeze the export window and data classes:
   - substrate rows
   - raw ingestion objects if contractually allowed
   - large object blobs/chunks
   - audit trails
   - generated reasoning artifacts
4. Generate the export inside the customer's approved security boundary.
5. Redact or exclude secrets, access tokens, webhook signatures, internal
   operator notes, and cross-tenant identifiers.
6. Encrypt the export with a customer-approved key.
7. Record the export metadata, actor, timestamp, data classes, and delivery
   method in audit history.
8. Delete temporary export staging objects after delivery confirmation.

Record the completed export with bounded metadata only:

```bash
python scripts/record_customer_data_export.py \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --approver-actor "$APPROVER_ACTOR_ID" \
  --export-reference "$SUPPORT_OR_COMPLIANCE_REFERENCE" \
  --data-class substrate \
  --data-class audit_trails \
  --purpose customer_request \
  --destination-boundary customer_boundary \
  --window-start "2026-06-01T00:00:00+00:00" \
  --window-end "2026-06-25T00:00:00+00:00" \
  --manifest-sha256 "$EXPORT_MANIFEST_SHA256" \
  --encrypted \
  --temporary-staging-deleted
```

The audit command writes `operator_action_log.action =
customer_data_export.record` and refuses to record a completed export unless
encryption and temporary-staging cleanup are attested. It records reference IDs,
enums, timestamps, and actor UUIDs only; do not put payload paths, object keys,
secrets, or customer text in the export reference.

Export bundles must never be attached to issue trackers, chat, or email unless
the customer's contract explicitly approves that path and encryption controls
are in place.

## Customer Deletion Workflow

Deletion is separate from source uninstall. Uninstall stops new ingestion and
removes credentials; deletion removes customer data.

1. Confirm requester identity, tenant, legal basis, and exact deletion scope.
2. Place the tenant or source in a write-paused state for the affected scope.
3. Offer/export data first if required by contract.
4. Record an approved deletion plan with tables, object prefixes, retention
   exceptions, and backup implications.
5. Delete or tombstone tenant-scoped Postgres rows in dependency order.
6. Delete object-store blobs and raw payloads by tenant/source prefix or
   manifest, using version-aware deletion where enabled.
7. Revoke and delete source secrets in the secret provider.
8. Preserve required audit/security records for the contractual retention
   window, unless law or contract requires complete erasure.
9. Run tenant isolation and object inventory checks after deletion.
10. Record completion evidence and any retained exception classes.

Backups may retain deleted data until the backup retention window expires.
Customer-facing deletion confirmations must state this explicitly when the
contract permits backup retention.
