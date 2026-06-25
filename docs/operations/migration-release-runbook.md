# Migration Release Runbook

This runbook defines the production migration policy for Fyralis releases. It
applies to database schema changes, data backfills, queue contract changes, and
any release that changes durable data semantics.

## Required Release Artifacts

Every release that includes migrations must attach:

- Release notes using [release-notes-template.md](release-notes-template.md).
- Migration list and expected runtime.
- Expand/contract phase for each migration.
- Staging clone rehearsal result.
- Schema drift report.
- Backup or snapshot verification evidence.
- Rollback or forward-fix plan.
- Release owner and on-call owner.

## Expand/Contract Policy

Use expand/contract for all customer-data schema changes unless the release
owner documents why the change is purely additive and immediately safe.

1. Expand:
   - Add tables, columns, indexes, constraints, and nullable fields before code
     reads or writes them as required.
   - Add new readers in compatibility mode. They must tolerate both old and new
     shapes.
   - Add new writers behind a feature flag when a dual-write period is needed.
2. Backfill:
   - Run bounded batches with progress checkpoints and retry-safe statements.
   - Emit metrics for scanned rows, changed rows, failures, and estimated
     remaining work.
   - Keep old code paths valid while the backfill runs.
3. Switch:
   - Move reads to the new shape only after the backfill is complete and schema
     drift passes.
   - Keep fallback reads for one release boundary when practical.
   - Keep write compatibility until the previous application version is no
     longer eligible for rollback.
4. Contract:
   - Remove old columns, tables, triggers, or indexes only after one release
     boundary.
   - Treat the removal as destructive and require the approval marker described
     below.
   - Never use deletion of customer data as the rollback mechanism.

## Destructive Migration Policy

The architecture ratchet rejects new destructive migrations unless the SQL file
contains an approval marker with backup, rollback, and owner evidence.

Use this exact marker shape near the top of the migration:

```sql
-- destructive-migration-approved: backup=<snapshot-or-ticket> rollback=<runbook-or-ticket> owner=<name>
```

The ratchet currently treats the following as destructive:

- `DROP TABLE`
- `DROP COLUMN`
- `DROP INDEX`
- `TRUNCATE`
- `DELETE FROM`
- `ALTER COLUMN ... TYPE`

The marker is not a substitute for review. It is a searchable release contract
that points reviewers to the evidence they need before merge or promotion.

Before approving a destructive migration:

1. Verify a restorable backup or snapshot exists for the target environment.
2. Restore the backup into a staging clone or prove the managed snapshot restore
   path with the provider's latest successful restore event.
3. Apply the migration to the staging clone.
4. Run schema drift and readiness gates.
5. Record the backup evidence and rollback or forward-fix plan in the release
   notes.

## Standard Migration Commands

Use the same command paths in local rehearsal, CI, staging, and production.

```bash
.venv/bin/python scripts/apply_db_migrations.py \
  --dsn "$DATABASE_URL"

.venv/bin/python scripts/check_schema_drift.py \
  --dsn "$DATABASE_URL"

.venv/bin/python scripts/check_architecture_ratchets.py

.venv/bin/python scripts/run_operational_readiness_gates.py
```

For staging clone rehearsals:

```bash
export DATABASE_URL="$STAGING_CLONE_DATABASE_URL"
.venv/bin/python scripts/apply_db_migrations.py --dsn "$DATABASE_URL"
.venv/bin/python scripts/check_schema_drift.py --dsn "$DATABASE_URL"
.venv/bin/python scripts/run_operational_readiness_gates.py
```

## Rollback And Forward-Fix Decision

Prefer application rollback when:

- The schema is backward-compatible with the previous application build.
- No destructive contract step has run.
- New writes can be paused through feature flags or worker scale-down.

Prefer forward-fix when:

- A destructive migration has already run.
- The previous application build cannot read the new durable shape.
- Rolling back would require deleting, rewriting, or guessing customer data.

Rollback procedure:

1. Pause rollout traffic or disable the release flag.
2. Pause autonomous write workers for the affected tenant cohort.
3. Snapshot queue depths and DLQ counts.
4. Revert the application build only after confirming schema compatibility.
5. Run schema drift and operational readiness gates.
6. Resume traffic gradually and watch error rate, queue depth, and DLQs.

Forward-fix procedure:

1. Keep traffic paused or canaried for the affected cohort.
2. Add a corrective migration or compatibility shim.
3. Apply the fix to the staging clone first.
4. Run schema drift and readiness gates.
5. Apply the fix to production.
6. Record the incident, cause, and prevention item in release notes.

## Backfill Requirements

Backfills must be safe to stop and resume.

- Use deterministic batch predicates, such as primary key ranges or
  `updated_at` windows.
- Commit each batch independently.
- Record progress in a durable checkpoint table or release log.
- Use `FOR UPDATE SKIP LOCKED` where concurrent workers may process rows.
- Rate-limit batches to protect gateway, worker, and database SLOs.
- Emit metrics without tenant IDs, emails, object keys, raw payload text, or
  channel names.

## Release Blockers

Block promotion when any of these are true:

- Schema drift fails after migration.
- Backup or restore evidence is missing for a destructive migration.
- The rollback or forward-fix plan is empty.
- The previous application build cannot run against the expanded schema during
  the rollback window.
- A backfill cannot be resumed safely.
- Migration rehearsal was skipped for a customer-data migration.
- New destructive SQL lacks the `destructive-migration-approved:` marker.
