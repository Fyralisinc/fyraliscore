# Ingestion DLQ Replay And Quarantine Runbook

This runbook covers the Kafka-first ingestion DLQ persisted in
`ingestion_failures`. It does not cover post-commit, model re-eval, or Think
trigger dead letters; those use `/api/admin/dead-letters`.

## Preconditions

- Confirm the failed source parser or writer defect is understood.
- Confirm the raw object still exists for replay. Rows without `raw_s3_key`
  cannot be replayed through the raw lane.
- Use an operator actor id that exists in the target tenant and has tenant-wide
  `admin` or `leadership`. Every list, replay, and quarantine action writes
  `operator_action_log`.
- Never replay rows that failed because the source payload is permanently
  malformed, revoked, out of retention, or tied to deleted credentials.

## List Open Failures

```bash
.venv/bin/python scripts/manage_ingestion_dlq.py list \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --source slack \
  --limit 50
```

The output is bounded JSON. Error previews are redacted for token-like strings
and emails. Raw payloads, source bodies, prompts, and object contents are never
printed by the tool.

## Replay A Failure

Use replay only after the processing defect is fixed or the missing dependency
has recovered.

```bash
.venv/bin/python scripts/manage_ingestion_dlq.py replay \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --failure-id "$INGESTION_FAILURE_ID" \
  --ingress-kind webhook \
  --reason "normalizer fix deployed"
```

Replay behavior:

- Acquires a per-failure advisory lock so two operators cannot replay the same
  row concurrently.
- Requires `raw_s3_key`.
- Derives `content_hash` from the raw object key.
- Publishes a fresh `RawEnvelope` to `ingestion.raw.<source>`.
- Flushes the producer before marking the row resolved.
- Marks `resolution_kind='replayed'` and `resolved_by='operator:<actor_id>'`
  only after publish succeeds.

If `--ingress-kind` is omitted, replay requires a valid `ingress_kind` in the
failure `error_context`.

## Quarantine A Failure

Use quarantine when replay would be unsafe or impossible but the row should
remain available for audit.

```bash
.venv/bin/python scripts/manage_ingestion_dlq.py quarantine \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --failure-id "$INGESTION_FAILURE_ID" \
  --reason "permanently malformed source payload"
```

Quarantined rows stay unresolved but are excluded from default list output.
Include them when needed:

```bash
.venv/bin/python scripts/manage_ingestion_dlq.py list \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --include-quarantined
```

## Validation

After replay:

- Verify `ingestion_failures.resolved_at IS NOT NULL`.
- Verify `operator_action_log` contains `dead_letter.retry` for the failure id.
- Watch `ingestion.raw.<source>`, `ingestion.normalized.<source>`, and writer
  metrics for drain.
- Confirm no new DLQ row appears with the same `(tenant_id, source, raw_s3_key,
  failure_kind)`.

After quarantine:

- Verify `quarantined_at`, `quarantined_by`, and `quarantine_reason` are set.
- Verify default `list` output no longer returns the row.
- Verify `operator_action_log` contains `dead_letter.quarantine`.

## Rollback

Replay does not delete raw objects or existing rows. If a replay produces a new
failure, quarantine the new row with the reason and leave both rows for RCA.
Do not manually delete `ingestion_failures` rows during an incident.
