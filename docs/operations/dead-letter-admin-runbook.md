# Durable Dead-Letter Admin Runbook

This runbook covers the gateway admin API for durable dead-letter queues used by
post-commit actions, model re-evaluation, and exhausted Think triggers.

It does not cover the Kafka ingestion DLQ persisted in `ingestion_failures`.
Use [ingestion-dlq-replay-quarantine-runbook.md](ingestion-dlq-replay-quarantine-runbook.md)
for ingestion replay and quarantine.

## Scope

Admin API:

```text
GET  /api/admin/dead-letters
POST /api/admin/dead-letters/{queue}/{item_id}/retry
POST /api/admin/dead-letters/{queue}/{item_id}/quarantine
```

Supported queues:

```text
post_commit
model_reeval
think_trigger
```

Security contract:

- Requests must carry a valid gateway bearer token.
- The actor must have the tenant-scoped `admin` role.
- Every list, retry, and quarantine action writes `operator_action_log`.
- List output is sanitized and must not include raw payload bodies.
- Quarantine preserves records for later investigation; do not delete rows
  during incidents.

## Triage Flow

1. Confirm the tenant and incident scope.
2. Inspect queue counts from metrics or the admin endpoint.
3. List the affected queue with a small limit first.
4. Classify each row as retryable, quarantine-only, or blocked on code/data
   repair.
5. Retry only after the owning code path has been fixed or the dependency has
   recovered.
6. Quarantine rows that are malformed, unsafe to replay, or require manual
   customer-data review.
7. Verify `operator_action_log` contains the operator action.
8. Re-check queue depth and user-visible product errors.

## List Dead Letters

```bash
curl -sS \
  -H "Authorization: Bearer $FYRALIS_ADMIN_TOKEN" \
  "https://$FYRALIS_GATEWAY_HOST/api/admin/dead-letters?queue=post_commit&limit=25"
```

List every supported queue:

```bash
curl -sS \
  -H "Authorization: Bearer $FYRALIS_ADMIN_TOKEN" \
  "https://$FYRALIS_GATEWAY_HOST/api/admin/dead-letters?queue=all&limit=50"
```

Include quarantined rows when auditing prior operator actions:

```bash
curl -sS \
  -H "Authorization: Bearer $FYRALIS_ADMIN_TOKEN" \
  "https://$FYRALIS_GATEWAY_HOST/api/admin/dead-letters?queue=think_trigger&include_quarantined=true"
```

Expected response shape:

```json
{
  "items": [
    {
      "queue": "post_commit",
      "id": "018f0000-0000-7000-8000-000000000001",
      "state": "dead_lettered",
      "attempts": 5,
      "last_error": "bounded error preview"
    }
  ],
  "queues": ["post_commit"],
  "limit": 25,
  "include_quarantined": false
}
```

Do not use this endpoint as a payload inspection tool. It intentionally omits
raw action payloads and customer content.

## Retry

Retry when the failure cause is transient or has been fixed.

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $FYRALIS_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"dependency recovered; retrying once"}' \
  "https://$FYRALIS_GATEWAY_HOST/api/admin/dead-letters/post_commit/$ITEM_ID/retry"
```

Success:

```json
{
  "status": "retry_scheduled",
  "queue": "post_commit",
  "id": "018f0000-0000-7000-8000-000000000001"
}
```

Queue-specific behavior:

- `post_commit`: clears `dead_lettered_at`, resets attempts, and allows the
  existing action row to be picked up again.
- `model_reeval`: creates a fresh model re-evaluation queue row and records the
  retry link on the dead-letter row.
- `think_trigger`: clears terminal failure fields so the trigger can be picked
  up again.

After retry:

1. Confirm `operator_action_log.action = 'dead_letter.retry'`.
2. Confirm queue depth moves in the expected direction.
3. Confirm no new dead-letter row appears for the same item.

## Quarantine

Quarantine when replaying the row could corrupt state, repeat a customer-visible
error, or require deeper investigation.

```bash
curl -sS -X POST \
  -H "Authorization: Bearer $FYRALIS_ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reason":"malformed payload; waiting for parser fix"}' \
  "https://$FYRALIS_GATEWAY_HOST/api/admin/dead-letters/think_trigger/$ITEM_ID/quarantine"
```

Success:

```json
{
  "status": "quarantined",
  "queue": "think_trigger",
  "id": "018f0000-0000-7000-8000-000000000001"
}
```

After quarantine:

1. Confirm the default list endpoint no longer returns the item.
2. Confirm `include_quarantined=true` does return it.
3. Confirm `operator_action_log.action = 'dead_letter.quarantine'`.
4. Link the quarantine reason in the incident or release record.

## Failure Responses

```text
401 unauthenticated
403 forbidden
404 dead_letter_not_found
409 dead_letter_quarantined
409 dead_letter_already_retried
409 not_dead_lettered
```

Treat repeated `409` responses as a signal to re-list the queue before taking
another action. Another operator or worker may have already advanced the row.

## Rollback Guidance

Retries are operational actions, not schema rollbacks. If a retry causes new
failures:

1. Stop retrying the affected queue.
2. Quarantine the new failures.
3. Pause the owning worker or feature flag if queue depth is rising.
4. Roll back or forward-fix the application code path.
5. Resume retries only after a focused staging or local reproduction passes.
