# Perception Pipeline Operations Runbook

## Runtime chain

```text
identity_resolution_outbox
  -> perception_knowledge_outbox
  -> perception_outbox (contract v3)
  -> episode lifecycle and snapshots
  -> episode_snapshot_outbox
  -> think_trigger_queue (episode_snapshot)
```

The required production processes are:

- `identity_resolution_worker`
- `perception_knowledge_worker`
- `episode_constructor_worker`
- `episode_settlement_worker`
- `episode_handoff_worker`

Every process exposes `/healthz` and `/metrics` on
`INGESTION_HEALTH_PORT`. Prometheus alerts if a required process is down or a
durable queue contains dead-letter work.

## Authority and rollback

`REASONING_INGRESS_MODE=episode` is the production setting. Observation
writers still create identity work, but they do not create direct
`event_arrival` T1 triggers. Only the episode handoff worker creates the T1
trigger, using the snapshot-outbox ID as the trigger ID.

Set a row in `reasoning_ingress_policies` to override one tenant. Set the
deployment environment to `direct` for a global rollback. Query-created
episodes always enter reasoning even during direct mode because their caller is
waiting for a bounded answer.

## Dead-letter response

1. Identify the process and inspect its `*_queue_dead_letter` metric.
2. Query the corresponding outbox for `status='dead_letter'`, including
   `last_error`, lineage IDs, and `attempt_count`.
3. Correct the data or deployment defect. Do not edit immutable identity,
   knowledge, membership, lifecycle, or snapshot rows.
4. Requeue only the exact failed outbox row by changing its status to `pending`,
   clearing lease fields, and setting `available_at=now()` in an audited
   operator transaction.
5. Confirm that downstream outbox depth advances and the item completes.

## Late data

A new or status-changed `perception_claim` automatically creates
`claim.changed` knowledge work against the latest identity snapshot. The new
knowledge snapshot produces a new v3 routing input. New membership reactivates
a dormant episode or reopens a settled episode. The settlement worker then
seals a successor snapshot; prior snapshots remain immutable.

## Cutover checks

- No pending or leased `perception_outbox` row has `contract_version < 3`.
- Every v3 row has identity and knowledge snapshot IDs/hashes plus a claim-set
  hash.
- Every episode membership created after cutover carries the same knowledge
  lineage as its router run.
- In episode mode, new observation ingestion does not create `event_arrival`
  triggers.
- Every completed automatic snapshot-outbox item has a matching
  `episode_snapshot` trigger with the same ID.
