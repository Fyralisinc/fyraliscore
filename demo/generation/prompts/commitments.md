# Generate commitments for {{ company_name }} (batch {{ batch_index }} of {{ batch_total }})

{{ company_name }} — {{ tagline }}.

## Context

You have access to actor IDs, goal IDs, decision IDs, and customer IDs
from prior generation steps. Prior commitment batches are also visible
so cross-references compound (a commitment in batch 3 can `depends_on`
a commitment from batch 1).

## What to produce

**{{ batch_size }} commitments** in this batch. The full company will
end up with ~{{ commitment_count }} active commitments across batches.

## Schema

Per commitment:

- `id` — UUIDv4.
- `title` — short imperative.
- `owner_id` — actor UUID.
- `contributors` — list of 0-3 additional actor UUIDs.
- `state` — one of: `proposed`, `active`, `at_risk`, `blocked`,
  `done`, `closed`.
- `due_date` — ISO 8601, can be in the future or past for
  done/closed states.
- `contributes_to_goal_id` — goal UUID or `null`.
- `depends_on` — list of commitment UUIDs from prior batches (can be
  empty).
- `constrained_by_decision_ids` — list of decision UUIDs (can be empty).
- `served_by_customer_id` — customer UUID or `null`.

Return `{ "items": [...] }` matching `CommitmentBatch`.

## Validation rules

1. Every `owner_id` resolves to a real actor.
2. Every `contributes_to_goal_id` (when non-null) resolves to a real
   goal.
3. `depends_on` graph stays acyclic across all batches.
4. State distribution: roughly 60% active/proposed, 20% at_risk/blocked,
   20% done/closed.
