# Generate goals for {{ company_name }}

{{ company_name }} — {{ tagline }}.

{{ description }}

## Context

You have access to {{ actor_count }} actors (see prior step's output)
and {{ customer_count }} customers. Generated actor IDs are listed in
the system context below.

## What to produce

**{{ goal_count }} active strategic goals** spanning product, growth,
fundraising, and hiring (proportions appropriate to the company stage).

## Schema

Per goal:

- `id` — UUIDv4.
- `title` — short imperative (e.g., "Land 5 enterprise design partners by Q3").
- `description` — 1-2 sentences of what success looks like.
- `owner_id` — actor UUID drawn from the prior step's output.
- `target_date` — ISO 8601 date roughly 1-12 months in the future.
- `parent_goal_id` — UUID of another goal in this list, or `null`.
- `altitude` — one of: `strategic` (top-level), `operational`, `tactical`.

Return JSON `{ "items": [...] }` matching `GoalBatch`.

## Validation rules

1. Exactly 2-3 goals at altitude `strategic` with no parent (the roof).
2. Tree must be acyclic. Every leaf goal has an `owner_id`.
3. Owners distributed — no single actor owns more than 3 goals.
