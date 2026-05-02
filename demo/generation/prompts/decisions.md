# Generate decisions for {{ company_name }}

{{ company_name }} — {{ tagline }}.

## Context

You have actors and {{ goal_count }} goals from prior steps. Some of
the recommendations the action list will surface depend on revisiting
old decisions whose conditions have changed; design decisions with that
in mind.

## What to produce

**{{ decision_count }} active decisions** on architecture, market
positioning, hiring philosophy, customer commitments, fundraising
readiness, etc. Cover a mix of recent and 6-18-month-old decisions
(reflected in `metadata.decided_at`).

## Schema

Per decision:

- `id` — UUIDv4.
- `title` — short, e.g., "Postgres-only data architecture".
- `decision_text` — 1-2 sentences describing what was chosen.
- `rationale` — 2-3 sentences explaining why, including the conditions
  assumed at the time.
- `scope` — JSON object: `{ "applies_to": [...], "exceptions": [...] }`.
- `revisit_triggers` — list of 1-3 string triggers that would warrant
  revisiting (e.g., "If query latency exceeds 500ms p95").

Return `{ "items": [...] }` matching `DecisionBatch`.

## Validation rules

1. Every decision has non-empty `rationale` and at least one
   `revisit_triggers` entry.
2. At least one decision feels potentially stale (its assumed
   conditions plausibly no longer hold) — this is the seed for the
   "decision_revisit" recommendation.
