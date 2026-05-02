# Generate actors for {{ company_name }}

You are seeding the actor roster for a synthetic company used in a sales
demo. The company is **{{ company_name }}** — {{ tagline }}.

{{ description }}

## What to produce

A list of **{{ actor_count }} actors** that fits this role mix:

```
{{ role_mix_yaml }}
```

The reporting structure should be `{{ reporting_depth }}` layers deep.
Exactly one actor is the CEO (named `{{ ceo_name }}`, email
`{{ ceo_email }}`); set their `manager_id` to `null`. Every other actor
references a real `id` from this same list as their manager.

## Schema

Each actor:

- `id` — UUIDv4 string (stable; you generate it).
- `name` — realistic full name; vary backgrounds.
- `role` — one role string drawn from the mix above (lowercase, snake_case).
- `manager_id` — UUID of another actor in this list, or `null` for the CEO.
- `personality_brief` — one sentence of operating style for use in
  signal generation later (e.g., "writes long PR descriptions, prefers
  async over meetings").

Return JSON matching the `ActorBatch` schema (a `{ "items": [...] }`
wrapper). Roles must sum to the role mix within ±1 per role.

## Validation rules

1. Exactly one actor with `role: founder` AND name `{{ ceo_name }}` —
   they are the CEO.
2. No reporting cycles. Every non-CEO actor has a manager.
3. UUIDs unique across the list.
4. Every email is plausible for `{{ company_name }}`.
