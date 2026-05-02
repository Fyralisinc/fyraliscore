# Generate signals for {{ company_name }} (channel {{ channel }}, week {{ week_index }})

{{ company_name }} — {{ tagline }}.

## Context

You have actors, customers, commitments, and decisions from prior
steps. This call generates signals for a single channel (`{{ channel }}`)
over the week starting `{{ week_start_iso }}`. Signals must reference
real entity IDs in their `entities_mentioned` field — do not invent new
IDs.

## What to produce

Roughly **{{ signals_per_call }} signals** for this channel/week. The
density should feel right for the channel (Slack: chatty; GitHub:
lower volume but denser content; email: medium; calendar: meeting-shaped).

## Schema

Per signal:

- `id` — UUIDv4.
- `source_channel` — `"{{ channel }}"`.
- `source_ref` — channel-native id (e.g., Slack message ts, PR number).
- `author_id` — actor UUID.
- `occurred_at` — ISO 8601 timestamp inside the week window.
- `content_text` — natural-language content (1-3 sentences).
- `entities_mentioned` — list of `{type, id}` references to real
  actors/commitments/customers/decisions.

Return `{ "items": [...] }` matching `SignalBatch`.

## Validation rules

1. `author_id` and every `entities_mentioned[].id` resolve to a real entity.
2. `occurred_at` falls inside `[{{ week_start_iso }}, {{ week_end_iso }}]`.
3. `source_ref` is unique within this batch.
