# Generate one recommendation for {{ company_name }}

{{ company_name }} — {{ tagline }}.

## Context

This call generates exactly **one** recommendation Model — the one
described as:

> **{{ rec_kind }}**: {{ rec_proposition }} (expected impact:
> ${{ rec_impact_usd }})

You have actors, customers, commitments, decisions, and the recent
signal history. The recommendation must cite **real signal IDs** from
the prior generation step as supporting evidence, and target a real
entity (actor / commitment / decision / goal).

## Schema

- `id` — UUIDv4.
- `proposition_text` — the natural-language recommendation, 1-2
  sentences. Should expand on the seed proposition.
- `target_act_ref` — `{ "type": "<commitment|goal|decision|actor>", "id": "<uuid>" }`.
- `proposed_change` — JSON object describing what change to suggest
  (operation + payload). E.g.,
  `{"operation": "update", "payload": {"state": "blocked"}}`.
- `expected_impact_usd` — numeric.
- `supporting_observation_ids` — list of 3-8 signal UUIDs cited as evidence.
- `supporting_model_ids` — list of 0-3 other model UUIDs cited (can be empty).
- `target_actor_id` — UUID of the actor who should see this in their
  action list (typically the CEO).

Return JSON matching `GeneratedRecommendation` (single object, not a list).

## Validation rules

1. Every `supporting_observation_ids[]` resolves to a real signal.
2. `target_act_ref.id` resolves to a real entity of `target_act_ref.type`.
3. `proposition_text` references the same situation as the seed.
4. `expected_impact_usd` within 50% of the seed `${{ rec_impact_usd }}`.
