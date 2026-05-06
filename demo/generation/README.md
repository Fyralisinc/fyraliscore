# demo/generation — one-shot LLM generation of demo company snapshots

Source-of-truth: [`DEMO-BUILD-PLAN.md`](../../DEMO-BUILD-PLAN.md), Session 2.

This package builds a SQL snapshot for the Pelago demo company by
running a sequenced LLM pipeline. Each step's output feeds the next
step's prompt so cross-references stay consistent.

## Layout

```
demo/generation/
  specs/             # company-shaped YAML inputs
    pelago.yaml
  prompts/           # markdown templates with {{ var }} placeholders
    actors.md
    customers.md
    goals.md
    decisions.md
    commitments.md
    signals.md
    recommendations.md
  schemas.py         # Pydantic schemas for each LLM call
  cache.py           # prompt-hash file cache (.cache/)
  validate.py        # internal-consistency checker
  sql_emit.py        # bundle -> SQL snapshot
  generate.py        # orchestrator + CLI
  tests/             # validate + sql_emit unit tests
```

## Run

```bash
# Plan + cost estimate. No LLM call. No API key required.
python -m demo.generation.generate --company pelago

# Execute. Requires LLM_API_KEY in env. Writes demo/snapshots/pelago-v1.sql.
python -m demo.generation.generate --company pelago --execute

# Compressed snapshot (recommended for the loader).
python -m demo.generation.generate --company pelago --execute --compress
```

The generation pipeline runs sequentially:

1. **actors** — one call producing the full roster for the spec's
   `role_mix` and `reporting_depth`.
2. **customers** — one call producing the `customer_count` Resources
   summing to roughly `customer_arr_total`.
3. **goals** — one call referencing actor ids.
4. **decisions** — one call.
5. **commitments** — `ceil(commitment_count / 30)` batched calls; later
   batches reference earlier batches via `depends_on`.
6. **signals** — one call per channel per week of the recent window
   (`SIGNAL_CHANNELS × 6 weeks`), plus a small number of older fills.
7. **recommendations** — one call per recommendation entry in the spec.

After generation, `validate.py` runs and the orchestrator exits non-zero
if any error is reported.

## Caching

Every LLM response is keyed by `sha256(system + user + model + schema_name)`
and stored as JSON under `demo/generation/.cache/`. Re-running with the
same inputs hits the cache. To force regeneration of a single step,
delete the matching cache file (or wipe the cache directory).

## Cost expectations

Per DEMO-BUILD-PLAN.md:

| Company    | Calls (approx) | Estimated cost |
|------------|----------------|----------------|
| Pelago     | ~50            | $10–25         |

Budget 2–3× for prompt iteration. The dry-run plan output gives a
per-call cost estimate based on the heuristic table in
`generate.py:COST_ESTIMATES_USD`.

## Validation rules (`validate.py`)

- Unique entity ids per kind.
- Actor reporting graph: every non-CEO has a real manager; no cycles.
- Goal tree: every leaf goal has a real owner; parent edges acyclic.
- Commitment refs: `owner_id`, `contributors`, `contributes_to_goal_id`,
  `served_by_customer_id`, `constrained_by_decision_ids`, `depends_on`
  all resolve. `depends_on` graph is acyclic.
- Signal refs: `author_id` and every `entities_mentioned[].id` resolve.
- Recommendation refs: `target_act_ref`, `target_actor_id`,
  `supporting_observation_ids`, `supporting_model_ids` all resolve.
- Counts within ±10% of spec for actors / customers / goals /
  decisions / commitments / recommendations.

## Dependencies

Already in `pyproject.toml`:

- `pydantic` (schemas)
- `pyyaml` (specs; under `[project.optional-dependencies].dev`)
- `anthropic` / `openai` (the LLM provider)

Optional:

- `zstandard` — only required when emitting `.sql.zst` snapshots
  (`--compress`). The loader at `services/demo/snapshot.py` also reads
  zst if installed.

## Tests

Run from repo root:

```bash
pytest demo/generation/tests/ -q
```

Tests do **not** require an LLM key — they exercise the validator and
SQL emitter against a tiny hand-crafted bundle.
