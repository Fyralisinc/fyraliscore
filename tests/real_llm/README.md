# Real-LLM Test Suite

## Purpose

This suite exercises Company OS end-to-end against a real LLM (DeepSeek) across
three hand-authored company scenarios. It is the only test layer that drives
the cognitive path (Ingestion → Think → Acts → Bridge) with a non-deterministic
model rather than a script. Coverage is intentionally narrow — three scenarios,
~25 tests — so iteration cost stays low. Tests use tolerance bands and
existential assertions rather than strict equality so LLM variance doesn't
become noise. Full design is in `REAL-LLM-TEST-SUITE-PLAN.md` at the repo root.

## Quick start

```bash
# Required env (export or place in .env):
#   DEEPSEEK_API_KEY  — DeepSeek API key
#   DATABASE_URL      — postgres://... (matches docker-compose default)
#   OLLAMA_URL        — http://localhost:11434

docker compose up -d postgres ollama
docker compose exec ollama ollama pull nomic-embed-text:v1.5

RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/ -v
```

The `RUN_REAL_LLM=1` opt-in is enforced by `tests/real_llm/conftest.py` —
without it, every test in this tree is auto-skipped. You can also pass
`-m real_llm` instead of the env var.

To run a single test file:

```bash
RUN_REAL_LLM=1 .venv/bin/python -m pytest tests/real_llm/tests/test_smoke.py -v
```

## Layout

```
tests/real_llm/
├── conftest.py                 — fixtures (provider, db_pool, embedder, scenarios)
├── infrastructure/
│   ├── real_llm_runner.py      — @real_llm_test decorator (retry, flake tracking)
│   ├── response_cache.py       — keyed LLM-response cache, prompt-hash epochs
│   ├── assertion_helpers.py    — range / set / existential assertion helpers
│   ├── scenario_loader.py      — loads + materializes scenario YAML
│   ├── flake_tracker.py        — persists flake_rates.json across runs
│   ├── report_generator.py     — markdown reports under reports/runs/<ts>/
│   ├── think_drain.py          — wait_for_think_to_drain helper
│   └── test_infrastructure.py  — 28 self-tests for the infra itself
├── scenarios/
│   ├── 01_early_startup.yaml
│   ├── 02_growth_saas.yaml
│   └── 03_enterprise_eng.yaml
├── tests/
│   ├── test_smoke.py                — 3 cheap sanity checks per scenario
│   ├── test_ingestion_real_llm.py   — ~4 tests
│   ├── test_think_reasoning.py      — ~8 tests (the expensive bulk)
│   ├── test_acts_cascade.py         — ~5 tests
│   ├── test_bridge_queries.py       — ~4 tests
│   └── test_cross_component.py      — ~4 tests (end-to-end flows)
├── cache/                       — gitignored; per-epoch cached LLM responses
└── reports/                     — flake_rates.json + per-run markdown reports
```

## Cache

Responses are cached on disk under `tests/real_llm/cache/<epoch>/` keyed by
`(prompt, model, temperature, seed)`. The epoch is the SHA of relevant Think
prompt source files, so prompt edits invalidate the cache automatically.

Env vars:

- `LLM_CACHE_BYPASS=1` — ignore cache for reads, but still write fresh
  responses. Use this for nightly runs where you want to catch real drift.
- `LLM_CACHE_DISABLE=1` — disable the cache entirely (no reads, no writes).
  Use this if you suspect a corrupt cache entry is masking a bug.

To invalidate manually:

```bash
rm -rf tests/real_llm/cache/<epoch>/    # nuke one epoch
rm -rf tests/real_llm/cache/            # nuke all epochs
```

## Adding a new test

```python
# tests/real_llm/tests/test_my_thing.py
from tests.real_llm.infrastructure.real_llm_runner import real_llm_test
from tests.real_llm.infrastructure.assertion_helpers import (
    assert_at_least_one_model_matching,
)

@real_llm_test(attempts=3, pass_threshold=2)
async def test_think_notices_thing(scenario_02, fresh_db, think_worker):
    sequence = scenario_02.get_sequence("alice_ships_refund_flow")
    await scenario_02.inject(sequence, pool=fresh_db)
    await wait_for_think_to_drain(scenario_02.tenant_id, pool=fresh_db)

    models = await load_active_models(fresh_db, scenario_02.tenant_id)
    assert_at_least_one_model_matching(
        models,
        scope_actor_id=scenario_02.actor_id("Alice Chen"),
        proposition_kind={"state", "hypothesis"},
        context="Should have a Model about Alice's work",
    )
```

Defaults: `attempts=3, pass_threshold=2`. For structural-only assertions use
`attempts=1`; for exploratory tests use `attempts=5, pass_threshold=3`.

## Adding a new scenario

Author a YAML file under `tests/real_llm/scenarios/`. The schema (foundation
actors / customers / goals / commitments / decisions, named signal_sequences,
expected_behaviors) is documented in
`tests/real_llm/infrastructure/scenario_loader.py`. Wire a fixture for it in
`conftest.py` mirroring `scenario_01` / `scenario_02` / `scenario_03`.

## Reading reports

Each suite run writes to `tests/real_llm/reports/runs/<UTC-timestamp>/report.md`
with per-test pass / fail / passes-needed / time / cost. The persistent
`tests/real_llm/reports/flake_rates.json` is updated in place — sort by
flake-rate descending to find the chronic flakes worth investigating.

The nightly CI workflow uploads the entire `reports/` tree as an artifact
named `real-llm-reports-<run-id>`.

## Cost expectations

DeepSeek-chat pricing per fresh full-suite run:

- Fresh run, no cache: ~$3-5
- Re-run with warm cache: ~$0
- Nightly cadence (cache bypass on): ~$1.5K/year

Single-test iteration during development is effectively free once the cache is
warm — only the test you're editing triggers fresh calls if its prompt path
changed.

## Known limitations

- The Think system prompt is in active flux. Tests that assert specific
  Model output shapes may flake when the prompt evolves. Use `flake_rates.json`
  to identify chronic flakes and decide whether to widen tolerance bands or
  fix the underlying assertion.
- LLM non-determinism is real. A test passing 2/3 attempts is the design
  target, not a bug. Treat sub-50% pass rates as either an over-tight
  assertion or a Think-output regression — investigate, don't suppress.
- Embeddings inserted by Think are zero-vectored at insert time; semantic
  retrieval over Think-generated Models is not exercised here. Tests that
  rely on semantic retrieval should construct Models via a fixture, not via
  Think.
- The suite does not exercise the UI, webhook signature paths, or
  multi-tenant access enforcement. Those have their own (mocked) test
  layers.
