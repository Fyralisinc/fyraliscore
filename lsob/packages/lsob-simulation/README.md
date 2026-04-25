# lsob-simulation

Tick-based deterministic simulator for the LSOB benchmark. Produces `Corpus`
objects (signals + monthly ground-truth snapshots) from a `SimulationConfig`.

## Architecture

- `Simulator` — top-level orchestrator. 1 tick = 1 simulated day. Advances
  commitment / customer / actor state, emits signals, injects scheduled
  turbulence events, records monthly ground truth.
- `ActorState`, `CommitmentState`, `CustomerState` — internal state machines.
- `SignalGenerator` — protocol with two implementations:
  - `TemplateSignalGenerator` (default; deterministic templates keyed by
    channel, commitment state, and actor persona). This is what tests and
    mini-corpus runs use — no API keys required.
  - `LLMSignalGenerator` (structural stub for Anthropic-backed generation;
    wired but not invoked in Phase 1).
- `GroundTruthRecorder` — emits monthly `GroundTruth` snapshots.
- `write_corpus` / `read_corpus` — JSON for plain `.json` paths, JSON Lines
  compressed with zstd for `.jsonl.zst` paths.
- `validate_corpus_file` — actor/commitment/customer reference checks and
  chronological ordering checks.

## Determinism

Every randomness call uses a seeded `random.Random(seed + tick + 1)` derived
from `SimulationConfig.seed`. Given the same config, two runs produce
byte-identical corpora (same signal IDs, text, timestamps).

## CLI

Installed via `[project.scripts]` as `lsob-simulation`.

```bash
# validate a corpus file (.json or .jsonl.zst)
uv run lsob-simulation validate-corpus fixtures/mini_corpus_a.json

# produce a corpus from a YAML config
uv run lsob-simulation run \
    --config packages/lsob-simulation/configs/CompanyA.yaml \
    --output corpora/companyA.jsonl.zst
```

## Mini-run (<30s)

```bash
cat > /tmp/mini.yaml <<'EOF'
company_id: MiniDemo
num_actors: 5
commitment_generation_rate: 0.1
customer_count: 2
seed: 42
start_date: "2026-01-01T00:00:00Z"
duration_months: 1
actor_personality_distribution:
  reliable: 0.5
  optimistic: 0.25
  pessimistic: 0.15
  flaky: 0.1
EOF

uv run lsob-simulation run --config /tmp/mini.yaml --output /tmp/mini.jsonl.zst
uv run lsob-simulation validate-corpus /tmp/mini.jsonl.zst
```

Completes in well under a second on a laptop; produces ~100 signals plus
one monthly ground-truth snapshot.

## Preset configs

- `configs/CompanyA.yaml` — 200 actors, steady-state, 12 months, seed 42.
- `configs/CompanyB.yaml` — 1000 actors, exec departure + layoff turbulence.
- `configs/CompanyC.yaml` — 500 actors, scheduled `reorg` event at month 6.

## Tests

```bash
uv run pytest packages/lsob-simulation/tests -n auto
```

Unit tests for the state machines, I/O roundtrips, validator behavior,
simulator determinism, and CLI smoke tests.
