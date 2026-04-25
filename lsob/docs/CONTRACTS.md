# LSOB Contracts

This document explains each Pydantic model in `lsob-contracts`. Every downstream package codes against these shapes — do not mutate them without a coordinated upgrade across all packages.

## Why contracts-first?

The benchmark has nine concurrent build streams after Phase 0. Each one needs a stable input/output shape to code against. Contracts are the "public API" of the benchmark; everything else is implementation.

## Core models

### `Signal`
A single raw emission in the simulated org. One Slack message, one PR description, one email.

| field | type | meaning |
|---|---|---|
| `signal_id` | str | globally unique within a corpus |
| `source_channel` | enum | one of `slack, email, pr, doc, calendar, ticket` |
| `author_id` | str | actor id emitting the signal |
| `content_text` | str | the raw text |
| `timestamp` | datetime | when it was emitted (simulated time) |
| `metadata` | dict | channel-specific extras (e.g. `commitment_ref`, `customer_ref`, `slip_signal`) |

Example:
```json
{
  "signal_id": "s42",
  "source_channel": "slack",
  "author_id": "alice",
  "content_text": "Hit a snag with the migration, might take longer.",
  "timestamp": "2026-01-09T16:45:00Z",
  "metadata": {"commitment_ref": "C-ingest"}
}
```

### `GroundTruth`
A snapshot of *simulated reality* at a timestamp. This is the oracle the evaluators compare the SUT's beliefs against. The simulator emits one at monthly cadence. It is **not** observable to the SUT.

Keys are loose dicts at this Phase 0.1 stage; Phase 0.2 extends them into strongly typed `CommitmentTruth`, `CustomerTruth`, `PatternTruth`, and `TurbulenceEvent` sub-models.

### `Corpus`
A complete simulated organization's output: metadata + signals + ground truth snapshots. Serialized as JSON Lines compressed with zstd in production; uncompressed JSON in fixtures.

### `EvalResult`
Output of any evaluator. Every layer emits a list of these. `breakdown_by` lets the evaluator stratify by month, entity kind, actor, etc.

Example:
```json
{
  "layer_id": 2,
  "metric_name": "commitment_state_accuracy",
  "value": 0.83,
  "confidence_interval": [0.79, 0.87],
  "breakdown_by": {"month": 6, "commitment_kind": "engineering"}
}
```

### `Trigger`
An event in the corpus that requires the SUT to produce a diff. The harness samples triggers for Layer 6 evaluation.

### `SUTConfig` / `SystemUnderTestSpec`
Declarative config for a system under test. The live *Protocol* interface lives in Phase 0.2.

### `AblationConfig`
Feature flags for which Company OS components to disable during a run. Named configs (registered in the harness's `AblationRegistry`) map to these flags.

### `RunManifest`
Top-level record of a single run. Persisted alongside results; makes runs reproducible and comparable.

## Phase 0.2 additions

The following types are added in Session 0.2 and not present at Phase 0.1:

- `SimulationConfig`, `ActorPersona`, `CommitmentTruth`, `CustomerTruth`, `PatternTruth`, `TurbulenceEvent`
- `DiffOp` (mirrors Company OS diff schema)
- `BeliefQuery`, `EvaluationContext`
- `Protocol` interfaces: `SystemUnderTest`, `Evaluator`, `Baseline`

## Fixtures

Three mini corpora live under `fixtures/`. Each has 10 signals, 1 month, 2 commitments, and conforms to this schema. They are hand-written to make expected metric values computable by hand.
