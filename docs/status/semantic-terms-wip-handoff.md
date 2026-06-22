# Semantic Terms Completion Note

First-class Model `semantic_terms` are implemented.

## What Landed

- `ModelCreate` and `ModelRow` expose `semantic_terms`.
- `construct_model(...)` derives precise, grounded terms from natural text,
  proposition content, falsifier, resolution criteria, and accepted suggestions.
- Legacy `proposition.semantic_terms` input is stripped from the canonical
  proposition and stored only as Model-level state.
- Terms are stored in `model_semantic_terms`, a Model-layer sidecar table with a
  GIN index. This keeps the core `models` table from growing wider while
  hydrating `ModelRow.semantic_terms` everywhere the domain model is read.
- Model insert, bulk insert, event snapshots, projection repo hydration,
  retrieval hydration, and `claim_ops.update` all preserve the field.
- Retrieval adds pathway `L`, mapped to lexical RRF dimension
  `DIMENSION_LEXICAL`.
- `primary_retrieve(...)` runs `L` when enabled. It applies trigger
  actor/entity scope when scope exists so lexical overlap stays precise and does
  not collapse adjacent customers or commitments.
- Full and compact Think prompts request top-level semantic terms and explicitly
  exclude actors, entities, UUIDs, source channels, dates, exact domain tags, and
  grammar-axis duplicates.

## Design Rule

`semantic_terms` are belief-specific lexical handles, not ontology tags,
projection labels, entity names, scope actors, or domain tags. They belong to
the Model layer because they describe the belief itself and give retrieval one
extra precise surface when embeddings or structural paths miss.

## Verification

Passed:

- Focused semantic/model/retrieval/prompt suite: `22 passed`.
- Broader repo/batch/pathway/primary/config/prompt suite: `138 passed`.
- Model event/projection/retrieval suite: `235 passed`.
- Applier semantic sidecar update test: `1 passed`.
- `ruff check` on touched implementation and test files.
- `python -m compileall services/domain/models services/domain/projections services/reasoning/retrieval services/reasoning/think lib/shared`.
- `git diff --check`.

Known unrelated failures from the full `services/domain/models/tests` package:

- `test_model_layer_stress_insert_time_topology_is_bounded_and_tenant_safe`
- `test_synthesized_situation_is_queryable_by_grammar_and_membership`
- `test_topology_emittable_edge_kinds_is_restricted`

Those failures are in topology/situation-quality behavior, not semantic-term
storage, hydration, prompting, or retrieval.
