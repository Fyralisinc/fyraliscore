# Model, Projection, And Retrieval Rewrite

## Shape

The Model layer is the belief kernel. It forms, updates, archives, and emits
neutral `model_events`. It does not know whether a downstream view is a
constraint, resource, customer view, act view, forecast, or any later subgroup.

The Projection layer consumes `model_events` and writes rebuildable
`projection_snapshots`. A snapshot is a typed operating view over canonical
Models:

- `projection_name`: stable projection family, such as `constraints`.
- `projection_version`: projection schema version.
- `subject_key`: stable subject inside that projection, such as
  `company:runway`.
- `payload`: projection-owned JSON.
- `source_model_ids`: backing beliefs.
- `source_event_ids`: backing belief-change events.

Retrieval is projection-first, model-backed. It infers relevant projection
subjects from the trigger seed, loads compact snapshots, and follows
`source_model_ids` back to canonical Models for reasoning context. If no
projection snapshot exists, existing retrieval pathways remain the fallback.

## Data Flow

1. `ModelsRepo` writes or archives a Model.
2. The Model layer emits a neutral `model_events` row in the same transaction.
3. Post-commit enqueues `materialize_projections`.
4. `ProjectionRunner` asks each registered projector for pending events.
5. Matching events are mapped to affected `subject_key` values.
6. Each subject is rebuilt into `projection_snapshots`.
7. Retrieval builds a `ProjectionSubjectSeed` from the trigger.
8. Subject resolvers produce `(projection_name, subject_key)` candidates.
9. `ProjectionRepo` loads snapshots, staleness, and source Models.

## Extension Points

Projectors can be contributed without editing core runtime callers:

- In-repo: `register_projector_factory("name", factory)`.
- Installed package entry point: `company_os.projections`.

Subject resolvers can be contributed without editing retrieval:

- In-repo: `register_subject_resolver("name", fn)`.
- Installed package entry point: `company_os.projection_subject_resolvers`.

Projector entry-point names are projection names. Resolver entry-point names are
resolver names. Discovery is cached and failure-isolated: broken extension
contributions are logged and skipped.

## Core Projections

The current core projection families are:

- `constraints`: runway, financial capacity, operating capacity, entity-scoped
  constraints.
- `resources`: financial, capacity, relational, infrastructure, regulatory, IP,
  and entity-scoped resources.

More projections should be added by implementing a projector plus, when needed,
a subject resolver. The Model layer should not receive new projection-specific
branches for them.

## Contracts

- Model events are projection-neutral.
- Projection snapshots are disposable and rebuildable.
- Retrieval never hydrates projection source Models through private retrieval
  SQL; it uses `ProjectionRepo`.
- Projection freshness is checked in batches through `ProjectionRepo`.
- Post-commit materialization defaults to `["all"]`, which means every known
  core, registered, and discovered projector.
- Projection runner failures are cursor-safe: a failed projector event is not
  checkpointed, later events for that projector are not skipped, and other
  projectors continue running.
