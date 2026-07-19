# Stage 1 Company Memory

Stage 1 is the smallest production path that turns company signals into
canonical company memory. It reuses the existing ingestion, entity-resolution,
retrieval, Think validation, and atomic Model application machinery, but gives
that machinery an explicit execution profile with one responsibility:

```text
source signals
  -> immutable Observations
  -> entity extraction and bootstrap resolution
  -> relevant Models and Observations
  -> one context packet
  -> LLM Model proposals or updates
  -> validation and atomic application
  -> canonical Models
```

The composition root is
[`services/reasoning/stage1/company_memory.py`](../../services/reasoning/stage1/company_memory.py).
Callers provide a `Stage1CompanyMemoryBatch` after ingestion has persisted the
Observations and the entity-resolver path has attached its best available
grounding. The batch preserves all Observation IDs, resolved entity seeds, actor
scope, and one trigger ID, then invokes the production Think kernel as one
`T1:event_batch` run.

## Contract

Stage 1 does the following:

1. Retrieves relevant prior Models and Observations through the existing
   inquiry/retrieval engine.
2. Builds the normal prompt-facing context packet in memory.
3. Uses a claims-only LLM schema to ask what the evidence changes about the
   company.
4. Binds proposed Models to retrieved evidence without inventing additional
   operations.
5. Validates the diff and applies accepted Model insertions, reconciliations,
   and lifecycle changes atomically.
6. Records the execution profile in the existing Think application receipt as
   `stage1_company_memory`.

Unresolved entity mentions remain unresolved. Stage 1 does not manufacture an
identity merely to make reasoning proceed.

Cold-start identity is supplied through the founder-authoritative bootstrap
manifest accepted by `scripts/run_epistemic_repair_p6_think.py`. The manifest
defines names and canonical referents only; it does not seed behavioral Models.
Unknown names continue through the conservative unresolved path.

## Deliberately Out of Scope

Stage 1 does not read or update SAGE policy, learned route utilities, retrieval
motifs, reflective rules, adaptive question budgets, or company-learning
credit. It also does not create relationship candidates, graph edges, Acts,
Resources, anomalies, latent gaps, cascades, post-commit discovery work, or
residual learning records.

Those capabilities remain available to the full Think profile. They are not
deleted; they are simply outside the Stage 1 critical path and can be evaluated
later as Stage 2 learning behavior.

## Why This Is Not a Second Runtime

The execution profile is a narrow composition and authority boundary around
the existing kernel. Ingestion still owns source normalization and immutable
Observation persistence. Entity resolution still owns mention grounding.
Inquiry still owns retrieval and context-packet compilation. Think still owns
reasoning, validation, reconciliation, and atomic application.

The new code chooses which existing capabilities are authorized for this run;
it does not duplicate their implementations.

## Production Activation

The normal Think worker selects this profile by default for source-signal
`T1:event_arrival` triggers and the `T1:event_batch` wrappers produced by its
batcher. Derived `T1:state_change` work and all T2/T3/T4 triggers continue to use
the full Think profile.

`THINK_STAGE1_COMPANY_MEMORY_FOR_T1=0` is the explicit rollback switch. Policy
objects injected by evaluation runners take precedence over automatic
trigger-scoped selection.

## Focused Validation

Run the profile and composition tests without external services:

```bash
python -m pytest \
  services/reasoning/stage1/tests \
  services/reasoning/think/tests/test_execution_policy.py \
  services/reasoning/think/tests/test_context_planner.py \
  services/reasoning/think/tests/test_run_pipeline_filters.py \
  services/platform/execution/tests/test_inquiry_bootstrap.py \
  -q
```

When PostgreSQL is available, also run the Think transaction boundary:

```bash
DATABASE_URL=postgresql:///fyralis_test \
python -m pytest \
  services/reasoning/think/tests/test_reason.py \
  services/reasoning/think/tests/test_end_to_end.py \
  -q
```

A provider-free replay proves deterministic entity grounding, retrieval reuse,
context use, validation, and persistence. A separate real-provider canary
proves the configured LLM transport and actual semantic proposal path; neither
proof should be presented as proving the other.

Score either frozen proof independently after execution:

```bash
python scripts/score_stage1_company_memory.py \
  --raw /tmp/stage1-execution.json \
  --output /tmp/stage1-quality.json
```

The scorer reads no live database or provider state. It measures exact
evidence-bound claim precision and recall, canonical-scope precision and recall,
duplicate avoidance, prior-Model use, and—when present in the observed
population—synthesis and correction-in-place.
