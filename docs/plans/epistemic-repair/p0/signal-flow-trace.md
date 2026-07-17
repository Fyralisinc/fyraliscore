# P0 Existing Signal-to-Learning Trace

**Baseline commit before P0:** `841f6f93e4de`

**Scope:** starts after a normalized Observation is persisted. Connector
transport is intentionally excluded.

This is a current-state trace, not the target contract. It identifies the
existing owners that later phases must tighten or reuse.

## Flow

```text
persisted Observation
  -> idempotent T1 event-arrival trigger
  -> optional T1 event-batch scheduling
  -> source/context and entity-grounding state
  -> primary retrieval and context assembly
  -> Think provider or deterministic reasoning
  -> post-reason enrichment and mutation compilation
  -> validation and context-use classification
  -> apply transaction and applied-trigger receipt
  -> durable post-commit action queue
  -> projection/topology/question/background workers
  -> retrieval/outcome feedback and SAGE-derived policy
  -> Company Vitals joins runtime state and evidence
```

## Current owners and receipts

| Stage | Current owner | Input | Durable/observable output | P0 finding |
| --- | --- | --- | --- | --- |
| Trigger guarantee | `services/domain/triggers.py::ensure_event_arrival_trigger` | tenant and persisted Observation ID | idempotent `think_trigger_queue` T1 row | Producer-side mechanism exists; duplicate ingest should not skip metabolism. |
| Batch scheduling | `services/reasoning/think/worker.py` | pending T1 rows | leased `T1:event_batch` processing context with Observation/member-trigger IDs | Batch membership is operational provenance, not a semantic episode contract. |
| Conversational context | `services/domain/conversation_context/*` and `services/workers/entity_resolver/context.py` | Observation/source topology and candidate state | candidate/probe/selection/snapshot records | Reusable context machinery exists; full mixed-stream sufficiency remains unproven. |
| Entity grounding | `services/domain/entity_grounding/*` and `services/workers/entity_resolver/*` | signal/context/mention/candidate state | detections, fates, assessments, admission/review/correction records | Bounded paths exist; authority and denominator completeness remain P2/P3 gates. |
| Retrieval | `services/reasoning/retrieval/primary.py` and `assembler.py` | `TriggerContext`, access context, seed objects | `RetrievalResult` plus bounded LLM-facing bundle and selection notes | Current observations, historical observations, and Models need stricter class-separated telemetry. |
| Reasoning | `services/reasoning/think/run_pipeline.py::prepare_reasoning_run_state` and provider path | trigger, retrieval result, assembled bundle | raw typed diff, reasoning trace, stage timings, LLM usage | Production post-reason injectors and fixture-like hooks are part of the P0-B blindness audit. |
| Mutation compilation | `services/reasoning/think/mutation_compiler.py` through `run_pipeline.py` | raw diff, retrieval result, bundle | compiled mutation proposals and summary | Intended consolidation seam for claim-local evidence and relation admission. |
| Validation | `services/reasoning/think/validator.py` through `validate_raw_reasoning_output` | compiled raw diff and allowed region | `ValidatedDiff`, dropped-operation errors, validation debug capture | Existing validator is reusable, but P2 must prove all canonical invariants across every writer. |
| Context-use classification | `services/reasoning/think/context_use.py` | selected context and raw/validated diff | selected/referenced counts and context-use grade in `think_runs.ops_applied` | Useful existing instrumentation; it is not yet exact decision/evidence/outcome credit. |
| Apply | `services/reasoning/think/reason.py` and `applier.py` | validated diff | canonical mutations, state changes, apply debug capture, applied-trigger identity | Existing central seam is a strong reuse target; current parallel writers still require census. |
| Immediate feedback | `services/reasoning/edge_intelligence/context_feedback.py` from `_record_apply_observability` | validated context-use report and trigger primitive | context-use pair feedback | Associative feedback exists; decision-level delayed/causal attribution remains unproven. |
| Post-commit durability | `services/reasoning/think/post_commit.py` | trigger, validated diff, exact applied IDs/summary | deduplicated `pending_post_commit_actions` rows | Durable at-least-once mechanism exists with retry/dead-letter behavior. |
| Derived/adaptive learning | `services/reasoning/sage/outcome_evaluator.py` and SAGE repositories | inquiry/retrieval/writer outcomes | outcome events, reward features, route/shortcut/policy sidecars | Design says proposal/control only; P2/P4 must prove it cannot mutate canonical truth and does not fan out batch credit. |
| System evaluation | `scripts/company_vitals.py` and `lib/evaluation/*` | database snapshot, run summary, bound artifacts | component metrics, incidents, proof boundaries, verdict | Existing report owner should be consolidated, not duplicated; coherent-run identity remains a hard gate. |

## Current causal gaps

1. The current trace does not prove that entity grounding processed every
   eligible mention before Think admission.
2. A T1 batch preserves operational provenance but does not discover the true
   episode boundary.
3. Selected context and referenced context are observable, but exact
   evidence-to-mutation-to-later-outcome attribution is incomplete.
4. End-of-run queue drain does not prove that batch N truth was available to
   batch N+1.
5. Multiple canonical writer paths and relation truth surfaces remain under
   inventory; the trace must not imply a single writer until P0-A closes.
6. Company Vitals can join bounded evidence portfolios, but those portfolios
   are not one coherent integrated execution.

## Reuse decision

Keep the queue, retrieval, Think validation/apply, post-commit durability,
context-use telemetry, SAGE policy, and Company Vitals mechanisms. Tighten the
authority, evidence, lifecycle, boundary, attribution, and coherent-run
contracts around them. Do not create another scheduler, resolver, policy
learner, graph, or report owner.
