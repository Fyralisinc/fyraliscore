# Latest System Component Map

**Status:** current cleanup baseline, not a production-readiness claim

**Date:** 2026-07-19
**Machine-readable source:** `architecture/registry.yaml`

## Verdict

The latest Fyralis system is not “the Think pipeline” and it is not one graph.
It is a set of separately owned semantic planes connected by typed contracts:

```text
evidence and source reality
  -> perception and grounding
  -> physical or institutional state
  -> revisable beliefs
  -> governed attention and temporary inquiry
  -> proposals and bounded agency
  -> independently observed outcomes
  -> settlement and governed control learning
  -> rebuildable projections and products
```

Runtime scheduling, authority, audit, and independent evaluation constrain that
flow but do not acquire its semantic ownership. The current repository only
partially reflects those boundaries. It is a hybrid of the latest system,
compatibility surfaces from earlier architectures, and evaluation harnesses.

The cleanup must therefore separate ownership before moving or deleting code.
An old name is not sufficient evidence that code is obsolete. A deletion is
safe only after imports, runtime manifests, migrations, readers, writers, tests,
replay windows, and rollback needs show that its responsibility has another
owner or no remaining consumer.

## Sources And Precedence

This map reconciles four evidence classes:

1. The user-supplied July 15 architecture candidate in Downloads, SHA-256
   `b55a0616ae361d772ea51e9c91017d1b3e0c61d81558fb05fd6da0014ff1a970`,
   supplies the agreed Physics–Brain–Intent target and component program.
2. The repository's July 16 normative projection, SHA-256
   `a3e75c96080101cc547b5ea144c51a57988b6ff5f054aa32b57d1d234daecf4a`,
   adds later constitutional and implementation amendments. Where it is more
   explicit without contradicting the core target, it is the later decision.
3. The July 19 autonomous-learning handoff and learning logs supply the latest
   observed implementation and failure evidence.
4. Migrations, live code, route/process wiring, and tests determine what exists
   now. They do not, by themselves, redefine the target architecture.

Conflicts are recorded as gaps. They are not silently resolved by choosing the
most convenient implementation.

## Classification Used During Cleanup

Every production path must receive one current classification:

| Class | Meaning | Cleanup rule |
| --- | --- | --- |
| Canonical | Owns one semantic class or its sole mutation boundary | Keep; isolate its public ports and writer |
| Compatibility | Preserves an old schema, API, event, or replay window | Keep only with named consumer and removal condition |
| Derived | Rebuildable projection, index, graph, rendering, or product view | Keep separate from canonical truth and make rebuildability testable |
| Temporary | Inquiry/context workspace that must not become a second graph | Keep bounded; persist only required trace and terminal summary |
| Runtime control | Transactions, outbox, work, leases, budgets, repair, quiescence | Keep semantically neutral and writer-fenced |
| Evaluation-only | Gold, simulator, scorer, benchmark, or experiment harness | Keep outside production imports and canonical writes |
| Retirement candidate | No required runtime, migration, replay, test, or rollback owner | Delete only after a written liveness proof |
| Planned | Target responsibility without a complete current implementation | Do not create fake package ownership or claim readiness |

## Component Boundaries

The checked registry contains the detailed paths, writers, contract references,
dependencies, forbidden responsibilities, and next component gate. This table
is the human-readable summary.

| ID | Latest-system responsibility | Current primary paths | Mixed legacy hotspots | Current state |
| --- | --- | --- | --- | --- |
| C0 | Semantic and transport kernel | `lib/contracts`, `lib/architecture_registry.py`, `architecture/registry.yaml` | None; domain behavior is forbidden here | Partial contract kernel |
| P1 | Evidence, conversational perception, entity grounding, physical state, independently observed outcomes | ingestion, observations, conversation context, entity grounding, canonical referents, source semantics, resolver workers | aliases, integrations, `domain/outcomes` | Partial and mixed |
| P2 | Constituted intent, grants, authorization, workflows, tasks, fenced effects | `domain/intent`, agency activation | acts, resources, obligations, execution | Partial and mixed |
| P3 | Revisable beliefs, epistemic admission, relational truth, representation admission | models, truth kernel, Think, synthesis, relationships, edge intelligence | Bridge and SAGE | Partial and mixed |
| P4 | Authorized context compilation and temporary inquiry | retrieval and platform execution | SAGE | Partial and mixed |
| P5 | Concerns, criteria impact, governed attention, work candidates | concerns and anomaly processing | SAGE | Partial and mixed |
| P6 | Proposals, predictions, intervention episodes, settlement, residuals, attribution | intervention runtime, episode coordinator, oracle | intent, outcomes, execution, recommendations | Partial and mixed |
| P7 | Governed control learning, experiment assignment, promotion, rollback | company learning and calibration | SAGE policy/feedback paths | Partial and mixed |
| P8 | Unified graph, projections, retrieval indexes, Ask, rendering, delivery candidate selection | projections, product, topology and topology workers | Bridge and Models | Partial and mixed |
| P9 | Transaction/outbox primitives, work, leases, repair, budgets, process manifest, quiescence | platform runtime, work scheduling, scheduler/maintenance workers | execution and platform execution | Partial and mixed |
| P10 | Authority decisions, revocation, audit, neutral fate/trace telemetry | platform access control | gateway checks, execution records, observability | Partial and mixed |
| E0 | Independent fixtures, gold, simulation, scoring, experiments, reports | `services/evaluation`, `lib/evaluation`, benchmarks, evaluation tests | synthetic ingest, epistemic-repair and real-LLM suites | Physically separated, proof incomplete |

### P1 Subcomponents

P1 is too large to treat as one component test target. Its required internal
sequence is:

| ID | Exclusive responsibility | First proof required |
| --- | --- | --- |
| P1A | Source fidelity, raw archive, normalization, revisions, tombstones | Golden source replay and one terminal fate per input |
| P1A2 | Conversation topology and interpretation context | Cutoff, edit/delete, cross-boundary, sufficiency, and contamination fixtures |
| P1B | Source assertions, semantic frames, speech acts | Attribution, negation, modality, time, and uncertainty cases |
| P1C | Mentions, type assessments, local roles | Explicit, implicit, nested, deictic, and negative mentions |
| P1D | Candidate generation | Open-set recall, tenant isolation, immutable candidate-set or terminal fate |
| P1E | Canonical referent lifecycle | Create, bind, merge, split, supersede, and correction history |
| P1F | Resolution assessment and consumer admission | Calibration plus purpose-specific accept/review/unresolved outcomes |
| P1G | Destination-plane admission, physical state, observed outcomes | Correct-plane routing and no interpretation-to-intent bypass |
| P1H | Correction and propagation | Dependency blast radius, repair receipt, convergence, and residue |
| P1I | Physics quality and telemetry | Complete denominators, cost, latency, calibration, and fate accounting |

No live P1 end-to-end claim is valid while an earlier row is red or bypassed.

## Current Architectural Seams To Split

These are not deletion decisions. They are the places where multiple latest-
system responsibilities still share one package or older abstraction.

1. `services/domain/outcomes` currently spans observed reality, authorization,
   prediction, settlement, and attribution. Independent observed outcomes must
   remain P1-owned while prediction/settlement/attribution belong to P6 and
   authorization belongs to P2.
2. `services/domain/execution` spans agency state, effects, work, leases, and
   repair-adjacent behavior. P2 semantic agency and P9 runtime control need
   public ports and separate writer ownership even if they initially share a
   physical package.
3. `services/domain/intent` contains both constituted intent and the cross-plane
   proposal boundary. ProposalAppender is P6's neutral ingress; IntentApplier is
   P2's constitutive writer.
4. `services/reasoning/sage` mixes belief support, inquiry, concerns, control
   learning, and derived projections. Its files must be assigned by
   responsibility before SAGE can be retained, split, or retired.
5. `services/domain/models`, `model_edges`, truth-kernel relations, topology,
   and Bridge still carry concepts from several graph/model architectures.
   Plane-owned canonical assertions must be distinguished from rebuildable
   graph projections and optional representation candidates.
6. `services/platform/execution` contains inquiry orchestration, retrieval
   learning, runtime metrics, and synthesis dossier code. It is not one
   component merely because the files share an orchestration package.
7. Gateway and product code contain authority and product composition logic.
   P10 decides access, P8 renders and selects delivery candidates, and neither
   may become a canonical semantic writer.
8. Evaluation code is physically named, but production-path benchmark hooks
   and evaluator-derived defaults must still be proven absent.

## Component-First Proof Ladder

Expensive end-to-end execution is the final diagnostic, not the first debugger.
Every component follows this order:

| Gate | Evidence | Cost | What it may authorize |
| --- | --- | --- | --- |
| L0 Contract | Schema identity, versions, writer, allowed/forbidden inputs, compatibility, failure and idempotency laws | Static | Component implementation work |
| L1 Pure component | Reducers, compilers, parsers, state machines, adversarial negative cases | Cheap | Database component tests |
| L2 Durable component | Real PostgreSQL transition, transaction/outbox atomicity, retries, replay, tenant and authority checks | Moderate | Adjacent-port integration |
| L3 Adjacent integration | One producer/consumer contract at a time with frozen fixtures and explicit fates | Moderate | Bounded vertical slice |
| L4 Vertical slice | One signal, correction, inquiry, or intervention across only required components | Higher | Preregistered E2E readiness audit |
| L5 End to end | Frozen population, provider/policy receipts, manifests, independent scorer, complete terminal accounting | Expensive | Only the exact claim preregistered |

Rules:

- A red or missing lower gate blocks the higher gate.
- A characterization test proves the old behavior is inventoried; it does not
  prove the behavior is correct.
- Provider schema-invalid and non-JSON results are test cases and experiment
  evidence, not exceptions to hide.
- Every concurrency test accounts for all terminal outcomes.
- Evaluation gold and production decisions remain separate at every gate.
- A successful narrow slice never implies complete architecture conformance.

## Immediate Example: Why TI3 Was Too Early

The July 19 handoff records a frozen/provider schema mismatch:

```text
frozen:      SynthesisSemanticDecision
             think-synthesis-semantic-decision-v1

implemented: SynthesisProviderDecision
             think-synthesis-provider-decision-v2
```

It also records missing durability for schema-invalid and arbitrary non-JSON
provider results. These are L0/L1/L2 component failures. The 21-call TI3 lane
was therefore being asked to discover contract and durability defects. Under
the new ladder, exact schema conformance, hidden-field attacks, non-JSON
retention, call accounting, and compiler binding are cheap mandatory blockers
before a new provider run. CF3-C remains outside this cleanup and locked.

## Definition Of A Clean Component

A component is clean only when all of the following are true:

1. Its purpose, plane, inputs, outputs, writer, authority and durability are
   explicit.
2. Every current source file is owned, shared as named debt, compatibility-only,
   derived, evaluation-only, or a retirement candidate.
3. It has no hidden upward import or private cross-plane database coupling.
4. Its schema and policy versions have one source of truth.
5. Its negative, failure, idempotency, concurrency, authority, tenant,
   correction, and observability behaviors have objective tests.
6. Its runtime process, if any, appears in the process manifest and deployment
   wiring with matching configuration.
7. Its tables have known writers, readers, replay requirements, and deletion
   constraints.
8. Its compatibility paths have consumers and a measurable retirement gate.
9. Its test command is cheap enough to run before integration.
10. Its component gate is green before it enters a vertical or end-to-end run.

## What This Map Does Not Claim

- It does not declare the architecture freeze complete.
- It does not claim that every registry writer is implemented.
- It does not prove that all files under a broad package already belong there.
- It does not authorize moving migrations or deleting compatibility readers.
- It does not authorize TI3, CF3-C, real-provider, or full-company runs.

It creates the boundary needed to do those audits without rediscovering the
system architecture in every debugging session.
