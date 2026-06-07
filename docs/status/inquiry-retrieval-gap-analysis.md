# Inquiry Retrieval Architecture — Implementation Gap Analysis

> **Status:** Point-in-time audit · **Date:** 2026-06-07 · **Trunk:** `main`
>
> **What this is.** A component-by-component comparison between the proposed
> *"Fyralis Inquiry Retrieval Architecture"* design document and what is actually
> implemented in this repository.
>
> **Method & caveat.** This is a **static code-location audit** — symbols and
> migrations were located and spot-verified, but the pipeline was **not** run.
> "Partial" verdicts are a judgement about depth versus the design doc, not a test
> result. Line numbers are accurate as of the date above and will drift; the
> file + symbol names are the stable references.

---

## TL;DR

**The architecture is largely already implemented.** This is not a greenfield
spec — the codebase contains a deliberate, mature implementation of the
inquiry-driven retrieval loop. The bulk of the design maps onto existing code;
the genuine gaps are a small, specific set (heat-diffusion engine, Dark Matter
nodes, a true value-density compiler, the full question grammar, a dedicated
exact-lookup path, reasoning correction-retry, and richer human validation).

The design's central reframing — *retrieval as bounded, hypothesis-driven
evidence acquisition that ends in a token-efficient context packet rather than a
pile of results* — is the model the code already follows.

---

## Where it lives

| Design layer | Implementation |
|---|---|
| Inquiry loop orchestration | `services/platform/execution/inquiry.py` (~4.3k lines) |
| Retrieval paths + merge/rank + assembly | `services/reasoning/retrieval/` (`pathways.py`, `primary.py`, `scoring.py`, `assembler.py`) |
| Graph relevance, subgraph & evidence projection | `services/reasoning/sage/` (`reader.py`, `subgraph_selector.py`, `evidence_projection.py`, `structural_gates.py`, `inquiry_traces/`) |
| Reasoning → validate → apply | `services/reasoning/think/` (`reason.py`, `llm_reason.py`, `validator.py`, `applier.py`) |
| Falsification | `services/domain/models/falsifier.py` |
| Data model | migrations `0046_inquiry_execution` and the `0084`–`0092` ("SAGE") series |

See also the [Reasoning — Think Pipeline](../architecture/reasoning.md) architecture
page and the [Comprehensive reference](../reference/FYRALIS.md).

---

## Coverage matrix

Legend: ✅ Full · 🟡 Partial · ❌ Absent

| # | Design component | Status | Primary location |
|---|---|:--:|---|
| 1 | Signal Intake | ✅ | trigger/signal intake feeding `run_inquiry_retrieval` |
| 2 | Signal Understanding Pass | 🟡 | `inquiry.py` → `_generate_hypotheses`, `_initial_unknowns` |
| 3 | Evidence State (+ updater) | ✅ | `inquiry.py` → `EvidenceCard`, `_add_result_to_reservoir`, `_upsert_evidence` |
| 4 | Hypothesis Engine (incl. H0) | ✅ | `inquiry.py` → `Hypothesis`, `_generate_hypotheses` |
| 5 | Question Path Planner | 🟡 | `inquiry.py` → `InquiryQuestion`, `_select_questions` |
| 6 | Retrieval Compiler | ✅ | `inquiry.py` → `_compile_retrieval_plan` → `RetrievalAction` |
| 7 | Adaptive retrieval executors | 🟡 | `reasoning/retrieval/pathways.py` (A/B/C/D/G) |
| — | **Heat Diffusion Engine** | ❌ | *absent* — simpler propagation in `sage/reader.py` + `sage/subgraph_selector.py` |
| 8 | Evidence Reservoir | 🟡 | `inquiry.py` → `evidence_by_key` (provenance-bearing dict) |
| 9 | Evidence State Updater | ✅ | `inquiry.py` → `_upsert_evidence`, per-round merge |
| 10 | Evidence Sufficiency Gate | ✅ | `inquiry.py` → `_sufficiency_gate` (all 6 stop states) |
| 11 | Context Packet Compiler | 🟡 | `inquiry.py` → `_compile_context_packet` |
| 12 | Synthesis Context Packet | ✅ | structured packet persisted + fed to think |
| — | Omission Ledger | ✅ | `omitted_evidence` table (mig 0084) + packet section |
| 13 | Deep Reasoning Agent | ✅ | `think/reason.py`, `think/llm_reason.py` → `RawDiff` |
| 14 | Validation Layer | ✅ | `think/validator.py` → `validate` |
| 15 | Apply Layer | ✅ | `think/applier.py` → `apply_diff` |
| — | Falsification (first-class) | ✅ | `domain/models/falsifier.py` → `LEGAL_FALSIFIER_KINDS`, `is_adequate_falsifier` |
| 22 | Dark Matter Nodes | ❌ | *absent* |
| 22 | Human Validation flow | 🟡 | `human_validation_required` stop status + contested counts |

---

## Fully implemented

### Inquiry loop & evidence state
`run_inquiry_retrieval` (`services/platform/execution/inquiry.py:356`) drives the
exact loop the design describes: seed → generate hypotheses → plan questions →
compile retrieval → execute → update evidence → sufficiency check → stop, bounded
by `InquiryConfig.max_rounds`. Live state is held in `EvidenceCard`
(`inquiry.py:233`) keyed in an `evidence_by_key` map, carrying provenance
(`retrieval_paths`, `retrieved_for_questions`) and per-hypothesis links
(`supports_/weakens_/contradicts_hypotheses`).

### Hypothesis engine (with H0)
`Hypothesis` (`inquiry.py:190`) + `_generate_hypotheses` (`inquiry.py:697`)
produce competing hypotheses with `confidence` and `impact_if_true`, and **always
seed a null hypothesis (H0)** ("the signal is local noise / already captured").

### Sufficiency gate
`_sufficiency_gate` (`inquiry.py:2859`) implements all six design stop states:
`sufficient_for_reasoning`, `insufficient_continue`, `insufficient_defer`,
`human_validation_required`, `no_update_needed`, `budget_exhausted`.

### Retrieval compiler & multi-path executors
`_compile_retrieval_plan` (`inquiry.py:1430`) turns a question into
`RetrievalAction`s. The executors live in
`services/reasoning/retrieval/pathways.py`:

- `pathway_a_structural:378` — typed `model_edges` graph traversal
- `pathway_b_semantic:931` — HNSW cosine vector search over Model embeddings
- `pathway_c_temporal:1177` — windowed observation/model retrieval
- `pathway_d_pattern:1305` — signature-based pattern / prediction retrieval
- `pathway_g_model_edges:1422` — explicit typed-edge / composition expansion

These are merged and ranked by `merge_and_rank_rrf`
(`reasoning/retrieval/scoring.py:298`, Reciprocal Rank Fusion across structural /
semantic / temporal / pattern / model-edge / activation / provenance dimensions),
orchestrated by `primary_retrieve` (`reasoning/retrieval/primary.py:484`) with a
per-trigger pathway mix, then access-controlled and budget-compressed in
`assembler.py`.

### Synthesis Context Packet & omission ledger
`_compile_context_packet` (`inquiry.py:3289`) emits a structured packet (signal,
scope, hypotheses, question path, evidence tiers, candidate state changes,
sufficiency, omission ledger, budget) which is persisted and handed to the think
pipeline. Omitted evidence is recorded both in the packet and in the
`omitted_evidence` table (with a constrained `omission_reason` enum).

### Deep reasoning → validation → apply
- **Reasoning:** `llm_reason` (`think/llm_reason.py:48`) / `reason.py` produce a
  structured `RawDiff` of claim/edge/act/resource ops.
- **Validation:** `validate` (`think/validator.py:535`) enforces reference
  resolution, confidence calibration, allowed state transitions, region
  containment, acyclicity, idempotency, and **mandatory falsifiers** above a
  confidence threshold.
- **Apply:** `apply_diff` (`think/applier.py:94`) applies transactionally under
  per-region advisory locks, idempotent via the `applied_triggers` ledger, and
  emits `state_change` observations for cascade.

### Falsification (first-class)
`services/domain/models/falsifier.py` defines five legal falsifier kinds
(`LEGAL_FALSIFIER_KINDS:243`) and `is_adequate_falsifier` (`:274`); the validator
**requires** an adequate falsifier on high-confidence inferential models. This
matches the design's "write only through validation, with falsification
conditions" principle.

---

## Partially implemented

| Component | What exists | How it differs from the design |
|---|---|---|
| Signal Understanding Pass | entity/uncertainty seeding + H0 in `_generate_hypotheses` / `_initial_unknowns` | spread across helpers; no single "interpretation pass" object |
| Question Path Planner | scored, diversity-selected question batches (`_select_questions`) | **7 primitives** implemented vs the design's **14** (e.g. `GROUNDING`, `CAUSE`, `HISTORY`, `FORECAST`, `COUNTERFACTUAL`, `ACTION`, `FALSIFICATION`-as-question not first-class) |
| Retrieval executors | A/B/C/D/G paths above | **no dedicated exact-lookup path** (seed entities used as scope filters); counterevidence is a re-ranked dimension, not its own path |
| Evidence Reservoir | provenance-bearing `evidence_by_key` dict | functionally a reservoir, but an in-memory dict rather than a formal object/store |
| Context Packet Compiler | tiering + token budget + omission ledger | **greedy, count-based** selection — no explicit *value-per-token* (value-density) scoring |
| Human Validation flow | `human_validation_required` stop status, contested counts, basic review | **no** multi-actor consensus, standing/authority gating, or per-response confidence calibration; no dedicated validation-request queue |

---

## Absent (genuine gaps)

These were confirmed absent across `services/`, `lib/`, and `db/`:

1. **Heat Diffusion Engine** — the design's signature mechanism (personalized
   random walk with restart `p(t+1) = (1−c)·W·p(t) + c·r`, typed hub policies,
   relation-aware degree regularization, and **watershed partitioning into
   basins / bridge nodes**) is **not** implemented. What exists is a *single-step*
   edge-conditioned propagation in `sage/reader.py` plus hub-rollup / bridge-
   preservation in `sage/subgraph_selector.py` — the same intent (localized graph
   relevance with hub/bridge handling) realized by a much simpler, non-iterative
   mechanism. No `random_walk`/`watershed`/`basin` constructs exist.
2. **Dark Matter Nodes** — no synthetic/inferred-cause node kind for offline
   decisions that leave no digital trace; no `synthetic_dark_matter`/speculative
   provenance marker.
3. **True value-density context compiler** — see Partial above; no `usefulness /
   token` optimisation.
4. **Full question-primitive grammar** — 7 of the 14 design primitives.
5. **Dedicated exact-lookup retrieval path.**
6. **Reasoning correction-retry** — only dead-letter exists (5-attempt cap +
   backoff); the design's "one correction attempt, then re-validate" is **not**
   wired (noted as deferred in code).
7. **Richer human validation** — multi-actor confirmation, standing/authority
   gating, per-actor confidence calibration.

---

## Data model mapping

| Design table | Status | Actual table / location | Migration |
|---|:--:|---|---|
| `inquiry_sessions` | ✅ | `inquiry_sessions` | `0046_inquiry_execution` |
| `retrieval_plans` | ✅ | `retrieval_plans` | `0084_sage_inquiry_trace_gap_fillers` |
| `omitted_evidence` | ✅ | `omitted_evidence` | `0084_sage_inquiry_trace_gap_fillers` |
| `inquiry_questions` | 🟡 | `inquiry_question_runs` (renamed equivalent) | `0046` |
| `retrieved_evidence` | 🟡 | `inquiry_evidence_items` (renamed equivalent) | `0046` |
| `context_packets` | 🟡 | JSONB column on `inquiry_sessions` (no dedicated table) | `0046` |
| `rejected_hypotheses` | 🟡 | subset of `negative_memory` (`memory_type='rejected_hypothesis'`) | `0087_sage_discovery_and_negative_memory` |
| `hypotheses` | ❌ | none — stored inline as JSONB in the session / sufficient-state | — |

**Core substrate the design assumes already exists** (and does): `models` (Nodes /
atomic claims), `model_edges` (relationships / composite claims), `audit_events`
(per-model state chain), and the prediction tables (`model_predictions`,
`0089_sage_model_predictions`).

---

## Recommended next steps (if full design parity is the goal)

In rough priority order:

1. **Heat-diffusion retrieval executor** — the largest behavioural gap and the
   design's distinctive contribution. Could extend the existing
   `sage/subgraph_selector.py` + `reader.py` propagation into an iterative
   random-walk-with-restart with watershed partitioning, exposed as a new
   retrieval path invoked by a "local operational circuit" question primitive.
2. **Value-density packet compiler** — replace greedy count-based tiering in
   `_compile_context_packet` with explicit `marginal_usefulness / token_cost`
   scoring and the design's budget split.
3. **Expand the question grammar** to the full 14 primitives, plus a dedicated
   exact-lookup retrieval path.
4. **Reasoning correction-retry** — one validation-feedback correction attempt
   before dead-lettering.
5. **Dark Matter / offline-cause** detection + the richer human-validation flow
   (multi-actor, standing, calibrated confidence) — the most net-new work.

---

## How this audit was produced

Distinctive design terms were grepped across `services/`, `lib/`, and `db/`;
parallel exploration mapped each design component to code; and the load-bearing
files, symbols, and migrations were spot-verified by direct lookup. The source
design document is the *"Fyralis Inquiry Retrieval Architecture"* spec provided
on 2026-06-07. Re-run the verification before treating any single file:line
citation as current.
