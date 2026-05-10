# Adversarial Coverage Gap Analysis

**Status:** Pre-implementation. Confirmation gate before scenarios are generated.
**Existing harness scope:** 52 cases across 7 stages (the prompt's "37" predates T5's reconciliation cases).
**Goal of this doc:** Map what is actually covered, what is not, and which failure modes are *structurally invisible* to single-shot scenarios — so adversarial work targets real gaps, not duplicates.

---

## 1. What the existing 52 cases actually test

The harness mechanic is: `setup(pool, ctx) → run(pool, ctx) → expected(ctx) → assertion(actual, expected, ctx)`. Each case is single-shot, isolated by `tenant_id`, runs in parallel up to a semaphore (8, or 4 for LLM cases), and returns a `(passed, diff)` tuple. There is no retry, no timeout per case, no fault injection, no time travel, and no concurrency primitive within a case.

### 1.1 Retrieval (`cases_retrieval.py`, 6 cases)

| Dim | Coverage |
|-----|----------|
| Pathway | A (actor scope), B (semantic), C (temporal window), D (pattern signature) — each in isolation |
| Fusion | RRF multi-pathway hit ranks above single-pathway (1 case) |
| Sparse trigger | Second-pass when k<5 primary hits (1 case) |
| Embedding dim | Always 768, always L2-normalized via `deterministic_vector` |
| k | Always default; never k=0, k=1, k>>40 |
| Tenant | Single tenant per case; never cross-tenant leak attempts |
| Temporal window | 7d (in-window vs out-of-window) — never 0, never multi-year |

### 1.2 Scope (`cases_scope.py`, 5 cases)

| Dim | Coverage |
|-----|----------|
| Primary entity precedence | One case mixes all 5 types in disorder |
| Tiebreak | id-asc tiebreak between two commitments (1 case) |
| Region lock hash | Permutation-stable (1 case) + tenant-isolated (1 case) |
| Touched-entity aggregation | Model scope ∪ acts ∪ resources ∪ trigger seeds (1 case) |
| Scope shape | All cases have ≤ 2 entities; never empty, never 20+, never null fields |
| Standing | scope/owner/contributor (in contest stage) — manager_chain *not* tested |

### 1.3 Contestation (`cases_contest.py`, 7 cases)

| Dim | Coverage |
|-----|----------|
| Path | belief (6 cases), reading (1 case) |
| Standing basis | scope (2), owner (1), contributor (1), no-standing rejection (1) |
| Multiplier | 0.3 (primary), 0.5 (secondary) |
| Floor | 0.15 clamp tested (2 cases) |
| Ceiling | Never tested at high confidence |
| Repeat contestation | Never (each model contested at most once) |
| Cross-path interaction | Never (no reading→belief sequence on same model) |
| `contested_false` status | Schema field exists, **never written by any test** |

### 1.4 Falsifier (`cases_falsifier.py`, 14 cases)

| Dim | Coverage |
|-----|----------|
| Kinds adequately tested | observation_pattern, prediction_deadline, commitment_outcome, explicit_contestation |
| Kinds **not tested at all** | `resource_threshold` (kind exists, zero coverage) |
| Adequacy | Vague pattern (✗), past evaluate_at (✗), unknown kind (✗), empty actors (✗), well-formed (✓) |
| Evaluation outcomes | confirmed, violated, inconclusive — all hit |
| `prediction_deadline` evaluation | Adequacy yes, evaluation **no** |
| `observation_pattern` direction | Only `violates`; `confirms` direction untested |
| `explicit_contestation` partial match | 0/N or all-N tested; not 1/2 partial |
| `within_window` parsing | 8 ISO + 8 human + 8 malformed (strong) |
| `within_window` edge cases | Negative durations, very long durations, leap-year crossings: none |

### 1.5 Cascade (`cases_cascade.py`, 6 cases)

| Dim | Coverage |
|-----|----------|
| Branch A (state_change) | unblock-sole-dep, no-unblock-other-deps, orphan-unblock invariant |
| Branch B (decision_revisited) | flag constrained_by commitments |
| Branch C (resource_terminal) | **Not tested** |
| Depth bound | max_depth=0 (forced) — never depth-49 saturation |
| Cycles | BFS dedup never exercised against an actual cycle |
| Goal health recompute | Never |
| Resource health recompute | Never |
| Cross-branch interaction | Never (e.g., unblock + decision-revisit on same commit) |
| Concurrent cascades | Never (single seed per case) |
| Invariant violations | C4 surfaced; C10 path implicit |

### 1.6 Reconciliation (`cases_reconcile.py` + `cases_reconciliation.py`, 14 cases)

| Dim | Coverage |
|-----|----------|
| Decision | auto_merge, human_review, no_match — each hit |
| Cosine boundaries | ≈1.0, 0.79, 0.72 (above/below 0.85 and 0.70) |
| Scope mismatch | Different scope_actors → no_match |
| Kind mismatch | state vs concern → no_match |
| Recency window | <30d auto, >30d no_match |
| Confidence math | max(existing, new) on auto_merge |
| Kill switch | Listed as case 14, **TODO — not actually wired up to assert decision="skipped" deterministically** |
| Audit table | reconciliation_events row creation tested; resolution flow not |
| Human_review → upgrade | Never (later high-confidence signal upgrading a flagged review) |
| Auto-merge with conflicting falsifiers | Never |

### 1.7 Calibration metadata coverage

`expected_confidence_range` and `ground_truth_correctness` are populated on contest cases and a couple of reconciliation cases — most cases have `None` for both, so the calibration ECE only consumes a subset.

---

## 2. What the existing cases do NOT test

### 2.1 Linguistic adversarial inputs (essentially zero coverage)

Every test fixture authors `content_text` as clean, declarative, single-clause English in a domain context that's deliberately easy to parse. The harness has no scenarios for:

- Sarcasm or irony where surface polarity inverts intent
- Negation, double negation, hedging, conditional commitments ("if X, we'll Y")
- Tense ambiguity (present continuous → "will" vs "currently")
- Code-switched / multilingual signal
- Typos, autocorrect failures, half-finished thoughts
- Quoted speech (whose claim is it?), reported decisions ("apparently leadership decided…")
- Aspirational vs committed phrasing ("we'd love to" vs "we will")

The Reasoning Engine consumes natural language; this is the largest single gap.

### 2.2 Boundary and degenerate inputs

| Input shape | Tested? |
|-------------|---------|
| Empty `content_text` | No |
| Single-character signal ("k", "+1") | No |
| Very long signals (50K chars) | No |
| Signals with no recognizable entity | No |
| Signals naming 20+ entities | No |
| Ambiguous entity references (two ACMEs) | No |
| Bot / system-actor signals | No |
| `occurred_at` in the future | No |
| `occurred_at` before the actor existed | No |
| Signals referencing non-existent Models | No |
| Reply / quoted threading | No |

### 2.3 Sequencing and ordering

The harness creates fixtures in one transaction and runs the production code against them. There is no scenario where signal A is processed, *then* signal B arrives and we observe what B does to the state A produced. Specifically untested:

- Out-of-order arrival (B's `occurred_at` < A's `occurred_at` but B arrives second)
- Rapid-fire updates on the same proposition
- A → not-A → A reversal
- Long supersession chains (5+ deep)
- Concurrent contradictions resolved by region lock
- Stale signal arriving after Node archival ("zombie reference")

### 2.4 Reconciliation pressure

Already-known gaps from the existing report:

- Same proposition phrased differently by different actors
- Same proposition at different scope precision (customer:acme vs deal:acme-q3)
- Near-duplicate with confidence disagreement (which confidence wins beyond `max`?)
- Long time-gap reconciliation (6 months apart)
- Partial overlap ("ACME and Beta" → split or merge?)
- Hierarchical entity reconciliation ("Engineering" vs "Sarah leads Engineering")

### 2.5 Falsifier adversarial cases

- Falsifier whose condition can never be observed (passes adequacy, never fires)
- Falsifier whose condition is tautological (always fires)
- Self-referential `observable_via` (circular dependency)
- High-confidence claim with weak falsifier (validator should reject; no test forces the path)
- Two falsifiers firing in opposite directions on same evidence
- `within_window` edge variants: empty string, very large, zero, negative

### 2.6 Cascade pressure

Cascade is the substrate's "alive" claim. Untested adversarial:

- Cascade depth bomb (Model touched by 50 others)
- Cascade cycle that actually exercises BFS dedup
- Cascade-during-cascade (region lock contention)
- Cascade against archived Model (stale reeval-queue entry)
- Cascade against Model with empty `scope_entities`
- Cascade-noop rate (does the system know when its work was wasteful?)

### 2.7 Multi-tenant isolation pressure

The current scope test (`region_lock_tenant_isolation`) verifies hash isolation. Not tested:

- Cross-tenant signal accidentally tagged with wrong `tenant_id`
- Two tenants both using `customer:acme` as their entity name (region lock collision check)
- pgvector codec leakage across tenants on shared pool
- Calibration label leakage across tenants
- Tenant deletion mid-think_run

---

## 3. Failure modes structurally invisible to single-shot scenarios

These categories cannot be tested with `setup → run → assert` against one fixture. They need different harness mechanics.

### 3.1 Concurrency / race conditions

The harness's `concurrency=8` semaphore parallelizes *across* cases (each its own tenant), not *within* a case. There is no primitive for "fire N signals against one tenant simultaneously and verify the post-state invariant."

What's invisible:
- Region-lock serialization correctness (do two parallel signals on the same entity actually serialize?)
- Region-lock ordering (lock acquisition order matters when regions overlap; deadlock surface)
- Connection pool exhaustion under contention
- pgvector codec registration race on a fresh pooled connection
- Cascade-during-cascade enqueue ordering

**Required mechanism:** a `concurrency_harness` that fires N coroutines into a single tenant, awaits all, then asserts an invariant on the resulting state.

### 3.2 Failure injection

The harness has no fault-injection mechanism. The DB is real, the LLM is real (or skipped), and ops either succeed or raise. Untested:

- DB connection drop mid-think_run (partial state? requeue? duplicate apply?)
- LLM timeout in Stage 5 (graceful degradation? backoff? retry budget?)
- LLM returns malformed JSON (validator should fail loudly with debuggable context)
- LLM returns syntactically valid but semantically nonsense `RawDiff`
- Embedding service unavailable (Pathway B should degrade, not crash the whole retrieval)
- Region lock acquisition timeout (does it fire? what's the configured timeout? what happens to the trigger?)
- Vector index corruption (Pathway B returns garbage; nothing detects it)
- Partial migration state (Node kind in code but not in schema)

**Required mechanism:** a `failure_injection_harness` with explicit fault wrappers (a `FailingLLM`, a `FailingPool`, a `FailingEmbedder`) that exercise each pipeline stage's fault path.

### 3.3 Slow-burn / accumulation corruption

These are the hardest to test and the most important for substrate integrity. Single-shot scenarios miss:

- Substrate state drift over 1000 signals into one region
- Random walk over Node lifecycle (create / contest / supersede / archive) — does the audit trail stay intact?
- Adversarial supersession chain (100 deep)
- Cascade saturation across the whole substrate
- Reconciliation drift: how many duplicate Models exist that should have been one?

**Required mechanism:** a `slow_burn_harness` that runs a long signal sequence against one tenant, periodically snapshots invariant metrics (orphan count, duplicate-by-cosine count, archived-without-reason count, broken-FK count), and reports drift over time.

### 3.4 Silent-corruption surface

Some failure modes don't raise and don't fail an assertion — they degrade quality silently. Examples:

- Cascade invariant violation logged but not raised (T1 fix made this loud; verify no other paths are still silent)
- Reconciler exception caught and demoted to `decision="skipped"` (by design, but: is the rate of `skipped` ever tracked?)
- Embedding registration fallback (string-cast path silently produces a different result)
- Validator drops ops to `dropped_ops` but caller can ignore that list

The single-shot harness can detect these only if the assertion explicitly inspects `dropped_ops`, the metric counters, or the error log — most do not.

### 3.5 Temporal anomalies

Every fixture clock is `isoplus(seconds_offset)` from `datetime.now(UTC)`. Untested:

- `occurred_at > now()` (clock skew)
- Very old `occurred_at` (>1 year)
- DST transition crossing
- Sub-second precision ordering (microsecond-level interleaving)
- Leap-second arithmetic in `within_window` evaluation

---

## 4. Proposed adversarial scope (for confirmation)

Based on the gaps above, the adversarial suite would add:

| Category | New scenarios | Mechanism |
|----------|---------------|-----------|
| 1. Linguistic adversarial | ~15 | single-shot |
| 2. Boundary / degenerate | ~15 | single-shot |
| 3. Sequencing & ordering | ~15 | **new: multi-signal sequence** |
| 4. Reconciliation pressure | ~15 | single-shot or multi-signal |
| 5. Falsifier adversarial | ~10 | single-shot |
| 6. Cascade pressure | ~10 | single-shot or multi-signal |
| 7. Concurrency / race | ~10 | **new: concurrency_harness** |
| 8. Failure injection | ~10 | **new: failure_injection_harness** |
| 9. Multi-tenant isolation | ~5 | single-shot or multi-tenant |
| 10. Slow-burn corruption | ~5 | **new: slow_burn_harness** |

**Total: ~110 scenarios**, plus three new harness submodules.

### Generation discipline (per the prompt)

- Every scenario carries a `failure_mode_under_test` field that names the specific behavior the case is trying to break (not the category).
- Where the right answer is unclear, scenarios are marked `expected_behavior: "underspecified"` with a note explaining the design question. These become architecture decisions, not test failures.
- Real-feeling natural language: typos, hedging, threading, sarcasm, multi-language, real workplace voice.
- Domains spread across sales, engineering, finance, hiring, customer support, leadership, product — not over-indexed on customer renewal.
- Production pipeline uses DeepSeek; **fixture text generation will use a different provider** (or human-authored) to avoid correlated failure modes.

### Reporting discipline

The deliverable is `TRIAGE.md`, not a green checkmark. Failures are categorized:

- **Real bug** — system did the wrong thing, file an issue
- **Underspecified behavior** — system did something, unclear if right, escalate to architecture
- **Test bug** — fixture expectation is wrong, fix the fixture

Target: **at least 15 underspecified-behavior cases documented** as architecture decisions for human review.

---

## 5. Open questions for confirmation

Before generating scenarios, please confirm or redirect on:

1. **Scope:** ~110 scenarios across the 10 categories above is the right size? More? Fewer? Drop a category entirely (e.g., skip slow-burn, defer to a separate effort)?

2. **Three new harness submodules** — concurrency, failure-injection, slow-burn — are in scope? These are bigger lifts than the single-shot scenario files; each is a few hundred lines plus its own invariant-checking primitives. Or: stay within the existing single-shot harness and skip the categories that need them?

3. **Fixture authoring:** human-authored adversarial text (slow, careful, ~5-10 minutes per case) vs LLM-generated with a different provider (faster, may have correlated artifacts)? My default is human-authored for the linguistic category (most failure-mode-sensitive) and LLM-assisted for the high-volume categories (boundary, reconciliation).

4. **Underspecified-behavior threshold:** the prompt asks for ≥15 underspecified cases. Are you OK with these landing as `expected_behavior: "underspecified"` and the case asserting only "the system did not crash" — i.e., the case passes regardless of the engine's choice, but the question goes into TRIAGE.md? Or do you want stricter assertions even when the right answer is unclear?

5. **No-softening rule:** confirmed — if a case finds a real bug, the suite ships failing and the bug becomes the deliverable. Right?

6. **Walkthrough doc:** the prompt references a "Stages 1-9 worked example" that does not exist in the repo. The state-mutation map (Section 4 of the explore agent's research, summarized in the linked report) substitutes for it. Acceptable?

---

**Awaiting confirmation before proceeding to scenario generation.**
