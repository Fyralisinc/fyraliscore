# Adaptive Question Planner Profiles TODO

Date: 2026-06-30

This is the implementation TODO for making the adaptive question planner behave
differently across Think processes. The goal is to preserve the existing inquiry
runtime while making question planning lane-aware: T1 stays cheap and
triage-oriented, while T4 and other background pattern-finding lanes can use a
deeper investigative interviewer policy.

## North Star

The question planner should behave like a good interviewer:

```text
listen to the signal
  -> infer what it may be pointing toward
  -> ask the highest-information question
  -> retrieve evidence
  -> update the unknowns
  -> ask a sharper follow-up
  -> stop when the core pattern is clear or the missing knowledge is explicit
```

Different Think lanes need different interviewer styles:

- T1: fast triage interviewer. Attach the signal to existing memory, classify
  no-op/update, and avoid extra cost.
- T2/T3: verification interviewer. Confirm state changes, contradictions,
  missing transition evidence, and authority-sensitive facts.
- T4/background pattern discovery: investigative interviewer. Follow recurrence,
  ownership, counterevidence, constraints, and downstream impact across several
  rounds.

## Current System Anchors

- Think context planning lives in
  `services/reasoning/think/context_planner.py`.
- The active retrieval path is `retrieve_for_execution(...)` and
  `run_inquiry_retrieval(...)` in `services/platform/execution/inquiry.py`.
- Inquiry configuration lives in
  `services/platform/execution/config.py`.
- Question planning lives in
  `services/platform/execution/question_planning.py`.
- Round execution and question selection live in
  `services/platform/execution/inquiry_rounds.py`.
- Bootstrap max-round behavior lives in
  `services/platform/execution/inquiry_bootstrap.py`.

Important current limitation:

- `candidate_questions_for_round(...)` currently hard-falls back to
  deterministic planning for non-T1 triggers. Giving T4 more rounds without
  changing that gate would only create deeper deterministic questioning, not the
  LLM-backed interviewer behavior this plan wants.

## Non-Goals

- Do not rewrite the retrieval engine.
- Do not replace the adaptive inquiry loop.
- Do not enable LLM question planning globally for every trigger kind.
- Do not make T1 more expensive by default.
- Do not hide cost changes; planner mode and profile must be visible in notes.

## Phase 1: Add Profile Fields To InquiryConfig

Primary file:

- `services/platform/execution/config.py`

Work:

- [ ] Add a compact planner profile field, for example
  `planner_profile: str = "triage"`.
- [ ] Add an explicit allowlist for LLM question planning by trigger kind, for
  example `llm_question_planning_trigger_kinds: tuple[str, ...] = ("T1",)`.
- [ ] Add a profile-controlled primitive weighting surface if needed, for
  example `question_primitive_weights: Mapping[str, float] | None = None`.
- [ ] Keep existing env behavior stable for generic inquiry callers.
- [ ] Add bounded env parsing only if operators need runtime overrides.

Exit criteria:

- Existing default behavior still allows T1 LLM planning.
- Non-T1 LLM planning remains disabled unless a caller config enables it.
- Config remains serializable and easy to inspect in inquiry notes.

## Phase 2: Resolve Think Inquiry Profiles By Trigger

Primary file:

- `services/reasoning/think/context_planner.py`

Work:

- [ ] Replace `_think_inquiry_config()` with
  `_think_inquiry_config_for_trigger(trigger)`.
- [ ] Preserve `THINK_INQUIRY_CONTEXT_PACKET_EVIDENCE_MODE` override behavior.
- [ ] Pass the trigger-aware config into `retrieve_for_execution(...)`.
- [ ] Keep `_plan_mode_for_trigger(...)` focused on fast/deep route selection.
  The new profile resolver owns lane-specific depth and planner policy.

Initial profile defaults:

```text
T1 triage
- planner_profile = "triage"
- max_rounds = 1
- questions_per_round = 2
- llm_question_planning_trigger_kinds = ("T1",)
- utility_governor_enabled = true
- context_packet_evidence_mode = "models_only"

T2/T3 verification
- planner_profile = "verification"
- max_rounds = 2
- questions_per_round = 3
- llm_question_planning_trigger_kinds = ()
- utility_governor_enabled = true
- context_packet_evidence_mode = "models_only"

T4 investigative pattern
- planner_profile = "investigative_pattern"
- max_rounds = 4
- questions_per_round = 3
- llm_question_planning_trigger_kinds = ("T4",)
- utility_governor_enabled = false or a lower skip threshold
- context_packet_evidence_mode = "model_first"
```

Exit criteria:

- T1 remains cost-controlled.
- T4 can request a deeper LLM-backed inquiry profile without changing product
  query behavior.
- The profile chosen for a trigger is visible in context or inquiry notes.

## Phase 3: Replace The T1-Only Planner Gate

Primary file:

- `services/platform/execution/question_planning.py`

Work:

- [ ] Remove the hardcoded non-T1 fallback:

```python
if trigger.kind != "T1":
    ...
```

- [ ] Replace it with a config-driven check:

```python
if trigger.kind not in config.llm_question_planning_trigger_kinds:
    return deterministic, {
        "mode": "deterministic_fallback",
        "reason": "llm_planning_disabled_for_trigger_kind",
        ...
    }
```

- [ ] Keep existing fallback behavior for missing provider, disabled config,
  utility governor skip, planner errors, and empty LLM results.
- [ ] Preserve deterministic safety questions even when the LLM planner runs.

Exit criteria:

- T4 can run LLM planning when its Think profile enables it.
- T2/T3 remain deterministic until explicitly enabled.
- Existing metrics still distinguish `llm`, `llm_delta`, and
  `deterministic_fallback`.

## Phase 4: Make The Planner Prompt Profile-Aware

Primary file:

- `services/platform/execution/question_planning.py`

Work:

- [ ] Add a compact helper such as `_planner_profile_instruction(config)`.
- [ ] Inject that profile instruction into both compact and full question
  planning prompts.
- [ ] Keep the instruction short to avoid planner-token creep.

Initial profile instructions:

```text
triage
- Ask only the minimum questions needed to attach the signal to existing memory,
  identify a no-op, or decide that one model update is needed.

verification
- Ask questions that confirm whether an apparent state transition, contradiction,
  or missing piece of evidence is real.

investigative_pattern
- Act like an investigative interviewer. Infer the latent pattern the signal may
  be pointing toward. Ask follow-up questions that separate recurrence,
  ownership, counterevidence, constraints, and downstream impact.
```

Exit criteria:

- T4 planner notes show LLM questions shaped around recurrence, ownership,
  counterevidence, constraints, or impact.
- T1 planner questions remain narrow and cheap.

## Phase 5: Bias Question Selection By Profile

Primary files:

- `services/platform/execution/inquiry_rounds.py`
- `services/platform/execution/question_policy.py`

Work:

- [ ] Decide whether primitive weighting belongs in `question_policy.py` or just
  before `select_questions(...)` in `_select_questions_for_round(...)`.
- [ ] Add profile primitive weights only after the config gate is working.
- [ ] Prefer deterministic score adjustment over prompt-only steering.

Initial primitive bias:

```text
T1 triage boosts
- COMMITMENT
- DEPENDENCY
- OWNERSHIP

T4 investigative boosts
- RECURRENCE
- COUNTEREVIDENCE
- OWNERSHIP
- GOAL_IMPACT
- CONSTRAINT
```

Exit criteria:

- The selected questions reflect the lane even when deterministic safety
  questions dominate the candidate set.
- Question selection remains auditable through existing planning notes.

## Phase 6: Keep Stop Criteria Cost-Aware

Primary files:

- `services/platform/execution/inquiry_bootstrap.py`
- `services/platform/execution/answer_evaluation.py`

Work:

- [ ] Let the Think profile set `max_rounds`, but preserve existing fast-path,
  human-validation, no-op, and weak-signal caps.
- [ ] Do not change sufficiency rules in the first patch unless tests prove T4
  stops too early.
- [ ] If needed later, add profile-specific sufficiency rules for investigative
  lanes, such as requiring recurrence evidence plus counterevidence check before
  stopping.

Exit criteria:

- T4 can run deeper when materially useful.
- Weak/noisy signals still stop early.
- Budget exhaustion remains explicit rather than silent.

## Phase 7: Observability And Benchmark Notes

Primary files:

- `services/platform/execution/inquiry_finalization.py`
- `services/platform/execution/runtime_metrics.py`
- `scripts/run_storyline_batch_benchmark.py` if benchmark summaries need updates.

Work:

- [ ] Record `planner_profile` in inquiry notes.
- [ ] Record whether LLM planning was allowed for the trigger kind.
- [ ] Record configured and effective max rounds.
- [ ] Preserve fallback reasons, especially:
  `llm_planning_disabled_for_trigger_kind`, `llm_provider_missing`,
  `disabled_by_config`, `execution_utility_governor`, and planner exceptions.
- [ ] Ensure benchmark summaries can count T4 LLM planning separately from T1.

Exit criteria:

- A run can answer:
  "Did T4 actually use the interviewer planner, or did it fall back?"
- Cost and quality interpretation cannot confuse deterministic T4 with LLM T4.

## Phase 8: Tests

Primary test files:

- `services/platform/execution/tests/test_question_planning.py`
- `services/reasoning/think/tests/test_context_planner.py`
- `tests/unit/test_storyline_batch_benchmark.py` if planner-mode aggregation
  changes.

Unit tests:

- [ ] T1 default config still allows LLM question planning.
- [ ] T4 without profile allowlist returns deterministic fallback.
- [ ] T4 with `llm_question_planning_trigger_kinds=("T4",)` uses the LLM planner
  path.
- [ ] T2/T3 remain deterministic by default.
- [ ] Fallback note reason is config-based rather than the old
  `non_t1_trigger_uses_seeded_retrieval`.
- [ ] `_think_inquiry_config_for_trigger(T1)` returns the triage profile.
- [ ] `_think_inquiry_config_for_trigger(T4)` returns the investigative profile.
- [ ] Env override for Think evidence mode still wins when set.

Suggested validation commands:

```bash
.venv/bin/python -m pytest services/platform/execution/tests/test_question_planning.py -v --tb=short
.venv/bin/python -m pytest services/reasoning/think/tests/test_context_planner.py -v --tb=short
ruff check --select E9,F63,F7,F82,F821,F811,F401 services/platform/execution services/reasoning/think
```

Optional behavior proof:

```text
Run a small T4-heavy benchmark and verify:
- llm_planning_events > 0 for T4
- question_planning_modes includes llm for T4
- T1 cost does not materially increase
- T4 packets show better recurrence, counterevidence, ownership, and impact coverage
```

## First Patch Scope

The first PR should be intentionally narrow:

- [ ] Add `planner_profile` and `llm_question_planning_trigger_kinds`.
- [ ] Add `_think_inquiry_config_for_trigger(trigger)`.
- [ ] Replace the hardcoded T1-only planner gate with config-driven gating.
- [ ] Add compact profile instructions to the planner prompt.
- [ ] Add focused unit tests for T1 and T4 behavior.
- [ ] Update docs only where current architecture references the old T1-only
  planner behavior.

Do not include primitive weighting, sufficiency changes, or benchmark harness
changes in the first PR unless the unit tests require small notes plumbing.

## Open Questions

- Should T4 disable the utility governor entirely, or use a lower skip
  threshold so obvious low-value background triggers still avoid LLM planning?
- Should T2/T3 get LLM planning for selected subkinds, or remain deterministic
  until T4 is proven?
- Should profile definitions live only in `context_planner.py`, or should they
  move to a small `services/reasoning/think/inquiry_profiles.py` module once
  there are more than three profiles?
- Should product/Ask inquiry ever use these profiles, or should this remain
  Think-only?
