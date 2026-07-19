# Autonomous Company Learning — TI3 Recovery Continuation Handoff

**Purpose:** Durable continuation record for the next implementation session

**Date:** 2026-07-19

**Required worktree:**
`/Users/rachinkalakheti/fyraliscore-autonomous-learning`

**Branch:** `codex/autonomous-company-learning`

**State captured from:**
`e5776b0e08bc01ad268e7ddce99836e30189acb3`

**Session origin:**
`c52978481661379648f051124d3378324224d34f`
(`docs(company-learning): add new-thread execution handoff`)

**Working tree at capture:** clean

**Current frozen contract:** Think Intelligence Gate v3

**Contract SHA-256:**
`8f90d9ecc723d61253f3e678fece1122967d280dcf8d8c23f1997d04d36c7a8f`

## 1. Read This First

This file continues
[the original new-thread handoff](autonomous-company-learning-new-thread-handoff.md).
It records what the session actually implemented and learned, including the
incomplete TI3 provider run and its provider-free recovery. The original
handoff remains authoritative for the overall proof ladder and system
objective. This file is authoritative for the current checkpoint and next
action.

Also read:

- [Think Intelligence Gate contract v3](think-intelligence-contract-freeze-v1.md)
- [Learning log](company-learning-epistemic-repair-learning-log.md), especially
  `LOG-071` through `LOG-078`
- [Journey status](autonomous-company-learning-journey-status.md)
- [Execution coordinator](company-learning-epistemic-repair-agent-coordinator.md)
- [Core fast path](autonomous-company-learning-core-fast-path.md)

## 2. Current Verdict

The implemented provider-free TI3 recovery behaviors and tests are green. TI3
itself is **not** green or complete, and one frozen schema-identity mismatch
still blocks authorization.

- TI0 observability, TI1 dossier construction, TI2 synthesis/abstention,
  TI4-min receipts/scoring, frozen fixtures, and the bounded TI3 runner exist.
- The one allowed observational Arm A four-batch baseline completed and is
  immutable red evidence.
- The first live TI3 attempt, `ti3-live-8d7d9b05-r1`, is incomplete terminal
  red. It selected no arm and no policy.
- The two defects exposed by that attempt were repaired provider-free:
  parse-failure durability and trusted dossier-identity binding.
- Additional final attacks closed arbitrary non-JSON failure retention and a
  stale contract-digest binding.
- A final documentation audit then found that frozen contract v3 names the
  provider type `SynthesisSemanticDecision` with
  `think-synthesis-semantic-decision-v1`, while current code and policy
  receipts name `SynthesisProviderDecision` with
  `think-synthesis-provider-decision-v2`. Their intended fields and authority
  match, but frozen schema identity does not. Reconcile this provider-free
  before authorizing a live run.
- No fresh TI3 run occurred after the repairs.
- **CF3-C remains locked. Do not run it until one complete fresh TI3 run
  selects and freezes a versioned policy and that policy is wired into the
  production Think path.**

## 3. What This Session Achieved

### 3.1 Shared contracts and ownership

The small Think Intelligence contract was frozen before implementation and
amended only when evidence proved an ownership error.

- Contract v2 digest:
  `b1e234eee1cdfaf279a431efda4abe39bb7aff5896d1f1d2de1f0b5fbcb48717`
- Contract v3 digest:
  `8f90d9ecc723d61253f3e678fece1122967d280dcf8d8c23f1997d04d36c7a8f`
- V3 changes one boundary: the provider returns only a semantic decision;
  trusted code binds the already-known dossier ID and digest before compiler
  validation.
- The governing responsibility split remains:
  the LLM owns semantic judgment; the compiler owns identity, evidence
  closure, legality, versions, and transaction construction; validators
  constrain; the applier mutates atomically; the scorer owns independent gold.

### 3.2 TI0 — observable cognition

The active Think/provider path can retain and join:

- exact system and user prompts;
- policy, schema, model, effort, and routing versions;
- raw structured provider text before normalization;
- raw-text and parsed-object digests as distinct representations;
- compiler, validation, and apply stages;
- logical and physical call identity;
- retries, cache, token usage, cost, and latency; and
- independent semantic score references.

The final recovery additionally preserves JSON-valid schema failures and
arbitrary non-JSON provider text as terminal red evidence without fabricating
a parsed decision.

### 3.3 TI1 — scope-local dossiers

The provider-free twelve-batch mechanical inspection built 48 snapshots.

- Atlas becomes structurally mature at batch four.
- Cobalt becomes structurally mature at batch nine.
- The null case never becomes a synthesis opportunity.
- Dossiers use local handles and omit canonical UUID transport, wrapper
  objects, evaluator gold, and malformed derived entities.
- Temporal order and evidence roles remain explicit.

This proves dossier mechanics, not live semantic quality or production call
site selection.

### 3.4 TI2 — synthesis or abstention

The intended new provider-facing contract is an identity-free semantic
decision containing exactly one discriminated
`SynthesisProposal | AbstentionDecision`.

There is one unresolved exact-identity mismatch:

- frozen contract v3 requires class `SynthesisSemanticDecision` and
  `schema_version = think-synthesis-semantic-decision-v1`;
- current code uses class `SynthesisProviderDecision` and
  `schema_version = think-synthesis-provider-decision-v2`; and
- current TI3 policy receipts bind the code's v2 identifier.

This is not a semantic-authority disagreement, but schema and policy identity
are frozen evidence. The next session should preferably conform code and tests
to the already-frozen v3 names/version, or issue an explicitly reviewed new
contract version and digest. Do not authorize live TI3 while they differ.

Trusted code binds the closed dossier identity into the compiler-facing
`SynthesisDecisionEnvelope`. The compiler then validates handles, scope,
tenant, current head versions, evidence roles, and relation legality.

The provider schema now closes generic synthesis constraints:

- relation kind is one of `blocks`, `depends_on`, `causes`, `influences`, or
  `predicts`;
- relation sources are a subset of cause/condition handles;
- at least one relation source is an accepted `M*` handle;
- effect handles cannot be relation sources; and
- support contains at least one direct `O*` observation.

Accepted synthesis still compiles to one composite and one governed canonical
relation path. Abstention compiles to no mutation. Compiler and atomicity gates
were not weakened.

### 3.5 TI4-min and TI3 evaluation

The checkpoint now contains:

- frozen Atlas, Cobalt, and null/adversarial dossiers;
- opaque provider-visible dossier and candidate IDs;
- no evaluator-case labels in provider prompts;
- independent continuous scores and noncompensatory hard gates;
- prompt/schema/model/effort/policy receipts;
- deterministic policy selection using cheapest-within-quality-tolerance;
- screening of 3 dossiers x 3 arms x 1 sample;
- confirmation of 2 arms x 3 dossiers x 2 samples;
- exact 21-call accounting for a completed run;
- no response cache, no provider retries, and concurrency at most three; and
- immutable, digest-bound attempt directories and run manifests.

The isolated experiment correctly leaves validator/apply facts `not_run` and
nullable. It does not award unexecuted mutation gates.

## 4. Observation-Only Arm A Baseline

The single preregistered current-interface four-batch baseline ran from clean
detached commit `43dcb197` against isolated PostgreSQL database
`fyralis_ti0_baseline_43dcb197`.

- Provider: Codex CLI
- Model: `gpt-5.3-codex-spark`
- Effort: explicit medium
- Tenant: `64d91147-9a2e-4ccf-83ac-559d50e7d6cb`
- Workload: four intact batches, 100 signals
- Elapsed time: `982.222s`
- Result: red; no expected Atlas composite or composite-bound canonical
  relation

Artifacts:

- `/tmp/fyralis-ti0-baseline-43dcb197/raw.json`
  - byte SHA-256
    `8355fac92daaaa018cb870bfb297040de7d393978e291e76fa764bb127bfaa73`
- evidence
  - byte SHA-256
    `adec20dbcfb6be6bf2d8be41798918ad84d9bc36c95064aa2ee684198b24137d`
- report
  - byte SHA-256
    `4d02ae2ac4c58ea3644d4641db73182d981d3edf49cb9a0b9f105a590144ea3d`

This result is immutable observational evidence. It does not authorize a
legacy prompt/compiler patch or rerun.

## 5. The Incomplete TI3 r1 Run

### 5.1 Identity and configuration

- Run: `ti3-live-8d7d9b05-r1`
- Commit:
  `8d7d9b05c4081889a93b26cff8af2fb4cb4347de`
- Provider: Codex CLI
- Model: `gpt-5.3-codex-spark`
- Arms A/B: medium effort
- Arm C: high effort
- Cache: forbidden
- Retries: zero; `max_attempts = 1`
- Concurrency: at most three
- Partial directory:
  `/tmp/fyralis-ti3-live-8d7d9b05/ti3/ti3-live-8d7d9b05-r1`

### 5.2 Exact evidence boundary

- Nine screening calls were initiated.
- Six Atlas/Cobalt attempts have complete, materialized, digest-bound local
  per-attempt directories.
- One null Arm A response is known to have failed the
  `BatchMemoryDecisionSet` schema because it returned `decision = no_op`.
- `no_op` is an operation, not a valid decision. The intended no-mutation pair
  is `decision = reject`, `operation = no_op`.
- The other two initiated null-call terminal outcomes are unknown.
- The failed call's complete raw body and receipt were not written to the run
  directory.
- There is no governing run manifest.
- Exact all-nine token, cost, and terminal-outcome reconciliation is therefore
  impossible.

The untouched directory contained 48 files across six attempt directories
(`276K`) at inspection time. From the run directory, SHA-256 over the byte
stream produced by
`find attempts -type f -print0 | sort -z | xargs -0 shasum -a 256`
was
`b13c9c49552a684f2272d947ec0487cef8efa88deb2a67286590ad258f5a32eb`.
That aggregate describes the inspected local tree; it is not a replacement for
the absent run manifest or the two unknown outcomes. Because it remains under
`/tmp` and was not copied to a governed artifact store, it is not durable
evidence under contract v3 Section 8.

| Attempt ID | Case | Arm | Materialized local fate |
| --- | --- | --- | --- |
| `0d1265e4-fe94-508f-b549-6d223cec826d` | Atlas | A | Successful call; red semantic score |
| `420e0521-1707-55af-b2f1-fdb621963af0` | Atlas | B | Successful call; compiler identity rejection |
| `d6506257-21ff-59b5-b40d-c37997cdee2c` | Atlas | C | Successful call; compiler identity rejection |
| `591467b9-131c-5f5d-85e7-0ba983912d7c` | Cobalt | A | Successful call; red semantic score |
| `b249aea1-a2b1-5869-b4cd-b22d9bc4fa85` | Cobalt | B | Successful call; compiler identity rejection |
| `f0330c71-e507-50bc-8f8a-3b349c2c311b` | Cobalt | C | Successful call; compiler identity rejection |

The run is incomplete terminal red, primary class `schema_binding`. It is not
a valid nine-call screening, not a completed 21-call experiment, and not a
policy comparison. No arm or policy was selected.

Never resume, overwrite, selectively complete, or substitute the six successful
attempts into a future run. The identity is permanently consumed as failed
evidence.

### 5.3 What the preserved B/C attempts revealed

All four completed B/C responses returned a semantic synthesis but could not
pass the old compiler boundary:

- provider-visible input supplied `dossier_id` but omitted `dossier_digest`;
- the output schema nevertheless required the provider to return both;
- three calls emitted an all-zero digest;
- one call invented a nonzero digest; and
- all four failed with `dossier identity or digest mismatch`.

The compiler behaved correctly. The task interface assigned trusted identity
bookkeeping to a model that did not possess the value.

A read-only replay extracted each untouched semantic `decision`, omitted the
obsolete provider-authored identity fields, and wrapped the decision in the
current code's identity-free `think-synthesis-provider-decision-v2` top-level
schema. The semantic decisions were unchanged; the obsolete top-level wrapper
and schema version were necessarily replaced. All four still remained red,
correctly:

- Atlas B: unsupported `supports`; also used effect `O3` as a relation source.
- Cobalt B: unsupported `supports_causal_gate`; also used `O3` as a source.
- Atlas C: unsupported `causal_chain`; its source structure otherwise fit.
- Cobalt C: unsupported `supports`; also used `O3` as a source.

The trusted-binding repair therefore does not launder old failures. It removes
impossible identity transport while the new generic semantic schema continues
to reject unsupported relation semantics and invalid direction structure.

## 6. Failure Learnings And Architecture Insights

### 6.1 Schema failure is experiment evidence

The first TI3 runner assumed every provider call would parse successfully.
That was incompatible with a study that explicitly measures schema validity.
A schema-invalid response must become a scored red outcome, not crash the
experiment or disappear from the population.

### 6.2 Raw text and parsed objects are different evidence

The provider's raw digest is over the structured text representation. The
semantic decision digest is over the parsed canonical object. Comparing those
as if they were the same representation creates false integrity failures.
Both digests must be preserved and linked separately.

### 6.3 Non-JSON failures must also be durable

An early repair handled JSON objects that violated Pydantic but still called
`json.loads` unconditionally. Arbitrary non-JSON provider text would have
crashed after the provider receipts were already available. The final repair
stores exact raw text and a failure artifact, leaves the parsed decision empty,
and scores the outcome red without losing call accounting.

### 6.4 Do not make the LLM echo trusted identity

Requiring the provider to copy a dossier digest added no semantic value and
made success impossible when the digest was not visible. Even exposing it
would waste attention and make the provider appear to own identity. Trusted
code already knows the closed dossier and must bind it after semantic output.

### 6.5 Close semantic vocabularies at the provider boundary

An arbitrary relation-kind string caused plausible prose such as
`causal_chain` and `supports_causal_gate` to reach the compiler even though no
such governed relation existed. The provider must see a closed generic
semantic vocabulary, while the compiler remains the final legality authority.

### 6.6 Cross-field rules must be visible before compilation

The compiler correctly requires relation sources to be causes and not effects,
but the original one-line provider instruction did not expose that rule.
Provider schemas/descriptions now state the generic subset, Model-source, and
direct-observation requirements. These are contract rules, not fixture gold.

### 6.7 Fail-closed compilers are valuable

The old B/C outputs contained strong-looking semantic prose. None was admitted
because identity and relation contracts failed. Continuous semantic scores are
diagnostic and cannot compensate for a failed hard gate.

### 6.8 Concurrent calls require terminal-outcome durability

An exception inside `asyncio.gather` allowed sibling provider calls to be
launched without all terminal outcomes being written. Expected negative
provider results now return as typed outcomes so the preregistered population
can finish and reconcile. Infrastructure exceptions still abort and require
their own failure classification; do not pretend they are semantic outcomes.

### 6.9 A run identity is immutable evidence

Once a physical call is consumed, missing attempts cannot simply be rerun
under the same 21-call identity. Reusing the ID would either overwrite evidence
or exceed the frozen call population. Use a fresh full identity after a
provider-free repair and written hypothesis.

### 6.10 Contract digests need one source of truth

After v3 was frozen, the synthesis compiler and experiment artifacts still
embedded the v2 digest. The final fix makes experiment artifacts import the
shared `CONTRACT_DIGEST`, now set to v3, instead of duplicating literals.

### 6.11 Audits need adversarial negative representations

The preflight audits correctly checked retries, cache, usage, labels, digests,
and call counts but missed two impossibilities: a hidden required digest and
arbitrary non-JSON failure retention. Future authorization reviews should
include at least one schema-invalid JSON object, one non-JSON body, one hidden
required field, and one cross-field semantic violation.

## 7. Final Provider-Free Recovery

The following commits form the terminal recovery chain:

| Commit | Meaning |
| --- | --- |
| `0d29188c` | Record incomplete TI3 r1 screening |
| `5f5f1d64` | Preserve schema-invalid provider outcomes |
| `134ed61e` | Add independent failure-evidence attacks |
| `64a7da4c` | Freeze trusted identity binding in contract v3 |
| `ed25b33b` | Implement identity-free provider output and trusted binding |
| `bca3ada0` | Record provider-free recovery checkpoint |
| `4e85d855` | Retain arbitrary non-JSON failures and bind v3 digest |
| `e5776b0e` | Record final parse/digest wrap closure |

Important earlier checkpoints in this execution sequence:

| Commit | Meaning |
| --- | --- |
| `1dd66aba` | Initial shared Think Intelligence contract freeze |
| `31d34ed2` | Shared contract freeze v2 |
| `466665ca` | TI1 dossier implementation |
| `5fb9cbf8` | TI2 initial contract/compiler implementation |
| `b0fa7f70`, `68b5a0e8` | TI0 observability and parse evidence |
| `9125190a`, `fd498f90` | Cognition-trace integrity and TI2 semantic-role conformance |
| `43dcb197` | Frozen observational baseline checkpoint |
| `eb130738` | Record immutable red Arm A baseline |
| `13d2d327` | Semantic scorer and TI4-min receipts |
| `f4313516` | Frozen dossiers and gold |
| `582cfb6d` | Bounded TI3 orchestration |
| `80750d46`, `f563f6f4`, `e3b33ed9` | Interface fidelity, live adapter, evidence integrity |
| `460427b1`, `8d7d9b05` | Opaque provider IDs and raw-representation binding |

## 8. Validation At The Wrap Checkpoint

At code commit `4e85d855` and documentation HEAD `e5776b0e`:

- 75 focused provider-free tests passed;
- one real PostgreSQL synthesis atomicity test passed against
  `postgresql://localhost/fyralis_ti2`;
- Python compilation passed for the changed runtime and evaluator files;
- architecture ratchets passed;
- `git diff --check` passed;
- the contract file's byte SHA-256 exactly matched
  `8f90d9ecc723d61253f3e678fece1122967d280dcf8d8c23f1997d04d36c7a8f`;
  and
- the integration worktree was clean.

Final Ruff validation was not run because no `ruff` executable or module was
installed in the integration environment. Earlier lane checkpoints reported
Ruff green before the final non-JSON change; do not represent that as a Ruff
result for final HEAD.

No provider call or CF3-C run occurred after r1.

These checks prove the implemented behavior, not exact conformance of the
current provider class/schema identifier to frozen contract v3. That mismatch
was found after the test checkpoint and is a live-authorization blocker.

## 9. What Remains Unproven

- Exact contract-to-code conformance for the provider-facing schema identity:
  frozen `SynthesisSemanticDecision` /
  `think-synthesis-semantic-decision-v1` versus implemented
  `SynthesisProviderDecision` / `think-synthesis-provider-decision-v2`.
- A complete fresh TI3 screening and confirmation population.
- Any eligible selected arm under all isolated hard gates.
- The cheapest-within-tolerance policy decision on real provider evidence.
- Production call-site routing from governed episodes through TI1 and the
  selected TI2 policy.
- End-to-end validator/applier behavior for the selected policy in CF3-C.
- Correct Atlas composite and canonical relation admission in a live
  four-batch canary.
- CF4 contradiction, lifecycle transition, corrected-head retrieval, and
  later reuse.
- M1: synthesis -> correction -> retrieval/use of corrected current head.
- CF5 twelve-batch mixed-stream development proof.
- CF6 sealed unseen company generalization.
- CF7 matched memory-value ablation.
- CF8 interruption, replay/idempotency, tenant isolation, bounded growth, and
  final closeout.
- The TI0 broad `threshold` forbidden-key matcher may still suppress legitimate
  retrieval debug-capture fields. Reassess before CF3-C only if it prevents the
  required complete trace; do not broaden scope otherwise.
- The r1 `/tmp` directory is local partial evidence, not a complete governed
  experiment artifact. Its six manifests do not repair the missing outcomes.

## 10. Exact Next Authorized Action

If work resumes, do **not** begin with CF3-C and do not reuse r1.

1. Reconcile the exact provider class/schema identity with frozen contract v3.
   The narrowest path is to rename current code and policy receipts to
   `SynthesisSemanticDecision` and
   `think-synthesis-semantic-decision-v1`, update focused tests, and keep the
   existing v3 contract bytes/digest unchanged. If semantics or ownership must
   change instead, issue an explicitly reviewed new contract version/digest.
2. Run the focused provider-free suite and PostgreSQL atomicity check after
   that reconciliation.
3. Verify the required worktree, branch, clean status, current HEAD, exact
   contract-to-code schema identity and digest, Python/PostgreSQL availability,
   Codex CLI authentication, free artifact path, disabled response cache, and
   absence of an active provider run.
4. Perform independent read-only authorization attacks on:
   raw/non-JSON evidence durability, exact one-attempt receipts, hidden
   identity exclusion, provider prompt/gold separation, policy versions, and
   21-call selection accounting.
5. If and only if all attacks authorize, run **one fresh complete TI3** under a
   new run ID and output directory. Use the current commit, not
   `8d7d9b05`, and preserve all prompt/raw/compiler/score/receipt/manifests.
6. Use exactly:
   - provider `codex` via CLI;
   - model `gpt-5.3-codex-spark`;
   - Arm A/B medium and Arm C high effort;
   - zero response cache;
   - `LLM_MAX_RETRIES=0` and `max_attempts=1`;
   - maximum concurrency three;
   - nine screening plus twelve confirmation calls; and
   - quality tolerance `0.03`.
7. Accept a semantic or schema failure as evidence. Do not tune and immediately
   rerun. If no arm satisfies the hard gates, stop for the architecture
   decision required by the handoff.
8. If TI3 completes green, independently score it, freeze the selected policy
   and its exact digest, then wire that policy into the production Think path.
9. Only after production wiring, focused/atomic validation, and an independent
   readiness audit may the integration owner unlock one clean CF3-C run.

Command template after preflight, using a genuinely fresh identity:

```bash
HEAD_FULL="$(git rev-parse HEAD)"
HEAD_SHORT="$(git rev-parse --short=8 HEAD)"
RUN_ID="ti3-live-${HEAD_SHORT}-r2"
OUTPUT_ROOT="/tmp/fyralis-ti3-live-${HEAD_SHORT}-r2"

env \
  LLM_PROVIDER=codex \
  CODEX_MODEL=gpt-5.3-codex-spark \
  CODEX_TRANSPORT=cli \
  LLM_MAX_RETRIES=0 \
  .venv/bin/python scripts/run_think_ti3_live.py \
  --output-root "${OUTPUT_ROOT}" \
  --run-id "${RUN_ID}" \
  --commit "${HEAD_FULL}" \
  --quality-tolerance 0.03 \
  --max-concurrency 3
```

Before executing, confirm that both the run directory and run ID have never
existed. The template is not standing authorization; the read-only preflight
and independent authorization must still pass. It is forbidden to use while
the frozen/implemented provider schema identities differ.

## 11. Stop Rules For The Next Session

- Do not run CF3-C before TI3 selects and freezes a policy and production
  wiring is green.
- Do not repair or rerun r1.
- Do not substitute historical successful calls into a fresh experiment.
- Do not change prompts, schema, model, effort, gold, thresholds, or selection
  after seeing a failed fresh run without the handoff's written hypothesis and
  architecture-review rules.
- Do not weaken compiler, validator, atomicity, or semantic hard gates.
- Do not expand into task autonomy, connectors, broad prompt platforms,
  general episode discovery, UI, or production polish.
- Do not open the sealed holdout before the release-candidate surface is
  frozen.
- Update the learning log, journey, coordinator, and this continuation record
  whenever the proof boundary changes.

## 12. Suggested New-Thread Instruction

> Read `docs/plans/autonomous-company-learning-ti3-recovery-handoff-20260719.md`
> completely, then read the linked contract v3 and learning-log entries
> LOG-071 through LOG-078. Continue from the clean
> `codex/autonomous-company-learning` worktree. Do not run CF3-C or reuse the
> incomplete TI3 r1 evidence. First reconcile frozen contract v3's
> `SynthesisSemanticDecision` / `think-synthesis-semantic-decision-v1` identity
> with the currently implemented provider schema and policy receipts, then
> revalidate and independently audit the provider-free TI3 recovery. If every
> preflight is green, authorize exactly one fresh full 21-call TI3 run under a
> new identity, independently score it, freeze the selected policy, and only
> then prepare production wiring and CF3-C readiness.
