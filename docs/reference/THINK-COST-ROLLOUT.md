# Think Cost-Optimization — Implementation Rollout

> **Status:** In progress. **Date:** 2026-06-11.
> **Companion to** [`THINK-COST-PLAN.md`](THINK-COST-PLAN.md): that file is the
> audited *what/why*; this file is the verified, sequenced *how*, grounded in a
> multi-agent re-verification of the actual current code (the plan's line numbers
> are stale — all locations here are real symbol positions).
>
> **Production provider is Codex** (confirmed by the owner), *not* DeepSeek
> or Anthropic as the plan assumed. Think main reasoning uses the configured
> Codex model/effort; question planning uses a separate low-effort Codex model.

## Implementation status (2026-06-11 pass)

Landed (cache-friendly prompt layout is unconditional; remaining
quality-affecting flags default **off**; full suite green incl. DB integration
tests):

| Item | Flag (default) | Status |
| --- | --- | --- |
| 0a dead-letter anti-amplification | none (bugfix) | ✅ shipped + test |
| 0b benchmark `--unbatched` arm | `--unbatched-run` | ✅ shipped |
| 0.1 cache-token capture + cache-tier pricing | unconditional | ✅ shipped + tests |
| 0.1 purpose + cache + real-retry ledger | unconditional (migration `0131`) | ✅ shipped + tests |
| 1.1 cache-friendly prompt layout | unconditional | ✅ shipped + tests |
| 1.2 `enforces_output_schema()` + lean shape prose | `THINK_STRICT_LEAN_PROMPT` (off) | ✅ mechanism + minimal-safe lean + tests |
| 1.3 env char budgets + candidates cap | config (defaults = current) | ✅ shipped + test |
| 2.4 validation-retry cap + feedback persist/append | `THINK_VALIDATION_MAX_ATTEMPTS` (unset) | ✅ worker cap + prompt feedback + tests |
| 2.2 `check_already_applied` + early idempotency skip | `THINK_EARLY_IDEMPOTENCY_SKIP` (off) | ✅ shipped |
| 2.5 fast planning for chosen classes | `THINK_FAST_PLAN_TRIGGER_KINDS` (empty) | ✅ shipped + tests |
| 3.1a provider/model footgun warnings + init log | `LLM_STRICT_CONFIG` (off→warn) | ✅ shipped |
| 3.1b per-tenant daily spend/token/request ceilings | `THINK_DAILY_BUDGET_ENFORCEMENT` (off) | ✅ shipped |
| 2.4 live model escalation | `THINK_ESCALATION_MODEL` (unset) | ✅ shipped + tests |
| 2.2 step 3 diff-reuse on tx retry | `THINK_REUSE_DIFF_ON_TX_RETRY` (off) | ✅ shipped + tests |
| 3.2 cascade-depth threading (T2/T3/T4) + tighter bound | `THINK_MAX_INFERENTIAL_LINEAGE_DEPTH` (unset) | ✅ shipped + tests |

Deferred (with rationale — documented, not silently skipped):

- **1.2 full lean prose** — analysis (R13) showed almost all system-prompt prose
  is *semantic* (edge-kind vocabulary, falsifier kinds, scoping/confidence),
  which the strict schema does **not** enforce; only the top-level JSON-shape
  skeleton is safely removable. The capability + flag + a single-source skeleton
  trim shipped; deeper prose authoring needs human review + benchmark gating and
  is dormant on Codex anyway (`enforces_output_schema` → False).
- **2.4 escalation model choice** (`THINK_ESCALATION_MODEL`) — the escalation
  *mechanism* now ships (a cached provider on the retry); which model to escalate
  *to* is still an unresolved `TODO(human)`. Unset → no escalation.
- **2.2 step 4 production response cache** — C5 (replaying an invalid diff on
  validation retry) is already neutralized by 2.4's feedback-append changing the
  prompt bytes; enabling a *production* response cache also needs a real backend.
  Deferred; the test-infra hook is untouched.

> **3.2 caveat:** the depth field is now threaded (T2 = real lineage; T3 via
> context_planner; T4 = lineage-root depth 1 since the topology sweep has no
> parent trigger). Per the plan, *observe* the new lineage distribution before
> tightening `THINK_MAX_INFERENTIAL_LINEAGE_DEPTH` below the hard `MAX_CASCADE_DEPTH`.

New env knobs are documented in `.env.production.example`.

---

## 0. Provider reality (read first)

`LLM_PROVIDER=codex`. `CodexProvider` ([`lib/llm/provider.py`](../../lib/llm/provider.py))
has three transports:

| Transport | `max_tokens` / `temperature` | Token usage | Cache-token signal |
| --- | --- | --- | --- |
| `responses` (OpenAI API key) | honored (`max_output_tokens`) | real `usage` block | yes (`prompt_tokens_details.cached_tokens`) |
| `app-server` (ChatGPT/Codex login) | **dropped** (`del`) | **estimated from chars** | none |
| `cli` (`codex exec` subprocess) | **dropped** (`del`) | **estimated from chars** | none |

Consequences vs. the plan:

- **Telemetry (Phase 0.1)** only yields *real* cache tokens on the `responses`
  transport. On CLI/app-server, usage is a char-based estimate — the ledger
  records approximate cost, never cache hits. The cache-token plumbing still
  ships (it's free and correct for the API path + future provider switches),
  but **the dollar verification of 1.1 on the CLI path is not directly
  measurable** — fall back to the storyline benchmark.
- **Output cap (Phase 3.1 "cap Codex output")** is a no-op on CLI/app-server —
  those transports own `max_tokens`. Only the `responses` path can be capped.
- **Prefix caching (Phase 1.1)** for Codex is the GPT-5 backend's *automatic*
  prompt cache (no `cache_control` needed); a stable static prefix still helps
  the `responses` path and the subscription backend. The Anthropic
  `cache_control` block-list rewrite is a non-prod readiness item, deprioritized.
- **Lean strict prompt (Phase 1.2)**: Codex sends the schema as a *non-strict*
  `json_schema` hint (`strict: False`) or as appended CLI text — i.e. the schema
  is **not server-enforced**. So the "delete prose the schema enforces" premise
  does **not** hold for Codex; the new `enforces_output_schema()` capability
  returns False for Codex, and 1.2 keeps full prose on this path (safe no-op).
  1.2 therefore stays off on the production Codex path.
- **Planning cheap-model seam (Phase 2.5)** is **live today**:
  `select_question_planning_provider` already downgrades planning to
  `gpt-5.3-codex-spark` at low effort *because* the provider is Codex
  ([`question_planning_provider.py`](../../services/platform/execution/question_planning_provider.py)).
  2.5's remaining work is reducing planning *rounds*, not adding the seam.

---

## 1. Reality-corrections (don't trust the plan verbatim)

| # | Plan claim | Reality | Location |
| --- | --- | --- | --- |
| R1 | opus-4-7 already repriced $5/$25 | still `15/75` (uncorrected) | `provider.py:78` |
| R2/R3 | usage extractors capture cache tokens; pricing has cache tiers | refuted — `(input,output)` only; 2 prices/model | `_extract_openai_usage`, `_extract_anthropic_usage`, `MODEL_PRICING` |
| R5 | Codex `_raw_call_responses` drops `max_tokens` | refuted there (`responses` honors it); CLI/app-server drop it | `provider.py:1305` vs `:1327,:1358` |
| R6 | retry counts threaded to ledger | hardcoded `retry_count=0`; real counters exist unused | `reason.py:443` |
| R7 | `model_reeval` authoritative | refuted — excluded from tuple; nudge handler exists. **§2.1 (making it authoritative) was dropped by owner decision — `model_reeval` stays on the LLM path.** | `deterministic.py` |
| R8 | `check_already_applied` extracted | inline in `apply_diff` | `applier.py:158-167` |
| R9 | `uses_strict_tool_schema()` exists | absent — only model-class gating | `provider.py:1954-1965` |
| R10 | `RawDiffAliased` exists | absent — greenfield (4.1) | — |
| R11 | cascade depth threaded everywhere | only `enqueue_cascade_t1`; T2/T3/T4 not | `cascade.py:114`, T2 `applier.py:626`, T3 `trigger_emitter.py:195`, T4 `field.py:855` |
| R13 | strict schema enforces 16-kind enum | regex `^[a-z][a-z0-9_]{2,63}$`; subset-only | `strict_schema.py:401` |
| R14 | response cache un-wired | already wired; keyed w/o purpose/retry → C5 footgun | `provider.py:930-950` |
| R15 | provider defaults safe | now defaults to `codex` and logs initialization; compatibility providers remain opt-in for tests/harnesses | `provider.py` |
| R18 | char budgets env-overridable | module constants | `prompt.py:37-46` |
| R19 | ledger has purpose/cache cols | only `retry_count` (=0); no purpose/cache | `db/migrations/0016_think_run_costs.sql` |
| R20 | 2.4 retry loop / 3.1 daily budget exist | neither — greenfield | `reason.py`, worker |
| R22 | `routing.py`/`contracts.py` present | deleted; `SignalRoute` inlined to `inquiry.py:57-64` | — |
| R24 | unbundle has anti-amplification | absent — released members re-batchable | `worker.py:1706-1723` |

**Build on in-flight uncommitted diffs (don't revert):** `provider.py` added
`_env_int()`+`LLM_MAX_RETRIES`; `llm_reason.py` landed claims-only
`max_tokens=1024`; prompt/diff/strict schemas added `"disputed"` review_status.

---

## 2. Sequenced waves

Order honors interaction rules: **C7** (dead-letter fix before any batching) →
**Phase 0.1** (ledger truth, unconditional) → **C1** (1.1 before 1.2) → **C5**
(2.4 before 2.2's response-cache enable) → **C8/C6** (no double-count).

| Wave | Item | Flag (default) | Risk |
| --- | --- | --- | --- |
| 0a | Dead-letter anti-amplification (`unbatched_from`) | none (bugfix) | low |
| 0b | Benchmark `--unbatched` A/B arm | none | low |
| 1 · 0.1 | Cache-token capture + cache-tier pricing + opus fix + purpose/retry ledger (new migration) | unconditional | low |
| 2 · 1.1 | Profile→user-top, operating-instructions→system | unconditional | moderate |
| 3 · 1.2 | Lean prompt + `enforces_output_schema()` capability | `THINK_STRICT_LEAN_PROMPT` (off) | moderate |
| 3 · 1.3 | Env char budgets (`PromptConfig.from_env`) + new caps | config (defaults = current) | low |
| 5 · 2.4 | Validation-retry feedback-append + escalation | `THINK_VALIDATION_MAX_ATTEMPTS`=1 | moderate |
| 6 · 2.2 | Extract `check_already_applied` + response-cache key fix | `THINK_RESPONSE_CACHE` (off) | low/high |
| 7 · 3.1 | Provider/model hardening + init log; per-tenant daily budget ceilings | `THINK_DAILY_BUDGET_ENFORCEMENT` (off) | low/mod |
| 7 · 3.2 | Thread `cascade_depth` through T2/T3/T4 | `THINK_MAX_INFERENTIAL_LINEAGE_DEPTH` | moderate |
| 7 · 2.5 | Reduce planning rounds for low-value T1 classes | `THINK_PLANNING_ROUNDS_*` | moderate |

All quality-affecting flags ship **off**; ledger/refactor/bugfix items ship
**unconditional**. Every quality-affecting enable is gated on the §9 storyline
benchmark + quality replay metrics and, where output could differ (2.4, 1.1,
1.2, 2.5), a **shadow** comparison first.

> **2.1 dropped (owner decision, 2026-06-11):** routing mechanical `model_reeval`
> re-evals to the deterministic confidence-nudge handler would forfeit any
> nuance the LLM adds (archive vs nudge, new edges). The owner rejected that
> tradeoff, so `model_reeval` keeps the LLM path (its original behavior) and
> the `THINK_DETERMINISTIC_MODEL_REEVAL` flag was removed entirely.

## 3. Blocked on prod sizing / human input (enable decisions only)

- Every flag *enable* (cache-hit rate, burst share, no-survivor rate, T4 volume,
  planning-spend share) — needs Phase 0.2 sizing on demo/prod DB.
- `LLM_DAILY_BUDGET_USD_PER_TENANT` value (finance/human).
- 2.4 `THINK_ESCALATION_MODEL` + `THINK_VALIDATION_MAX_ATTEMPTS` (TODO(human)).
- opus reprice target — fetch live pricing (dashboards-only since prod is Codex).

---

## 4. Verification protocol (per §9 of the plan)

1. Storyline benchmark (`scripts/run_storyline_batch_benchmark.py`) — baseline
   $10.92/10k, CI 0.8879; establish a same-config variance band first
   (the new `--unbatched` arm enables the A/B).
2. Think quality replay (`quality_report.py`): per-trigger-kind
   `flagged_success_rate`, `unused_selected_context_rate`, context coverage.
3. Flag-gating + instant env rollback; shadow mode where output could differ.
4. Ledger-verified savings (Phase 0.1) — **note Codex CLI yields estimated
   tokens only; real cache-hit verification requires the `responses` transport.**
