# Fyralis Core — Architecture Repair Plan

Last reviewed from code: 2026-06-10

## What this document is

This is an implementation plan for repairing the structural debt in Fyralis Core
**without changing its architecture**. It is the output of a multi-agent audit
(12 area maps, 3 cross-cutting diagnoses, 4 competing redesign proposals, 12
adversarial reviews) reconciled into a single sequenced plan. Every file/line
reference below was verified directly against source on 2026-06-10.

It is a companion to:

- `SYSTEM-ARCHITECTURE.md` — what the system *is* and how it is wired.
- `docs/hardening-backlog.md` — the prioritized defect backlog (referenced by P-number).
- `docs/adr/` — where the decisions this plan implies will be recorded.

Read `SYSTEM-ARCHITECTURE.md` first. This document assumes that mental model.

---

## 1. The thesis: don't redesign, make the design true

The core design is sound and should be kept. Specifically, these are good and
stay:

- **Observations are evidence; models/edges/acts/resources are derived beliefs.**
- **Memory mutates only through validated diffs** (`diff_schema` → validator →
  applier with region locks + `applied_triggers` idempotency). This is the
  single best idea in the codebase.
- **Layers depend downward**: `lib → domain → {ingest, platform, reasoning} →
  {product, app, workers}`.
- **Postgres is the queue, the lock manager, and the system of record.**
  `FOR UPDATE SKIP LOCKED` leasing, advisory locks, `NULLS NOT DISTINCT` dedup.
  No external job system is needed at this scale.
- **The ingest handler registry** (`@register` → `ObservationDraft` →
  `ingest_from_draft`) is the cleanest seam in the repo.
- **Gateway as composition root** with startup-status tracking and graceful
  degradation.

The problem is **not** the architecture. The problem is that the six contracts
that define the architecture are enforced *socially* (by docs and discipline),
not *structurally* (by code, schema, or CI). In this codebase every socially
enforced contract has decayed, and every mechanically enforced one has held:

| Enforcement | Examples | Status |
|---|---|---|
| **Mechanical** | import-linter's 3 contracts, `applied_triggers` idempotency, `FOR UPDATE SKIP LOCKED` leasing, region advisory locks | **Held** |
| **Social** | "memory mutates only through diffs", "layers depend downward", "inline ≡ Kafka", "vectors are semantic", dual-write parity, the trigger-queue contract | **Decayed** |

This pattern is decisive for a **one-founder + AI-agent team**: agents amplify
whatever the structure makes checkable and silently violate whatever it does
not. Therefore the organizing principle of this plan is:

> **Convert each load-bearing social contract into a mechanical one** — a CI
> ratchet, a schema constraint, a single owning module, or a deleted ambiguity —
> and pay down the specific seams where reality has already diverged.

No directory re-layering, no new infrastructure kind, no new abstraction layer.

---

## 2. The six divergences (verified)

Each is a place where the stated contract and the actual code disagree. These
are the targets; everything in §4 maps back to one of them.

### D1 — Two mutation grammars into the system of record

The stated contract is that domain memory mutates only through Think's validated
diff pipeline. In reality, product code writes the same tables directly:

- `services/product/recommendations/handlers.py:37-41` imports
  `services.domain.acts.{commitments,decisions,goals}`, `resources.repo`, and
  `emit_state_change`, and mutates acts directly.
- `services/product/decision_deltas/apply.py:267` runs raw `UPDATE resources`
  and `:327` runs raw `INSERT INTO topology_events`.

Consequence: region locking, `diff_hash` replay, `applied_triggers`
auditability, and the C1–C10/G1–G4 invariants cover only *one* of two write
paths.

### D2 — Domain executes reasoning and product code inside its insert transaction

`services/domain/models/repo.py` (2,972 lines) is the cognitive core wearing a
repository name:

- `:116-118` import `LatentTopologyService`, sage affordance policy, and
  topology `content_anchor` **at module load**.
- `:118-124` carry a comment admitting that importing
  `reasoning.think.audit` at module load is **"fatal"** (a circular import) —
  proof that the package boundary is drawn through the middle of one cohesive
  subsystem.
- `:1665` lazily imports
  `services.product.recommendations.handlers.act_on_recommendation` and **calls
  it during model insert** to auto-accept low-risk recommendations. That product
  handler imports `services.domain.acts.*` — a full **domain → product → domain
  loop executing inside a repo insert transaction**.

The same insert also computes an Ollama embedding via an HTTP round-trip *inside*
the transaction, under Think's region lock.

### D3 — Inline and Kafka ingestion are not set-equal

`ingest_from_draft` declares an N1 invariant: writer (Kafka) output must be
set-equal to inline output for the same input. It is already violated: Gmail
thread canonicalization runs only on the inline path
(`services/ingest/ingestion/handlers/gmail.py`, a 3-transaction dispatcher),
while the Kafka normalizer maps Gmail backfill/poll straight to the plain
`gmail:` handler — so `observations.thread_canonical_id` is **NULL** for
Kafka-ingested mail and the same thread splits across paths. The writer's draft
reconstruction also drops `unresolved_phrases` and `raw_payload`.

### D4 — `models.embedding` mixes fake vectors with real ones, and the promised backfill does not exist

When an LLM diff omits an embedding (essentially always — LLMs cannot emit
768-dim vectors), the Think applier inserts a deterministic hash-bucket
*lexical* vector into the **same** `vector(768)` column that pathway-B semantic
retrieval cosine-searches via the HNSW index. The applier comment says these are
placeholders "until a production embedding backfill refreshes it," but **no code
anywhere updates `models.embedding` after insert**. Unlike `observations`,
`models` has no `embedding_pending` column, so polluted vectors are
indistinguishable from real ones and unrebuildable. Every model born from LLM
reasoning silently and permanently degrades semantic retrieval.

### D5 — The deployed process set no longer matches the tree

- **8 of 11** `services/workers/*` packages never run anywhere (no compose
  service, no launcher): `anomaly_processor`, `calibration_updater`,
  `deadline_resolver`, `edge_drift`, `entity_resolver`, `maintenance`,
  `precipitation`, `topology_sweeper`. Only `sage_structural_features`,
  `sage_topology_optimizer`, and `relationship_ontology_proposals` are wired in
  `docker-compose.yml`. This breaks closed loops the live code *assumes*:
  calibration offsets read on every model insert are never recomputed; overdue
  predictions never get T2 triggers; ACL materialized views never refresh
  (the only caller is the unlaunched `MaintenanceScheduler`, and its incremental
  path is an in-memory `_DIRTY` set lost on crash); dual-write edge parity is
  never checked.
- The deterministic routing gate (`services/platform/execution/routing.py`:
  `decide_route`, `record_routing_decision`) and its `signal_routing_decisions`
  table (migration 0046) have **zero callers** outside the package's own
  `__init__` and tests.
- `topo_dirty_queue` (migration 0032) has **zero references** in `services/` or
  `lib/`.
- `docker-compose.yml` defines a large process set with **no memory limits**,
  targeting a 4 GB demo box; the Kafka data plane is sized for a platform
  company (≈100 topics × 12 partitions on one unreplicated broker) while serving
  one tenant on a few sources.
- Legacy top-level `services/{gateway,models,think,query}` dirs are dead residue
  containing only tests and `__pycache__`.

### D6 — `think_trigger_queue` has no entry point

The central reasoning queue is raw-SQL-inserted by **11 modules across 4
layers**: `ingest/ingestion/core.py`; `product/recommendations/handlers.py`,
`product/ask/store.py`; `reasoning/think/worker.py`,
`reasoning/think/cascade.py`, `reasoning/topology/field.py`,
`reasoning/dynamics/trigger_emitter.py`; `workers/deadline_resolver/worker.py`,
`workers/anomaly_processor/worker.py`, `workers/precipitation/proposer.py`,
`workers/entity_resolver/worker.py`. The trigger contract (kind/payload/dedup/
`scheduled_for`/batch columns) lives implicitly in 11 divergent SQL strings;
migration 0125 (batch/parent-trigger) had to touch all of them. Relatedly,
`lib/shared/edge_registry.py` — the bottom layer — raw-inserts
`model_reeval_queue`, making a "shared primitive" a writer of a reasoning queue.

### Cross-cutting: the import-linter only guards edges that were already clean

`pyproject.toml` defines exactly 3 `forbidden` contracts (core ⊥ demo/sim, lib ⊥
services, reasoning ⊥ app/product/ingest). It honestly encodes only invariants
"empirically true today." Every divergence above (domain → reasoning, domain →
product, platform ↔ reasoning, ingest → app, lib writing reasoning queues) has
**no contract, no failing check, no ratchet** — so the worst boundaries can only
degrade. This is the single cheapest, highest-leverage gap to close.

---

## 3. Do-regardless fixes (independent of the redesign)

These surfaced in the audit, are cheap, and should land first regardless of
whether the rest of the plan proceeds:

- **R1 — Header redaction (P0-3).** Full request headers, including
  `Authorization` and `X-Bootstrap-Secret`, flow unredacted into ingest and
  webhook capture paths (`services/app/gateway/core_router.py:184` →
  `ingestion/core.py`; webhook capture at three sites). Add one `safe_headers`
  helper + one structlog redaction processor. ~30 lines; blast radius is leaked
  replayable credentials on the demo box.
- **R2 — Loud rendering-backend guard.** `build_rendering_adapter()` returns a
  **mock** adapter whenever `GRT_RENDERING_BASE_URL` is unset, and that var is in
  neither `docker-compose.yml` nor `.env.production.example` — so the flagship
  surface can silently serve mock copy in production. Make absence a loud startup
  warning (or hard fail in prod). One line.
- **R3 — LLM spend telemetry → ceilings (P2-14).** Cost is recorded
  (`think_run_costs`, `view_render_costs`) but nothing is enforced; greeting
  re-renders ~15 surfaces every 15 min per tenant with no content-hash skip. Add
  a per-tenant/per-day ceiling check at the provider boundary and a content-hash
  skip in the greeting scheduler. A retrieval-loop bug currently has no kill
  switch short of revoking API keys.

---

## 4. The five structural moves

Each move (a) names the divergence it closes, (b) explains *why* it is the right
fix, (c) gives concrete steps, and (d) states the risk the adversarial review
surfaced. They are written in dependency order but sequenced for shipping in §5.

### Move 1 — One mutation grammar, with a written ledger

**Closes:** D1 (and is a prerequisite for trusting D2's side-effect inversion).

**Why.** As long as two write grammars exist over acts/resources/observations,
the diff pipeline's guarantees (region locks, idempotency, invariants, replay)
are partial — they protect Think's writes and not product's. Unifying the write
path is what makes "memory mutates only through validated diffs" *true* rather
than aspirational. The ledger (`applied_diffs`) is the cheap half and delivers
standalone value: today only a `diff_hash` survives an apply, so memory is
un-replayable and un-debuggable beyond "a diff with this hash happened."

**Steps.**

1. **`applied_diffs` table** (days, zero behavior change). Migration adds
   `applied_diffs(trigger_id FK, tenant_id, diff JSONB with resolved IDs,
   schema_version, applied_at)`. `services/reasoning/think/applier.py` writes the
   full materialized diff in the *same* transaction where `applied_triggers`
   flips to `success`. Ship `ops/rebuild memory --dry-run` that replays the diff
   log into a scratch schema and reports divergence. **Value alone:** full
   mutation audit + replay debugging immediately.
2. **Sanctioned domain command seam** for product writes. Move
   `decision_deltas/apply.py`'s raw `UPDATE resources` / `INSERT INTO
   topology_events` and `recommendations/handlers.py`'s act mutations into named
   functions in the `services/domain/acts` and `services/domain/resources`
   modules they already call. Each function emits `state_change` and enqueues
   triggers via Move 5's helper. Product keeps calling domain (downward, legal)
   but **never ships raw SQL against domain tables**.

**Deliberately deferred:** forcing every product action through a *synthesized
ValidatedDiff* (the full event-log doctrine). The adversarial review was
unanimous that this solves an aspirational pain not yet on record and is the
wrong place to spend budget now. The domain-command seam captures invariants and
state-change emission in one place — that is the recorded-pain fix. Revisit
diff-synthesis only if a second mutation channel reappears.

**Risk.** Replay determinism is only a real guarantee once Move 2 lands
(side-effect inversion); until then `rebuild memory` is dry-run-safe only. Mark
this explicitly in docs so the partial guarantee is not assumed complete.
`applied_diffs` rows must carry `schema_version` and a replay-through-pinned-code
policy, or the rebuild guarantee silently expires at the first breaking
`diff_schema` change.

### Move 2 — De-fang ModelsRepo (side-effect inversion)

**Closes:** D2, D4 (the in-transaction embedding half), and the worst layering
violations in one stroke.

**Why.** This single 2,972-line file is the source of the domain → reasoning
module-load imports, the domain → product → domain loop, the
network-call-inside-transaction-under-region-lock stability risk
(`docs/hardening-backlog.md` P0-5 calls this "the single largest stability
risk"), and the pgvector pollution. All of them are fixed by **queue-row
indirection through machinery that already runs in compose** — the
`pending_post_commit_actions` outbox and the `post_commit_worker`. No new
pattern, no new process.

**Steps** (this is the only step with real semantic risk; design it before
coding).

1. **Reduce insert to its essence:** `insert = validate + store +
   emit_state_change`. Move topology dispatch, affordance-profile upsert,
   calibration application, and the `_maybe_auto_accept → act_on_recommendation`
   call to `pending_post_commit_actions` rows drained post-commit. Delete the
   module-load reasoning imports at `repo.py:116-118` (and the parallel ones in
   `constructor.py` / `edges_repo.py`).
2. **Embedding lifecycle parity with observations:** add
   `models.embedding_pending` + a provenance/`provider`/`version` marker; compute
   the embedding *before* the transaction (mirroring `ingestion/core.py`); stop
   the applier injecting deterministic lexical vectors into the semantic column —
   mark `pending` instead. One backfill job (Move 4's housekeeper) re-embeds
   both `observations` and `models`; `ops/rebuild embeddings --table models`
   re-embeds everything. (Closes D4.)
3. **Ratchet:** once the imports are gone, add the import-linter contracts
   `services.domain ⊥ services.reasoning` and `services.domain ⊥
   services.product` — now empirically true.

**Risk (verified caveat).** The `pending_post_commit_actions` path currently has
near-no-op handlers, a `CHECK`-constrained action registry, a `NOT NULL`
`trigger_id`, and a per-trigger dedup key shaped for Think. So this is **real
design work, not just wiring** — the post-commit schema and dedup key need a
written design first. Deferring topology/affordance/auto-accept to post-commit
also changes timing: a freshly inserted model briefly lacks its affordance
profile and edges; Think must tolerate that (it already must, since the
post-commit worker can lag today), but it needs explicit eventual-consistency
tests.

### Move 3 — One ingestion path; Kafka becomes a scaling profile

**Closes:** D3, the Kafka-oversizing operational finding, and a large chunk of
the 4 GB process-overcommit.

**Why.** Two convergent paths mean every handler change must be verified twice
forever, and the convergence is already broken (D3). With Postgres+S3 canonical,
Kafka stops being a second source of truth and becomes a scaling lever you can
re-add *per source* without recreating split-brain.

**Steps.**

1. **Unconditional raw capture** (a week). Today raw blobs are durable only when
   the Kafka path or best-effort shadow-write runs, and `observations` has no
   `raw_payload` column — so inline-only providers and any shadow-write failure
   produce evidence that can never be re-normalized. Create `raw_events(uuid7 PK
   as cursor, tenant, source, ingress_kind, s3_key, content_hash, status)`;
   every ingress (webhook, poller, backfill, Discord socket, Gmail watch) calls
   `capture()` first, unconditionally, keeping today's downstream behavior
   untouched. **Value alone:** no ingress is ever unreplayable again; the webhook
   router sheds its conditional shadow-write branch.
2. **Move Gmail canonicalization into the shared draft path** so set-equality
   holds structurally when Kafka is re-enabled (fixes D3).
3. **Demote Kafka to a compose profile.** Move the 6 always-on consumers
   (normalizer, observation_writer, dlq_writer, embedding_worker,
   embedding_backlog, circuit_breaker) behind a `kafka` profile; inline
   `ingest_from_draft` is the default active write path. Keep the S3 raw tap for
   replay durability. Delete `kafka_path_enabled` TenantFlags, the circuit
   breaker, and shadow-write from the default path. Write an ADR superseding
   ADR-0001 (Kafka-first) that documents the re-enable trigger (a second real
   tenant, or sustained volume) and the rule that **`raw_events` remains the
   canonical cursor even when Kafka carries the bytes** — or split-brain returns.

**Risk.** Going 202-async on ingestion changes latency semantics for anything
that assumed synchronous `ingest → observation → trigger` (the inline path
returns the observation today). Add a `raw_events` depth/lag metric to
`/healthz` before any cutover. If the S3 raw tap isn't verified actually landing
envelopes before the default flips, replayability is silently lost — add a
startup/CI smoke check.

### Move 4 — Make the runtime match the tree

**Closes:** D5.

**Why.** Compose is the de-facto truth and it disagrees with the tree, which for
a solo operator is the difference between a 5-minute and a 5-hour incident.
"Implemented" and "operative" have silently diverged, breaking loops the live
code assumes are closed. Every fix here is wiring, deletion, or a CI check —
the scheduler and job bodies already exist.

**Steps.**

1. **One `housekeeper` process.** `scripts/run_housekeeper.py` launches the
   existing `MaintenanceScheduler` (`JobDescriptor` + `pg_advisory_lock`) and
   registers the dormant workers' existing `run_once` functions as jobs:
   `calibration_updater`, `deadline_resolver`, `edge_drift`, `entity_resolver`,
   `anomaly_processor`, and `access_control.materialized.refresh_all`. Replace
   the in-memory `_DIRTY` set with a small durable `acl_dirty` table. One
   container (~250 MB) closes four live functional gaps on the 4 GB box.
   *(Verified caveat: two of these packages lack a `run_once` entry point today —
   estimate accordingly.)*
2. **Greeting scheduler → housekeeper.** Move `GreetingScheduler` out of the
   gateway lifespan (its four loops become one leased job under a
   `pg_advisory_lock`); the gateway only reads `view_ceo_cache`. Set
   `GATEWAY_START_GRT_SCHEDULER=0`. Fixes P0-4 (N× duplicate LLM renders per
   replica; CEO-home refresh dying with the HTTP process) and shrinks the
   330-line lifespan by its largest phase. *(Verified caveat: the scheduler has
   an in-process stream-publisher coupling; the move needs a NOTIFY-based bridge
   so realtime updates still fire.)*
3. **Delete verified-dead weight.** Migration dropping `signal_routing_decisions`
   and `topo_dirty_queue`; delete `platform/execution/routing.py` + contracts +
   the debug-router reader; `rm` the legacy `services/{gateway,models,think,query}`
   residue dirs; stop committing `docker-compose.per-source.yml` (keep the
   generator). Platform shrinks to `access_control` + process manifest.
4. **Add memory limits and a default profile** to `docker-compose.yml`: infra +
   gateway + `think_worker` + `post_commit_worker` + `embedding_backlog` +
   `housekeeper` + the live sources actually in use; everything else behind
   profiles.

**Risk.** The housekeeper concentrates 6+ jobs in one process on a
memory-starved box — a leaking or LLM-looping job (`entity_resolver`) can starve
the rest. Per-job `try/except` + advisory locks isolate failures, but per-job
intervals, an LLM-call cap for `entity_resolver`, and a container `mem_limit` are
required from day one. Forward-only drops are one-way; grep coverage for external
readers (dashboards, `scripts/check_schema_drift.py`) is good but not proof.

### Move 5 — Ratchet every dirty edge in CI

**Closes:** the cross-cutting enforcement gap; locks in Moves 1–4 so they cannot
regress.

**Why.** This is the highest-leverage change for an agent-maintained codebase: it
converts the `CODEBASE-MANAGEMENT.md` §8.1 "tracked debt" prose into a mechanical
scoreboard at near-zero cost. Without it, every decoupling above can silently
re-break the moment an agent adds a convenient import.

**Steps.**

1. **`forbidden`-with-allowlist contracts** for every dirty edge: domain →
   reasoning, domain → product, ingest → app, lib → services, and deep-sage /
   deep-retrieval imports (everything except published facades). Each contract
   gets an `ignore_imports` allowlist naming today's *exact* offenders. CI hard-
   fails on any new entry; each move above deletes allowlist lines.
2. **`services/domain/triggers.py`** — a single `enqueue_think_trigger(conn,
   ...)` helper owning the kind/payload/dedup/`scheduled_for`/batch-column
   contract. Port the 11 raw `INSERT INTO think_trigger_queue` sites to it; move
   `edge_registry.py`'s `model_reeval_queue` SQL behind it too (so lib stops
   writing reasoning queues). Add a CI grep banning the raw insert outside the
   helper. Add one SQL view unioning depth/lease-age across queue tables for the
   operator. (Closes D6. Domain is the one package everything already imports, so
   the helper adds zero new edges — explicitly *not* a queue framework.)
3. **`git mv` `inquiry.py` into `reasoning/execution/`** with a re-export shim.
   It is 9,367 lines of retrieval orchestration misfiled under `platform`, and
   `reasoning/think/context_planner.py:28` imports it back — a genuine
   cross-package module-load cycle. Moving the file makes the cycle same-layer
   (which current doctrine accepts) without touching its contents. Add
   `reasoning/sage/api.py` exporting the ~5 externally-consumed symbols and
   forbid deeper sage imports via the ratchet.

**Risk.** Ratchet decay: with one founder the temptation is to grow allowlists
instead of fixing edges. The tooling can hard-fail on *additions* but cannot
force *shrinking* — that is process discipline. Re-homing the 9,367-line
`inquiry.py` must pass the throwaway-DB runtime gate before merge, and the shim
stays until the allowlist shows zero old-path imports (any missed dynamic import
breaks Think and Query simultaneously).

---

## 5. Sequencing

Each step merges to `main` alone and delivers value alone. Ordered by
leverage-per-risk, front-loading the zero-behavior-change and do-regardless work.

| # | Step | Closes | Effort | Risk |
|---|---|---|---|---|
| 1 | Ratchet contracts + delete dead code (routing gate, `topo_dirty_queue`, residue dirs, per-source compose) + **R1 header redaction** + **R2 render guard** | D5 (partial), enforcement gap, R1, R2 | 1–2 days | None (deletions + lints) |
| 2 | Housekeeper process (wire 6 dormant jobs + durable `acl_dirty`) | D5 | 2–3 days | Low (memory budget) |
| 3 | `applied_diffs` ledger + `ops/rebuild memory --dry-run` | D1 (audit half) | days | None |
| 4 | ModelsRepo side-effect inversion + `models.embedding_pending` + pre-txn embedding | D2, D4 | ~1 week | **Semantic** — design post-commit schema first |
| 5 | Greeting scheduler → housekeeper + NOTIFY stream bridge | D5, P0-4 | 2–3 days | Low–med (stream coupling) |
| 6 | `domain/triggers.py` across 11 sites + queue-depth view | D6 | days | Low |
| 7 | Domain command seam for `decision_deltas` / `recommendations` | D1 (write half) | days | Low |
| 8 | `git mv inquiry.py` → `reasoning/execution` + sage facade | enforcement gap | days | Med (large file, shim) |
| 9 | Unconditional raw capture (`raw_events`) + `/healthz` lag metric | D3 (prereq) | ~1 week | Med (async semantics) |
| 10 | Gmail canonicalization into shared path + Kafka → compose profile + ADR-0004 + R3 ceilings | D3, Kafka oversizing, R3 | ~1 week | Med (ADR reversal) |

The plan is safe to **stop after step 6**: that leaves the layering fixed, the
runtime honest, the worst stability risk closed, and every edge ratcheted —
without touching the two highest-risk surfaces (ingestion semantics, product
write paths). Steps 7–10 are then a separate, re-priced decision.

---

## 6. What we deliberately will NOT do

The adversarial review flagged these as changes the recorded pain does not
justify. They are out of scope; revisit only with fresh justification.

- **No directory re-layering** into `kernel/adapters/connectors` (hexagonal) or
  `slices/` (vertical). The 2026-06 re-layering cost ~4,169 reference rewrites
  and left 30+ zombie dirs; the likely outcome of another rename is a
  half-migrated tree with three vocabularies — slower for an agent than today.
  **Change the import graph first; rename last, if ever.**
- **No 25-connector SDK rewrite.** The duplication estimates did not survive
  spot-checking (off by ~2–4×). Build the chassis for the *next new* source,
  backport one as proof, and measure before committing to a fleet rewrite.
- **No Ask/Query/Conversations merge yet.** Three QA stacks binding three
  different deep reasoning internals is real debt, but the minimum fix is a
  single retrieval facade + freezing Query. Merging the founder's two daily-use
  surfaces is the riskiest place to spend budget. Contain via the sage/retrieval
  facade now; unify only when Query is actually retired from the UI.
- **No full event-sourcing** (replaying LLM calls from raw blobs). It is
  nondeterministic and expensive. Deterministic replay of the `applied_diffs`
  log is the honest version of "rebuildable."
- **No diff-synthesis for product actions** beyond the domain-command seam, until
  pain recurs.
- **No drop of the `supporting_model_ids` / `model_edges` dual-write** mid-reorg.
  Wire `edge_drift` via the housekeeper (step 2), observe 14 clean days, then
  drop the arrays as its own isolated change.
- **No unified queue framework.** `domain/triggers.py` + one depth view is the
  whole intervention; nine queues do not need one abstraction.

---

## 7. How this plan was produced (methodology)

A multi-agent workflow over the core backend (tests, benchmarks, simulation, UI,
and legacy residue excluded):

1. **Map** — 12 parallel readers, one per area (app, product, reasoning, ingest,
   domain, platform, workers, lib, db schema, data plane, cross-layer dependency
   audit, design history), each producing structured pain points grounded in
   file paths.
2. **Diagnose** — 3 cross-cutting analysts (boundaries, data-flow, operational)
   re-verified the load-bearing claims against source.
3. **Design** — 4 independent architects produced competing redesigns from
   distinct lenses (event-log-first, hexagonal kernel, vertical slices,
   pragmatic minimum), all constrained to the existing building blocks.
4. **Judge** — 3 adversarial critics per proposal (migration realism, pain-fit,
   complexity budget) scored each.

**Convergence signal.** All four independent architects, from different starting
philosophies, proposed the **same first moves**: a single trigger-enqueue API,
ModelsRepo side-effect inversion, the embedding lifecycle fix, the scheduler out
of the gateway, the `inquiry.py` relocation, and the CI ratchet. The critics'
strongest consensus was *against* the larger structural rewrites (directory
re-layering, connector SDK, QA merge). This plan is the pragmatic skeleton
carrying the event-log proposal's two cheapest high-value ideas (the
`applied_diffs` ledger and unconditional raw capture).

Proposal scores (feasibility / value, 1–10):

| Proposal | Feasibility | Value |
|---|---|---|
| Pragmatic minimum | 7–8 | 8 |
| Event-log ledger | 5–7 | 7 |
| Hexagonal kernel | 6–7 | 7–8 |
| Vertical slices | 6–7 | 7–8 |
