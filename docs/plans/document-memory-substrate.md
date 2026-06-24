# Document Memory Substrate — Layer 0 + Layer 2

- **Status:** DRAFT (spec + implementation plan; no code yet)
- **Branch:** `feat/document-memory-substrate` (off `origin/main` @ `49bdbf8`)
- **Author/date:** 2026-06-24
- **Scope:** Make ingested documents *remembered* by the reasoning engine — by (0) keeping the structured extraction the summarizer already produces, and (2) turning that extraction into durable **Models** the existing retrieval pathways recall.

---

## 1. Problem & scope

Today a large document (Drive PDF, Notion page, Fireflies transcript) is summarized **once** at
ingest to a ~1800-char flattened blob in `observations.content_text`; the structured fields the
summarizer extracts (`decisions / action_items / risks / key_points`) are **discarded**, the full
text is stranded in S3 (read only by the summarizer), and the observation's embedding is written to
a **dead** search index. When reasoning later runs, the meeting is effectively forgotten: nothing
durable, scoped, or graph-linked survives in the **Models** substrate that retrieval actually
recalls.

This plan implements the substrate spine of the four-layer design:

- **Layer 0 — stop discarding the structured summary.** Persist `decisions/commitments/risks/...`
  verbatim, and map-reduce very large docs so fidelity doesn't depend on one context window.
- **Layer 2 — make the document a first-class memory object.** Distill the structured summary into
  durable Models (a `situation` anchor + `prediction`/`concern`/`recommendation` claims), scoped to
  the document's entities/actors and linked into the graph, so Pathways A/B/G recall them like any
  other belief.

**Non-goals (explicitly deferred):**
- **Layer 1** — chunked RAG over retained S3 text (verbatim drill-down).
- **Layer 3** — agentic recall (giving the think LLM retrieval tools).
- Generalizing beyond the current allowlist (`google_drive:file, notion:object, fireflies:transcript`).

**Key property:** the core of 0 + 2 is **migration-free** — `content` is JSONB, the Models substrate
already exists, and we deliberately reuse existing `claim_role` values and provenance fields. The
only schema-ish/ops work is optional (deploying `deadline_resolver`).

---

## 2. Current-state contracts (verified on this branch)

### 2.1 Summarizer pipeline
| Concern | Location | Note |
|---|---|---|
| Structured schema | `services/ingest/ingestion/summarization/llm.py:33-38` | `DocumentSummarySchema{summary, key_points[], decisions[], action_items[], risks[]}` |
| Flatten + discard | `llm.py:55-66` (`render_summary`) | Collapses schema → one capped string; **structured fields dropped** |
| Single LLM call (live) | `llm.py:120-141` (`LLMSummarizer.summarize`) | one `provider.structured(...)` over full text; **no chunking** |
| Single LLM call (batch) | `summarization/batch_api.py:73-111` (`build_batch_request_line`) | same prompt/schema/`max_tokens=1200` |
| Result type | `SummaryResult{summary_text, model}` | only the flattened string survives |
| Write-back (shared) | `writers/summarization_worker/summarization_worker.py:127-161` (`_write_summary_and_enqueue`) | sets `content_text`, `content.summarization.*`, `summary_provenance`, `embedding=NULL`, `embedding_pending=TRUE`, enqueues T1 |
| Shared apply (both lanes) | `apply_summary_to_observation` (worker) — imported by batch worker `summarization_batch_worker.py:39-41`, applied at `:453-491` | **live + batch share the write path** ✅ |
| Ingest decision / pending shape | `services/ingest/ingestion/core.py:180-217` (`_prepare_document_summarization`) | threshold `INGEST_DOCUMENT_SUMMARY_THRESHOLD_CHARS=8192`; allowlist `INGEST_DOCUMENT_SUMMARY_CHANNELS`; sets `content.summarization={status:"pending",...}` |
| Source-text recovery | `summarization/source_text.py` | S3 → inline JSONB → content fallback |

`content.summarization` shape today — **pending:** `{status, reason, original_chars, raw_s3_key,
ingress_kind, source_channel, model, source_text?}`; **complete:** adds `{completed_at, model,
summary_chars, source_chars}`, drops `source_text`, and sets sibling `content.summary_provenance =
"llm_summarizer"`.

### 2.2 Models substrate
| Concern | Location | Note |
|---|---|---|
| Create payload | `lib/shared/types.py:320-346` (`ModelCreate`) | required: `tenant_id, born_from_event_id, proposition{kind}, natural, embedding, scope_temporal, confidence, confidence_at_assertion` |
| Insert pipeline | `services/domain/models/repo.py` (`_insert_core`, 9 steps) | falsifier adequacy (if `confidence>0.7`), proposition validation, calibration, confidence clip [0.05,0.95], **scope-actor existence check**, embedding compute, INSERT, `state_change` emit |
| Constructor | `services/domain/models/constructor.py:115-220` | normalizes proposition, derives memory grammar, normal-form rules (atomic vs composite; `situation` ⇒ `abstraction_level='composite'`) |
| Grammar (claim_role) | `lib/shared/memory_grammar.py:15-54` | `ClaimRole = Literal[fact, concern, hypothesis, prediction, pattern, situation, capability, relation, recommendation]` |
| claim_role storage | `db/migrations/0047/0048` | **GENERATED column + CHECK** → new values need a migration (we avoid) |
| Edges | `services/domain/models/edges_repo.py` (`link`), registry `lib/shared/edge_registry.py:330-480` | kinds incl. `supports, instance_of, explains, causes, blocks, contradicts, relates/…`; **`derived_from` does NOT exist** |
| Retrieval pathways | `services/reasoning/retrieval/pathways.py` | A structural (scope GIN), B semantic (ANN over `models.embedding`, HNSW partial `status='active'`), G edges |
| Proactive deadline | `services/workers/deadline_resolver/worker.py` (+ `evaluators.py`) | polls `models WHERE evaluate_at <= now()` → enqueues T2; **NOT in docker-compose** |
| Entity/actor resolution | `core.py:386-387,451-473` (`_resolve_entities` over `draft.content_text`; `ActorRepo.resolve_by_source_actor_ref`) | runs at ingest; for large docs `content_text` is the placeholder ⇒ `entities_mentioned` unreliable |

---

## 3. Layer 0 spec — keep what the summarizer already extracts

### 3.1 Persist the structured summary (low-risk, both lanes)
- Extend `SummaryResult` to carry the parsed `DocumentSummarySchema` (or its `.model_dump()`),
  **in addition to** the flattened `summary_text`. (`llm.py` `summarize` + `parse_summary_text`.)
- In `_write_summary_and_enqueue`, write the structured payload into the observation:
  ```jsonc
  content.summarization.structured = {
    "summary": "...",
    "key_points":   [...],
    "decisions":    [...],
    "action_items": [...],   // each item: keep owner/due if the model emitted them
    "risks":        [...]
  }
  ```
  Keep `content_text = render_summary(...)` unchanged (still the short brief used for prompt
  rendering). Structured fields are stored **unflattened** — this is the data Layer 2 consumes.
- Batch lane needs **no extra wiring** beyond extending the shared `parse_summary_text` /
  `SummaryResult` (it already calls `apply_summary_to_observation`).
- *Optional* schema tightening: change `action_items: list[str]` →
  `list[{who?, what, due?}]` so commitments carry owner/deadline. The prompt already asks for owners
  and dates; making the schema structured improves Layer 2's commitment Models. Keep backward-compat
  parsing (accept bare strings).

### 3.2 Map-reduce for very large documents
- Add a shared helper `summarize_mapreduce(source_text, metadata, *, provider, max_chars)`:
  - **map:** split `source_text` into sections (token/char-bounded, e.g.
    `INGEST_SUMMARY_MAPREDUCE_CHARS` ~24k with overlap), summarize each into a partial
    `DocumentSummarySchema`.
  - **reduce:** merge partials (concat key_points/decisions/etc., dedup) and run one final reduce
    pass into a single `DocumentSummarySchema`.
  - Engage only when `len(source_text) > INGEST_SUMMARY_MAPREDUCE_CHARS`; below it, the current
    single call is unchanged.
- **Live lane:** call the helper from `LLMSummarizer.summarize`.
- **Batch lane caveat:** the OpenAI Batch API is one request line per item, so true map-reduce there
  is multi-stage and awkward. **Phase 1:** run map-reduce only in the synchronous helper; for batch,
  raise the single-call input cap and log when an item exceeds it (no silent truncation). **Phase 2
  (optional):** model batch map-reduce as queued sub-requests + a reduce pass. Flag in OPEN DECISIONS.

### 3.3 Env knobs (new)
- `INGEST_SUMMARY_MAPREDUCE_CHARS` (default ~24000) — map-reduce trigger.
- `INGEST_SUMMARY_SECTION_CHARS` / `_OVERLAP` — section sizing.
- (existing `INGEST_SUMMARY_MAX_CHARS=1800` still caps `content_text`.)

---

## 4. Layer 2 spec — distill the document into durable Models

### 4.1 ⚖️ DECISION D1 — who mints the Models? (the pivotal call)

**Context that forces the decision:** `ModelsRepo.insert` is **only ever called from Think today**.
Minting at ingest must satisfy `insert()` invariants manually (falsifier adequacy, scope-actor
existence, embedding present) **and** resolve scope — but the observation's `entities_mentioned` is
unreliable for large docs (resolved over the placeholder). Think already owns calibration, edge
mining, contestation, and entity context.

**Option A — Think-mediated (RECOMMENDED).** The summary worker doesn't create Models. Instead it
(1) re-resolves entities/actors over the structured summary and (2) enqueues an *enriched* T1 so
Think distills the document into Models via its sanctioned path.
- *Pros:* reuses the only blessed Model-creation path; proper calibration/falsifiers/edges/scope;
  no first-ever non-Think `insert` caller; resilient to the entity-resolution gap.
- *Cons:* less deterministic (depends on a Think run); requires touching the **reasoning layer**
  (think representation contract + prompt) so Think reliably emits document-memory Models.

**Option B — Direct mint at summarization.** The worker calls `ModelsRepo.insert` itself.
- *Pros:* deterministic, immediate, document Models exist right after summarization.
- *Cons:* fights the architecture (skips Think's machinery); must hand-satisfy every `insert`
  invariant; duplicates resolution; no precedent.

**Option C — Hybrid (pragmatic).** Directly mint only the **low-risk `situation` anchor Model**
(confidence ≤ 0.7 ⇒ **no falsifier required**, scope from re-resolved entities, embedding
precomputed) for an immediate, deterministic doc-memory object; let **Think** mint the sharp
`prediction`/`concern`/`recommendation` claims (calibration + deadlines belong there).

**Recommendation: A (Think-mediated).** *(Revised from an earlier C/hybrid lean.)* The decisive
reason is that Think creates Models **in retrieval context**, so a claim is deduped/updated/contested
against existing Models — minting without that context (B, and C's anchor) is the core anti-pattern:
a second meeting restating the Acme SOW should *update* the commitment Model, but an ingest-time mint
has no retrieval context and blindly inserts a duplicate. C's "deterministic anchor day one" is a
blind, edge-less, possibly-duplicate `situation` Model that Think must enrich anyway — yet C still
pays the full cost of being the first non-Think `insert` caller (wiring ModelsRepo + embedder + scope
into the worker). So A is both cleaner and safer: single Model-author, ingest stays a data pipeline,
native calibration/edges/falsifiers.

A's only weakness — "least deterministic" — is small and cheaply mitigated: the worker **already
enqueues a T1** post-summary (Think is guaranteed to run), so the risk reduces to "will Think emit
good claim_ops," handled by the representation contract + golden tests + a `doc → models_minted`
metric with T1 replay. Pick **C** only if the Think-contract change is too slow to land and an
immediate recallable artifact is required — but Phase 0 (structured persistence) is the better
"ship sooner" lever, so a stopgap anchor isn't needed.

Under A, the separate direct-mint anchor phase collapses into the Think-mediated phase: §4.2–§4.6
describe the claim_role mapping, scope resolution, provenance, and deadlines that **Think** applies;
the ingest side only re-resolves scope (§4.3) and enriches the T1.

### 4.2 claim_role mapping (no new values, no migration)
| Extracted item | `proposition.kind` | `claim_role` | Extra |
|---|---|---|---|
| document anchor | `belief` | `situation` (⇒ `abstraction_level='composite'`) | members = the claim Models |
| commitment / action_item w/ due | `prediction` | `prediction` | `evaluate_at = due`, deadline `falsifier`, `resolution_criteria` |
| risk | `belief` | `concern` | `polarity='negative'` |
| decision | `belief` | `recommendation` (or `fact`) | `time_mode='current'` |
| key_point (factual) | `belief` | `fact` | optional; may be noise — gate by salience |

`natural` = the item text (well-phrased for embedding recall — see §4.5). Grammar axes are derived
by the constructor; we set explicit axes only where they matter (e.g. `polarity` for risks).

### 4.3 Scope resolution (the hard part)
At summary-land time, **re-resolve over the structured summary** rather than trusting the
observation:
- Reuse `EntityAliasRepo.fast_path_resolve_many(candidate_phrases(structured_text), tenant_id)` and
  `ActorRepo.resolve_by_source_actor_ref` (the same helpers `core.py` uses) against the concatenated
  structured fields (decisions/commitments/risks), not the placeholder `content_text`.
- Update `observations.entities_mentioned` with the richer resolution (also benefits any reactive
  Pathway-A recall of the observation itself).
- Build `scope_entities` from resolved refs (`[{"type","id"}, ...]`) and `scope_actors` from resolved
  actor UUIDs **only** (insert validates actor existence — never invent IDs; unresolved actors stay
  as text in the proposition/`natural`, not in `scope_actors`).
- `scope_temporal = {valid_from: occurred_at, valid_until: null}`; for commitments, the due date
  drives `evaluate_at`.

### 4.4 Provenance & edges (avoid a new edge kind)
- **Provenance is free:** set `born_from_event_id = observation_id` and
  `supporting_event_ids = [observation_id]` on every document-derived Model. That already encodes
  "this came from document X" without any new edge kind. (`derived_from` does **not** exist; do **not**
  add it in this phase.)
- **Cross-claim edges:** link risk↔decision / anchor↔claims using **existing** kinds
  (`explains`, `supports`, `instance_of`, `relates`/`co_occurs_with`) via `EdgesRepo.link(conn, ...,
  detected_by="document_summarization")` inside the same transaction.
- If a true `derived_from` semantic is wanted later, register it in `edge_registry.py` (+ verify any
  edge-kind CHECK) — **out of scope here.**

### 4.5 Mint-time retrievability contract (so Models are *born recallable*)
A document Model is only as retrievable as what we give it (see Pathways A/B/G):
- **Pathway A** needs accurate `scope_entities` / `scope_actors` (§4.3).
- **Pathway B** needs a `natural` phrased like a claim ("Priya to send Acme the revised SOW by
  2026-06-17"), not a fragment — embed `natural`, not raw bullet text.
- **Pathway G** needs the cross-claim edges (§4.4).

### 4.6 Proactive deadline firing (commitments)
- A commitment minted as `prediction` with `evaluate_at = due` + deadline `falsifier` +
  `resolution_criteria` is picked up by `deadline_resolver` → T2 `prediction_overdue` when overdue —
  i.e. the system flags the overdue SOW **without** waiting for a nudge.
- **Dependency:** `deadline_resolver` is **not in docker-compose**. Proactive firing requires adding
  it to the compose/worker fleet. Until then, commitments are still **reactively** recalled — just not
  proactively fired. Tracked in OPEN DECISIONS / ROLLOUT.

---

## 5. Worked example (Acme transcript → June 24 trigger)

1. **Ingest (June 3):** 9k-word Fireflies transcript → S3; `content.summarization = pending`.
2. **Layer 0:** map-reduce summarizes section-by-section (the minute-31 SOW line survives); shared
   apply writes `content_text` (brief) **and** `content.summarization.structured` with
   `decisions=["ship billing revamp before Sept 30"]`,
   `action_items=[{who:"Priya", what:"send Acme revised SOW", due:"2026-06-17"}]`,
   `risks=["SOC2 slip endangers renewal"]`.
3. **Layer 2 scope:** worker re-resolves over the structured text → entity `Acme`, actor `Priya`
   (if resolvable); updates `entities_mentioned`.
4. **Layer 2 mint (Think-mediated, ratified A):** the worker re-resolves scope and enqueues an
   enriched T1; **Think** mints a `situation` anchor Model plus a `prediction` commitment
   (`evaluate_at=2026-06-17`, deadline falsifier), a `concern` risk, and a `recommendation` decision —
   all Acme-scoped, edge-linked, `born_from_event_id`/`supporting_event_ids=[obs]` — deduping against
   any existing Acme Models in the same retrieval context.
5. **June 17 (proactive, if `deadline_resolver` deployed):** commitment is overdue → T2 fires
   *before* anyone asks.
6. **June 24 trigger** ("Acme asking where the SOW is"): Pathway A (scope=Acme) + Pathway B
   (semantic) surface the commitment/risk/decision Models; Pathway G pulls the neighborhood;
   reconsolidation bumps activation. Reasoning answers with the overdue commitment, the renewal
   linkage, and the SOC2 risk. **Remembered.**

---

## 6. Schema / migration impact

- `content.summarization.structured` — **JSONB, no migration.**
- claim_role mapping — reuses existing values ⇒ **no grammar migration.**
- Provenance via `born_from_event_id` / `supporting_event_ids` — existing columns ⇒ **no migration.**
- No `derived_from` edge kind in this phase ⇒ **no edge migration.**
- `deadline_resolver` in compose — **ops change**, not a DB migration.

Net: **the substrate ships migration-free.** (Only optional later work — a `derived_from` edge kind
or a new claim_role — would touch migrations.)

---

## 7. Implementation phases

**Phase 0 — Layer 0 (prerequisite, low risk).**
1. ✅ **DONE** — Extend `SummaryResult` + `parse_summary_text`/`summarize` to retain the parsed
   schema. (`llm.py`) — commit `<this branch>`; carried by both live + batch lanes.
2. ✅ **DONE** — Persist `content.summarization.structured` in `_write_summary_and_enqueue`.
   (`summarization_worker.py`)
3. (Optional) structured `action_items` schema with back-compat. (`llm.py`)
4. Map-reduce helper + live-lane wiring + batch input-cap guard + new env knobs. (`llm.py`,
   `batch_api.py`)
5. ✅ unit tests added (`summarization/tests/test_llm.py`); still TODO: DB-level persistence test
   (live+batch), map-reduce fidelity, threshold unchanged.

**Phase 1 — Layer 2 via Think (ratified A).**
6. Re-resolution helper over the structured text; update `entities_mentioned`. (worker + reuse
   `core.py` resolvers — factor shared helpers out of `core.py`.)
7. Enriched T1 payload carrying the structured extraction; surface it to Think's context builder.
   (`summarization_worker` trigger enqueue + `services/reasoning/think/*` context)
8. Think representation-contract/prompt: recognize "document structured summary" evidence → emit a
   `situation` anchor + `prediction`/`concern`/`recommendation` claim_ops with deadlines + edges,
   deduping against retrieved Models. (`services/reasoning/think`, `edge_intelligence` if relevant)
9. Feature-flag the Layer-2 path (`INGEST_DOC_MEMORY_ENABLED`, default off).
10. Tests: a think run over a doc observation produces the expected anchor + claim Models + edges;
    commitment carries `evaluate_at` + falsifier; claims are retrievable by Pathway A/B.

**Phase 2 — Proactive + observability.**
11. Add `deadline_resolver` to docker-compose; verify T2 fires on an overdue commitment.
12. Metrics: `doc_memory.models_minted`, `doc_memory.scope_unresolved`, map-reduce section counts;
    dashboards/alerts.

---

## 8. Invariants & edge cases to respect
- **Idempotency:** apply path is re-run-safe today (status guard). A re-fired T1 must not duplicate
  Models — Think dedups in retrieval context (and may key on `born_from_event_id`).
- **Falsifier adequacy:** Think supplies proper falsifiers; the `situation` anchor stays
  `confidence ≤ 0.7` to avoid the falsifier requirement, `prediction` commitments carry a deadline
  falsifier.
- **Scope-actor existence:** only resolved actor UUIDs go in `scope_actors`; unresolved → text only.
- **Both lanes:** every Layer-0/2 change must hold for live **and** batch (shared apply path —
  verify in tests).
- **Failure isolation:** a Model-mint failure must **not** fail summarization (the brief + embedding
  must still land). Mint errors → metric + log, summary succeeds.
- **Dedup / re-summarize:** re-summarizing an observation must not duplicate Models (key on
  `born_from_event_id` + role/text hash).
- **Noise control:** gate `key_point`→`fact` Models (and low-salience items) to avoid flooding the
  Models table; prefer decisions/commitments/risks.
- **Entity-resolution gap:** anchor is still useful with empty scope (Pathway B semantic recall);
  don't hard-require scope.

## 9. Testing
- Unit: schema retention, `render_summary` unchanged, map-reduce merge/dedup.
- Integration (live + batch): structured JSONB persisted; anchor Model minted, scoped, embedded.
- Retrieval: seed a think-retrieval over the doc's entity → Pathways A/B return the doc Models
  (mirror existing `retrieval/tests`).
- Think (Phase 2): doc observation → expected claim Models + edges + commitment `evaluate_at`.
- Proactive (Phase 3): overdue commitment → `deadline_resolver` enqueues T2.
- DB-gated tests follow the throwaway-pgvector recipe (see memory: *running-signal-source-tests*).

## 10. Rollout, flags, observability
- `INGEST_DOC_MEMORY_ENABLED` (default **off**) gates all of Layer 2; Layer 0 persistence is safe to
  enable first (additive JSONB).
- Stage: enable Layer 0 → backfill-spot-check structured fields → enable the Think-mediated Layer 2
  on one tenant → deploy `deadline_resolver`.
- Metrics per Phase 2; alert on `doc_memory.mint_failure` and scope-unresolved rate.

## 11. Open decisions & risks
- **D1 (pivotal): RATIFIED 2026-06-24 → A (Think-mediated).** Rationale in §4.1. The separate
  direct-mint anchor phase is dropped; the `situation` anchor + claim Models are all minted by Think.
- **D2:** batch-lane map-reduce now (multi-stage) vs input-cap guard now + defer (recommended).
- **D3:** structured `action_items` schema change now vs keep `list[str]` and parse owner/due
  heuristically.
- **D4:** ship a `derived_from` edge kind, or rely on `born_from_event_id`/`supporting_event_ids`
  (recommended: rely on provenance fields this phase).
- **Risk:** `deadline_resolver` not deployed ⇒ no proactive firing until Phase 2 (reactive recall
  still works).
- **Risk:** entity resolution weak for large docs ⇒ scoped recall degrades to semantic-only; mitigated
  by §4.3 re-resolution + Pathway B.
- **Risk:** Model-table volume if key_points minted indiscriminately ⇒ salience gating (§8).

## 12. Build order & rough size
1. **Phase 0 (Layer 0)** — small, additive, migration-free. *Highest value-to-risk; in progress (0.1–0.2 done).*
2. **Phase 1 (Layer 2 via Think)** — largest; scope re-resolution + enriched T1 + Think contract.
3. **Phase 2 (proactive + obs)** — small/ops.

Layer 1 (chunked drill-down) and Layer 3 (agentic recall) build on top of this substrate later.
