# Summarize-on-Ingest — final implementation note

> Branch: `feat/summarize-on-ingest`. This note is the PR description + the
> pre-implementation gate artifact required by the task. It records the verified
> code anchors, the contradictions found against the spec, the resolutions
> chosen, the files to touch, and the per-invariant preservation plan.

## Intent

Large structured documents (Drive PDFs, Notion pages, Fireflies transcripts)
today collapse into one observation whose `content_text` is lossily truncated
(Drive 64 KB, most handlers ~600 chars) and cut again to 1,500 chars at prompt
assembly. A 180-page design doc's reasoning run sees only its title + opening.
We fix the actual broken thing: **the reasoning engine cannot see the
document's meaning.**

Decision (fixed): compress the document to fit the ~1.5–2 KB observation ceiling
by **summarizing** (not chunking). One document = one observation = one T1 run.
S3 holds fidelity (raw bytes + full extracted text); Postgres holds meaning (the
lossy reasoning-brief summary as `content_text`). The only natural "Phase 2"
after this is chunked retrieval over retained S3 text, but that is a separate
retrieval feature, not required for summarize-on-ingest to be complete.

## Verified anchors (spec matches code)

- S3 offload: `shadow_write_raw` + `S3Client.put_if_absent(key, body)` idempotent
  via `IfNoneMatch='*'` (`raw_tier/s3.py:148`). Reusable for a `.txt` key.
- Dedup (IDEM1): `ON CONFLICT (tenant_id, source_channel, external_id, occurred_at)
  DO NOTHING` + SHA256 advisory lock (`core.py:444`).
- T1 enqueue: `core.py:518`; **`enqueue_trigger=False` already exists** (`core.py:156`)
  → deferring T1 is feasible.
- Embedding-worker pattern fully mirror-able (topic_for / consumer_group / getmany
  + commit / publish_dlq / compose).
- Prompt truncation: `_PER_ITEM_CHAR_LIMIT=1500`, `_OBS_CHAR_BUDGET=4000`
  (`prompt.py:72`/`:55`, truncation `:1108`). A good `content_text` fixes reasoning
  with no prompt change.
- Retrieval ANN only over `models` (`pathways.py:1914`); observation/chunk ANN is
  dead/test-only → chunked retrieval is a separate future retrieval feature.
- Extraction lib: **pypdf (BSD-3-Clause)** is the only PDF lib — no AGPL exposure.

## Contradictions found → resolutions (decided)

1. **Summarizer models `gpt-5.4-mini`/nano/`gpt-5.4` do not exist** in
   `MODEL_PRICING`/`MODEL_TIMEOUTS`, and `LLM_PROVIDER=codex` rejects non-codex
   models. → **Use `gpt-5.3-codex-spark` at `reasoning_effort=low`** (already in
   the pricing/timeout tables — no table change needed), swappable via config.
2. **`reasoning.effort: "none"` invalid** (codex allows `low|medium|high|xhigh`).
   → **Use `low`.**
3. **No Batch API path existed.** → **Built now**, with latency reconciled below:
   Batch (≤24 h, 50% off) for the **backfill** lane; synchronous
   `gpt-5.3-codex-spark@low` for the **live/poll** lane so deferred-T1 stays fresh.
4. **Fireflies native summary is empty in our fixtures** (evidence was a mock, not
   the live API). → **Fix the fetch path:** expand the GraphQL `summary` field set,
   promote the native summary into `content_text`, populate mock/fixtures, and
   fall back to the LLM summarizer when null.
5. **No streaming PDF extraction; pypdf buffers; >10 MB skip + 64 KB cap.** →
   bounded **page-by-page iteration** + raise caps (configurable). "Streaming" is a
   misnomer for PDF (needs the xref/full buffer); intent = bounded memory + full
   text.
6. **Invariants `DLQ1`/`DG1` do not exist.** Real set: N1, D1, IDEM1 + F1, F2, F3.
   We preserve the real ones (below) and treat DLQ discipline via WireFailureKind.
7. **#46 still live** (`observation_writer.py:606` uses invalid
   `writer.full_mode_permanent_failure` → silent DLQ drop). → **Fix it** +
   regression test, and add a new valid `summarization.llm_failure`.
8. **Harness path** is `services.ingest.synthetic.validation_runs.runner` (not
   `services.synthetic…`); suite is **~4,924 tests** (not 555). Bar: no regressions.

## Batch-API latency reconciliation

OpenAI Batch API completes within ≤24 h. Since T1 is fired strictly after the
summary exists, batching live docs would delay their reasoning up to 24 h. So:

- **Live / poll ingress → synchronous** `gpt-5.3-codex-spark@low` (fresh T1).
- **Backfill ingress → Batch API** (bulk, 50% off, 24 h acceptable).

The summarizer worker routes on `ingress_kind`. Backfill requests are durably
queued in Postgres, submitted to OpenAI Batch, polled by a singleton batch
worker, then applied through the same summary-update/T1/embedding flow as live
summaries. This honors "build Batch now" while keeping live reasoning fresh.

## Per-invariant preservation

- **N1** (publish→flush→advance): summarization is downstream of `normalized`;
  observation write + deferred-T1 stay transaction-safe; no cursor advances
  without the observation.
- **D1** (Google DWD): untouched (Drive keeps Gmail's DWD auth).
- **IDEM1**: observation dedup unchanged; summary re-runs idempotent via
  `UPDATE ... summarization.status <> complete`; the summary update and deferred
  T1 insert share one transaction, so redelivery produces a guard no-op and
  still exactly one T1.
- **F1/F2/F3**: backfill-always-persists, shadow-drop audit, poison cap unchanged;
  summarizer worker adopts F3-style terminal-error → DLQ.

## Components & files

1. **DLQ** — `dlq/models.py` (+`summarization.llm_failure`), fix `observation_writer.py:606` (#46).
2. **Observation shape** — `content_text=summary`; `content` JSONB +`extracted_text_s3_key`,
   `raw_s3_key`, `is_document`, `text_yield`, `needs_multimodal`, summary provenance.
3. **S3 full-text offload** — `raw_tier/s3.py` extracted-text key variant.
4. **Extraction** — `fetchers/google_drive.py`, `integrations/google_drive/client.py`:
   bounded page iteration, raise caps, capture `text_yield`/`needs_multimodal`.
5. **Document gate** — `core.py`: `is_document AND body>8KB → enqueue_trigger=False`;
   structured → offload only; small → unchanged.
6. **Summarizer pipeline** — new `ingestion/summarization/{models,publish}.py` +
   `writers/summarization_worker/`; add `summarization` to `DATA_PLANE_STAGES`;
   compose + `gen_per_source_compose.py`.
7. **Summarizer call** — `build_provider(LLMConfig(model='gpt-5.3-codex-spark',
   reasoning_effort='low', …))` per the `question_planning_provider.py:86` pattern;
   config-driven, per-source swappable.
8. **Batch API** — durable `summarization_batch_items/jobs` tables, OpenAI Batch
   submit/retrieve/file helpers, and singleton `summarization_batch_worker`.
9. **Deferred-T1** — plumb `enqueue_trigger` through the writer; idempotent
   post-summary enqueue.
10. **Handlers** — Drive/Notion `text_dominant`; Fireflies query+mapping+fallback.
11. **Tests** — B16–B23 in `cases_boundary.py` + integration tests; faithfulness
    eval harness as a non-blocking follow-up.

## Rollout

Drive (extract→summarize, the hard case) and Fireflies (native-summary fetch
fixed) in parallel where independent; then Notion; then generalize to Tier-2
prose (Gmail/Jira). Small, reviewable commits; CI disposes.

## Extraction / AGPL decision

Stay on **pypdf (BSD-3)** — already present, no new dependency, no AGPL exposure.
pdfplumber/docling (MIT) are fallbacks only if table fidelity proves inadequate.
