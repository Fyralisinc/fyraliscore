# ADR-0005: Large payloads ingest through a dedicated Large Object Pipeline (blob tier → extract → chunk → multi-vector), with no truncation or size-based exclusion

- **Status:** Proposed <!-- Proposed | Accepted | Superseded by ADR-XXXX | Deprecated -->
- **Date:** 2026-06-14
- **Deciders:** Ingestion / platform
- **Related:** [ADR-0001](0001-kafka-first-ingestion-default.md) (Kafka-first ingestion — the lanes this extends), the [Ingest architecture page](../architecture/ingest.md), the [Runtime & Data Plane page](../architecture/data-plane.md), the implementation plan at `specs/large-object-pipeline/plan.md`, and the 25 sources under `services/ingest/integrations/`.

## Context

Every ingestion path today is built on one assumption: **one event = one small
JSON payload = one `content_text` = one embedding vector**. That assumption is
enforced at three places:

- **Gateway / validation:** `MAX_PAYLOAD_BYTES = 1 MB`
  (`services/ingest/ingestion/payload_validation.py`) rejects anything larger
  with HTTP 413.
- **Per-source truncation:** handlers cap `content_text` (the canonical pattern is
  `_truncate(text, 600)`), and the Google Drive source specifically skips files
  over **10 MB**, caps extracted text at **64 KB** (`GOOGLE_DRIVE_EXTRACT_MAX_BYTES`),
  and caps PDFs at **50 pages** (`GOOGLE_DRIVE_PDF_MAX_PAGES`).
- **Single-vector storage:** `observations.embedding VECTOR(768)` holds exactly
  one vector for the whole observation; there is no per-chunk representation, so
  even fully-extracted long text is not fully searchable.

Two physically different classes of "large payload" break this, and they need
different machinery:

- **Class A — large binary / file content the system never even downloads.**
  Drive file bodies (GBs), Gmail/Slack/Discord attachments (**not fetched at all
  today — only counted**), Fireflies transcripts (truncated to 600 chars) and
  recordings, Notion block trees (depth-capped at 3) and file blocks,
  Telegram/Signal media, Figma/Miro exports. *(Sizes are inferences from each
  provider's documented limits + the source audit, not measured production
  figures.)*
- **Class B — a single structured JSON record that exceeds the inline 1 MB cap.**
  Carta cap tables (1000+ grants × vesting schedules → 1.5–3 MB; **highest
  risk**), QuickBooks invoices with many line items, Jira issues with
  `expand=changelog` inlining hundreds of transitions, HiBob bulk employee +
  payroll, AWS CloudTrail events with large `responseElements`. These are
  rejected at the gateway or pass through into unbounded `content` JSONB.

The product requirement that forces this ADR is explicit: **no shortcuts, no
exclusions** — large content must be ingested in full and made fully searchable,
not skipped, truncated, or capped on *coverage*.

What already exists in our favour (so this is an extension, not a greenfield
build): an S3-compatible **raw tier** (`fyralis-raw`, content-addressed
`blake2b` keys, zstd, idempotent `put_if_absent`) under
`services/ingest/ingestion/raw_tier/`; **per-source Kafka lanes**
`ingestion.{raw,normalized,embedding,dlq}.{source}`
(`DATA_PLANE_STAGES`, `kafka/topics.py`); an **async embedding worker** with an
`embedding_pending` flag + DLQ; and a **partitioned `observations`** table.

The tension: the work needed for large payloads (streaming GB-scale downloads,
MIME-specific extraction over untrusted files, chunking, batch embedding) is
slow, memory-heavy, and a real security surface — it **cannot** run in the
synchronous gateway/handler request path, and it cannot share the
"one row, one vector" storage shape.

## Decision

We will build a **Large Object Pipeline (LOP)**: handlers stop fetching and
extracting large content inline and instead *declare* it as typed references; a
dedicated asynchronous, content-addressed, backpressured pipeline does
fetch → store → extract → chunk → embed, writing a **multi-vector** store that the
retrieval layer rolls up. The eight load-bearing decisions:

**1. Handlers declare large content; they no longer fetch or extract it.**
We introduce a typed `LargeContentRef` (`kind ∈ {file, attachment, transcript,
export, oversized_json}`, `source_uri`, `mime_hint`, `size_hint`, `filename`,
`auth_scope`, `extract: bool`). A handler emits its `ObservationDraft` **plus**
zero-or-more refs; all slow/dangerous work moves out of the request path. This is
the single uniform change every source makes. *Rejected: keep extracting inline
but raise the caps* — it would push multi-second, multi-hundred-MB work into the
synchronous gateway path and couple every source to heavy parser/STT
dependencies.

**2. Full bytes live in a new blob tier, kept indefinitely.** A new bucket
`fyralis-blobs` (separate from `fyralis-raw` so retention/lifecycle differ — raw
webhook bodies expire in days; source-of-truth file bytes are retained
**indefinitely** so we can re-extract/re-chunk when parsers and embedding models
improve). Uploads are **streaming multipart** (bounded worker memory regardless
of file size) and **content-addressed** by streamed `blake2b`. A `blobs` catalog
table (`blob_id, tenant_id, source, content_hash, storage_key, mime, byte_size,
filename, status, extracted_text_key, created_at`) with `UNIQUE(tenant_id,
content_hash)` makes re-ingest idempotent and prevents re-downloading identical
files. *Rejected: store extracted text only / expire bytes* — discarding originals
forecloses re-processing with better models, which is the main reason to keep a
blob tier at all.

**3. Three new worker stages; fetch is per-source, extract + chunk-embed are
shared and MIME-driven.** We extend the data plane with:
`ingestion.blob.{source}` (fetch jobs — needs the source's credentials, so it is
per-source like `raw`), then **shared** `ingestion.extract` and
`ingestion.chunk_embed` lanes (a PDF is a PDF regardless of which source
delivered it). Each reuses the existing idempotent producer, per-source DLQ, and
`embedding_pending` patterns, so retry/replay/observability come for free.
*Rejected: a single monolithic "large content" worker* — fetch (I/O + auth-bound),
extract (CPU/memory-bound, security-sensitive), and embed (GPU/embedder-bound)
have different scaling and failure profiles and must be independently
back-pressured.

**4. Extraction is bounded on *resources*, never on *coverage*.** The extractor
is MIME-routed (PDF → all pages; Office docx/xlsx/pptx; CSV/Sheets → all rows;
archives → recurse) and runs in a **resource-jailed** worker with hard
wall-clock / memory / recursion limits. This is the line between *bounded*
(allowed) and *excluded* (forbidden): we always **attempt the whole artifact**;
we cap the *resources* a single item may consume, and when a cap is hit we **DLQ
with a reason and emit a metric — never silently truncate**. Untrusted file
parsing (zip bombs, malicious PDFs, decompression bombs, SSRF via signed
attachment URLs) is treated as an attack surface, not a happy path.

**5. Media bytes are stored now; transcription/OCR is deferred behind a cost
gate.** Audio/video/image bytes are captured into the blob tier immediately
(nothing is lost or excluded), but STT (Whisper-class) and OCR/caption run in a
later phase (P5) gated on cost — because they require GPU/STT infrastructure that
materially changes the operating budget. Until then media is stored-but-not-yet-
searchable, and because bytes are kept indefinitely (decision 2) we backfill
transcripts by re-running extraction, with no re-fetch. *Rejected: full STT/OCR in
the first phase* — highest fidelity but front-loads the largest cost and infra
dependency onto the riskiest part of the build.

**6. Long text becomes many vectors via a new `observation_chunks` table.** A
structure-aware splitter produces token-bounded overlapping chunks (target
**~512 tokens, ~15% overlap**, respecting page/paragraph boundaries); each chunk
is one row in a partitioned `observation_chunks` table
(`chunk_id, tenant_id, observation_id, occurred_at, blob_id, chunk_index,
char_start, char_end, token_count, chunk_text, embedding VECTOR(768),
embedding_pending`) with its own HNSW index. The parent `observations` row keeps a
bounded summary `content_text` (so the headline stays meaningful and a coarse
doc-level vector exists) and gains `has_blobs`, `chunk_count`, `blob_ids[]`.
**Every page of every document becomes an independently searchable vector.**
*Rejected: keep one vector by averaging chunk embeddings* — mean-pooling a long
document destroys the local detail that makes retrieval useful.

**7. Class B (large structured JSON) fans out into child observations.** Where a
payload is really a *collection*, the handler emits **one child observation per
logical unit** — Carta → one per grant/stakeholder, QuickBooks → one per invoice,
Jira changelog → one per transition, HiBob → one per employee/payroll line. Each
unit gets its own embedding and is independently searchable, which fits the
existing per-entity grain (Carta already emits per-entity; the fix is ensuring no
single observation accumulates an unbounded array). When a *single* logical record
still exceeds the inline cap, it spills as an `oversized_json` ref into the blob
tier with a bounded projection kept in `content`, and the serialized form runs
through the same chunk-embed backend; the gateway stops returning 413 for these
and routes them async. *Rejected: one observation + always-overflow-blob* —
coarser retrieval granularity and it fights the substrate's per-entity model.

**8. Retrieval must search chunks and roll up — this reaches into reasoning.**
Multi-vector storage is inert unless the retrieval/memory layer searches
`observation_chunks` and rolls chunks up to their parent observation (max/mean
pooling, top-k chunks attached to the observation's seed text fed to Think). This
is the one place the change extends beyond `services/ingest` into
`services/reasoning`, and it is a required part of the decision, not a follow-on.

Backpressure and dedup are cross-cutting and non-optional: per-tenant + global
byte/egress budgets (reusing the Redis token-bucket limiter), concurrency caps on
the CPU-bound extractor, and content-hash dedup so redelivered webhooks and
identical files never re-download or re-embed.

## Consequences

**Easier / now possible.**

- Large files are ingested **in full** and made fully searchable at page/chunk
  granularity — the Drive 10 MB skip, the 64 KB / 50-page / 600-char caps, and the
  1 MB Class-B rejection all go away as *coverage* limits.
- Attachments that are invisible today (Gmail/Slack/Discord/Telegram) become
  first-class ingested content.
- Because original bytes are retained, re-extraction with better parsers/models is
  a replay, not a re-fetch — including the deferred STT/OCR backfill.
- The pipeline reuses the existing Kafka lanes, idempotent producer, DLQ, and
  `embedding_pending` machinery, so operational tooling carries over.

**Harder / new constraints.**

- **New storage cost curve.** Keeping all file bytes indefinitely is a deliberate,
  unbounded-by-design storage commitment. **TODO(human):** set a budget alert /
  cap policy and decide the eventual cold-storage tiering for `fyralis-blobs`.
- **New security surface.** The extractor parses untrusted files; the resource
  jail, SSRF egress controls on attachment fetches, and per-item caps are
  load-bearing safety, not nice-to-haves.
- **Multi-vector retrieval is a contract change.** Anything reading
  `observations.embedding` must learn about `observation_chunks`; mixed
  doc-level + chunk-level search needs a defined ranking/rollup.
- **More rows.** Class-B fan-out and per-chunk storage multiply row counts;
  partition/index sizing for `observation_chunks` needs validation at scale.
- **Cost gate is a known coverage gap until P5.** Media is stored but not
  searchable until STT/OCR ships; this must be surfaced (a metric / status), not
  left implicit — silent gaps read as "covered" when they are not.
- **Embedder input limits still apply per chunk.** `nomic-embed-text`'s context
  window bounds chunk size; the ~512-token target is chosen against it.
  **TODO(human):** confirm the production embedder's exact token window and tune
  chunk size/overlap empirically.

**How this is revisited / falsified.** If per-tenant blob volume or extraction
cost proves unsustainable, decision 2's "indefinitely" becomes a tiered-retention
policy (supersede with a new ADR) — the pipeline shape is unchanged, only the
lifecycle. If chunk-level retrieval does not measurably improve reasoning quality
over doc-level vectors, decision 6's multi-vector store is over-built and should be
collapsed back. The phased plan (`specs/large-object-pipeline/plan.md`) is
explicitly ordered so P0–P2 prove the path on **one** source (Google Drive) before
fanning out, so the architecture can be falsified cheaply before the expensive
phases.
