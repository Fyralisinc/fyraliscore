# Large Object Pipeline — Implementation Plan

> **Status:** Draft plan for review (no code written yet).
> **Decision record:** [ADR-0005](../../docs/adr/0005-large-object-pipeline.md).
> **Scope:** ingest full large payloads (files, attachments, transcripts, media
> bytes, oversized structured JSON) with **no truncation and no size-based
> exclusion**, and make them searchable at chunk granularity.

This plan is ordered so the architecture is **proven on one source (Google
Drive) before the expensive fan-out**. Each phase is independently shippable and
ends in an acceptance gate. Migrations continue from the current head (`0128`),
so new migrations start at **`0129`**.

---

## Locked decisions (from ADR-0005)

| Question | Decision |
|---|---|
| Media (audio/video/image) | **Store bytes now, STT/OCR deferred to P5** behind a cost gate. Nothing dropped. |
| Blob retention | **Keep original bytes indefinitely** (`fyralis-blobs`); re-extraction is a replay, not a re-fetch. |
| Class B (collections) | **Fan out into child observations** (one per grant/invoice/transition/employee); single oversized records spill as `oversized_json`. |
| First artifact | This plan + the ADR; **review before code**. |

---

## Component inventory (new vs. reused)

**Reused as-is:** `raw_tier/s3.py` `S3Client` + content-hash key builder pattern;
`kafka/topics.py` `DATA_PLANE_STAGES` + idempotent producer; per-source DLQ;
`embedding_pending` flag + embedding worker pattern; Redis token-bucket limiter;
the `lib/embeddings` backend factory.

**New:**
- `services/ingest/ingestion/large_object/refs.py` — `LargeContentRef` dataclass + the handler-emit seam.
- `services/ingest/ingestion/large_object/blob_store.py` — streaming multipart `BlobClient` over `fyralis-blobs`.
- `services/ingest/ingestion/large_object/blob_fetcher/` — per-source fetch worker (`ingestion.blob.{source}`).
- `services/ingest/ingestion/large_object/extractor/` — shared MIME-routed extractor worker (`ingestion.extract`).
- `services/ingest/ingestion/large_object/chunk_embed/` — shared splitter + batch embed worker (`ingestion.chunk_embed`).
- DB: `blobs` catalog table; `observation_chunks` partitioned multi-vector table; `observations` column additions.
- `services/reasoning` retrieval roll-up over `observation_chunks`.

---

## Phase P0 — Blob tier + catalog + the handler seam (no behaviour change)

**Goal:** the plumbing exists and is tested; nothing yet routes through it.

1. **Bucket + compose:** add `fyralis-blobs` to `docker-compose.yml` + `minio-init`
   (`mc mb --ignore-existing local/fyralis-blobs`); env `S3_BLOB_BUCKET`.
2. **`BlobClient`** (`blob_store.py`): streaming **multipart** `put_stream(...)`
   (bounded memory, any size), content-addressed by streamed `blake2b`,
   `put_if_absent` semantics; `get_stream(...)`. Key scheme mirrors the raw tier:
   `{env}/{source}/{tenant_id}/{yyyy-mm}/{hash[:2]}/{hash}` (+ extension/codec).
3. **Migration `0129_blobs_catalog.sql`:** `blobs(blob_id, tenant_id, source,
   content_hash, storage_key, mime, byte_size, filename, status, extracted_text_key,
   error, created_at)`, `UNIQUE(tenant_id, content_hash)`, RLS on `tenant_id`.
4. **`LargeContentRef`** (`refs.py`) + extend `ObservationDraft` to carry
   `large_refs: list[LargeContentRef]` (default empty — every existing handler is
   unaffected).

**Gate P0:** unit tests for multipart upload of a >part-size payload, content-hash
idempotency (second `put` is a no-op), catalog dedup on `(tenant, content_hash)`,
RLS isolation. Existing ingestion test suite stays green.

---

## Phase P1 — End-to-end on Google Drive only

**Goal:** one source proves fetch → store → extract → chunk → embed, full coverage.

1. **Topics:** extend `DATA_PLANE_STAGES` with `blob` (per-source) and register
   shared lanes `ingestion.extract`, `ingestion.chunk_embed` in
   `provision_kafka_topics.py`.
2. **Blob fetcher worker** (`ingestion.blob.google_drive`): resolves the Drive
   install/credential, **streams** the file body (multipart, no 10 MB skip) into
   `fyralis-blobs`, writes the `blobs` row, enqueues an extract job. Honors a
   per-tenant + global **byte budget** (Redis). DLQ on failure.
3. **Extractor worker** (`ingestion.extract`, MIME-routed, **resource-jailed**:
   hard wall/mem/recursion caps): PDF → **all** pages; Google-native exports;
   `text/*`; Office docx/xlsx/pptx; CSV. Writes extracted text as its own blob
   (`extracted_text_key`) + metadata (page/sheet counts). Cap-hit ⇒ DLQ + metric,
   **never silent truncation**.
4. **Migration `0130_observation_chunks.sql`:** partitioned
   `observation_chunks(chunk_id, tenant_id, observation_id, occurred_at, blob_id,
   chunk_index, char_start, char_end, token_count, chunk_text, embedding
   VECTOR(768), embedding_pending)` + HNSW(`vector_cosine_ops`) +
   btree(`tenant_id, observation_id`); RLS. **Migration `0131_observations_blobs.sql`:**
   add `has_blobs BOOL`, `chunk_count INT`, `blob_ids UUID[]` to `observations`.
5. **Chunk+embed worker** (`ingestion.chunk_embed`): structure-aware splitter
   (~512 tokens, ~15% overlap, page/paragraph aware), batch-embed via the existing
   backend, insert chunk rows, set parent `has_blobs/chunk_count/blob_ids` and a
   bounded summary `content_text`.
6. **Drive handler change:** replace inline `export_text(...)` with emitting a
   `LargeContentRef(kind="file", ...)`; **remove** the 10 MB / 64 KB / 50-page caps
   from the Drive path.

**Gate P1:** synthetic Drive run with (a) a >100 MB binary — stored whole, not
skipped; (b) a 300-page PDF — every page chunked + embedded; (c) re-delivery —
no re-download/re-embed (hash dedup). DLQ-replay + S3 recovery covered.

---

## Phase P2 — Retrieval roll-up (the read side)

**Goal:** chunks are actually searchable and reach Think.

1. Extend the `services/reasoning` retrieval/memory layer to vector-search
   `observation_chunks`, roll up to parent observations (max/mean pool), and attach
   top-k chunks to the observation seed text.
2. Define ranking when both doc-level (`observations.embedding`) and chunk-level
   hits exist; dedupe to parent.

**Gate P2:** a query whose answer lives only in page 200 of a long PDF retrieves
the right chunk and surfaces it to Think; mixed doc+chunk ranking validated.

---

## Phase P3 — Fan out Class A (binary/attachment sources)

Apply the P1 seam to: **Gmail** (download attachments — currently never fetched),
**Slack** (`url_private` file uploads), **Discord** (attachments), **Fireflies**
(full transcript, not 600 chars; recording bytes stored), **Notion** (remove
depth-3 cap; file blocks), **Telegram/Signal** (media bytes), **Figma/Miro**
(exports), **Google Calendar** (attachments). Each = a handler emitting refs + a
per-source fetch adapter (auth + `source_uri` resolution + SSRF egress controls on
signed URLs). Extractor/chunk-embed are unchanged (MIME-driven).

**Gate P3:** per-source synthetic runs proving attachments/media land in
`fyralis-blobs` and text-extractable types are chunked; media stored-but-not-yet-
transcribed is **explicitly surfaced** (status/metric), not silently absent.

---

## Phase P4 — Class B (large structured JSON)

1. **Fan-out:** Carta (per grant/stakeholder), QuickBooks (per invoice, line items
   bounded), Jira changelog (per transition, or capped rollup + full history as
   `oversized_json`), HiBob (per employee/payroll line), Gusto/Deel (per payroll
   line), AWS (large `responseElements` → `oversized_json`). Ensure **no single
   observation accumulates an unbounded array**.
2. **`oversized_json` path:** serialize → blob tier; bounded projection in
   `content`; serialized/flattened form runs through `ingestion.chunk_embed`.
3. **Gateway:** stop returning 413 for oversized-but-valid bodies; route to the
   async overflow path instead.

**Gate P4:** a 3 MB Carta cap table ingests fully (no 413, no dropped grants),
fanned into child observations each independently retrievable.

---

## Phase P5 — STT / OCR (cost-gated)

Add extractor handlers for audio/video (Whisper-class STT) and images
(OCR/caption), gated on a cost/infra switch. Because P1 retained original bytes,
this is a **re-run of extraction over existing blobs** — no re-fetch. Backfill
existing media.

**Gate P5:** a stored meeting recording produces a full transcript + chunks via
replay; cost controls (rate, concurrency, per-tenant budget) enforced.

---

## Cross-cutting (every phase)

- **Idempotency/dedup:** content-hash on bytes; `UNIQUE(tenant, content_hash)`;
  chunk re-embed guarded by `embedding_pending`.
- **Backpressure/cost:** per-tenant + global byte/egress budgets; extractor
  concurrency caps; all heavy work async (gateway stays fast).
- **Security:** resource-jailed extraction (zip/PDF/decompression bombs); SSRF
  egress allowlist on attachment fetches; NUL-byte handling preserved.
- **Observability:** reuse the Prometheus registry — bytes fetched, extract latency
  by MIME, chunks/doc, embed backlog, DLQ depth per new stage; a metric for
  "stored-but-not-yet-extracted" coverage gaps.
- **Docs:** update [ingest.md](../../docs/architecture/ingest.md) and
  [data-plane.md](../../docs/architecture/data-plane.md) from "Planned" to "live"
  per phase, and add per-source notes under `docs/ingestion/sources/`.

---

## Open items needing a human decision

- **TODO(human):** storage budget cap / cold-tiering policy for indefinitely-retained `fyralis-blobs`.
- **TODO(human):** confirm the production embedder's exact token window; tune chunk size/overlap empirically.
- **TODO(human):** STT/OCR provider + cost ceiling for P5 (self-hosted Whisper vs. managed).
- **TODO(human):** SSRF egress allowlist policy for fetching signed attachment URLs.
