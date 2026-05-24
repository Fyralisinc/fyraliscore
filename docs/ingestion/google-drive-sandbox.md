# Google Drive ingestion sandbox (IN-16)

`scripts/sandbox_google_drive.py` drives the **real** Google Drive ingestion
pipeline end-to-end against a **local mock** of the Drive v3 + DWD token
endpoints — no Google credentials, no network. It is the artifact that proves
the mutable-source semantics (see below) actually hold, the same way the
Calendar sandbox (IN-15) did.

## What it exercises

```
fake service-account (real RSA key, token_uri -> mock)
  -> get_minter() / GoogleHttpClient   real DWD JWT mint -> mock /token
  -> GoogleDriveClient                 real httpx -> mock /files, /changes, /export
  -> fetch_page_google_drive           real cursor + Changes-API logic + text export
  -> handle_google_drive_file          real ObservationDraft
  -> ingest()                          real observation insert + dedup
```

Checks (all must PASS):

1. **Provision** — `finalize_install` writes `google_drive_installations` + two
   `google_drive_targets` (a My Drive + a Shared Drive) + an
   `onboarding_triggers` row with `source='google_drive'`.
2. **Plan** — one `google_drive_files` shard per active target.
3. **Backfill** — the fetcher walks `files.list`, **exports document text**
   (Doc→text, Sheet→csv, **PDF→pypdf**), and ingests. 4 file observations land;
   a binary `image/png` lands metadata-only; the Doc's body and the **PDF's
   text** are embedded in `content_text`.
4. **Comments + revisions** — for each non-folder file the fetcher also pulls
   `comments.list` (with replies) and `revisions.list`, which land as **distinct
   observations** (`content.object_type` = `comment` / `revision`, with their own
   `external_id` namespaces). A resolved comment is a `state_change`.
5. **Start-page-token captured up-front** — the warm start for the first poll
   is taken at the *start* of backfill (so edits during the backfill window
   aren't lost).
6. **Incremental (Changes delta)** — an edit bumps Drive's `version` (3 → 5) so
   it lands a **new** observation; a trash lands a distinct `state_change`.
7. **Dedup** — re-ingesting an identical file (same `version`) collapses to one
   observation.
8. **Reconciler** — pool-provider wiring + gap-probe smoke.

## Run

```bash
python scripts/sandbox_google_drive.py            # throwaway DB on :5434, dropped on exit
python scripts/sandbox_google_drive.py --keep     # retain the DB for inspection
DATABASE_URL=postgresql://.../my_sandbox python scripts/sandbox_google_drive.py
```

## Why the `external_id` is versioned (the landmine)

`services/observations/repo.py` dedups on `(source_channel, external_id)`
**ignoring `occurred_at`** — one stable observation per `external_id`. Drive
files mutate constantly (rename, edit, move, re-share, trash), so the version
must be in the key. Drive's `version` field is a monotonic counter:

```
gdrive:{file_id}:{version}                 # a file state
gdrive:{file_id}:removed:{change_time}     # a removed / lost-access change
```

So identical re-fetches (backfill twin == poll twin) dedup; each edit lands a
fresh observation with current content; a trash lands a `state_change`. The
single-version unit tests don't catch this — the sandbox mutation does.

## Content extraction

The **fetcher** (not the handler — handlers are pure) exports text:
Doc → `text/plain`, Sheet → `text/csv`, Slides → `text/plain`, **PDF →
`alt=media` + pypdf**, `text/*` → `alt=media`. Other binary types (images,
video, archives) are metadata-only. Extracted text is truncated to
`GOOGLE_DRIVE_EXTRACT_MAX_BYTES` (default 65536) and embedded in `content_text`.
Guards: files larger than `GOOGLE_DRIVE_MAX_EXTRACT_FILE_BYTES` (default 10 MB)
are skipped; PDFs are read up to `GOOGLE_DRIVE_PDF_MAX_PAGES` (default 50) pages.

## Comments & revisions

For each non-folder, non-removed file the fetcher pulls comments (with nested
replies) and revision history and emits them as separate records (tagged
`_fyralis_record_type`). They all route through the single `google_drive:file`
channel (the normalizer maps one channel per source) — `core.ingest` requires
`draft.source_channel == channel`, so the three kinds **share
`source_channel=google_drive:file`** and are distinguished by:

| object_type | external_id | kind |
|---|---|---|
| `file` | `gdrive:{file_id}:{version}` | signal / state_change (removed) |
| `comment` | `gdrive-comment:{file_id}:{comment_id}:{modifiedTime}` | signal / state_change (resolved) |
| `revision` | `gdrive-revision:{file_id}:{revision_id}` | signal |

Toggle with `GOOGLE_DRIVE_FETCH_COMMENTS` / `GOOGLE_DRIVE_FETCH_REVISIONS`
(default on). Note: comment authors often have **no `emailAddress`** (Drive
privacy) — the handler falls back to `displayName`. "Suggestions" (Docs
suggestion-mode edits) are a separate Docs-API concern, not covered here.

## Operational notes (shared with all DWD sources)

- The per-tenant `KAFKA_PATH_ENABLED` flag gates `observation_writer`
  full-mode; a data-plane source writes nothing until it's set for the tenant.
- `observations` is monthly-partitioned — backfilling old `modifiedTime`s needs
  `services.observations.partitions.ensure_partitions(...)` for those months.
- Steady-state incremental polling reuses the captured start-page-token via the
  cursor; like Calendar there is no production writeback to
  `google_drive_targets.start_page_token` yet, so steady-state re-syncs are full
  and idempotent via dedup, and the reconciler drives gap detection.
