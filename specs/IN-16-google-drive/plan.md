# IN-16 — Google Drive as an ingestion signal source

Status: implementation. Models on IN-15 (Google Calendar), which is the closest
analog: a Google Workspace API on the **shared Gmail Domain-Wide-Delegation
(DWD) auth substrate** (`services/integrations/gmail/dwd.py::get_minter()` +
`GoogleHttpClient`), one shard kind, two sync modes (full backfill + native
incremental), mutable entities requiring a **versioned `external_id`**.

## Signal value

Drive is the org's document substrate. File create / edit / share / trash
events are collaboration signals (who is working on what, with whom, when).
Unlike Calendar, IN-16 ALSO extracts document **text content** so the
semantic-search / embedding layer can reason over what the documents say, not
just that they changed (per product decision: "file activity + document
content", and coverage = "My Drive + Shared Drives").

## Auth (D1 — reuse Gmail DWD)

Service-account impersonation, exactly like Gmail/Calendar. Scope
`drive.readonly` (alias `drive.readonly`) — the readonly metadata scope is
insufficient because we export Doc/Sheet/Slide bodies. We impersonate a user
per target and address files within that user's corpus (My Drive) or a shared
drive the user can see.

## Coverage (D6 — two target kinds)

A **target** is the shardable unit:
- `my_drive`  — one per resolved user email; `drive_id` sentinel `'my-drive'`,
  `owner_email` = the impersonated user. Backfill via `files.list?corpora=user`.
- `shared_drive` — one per org shared drive; `drive_id` = the shared-drive id,
  `owner_email` = an admin/impersonation identity with access. Backfill via
  `files.list?corpora=drive&driveId=…&includeItemsFromAllDrives=true`.

Shared drives are enumerated at onboarding via `drives.list?useDomainAdminAccess`.

## Sync model (D2 — Changes API is the syncToken analog)

One shard kind `google_drive_files`. Two modes share the shard cursor:

- **FULL (backfill).** First seeded call captures the incremental warm-start
  token via `changes.getStartPageToken` and stashes it into the cursor
  IMMEDIATELY (so edits made *during* the backfill window are caught by the
  first poll — the canonical Drive ordering). Then pages `files.list` windowed
  by `modifiedTime > now-N days`, `orderBy=modifiedTime`.
- **INCREMENTAL (poll).** When the cursor carries a `start_page_token`,
  `changes.list?pageToken=…&includeRemoved=true` returns only changed/removed
  files. `newStartPageToken` (returned on the last page) is the warm start for
  the next run. A stale/invalid page token reseeds a full sync (Risk #1),
  identical to Calendar's 410→full-reseed.

`end_of_data=True` when a page returns no `nextPageToken`.

## Content extraction (D8 — fetcher-side, handler stays pure)

Handlers are pure (no I/O), so the **fetcher** performs the export network call
and injects `_fyralis_extracted_text` onto each non-removed record. Extractable
types:
- `application/vnd.google-apps.document`    → export `text/plain`
- `application/vnd.google-apps.spreadsheet` → export `text/csv`
- `application/vnd.google-apps.presentation`→ export `text/plain`
- `application/pdf`                         → `alt=media` + **pypdf** text extract
- `text/*`                                  → `alt=media`

Other binary types (images, video, archives) are metadata-only. Extracted text
is truncated to `GOOGLE_DRIVE_EXTRACT_MAX_BYTES` (default 65536). Guards: skip
files over `GOOGLE_DRIVE_MAX_EXTRACT_FILE_BYTES` (default 10 MB); PDFs read up to
`GOOGLE_DRIVE_PDF_MAX_PAGES` (default 50). A small `request_bytes` was added to
the shared `GoogleHttpClient` for the non-JSON export/media bodies.

## Comments & revisions (D9 — extra record types, one routing channel)

For each non-folder, non-removed file the fetcher also pulls `comments.list`
(with nested replies) and `revisions.list`, emitting them as separate records
tagged `_fyralis_record_type` ∈ {file, comment, revision}. The normalizer routes
one channel per (source, ingress), and `core.ingest` requires
`draft.source_channel == channel`, so all three **share
`source_channel=google_drive:file`** and are distinguished by
`content.object_type` + a distinct `external_id` namespace
(`gdrive:` / `gdrive-comment:` / `gdrive-revision:`). The single registered
handler branches on the record type. A resolved comment → `state_change`;
revisions are always `signal`. Gated by `GOOGLE_DRIVE_FETCH_COMMENTS` /
`GOOGLE_DRIVE_FETCH_REVISIONS`. Comment authors frequently lack an
`emailAddress` (Drive privacy) → fall back to `displayName`. Docs
suggestion-mode edits (a Docs-API concept) are out of scope.

## external_id — VERSIONED (Risk #3 / mutable-source landmine)

The observations repo dedups on `(source_channel, external_id)` IGNORING
`occurred_at`. Drive files mutate constantly (rename, edit, move, re-share,
trash), so the version must be in the key. Drive's `version` field is a
monotonic counter that bumps on every metadata/content change:

    gdrive:{file_id}:{version}                  # normal file state
    gdrive:{file_id}:removed:{change_time}      # a removed/lost-access change (no version)

So: identical re-fetches (backfill twin == poll twin) dedup; each edit lands a
fresh observation; a trash/removal lands a `state_change`.

## Channel / handler (D3, D4)

One channel `google_drive:file`. Trust `authoritative` (Drive is the system of
record for file existence/metadata). `kind = state_change` when the file is
removed or `trashed`, else `signal`. `content_text` = a legible activity line
PLUS the extracted body (both embedded). `entities_hint` = owners,
last-modifying user, sharing recipients (external flag by domain), and the file
as a `document` entity.

## Schema (migration 0061)

- `google_drive_installations` — one row per (tenant, workspace_domain). Mirrors
  `google_calendar_installations`.
- `google_drive_targets` — one row per `my_drive`/`shared_drive` target; carries
  `start_page_token` (the Changes cursor, analog to `sync_token`).
- RLS (tenant_isolation) on both, mirroring the gmail_* template.
- Widen the four M6 substrate source CHECKs (`source_onboarding_runs`,
  `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`) to admit
  `'google_drive'`.

## Dispatch / allowlist wiring (the IN-14/IN-15 checklist)

planner / fetcher / reconciler / handler dispatch + `SourceLiteral`
(envelope, progress events), `build_raw_s3_key` guard, `_S3_KEY_RE`,
`channel_mapping`, `tenant_onboarding`/`source_onboarding` VALID_SOURCES +
install loaders, reconciler + periodic_reconciler pool-provider wiring,
`dlq/publish._VALID_SOURCES`, `endpoints.google_drive_api`.

## Validation

Mock Drive API server + fixtures + a `scripts/sandbox_google_drive.py` that
drives the real fetcher→ingest path and asserts: backfill lands file
observations with extracted text; an edit (version bump) lands a new
observation; a trash lands a `state_change`; an unchanged re-fetch dedups; the
incremental poll catches a change made after the start-page-token capture.
