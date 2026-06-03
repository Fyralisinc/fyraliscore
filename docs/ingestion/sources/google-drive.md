# Google Drive (IN-16)

> Documents as signals — modeled on Calendar (same DWD substrate, one shard kind,
> two sync modes, mutable→versioned external_id). The new capability is
> **document text extraction** + **two target kinds** (My Drive + Shared Drives).

| Field | Value |
|---|---|
| Source | `google_drive` |
| Primary channel | `google_drive:file` (files, comments, revisions all here) |
| Trust tier | `authoritative` |
| Live ingress | **none** — poll-only |
| Backfill | enumerate My Drive (per user) + Shared Drives → `files.list` |
| Incremental | Changes API (`changes.list?pageToken=…`) |
| Auth | Domain-Wide Delegation, scope `drive.readonly` |

## Auth (D1)

Reuses the Gmail DWD substrate; scope `drive.readonly` (needed to **export** Doc/
Sheet/Slide bodies, not just metadata).
[google_drive/onboarding.py](../../../services/ingest/integrations/google_drive/onboarding.py)
resolves targets and finalizes the install.

## Backfill & incremental (the quartet)

- [planners/google_drive.py](../../../services/ingest/ingestion/planners/google_drive.py)
  — **two target kinds** (D6): `my_drive` (per resolved user) + `shared_drive`
  (enumerated via `drives.list?useDomainAdminAccess`).
- [fetchers/google_drive.py](../../../services/ingest/ingestion/fetchers/google_drive.py)
  via [google_drive/client.py](../../../services/ingest/integrations/google_drive/client.py):
  - **Changes API is the syncToken analog** (D2): backfill captures
    `changes.getStartPageToken` **up front** (so edits during the window aren't
    lost), then walks `files.list`; poll runs `changes.list?pageToken=…`.
  - **Content extraction in the fetcher** (D8 — handlers stay pure): Doc→text,
    Sheet→csv, Slides→text, **PDF→pypdf**, `text/*`→media; other binary types are
    metadata-only. Truncated to `GOOGLE_DRIVE_EXTRACT_MAX_BYTES` (+ file-size and
    PDF-page caps). A small `request_bytes` was added to the shared
    `GoogleHttpClient` for the non-JSON export/media bodies.
  - **Comments + revision history** are pulled per file as extra record types (D9).
- [reconcilers/google_drive.py](../../../services/ingest/ingestion/reconcilers/google_drive.py).
- [handlers/google_drive.py](../../../services/ingest/ingestion/handlers/google_drive.py)
  — **one channel `google_drive:file`** for files, comments, and revisions
  (the normalizer routes one channel per source and `core.ingest` requires
  `draft.source_channel == channel`); distinguished by `content.object_type` +
  `external_id` namespace. A resolved comment → `state_change`. Trust
  `authoritative`; `kind=state_change` for removed/trashed files, else `signal`.

## Dedup / external_id (mutable entities)

Versioned `external_id` using Drive's monotonic `version`, namespaced per record
type:

```
gdrive:{file_id}:{version}            # file
gdrive-comment:{...}                  # comment
gdrive-revision:{...}                 # revision
```

An edit (version bump) lands a new observation; identical re-fetches dedup.

## Migration

`0061_google_drive.sql` — 2 tables (`installations` + `targets`) + widen the four
M6 source CHECKs. Onboarding emits an `onboarding_triggers` row
(`source=google_drive`).

**Sandbox**: `scripts/sandbox_google_drive.py` + a Drive mock drive the real
minter → fetcher (with text export) → ingest end-to-end (backfill, incremental
Changes delta incl. an edit version-bump + a trash, dedup) against a throwaway
Postgres — 11/11 checks.

Spec: `specs/IN-16-google-drive/plan.md`. See [architecture.md](../architecture.md).
