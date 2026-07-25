# Google Drive Ingestion — How Fyralis Pulls Google Drive Data

This document explains, in detail, **how Google Drive data enters Fyralis**:
which Drive v3 REST APIs are called, with which credential, and how the Drive
signal set — **files, file comments, and file revisions** across both **My
Drive** (per-user) and **Shared Drives** — is each ingested.

It deliberately stops at the point where a Drive change becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Layout note.** Drive ingestion lives under the `services/ingest/…` tree
> (the newer layout), not the `services/ingestion/…` paths used by the older
> Slack/GitHub flow docs. All links below resolve under `services/ingest/`.

---

## 1. The two ways data arrives

Google Drive data reaches Fyralis through **two paths that converge on one
handler**. Unlike Slack/GitHub, Drive's "live" path is **not a vendor-pushed
content webhook** — it is a **poll** that re-runs the *same* backfill fetcher
incrementally against Google's **Changes API**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* history via the Drive **v3 REST API** (`files.list`, windowed by `modifiedTime`) | [planners/google_drive.py](../../../services/ingest/ingestion/planners/google_drive.py), [fetchers/google_drive.py](../../../services/ingest/ingestion/fetchers/google_drive.py) |
| **Live (poll)** | A short-cadence worker | Fyralis *polls* the **Changes API** (`changes.list?pageToken=…`) from a stored `start_page_token`, re-running the **same fetcher** in incremental mode | [google_drive/live_poller.py](../../../services/ingest/integrations/google_drive/live_poller.py), [_google_live.py:32-82](../../../services/ingest/integrations/_google_live.py#L32-L82) |

Crucially, **both paths produce the exact same record shape** — a RAW Drive v3
file/comment/revision object plus injected `_fyralis_*` private keys — and both
are parsed by the **single** `google_drive:file` handler
([handlers/google_drive.py:127-143](../../../services/ingest/ingestion/handlers/google_drive.py#L127-L143)).
Both derive the **same** dedup key per record type
([idempotency/__init__.py:99-121](../../../services/ingest/ingestion/idempotency/__init__.py#L99-L121)):

```
file      external_id = gdrive:{file_id}:{version}                  # versioned; removed → gdrive:{file_id}:removed:{change_time|now}
comment   external_id = gdrive-comment:{file_id}:{comment_id}:{modifiedTime}
revision  external_id = gdrive-revision:{file_id}:{revision_id}     # immutable
```

Because Drive's `version` is a monotonic counter that bumps on every
metadata/content change, a file that is both backfilled *and* picked up by the
poll collapses into **one** observation when nothing changed (same `version`),
while a genuine edit lands a fresh observation (`version` bumped). This is the
central design invariant of Drive ingestion — the same fetcher is driven under
`ingress_kind="backfill"` and `ingress_kind="poll"`, routed to the **same**
channel by the normalizer
([channel_mapping.py:92-93](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L92-L93)).

> **All three record kinds share one channel.** Files, comments, and revisions
> all carry `source_channel = "google_drive:file"` and are distinguished only by
> `content.object_type` + the `external_id` namespace, because the normalizer
> routes one channel per source and `core.ingest` requires
> `draft.source_channel == channel`
> ([handlers/google_drive.py:1-12](../../../services/ingest/ingestion/handlers/google_drive.py#L1-L12)).

> **TODO(human): an opt-in native push channel also exists in the code, contradicting the "poll-only / no push-webhook in v1" claim.**
> A Drive `changes.watch` push channel is fully implemented and wired
> ([google_drive/watch.py](../../../services/ingest/integrations/google_drive/watch.py),
> the generic engine [_google_watch.py](../../../services/ingest/integrations/_google_watch.py),
> the ingress [app/webhooks/google_push.py:83-85](../../../services/app/webhooks/google_push.py#L83-L85),
> and the `google_drive_watch_scheduler` process
> [process_manifest.py:302-309](../../../services/platform/runtime/process_manifest.py#L302-L309)).
> Yet three canonical comments assert Drive is poll-only with **no** push:
> [channel_mapping.py:85](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L85)
> ("no push-webhook in v1"), [oauth.py:32-33](../../../services/ingest/integrations/google_drive/oauth.py#L32-L33)
> ("Drive is poll-only … no push channel"), and the short source ref
> [sources/google-drive.md:12](../sources/google-drive.md) ("Live ingress: none —
> poll-only"). The most consistent reading of the code (the push scheduler idles
> when `GOOGLE_PUSH_WEBHOOK_BASE` is unset and the poller is the documented
> liveness guarantee — [_google_watch.py:206-222](../../../services/ingest/integrations/_google_watch.py#L206-L222))
> is that **poll is the canonical v1 live path** and push is an **opt-in latency
> optimization** layered on later. §8 documents the push channel as it exists;
> please confirm whether push is intended-for-v1 and reconcile the stale
> "no push" comments (or this doc) accordingly.

---

## 2. Authentication & token model

Drive ingestion uses **a single credential model: Google Workspace
Domain-Wide Delegation (DWD)** — the *shared Gmail DWD substrate*. There are no
per-user OAuth tokens and no installation tokens stored per drive. A service
account impersonates a Workspace user and mints a short-lived, scope-bound
bearer token on demand.

### 2.1 The DWD impersonation flow

1. **Service-account JWT → impersonated bearer token.** `DwdTokenMinter` signs a
   JWT carrying `sub = <impersonated-user-email>` and
   `scope = "https://www.googleapis.com/auth/drive.readonly"`, exchanges it at
   Google's token endpoint
   (`grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer`), and caches the
   result keyed on `(service_account, user_email, frozenset(scopes))` with a
   per-key `asyncio.Lock` to prevent a mint stampede
   ([gmail/dwd.py:1-27](../../../services/ingest/integrations/gmail/dwd.py#L1-L27),
   [gmail/dwd.py:130-197](../../../services/ingest/integrations/gmail/dwd.py#L130-L197)).
2. **Per-call impersonation.** Every Drive request passes the impersonated
   `user_email` + scope; the shared `GoogleHttpClient` sets
   `Authorization: Bearer {token}`, and on a `401` invalidates the cached token
   and retries once
   ([gmail/client.py:88-115](../../../services/ingest/integrations/gmail/client.py#L88-L115)).

For a user's **My Drive** we impersonate that user and read their corpus
(`corpora=user`); for a **Shared Drive** we impersonate a user/admin who can see
it and address it by `driveId`
([google_drive/client.py:9-12](../../../services/ingest/integrations/google_drive/client.py#L9-L12),
[google_drive/client.py:181-219](../../../services/ingest/integrations/google_drive/client.py#L181-L219)).

### 2.2 Scope

Drive exposes a **single read scope** today:
`https://www.googleapis.com/auth/drive.readonly`, stored on the install as the
alias `drive.readonly`
([google_drive/client.py:37-77](../../../services/ingest/integrations/google_drive/client.py#L37-L77)).
The readonly scope (not a narrower metadata scope) is required so the fetcher can
**export Doc/Sheet/Slide bodies** and download PDF/text content, not just
metadata ([sources/google-drive.md:19](../sources/google-drive.md)).

### 2.3 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Service account | shared Gmail DWD service account | one per deployment; `service_account_email` recorded on `google_drive_installations` |
| DWD client ID | `GMAIL_SERVICE_ACCOUNT_CLIENT_ID` (env) | numeric; surfaced in the Admin-Console remediation payload ([oauth.py:229-235](../../../services/ingest/integrations/google_drive/oauth.py#L229-L235)) |
| Per-user bearer token | minted on demand, in-process cache only | never persisted; no `secret_ref` per drive |

> **Contrast with Slack/GitHub.** There is **no OAuth `code` exchange and no
> stored token** here. The DWD grant is configured **out-of-band** in the
> customer's Workspace Admin Console; Fyralis mints tokens at read time. There is
> no per-drive secret in the secret store.

### 2.4 The connect wizard (how an install gets registered)

`services/ingest/integrations/google_drive/oauth.py` is the admin **connect
wizard** (DWD-shaped, mirroring Gmail/Calendar — not the install/callback shape
of Slack/GitHub/Notion):

1. **`POST /integrations/google_drive/connect/preflight`** (auth'd) — impersonates
   `admin_email` at directory scopes and enumerates users/groups/org-units for
   the selector UI. If the DWD grant is missing/mis-scoped it returns a
   structured `dwd_grant_invalid` error carrying the exact client ID + scope
   strings to paste into the Admin Console
   ([oauth.py:100-158](../../../services/ingest/integrations/google_drive/oauth.py#L100-L158)).
2. **`POST /integrations/google_drive/connect/finalize`** (auth'd) — resolves the
   `inclusion_spec` to per-user My-Drive targets via the shared Directory API,
   optionally enumerates the org's Shared Drives via `drives.list`, then **in one
   tenant-scoped transaction** upserts `google_drive_installations`, inserts one
   `google_drive_targets` row per resolved target, and emits an
   `onboarding_triggers` row (`source='google_drive'`) so the existing M6
   backfill chain fires
   ([oauth.py:161-226](../../../services/ingest/integrations/google_drive/oauth.py#L161-L226),
   [onboarding.py:121-216](../../../services/ingest/integrations/google_drive/onboarding.py#L121-L216)).

---

## 3. The Drive v3 API surface that is actually called

All read calls funnel through the shared `GoogleHttpClient`
([gmail/client.py:61-115](../../../services/ingest/integrations/gmail/client.py#L61-L115)),
which:

- sets `Authorization: Bearer {impersonated_token}` (resolved per call from the
  DWD minter),
- retries once on `401` after invalidating the cached token,
- maps `429`, `5xx`, and `403 quotaExceeded` to a typed `GoogleRateLimited`
  carrying the suggested `Retry-After`
  ([gmail/client.py:159-183](../../../services/ingest/integrations/gmail/client.py#L159-L183)),
- exposes `request_bytes` for the **non-JSON** export/media bodies
  ([gmail/client.py:117-139](../../../services/ingest/integrations/gmail/client.py#L117-L139)).

The endpoints invoked, all wrapped by `GoogleDriveClient`:

| Drive v3 endpoint | Wrapper | Purpose | Code |
|-------------------|---------|---------|------|
| `GET /drives?useDomainAdminAccess=true` | `list_shared_drives()` | enumerate org Shared Drives at onboarding | [client.py:140-159](../../../services/ingest/integrations/google_drive/client.py#L140-L159) |
| `GET /changes/startPageToken` | `get_start_page_token()` | warm-start token for incremental sync (the syncToken analog) | [client.py:161-179](../../../services/ingest/integrations/google_drive/client.py#L161-L179) |
| `GET /files` | `list_files()` | **backfill** walk, windowed by `modifiedTime` | [client.py:181-219](../../../services/ingest/integrations/google_drive/client.py#L181-L219) |
| `GET /changes?pageToken=…` | `list_changes()` | **incremental** delta (`includeRemoved`) | [client.py:221-252](../../../services/ingest/integrations/google_drive/client.py#L221-L252) |
| `GET /files/{id}/export` · `GET /files/{id}?alt=media` | `export_text()` | text extraction (Doc/Sheet/Slide export; PDF/text via media) | [client.py:254-288](../../../services/ingest/integrations/google_drive/client.py#L254-L288) |
| `GET /files/{id}/comments` | `list_comments()` | comments + nested replies | [client.py:290-316](../../../services/ingest/integrations/google_drive/client.py#L290-L316) |
| `GET /files/{id}/revisions` | `list_revisions()` | edit timeline | [client.py:318-341](../../../services/ingest/integrations/google_drive/client.py#L318-L341) |
| `GET /changes?…&pageSize=1` | `has_changes_since()` | reconciler gap probe | [client.py:343-356](../../../services/ingest/integrations/google_drive/client.py#L343-L356) |
| `POST /changes/watch` · `POST /channels/stop` | `watch_changes()` · `stop_channel()` | opt-in push channel (§8) | [client.py:358-409](../../../services/ingest/integrations/google_drive/client.py#L358-L409) |

The metadata `fields` selector is shared verbatim between `files.list` and
`changes.list` so the handler sees one uniform file shape
([client.py:52-57](../../../services/ingest/integrations/google_drive/client.py#L52-L57)).

### 3.1 Pagination — `pageToken` based

Every list endpoint paginates the same way: request `?pageSize=200&pageToken=…`,
read `nextPageToken` from the response, stop when it is empty (`pageSize` caps at
Drive's 1000; default 200 — [client.py:44-45](../../../services/ingest/integrations/google_drive/client.py#L44-L45)).

- `list_shared_drives` loops **to completion** internally at onboarding
  ([onboarding.py:100-116](../../../services/ingest/integrations/google_drive/onboarding.py#L100-L116)).
- `list_files` / `list_changes` return **one page** plus the next token to the
  fetcher, which persists it in the shard cursor and resumes next invocation
  (the N1 "exactly one HTTP fetch per call" contract —
  [fetchers/google_drive.py:1-23](../../../services/ingest/ingestion/fetchers/google_drive.py#L1-L23)).
- `changes.list` additionally returns `newStartPageToken` on its **last page** —
  the warm start for the next run.

### 3.2 Rate limits — no dedicated bucket

There is **no `google_drive` entry in the client-side token-bucket table**
([rate_limit/buckets.py:82-90](../../../services/ingest/ingestion/rate_limit/buckets.py#L82-L90)
declares only `slack`, `github`, `gmail`, `discord`). Drive relies entirely on
the shared `GoogleHttpClient`'s server-driven backpressure: a `429` / `5xx` /
`403 quotaExceeded` becomes a `GoogleRateLimited`, which the fetcher wraps in
`retry_with_backoff_on_429` (exponential backoff, default `max_attempts=5`, base
1 s, cap 60 s — [workflows/retry.py:83-102](../../../services/ingest/ingestion/workflows/retry.py#L83-L102)).
When the retry budget is spent the fetcher leaves the cursor unadvanced and
returns an empty, non-terminal page so ShardFetch re-enters next tick
([fetchers/google_drive.py:345-355](../../../services/ingest/ingestion/fetchers/google_drive.py#L345-L355)).

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one `google_drive_files` shard per
active target** ([planners/google_drive.py:47-79](../../../services/ingest/ingestion/planners/google_drive.py#L47-L79)).
A "target" is one drive — a user's **My Drive** or a **Shared Drive**:

| Target kind | `drive_kind` | `drive_id` | Enumerated by |
|-------------|--------------|------------|---------------|
| My Drive (per resolved user) | `my_drive` | `my-drive` (the `MY_DRIVE_SENTINEL`) | inclusion_spec → Directory API (`resolve_inclusion`) |
| Shared Drive (per org drive) | `shared_drive` | the Drive's id | `drives.list?useDomainAdminAccess` (impersonating the first resolved user) |

The planner reads **DB state only** — `ctx.source_client` is `None`. The active
target list is pre-aggregated by the SourceOnboarding loader into
`ctx.install["targets"]` (mirroring Gmail/Calendar), so the planner stays
stateless and does no DB I/O
([planners/google_drive.py:1-12](../../../services/ingest/ingestion/planners/google_drive.py#L1-L12),
[planners/google_drive.py:30-44](../../../services/ingest/ingestion/planners/google_drive.py#L30-L44)).

Each shard carries `drive_kind`, `drive_id`, `owner_email`, `installation_id`,
an optional warm-start `start_page_token`, at baseline `recency_score=1.0`
([planners/google_drive.py:61-73](../../../services/ingest/ingestion/planners/google_drive.py#L61-L73)).
A target with no `owner_email` is skipped.

---

## 5. One shard kind, two sync modes (the fetcher)

`fetch_page_google_drive` ([fetchers/google_drive.py:278-445](../../../services/ingest/ingestion/fetchers/google_drive.py#L278-L445))
fetches one page, advances the cursor, extracts text, and shapes each item into
the handler's record. The same fetcher serves **backfill, poll, push, and
reconciler reshares** — the only difference is whether the cursor carries a
`start_page_token`.

### 5.1 Cursor

```python
class GoogleDriveCursor:
    page_token: str | None             # nextPageToken within the current run
    start_page_token: str | None       # the ACTIVE incremental token (set ⇒ incremental mode)
    next_start_page_token: str | None  # warm start for the next run + reconciler reference
    time_min: str | None               # windowed-backfill lower bound, frozen on first call
    files_seen: int                    # diagnostic
    seeded: bool                       # whether first-call setup has run
```

([fetchers/google_drive.py:110-135](../../../services/ingest/ingestion/fetchers/google_drive.py#L110-L135)).

### 5.2 FULL mode (initial backfill)

On the first seeded call with **no** warm token, the fetcher
([fetchers/google_drive.py:299-321](../../../services/ingest/ingestion/fetchers/google_drive.py#L299-L321)):

1. **Captures `changes.getStartPageToken` UP FRONT** and stashes it in
   `next_start_page_token`, *before* walking files — so edits made *during* the
   backfill window are caught by the first poll (the canonical Drive ordering).
2. Freezes the window lower bound `time_min = now − GOOGLE_DRIVE_BACKFILL_DAYS`
   (default 180).
3. Walks `files.list` with `q="trashed = false and modifiedTime > '{time_min}'"`,
   `orderBy=modifiedTime`, paged via `pageToken`
   ([client.py:181-219](../../../services/ingest/integrations/google_drive/client.py#L181-L219)).

### 5.3 INCREMENTAL mode (poll / reshare / warm start)

When the cursor (or the planner-seeded shard) carries a `start_page_token`, the
fetcher calls `changes.list?pageToken=…&includeRemoved=true`, which returns
**only changed/removed files** since the token, plus a `newStartPageToken` on the
last page ([fetchers/google_drive.py:323-411](../../../services/ingest/ingestion/fetchers/google_drive.py#L323-L411)).
`end_of_data=True` when a page has no `nextPageToken`.

### 5.4 Stale page token → full reseed

An aged-out page token yields **HTTP 410 (or 400 invalid)**. The fetcher catches
it, builds a fresh unseeded cursor (preserving `next_start_page_token`), and
returns an empty cursor-reset page so ShardFetch re-enters and runs a fresh
windowed full sync. Dedup makes the re-walk idempotent
([fetchers/google_drive.py:356-373](../../../services/ingest/ingestion/fetchers/google_drive.py#L356-L373)).

### 5.5 Content extraction + collaboration records (per file)

For each non-trashed file the fetcher does best-effort enrichment **in the
fetcher** so the handler stays a pure function:

- **`_maybe_extract`** injects `_fyralis_extracted_text` — Doc/Sheet/Slide via
  `files.export`, PDF via `alt=media` + pypdf, `text/*` via `alt=media`. Other
  binary types (images, video, archives) are metadata-only. Bounded by
  `GOOGLE_DRIVE_EXTRACT_MAX_BYTES`, `GOOGLE_DRIVE_PDF_MAX_PAGES`, and a
  `GOOGLE_DRIVE_MAX_EXTRACT_FILE_BYTES` size guard. Any failure is logged + counted
  and leaves the metadata observation intact
  ([fetchers/google_drive.py:164-203](../../../services/ingest/ingestion/fetchers/google_drive.py#L164-L203)).
- **`_collab_records`** fetches comments (`GOOGLE_DRIVE_FETCH_COMMENTS`,
  default on) and revisions (`GOOGLE_DRIVE_FETCH_REVISIONS`, default on) as
  **separate records** tagged `_fyralis_record_type`, skipped for folders /
  removed files ([fetchers/google_drive.py:205-261](../../../services/ingest/ingestion/fetchers/google_drive.py#L205-L261)).

Every record is `_stamp`'d with `_fyralis_drive_id`, `_fyralis_drive_kind`,
`_fyralis_owner_email`, `_fyralis_removed`, and (for incremental changes)
`_fyralis_change_time` ([fetchers/google_drive.py:264-275](../../../services/ingest/ingestion/fetchers/google_drive.py#L264-L275)).
A removed/lost-access change carries only a `fileId`, so the fetcher synthesizes a
minimal `{"id": fileId}` record with `removed=True`
([fetchers/google_drive.py:389-398](../../../services/ingest/ingestion/fetchers/google_drive.py#L389-L398)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_google_drive_file` ([handlers/google_drive.py:127-143](../../../services/ingest/ingestion/handlers/google_drive.py#L127-L143))
is the single registered handler for `google_drive:file`; it dispatches on the
injected `_fyralis_record_type` to one of three builders. Trust posture: Drive is
the system of record for file existence/metadata, so **all three are
`authoritative`** ([handlers/google_drive.py:66](../../../services/ingest/ingestion/handlers/google_drive.py#L66),
[handlers/google_drive.py:441](../../../services/ingest/ingestion/handlers/google_drive.py#L441)).

| Record (`_fyralis_record_type`) | Builder | `source_channel` | `content.object_type` | `external_id` | `occurred_at` | `kind` | Trust |
|---|---|---|---|---|---|---|---|
| `file` | `_build_file_draft` | `google_drive:file` | `file` | `gdrive:{file_id}:{version}` (or `…:removed:{change_time\|now}`) | `modifiedTime` → `change_time` → `createdTime` → now | `state_change` if removed/trashed, else `signal` | authoritative |
| `comment` | `_build_comment_draft` | `google_drive:file` | `comment` | `gdrive-comment:{file_id}:{comment_id}:{modifiedTime}` | `modifiedTime` → `createdTime` → now | `state_change` if resolved, else `signal` | authoritative |
| `revision` | `_build_revision_draft` | `google_drive:file` | `revision` | `gdrive-revision:{file_id}:{revision_id}` | `modifiedTime` → now | always `signal` | authoritative |

Highlights:

- **`source_actor_ref`** is `email:{last_modifier or primary_owner}` for files,
  `email:{author}` for comments, `email:{editor}` for revisions; `None` when no
  email is known (Drive often omits author email on comments for privacy)
  ([handlers/google_drive.py:276-279](../../../services/ingest/ingestion/handlers/google_drive.py#L276-L279),
  [handlers/google_drive.py:101-111](../../../services/ingest/ingestion/handlers/google_drive.py#L101-L111)).
- **`content_text`** embeds **both** a legible activity line *and* the extracted
  body, so the semantic layer can reason over what the document says, not just
  that it changed ([handlers/google_drive.py:194-206](../../../services/ingest/ingestion/handlers/google_drive.py#L194-L206)).
- **`entities_hint`** carries typed `email_address` refs with roles
  (`owner` / `editor` / `shared_with` / `commenter`) and an `external` flag (true
  when the address's domain differs from the owner's), plus a `document` entity
  ([handlers/google_drive.py:208-233](../../../services/ingest/ingestion/handlers/google_drive.py#L208-L233),
  [handlers/google_drive.py:114-118](../../../services/ingest/ingestion/handlers/google_drive.py#L114-L118)).
- A payload that is not a JSON object, or a record missing its `id`, is rejected
  with a `ValidationError`
  ([handlers/google_drive.py:134-152](../../../services/ingest/ingestion/handlers/google_drive.py#L134-L152)).

### The versioned-`external_id` landmine

The observations repo dedups on `(source_channel, external_id)` **ignoring
`occurred_at`**, but Drive files mutate constantly (rename, edit, move, re-share,
trash). Keying on `file_id` alone would silently drop every edit. Keying on
Drive's monotonic `version` instead means
([handlers/google_drive.py:28-42](../../../services/ingest/ingestion/handlers/google_drive.py#L28-L42)):

- identical re-fetches (backfill twin == poll twin) collapse to one observation
  (same `version`);
- an edit (version bumps) lands a **new** observation so the activity signal +
  fresh content stay current;
- a trash/removal lands a **new** observation with `kind=state_change`.

---

## 7. Live (poll) ingestion — the canonical live path

Drive has **no vendor-pushed content webhook in the canonical v1 design**. The
"live" path is a **poll**: the `google_drive_live_poller` process leases active,
cursor-seeded targets on a short cadence and drains the Changes delta through the
*same* fetcher + `core.ingest` path as backfill, so a poll-driven and a
backfilled observation are indistinguishable and dedup at `observations.UNIQUE`
([live_poller.py:1-18](../../../services/ingest/integrations/google_drive/live_poller.py#L1-L18)).

### 7.1 Leasing

`_lease_due_targets` claims `google_drive_targets` rows that are `state='active'`,
have a non-NULL `start_page_token`, belong to a non-disabled install, and whose
`last_live_poll_at` is older than `_POLL_GAP_S` (120 s) — using
`FOR UPDATE … SKIP LOCKED` over the `last_live_poll_at` claim slot
([live_poller.py:50-79](../../../services/ingest/integrations/google_drive/live_poller.py#L50-L79)).
The loop ticks every 60 s by default
([live_poller.py:38-44](../../../services/ingest/integrations/google_drive/live_poller.py#L38-L44)).

### 7.2 Drain + cursor advance

`poll_one` calls the shared `drain_live`, which runs the fetcher incrementally
from `start_page_token`, ingests each record, and returns the advanced token
(bounded at `_MAX_PAGES=200` per drain). On success the target's
`start_page_token` is advanced and `consecutive_live_failures` reset; on
`GoogleRateLimited`/`GoogleApiError` the failure counter bumps and the target
flips to `errored` after `_MAX_FAILURES=5`
([live_poller.py:82-154](../../../services/ingest/integrations/google_drive/live_poller.py#L82-L154),
[_google_live.py:32-82](../../../services/ingest/integrations/_google_live.py#L32-L82)).

---

## 8. Opt-in native push channel (`changes.watch`)

> See the §1 `TODO(human)` callout: this path is **present and wired in code**
> but is described as nonexistent by three canonical comments. It is documented
> here as it exists; treat it as an opt-in latency optimization layered over the
> poll, **not** as the guaranteed liveness path.

A second `live-source` process, `google_drive_watch_scheduler`
([process_manifest.py:302-309](../../../services/platform/runtime/process_manifest.py#L302-L309)),
opens a native Drive push channel via the generic engine in `_google_watch.py`,
parameterised by a per-source `WatchSpec`
([google_drive/watch.py:69-89](../../../services/ingest/integrations/google_drive/watch.py#L69-L89)):

1. **Register.** For each due target the scheduler mints a `channel_id` + shared
   `token`, POSTs `changes.watch?pageToken=…` with a `web_hook` `address`, and
   persists `{watch_channel_id, watch_resource_id, watch_token, watch_expiration,
   watch_state='active'}` on the target row. Channels are requested with a 7-day
   TTL and renewed inside a 24 h window
   ([_google_watch.py:42-44](../../../services/ingest/integrations/_google_watch.py#L42-L44),
   [_google_watch.py:123-174](../../../services/ingest/integrations/_google_watch.py#L123-L174)).
   **Gating:** the `web_hook` address is derived from `GOOGLE_PUSH_WEBHOOK_BASE`;
   when that env var is unset the scheduler logs `disabled_no_address` and idles —
   *"the live poller is the liveness path"*
   ([_google_watch.py:68-75](../../../services/ingest/integrations/_google_watch.py#L68-L75),
   [_google_watch.py:206-222](../../../services/ingest/integrations/_google_watch.py#L206-L222)).
2. **Ingress.** Google pings a content-less `POST /webhooks/google_drive/push`
   carrying `X-Goog-Channel-ID` / `X-Goog-Channel-Token` / `X-Goog-Resource-State`.
   The handler verifies the token (constant-time, via `resolve_push`), acks the
   initial `state=sync` handshake with no work, and on a real ping drains the
   delta via the **same** `drain_live` the poller uses. It **always returns 200**
   — unknown channel, token mismatch, or transient drain error are swallowed,
   because the poller is the safety net and a non-2xx would only make Google retry
   ([app/webhooks/google_push.py:1-19](../../../services/app/webhooks/google_push.py#L1-L19),
   [app/webhooks/google_push.py:48-85](../../../services/app/webhooks/google_push.py#L48-L85),
   [_google_watch.py:239-297](../../../services/ingest/integrations/_google_watch.py#L239-L297)).

Because both push and poll funnel through `drain_live`, a push-driven and a
poll-driven observation are indistinguishable and dedup at the same
`(source_channel, external_id)` key.

> Drive is **not** in any inbound-signature `VERIFIERS` registry (no such
> registry references `google_drive`, verified by grep). The push channel's
> authentication is the per-channel `X-Goog-Channel-Token` we set at watch time,
> not an HMAC over the body (the body is empty).

---

## 9. Reconciliation — gap detection

`reconcile_google_drive` ([reconcilers/google_drive.py:134-172](../../../services/ingest/ingestion/reconcilers/google_drive.py#L134-L172))
re-checks completed (`state='done'`) drive shards for new activity. For each, it
loads the shard's captured `next_start_page_token` from `workflow_states` and
issues **one cheap `changes.list?pageToken=<token>&pageSize=1&includeRemoved=true`
probe** via `has_changes_since`
([reconcilers/google_drive.py:86-131](../../../services/ingest/ingestion/reconcilers/google_drive.py#L86-L131),
[client.py:343-356](../../../services/ingest/integrations/google_drive/client.py#L343-L356)):

- **No reference token** (none captured) → nothing to compare → skip.
- **Probe finds a change** → reshare a `google_drive_files` shard at
  **`recency_score=1.5`**, *warm-started* from the captured token so it runs an
  incremental (delta) walk rather than a full backfill. `external_id` parity means
  re-walked files dedup against what backfill already wrote — only genuinely
  new/changed files produce new observations.

This is pragmatic v1: the probe can **over-reshare but never under-reshares**, and
dedup makes re-walks idempotent
([reconcilers/google_drive.py:1-19](../../../services/ingest/ingestion/reconcilers/google_drive.py#L1-L19)).
A trash counts as a change (`includeRemoved=true`).

The reconciler resolves the install per tenant from `google_drive_installations`
where `disabled_at IS NULL`, opens one shared client, and probes all active
shards under it ([reconcilers/google_drive.py:141-165](../../../services/ingest/ingestion/reconcilers/google_drive.py#L141-L165)).

---

## 10. Revocation / recoverable-error behavior

There is **no dedicated Drive revocation chokepoint** of the GitHub
`_maybe_disable_on_revocation` kind (verified — no such helper exists in the Drive
integration). Failure handling is layered instead:

- **Backfill fetch:** rate-limit / quota errors (`GoogleRateLimited`) leave the
  cursor unadvanced and return a non-terminal empty page so the shard re-enters
  later ([fetchers/google_drive.py:345-355](../../../services/ingest/ingestion/fetchers/google_drive.py#L345-L355));
  a `400/410` stale token triggers a full reseed (§5.4); other `CompanyOSError`s
  propagate.
- **Live poll:** repeated `GoogleApiError`/`GoogleRateLimited` bump
  `consecutive_live_failures`; after `_MAX_FAILURES=5` the **target row** flips to
  `state='errored'` and stops being leased
  ([live_poller.py:136-154](../../../services/ingest/integrations/google_drive/live_poller.py#L136-L154)).
- **Watch:** a `*.watch` failure flips `watch_state='errored'` on the row but
  leaves the target itself active (the poller still covers it)
  ([_google_watch.py:177-190](../../../services/ingest/integrations/_google_watch.py#L177-L190)).
- **Install-level disable:** `google_drive_installations.disabled_at` (set by the
  connect upsert / re-onboard, [onboarding.py:158-166](../../../services/ingest/integrations/google_drive/onboarding.py#L158-L166))
  is the master switch — the live poller, watch scheduler, and reconciler all
  filter on `gi.disabled_at IS NULL`.

> **(inferred)** Because a DWD grant is revoked **out-of-band** in the Workspace
> Admin Console (not via an OAuth token Fyralis holds), recovery is "re-configure
> the DWD grant + re-run `connect/finalize`," which `disabled_at = NULL`s the
> install. This mirrors the Gmail/Calendar substrate; it is not spelled out in the
> Drive code itself.

---

## 11. End-to-end summary

```
                       ┌─────────────────────── BACKFILL (pull) ───────────────────────┐
                       │  DWD: service account ─► impersonate user ─► Bearer token       │
   ALL DRIVE TARGETS   │  connect/finalize: My-Drive (per user) + Shared-Drive targets   │
   (my_drive +         │     └─► one google_drive_files shard per target                 │
    shared_drive)      │  fetcher FULL: capture changes.getStartPageToken UP FRONT,      │
                       │     then files.list (q: trashed=false & modifiedTime>now-N d)   │
                       │     └─► extract text (Doc/Sheet/Slide/PDF/text) + comments/revs  │
                       └────────────────────────────────────────────────────────────┬───┘
                                                                                      │
                       ┌─────────────────────── LIVE (poll) ───────────────────────┐ │
   short-cadence  ─────►  google_drive_live_poller leases due targets               │ │
   worker (60s)        │     fetcher INCREMENTAL: changes.list?pageToken=… (delta)  │ │
                       │     └─► same drain_live + core.ingest as backfill          │ │
                       └────────────────────────────────────────────────────────────┘ │
                       ┌─── OPT-IN PUSH (gated on GOOGLE_PUSH_WEBHOOK_BASE) ─────────┐ │
   Drive ping ─────────►  POST /webhooks/google_drive/push (X-Goog-* headers)        │ │
                       │     verify X-Goog-Channel-Token → drain_live (same path)    │ │
                       └────────────────────────────────────────────────────────────┘ │
                                                                                      │
                                                  ┌───────────────────────────────────▼─┐
                                                  │  handle_google_drive_file            │
                                                  │  dispatch on _fyralis_record_type    │
                                                  │  channel = google_drive:file (all 3) │
                                                  │  external_id = gdrive:{file}:{version}│
                                                  │  → ObservationDraft (authoritative)  │
                                                  └──────────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Files, comments, and revisions all land
   on `google_drive:file`, distinguished by `content.object_type` + the
   `external_id` namespace. A backfilled record and its live (poll/push) twin
   dedup to one observation via the versioned `external_id`.
2. **One credential model.** A single Workspace **DWD service account** impersonates
   users at `drive.readonly` and mints per-call bearer tokens. No OAuth code
   exchange, no stored per-drive secret.
3. **One shard kind, two sync modes.** `google_drive_files` runs FULL
   (`files.list` windowed by `modifiedTime`, with the start-page-token captured up
   front) or INCREMENTAL (`changes.list` from the token); the *same* fetcher serves
   backfill, poll, push, and reconciler reshares.
4. **Versioned `external_id` for a mutable source.** Drive's monotonic `version`
   keys file observations so an edit re-observes while identical re-fetches dedup;
   removals key on `change_time`.
5. **Poll is the guaranteed live path** (the watch push channel is opt-in and
   gated on `GOOGLE_PUSH_WEBHOOK_BASE`; both funnel through `drain_live`).
6. **`pageToken` pagination + server-driven backpressure** — no dedicated
   client-side rate-limit bucket; `429`/`5xx`/`403 quotaExceeded` → `GoogleRateLimited`
   → bounded exponential backoff; stale page token (`410`/`400`) → full reseed.

---

## 12. Configuration & compliance

Verified against Google's Drive v3 docs (Changes API, DWD, push notifications).

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `GMAIL_SERVICE_ACCOUNT_CLIENT_ID` | — (required) | DWD client ID (numeric); surfaced in the Admin-Console remediation payload |
| `GOOGLE_DRIVE_API_BASE_URL` | `https://www.googleapis.com/drive/v3` | Drive v3 base; pointed explicitly at Provider Lab in dev (`endpoint("google_drive_api")`) |
| `GOOGLE_DRIVE_BACKFILL_DAYS` | `180` | windowed-backfill horizon (`modifiedTime > now − N days`) |
| `GOOGLE_DRIVE_EXTRACT_MAX_BYTES` | `65536` | cap on extracted text per file |
| `GOOGLE_DRIVE_PDF_MAX_PAGES` | `50` | cap on PDF pages parsed by pypdf |
| `GOOGLE_DRIVE_MAX_EXTRACT_FILE_BYTES` | `10485760` (10 MB) | skip text extraction for files larger than this |
| `GOOGLE_DRIVE_FETCH_COMMENTS` | on | emit per-file `comment` records |
| `GOOGLE_DRIVE_FETCH_REVISIONS` | on | emit per-file `revision` records |
| `GOOGLE_PUSH_WEBHOOK_BASE` | — (unset) | public base for the opt-in `changes.watch` push channel; unset ⇒ poll-only |

### 12.2 Verified compliant

- **Auth** — Workspace DWD: signed JWT (`sub=user`, `scope=drive.readonly`) →
  impersonated bearer token, cached per `(sa, user, scopes)`, re-minted on 401. ✅
- **Incremental sync** — `changes.getStartPageToken` captured up front;
  `changes.list?pageToken=…&includeRemoved=true`; `newStartPageToken` carried
  forward; stale token (`410`/`400`) → windowed full reseed. ✅
- **Pagination** — `nextPageToken` everywhere, `pageSize=200` (Drive cap 1000). ✅
- **Shared Drives** — `supportsAllDrives=true` + `corpora=drive&driveId=…` +
  `includeItemsFromAllDrives=true`. ✅
- **Push (opt-in)** — `changes.watch` with a per-channel token; ingress verifies
  `X-Goog-Channel-Token` constant-time and always 200s; channels TTL'd + renewed. ✅
- **Least secret surface** — no stored per-drive token; DWD grant lives in the
  customer's Admin Console. ✅

### 12.3 Dev / Provider Lab mode

Drive has **no `_clients.py` builder branch and no token preseed** (unlike
GitHub/Slack/Discord/Notion — verified: `_clients.py` has no `google_drive`
reference). The fetcher always builds a real `GoogleDriveClient` over the shared
Gmail DWD minter via `_open_drive_client`
([fetchers/google_drive.py:147-161](../../../services/ingest/ingestion/fetchers/google_drive.py#L147-L161)).
Dev/Provider Lab testing is **pure config**: set `GOOGLE_DRIVE_API_BASE_URL` to
the explicit Provider Lab path `/gdrive/drive/v3`,
[endpoints.py:156](../../../lib/integrations/endpoints.py#L156)) points the client
at the local Drive mock while auth still flows through the (sandbox) minter. The
sandbox harness (`scripts/sandbox_google_drive.py`) drives the real minter →
fetcher (with text export) → ingest end-to-end against a throwaway Postgres
([sources/google-drive.md:67-70](../sources/google-drive.md)).
