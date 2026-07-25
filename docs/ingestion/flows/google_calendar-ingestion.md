# Google Calendar Ingestion — How Fyralis Pulls Google Calendar Data

This document explains, in detail, **how Google Calendar data enters Fyralis**:
which Calendar v3 REST APIs are called, with which credential, and how a single
signal surface — **calendar events** (created, rescheduled, cancelled) on each
included user's primary calendar — is ingested.

It deliberately stops at the point where a Calendar event becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Scope note.** This doc covers the `google_calendar:event` channel (IN-15) —
> the production backfill + incremental/poll + native-push pipeline that lives in
> `services/ingest/integrations/google_calendar/` and the matching
> planner/fetcher/handler/reconciler quartet. There is a **separate, older**
> `calendar:sync` channel
> ([handlers/calendar.py](../../../services/ingest/ingestion/handlers/calendar.py#L63))
> from an earlier build plan that expects a pre-fetched `{action, event}` payload
> and a `CALENDAR_WEBHOOK_TOKEN`. That channel is **not** the path described here
> and is not wired into the `google_calendar` source pipeline; don't conflate them.

---

## 1. The two ways data arrives

Google Calendar data reaches Fyralis through **two cursor-sharing modes that
converge on one handler** — and, optionally, a third (push) ingress that drains
through the *same* fetcher. Calendar is a Google Workspace API on the **shared
Gmail Domain-Wide-Delegation (DWD) auth substrate**; it has **no native HTTP
push by default**, and even when push is enabled it reuses the poll machinery.

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* a windowed event history via the Calendar **v3 REST API** (`events.list?timeMin=…`) | [planners/google_calendar.py](../../../services/ingest/ingestion/planners/google_calendar.py), [fetchers/google_calendar.py](../../../services/ingest/ingestion/fetchers/google_calendar.py) |
| **Live (poll)** | Cadence (every ~120 s per calendar) | Fyralis *re-runs the same fetcher* incrementally using Google's native **`syncToken`** — there is **no webhook in this path** | [google_calendar/live_poller.py](../../../services/ingest/integrations/google_calendar/live_poller.py), [_google_live.py](../../../services/ingest/integrations/_google_live.py) |
| **Live (push, opt-in)** | A `events.watch` channel pings Fyralis | Content-less `X-Goog-*` ping → `drain_push` → **the same fetcher** drains the `syncToken` delta | [_google_watch.py](../../../services/ingest/integrations/_google_watch.py), [webhooks/google_push.py](../../../services/app/webhooks/google_push.py) |

The first two are the **only** ingresses the channel map knows about
([normalizer/channel_mapping.py:82-83](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L82-L83)):

```
("google_calendar", "backfill") → "google_calendar:event"
("google_calendar", "poll")     → "google_calendar:event"
```

There is **no `("google_calendar", "webhook")` mapping** and **no
`google_calendar` entry in the webhook `VERIFIERS` registry**
([signatures/__init__.py:44-68](../../../services/app/webhooks/signatures/__init__.py#L44-L68)) —
the push edge is a self-contained ingress (§9) that *bypasses* the normalizer by
calling `ingest()` directly via `drain_live`, not a `VERIFIERS`-dispatched
webhook like Slack/GitHub/Notion.

Crucially, **all three modes produce the exact same record shape** — the **RAW
Calendar v3 event object** plus two injected private keys (`_fyralis_calendar_id`,
`_fyralis_owner_email`) — and all three are parsed by the **single**
`google_calendar:event` handler
([handlers/google_calendar.py:131-250](../../../services/ingest/ingestion/handlers/google_calendar.py#L131-L250)).
All three derive the **same** dedup key — a **VERSIONED** `external_id`
([idempotency/__init__.py:89-95](../../../services/ingest/ingestion/idempotency/__init__.py#L89-L95)):

```
external_id = "gcal:{calendar_id}:{event_id}:{status}:{start_instant}"
```

> **Note — the external_id is versioned, not bare.** The shorter form
> `gcal:{calendar_id}:{event_id}` appears in the `channel_mapping.py` comment and
> the source-summary doc, but the *actual* key emitted by the handler and the
> `idempotency` helper appends `:{status}:{start_instant}` (verified in
> [handlers/google_calendar.py:164-167](../../../services/ingest/ingestion/handlers/google_calendar.py#L164-L167)).
> Calendar events **mutate** (cancel / reschedule / re-RSVP), and the observations
> repo dedups on `(source_channel, external_id)` *ignoring* `occurred_at` — so the
> version discriminator is what makes a cancellation or a reschedule land as a
> *new* observation while identical re-fetches and RSVP-only churn collapse.

So an event that is both backfilled *and* re-seen by the poll (with the same
`status` + start) collapses into **one** observation. This is the central design
invariant of Google Calendar ingestion — **one channel, one dedup namespace,
identical record shape across every ingress** (D3 / D7).

---

## 2. Authentication & token model — shared Gmail DWD substrate (D1)

Unlike Slack (per-user/bot OAuth tokens) or GitHub (a GitHub App), Google Calendar
uses **no per-user or per-installation OAuth token at all**. It reuses the **Gmail
Domain-Wide-Delegation service account**: one service account, granted the
Calendar read scope in the customer's Workspace Admin Console, that **impersonates
each calendar owner** at ingest time.

### 2.1 The minted-token flow

1. **Scope.** Calendar exposes exactly one read scope today:
   `https://www.googleapis.com/auth/calendar.readonly`, stored on the install as
   the alias `calendar.readonly`
   ([client.py:29-34, 41-48](../../../services/ingest/integrations/google_calendar/client.py#L29-L48)).
2. **JIT impersonation token.** The shared `DwdTokenMinter.mint(user_email, scopes)`
   signs an RS256 JWT (`sub=user_email`, `scope=calendar.readonly`) against the
   service-account private key and exchanges it for a per-(service-account, user,
   scopes) bearer token, cached with TTL headroom and guarded by a per-key
   `asyncio.Lock` against a mint stampede
   ([gmail/dwd.py:162-195](../../../services/ingest/integrations/gmail/dwd.py#L162-L195)).
3. **Read calls** carry `Authorization: Bearer {token}` via the shared
   `GoogleHttpClient.request`, which on a **`401`** invalidates the cached token
   and retries **once** (covers a rotated/revoked grant)
   ([gmail/client.py:101-115](../../../services/ingest/integrations/gmail/client.py#L101-L115)).

The auth model (D1) is: `user_email` = **who we impersonate** (the calendar owner);
`calendar_id` = **what we read** (a user's primary calendar is addressed *by their
email*) ([client.py:1-21](../../../services/ingest/integrations/google_calendar/client.py#L1-L21)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Service-account key | `GMAIL_SERVICE_ACCOUNT_JSON_FILE` **or** `GMAIL_SERVICE_ACCOUNT_JSON` | shared with Gmail/Drive; loaded by `ServiceAccountKey.from_env` ([gmail/dwd.py:82-120](../../../services/ingest/integrations/gmail/dwd.py#L82-L120)) |
| DWD client ID | `GMAIL_SERVICE_ACCOUNT_CLIENT_ID` | numeric; surfaced to the admin for the Admin-Console grant ([google_calendar/oauth.py:215-221](../../../services/ingest/integrations/google_calendar/oauth.py#L215-L221)) |
| Install row | `google_calendar_installations` (per tenant + workspace_domain) | carries `scope`, `service_account_email`, `resolved_calendar_count` — **no token column** |
| Per-calendar rows | `google_calendar_calendars` | one row per included calendar; holds `sync_token`, `watch_*`, `last_live_poll_at` |

> **Contrast with Slack/GitHub.** There is **no `secret_ref` and no per-install
> token** — the only secret is the service-account key, and it is Workspace-global.
> Calendar is *not* in `provider_installations`; it lives in its own DWD-style
> tables and emits its own `onboarding_triggers` row (§5).

### 2.3 The admin connect wizard (how an install is registered)

`google_calendar/oauth.py` mirrors the **Gmail** connect/finalize shape (not the
slack/github OAuth bounce — the user never visits Google, because DWD is
pre-granted) ([oauth.py:1-35](../../../services/ingest/integrations/google_calendar/oauth.py#L1-L35)):

1. **`POST /integrations/google_calendar/connect/preflight`** (Bearer-authed) —
   impersonates `admin_email` at directory scopes and enumerates users / groups /
   org-units for the selector UI. If the DWD grant is missing it returns the exact
   `client_id` + scope strings to paste into the Admin Console
   ([oauth.py:124-156](../../../services/ingest/integrations/google_calendar/oauth.py#L124-L156)).
2. **`POST /integrations/google_calendar/connect/finalize`** — resolves the
   admin's `inclusion_spec` → concrete calendar emails (shared `DirectoryClient`
   + `resolve_inclusion`), then in **one tenant-scoped transaction** UPSERTs the
   install, one `google_calendar_calendars` row per resolved calendar, and an
   `onboarding_triggers` row (`source='google_calendar'`) that fires the existing
   M6 backfill chain (`oauth_poller → tenant_onboarding → source_onboarding`)
   ([oauth.py:159-212](../../../services/ingest/integrations/google_calendar/oauth.py#L159-L212),
   [onboarding.py:64-147](../../../services/ingest/integrations/google_calendar/onboarding.py#L64-L147)).
3. The router is mounted only when a service-account env var is present
   ([ceo_view_wiring.py:164-185](../../../services/app/gateway/ceo_view_wiring.py#L164-L185)).
   There is **no out-of-band provisioning step** — resolution + persistence
   complete inline (Calendar is poll-first; push is opt-in, §9).

---

## 3. The Calendar v3 REST API surface that is actually called

All read calls funnel through the **shared** `GoogleHttpClient` (owned by the
Gmail substrate); `GoogleCalendarClient` adds only the Calendar v3 request shapes
([client.py:1-7, 51-64](../../../services/ingest/integrations/google_calendar/client.py#L1-L64)).
The shared client:

- mints + attaches `Authorization: Bearer {token}` (DWD, §2.1),
- retries once on `401` (token invalidation),
- maps **`429` / `5xx`** and **`403` quota reasons** (`quotaExceeded`,
  `userRateLimitExceeded`, `rateLimitExceeded`) to a typed `GoogleRateLimited`,
  with `403` carrying a synthetic `retry_after_s=60`
  ([gmail/client.py:157-188](../../../services/ingest/integrations/gmail/client.py#L157-L188)),
- maps any other non-2xx to `GoogleApiError`.

The endpoints invoked for ingestion:

| Calendar v3 endpoint | Wrapper | Purpose | Code |
|----------------------|---------|---------|------|
| `GET /users/me/calendarList` | `list_calendars()` | confirm access during onboarding (v1 shards the primary calendar = the email) | [client.py:66-81](../../../services/ingest/integrations/google_calendar/client.py#L66-L81) |
| `GET /calendars/{calendarId}/events` (`timeMin`, `orderBy=startTime`) | `list_events(time_min=…)` | **FULL/windowed** backfill page | [client.py:83-135](../../../services/ingest/integrations/google_calendar/client.py#L83-L135) |
| `GET /calendars/{calendarId}/events` (`syncToken`, `showDeleted=true`) | `list_events(sync_token=…)` | **INCREMENTAL** delta page (poll/push) | ″ |
| `GET /calendars/{calendarId}/events` (`updatedMin`, `maxResults=1`, `showDeleted`) | `has_updates_since()` | reconciler "did anything change?" 1-row probe | [client.py:137-152](../../../services/ingest/integrations/google_calendar/client.py#L137-L152) |
| `POST /calendars/{calendarId}/events/watch` | `watch_events()` | open a native push channel (opt-in, §9) | [client.py:154-187](../../../services/ingest/integrations/google_calendar/client.py#L154-L187) |
| `POST /channels/stop` | `stop_channel()` | tear down a push channel (idempotent) | [client.py:189-199](../../../services/ingest/integrations/google_calendar/client.py#L189-L199) |

The base URL is resolved via `endpoint("google_calendar_api")` so it can point at
Provider Lab in dev (§12.3); production default is
`https://www.googleapis.com/calendar/v3`
([endpoints.py:44](../../../lib/integrations/endpoints.py#L44)).

### 3.1 Pagination — `pageToken` + `nextSyncToken`

Every `events.list` call pages with `pageToken` and returns the raw body
`{items, nextPageToken, nextSyncToken}`. The fetcher persists `nextPageToken` in
its cursor and resumes on the next invocation; the **final** page of a full or
incremental sync returns a `nextSyncToken`, which is the warm start for the next
incremental run ([fetchers/google_calendar.py:237-243](../../../services/ingest/ingestion/fetchers/google_calendar.py#L237-L243)).
Page size defaults to `250` (Calendar caps at 2500; kept conservative to bound a
single fetch wall budget) ([client.py:36-38](../../../services/ingest/integrations/google_calendar/client.py#L36-L38)).

**Mutually-exclusive modes.** Google rejects `syncToken` combined with
`timeMin`/`orderBy`, so `list_events` puts time bounds + ordering *only* on the
full-sync branch and `syncToken` on the incremental branch — never both
([client.py:99-135](../../../services/ingest/integrations/google_calendar/client.py#L99-L135)).

### 3.2 Rate limits — **no dedicated bucket**

There is **no `("google_calendar", …)` entry in the client-side token-bucket
table** ([rate_limit/buckets.py](../../../services/ingest/ingestion/rate_limit/buckets.py) —
only `("gmail","per-user")` exists for the Google family, *not* shared by
Calendar). Calendar throttling is handled **reactively**, two ways:

- the shared `GoogleHttpClient` maps `429`/`403`-quota → `GoogleRateLimited`
  (§3 above), and
- the fetcher wraps each `events.list` in `retry_with_backoff_on_429`
  (exponential backoff, `max_attempts=5`, base 1 s, cap 60 s by default), then —
  if the budget is spent — leaves the cursor unadvanced and ends the round empty
  so ShardFetch re-enters next tick
  ([fetchers/google_calendar.py:171-204](../../../services/ingest/ingestion/fetchers/google_calendar.py#L171-L204),
  [workflows/retry.py:83-130](../../../services/ingest/ingestion/workflows/retry.py#L83-L130)).

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per active calendar**, all of
`shard_kind = "google_calendar_events"`
([planners/google_calendar.py:48-79](../../../services/ingest/ingestion/planners/google_calendar.py#L48-L79)).
There is exactly **one shard family** (contrast GitHub's two fetch classes).

The planner is **stateless / no DB I/O** (`ctx.source_client is None`): the
SourceOnboarding loader JSON-aggregates the `google_calendar_calendars` rows into
`ctx.install["calendars"]`, exactly like the Gmail mailbox aggregation. Each shard
carries:

```python
shard_identifier = {
    "shard_kind":      "google_calendar_events",
    "calendar_id":     cal["calendar_id"],
    "owner_email":     cal["owner_email"] or calendar_id,   # who we impersonate
    "installation_id": install_id,
    "sync_token":      cal["sync_token"],   # NULL on first backfill; set → warm-start incremental
}
# recency_score = 1.0 (baseline)
```

A calendar whose `sync_token` is already seeded (a prior backfill finished) makes
the shard **warm-start straight into incremental mode** — that is how a reshare
or a re-onboard avoids re-walking the whole window.

---

## 5. The fetcher — one shard kind, two sync modes

`fetch_page_google_calendar`
([fetchers/google_calendar.py:141-255](../../../services/ingest/ingestion/fetchers/google_calendar.py#L141-L255))
fetches one page, advances an opaque cursor (persisted by ShardFetch between
calls, per the N1 invariant), and shapes each event for the handler.

### 5.1 Cursor

```python
class GoogleCalendarCursor:
    page_token: str | None            # nextPageToken within the current run
    sync_token: str | None            # ACTIVE incremental token (incremental when set)
    next_sync_token: str | None       # captured from the last page → warm start for next run
    time_min: str | None              # windowed-backfill lower bound, frozen on first call
    events_seen: int                  # diagnostic
    high_water_updated: str | None    # max event `updated` seen → reconciler reference (§8)
    seeded: bool                      # first-call setup done
```

([fetchers/google_calendar.py:79-104](../../../services/ingest/ingestion/fetchers/google_calendar.py#L79-L104)).

### 5.2 Mode selection (first-call setup)

On the first call the fetcher chooses the mode and freezes the window
([fetchers/google_calendar.py:155-166](../../../services/ingest/ingestion/fetchers/google_calendar.py#L155-L166)):

- **Warm `sync_token` on the shard** → incremental mode (poll/reshare/push).
- **Otherwise** → FULL mode: `time_min = now − GOOGLE_CALENDAR_BACKFILL_DAYS`
  (default **180** days, env-overridable), frozen so paging stays stable across
  ticks ([fetchers/google_calendar.py:71-77](../../../services/ingest/ingestion/fetchers/google_calendar.py#L71-L77)).

| Mode | Request | Page end | Token captured |
|------|---------|----------|----------------|
| **FULL** (initial backfill) | `events.list?timeMin=…&singleEvents=true&orderBy=startTime` | no `nextPageToken` → `end_of_data=True` | `nextSyncToken` on the last page → `next_sync_token` |
| **INCREMENTAL** (poll / push / reshare) | `events.list?syncToken=…&showDeleted=true` | no `nextPageToken` → `end_of_data=True` | refreshed `nextSyncToken` |

`singleEvents=true` expands recurring series into individual instances.
`showDeleted=true` on incremental is what lets a **cancellation** flow through as
a delta (a `status: cancelled` event).

### 5.3 Sync-token expiry — `410 GONE` → full reseed

An aged-out sync token yields **HTTP 410** (surfaced as
`GoogleApiError(status=410)`). The fetcher catches it *only in incremental mode*,
clears the token, switches to a fresh windowed FULL sync, and returns an empty
cursor-reset page so ShardFetch re-enters cleanly. Dedup makes the re-walk
idempotent ([fetchers/google_calendar.py:205-226](../../../services/ingest/ingestion/fetchers/google_calendar.py#L205-L226)).

### 5.4 Handler conformance — inject `_fyralis_*` keys

Each record is the **RAW Calendar event** plus two injected private keys so the
handler can derive a stable `external_id` and detect external attendees without a
second lookup ([fetchers/google_calendar.py:231-235](../../../services/ingest/ingestion/fetchers/google_calendar.py#L231-L235)):

```python
event["_fyralis_calendar_id"] = calendar_id   # the calendar this was read from
event["_fyralis_owner_email"] = owner_email    # the impersonated owner (domain → external check)
```

The client builder `_open_calendar_client` (the production seam, monkeypatched in
tests) constructs the client over the **shared Gmail DWD minter + `GoogleHttpClient`**
([fetchers/google_calendar.py:124-138](../../../services/ingest/ingestion/fetchers/google_calendar.py#L124-L138)).

> **Aside — no `_clients.py` branch.** Unlike github/slack/discord/notion, Calendar
> has **no builder in** `services/ingest/ingestion/fetchers/_clients.py` and **no
> Provider Lab token-preseed there**. It opens its own client through the Gmail DWD
> substrate; dev/Provider Lab redirection is **pure endpoint config** (§12.3), not a
> preseeded token (verified — `_clients.py` contains no `google`/`calendar`
> reference).

---

## 6. The handler — shaping events into `ObservationDraft`

`handle_google_calendar_event`
([handlers/google_calendar.py:131-250](../../../services/ingest/ingestion/handlers/google_calendar.py#L131-L250))
is a **pure function** (no DB / network), registered on the single channel via
`@register("google_calendar:event")` and recorded in `CHANNEL_TRUST_MAP`
([handlers/google_calendar.py:131, 255](../../../services/ingest/ingestion/handlers/google_calendar.py#L131-L255);
the channel is also imported in
[handlers/__init__.py:172](../../../services/ingest/ingestion/handlers/__init__.py#L172)).
It branches on the event `status` to set `kind`.

| Field | Derivation | Code |
|-------|-----------|------|
| `source_channel` | constant `"google_calendar:event"` | [handlers/google_calendar.py:53](../../../services/ingest/ingestion/handlers/google_calendar.py#L53) |
| `external_id` | `gcal:{calendar_id}:{event_id}:{status}:{start_instant}` (VERSIONED) | [handlers/google_calendar.py:164-167](../../../services/ingest/ingestion/handlers/google_calendar.py#L164-L167) |
| `occurred_at` | event `start` → `originalStartTime` → `updated` → `created` → now | [handlers/google_calendar.py:92-104](../../../services/ingest/ingestion/handlers/google_calendar.py#L92-L104) |
| `kind` | `state_change` if `status == "cancelled"`, else `signal` | [handlers/google_calendar.py:153-154](../../../services/ingest/ingestion/handlers/google_calendar.py#L153-L154) |
| `trust_tier` | constant **`authoritative`** (calendar is the system of record for scheduling, D4) | [handlers/google_calendar.py:54](../../../services/ingest/ingestion/handlers/google_calendar.py#L54) |
| `source_actor_ref` | `email:{organizer_email}` (organizer, else creator), or `None` | [handlers/google_calendar.py:246](../../../services/ingest/ingestion/handlers/google_calendar.py#L246) |

Highlights:

- **Time parsing** handles both timed events (`start.dateTime`) and all-day events
  (`start.date = YYYY-MM-DD`), normalising to UTC
  ([handlers/google_calendar.py:76-89](../../../services/ingest/ingestion/handlers/google_calendar.py#L76-L89)).
- **`content_text`** is synthesized human-legible prose: a cancellation reads
  `"Calendar event '{summary}' was cancelled (was …)"`; otherwise
  `"{organizer} scheduled '{summary}' at {time} with N attendee(s): …"`
  ([handlers/google_calendar.py:177-188](../../../services/ingest/ingestion/handlers/google_calendar.py#L177-L188)).
- **`entities_hint`** emits typed `email_address` refs for the organizer + each
  attendee, each flagged `external` when its domain differs from the owner's, plus
  a `meeting_topic` ([handlers/google_calendar.py:190-210](../../../services/ingest/ingestion/handlers/google_calendar.py#L190-L210)).
- **`content`** preserves the structured event (status, `eventType`, start/end,
  computed `duration_minutes`, recurrence link, hangout/html links, owner) for
  downstream capacity reasoning
  ([handlers/google_calendar.py:212-237](../../../services/ingest/ingestion/handlers/google_calendar.py#L212-L237)).
- A payload that isn't a dict, or an event missing `id`, is rejected with a
  `ValidationError` ([handlers/google_calendar.py:136-144](../../../services/ingest/ingestion/handlers/google_calendar.py#L136-L144)).

### 6.1 Why the versioned external_id (mutation semantics)

The observations repo dedups on `(source_channel, external_id)` **ignoring
`occurred_at`** — one stable observation per `external_id`. That fits immutable
sources (a sent email, a merged PR) but calendar events mutate. Encoding
`:{status}:{start_instant}` means
([handlers/google_calendar.py:20-37](../../../services/ingest/ingestion/handlers/google_calendar.py#L20-L37)):

| Change | external_id effect | Result |
|--------|-------------------|--------|
| identical re-fetch (backfill twin == poll twin) | unchanged | **dedups** to one observation |
| cancellation (`confirmed → cancelled`) | new `status` segment | **new** observation, `kind=state_change` |
| reschedule (start moves) | new `start_instant` segment | **new** `signal` (temporal signal stays current) |
| RSVP-only churn (attendee `responseStatus` flips) | unchanged | **dedups** (no observation spam) |

---

## 7. Live (real-time) ingestion — the poll (no webhook in this path)

Google Calendar's **default, always-on** live path is a **poll**, not a webhook.
The `google_calendar` live poller is the analog of Gmail's history poller
([google_calendar/live_poller.py:1-17](../../../services/ingest/integrations/google_calendar/live_poller.py#L1-L17)):

1. On a ~60 s tick it **leases** active calendars whose `sync_token` is seeded
   (i.e. backfill finished) and whose `last_live_poll_at` is older than
   `_POLL_GAP_S` (120 s), using `FOR UPDATE SKIP LOCKED` so replicas don't
   double-drain ([live_poller.py:50-79](../../../services/ingest/integrations/google_calendar/live_poller.py#L50-L79)).
2. For each leased calendar it calls the **shared** `drain_live` helper, which
   reconstructs a minimal `install`, drives **the same `fetch_page_google_calendar`
   fetcher** incrementally from the warm `sync_token`, and feeds every record
   through `core.ingest()` under **`ingress_kind="poll"`** → the same
   `google_calendar:event` channel and the same dedup key as backfill
   ([live_poller.py:82-103](../../../services/ingest/integrations/google_calendar/live_poller.py#L82-L103),
   [_google_live.py:32-82](../../../services/ingest/integrations/_google_live.py#L32-L82)).
3. On success it advances `sync_token` and resets the failure counter; on
   `GoogleRateLimited`/`GoogleApiError` it bumps `consecutive_live_failures` and
   marks the calendar `errored` after `_MAX_FAILURES` (5)
   ([live_poller.py:118-155](../../../services/ingest/integrations/google_calendar/live_poller.py#L118-L155)).
4. `drain_live` has a hard `_MAX_PAGES = 200` safety bound against a runaway token
   loop, and falls back to the prior token when a drain produces none — so a
   transient failure never erases the bookmark
   ([_google_live.py:26-29, 76-79](../../../services/ingest/integrations/_google_live.py#L26-L79)).

> **There is genuinely no webhook in this path.** A push-driven and a poll-driven
> observation are indistinguishable downstream because they share the fetcher,
> channel, and dedup key — but the poll requires no inbound HTTP at all.

---

## 8. Reconciliation — gap detection

`reconcile_google_calendar`
([reconcilers/google_calendar.py:168-206](../../../services/ingest/ingestion/reconcilers/google_calendar.py#L168-L206))
re-checks **completed** calendar shards for new activity. For each done shard it
loads the cursor's `high_water_updated` (the max event `updated` the fetcher
walked) and issues **one cheap 1-row probe**:
`events.list?updatedMin={high_water+1ms}&showDeleted=true&maxResults=1`. If
anything changed strictly after the high-water, it reshares a
`google_calendar_events` shard at **`recency_score=1.5`**
([reconcilers/google_calendar.py:114-165](../../../services/ingest/ingestion/reconcilers/google_calendar.py#L114-L165)).

**Exclusive floor (load-bearing convergence fix).** Calendar's `updatedMin` is an
**inclusive** lower bound, and `high_water` is by construction the max `updated`
already walked — so a probe at `updatedMin=high_water` would *always* re-match
that same boundary event and reshare forever. The reconciler therefore probes at
`high_water + 1ms` (`_exclusive_updated_floor`), excluding the boundary event so
only genuinely newer edits trip a reshare
([reconcilers/google_calendar.py:90-100, 130-137](../../../services/ingest/ingestion/reconcilers/google_calendar.py#L90-L137)).
A cancellation counts because the probe sets `showDeleted=true`. A failed probe is
logged and treated as "no gap" (best-effort, never a hard error)
([reconcilers/google_calendar.py:144-149](../../../services/ingest/ingestion/reconcilers/google_calendar.py#L144-L149)).

> The reconciler over-reshares but never under-reshares; `external_id` parity +
> dedup make the re-walk idempotent.

---

## 9. Native push channel (`events.watch`) — opt-in, not the default

A native Google **push** substrate exists and **is wired**, but it is **off by
default** and gated on `GOOGLE_PUSH_WEBHOOK_BASE` being set. It is the low-latency
path; the poll (§7) is the guaranteed liveness net even with no push at all.

- **Registration / renewal.** A generic engine `_google_watch.run_watch_scheduler`
  leases active, cursor-seeded calendars and opens an `events.watch` channel
  (`type=web_hook`, address = `{GOOGLE_PUSH_WEBHOOK_BASE}/webhooks/google_calendar/push`),
  minting a fresh `channel_id` + shared `token`, persisting them on the calendar
  row with a 7-day TTL + 24 h renewal window. **If `GOOGLE_PUSH_WEBHOOK_BASE` is
  unset, the scheduler idles** and logs `disabled_no_address`
  ([_google_watch.py:68-75, 206-222](../../../services/ingest/integrations/_google_watch.py#L68-L222);
  Calendar's `WatchSpec` at [google_calendar/watch.py:69-84](../../../services/ingest/integrations/google_calendar/watch.py#L69-L84)).
- **Inbound ping.** Google sends a **content-less** ping carrying `X-Goog-Channel-ID`
  + `X-Goog-Channel-Token` + `X-Goog-Resource-State`. The ingress
  `POST /webhooks/google_calendar/push`
  ([webhooks/google_push.py:48-85](../../../services/app/webhooks/google_push.py#L48-L85))
  acks `state=sync` handshakes, looks up the channel and **constant-time-verifies
  the token** (`resolve_push`), then drains the delta via `drain_push` → the same
  `drain_live` + fetcher as the poll
  ([_google_watch.py:239-297](../../../services/ingest/integrations/_google_watch.py#L239-L297)).
- It **always returns `200`** (unknown channel, token mismatch, or transient drain
  error) so Google doesn't retry — the poller is the backstop
  ([webhooks/google_push.py:16-19, 63-75](../../../services/app/webhooks/google_push.py#L16-L75)).

> **Why this isn't a `VERIFIERS` webhook.** This push edge is **not** registered in
> the `VERIFIERS` map and has **no `("google_calendar","webhook")` channel mapping**.
> It is a self-contained router (`google_push.py`, mounted in
> [ceo_view_wiring.py:155-162](../../../services/app/gateway/ceo_view_wiring.py#L155-L162))
> that verifies the channel **token** (a shared secret we set, **not an HMAC
> signature**) and calls `ingest()` directly. So from the normalizer/handler's
> point of view there are only two ingress kinds (`backfill`, `poll`); push reuses
> the `poll` machinery without its own ingress label.
>
> **TODO(human):** the short source doc
> [docs/ingestion/sources/google-calendar.md](../sources/google-calendar.md) still
> states *"Live ingress: none — poll-only (no push/webhook in v1)"*. The push edge
> above is wired in code (mounted, with tests + a synthetic generator), gated on
> `GOOGLE_PUSH_WEBHOOK_BASE`. Confirm whether push is "shipped but disabled by
> default in v1" vs "post-v1" so the two docs agree — the code does not state the
> product decision. *(inferred from code: push is implemented but opt-in.)*

---

## 10. Revocation / recoverable-error behavior

Google Calendar has **no installation-disabling revocation chokepoint** in the
outbound client (contrast GitHub's `_maybe_disable_on_revocation`). Instead:

- **`401`** (revoked / rotated grant) → the shared client invalidates the cached
  DWD token and **retries once**; a persistent 401 then surfaces as
  `GoogleApiError` ([gmail/client.py:110-115](../../../services/ingest/integrations/gmail/client.py#L110-L115)).
- **`429` / `403`-quota** → `GoogleRateLimited`, handled by backoff in the fetcher;
  if the budget is spent the cursor is left unadvanced and the round ends empty,
  so the shard re-enters next tick (no data loss, no fail)
  ([fetchers/google_calendar.py:194-204](../../../services/ingest/ingestion/fetchers/google_calendar.py#L194-L204)).
- **`410`** (expired sync token) → full reseed (§5.3).
- The **live poller / watch scheduler** disable a *single calendar* (`state =
  'errored'`) after `_MAX_FAILURES` consecutive failures, but never the whole
  install ([live_poller.py:137-155](../../../services/ingest/integrations/google_calendar/live_poller.py#L137-L155),
  [_google_watch.py:177-190](../../../services/ingest/integrations/_google_watch.py#L177-L190)).
- An install is disabled administratively by setting
  `google_calendar_installations.disabled_at` (every lease/probe query filters on
  `disabled_at IS NULL`); recovery is re-running the connect wizard (the finalize
  UPSERT clears `disabled_at`, [onboarding.py:92-99](../../../services/ingest/integrations/google_calendar/onboarding.py#L92-L99)).

---

## 11. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  DWD service account ─► mint impersonated Bearer (owner_email)   │
   INCLUDED CALENDARS     │  onboarding: resolve inclusion_spec → google_calendar_calendars  │
   (one per user's        │     └─► one google_calendar_events shard per calendar            │
    primary calendar)     │  fetcher FULL: events.list?timeMin=now-180d&singleEvents&orderBy │
                          │     └─► last page → nextSyncToken (warm start for incremental)   │
                          │     └─► inject _fyralis_calendar_id / _fyralis_owner_email        │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────── LIVE (POLL — no webhook) ─────────────────────┐│
   ~120s per calendar ────►  live_poller leases seeded calendars (SKIP LOCKED)            ││
                          │     └─► drain_live → SAME fetcher INCREMENTAL (syncToken,      ││
                          │            showDeleted=true) → ingest(ingress_kind="poll")     ││
                          │     410 GONE → reseed windowed full sync                       ││
                          └───────────────────────────────────────────────────────────────┘│
                          ┌──────────── LIVE (PUSH — opt-in, GOOGLE_PUSH_WEBHOOK_BASE) ────┐│
   events.watch ping ─────►  /webhooks/google_calendar/push (X-Goog-* headers)            ││
                          │     verify channel TOKEN (shared secret, NOT HMAC) →           ││
                          │     drain_push → drain_live → SAME fetcher (syncToken)         ││
                          │     (not in VERIFIERS; not a channel_mapping ingress)          ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_google_calendar_event    │
                                                            │  branch on status (cancelled →   │
                                                            │     state_change; else signal)   │
                                                            │  external_id =                    │
                                                            │   gcal:{cal}:{id}:{status}:{start}│
                                                            │  trust = authoritative            │
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace, one channel.** Backfill, poll, and push all
   produce the RAW Calendar event (+ `_fyralis_*` keys) and land on
   `google_calendar:event` with a **versioned** `external_id`
   `gcal:{calendar_id}:{event_id}:{status}:{start_instant}`. Identical re-fetches +
   RSVP churn dedup; cancellations + reschedules become distinct observations.
2. **One credential model — shared Gmail DWD.** A single service account
   impersonates each calendar owner at the `calendar.readonly` scope. No per-user /
   per-install token, no `secret_ref`, no `provider_installations` row.
3. **One shard family, two sync modes.** `google_calendar_events`: FULL windowed
   backfill (`timeMin`) seeds a `nextSyncToken`; INCREMENTAL (`syncToken`,
   `showDeleted`) drains deltas for poll/push/reshare. The two modes are mutually
   exclusive (Google rejects `syncToken` + `timeMin`).
4. **The live default is a POLL, not a webhook.** A native `events.watch` push edge
   exists but is **opt-in** (`GOOGLE_PUSH_WEBHOOK_BASE`); when present it verifies a
   channel **token** (not an HMAC) and reuses the poll's fetcher + dedup.
5. **Reactive rate-limiting (no dedicated bucket).** `429`/`403`-quota →
   `GoogleRateLimited`, handled by bounded backoff; a spent budget pauses the shard
   without failing it. `410` → full reseed.

---

## 12. Configuration & compliance

Verified against Google Calendar v3 docs (incremental sync `nextSyncToken`,
`410` expiry, `events.watch` push channels) and the in-repo code paths.

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `GMAIL_SERVICE_ACCOUNT_JSON_FILE` / `GMAIL_SERVICE_ACCOUNT_JSON` | — (one required) | DWD service-account key (shared Gmail/Calendar/Drive); also gates router mounting |
| `GMAIL_SERVICE_ACCOUNT_CLIENT_ID` | — | numeric DWD client ID surfaced to the admin for the Workspace grant |
| `GOOGLE_CALENDAR_BACKFILL_DAYS` | `180` | windowed full-sync horizon (`timeMin = now − N days`) |
| `GOOGLE_CALENDAR_API_BASE_URL` | `https://www.googleapis.com/calendar/v3` | explicit base URL override (points backfill at Provider Lab in dev) |
| `GOOGLE_PUSH_WEBHOOK_BASE` | — (unset → push disabled, poll is liveness) | public base for the `events.watch` `web_hook` address (§9) |

> **TODO(human):** the retry knobs for `retry_with_backoff_on_429`
> (`max_attempts=5`, `base_delay=1.0`, cap `60` s) are **call-site defaults**, not
> environment variables in the Calendar path. Confirm whether ops wants these
> exposed as env for Calendar specifically, or whether the shared defaults are the
> intended contract.

### 12.2 Verified compliant

- **Auth** — DWD service-account impersonation (RS256 JWT, `sub=owner_email`,
  `scope=calendar.readonly`), token cached with TTL headroom, `401` → invalidate +
  single retry. ✅
- **Incremental sync** — `nextSyncToken` captured on the last full-sync page; passed
  back as `syncToken` for deltas; `syncToken` never combined with `timeMin`/`orderBy`. ✅
- **Token expiry** — `410 GONE` caught → windowed full reseed; dedup keeps the
  re-walk idempotent. ✅
- **Cancellations** — `showDeleted=true` on incremental + reconciler probe; handler
  maps `status=cancelled` → `kind=state_change`. ✅
- **Push (when enabled)** — content-less `X-Goog-*` ping, channel-token
  constant-time verify, always-200 (poller backstop), 7-day channel TTL with 24 h
  renewal. ✅
- **Least secret surface** — single Workspace-global service-account key; no
  per-install token, no `secret_ref`. ✅

### 12.3 Dev / Provider Lab mode

Backfill is pointed at the local mock by **endpoint config only**: setting
`GOOGLE_CALENDAR_API_BASE_URL` explicitly redirects
`GoogleCalendarClient`'s base URL to the mock Calendar v3 server
([client.py:60-64](../../../services/ingest/integrations/google_calendar/client.py#L60-L64),
[endpoints.py:44,125,155](../../../lib/integrations/endpoints.py#L44)). Unlike
github/slack/discord/notion, **there is no token preseed** for Calendar in
`_clients.py` — the DWD minter + mock token-exchange server stand in for the real
Google auth, and `scripts/sandbox_google_calendar.py` drives the real minter →
fetcher → `ingest()` end-to-end (backfill, syncToken delta incl. a cancellation,
dedup, reconciler probe) against a throwaway Postgres. The synthetic push
generator ([synthetic/live_generators/google_push.py](../../../services/ingest/synthetic/live_generators/google_push.py))
exercises the §9 push edge against the mock.
