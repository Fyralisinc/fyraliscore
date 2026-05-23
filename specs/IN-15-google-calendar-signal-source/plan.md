# Implementation Plan: Google Calendar as a Signal Source — DWD Auth, Poll-Based Backfill + Incremental Sync, Calendar Events as Observations

**Branch**: `integration/google-calendar-signal-source` (cut from `integration/notion-signal-source`) | **Date**: 2026-05-23 | **Spec**: this plan is the anchoring artifact (mirrors the IN-14 Notion shape)
**Input**: User request — "Integrate Google Calendar as a signal source for Fyralis. What should we fetch as signals such that it allows Fyralis to make rich decisions?"

## Summary

Google Calendar becomes the **sixth ingestion source** alongside gmail / github / slack / discord / notion. Where the existing sources capture what *happened* (GitHub merges, Slack messages) or what is *declared/planned* (Notion roadmaps), Google Calendar captures **where the org's scarcest resource — attention/time — is allocated, and who is meeting whom about what.** It is the *operating-rhythm + relationship-allocation* layer.

Google Calendar is a **Google Workspace API**, identical in auth model to the existing Gmail integration (service account + Domain-Wide Delegation). The single biggest architectural decision (D1) is therefore: **reuse the Gmail DWD auth substrate** (`DwdTokenMinter` / `get_minter()`, `GoogleHttpClient`, `DirectoryClient`, `resolve_inclusion`) rather than reinvent Notion's per-workspace OAuth-bot-token flow. Calendar is just another Google API the same service account can be scoped to.

Like Notion, Google Calendar in v1 is **poll-first** (no push-webhook ingest):

- **Backfill** rides the existing M6.2a `ShardFetch` loop via a new `FETCHER_DISPATCH["google_calendar"]`, paging `events.list` per calendar.
- **Incremental** ("live") updates ride the existing `PeriodicReconciler` / poll machinery — re-running the fetcher with Google Calendar's native **`syncToken`** (the API's first-class incremental primitive; far cleaner than Notion's `last_edited_time` high-water mark).
- v1 makes **zero changes** to `services/webhooks/`. Calendar *does* support push channels (`events.watch`), but they expire every ~7 days and require renewal infra; that is an explicit follow-up (see [Out of Scope](#out-of-scope)).

This task adds:

- A `services/integrations/google_calendar/` package: `client.py` (`GoogleCalendarClient` wrapping the shared `GoogleHttpClient`), `onboarding.py` (resolve workspace users → calendars via the shared `DirectoryClient`, finalize an install, emit an onboarding trigger), `metrics.py`. **No `oauth.py` / `dwd.py`** — auth is the shared Gmail DWD minter.
- `services/ingestion/fetchers/google_calendar.py` (`fetch_page_google_calendar` + `GoogleCalendarCursor`) → `FETCHER_DISPATCH["google_calendar"]`.
- `services/ingestion/planners/google_calendar.py` (`PLANNER_DISPATCH["google_calendar"]`) — one `google_calendar_events` shard per resolved calendar.
- `services/ingestion/reconcilers/google_calendar.py` (`RECONCILER_DISPATCH["google_calendar"]`) — gap detection comparing each shard's high-water `updated` timestamp against the live latest event update.
- `services/ingestion/handlers/google_calendar.py` registering ONE channel `google_calendar:event`, branching on event `status`/`eventType`.
- One migration (`0060_google_calendar.sql`): two new tables (`google_calendar_installations`, `google_calendar_calendars`, mirroring `gmail_installations` / `gmail_mailbox_watches`) + widen the four `source IN (...)` CHECK constraints to include `'google_calendar'`.
- Dispatch + routing wiring: `SourceLiteral`, `channel_mapping`, `CHANNEL_TRUST_MAP`, `VALID_SOURCES`, `_build_source_client`, the planner-loader SQL.

**Existing assets reused unchanged:**

- The Gmail DWD substrate: `get_minter()` (process-global `DwdTokenMinter`), `GoogleHttpClient` (token mint + bearer + 401 retry + 429/5xx → `GoogleRateLimited`), `DirectoryClient` + `resolve_inclusion`.
- `lib/integrations/endpoints.py` resolver — a new `google_calendar_api` name lets backfill point at a local spammer for tests.
- The M6.2a `ShardFetch` loop + the N1 cursor-advance primitive — the fetcher just returns `FetchResult`.
- `services/ingestion/core.py::ingest()` — actor resolution, entity fast-path, embedding, `observations` insert with `UNIQUE(source_channel, external_id, occurred_at)` dedup, `think_trigger_queue` enqueue. Calendar observations flow Think downstream with no Think changes.
- `source_onboarding` / `tenant_onboarding` / `onboarding_triggers` machinery.

## Signal Value: What Fyralis Gets

The CEO's calendar is the single richest unobtrusive signal of **how the company actually spends its most finite resource — leadership and team attention.** Fyralis already knows what shipped (GitHub) and what is discussed/planned (Slack/Notion); the calendar adds the **allocation + relationship + cadence** dimension that none of the others carry.

| Calendar field | Signal it carries | Reasoning use |
|---|---|---|
| Event summary + description | Meeting topic, agenda, linked docs | Entity grounding; "what is leadership spending time on" |
| `start` / `end` / duration | Time block size | **Capacity Resource** signal — meeting load, focus-time erosion |
| `organizer` | Who *drives* the meeting | Ownership / leadership-attention attribution |
| `attendees[]` + `responseStatus` | Who collaborates; accepted vs declined vs tentative | Relationship-graph edges; engagement / commitment signal |
| External-domain attendees | Customer / investor / vendor / candidate meetings | **External-relationship** signal (sales motion, fundraising, hiring) |
| `recurrence` / recurring instances | Operating cadence (1:1s, standups, board, all-hands) | The org's *rhythm*; a dropped recurring 1:1 is a relationship signal |
| `status="cancelled"` | Meeting dropped / rescheduled | `state_change`; deprioritization or churn signal |
| `eventType` (`default`/`outOfOffice`/`focusTime`/`workingLocation`) | Availability, deep-work blocks, OOO | Capacity + availability reasoning |
| `conferenceData` / `hangoutLink` | Remote vs in-person | Working-mode context |

**Cross-source reasoning unlocked** (the real payoff): Calendar × GitHub × Notion lets Fyralis reason about **intent-vs-attention-vs-reality drift** — e.g. "the Q3 roadmap (Notion) prioritizes Project Atlas, but the CEO booked zero hours on it this month (Calendar) and no PRs landed (GitHub)," or "three customer meetings this week (Calendar) but the deal stage never advanced (CRM/Notion)."

**Trust posture (D4)**: a Google Calendar event is the **system of record** for the fact "this meeting is scheduled at time T with these attendees" — directly comparable to the existing `calendar:sync` channel, which is `authoritative`. We use **`authoritative`**, NOT `attested_agent`. (Caveat: the event records *intended* attendance, not verified attendance — a reasoner must not treat `accepted` as "showed up." That is a downstream Think concern, not an ingestion-trust one.)

## Technical Context

**Language/Version**: Python 3.12 (`.venv`); `from __future__ import annotations`, full type hints, Pydantic v2 `extra="forbid"` at wire boundaries.

**Primary Dependencies**:
- **httpx** — via the shared `GoogleHttpClient`. The Calendar API is plain HTTPS + `Authorization: Bearer <impersonated-token>`; **no new dependency, no SDK**.
- **asyncpg** — factory-injected pool.
- The Gmail DWD substrate (`services/integrations/gmail/dwd.py`, `client.py`, `directory.py`) — reused, unchanged.

**Storage**: Postgres 16 + pgvector. Two new tables (`google_calendar_installations`, `google_calendar_calendars`); reused tables: `source_onboarding_runs`, `onboarding_shards`, `onboarding_triggers`, `ingestion_failures`, `observations`. One migration alters four CHECK constraints + creates two tables. RLS via `app.current_tenant`, mirroring the gmail_* tables.

**Testing**: pytest. Pure-unit layers (client / planner / fetcher / reconciler / handler) run with fakes + `respx`-style HTTP fakes, **no DB**. The end-to-end pipeline test uses the `integration` marker (live Postgres) when available.

**Performance / limits**:
- Calendar API default quota is generous (~per-user-per-100s limits); `events.list` returns up to 2500 events/page. The fetcher uses a conservative page size and honors 429/403-quota via the shared `GoogleRateLimited` → cursor-preserving empty page (mirrors Gmail/Notion).
- `syncToken` is the incremental primitive: after a full backfill the fetcher stores `nextSyncToken`; the poll re-run passes it and Google returns only deltas. On `410 GONE` (token expired) the fetcher transparently falls back to a fresh windowed full sync.
- Backfill window: `timeMin = now − GOOGLE_CALENDAR_BACKFILL_DAYS` (default 180), `singleEvents=true`, `orderBy=startTime` so recurring series expand to concrete instances with stable per-instance ids.

**Constraints**:
- `singleEvents=true` is required to (a) expand recurrences into datable instances and (b) make `syncToken` usable; without it Calendar rejects `syncToken`. The fetcher always sets it.
- `external_id` is **versioned**: `gcal:{calendar_id}:{event_id}:{status}:{start_instant}`. The observations repo dedups on `(source_channel, external_id)` *ignoring* `occurred_at` (it assumes one stable observation per external_id) — which suits immutable sources but NOT mutable calendar events. Encoding status+start means identical re-fetches (backfill twin == poll twin) collapse, while a cancellation (`confirmed`→`cancelled`) or a reschedule (start changes) lands as a distinct observation; RSVP-only churn dedups. See D7. (This was caught by the sandbox — see `docs/ingestion/google-calendar-sandbox.md`.)
- `FYRALIS_ENV=prod` reuses the Gmail DWD prod-safety posture (service-account JSON must be configured); no new webhook secret to assert (poll-only).

**Scale/Scope**: Per-tenant workspaces in the low hundreds of users; one calendar shard per included user's primary calendar. Shared/secondary calendars are an additive follow-up.

## Constitution Check

| Principle | Status | Notes |
|---|---|---|
| §I Four Foundations distinct | PASS | Calendar data lands as **Observations** (`kind ∈ {signal, state_change}`); a `cancelled` event is a natural `state_change`. No Model/Act/Resource writes here. `google_calendar_*` are permitted per-feature side tables. |
| §II Append-only migrations | PASS | `0060_google_calendar.sql`: `CREATE TABLE IF NOT EXISTS` (×2) + idempotent `DROP CONSTRAINT IF EXISTS … ; ADD CONSTRAINT … CHECK (… 'google_calendar')` on four tables. Constraint widening is non-destructive (existing rows are a subset). No applied migration edited. |
| §III Tenant isolation structural | PASS | New tables enforce RLS via `app.current_tenant` (mirrors gmail_* policy template). All new queries hand-roll `WHERE tenant_id = $1`. |
| §IV Integration tests, real DB | PASS | The pipeline test runs on live Postgres; pure layers use fakes. |
| §V Reasoning vs rendering | N/A | Pure ingestion plumbing; observations trigger Think via the existing `ingest()` path. |
| §VI Trust/confidence/falsifiers | PASS | Calendar observations carry `trust_tier='authoritative'` per `CHANNEL_TRUST_MAP` (system-of-record for scheduling). No Model writes ⇒ no falsifier obligation. |
| §VII Determinism + audit | PASS | `uuid7()` for new substrate rows. Observation idempotency via the existing dedup; versioned `external_id = gcal:{calendar_id}:{event_id}:{status}:{start}` stable across backfill + poll for a given event version (D7). |
| §VIII Structured errors | PASS | Reuses `GoogleApiError` / `GoogleRateLimited` (shared Google client). No new error classes needed (simpler than Notion, which needed bespoke OAuth errors). |
| §IX Dual-write until proven | PASS — N/A | No hot-path field, no existing Calendar data, no parallel writer. CHECK widening is backward-compatible. |
| §X Simplicity / YAGNI | PASS | No push-webhook in v1. No SDK. Reuses the Gmail DWD substrate rather than a second Google auth path. One channel, not three. Each deferral is listed in Out of Scope. |

**No NON-NEGOTIABLE violations.** Complexity Tracking table is empty.

## Project Structure

New files:

```text
services/integrations/google_calendar/
├── __init__.py
├── client.py          # GoogleCalendarClient over the shared GoogleHttpClient: list_calendars, list_events, latest_event_update
├── onboarding.py      # resolve workspace users → calendars (shared DirectoryClient + resolve_inclusion); finalize install; emit onboarding trigger
└── metrics.py         # gcal_provision_* + gcal_fetch_* counters

services/ingestion/fetchers/google_calendar.py        # fetch_page_google_calendar + GoogleCalendarCursor; FETCHER_DISPATCH["google_calendar"]
services/ingestion/planners/google_calendar.py        # plan_shards_google_calendar; PLANNER_DISPATCH["google_calendar"]
services/ingestion/reconcilers/google_calendar.py     # reconcile_google_calendar; RECONCILER_DISPATCH["google_calendar"]
services/ingestion/handlers/google_calendar.py        # @register("google_calendar:event")

db/migrations/0060_google_calendar.sql                # 2 tables + widen four CHECK constraints
```

Colocated tests:

```text
services/integrations/tests/test_client_google_calendar.py        # pagination, syncToken, 410-GONE fallback, 429
services/integrations/tests/test_onboarding_google_calendar.py     # inclusion → calendars; install + trigger
services/ingestion/fetchers/tests/test_google_calendar.py          # page/cursor round-trip, syncToken capture, end_of_data, rate-limit empty page
services/ingestion/planners/tests/test_google_calendar.py          # calendar aggregation → one shard each
services/ingestion/reconcilers/tests/test_google_calendar.py       # clean vs gap (high-water < live latest)
services/ingestion/handlers/tests/test_google_calendar.py          # event → ObservationDraft; cancelled → state_change; attendee/organizer entity hints; external_id stability
services/ingestion/fetchers/tests/test_google_calendar_pipeline.py # end-to-end: provision → plan → fetch → ingest (integration)
```

Changed files:

```text
services/ingestion/fetchers/__init__.py        # add "google_calendar" stub + import
services/ingestion/planners/__init__.py        # add "google_calendar" stub + import
services/ingestion/reconcilers/__init__.py     # add "google_calendar" stub + import
services/ingestion/handlers/__init__.py        # add google_calendar:event to CHANNEL_TRUST_MAP + import handler
services/ingestion/raw_tier/envelope.py        # SourceLiteral gains "google_calendar"
services/ingestion/normalizer/channel_mapping.py  # (google_calendar, backfill|poll) → google_calendar:event
services/ingestion/workflows/source_onboarding.py # VALID_SOURCES + _LOAD_GCAL_INSTALL_SQL + _load_install branch
lib/integrations/endpoints.py                  # google_calendar_api (prod + env + spammer subpath)
CODEBASE-ARCHITECTURE.md                        # append an IN-15 section
```

**Structure Decision**: Mirror the per-source layout the M6 substrate established (planner / fetcher / reconciler / handler + a `services/integrations/<source>/` package). Calendar reuses the *Gmail* shape specifically — installations + per-resource (calendar) table, DB-state planner with `source_client=None`, DWD auth — because it is a Google Workspace API, not the *Notion* shape (per-workspace OAuth bot token).

## Decisions (locked — implementation phase, 2026-05-23)

### D1 — Auth: reuse the Gmail Domain-Wide-Delegation substrate (NOT a new OAuth flow)
Google Calendar is a Google Workspace API. The same service account that powers Gmail can be granted the Calendar scope (`https://www.googleapis.com/auth/calendar.readonly`) in the Workspace Admin Console. We reuse `get_minter()` / `GoogleHttpClient` / `DirectoryClient` verbatim. **Rationale**: one Google auth path, not two; zero new JWT/token-cache code; admin configures DWD once. Calendar gets its own *installations* + *calendars* tables (independent lifecycle, scope auditing) but shares the auth primitives.

### D2 — Incremental via Google's native `syncToken` (NOT a hand-rolled high-water mark)
Calendar's `events.list` returns a `nextSyncToken` on the final page of a full sync; passing it on the next call returns only changed/deleted events. The fetcher stores it in the cursor and the poll re-run consumes it. On `410 GONE` (token aged out, ~weeks) the fetcher falls back to a fresh windowed full sync. **Rationale**: this is the API's first-class delta mechanism — strictly better than Notion's `last_edited_time` polling; minimal request volume on steady state.

### D3 — One channel `google_calendar:event`, handler branches on `status`/`eventType`
Mirrors the Notion D3 / GitHub one-channel-many-types precedent and the normalizer's `(source, ingress_kind) → one channel` routing. `status="cancelled"` → `kind="state_change"`; everything else → `kind="signal"`. `eventType` is preserved in `content` for downstream reasoning.

### D4 — Trust tier `authoritative`
A calendar event is the system of record for scheduling, consistent with the pre-existing `calendar:sync` channel. (Intended-vs-actual attendance nuance is a Think concern, not ingestion.)

### D5 — Backfill window 180 days, `singleEvents=true`, env-overridable
Full history is unbounded and low-value; the last ~6 months captures current cadence + relationships. `GOOGLE_CALENDAR_BACKFILL_DAYS` (default 180) tunes it. `singleEvents=true` is mandatory (recurrence expansion + syncToken eligibility). The backfill window is the `timeMin`; the reconciler's gap probe uses `updatedMin`.

### D6 — One shard per included user's primary calendar
`resolve_inclusion` (shared) expands the admin inclusion spec to user emails; each user's primary calendar is addressed by their email as `calendarId`. Shared/secondary calendars (`calendarList`) are an additive follow-up.

## Risk Register

1. **`syncToken` expiry (`410 GONE`).** Steady-state poll fails if the token aged out. **Mitigation**: the fetcher catches 410 and reseeds a windowed full sync; dedup makes the re-walk idempotent.
2. **Recurring-event explosion.** `singleEvents=true` on a many-year recurrence inflates instance count. **Mitigation**: `timeMin` window (D5) bounds expansion to the backfill horizon.
3. **`external_id` parity across backfill vs poll, AND mutation capture.** Because the observations repo dedups on `(source_channel, external_id)` ignoring `occurred_at`, a naive `gcal:{calendar_id}:{event_id}` would silently drop cancellations/reschedules (they'd dedup onto the original). **Mitigation**: versioned external_id `…:{status}:{start}` (D7) — twins collapse, mutations land. Caught by the sandbox; tested.
4. **Quota / rate limits.** **Mitigation**: shared `GoogleRateLimited` handling → cursor-preserving empty page; conservative page size.
5. **Cancelled events in a sync delta carry no `start`.** **Mitigation**: handler falls back to `updated`/`originalStartTime` for `occurred_at` when `start` is absent.
6. **Trust over-claiming attendance.** `accepted` ≠ attended. **Mitigation**: `authoritative` covers *scheduling*, not attendance; documented for Think.

## Out of Scope (deferred, with rationale)

- **Push-webhook ingest** (`events.watch` channels): channels expire ~7 days and need renewal infra; v1 poll covers correctness. Additive later.
- **Shared / secondary / resource calendars** (`calendarList`): v1 covers each user's primary calendar; multi-calendar fan-out is additive.
- **Writing to Calendar** (creating/moving events): read-only signal source.
- **Free/busy aggregation as a derived Resource**: a Think-side capacity model, not ingestion.
- **A full admin onboarding wizard** (preflight/finalize UI like gmail/oauth.py): `onboarding.py` ships the callable provisioning (resolve → install → trigger); the HTTP/UI surface mirrors the Gmail wizard as a follow-up.
