# Google Calendar (IN-15)

> The *attention-allocation / operating-rhythm / relationship* layer — where
> leadership and team time actually goes, and who meets whom about what. Unlocks
> intent-vs-attention-vs-reality drift (roadmap says X; calendar shows zero hours
> on it; GitHub shows no PRs).

| Field | Value |
|---|---|
| Source | `google_calendar` |
| Primary channel | `google_calendar:event` (handler branches on `status`) |
| Trust tier | `authoritative` |
| Live ingress | **none** — poll-only (no push/webhook in v1) |
| Backfill | windowed per included user's primary calendar (`GOOGLE_CALENDAR_BACKFILL_DAYS`, default 180, `singleEvents=true`) |
| Incremental | Google's native `nextSyncToken` |
| Auth | Domain-Wide Delegation (reuses the Gmail substrate) |

## Auth — reuses the Gmail DWD substrate (D1)

One Google auth path: the same service account is granted the Calendar scope, via
[gmail/dwd.py](../../../services/ingest/integrations/gmail/dwd.py) `get_minter()`,
`GoogleHttpClient`, `DirectoryClient`, `resolve_inclusion`. Calendar keeps its own
`google_calendar_installations` + `google_calendar_calendars` tables (independent
lifecycle / scope auditing) but shares the minter.
[google_calendar/onboarding.py](../../../services/ingest/integrations/google_calendar/onboarding.py)
resolves workspace users → calendars and finalizes the install (UPSERT install +
calendars + onboarding trigger).

## Backfill & incremental (the quartet)

- [planners/google_calendar.py](../../../services/ingest/ingestion/planners/google_calendar.py)
  — one shard per included user's primary calendar (D5/D6).
- [fetchers/google_calendar.py](../../../services/ingest/ingestion/fetchers/google_calendar.py)
  via [google_calendar/client.py](../../../services/ingest/integrations/google_calendar/client.py)
  — `events.list` full sync returns a `nextSyncToken` on the last page; the poll
  re-run passes it for **deltas only** (D2). On `410 GONE` the fetcher reseeds a
  windowed full sync. Strictly better than a hand-rolled high-water mark.
- [reconcilers/google_calendar.py](../../../services/ingest/ingestion/reconcilers/google_calendar.py)
  — `has_updates_since` probe.
- [handlers/google_calendar.py](../../../services/ingest/ingestion/handlers/google_calendar.py)
  — **one channel `google_calendar:event`**; branches on `status` (`cancelled` →
  `kind=state_change`, else `signal`) (D3). Trust `authoritative` (D4) — a
  calendar event is the system of record for scheduling.

## Dedup / external_id (D7 — mutable entities)

The observations repo dedups on `(source_channel, external_id)`. Calendar events
mutate, so a **versioned** `external_id` is used:

```
gcal:{calendar_id}:{event_id}:{status}:{start_instant}
```

This collapses identical re-fetches (backfill twin == poll twin) and RSVP-only
churn, while a **cancellation or reschedule lands as a distinct observation**
(preserving the `state_change` signal).

## Migration

`0060_google_calendar.sql` — 2 tables (`installations` + `calendars`) + widen the
four M6 source CHECKs.

Onboarding emits an `onboarding_triggers` row (`source=google_calendar`,
DWD-style) that flows oauth_poller → tenant_onboarding → source_onboarding →
shard_fetch.

**Sandbox**: `scripts/sandbox_google_calendar.py` + a mock DWD-token + Calendar
v3 API drive the real minter → fetcher → ingest end-to-end (backfill, syncToken
delta incl. a cancellation, dedup, reconciler probe) against a throwaway Postgres.

Spec: `specs/IN-15-google-calendar-signal-source/plan.md`. See
[architecture.md](../architecture.md).
