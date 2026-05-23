# Google Calendar ingestion sandbox (IN-15)

A local, credential-free sandbox that drives the **real** Google Calendar
ingestion pipeline end-to-end against a **mock** Google API.

Google Calendar is poll-only (no webhooks, so no ngrok) and authenticates via
Domain-Wide Delegation (service-account JWT → token exchange → Calendar v3
REST). The real-API + OAuth-browser sandbox flow used for slack/github/discord
therefore doesn't apply. Instead this sandbox stands up a real local HTTP mock
of the two endpoints the DWD path touches and exercises everything else for
real:

```
fake service account (real RSA key; token_uri → mock)
  → get_minter() / GoogleHttpClient    real DWD JWT mint → mock POST /token
  → GoogleCalendarClient               real httpx → mock GET /calendars/{id}/events
  → fetch_page_google_calendar         real cursor + syncToken logic
  → handle_google_calendar_event       real ObservationDraft
  → ingest()                           real observation insert + dedup
```

## Run it

```bash
# One command. Creates a throwaway Postgres DB, runs the pipeline, drops the DB.
python scripts/sandbox_google_calendar.py

# Keep the throwaway DB for inspection:
python scripts/sandbox_google_calendar.py --keep

# Point at your own sandbox DB instead of a throwaway (migrations applied
# idempotently — use a disposable DB, never production):
DATABASE_URL=postgresql://company_os:company_os@localhost:5434/my_sandbox \
  python scripts/sandbox_google_calendar.py
```

By default the throwaway DB is created on `SANDBOX_ADMIN_URL`
(default `postgresql://company_os:company_os@localhost:5434/company_os`).
Override `SANDBOX_ADMIN_URL` if your Postgres is elsewhere. No Ollama/Kafka/S3
needed — the driver calls `ingest()` directly (embeddings are left pending, as
in production when Ollama is briefly unavailable).

## What it exercises (13 checks)

1. **Provision** — `onboarding.finalize_install` writes the
   `google_calendar_installations` row, one `google_calendar_calendars` row per
   calendar, and an `onboarding_triggers` row (`source='google_calendar'`).
2. **Plan** — the `SourceOnboarding` loader SQL aggregates the calendars and the
   planner emits one shard per calendar.
3. **Backfill** — the real fetcher pages `events.list` (windowed by `timeMin`),
   captures the `nextSyncToken`, and each event is ingested as an observation.
4. **Incremental** — a warm-started shard re-runs with the captured `syncToken`;
   the mock returns a delta containing a brand-new meeting (→ `signal`) and a
   **cancellation** (→ `state_change`).
5. **Dedup** — re-ingesting an unchanged event collapses (external_id parity).
6. **Reconciler probe** — `has_updates_since` (the `updatedMin` gap probe)
   detects changes since an old high-water mark.
7. **Inspect** — prints every observation that landed (kind, trust, external_id,
   content_text) and asserts the final shape.

Expected: **13/13 checks pass**, **5 observations** (3 from backfill + 2 from the
delta), all `authoritative` on channel `google_calendar:event`.

## Why the `external_id` is versioned (the bug the sandbox caught)

The observations repo dedups on `(source_channel, external_id)` **ignoring
`occurred_at`** — it assumes one stable observation per external_id, which fits
immutable sources (a sent email, a merged PR) but **not** calendar events, which
mutate. With a naive `gcal:{calendar_id}:{event_id}`, a cancellation dedups onto
the original confirmed event and the `state_change` signal is silently lost.

The handler therefore versions the key:

```
gcal:{calendar_id}:{event_id}:{status}:{start_instant}
```

- identical re-fetches (backfill twin == poll twin) → same key → dedup;
- cancellation (`confirmed`→`cancelled`) → new key → lands as `state_change`;
- reschedule (start changes) → new key → lands as a fresh `signal`;
- RSVP-only churn (status + start unchanged) → same key → dedup.

The bare event id is preserved in `content.event_id`. See decision **D7** in
`specs/IN-15-google-calendar-signal-source/plan.md`.

## Files

- `scripts/sandbox_google_calendar.py` — the driver.
- `services/synthetic/mock_servers/google_calendar.py` — the threaded mock
  serving `POST /token` and `GET /calendars/{id}/events` (full sync, syncToken
  delta with a 410-expiry mode, and the `updatedMin` reconciler probe). Fixtures
  are plain Calendar v3 event objects, so you control exactly what lands.

## Full worker-topology sandbox (Kafka + S3 + the real workers)

The in-process sandbox above proves the ingestion *logic*, but drives `ingest()`
directly. To exercise the **full M6 worker chain** —

```
onboarding_triggers
  -> oauth_poller -> tenant_onboarding -> source_onboarding
  -> shard_fetch        (raw blob -> S3, publish -> Kafka ingestion.raw)
  -> normalizer         (consume ingestion.raw, run handler, publish ingestion.normalized)
  -> observation_writer (consume ingestion.normalized, write observations)
  -> reconciler
```

— run:

```bash
python scripts/sandbox_google_calendar_full.py        # stands up + tears down everything
python scripts/sandbox_google_calendar_full.py --keep # keep the DB + kafka container
```

It stands up the infra and runs the existing `BackfillHarness` (the 7 workers as
**real subprocesses on this branch's code**) for a `google_calendar` tenant:

- **Kafka** — a throwaway single-node KRaft container on host `:29092` (the
  compose Kafka advertises `kafka:9092` internally and isn't host-reachable, so
  the runner uses its own; the running stack is untouched). Requires Docker.
- **S3** — moto, in-process, bucket `fyralis-raw`.
- **Postgres** — a throwaway DB on `SANDBOX_ADMIN_URL` (default `:5434`),
  migrated + partitioned, dropped on exit.

The `google_calendar` source is mocked in-process at the `_open_calendar_client`
seam by the X3 helper (injected into each subprocess) — no Google creds; the
rest is the real chain. Expected: **6 observations** (2 calendars × 3 events),
all 4 harness assertions PASS.

This required wiring `google_calendar` into the harness (a fixture builder, a
`MockGoogleCalendarClient`, helper-template factory registration, install
seeding, scenario support) and registering the `google_calendar` reconciler
pool provider in `services/ingestion/workflows/reconciler.py` (the harness runs
that worker's `main()` directly, not `workflows/__main__.py`). The
default-skipped CI test lives at
`services/synthetic/backfill_harness/tests/test_harness_e2e_google_calendar.py`
(gate it with `X3_HARNESS_E2E=1` + Kafka + moto).

Note: a single-node broker can emit transient `Coordinator load in progress`
warnings at startup; the runner settles Kafka (~10s) and uses a 120s consumer
drain so the chain reliably completes.

## Extending it

- **More / different events**: edit `_build_fixtures()` in the driver. Add a
  `delta` entry with `"status": "cancelled"` to model a drop, or change a start
  time to model a reschedule.
- **Sync-token expiry path**: have the mock return a `syncToken` of `"EXPIRED"`
  (the mock replies `410 GONE`); the fetcher reseeds a windowed full sync.
- **Full docker sandbox** (Kafka/S3/workers, prod guards): the gmail-style DWD
  source slots into `docker-compose.sandbox.yml` the same way the other sources
  do — point `GOOGLE_CALENDAR_API_BASE_URL` (and the SA `token_uri`) at a mock
  service and run the `source_onboarding` + `shard_fetch` + `normalizer` +
  `observation_writer` workers. The in-process driver here is the faster loop
  for validating ingestion correctness.
