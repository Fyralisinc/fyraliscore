# Canonical Ingestion Architecture — How Every Signal Lands as an Observation

> **Status:** authoritative as of branch `feature/unify-ingestion-pipeline`
> (off `integration/ingestion-hardening`), 2026-05-26.
>
> **Scope:** the 8 production signal sources —
> `slack`, `discord`, `notion`, `github`, `google_drive`, `google_calendar`,
> `gmail`, `jira` — and the one pipeline they all share.
>
> **The one-sentence model:** every source's *primary* path is the **full
> Kafka pipeline** (`source → ingestion.raw → normalizer → ingestion.normalized
> → observation_writer → observations`); synchronous in-process `ingest()` is
> only a **fallback** for Kafka-outage degradation (and one deliberate
> exception: Discord *interactions*, which need a synchronous response shape).

---

## 1. The pipeline in one diagram

```
                          INGRESS (per-source, §4)
   ┌─────────────────────────────────────────────────────────────────────┐
   │  webhook (slack/github/jira/discord) ── gateway /webhooks/{provider}  │
   │  push    (gmail Pub/Sub) ──────────────  gateway /webhooks/gmail/...   │
   │  poll/backfill (ALL 8) ──────────────── ingestion workers (shard_fetch)│
   └─────────────────────────────────────────────────────────────────────┘
                                    │
                 shadow_write_raw() │  (S3 PutIfAbsent + Kafka produce+flush)
                                    ▼
              ┌──────────────────────────────────────┐
              │  S3/MinIO  fyralis-raw/<env>/<source>/│   raw body, content-addressed
              │            <tenant>/<ymd>/<hash>      │
              └──────────────────────────────────────┘
                                    │  RawEnvelope (pointer + identity)
                                    ▼
                       Kafka topic  ingestion.raw
                                    │
                                    ▼
              ┌──────────────────────────────────────┐
              │  NORMALIZER worker  (no DB)           │   fetch raw from S3,
              │  resolve_channel() → handler →        │   run handler → draft
              │  NormalizedEnvelope                   │
              └──────────────────────────────────────┘
                                    │  NormalizedEnvelope
                                    ▼
                       Kafka topic  ingestion.normalized
                                    │
                                    ▼
              ┌──────────────────────────────────────┐
              │  OBSERVATION_WRITER worker            │   per-tenant flag gate;
              │  if kafka_path_enabled → ingest_from_ │   actor + entity resolve,
              │  draft() → INSERT observations        │   embed, dedup, enqueue T1
              └──────────────────────────────────────┘
                                    │
                       ┌────────────┴───────────┐
                       ▼                         ▼
            Postgres  observations     Kafka  ingestion.embedding  (async embed)
                       │                         │   ingestion.dlq  (failures)
                       ▼
            think_trigger_queue  (T1 trigger → downstream reasoning)
```

The synchronous **fallback** collapses all of the above into one call:
`services/ingestion/core.py::ingest()` runs handler → resolve → embed → INSERT
in a single transaction against Postgres, with no Kafka/S3 hop.

---

## 2. The two paths, and why the full pipeline is primary

| | **Full pipeline (primary)** | **Inline `ingest()` (fallback)** |
|---|---|---|
| Trigger | tenant flag `ingestion.kafka_path_enabled = TRUE` and deps wired | flag FALSE, OR Kafka/S3 produce fails, OR direct call |
| Webhook response | `202 Accepted` (durable on broker, async finish) | `200/201` (written before responding) |
| Durability | S3 PutIfAbsent + Kafka idempotent produce + `flush()` | single Postgres transaction |
| Where the observation is written | `observation_writer` worker | the caller's process |
| Decoupling | provider ack is independent of DB load; back-pressure absorbed by Kafka | provider ack blocks on DB |

**Why the full pipeline is the canonical/primary path:** it decouples provider
acknowledgement from observation persistence, so a slow/locked Postgres or an
embedding stall never makes us drop or 5xx a provider webhook; Kafka absorbs
bursts; and a single normalizer→writer chain gives one place to apply
back-pressure, retries, and DLQ. The inline path exists for **graceful
degradation** (`_attempt_kafka_path` returns `False` → caller falls back to
`ingest()`), kept correct by idempotency: S3 `PutIfAbsent` on the content hash +
`observations UNIQUE (source_channel, external_id, occurred_at)` dedup mean a
fallback write and a later pipeline replay converge to the same single row.

The gate is read in **two** places, both defaulting to `FALSE`
(`services/ingestion/feature_flags/client.py`, flag
`ingestion.kafka_path_enabled`, 30 s TTL cache):

1. **Webhook router** (`services/webhooks/router.py:880-950`) — decides
   *cutover vs inline* at ingress for cutover-enabled providers.
2. **Observation writer** (`services/ingestion/writers/observation_writer.py`)
   — per envelope, decides *full write vs shadow-only*; a FALSE tenant's
   normalized envelopes are recorded as shadow events and **not** persisted.

> **Operational note (the "0 observations despite a clean run" trap):** a fresh
> tenant defaults to `FALSE`, so its data-plane envelopes flow through Kafka but
> the writer drops them in shadow mode. Enable per tenant:
> `TenantFlags.set_bool(tenant, "ingestion.kafka_path_enabled", True, set_by=...)`.
> The sandbox tenant `00000000-…0001` already has it TRUE.

---

## 3. The five pipeline stages (canonical, source-agnostic)

| Stage | Entry / file | Topic in → out | Notes |
|---|---|---|---|
| **1. Ingress** | webhook `services/webhooks/router.py::receive`; push `services/webhooks/gmail_pubsub.py`; poll/backfill `services/ingestion/workflows/shard_fetch.py` | — → `ingestion.raw` | size check, tenant resolution, signature verify |
| **2. Shadow-write raw** | `services/ingestion/shadow_write.py::shadow_write_raw` | — → `ingestion.raw` (+S3) | `build_raw_s3_key`, content hash, `PutIfAbsent`, produce **+ flush** at the webhook boundary |
| **3. Normalize** | `services/ingestion/normalizer/worker.py` (`python -m services.ingestion.normalizer.worker`) | `ingestion.raw` → `ingestion.normalized` | **no DB**; fetch raw from S3 → `resolve_channel(source, ingress_kind, meta)` → `get_handler(channel)` → `ObservationDraft` → `NormalizedEnvelope` |
| **4. Write observation** | `services/ingestion/writers/observation_writer.py` | `ingestion.normalized` → Postgres `observations` | flag-gated `_full_mode_write` → `ingest_from_draft` (actor resolve, fast-path entities, embed, dedup, INSERT, enqueue T1) |
| **5. Async tails** | `writers/embedding_worker/`, `recovery/embedding_backlog`, `writers/dlq_writer`, `dlq/publish` | `ingestion.embedding`, `ingestion.dlq` | embeds pending rows; persists failures to `ingestion_failures` |

The **inline core** (`services/ingestion/core.py`) is the shared
implementation of stage-4 logic: `ingest()` (fallback entry) and the writer's
`_full_mode_write()` both converge on `ingest_from_draft()`, so the observation
produced is byte-identical regardless of path (verified by
`test_observation_writer_m5.py::test_writer_observations_match_inline_for_same_input`).

### Workers (all launched from `docker-compose.yml`, one process each)

`normalizer` · `observation_writer` · `embedding_worker` · `embedding_backlog`
· `dlq_writer` · `shard_fetch` · `source_onboarding` · `tenant_onboarding` ·
`oauth_poller` · `reconciler` · `periodic_reconciler` · `discord_gateway_worker`
· `gateway` (FastAPI ingress).

---

## 4. Per-source landing — how each of the 8 reaches `observations`

Every source has a **planner → fetcher → handler → reconciler** quartet
(`services/ingestion/{planners,fetchers,handlers,reconcilers}/<source>.py`) for
the poll/backfill path, plus its source-specific ingress. **Backfill/poll
always feeds the full pipeline**: `shard_fetch` writes the raw body to S3 and
produces a `RawEnvelope` to `ingestion.raw` exactly like a webhook — it never
calls inline `ingest()`.

### Cutover-enabled webhook sources (full pipeline at ingress when flag on)

These are in `_CUTOVER_ENABLED_PROVIDERS` (`router.py:93`). Flag TRUE → router
calls `_attempt_kafka_path` → `202`; flag FALSE or Kafka down → inline `ingest()`.

- **slack** — webhook `/webhooks/slack` → `slack:message`; backfill enumerates
  channels → history. Source `slack`.
- **github** — webhook `/webhooks/github` → `github:webhook` (App installation
  events handled separately first); backfill per accessible repo. Source `github`.
- **jira** — webhook `/webhooks/jira/events` (HMAC `X-Hub-Signature`, GitHub-style)
  → `jira:issue`; backfill via `POST /rest/api/3/search/jql` (classic `/search`
  is HTTP 410 since 2025). Source `jira`. Live tenant resolution reuses the
  generic `provider_installations` edge; backfill uses dedicated `jira_*` tables.

### Special webhook source

- **notion** — webhook `/webhooks/notion/...` **always** routes to the full
  pipeline via a dedicated handler that fetches the full page and calls
  `shadow_write_raw()` directly (using the `app.state.notion_data_plane` alias,
  which points at the *same* producer/S3 instances). There is **no** inline
  path and **no** `_PROVIDER_CHANNEL` entry for Notion — the handler
  short-circuits the router before the inline block (`router.py:809-820`).
  Channel `notion:object`; a DB row with a status property → `kind=state_change`.
  Backfill enumerates databases/pages → blocks → comments. The "live" path is a
  **poll** via `periodic_reconciler` (`NOTION_POLL_INTERVAL_SECONDS`).

### Push (Pub/Sub) source

- **gmail** — Google Pub/Sub push hits the dedicated endpoint
  `services/webhooks/gmail_pubsub.py` (NOT the generic `/webhooks/{provider}`
  router), which publishes to `ingestion.raw` via the canonical
  `app.state.kafka_producer`/`s3_raw_client` (with `flush()`). Channel `gmail:`,
  `ingress_kind="pubsub"`. Backfill/poll via the History API
  (`ingress_kind="poll"`). Source `gmail`.

### Poll/backfill-only sources (no webhook — full pipeline via workers)

- **google_calendar** — **no push/webhook**. Planner enumerates calendars →
  fetcher pulls events (incremental via `nextSyncToken`) → `google_calendar:event`.
  Mutable entities use a versioned `external_id`
  (`gcal:{cal}:{event}:{status}:{start}`) so reschedules/cancellations are not
  deduped away. Source `google_calendar`.
- **google_drive** — **no push/webhook**. Planner enumerates My Drive +
  Shared Drives → fetcher pulls file activity + **content extraction**
  (Docs/Sheets/Slides/PDF→text, in the fetcher to keep handlers pure) + comments
  + revisions → all on channel `google_drive:file`, distinguished by
  `content.object_type` + `external_id` namespace. Incremental via the Changes
  API start-page-token captured at backfill **start**. Versioned `external_id`
  `gdrive:{file_id}:{version}`. Source `google_drive`.

### The one intentional inline exception

- **discord** — has **three** surfaces:
  - **interactions** (slash commands, webhook type-2) → `discord:interaction`.
    **Stays inline by design**: Discord requires a specific synchronous
    response body (`CHANNEL_MESSAGE_WITH_SOURCE`), which the async `202` cutover
    contract cannot satisfy. Documented at `router.py:85-92` as an M5.4
    deferral; *not* in `_CUTOVER_ENABLED_PROVIDERS`. It is still
    shadow-source-mapped, so it appears on the raw tier.
  - **gateway messages** (live `MESSAGE_CREATE` over the bot WSS) →
    `discord_gateway_worker` builds a `RawEnvelope` with the canonical producer
    → full pipeline → `discord:message`.
  - **backfill** (channel-window sampling) → full pipeline → `discord:message`.

  So Discord *content* (messages) lands via the full pipeline; only the
  *interaction acknowledgement* is synchronous. Source `discord`.

### Source/channel quick reference

| Source | Webhook→pipeline | Push | Poll/backfill→pipeline | Primary channel(s) | Inline-only? |
|---|---|---|---|---|---|
| slack | ✅ cutover | — | ✅ | `slack:message` | no |
| github | ✅ cutover | — | ✅ | `github:webhook` | no |
| jira | ✅ cutover | — | ✅ | `jira:issue` | no |
| notion | ✅ always (special handler) | — | ✅ (poll) | `notion:object` | no |
| gmail | — | ✅ Pub/Sub | ✅ | `gmail:` | no |
| google_calendar | — | — | ✅ | `google_calendar:event` | no |
| google_drive | — | — | ✅ | `google_drive:file` (+comment/revision) | no |
| discord | interactions inline (by design) | — | ✅ messages | `discord:message`, `discord:interaction` | interactions only |

---

## 5. The source registry — every place a source must be allow-listed

Adding/keeping a source on the pipeline means it must appear in **all** of these
(missing any one silently drops the source — usually as a `normalizer_parse_error`
DLQ or a never-started run). All 8 sources are currently present in all of them:

- `services/ingestion/raw_tier/envelope.py` — `SourceLiteral` + `IngressKindLiteral`
- `services/ingestion/raw_tier/s3.py::build_raw_s3_key` — source guard
- `services/ingestion/normalizer/invariants.py::_S3_KEY_RE` — source alternation
- `services/ingestion/core.py` — embedding gate
- `services/ingestion/progress/events.py` — `Source` Literal
- `services/ingestion/dlq/publish.py` — `_VALID_SOURCES`
- `services/ingestion/workflows/tenant_onboarding.py` — `VALID_SOURCES` +
  `_LOAD_ACTIVE_SOURCES_SQL` `provider IN (...)`
- `services/ingestion/workflows/source_onboarding.py` — `VALID_SOURCES` +
  install-load SQL `SELECT secret_ref`
- `services/ingestion/workflows/shard_fetch.py` — install-load SQL `SELECT secret_ref`
- `services/ingestion/handlers/__init__.py` — handler import (runs `@register`);
  trust tier via `CHANNEL_TRUST_MAP` entry **or** the handler's own
  `CHANNEL_TRUST_MAP.setdefault(_CHANNEL, _TRUST)` at import (gcal/drive/jira/notion)
- DB migrations — the four M6 source `CHECK` constraints (newest migration must
  carry forward every prior source or it drops them; see migration `0062_jira.sql`)
- Webhook-only: `services/webhooks/router.py` maps
  (`_PROVIDER_TO_SHADOW_SOURCE`, `_CUTOVER_ENABLED_PROVIDERS`, `_PROVIDER_CHANNEL`)
  + a `tenant_resolver` extractor + a `signatures/<provider>.py` verifier

---

## 6. Non-pipeline handlers (intentionally kept, NOT among the 8 sources)

These are registered handlers that are **not** production ingestion sources;
they exist for the **demo simulator** and earlier scaffolding. They are kept
because they have live callers/tests, but they have **no** planner/fetcher/
reconciler and are not in the source registry:

- `services/ingestion/handlers/email.py` (`email:inbound`) and
  `handlers/calendar.py` (`calendar:sync`) — imported only by
  `services/demo/simulator.py` (the demo UI routes friendly payloads through
  inline `ingest()`). Superseded for real ingestion by `gmail`/`google_calendar`.
- `services/ingestion/handlers/linear.py` (`linear:webhook`) and
  `handlers/stripe.py` (`stripe:webhook`) — registered handlers with signature
  verifiers + tests, but no source-registry/quartet wiring; not reachable via
  the production webhook router. Legacy/forward-looking.
- `services/ingestion/handlers/system.py` (`internal:*`) — system-originated
  observations (state_change/anomaly/prediction_resolution), not external ingress.

---

## 7. Cleanup done on this branch

Removed dead scaffolding that nothing imported (only self-referential smoke
tests), confirmed by a full `services/ingestion` test run (410 passing; the 159
errors are the pre-existing migration-CHECK-vs-populated-DB landmine, unrelated):

- `services/ingestion/reconciler/` (singular) — empty stub superseded by
  `services/ingestion/reconcilers/` (the real per-source implementations).
- `services/ingestion/activities/` — empty Temporal-activity stub (LLD §4/§9
  scaffolding never filled; the real logic lives in `workflows/`).

---

## 8. Proving it — the sandbox

The real-API sandbox (`docs/ingestion/sandbox-real-api-runbook.md`,
`docker-compose.yml` + `docker-compose.sandbox.yml`, `.env.sandbox`) stands the
whole pipeline up locally under **prod guards** (`FYRALIS_ENV=prod`: real
signature verification, real OAuth) and exercises the **real** Slack / GitHub /
Discord / Notion / Jira APIs (Google suite intentionally out of scope here —
needs GCP domain-wide delegation). ngrok tunnels provider webhooks to the local
gateway; the Discord live path is the bot WSS (no public URL).

Validation oracle: `scripts/sandbox_inspect.py` — pass when, per source, there
is an enabled install, an `install` onboarding trigger, runs reaching
`complete`/`feels_onboarded`, observations on the right `source_channel`, per-channel
`total == distinct_external_id` (cross-path dedup holds), embeddings draining,
and an empty `ingestion_failures`. The raw tier in MinIO
(`fyralis-raw/dev/<source>/...`) is the durable evidence each source produced to
`ingestion.raw`.
