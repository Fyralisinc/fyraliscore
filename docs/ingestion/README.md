# Ingestion — End to End

> **What this folder is.** The complete description of how every external signal
> (a Slack message, a GitHub push, a calendar event, …) becomes an **Observation**
> row that the rest of Fyralis reasons over. Start here, then drill into the
> [canonical architecture](architecture.md) for diagrams, or a
> [per-source file](#per-source-integrations) for one integration's specifics.

---

## The one-sentence model

Every source's **primary** path is the full **Kafka pipeline**

```
source → ingestion.raw → normalizer → ingestion.normalized → observation_writer → observations
```

Synchronous in-process `ingest()` is only a **fallback** for Kafka-outage
degradation, plus one deliberate exception (Discord *interactions*, which need a
synchronous response body). Both paths converge on the same
`ingest_from_draft()` logic, so the observation written is byte-identical
regardless of route.

## The eight production sources

| Source | Ingress | Live path | Backfill | Primary channel | Trust |
|---|---|---|---|---|---|
| [Slack](sources/slack.md) | webhook | Events API webhook → pipeline (cutover) | channels → history | `slack:message` | `attested_agent` |
| [GitHub](sources/github.md) | webhook | App webhook → pipeline (cutover) | per accessible repo | `github:webhook` | `authoritative` |
| [Discord](sources/discord.md) | webhook + WSS | gateway `MESSAGE_CREATE` → pipeline | channel-window sampling | `discord:message`, `discord:interaction` | `attested_agent` |
| [Gmail](sources/gmail.md) | Pub/Sub push | Pub/Sub → pipeline (`poll` ingress) | History API | `gmail:` | `attested_agent` |
| [Notion](sources/notion.md) | webhook (special) | poll via periodic reconciler | databases/pages → blocks → comments | `notion:object` | `attested_agent` |
| [Google Calendar](sources/google-calendar.md) | none | poll (`syncToken`) | windowed per calendar | `google_calendar:event` | `authoritative` |
| [Google Drive](sources/google-drive.md) | none | poll (Changes API) | My Drive + Shared Drives | `google_drive:file` | `authoritative` |
| [Jira](sources/jira.md) | webhook | HMAC webhook → pipeline (cutover) | `POST /rest/api/3/search/jql` | `jira:issue` | `authoritative` |

`ingress_kind` is one of `webhook`, `gateway`, `pubsub`, `backfill`, `poll`
([raw_tier/envelope.py](../../services/ingest/ingestion/raw_tier/envelope.py)). The
normalizer maps `(source, ingress_kind) → channel`
([normalizer/channel_mapping.py](../../services/ingest/ingestion/normalizer/channel_mapping.py)).

## The five pipeline stages

| # | Stage | Entry point | Topic in → out |
|---|---|---|---|
| 1 | **Ingress** | webhook router / gmail pubsub / `shard_fetch` | — → `ingestion.raw` |
| 2 | **Shadow-write raw** | [shadow_write.py](../../services/ingest/ingestion/shadow_write.py) | — → `ingestion.raw` (+ S3) |
| 3 | **Normalize** | [normalizer/worker.py](../../services/ingest/ingestion/normalizer/worker.py) (no DB) | `ingestion.raw` → `ingestion.normalized` |
| 4 | **Write observation** | [writers/observation_writer.py](../../services/ingest/ingestion/writers/observation_writer.py) | `ingestion.normalized` → Postgres `observations` |
| 5 | **Async tails** | embedding worker, embedding backlog, DLQ writer | `ingestion.embedding`, `ingestion.dlq` |

See [architecture.md](architecture.md) for the full diagrams of each stage, the
two-path decision, the per-source ingress, and the onboarding/backfill chain.

## How signals enter (three ingress shapes)

1. **Webhook** — Slack / GitHub / Jira / Discord-interactions hit
   `gateway /webhooks/{provider}`; Notion hits a dedicated handler. Signature is
   verified, tenant is resolved, body is shadow-written to `ingestion.raw`, and
   the provider gets a `202 Accepted` (cutover) or `200/201` (inline fallback).
2. **Push** — Gmail's Google Pub/Sub notification hits the dedicated
   [gmail_pubsub.py](../../services/app/webhooks/gmail_pubsub.py) endpoint, which
   fetches the real message and publishes it to `ingestion.raw`.
3. **Poll / backfill** — for **all 8** sources the
   [shard_fetch](../../services/ingest/ingestion/workflows/shard_fetch.py) worker pulls
   from the provider API and produces a `RawEnvelope` to `ingestion.raw` exactly
   like a webhook. Backfill/poll **never** calls inline `ingest()`.

## The per-tenant gate (the "0 observations despite a clean run" trap)

The full pipeline only persists for a tenant whose flag
`ingestion.kafka_path_enabled` is `TRUE`
([feature_flags/client.py](../../services/ingest/ingestion/feature_flags/client.py),
30 s TTL cache, **defaults to FALSE**). A fresh tenant's envelopes flow through
Kafka but the writer records them as *shadow events* and does not persist. Enable
per tenant:

```python
TenantFlags.set_bool(tenant, "ingestion.kafka_path_enabled", True, set_by=...)
```

The sandbox tenant `00000000-…-0001` already has it `TRUE`.

## Per-source integrations

Each file documents the integration's OAuth/auth model, ingress, backfill +
incremental strategy, channel(s) and trust, dedup/`external_id` scheme, tables,
migrations, and gotchas:

- [Slack](sources/slack.md)
- [GitHub](sources/github.md)
- [Discord](sources/discord.md)
- [Gmail](sources/gmail.md)
- [Notion](sources/notion.md)
- [Google Calendar](sources/google-calendar.md)
- [Google Drive](sources/google-drive.md)
- [Jira](sources/jira.md)

## Adding a new source

A source must be allow-listed in **every** place listed in
[architecture.md §"The source registry"](architecture.md#the-source-registry-every-place-a-source-must-be-allow-listed)
— missing any one silently drops it (usually as a `normalizer_parse_error` DLQ
or a run that never starts). The newest source's DB migration must carry forward
every prior source in the four M6 `CHECK` constraints.

## Non-pipeline handlers (kept, but not among the 8 sources)

`email:inbound` and `calendar:sync` (demo simulator only), `linear:webhook` and
`stripe:webhook` (registered + verifier + tests, but no quartet wiring), and
`internal:*` (system-originated observations). See
[architecture.md §"Non-pipeline handlers"](architecture.md#non-pipeline-handlers).
