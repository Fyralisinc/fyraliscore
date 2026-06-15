# Ingest — Signal Intake

> Source: `services/ingest/` (packages `ingestion`, `integrations`, `synthetic`,
> `code_intel`, `github_intel`). Part of the [architecture overview](index.md).

**One-line:** normalizes every external company signal into a tenant-scoped
`ObservationDraft`, persists it as a deduped [observation](../glossary.md), and
enqueues a `T1` Think trigger — via a synchronous **inline** path and a parallel
**Kafka/S3 pipeline** that converge on the same `ingest_from_draft` logic.

## Responsibilities

The ingest layer turns external signals into `observations` rows and kicks off
downstream reasoning. There are **two convergent paths**:

1. **Inline** — `core.ingest()` is called synchronously from the gateway webhook
   router, the Slack/finance routers, the Discord gateway, and the synthetic
   injector.
2. **Kafka full-pipeline** — `source → ingestion.raw.{source}` (raw body in S3 +
   a `RawEnvelope`) → **normalizer** worker → `ingestion.normalized.{source}` →
   **observation_writer**.

Both converge on `core.ingest_from_draft()`, so the written observation is
identical regardless of route. The `observation_writer` branches per tenant on
the `ingestion.kafka_path_enabled` flag, which is **kafka-first by default** — a
tenant with no flag row takes the full pipeline; an explicit `FALSE` (operator or
circuit-breaker **kill-switch**) forces it back to inline. Ingress and the writer
read this through one helper (`TenantFlags.kafka_path_enabled()`) so they cannot
drift. See [ADR-0001](../adr/0001-kafka-first-ingestion-default.md).

**The 7-step ingest** (`core.py`): (1) handler extracts an `ObservationDraft`
[+ step 1.5 inline GitHub enrichment for `github:webhook`]; (2) pre-assign a
`uuid7`; (3) `ActorRepo` resolves the source actor ref (misses →
`content._unresolved_actor_ref`); (4) `EntityAliasRepo` fast-path entity lookup
over 1–3-gram phrases (misses → `content._unresolved_phrases` for the
[entity_resolver worker](workers.md)); (5) Ollama embedding (768-d; failure →
`embedding_pending=True`); (6) `ObservationRepository.insert` in a transaction
(dedup on `(source_channel, external_id, occurred_at)`, where each source's
composed `external_id` comes from the central `idempotency` constructors —
see the module table); (7) enqueue a
`T1`/`event_arrival` row into `think_trigger_queue` unless deduped. Post-commit
`observations_new` NOTIFY is flushed after commit; missing monthly partitions
self-heal and retry once.

**Handler registry + trust map** (`handlers/__init__.py`): a `register(channel)`
decorator self-registers handlers at import; `CHANNEL_TRUST_MAP` is the
authoritative channel→trust-tier table (handlers may override per event). The
registered channels span Slack, internal, GitHub, Linear, Stripe, Discord,
Gmail/email, Notion, Google Calendar/Drive, calendar sync, Jira, Mercury,
QuickBooks, Grafana, and Telegram.

**Raw-tier publish** (`shadow_write_raw`): hashes the raw body, `PutIfAbsent`
to S3 (`s3://fyralis-raw`), builds a `RawEnvelope`, and publishes to
`ingestion.raw.{source}`. This same mechanism serves two roles: for a kafka-first
tenant it is the **primary write** (ingress returns `202`, skips inline); for a
killed tenant it is the best-effort **post-inline audit** that never fails the
inline `200`. The request-path flush is bounded by `CUTOVER_FLUSH_TIMEOUT_SEC`
(default 2.0s) so a slow broker trips the inline fallback fast. Per-source topic
lanes prevent head-of-line blocking across sources.

**Intelligence enrichment** — `code_intel` maintains a commit-SHA-versioned
per-repo code graph + code-RAG embeddings ("blast radius"); `github_intel`
maintains PR/CI/branch/issue FSMs from `github:webhook` observations and writes
causal context inline into the observation's `content['intelligence']`
(raw-on-failure) and to `github_signal_enrichment` (an ordered per-repo worker
drains `github_intel_queue`).

## How it's wired

```mermaid
graph TD
    SRC["External source"]
    GWR["Gateway / webhook router"]
    DSC["Discord gateway"]
    SYN["Synthetic injector"]

    ING["core.ingest()"]
    REG["Handler registry"]
    DRAFT["ObservationDraft"]
    GHI["github_intel inline enrich"]
    IFD["core.ingest_from_draft()"]
    OREPO["ObservationRepository"]
    OBS[("observations")]
    TTQ[("think_trigger_queue (T1)")]

    SW["shadow_write_raw"]
    S3["S3 raw bucket"]
    KRAW["Kafka ingestion.raw"]
    NORM["Normalizer worker"]
    KNORM["Kafka ingestion.normalized"]
    OW["Observation writer"]

    SRC -->|"webhook / pubsub"| GWR --> ING
    DSC -->|"discord:message/interaction"| ING
    SYN -->|"direct injection"| ING
    ING --> REG --> DRAFT
    ING --> IFD
    IFD -->|"if github:webhook"| GHI
    IFD -->|"ActorRepo · EntityAliasRepo · Ollama"| IFD
    IFD --> OREPO --> OBS
    IFD -->|"step 7"| TTQ
    GWR -. "cutover (202) / audit" .-> SW
    SW --> S3
    SW --> KRAW --> NORM
    NORM -->|"fetch body"| S3
    NORM --> KNORM --> OW
    OW -->|"default; unless killed"| IFD
```

## Integration sources

The handler registry / `RawEnvelope.SourceLiteral` define **twelve source
families**: Slack, GitHub, Discord, Gmail, Notion, Google Calendar, Google Drive,
Jira, Mercury, QuickBooks, Grafana, and Telegram (plus internal/system channels,
and Linear/Stripe handlers registered in code). Each lives under
`services/ingest/integrations/<source>/` (OAuth, client, onboarding) with pipeline
glue in `services/ingest/ingestion/`. **Telegram** is gateway-style — the MTProto
user-account API, a persistent updates connection with no HTTP webhook (backfill
on `messages.getHistory`/`offset_id`, live on `pts/qts/seq/date`/`getDifference`);
see [ADR-0003](../adr/0003-telegram-mtproto-user-account-ingestion.md) and the
[Telegram source spec](../ingestion/sources/telegram.md).

!!! note "Doc-vs-code discrepancies (verified)"
    - Older architecture notes described a routing decision via
      `services/platform/execution/routing.py` writing `signal_routing_decisions`.
      **No such import exists in `services/ingest/`**, and migration `0127`
      dropped that retired ledger. Active ingest writes `think_trigger_queue`
      directly through the shared trigger helper.
    - `docs/ingestion/README.md` says "eight production sources" while the code
      defines **ten** source families (adds Google Drive + the Mercury/QuickBooks
      finance pair). **TODO(human):** are finance/Drive non-production, or is the
      doc stale?

## Key modules & entry points

| Module | Path | Role |
|--------|------|------|
| Uniform ingest | `services/ingest/ingestion/core.py` | `ingest()` / `ingest_from_draft()` — the shared normalize→persist→enqueue path. |
| Handler registry | `services/ingest/ingestion/handlers/__init__.py` | `register`/`get_handler`, `CHANNEL_TRUST_MAP`, the `ObservationDraft` dataclass. |
| `external_id` constructors | `services/ingest/ingestion/idempotency/__init__.py` | The single home for every **composed** dedup key (M5 / LLD §6) — the 18 namespaced + IN-15-versioned `external_id` formats (slack, gmail, discord, notion, github-push, grafana, gcal, gdrive, jira, mercury, qbo). Each handler imports `idempotency` and calls them, so a source's key can't drift across its webhook/backfill/poll paths (the `test_backfill_external_id_parity` invariant becomes structural). Adopted-verbatim keys (Stripe `evt_…`, GitHub `node_id`, `Message-ID`, Linear ids) are assigned inline by their handler — there's nothing to compose, so they're intentionally not here. |
| Workflow services | `services/ingest/ingestion/workflows/*.py` | One long-running asyncio service per file (`oauth_poller`, `tenant_onboarding`, `source_onboarding`, `shard_fetch`, `reconciler`, `periodic_reconciler`, `feels_onboarded_monitor`); compose launches each as `python -m …workflows.<module>`. The legacy `__main__.py` `WORKFLOW_SERVICE` selector still resolves them but no compose service uses it. |
| Progress publisher | `services/ingest/ingestion/progress/{events,publisher}.py` | The `onboarding.progress` Kafka contract Bridge consumes — 7 Pydantic event models + `publish_progress_event(s)`. See [Onboarding progress events](#onboarding-progress-events). |
| Normalizer | `services/ingest/ingestion/normalizer/worker.py` | Kafka Path B (no DB): raw → handler → `NormalizedEnvelope`. |
| Observation writer | `services/ingest/ingestion/writers/observation_writer.py` | Kafka Path A: normalized → `ingest_from_draft` when full-mode. |
| Cutover circuit breaker | `services/ingest/ingestion/feature_flags/circuit_breaker.py` | Singleton lag guardrail — per-source lag → auto-flips `kafka_path_enabled=FALSE` on sustained breach. `python -m services.ingest.ingestion.feature_flags`. |
| Re-enable tool | `scripts/reenable_kafka_path.py` | Operator: list tripped tenants + flip one back onto the Kafka path. |
| GitHub intel | `services/ingest/github_intel/worker.py`, `api.py` | Per-repo FSM + enrichment worker; read-only `/github-intel/*` router. |
| Integrations OAuth | `services/ingest/integrations/router.py` | `/integrations/{provider}/{install,callback}` (Slack/Discord/GitHub/Notion). |
| Discord gateway worker (HA) | `scripts/run_discord_gateway_worker.py`, `services/ingest/integrations/discord/gateway/{worker,lifecycle,leader_lock,session_state}.py` | The single live WSS consumer for Discord. A bot token may be connected by exactly **one** gateway session — two replicas double-deliver every frame. The launcher composes `lifecycle.py`: it acquires the `gateway:discord:leader_lock` Redis lease (M4.1, 30s TTL / 10s refresh) **before** connecting and refreshes it alongside the WS loop, standing down (not fighting) when another pod takes over. `REDIS_URL` is **mandatory** — without the lease there is no double-delivery guard, so a missing DSN fails loud (exit 2). For crash-RESUME (M4.2) it loads the persisted `gateway_session_state` on startup (so a restart RESUMEs and Discord replays buffered frames instead of re-IDENTIFYing) and passes an `on_dispatched` save hook that persists the session cursor after every dispatched frame (keyed by `DISCORD_CLIENT_ID`; RESUME degrades off, lease stays, when it's unset). Lease-acquire timeout / mid-run loss exit `3` (transient → orchestrator restarts to stand by). |
| Telegram gateway worker (HA) | `scripts/run_telegram_gateway_worker.py`, `services/ingest/integrations/telegram/gateway/{worker,dispatch,client,session_state}.py` | The single live MTProto updates connection for Telegram — the Discord-gateway analog. A Telegram authorization may be driven by only one live connection at a time, so the launcher acquires the `gateway:telegram:leader_lock` Redis lease (`REDIS_URL` mandatory) **before** connecting. Holds the persistent updates connection on the **live session**, advances the `pts/qts/seq/date` update-state (`telegram_update_state`) using `updates.getDifference`/`getChannelDifference` as the native reconciler, and shadow-writes each update to `ingestion.raw.telegram` (`ingress_kind=gateway`) for the kafka-first path (inline `core.ingest` fallback). Backfill runs on a **separate** authorization (the backfill session) so the two never share one `auth_key` — see [ADR-0003](../adr/0003-telegram-mtproto-user-account-ingestion.md). |
| Google DWD connect | `services/ingest/integrations/{gmail,google_calendar,google_drive}/oauth.py` | `POST /integrations/{gmail,google_calendar,google_drive}/connect/{preflight,finalize}` — first-party Domain-Wide-Delegation connect wizard (no OAuth bounce): `preflight` enumerates the Workspace domain for the selector UI (or returns the exact client_id + scopes to grant if the DWD grant is missing); `finalize` resolves the inclusion_spec → per-user mailbox/calendar/My-Drive targets (Drive also enumerates org Shared Drives) and writes the install + per-resource rows + an `onboarding_triggers` row in one transaction so the M6 backfill chain fires. All three mount in the gateway behind the `GMAIL_SERVICE_ACCOUNT_JSON*` gate (Calendar + Drive reuse Gmail's service account). Gmail additionally provisions Pub/Sub watches out-of-band; Calendar + Drive are poll-only (no async provisioning). Gmail's router also carries the install-lifecycle management endpoints — `GET …/gmail/status` (watch counts by state + last push/poll + errored mailboxes + recent audit, from `gmail/status_api.py`) and `POST …/gmail/{uninstall,mailbox/stop}` (full teardown / per-mailbox pause, from `gmail/uninstall.py`; idempotent, RLS-scoped to the tenant). |
| Jira connect | `services/ingest/integrations/jira/oauth.py` | `POST /integrations/jira/connect/{preflight,finalize}` — Bearer-authed admin connect wizard for the API-token source: `preflight` verifies the token (`JiraClient.myself()`) + enumerates projects for the selector; `finalize` re-verifies creds before any write, persists the API token + optional webhook HMAC secret in the gateway `secret_store` (opaque refs only — plaintext never hits the DB), then calls `finalize_install` (jira_installations + jira_projects + `onboarding_triggers`) and `register_webhook_installation` (the `provider_installations` row the webhook edge keys on by site host). Mounts unconditionally next to the integrations router — a genuine prod surface, not the synthetic `finance_router` panel. |
| Calendar/Drive live | `services/ingest/integrations/{google_calendar,google_drive}/{live_poller,watch}.py`, `services/ingest/integrations/_google_{live,watch}.py`, `services/app/webhooks/google_push.py` | Near-real-time live ingestion for Calendar + Drive, mirroring `gmail_watch`+`gmail_history`. `live_poller` (compose: `google_{calendar,drive}_live_poller`) leases active, cursor-seeded resources via the `last_live_poll_at` claim slot and drains the delta through the shared `drain_live` (existing fetcher + `ingest()`, dedups at `observations.UNIQUE`). `watch` (compose: `google_{calendar,drive}_watch_scheduler`) registers/renews native `events.watch`/`changes.watch` push channels; the always-mounted `/webhooks/google_{calendar,drive}/push` ingress constant-time-verifies `X-Goog-Channel-Token` and drains via the same path. Push needs `GOOGLE_PUSH_WEBHOOK_BASE` (a domain-verified HTTPS endpoint); the poller guarantees liveness when it's unset. |
| Finance connect | `services/ingest/integrations/{mercury,quickbooks}/oauth.py` | `POST /integrations/{mercury,quickbooks}/connect/{preflight,finalize}` — Bearer-authed credential wizards for the finance sources, the prod counterpart to the dev `finance_router` panel. Mercury submits a Bearer API token (verified + accounts enumerated via `list_accounts()`); QuickBooks submits `realm_id` + access/refresh tokens (verified via `company_info()` — no Intuit OAuth-bounce in-repo, so install is operator-mediated and the `oauth_poller` owns refresh thereafter). Both verify before any write, store credentials in the `secret_store` (opaque refs only), then `finalize_install` (mercury_/quickbooks_installations + sub-resources + `onboarding_triggers`) and, when a webhook secret + org/realm id are supplied, `register_webhook_installation`. Mount unconditionally next to the integrations router. |
| Synthetic | `services/ingest/synthetic/core.py` | Blessed direct-injection bypass routed through `core.ingest()` (tags `content.synthetic=true`). |
| FetchPage rate limiter | `services/ingest/ingestion/rate_limit/{client,buckets,gate}.py` | LLD §13 Lua token bucket + the `FetchRateLimiter` gate `shard_fetch`'s fetch loop calls **before each page fetch** — one token per upstream call from `rate:<tenant>:<source>:<method>`. Budgets live in `BUCKET_DEFAULTS` (slack/github/gmail/discord via `PRIMARY_FETCH_METHOD`); unbudgeted sources pass through. Enabled by `REDIS_URL`; `SHARD_FETCH_RATE_LIMIT=0` opts out. |

## Cutover circuit breaker

The Kafka full pipeline is the primary path (`kafka_path_enabled` defaults ON; a
missing flag row = kafka-first). The **cutover circuit breaker**
(`feature_flags/circuit_breaker.py`, run as the `circuit_breaker` compose
singleton) is its safety net: it watches consumer lag and pulls a tenant back to
the always-safe inline path before observations pile up.

- **What it measures.** Every tick (`BREAKER_TICK_INTERVAL_SEC`, default 60s) it
  reads committed-offset lag-in-seconds on **every** per-source raw lane
  (`ingestion.raw.<source>` vs. group `normalizer.<source>`, both derived from
  `kafka/topics.py`) and samples active tenants from the 1% traffic-signal topic
  (which carries `source` + `raw_partition`). Each tenant is judged on its
  **worst lane**.
- **When it trips.** Lag > `BREAKER_THRESHOLD_SEC` (60s) for
  `BREAKER_WINDOW_TICKS` (5) consecutive ticks — ~5 min sustained — flips that
  tenant's `kafka_path_enabled` to FALSE (`set_by=auto:circuit_breaker`), records
  `circuit_breaker_state`, and alerts (`INGESTION_ALERT_WEBHOOK_URL` if set, else
  a log). Ingress + writer observe the flip within the 30s flag-cache TTL. The
  flag is per-tenant, so a single lagging lane reverts the tenant's whole path.
- **Recovery is operator-driven** — no auto-recovery, to avoid flapping during an
  incident. After confirming the lane drained, re-enable with
  `scripts/reenable_kafka_path.py <tenant> --operator <you>` (or `--list` to see
  every tripped tenant). The breaker auto-resets its own bookkeeping on the next
  tick once it sees the flag back at TRUE.
- **Health + resilience.** Exposes `/healthz` + `/metrics` on
  `INGESTION_HEALTH_PORT` (9300) — a wedged tick loop goes 503 and is restarted. A
  per-lane probe failure is isolated (treated as no-lag for that lane that tick)
  so one bad lane can't blind the others. It detects *slow* consumption, not a
  normalizer that never committed (no offsets → reads as caught-up).
- **Verifying the live Kafka path.** The reader functions talk to a real broker,
  which the unit tests mock; `scripts/smoke_circuit_breaker_lag.py` exercises them
  — and a full unmocked `_process_tick` → flag flip — against a live broker. Run
  it after touching the lag/active-tenant readers.

## Onboarding progress events

The backfill workflow services emit a stream of `onboarding.progress` Kafka
events (the LLD §6 Bridge contract — partitioned by `tenant_id` for per-tenant
ordering). The 7 event models live in `progress/events.py`; every one now has a
producer call site:

| Event kind | Emitted by | When |
|------------|-----------|------|
| `tenant.onboarding.started` | `tenant_onboarding` | run goes `pending → running` (sources fanned out) |
| `source.onboarding.started` | `source_onboarding` | a source's plan is produced (`planned_shard_count`) |
| `shard.fetched` | `shard_fetch` | a shard reaches `done` (`observation_count` = records fetched) |
| `source.onboarding.complete` | `reconciler` | the first clean reconciliation pass (`coverage_confidence`/`gaps_resolved` from the pass count + re-shared children) |
| `source.onboarding.feels_onboarded` | `feels_onboarded_monitor` | a source's last-N-days are queryable |
| `tenant.onboarding.complete` | `tenant_onboarding` | all sources roll up successfully |
| `tenant.onboarding.behind_schedule` | `feels_onboarded_monitor` | ops-only; a run is past the threshold with no `feels_onboarded` |

**Ordering / dedup.** Each lifecycle transition is claim-via-UPDATE guarded, so
each orchestrator collects its events inside the per-signal transaction and
publishes them **post-commit** via the shared `publish_progress_events` helper
(a no-op when no producer is wired — the orchestrators take an *optional*
`kafka_producer`, present only in their `_run_*` entrypoints). A publish that
fails after the commit drops a progress (not load-bearing) event; the transition
itself is durable and Bridge dedups on `(event_kind, tenant_id, source?,
shard_id?)`. `feels_onboarded` and `behind_schedule` are single-fire per run via
the `onboarding_runs.feels_onboarded_at` / `behind_schedule_emitted_at`
(migration `0080`) claim slots.

## Dependencies

**Inbound** *(verified)*: gateway webhook/slack/finance routers, the Discord
gateway dispatch, the synthetic injector, and the Kafka `raw`/`normalized` topics.

**Outbound** *(verified)*: `services.domain.observations` (insert + state-change
NOTIFY + partition self-heal), `services.domain.actors` / `entity_aliases`,
`lib.embeddings` (Ollama), `think_trigger_queue`, S3 + Kafka, and inline
`github_intel`/`code_intel` enrichment.

## Design rationale

The Kafka full pipeline is the **default** ingestion path and inline ingest is the
fallback + kill-switch. Inline was historically primary only as a migration
de-risking step (the "zero-divergence soak" validated the async lane against the
synchronous source of truth); with that complete, the default is now kafka-first
and `ingestion.kafka_path_enabled` is a per-tenant kill-switch — so there is no
longer a per-tenant *enable* ramp. The *automated* FALSE direction (sustained lag
→ revert a tenant to inline) and the operator recovery path are the
[Cutover circuit breaker](#cutover-circuit-breaker) above. Full context,
alternatives, and rollout in
[ADR-0001](../adr/0001-kafka-first-ingestion-default.md).

> **TODO(human):** Capture the *why* for:
>
> - The full trust-tier ordering/semantics (the *mapping* is in code; the rationale
>   for `authoritative` vs `attested_agent` vs `inferential` vs `unvetted` is not).
> - The intended meaning of the `feels_onboarded` state and its thresholds.
> - The intended lifecycle of the Linear/Stripe/calendar/email handlers vs. the
>   "documented" sources — which are legacy or test-only.
