# Finance Signal Sources — Mercury + QuickBooks

> Two new ingestion sources that give the reasoning layer a company's **money
> picture** — cash position, burn/runway (Mercury) and revenue, AR/AP,
> obligations (QuickBooks) — alongside the engineering/ops signals it already
> has. Both flow through the existing full pipeline (S3 → Kafka → normalizer →
> observation_writer → observations → think_worker). No pipeline changes; we
> replicate the per-source contract.

Branch: `feature/finance-signal-sources` (off `integration/ingestion-hardening`,
the canonical 8-source base: slack, github, discord, gmail, notion,
google_calendar, google_drive, jira).

| | Mercury | QuickBooks Online |
|---|---|---|
| Source key | `mercury` | `quickbooks` |
| Primary channel | `mercury:transaction` | `quickbooks:object` |
| Trust tier | `authoritative` | `authoritative` |
| Auth | API token, HTTP **Basic** (token as username) — Jira-shaped | OAuth 2.0 + `realmId`, refresh-token **rotation** |
| Backfill | `GET /accounts`, `GET /accounts/{id}/transactions` (cursor) | query language `SELECT … WHERE Metadata.LastUpdatedTime …` + `STARTPOSITION` |
| Live ingress | webhook `/webhooks/mercury/events` (HMAC) | webhook `/webhooks/quickbooks/events` (HMAC-SHA256 `intuit-signature`, CloudEvents) |
| Dedicated tables | `mercury_installations`, `mercury_accounts` | `quickbooks_installations`, `quickbooks_entities` |
| Migration | `0066_mercury.sql` | `0067_quickbooks.sql` |

Implementation order: **Mercury first** (token auth = the smaller Jira-shaped
surface, proves the finance-source pattern end-to-end), then **QuickBooks**
(OAuth + realm + query language). A mock-driven testing environment + UI panel
is the deliverable: start a backfill and drive concurrent live ingestion for
both sources from the browser.

---

## 1. What signals we get

### Mercury — banking / cash (`https://api.mercury.com/api/v1/`)
Sandbox: `https://api-sandbox.mercury.com/api/v1/`.

- **Resources (core):** `GET /accounts` (id, name, type, **available + current
  balance**), `GET /accounts/{id}/transactions` (amount, counterparty, kind,
  **status** `pending|sent|failed|cancelled`, postedAt, createdAt). Cursor
  pagination: `limit`, `order`, `start`/`end` (date), `start_after` (id cursor).
- **Signals derived:** cash balance / position per account; inflow vs outflow;
  monthly **burn** and **runway**; large transactions; **failed/declined**
  payments; balance deltas / threshold crossings.
- **Auth:** API token issued from the Mercury dashboard. HTTP **Basic** with
  the token as the username + empty password (also accepts `Bearer`). OAuth2
  exists but requires Mercury approval → **deferred**. Tokens auto-delete after
  ~45 days idle → the periodic reconciler doubles as a keepalive.
- **Webhooks:** HMAC-signed POSTs on resource changes; plus a pull-based Events
  API for backfill/reconcile.
- **Mutability:** a transaction changes status (pending → sent → failed) →
  **versioned external_id** required.

### QuickBooks Online — accounting / GL (`https://quickbooks.api.intuit.com/v3/company/{realmId}/`)
Sandbox: `https://sandbox-quickbooks.api.intuit.com/v3/company/{realmId}/`.

- **Resources (core):** `Invoice`, `Bill`, `BillPayment`, `Payment`. Read via
  the **query endpoint** (`/query?query=SELECT * FROM Invoice WHERE
  Metadata.LastUpdatedTime > 'ts' ORDERBY Metadata.LastUpdatedTime
  STARTPOSITION n MAXRESULTS 100`). `Metadata.LastUpdatedTime` is the
  incremental cursor; `SyncToken` is the per-entity version.
- **Signals derived:** revenue (invoices/payments); **AR aging** / overdue
  invoices / DSO; **AP aging** / upcoming obligations (bills); payment events;
  cash-flow.
- **Auth:** OAuth 2.0 only. Access token ~60 min; **refresh token ~100 days but
  ROTATES on every refresh** (old invalid within 24h) → must persist the new
  refresh token each cycle. `realmId` = company id, returned at callback,
  scopes every call. Scope `com.intuit.quickbooks.accounting`.
- **Limits:** 10 req/s, 120/min batch, 10 concurrent per realm; 429 on
  throttle.
- **Webhooks:** HMAC-SHA256 `intuit-signature` (verifier token). **CloudEvents
  format is the go-forward** (legacy format deprecated 2026-05-15) → build the
  handler to CloudEvents directly.
- **Mutability:** invoices/bills are highly mutable (draft → sent → paid →
  overdue) → **versioned external_id** by `SyncToken`.

---

## 2. Signal modeling (per-source contract)

The handler is a pure function returning one `ObservationDraft` per call,
branching on a private `_fyralis_record_type` for backfill/poll and on the
webhook event shape for live. One channel per source (the github/jira pattern).

| | Mercury | QuickBooks |
|---|---|---|
| Channel | `mercury:transaction` | `quickbooks:object` |
| Record types | `transaction`, `account_snapshot` | `invoice`, `bill`, `bill_payment`, `payment` |
| `kind=signal` | new transaction; balance snapshot | entity created |
| `kind=state_change` | txn status change (failed/declined); balance threshold cross | invoice/bill status change (sent→paid→overdue) |
| `external_id` (versioned) | `mercury:{account_id}:txn:{txn_id}:{status}` · `mercury:{account_id}:balance:{as_of}` | `qbo:{realm_id}:{entity}:{id}:{SyncToken}` |
| `source_actor_ref` | counterparty (recipient) | customer / vendor |
| `trust_tier` | `authoritative` | `authoritative` |

**Why versioned external_id matters:** the observations repo dedups on
`(source_channel, external_id)` *ignoring* `occurred_at`. A status change on the
same txn/invoice must land as a NEW observation, so the changing attribute
(status / SyncToken) is embedded in the id (the IN-15/IN-17 mutable-source
lesson).

---

## 3. How it maps onto the full pipeline

Identical to every existing source — no new pipeline stages:

```
external API ─┬─ backfill/poll (planner → fetcher, ShardFetch loop)
              └─ live webhook (gateway /webhooks/{src})
                       │
                  shadow_write → S3 (raw envelope) → Kafka ingestion.raw.{src}
                       │
                  normalizer (channel_mapping → handler → ObservationDraft)
                       │
                  Kafka ingestion.normalized.{src}
                       │
                  observation_writer → observations table (+ embedding, +T1 trigger)
                       │
                  think_worker / models
```

### Per-source code surface (mirrors Jira / Google Drive)
- **Source registration** — add `"mercury"`/`"quickbooks"` to the 17 allowlist
  sites (see §5). The `SourceLiteral` in
  [raw_tier/envelope.py](../../services/ingest/ingestion/raw_tier/envelope.py) is the
  single source of truth that Kafka topics + provisioning derive from.
- **Integration client** — `services/ingest/integrations/{src}/client.py` (+
  `onboarding.py`, `metrics.py`). Mercury mirrors
  [jira/client.py](../../services/ingest/integrations/jira/client.py) (Basic auth from
  `secret_ref`); QuickBooks mirrors the OAuth bot-token shape
  ([notion/oauth.py](../../services/ingest/integrations/notion/oauth.py)) + the
  [oauth_poller](../../services/ingest/ingestion/workflows/oauth_poller.py) for refresh.
- **Client builders** — `services/ingest/ingestion/fetchers/_clients.py`: both
  `build_{src}_client` AND `open_{src}_client` (the fetcher imports the opener;
  a missing opener passes unit tests but fails the real worker).
- **Planner / Fetcher / Reconciler / Handler** — register into
  `PLANNER_DISPATCH` / `FETCHER_DISPATCH` / `RECONCILER_DISPATCH` and
  `@register(channel)`. Contracts: `ShardPlan(shard_kind, shard_identifier,
  seed_cursor)`, `FetchResult(records, next_cursor, end_of_data)`,
  `ReconcileResult(records, gap_closed, detail)`, `ObservationDraft(...)`.
- **Webhook edge** — `services/app/webhooks/signatures/{src}.py` (HMAC) +
  `tenant_resolver._extract_{src}` + router maps
  (`_PROVIDER_TO_SHADOW_SOURCE`, `_CUTOVER_ENABLED_PROVIDERS`,
  `_PROVIDER_CHANNEL`). Tenant resolution + secret loading reuse
  `provider_installations` (no generic secret-loader change — the Jira reuse trick).
- **Migration** — `db/migrations/00NN_{src}.sql`: dedicated tables + RLS +
  widen the four M6 source CHECKs **carrying every prior source forward**.

---

## 4. Dedicated tables (migrations)

### `0066_mercury.sql`
- `mercury_installations` — one per (tenant, api token): `id, tenant_id,
  base_url, secret_ref` (API token), `webhook_secret_ref`, `created_at,
  disabled_at`. `UNIQUE (tenant_id, base_url)`.
- `mercury_accounts` — one per account to shard on: `id, tenant_id,
  mercury_installation_id (FK), account_id, account_name, account_kind,
  txn_cursor` (high-water id/date), `last_synced_at, state`.
  `UNIQUE (mercury_installation_id, account_id)`.

### `0067_quickbooks.sql`
- `quickbooks_installations` — one per (tenant, realm): `id, tenant_id,
  realm_id, base_url, secret_ref` (access token), `refresh_secret_ref`,
  `token_expires_at`, `webhook_secret_ref`, `created_at, disabled_at`.
  `UNIQUE (tenant_id, realm_id)`.
- `quickbooks_entities` — one per (realm, entity-type) shard: `id, tenant_id,
  quickbooks_installation_id (FK), entity_type` (Invoice|Bill|BillPayment|
  Payment), `updated_cursor` (LastUpdatedTime high-water), `last_synced_at,
  state`. `UNIQUE (quickbooks_installation_id, entity_type)`.

Both: ENABLE + FORCE RLS with the `tenant_isolation` policy
(`current_setting('app.current_tenant')`), mirroring the `jira_*` template.

**Source-CHECK carry-forward (landmine):** every new source migration
DROP+re-ADDs the same four constraints (`source_onboarding_runs`,
`onboarding_shards`, `ingestion_failures`, `onboarding_triggers`); the last
applied wins, so each MUST list **every** prior source. `0066` lists
`…,'jira','mercury'`; `0067` lists `…,'jira','mercury','quickbooks'`.

---

## 5. Source-registration checklist (17 files)

Every file below hardcodes the valid-source set; missing one silently drops the
new source at a different stage. Verified by `grep -rln "'jira'" services/ lib/
scripts/`:

1. [raw_tier/envelope.py](../../services/ingest/ingestion/raw_tier/envelope.py) — `SourceLiteral`
2. [raw_tier/s3.py](../../services/ingest/ingestion/raw_tier/s3.py) — `build_raw_s3_key` source guard
3. [normalizer/invariants.py](../../services/ingest/ingestion/normalizer/invariants.py) — `_S3_KEY_RE`
4. [normalizer/channel_mapping.py](../../services/ingest/ingestion/normalizer/channel_mapping.py) — (source, ingress_kind) → channel
5. [core.py](../../services/ingest/ingestion/core.py) — embedding gate
6. [shadow_write.py](../../services/ingest/ingestion/shadow_write.py)
7. [dlq/publish.py](../../services/ingest/ingestion/dlq/publish.py) — `_VALID_SOURCES`
8. [progress/events.py](../../services/ingest/ingestion/progress/events.py)
9. [kafka/topics.py](../../services/ingest/ingestion/kafka/topics.py) (derives from `SourceLiteral`)
10. [workflows/tenant_onboarding.py](../../services/ingest/ingestion/workflows/tenant_onboarding.py) — `VALID_SOURCES` + `_LOAD_ACTIVE_SOURCES_SQL`
11. [workflows/source_onboarding.py](../../services/ingest/ingestion/workflows/source_onboarding.py) — `VALID_SOURCES` + install-load SQL (`SELECT secret_ref`)
12. [workflows/shard_fetch.py](../../services/ingest/ingestion/workflows/shard_fetch.py) — install-load SQL
13. [feels_onboarded_monitor.py](../../services/ingest/ingestion/feels_onboarded_monitor.py)
14. [recovery/embedding_backlog.py](../../services/ingest/ingestion/recovery/embedding_backlog.py)
15. [webhooks/router.py](../../services/app/webhooks/router.py) — provider maps
16. [webhooks/tenant_resolver.py](../../services/app/webhooks/tenant_resolver.py) — `ResolverProvider` + extractor
17. [scripts/provision_kafka_topics.py](../../scripts/provision_kafka_topics.py)

---

## 6. Testing environment (the deliverable)

A Provider-Lab-driven environment where a user can, **from the browser**, start a
backfill and concurrently drive live ingestion for both sources.

### Provider Lab
`services/ingest/synthetic/provider_lab/` serves catalog-validated,
per-tenant fixtures (accounts/transactions; invoices/bills/payments) below
`/mercury` and `/quickbooks`. Local runs set `PROVIDER_LAB_URL` for test
credentials and pass `MERCURY_API_BASE_URL` / `QUICKBOOKS_API_BASE_URL`
explicitly; the production endpoint resolver has no single-host fallback.

### Gateway control endpoints (`services/app/gateway/finance_routes.py`)
A new router mounted in [gateway/app.py](../../services/app/gateway/app.py),
modeled on [demo_routes.py](../../services/app/gateway/demo_routes.py):
- `POST /finance/{source}/install` — seed install + accounts/entities, flip
  `ingestion.kafka_path_enabled`, emit the onboarding trigger (starts backfill).
- `POST /finance/{source}/backfill` — (re)trigger backfill (manual_replay).
- `POST /finance/{source}/live/emit` — synthesize one live event and POST it to
  the real `/webhooks/{source}` edge (HMAC-signed) → full pipeline.
- `GET  /finance/{source}/status` — counts: raw envelopes, observations,
  per-channel breakdown, last N observations (so the UI shows progress).

### UI panel (`/finance` route)
A `FinanceConsole` page (template:
[SignalSimulator](../../ui/src/components/SignalSimulator.tsx) +
[demo-client.ts](../../ui/src/api/demo-client.ts)) with, per source: an
**Install + Backfill** button, a **live emit** control (single + auto-loop for
concurrent live traffic), and a live **status** table that polls
`/finance/{source}/status`. Runs against the gateway at `:8000` via the Vite
`/api` proxy.

### Sandbox scripts + tests
- `scripts/sandbox_mercury.py` / `sandbox_quickbooks.py` — headless e2e
  (seed → backfill → assert observations → webhook → assert), template
  [scripts/sandbox_jira.py].
- Unit tests: handler / fetcher / reconciler / client per source (template
  `services/ingest/ingestion/handlers/tests/test_jira.py`).
- Integration tests: raw → normalized → observation per source (use the
  `fresh_db` fixture — the `company_os` role is superuser and bypasses RLS).

---

## 7. Known landmines (carried from prior source work)

- **Source-CHECK carry-forward** — newest migration must list every prior source.
- **Mutable dedup** — version `external_id` (status / SyncToken) or changes
  silently dedup away.
- **Partitions** — `services.domain.observations.partitions.ensure_partitions` before
  historical backfill (dev DB ships ~4 forward months → old rows fail
  `partition_missing`).
- **`KAFKA_PATH_ENABLED` per-tenant flag** — data-plane writes are invisible in
  shadow mode until the flag is ON; the install endpoint flips it.
- **DB on host port 5434** (not 5433):
  `DATABASE_URL=postgresql://company_os:company_os@localhost:5434/company_os`.
- **`company_os` role is superuser → bypasses RLS even FORCEd** → integration
  tests must use `fresh_db` / per-run-unique keys.
- **`_clients.py` needs both `build_*` and `open_*`** (opener gap passes unit
  tests, fails the real worker).
- **QuickBooks refresh-token rotation** — persist the new refresh token every
  cycle (old invalid within 24h).
