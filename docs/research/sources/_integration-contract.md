# Source Integration Contract — Fyralis Ingestion Pipeline

> **Status:** code-grounded reference, extracted from the pipeline on branch
> `feat/telegram-mtproto-ingestion` (2026-06-07). This is the canonical map for
> wiring **any** new SaaS source onto the pipeline. The per-source research docs
> in this folder map each candidate onto the archetypes and template defined here.

Twelve sources are fully wired today (on this branch): `gmail`, `github`,
`slack`, `discord`, `notion`, `google_calendar`, `google_drive`, `jira`,
`mercury`, `quickbooks`, `grafana`, `telegram`. On `main` it is eleven (telegram
is the in-flight 12th). The source enum (`SourceLiteral`) lives in
[envelope.py:24](../../../services/ingest/ingestion/raw_tier/envelope.py#L24).

Per-source migration numbers: notion=`0070`, google_calendar=`0071`,
google_drive=`0072`, jira=`0073`, mercury=`0074`, quickbooks=`0075`,
grafana=`0081`, telegram=`0094`.

---

## 1. Source taxonomy / auth shapes

Every source has a **dual edge**: a *pull* edge (backfill + incremental poll, via
a fetcher) and a *push* edge (live, via a webhook/gateway/push handler). The auth
archetype is the primary axis of variation. Credentials are always resolved at
fetch time from the **install row** for that source via
[_clients.py](../../../services/ingest/ingestion/fetchers/_clients.py) (one
`build_<source>_client(install, *, pool)` + `open_<source>_client(install) -> (client, close)`
pair per source); webhook signing secrets are resolved from a
`*_secret_ref`/`webhook_secret_ref` via
[secrets.py](../../../services/app/webhooks/secrets.py), decrypting through the
envelope-encrypted `lib.shared.secrets` store.

| Archetype | Exemplar | Pull auth | Live auth | Install table |
|---|---|---|---|---|
| **API-token Bearer/Basic, per-tenant** | `mercury` | API token (Bearer; also accepted as Basic), one `secret_ref` | HMAC-SHA256 webhook, `webhook_secret_ref` | `mercury_installations` (+ child `mercury_accounts`) |
| **API-token Basic, per-site** | `jira` | API token Basic-auth, username = `account_email`, per-install `base_url` | HMAC-SHA256 webhook | `jira_installations` (+ `jira_projects`) |
| **OAuth2 + realm + query language** | `quickbooks` | OAuth2 access token + `refresh_secret_ref`, realm-scoped `base_url`; `SELECT ... STARTPOSITION n MAXRESULTS m` | HMAC webhook (`eventNotifications[].realmId`) | `quickbooks_installations` |
| **Service-account Bearer + opaque-URL webhook** | `grafana` | service-account token (Bearer), per-instance `base_url`, `org_id` | HMAC-SHA256 bare hex in `X-Grafana-Alerting-Signature` (Grafana 12.0+) | `grafana_installations` (no child — org-wide → one shard) |
| **OAuth bot-token, workspace** | `notion` | long-lived bot token, `installation_id`=workspace_id | webhook (unsigned one-time verify; later HMAC); thin events → fetch + shadow-write | `provider_installations` (provider='notion') |
| **OAuth App / bot, multi-resource** | `github`, `slack` (xoxb), `discord` | GitHub App installation JWT→token; Slack `xoxb`; Discord bot token | HMAC (github/slack), Ed25519 (discord), gateway (discord) | `provider_installations` keyed by `installation_id`/`team_id`/`guild_id` |
| **Per-user OAuth (xoxp)** | `slack` DM (partial) | per-consenting-user `xoxp` token | n/a | `slack_dm_installations` (grain = user) |
| **DWD + Google push** | `gmail`, `google_calendar`, `google_drive` | service account impersonating `owner_email` (DWD) | gmail = Pub/Sub OIDC JWT; calendar/drive = Google `web_hook` push w/ `X-Goog-Channel-Token` | `gmail_installations`, `google_calendar_installations`, `google_drive_installations` |
| **MTProto persistent session** | `telegram` | persisted Telethon `StringSession` (`backfill_session_secret_ref`); `api_id` + `api_hash_secret_ref` | gateway-style persistent MTProto connection (no HTTP) | `telegram_installations` |

**Provider Lab seam.** When `PROVIDER_LAB_URL` is set outside production,
shared client builders use deterministic lab credentials (`spam-mercury`,
`spam-jira`, `spam-gh::<inst>`…). Routing remains explicit through each
source's `*_API_BASE_URL`; multi-source subprocesses derive those variables
with `lib.integrations.provider_lab.provider_lab_endpoint_overrides`.

---

## 2. The files you must add for a new source

Adding a source touches **two services** (`services/ingest` for ingestion
machinery, `services/app` for the webhook gateway) plus `db/migrations`.

### 2.1 Fetcher — `services/ingest/ingestion/fetchers/<source>.py`
Pull-edge contract (codified in `fetchers/__init__.py`):
```
FETCHER_DISPATCH[source](install, shard_identifier: dict, cursor: dict | None) -> FetchResult
```
`FetchResult` = `(records: list[dict], next_cursor: dict|None, end_of_data: bool)`.
The cursor is **opaque to ShardFetch** — the fetcher owns its schema (a Pydantic
`BaseModel` with `extra="forbid"`, e.g. `MercuryCursor`, `JiraCursor`). The module:
- defines `SHARD_KIND_* = "<source>_<resource>"`, `_decode_cursor`/`_encode_cursor`,
  and a `_open_<source>_client(install)` test seam,
- on the **first call** seeds the cursor (warm-start → incremental floor) from a
  shard hint like `shard_identifier["updated_cursor"]`,
- tags each emitted record with `_fyralis_record_type` (and `_fyralis_<namespace>`
  for external_id namespacing, e.g. `_fyralis_site`/`_fyralis_instance`/`_fyralis_account_id`),
- on rate-limit, returns `FetchResult(records=[...], next_cursor=<unadvanced>, end_of_data=False)`,
- registers itself: `FETCHER_DISPATCH["<source>"] = fetch_page_<source>`, imported at bottom of `fetchers/__init__.py`.

### 2.2 Planner — `services/ingest/ingestion/planners/<source>.py`
`PLANNER_DISPATCH[source](ctx: PlannerContext) -> list[Shard]`.
`Shard(shard_kind, shard_identifier: dict, recency_score=1.0, window_start, window_end)`.
Multi-resource sources read a pre-aggregated child list off `ctx.install` (mercury's
planner reads `ctx.install["accounts"]`); org-wide sources (grafana) emit exactly one shard.

### 2.3 Handler — `services/ingest/ingestion/handlers/<source>.py`
Pure normalization (no DB/network): `async def handle(payload, headers) -> ObservationDraft`,
registered with `@register("<source>:<channel>")`. Branches on input shape to produce
**exactly one `ObservationDraft` per call**, handling both the backfill/poll path
(records tagged `_fyralis_record_type`) and the live-webhook path (raw provider body).
A source may register multiple channels (grafana = `grafana:annotation` + `grafana:alert`).

Verbatim observation construction, from `handlers/mercury.py:216`:
```python
return ObservationDraft(
    source_channel=_CHANNEL,                  # "mercury:transaction"
    content_text=_truncate(content_text),     # human-legible one-liner
    content=content,                          # JSONB; object_type + fields
    occurred_at=occurred,                     # event time from source
    trust_tier=_TRUST,                        # "authoritative"
    kind="state_change" if is_state_change else "signal",
    source_actor_ref=None,                    # channel-native actor id or None
    external_id=external_id,                  # idempotency.mercury_transaction(account_id, txn_id, status)
    entities_hint=entities,                   # [{"type":..,"id":..,"role":..}]
    raw_payload=txn,                          # stashed → content["_raw"]
)
```
**`kind` rule:** a status/resolution/alert-state transition (change of world-state)
→ `state_change`; everything else (creation, snapshot, comment) → `signal`.

### 2.4 external_id constructor — `services/ingest/ingestion/idempotency/__init__.py`
Single source of truth so webhook and backfill paths can't drift (guarded by
`test_backfill_external_id_parity.py`). Two families:
- **Immutable**: `notion:{type}:{id}`, `gmail:{install}:{message_id}` — re-fetches collapse.
- **Versioned** (the mutable-source lesson): encode the mutation dimension so a real
  change lands a NEW observation: `mercury:{account}:txn:{id}:{status}`,
  `jira:{site}:issue:{id}:{updated}`, `qbo:{realm}:{kind}:{id}:{sync_token}`,
  `grafana:{instance}:annotation:{id}:{time}`, `telegram:{install}:{dialog}:{message_id}:{edit_date}`.

### 2.5 Webhook signature verifier — `services/app/webhooks/signatures/<source>.py`
Implements the `Verifier` protocol: `async def verify(*, body, headers, secrets, now) -> VerifiedContext`.
Mercury/jira = `Mercury-Signature: sha256=<hex>` over the raw body; grafana = bare
lowercase hex in `X-Grafana-Alerting-Signature`; linear = bare hex + optional
`webhookTimestamp` replay window. Register in `signatures/__init__.py::VERIFIERS`.

### 2.6 Tenant resolver extractor — `services/app/webhooks/tenant_resolver.py`
Add the provider to the `ResolverProvider` Literal, add `_extract_<source>(payload, headers) -> str | None`
that pulls the install identifier from the webhook (mercury=`organizationId`,
quickbooks=`eventNotifications[0].realmId`, jira=host from `issue.self`,
grafana=host from `externalURL`, slack=`team_id`, github=`installation.id`,
stripe=`Stripe-Account` header), and register it in `PROVIDER_EXTRACTORS`. The
resolver maps `(provider, installation_id)` → tenant via
`provider_installations WHERE provider=$1 AND installation_id=$2 AND enabled=TRUE`.

### 2.7 Router wiring — `services/app/webhooks/router.py`
Add the provider to up to three dicts: `_PROVIDER_TO_SHADOW_SOURCE` (data-plane
sources), `_CUTOVER_ENABLED_PROVIDERS` (202 Kafka cutover), `_PROVIDER_CHANNEL`
(inline-ingest fallback channel). Inline-only sources (linear/stripe) appear only
in `_PROVIDER_CHANNEL`.

### 2.8 ShardFetch install loader — `services/ingest/ingestion/workflows/shard_fetch.py`
Add `_LOAD_<SOURCE>_INSTALL_SQL` and a branch in `_load_install`. **Mandatory and
easy to miss:** sources not in `provider_installations` (every per-tenant install
table source) otherwise fall through, find nothing, and the shard **parks forever**.

### 2.9 Client builder — add `build_<source>_client` + `open_<source>_client` to `_clients.py`.

### 2.10 Migration — `db/migrations/NNNN_<source>.sql` (see §3/§6).

---

## 3. Backfill mechanics

Driven by `services/ingest/ingestion/workflows/shard_fetch.py` (the `ShardFetch`
`LongRunningService`).

1. **SourceOnboarding** creates a `source_onboarding_runs` row (`pending`→`in_progress`),
   runs the planner to produce `Shard`s, INSERTs them into `onboarding_shards`
   (`shard_kind`, `shard_identifier` JSONB, `state='pending'`, `recency_score`), and
   emits a `shard_fetch_requested` signal per shard.
2. **ShardFetch.tick()** drains signals (claim via UPDATE `pending→in_progress` with
   SKIP LOCKED) and scans for **orphans** (in-progress shards whose
   `workflow_states.last_advanced_at` is older than `lease_timeout_seconds`, default 30s).
3. **Fetch loop** (`_run_fetch_loop`, runs *outside* the claim transaction): load
   cursor from `workflow_states.state_data["cursor"]` (**not** `onboarding_shards.cursor_token`,
   which stays NULL), load install via `_load_install`, optional Redis `FetchRateLimiter`,
   call the fetcher, then per record write a content-addressed S3 blob (`put_if_absent`)
   and build a `RawEnvelope(ingress_kind="backfill")` pointer KafkaMessage on the
   **per-source** topic `topic_for("raw", source)`.
4. **N1 invariant** (`advance_cursor_atomic_with_kafka_publish`): publish all records →
   flush → **only if flush==0** advance the cursor. "S3-write → publish → flush →
   advance, never advance-then-publish." A failure leaves the shard `in_progress`
   (orphan-scan resumes); idempotency holds because S3 keys are content-addressed and
   the observation UNIQUE index + idempotent producer dedup re-publishes.
5. On `end_of_data=True`: mark shard `done`, emit `shard_fetch_completed`, publish a
   `ShardFetched` progress event with cumulative `observation_count`.

**Pagination/cursor dimensions in use:** Mercury offset + `incremental_floor` (start
date); QuickBooks `STARTPOSITION` + `Metadata.LastUpdatedTime >`; Jira `next_page_token`
+ JQL `updated >= "<floor>" ORDER BY updated ASC`; Grafana backward time-walk
(`page_to_ms = min_time - 1`, floor = `now - GRAFANA_BACKFILL_WINDOW_DAYS`). Each
cursor carries a `high_water_*` that becomes the reconciler's warm-start reference.

**Partition gotcha:** `occurred_at` values **must land inside the live `observations`
partition window** (current month + next 3 months); out-of-range timestamps silently
fail the insert. `validation_runs/preflight.py` is the fail-fast gate.

---

## 4. Live ingestion mechanics

Four live paths; a new source picks one:

- **(a) HMAC webhook → Kafka cutover → 202.** Default for token/HMAC sources.
  `router.py::receive`: raw body → 1MB precheck → resolve `VERIFIERS[provider]` →
  tenant-resolve → `load_secrets` → **verify signature first** (401 before tenant
  enforcement, so attackers can't probe tenant existence) → enforce tenant → if
  `_CUTOVER_ENABLED_PROVIDERS[provider]` and tenant `ingestion.kafka_path_enabled` →
  `_attempt_kafka_path` (S3 PutIfAbsent → publish `RawEnvelope(ingress_kind="webhook")`
  → flush → 202). Else inline `ingest()` (200/201) + best-effort shadow-write.
  HMAC-cutover sources: `_HMAC_SOURCES = ("jira", "mercury", "quickbooks", "grafana")`
  (+ slack/github).
- **(b) Google push → inline incremental drain → 200.** Calendar/drive push verified
  by the channel token minted at watch-time, then drains the delta via the **same
  path the poller uses** (push and poll observations dedup at the UNIQUE index). Gmail
  uses Pub/Sub OIDC, publishing with `ingress_kind="poll"`.
- **(c) Notion shadow-write → 200.** Thin events (`entity.id` + dotted type); the
  router fetches the full object via the workspace bot token and shadow-writes onto
  `ingestion.raw.notion`. The one-time subscription verification POST is unsigned.
- **(d) Gateway / direct dispatch (no HTTP).** Discord gateway + Telegram MTProto
  dispatch directly; no HTTP status (`_EXPECTED_LIVE_STATUS[discord]=set()`, `[telegram]=set()`).

**The `kafka_path_enabled` gate** (`feature_flags/client.py`): kafka-first default —
a resolved tenant is on the full pipeline unless an operator/circuit-breaker set the
flag FALSE. The default lives in one place (`TenantFlags.kafka_path_enabled()`),
shared by ingress and the observation writer.

**Backfill + live overlap evidence:** source certification artifacts record the
per-source overlap and dedup invariants. Provider Lab coverage is checked against
the canonical source catalog, avoiding copied source/count tables. A backfilled
record and its live twin still collapse via identical `external_id` (the
`_fyralis_<namespace>` on backfill must equal what the webhook handler derives —
jira `_fyralis_site` == host of `issue.self`; grafana `_fyralis_instance` == host
of `externalURL`).

---

## 5. The observation / edge data model

`observations` (partitioned by `occurred_at`). Canonical columns the draft maps onto
(via `ObservationCreate` in `core.py`):

- `id` (uuid7), `tenant_id`, `occurred_at`, `ingested_at`
- `kind` — `signal` | `state_change`
- `source_channel` — registered handler channel (`mercury:transaction`)
- `source_actor_ref` — channel-native actor id (`"jira:account:..."`) or None
- `actor_id` UUID FK → `actors(id)` — resolved by `ActorRepo.resolve_by_source_actor_ref`;
  unresolved refs stashed in `content["_unresolved_actor_ref"]`
- `content` JSONB — carries `object_type`; the GitHub-intel layer augments
  `content["intelligence"]` **in place** for `github:webhook` with a **raw-on-failure**
  guarantee (enrichment errors are swallowed and the unenriched draft persisted);
  `raw_payload` is stashed under `content["_raw"]`
- `content_text` — human-legible one-liner (embedded)
- `embedding` VECTOR(768), `embedding_pending` BOOL
- `trust_tier` — from `CHANNEL_TRUST_MAP`; jira/mercury/grafana = `authoritative`;
  slack/discord/gmail = `attested_agent`
- `external_id` — the dedup key (§2.4)
- `entities_mentioned` JSONB (GIN) — seeded from the handler's `entities_hint`, then
  augmented by `EntityAliasRepo.fast_path_resolve`; unresolved phrases →
  `content["_unresolved_phrases"]`

**Edges/actors:** no separate "edges" table at this layer — relationships are
`actor_id` FK + typed `entities_mentioned` hints (`{type,id,role}`). Actor identity
resolves through `actor_identity_mappings (source_channel, source_actor_ref)`.

**Dedup invariant + tenant gotcha:** `UNIQUE (source_channel, external_id,
occurred_at)` has **no `tenant_id`** — it is **global across tenants**. Every
`external_id` must therefore be globally unique, which is why install-scoped sources
namespace the key by a per-tenant install identifier (`gmail:{install}:...`, jira
`{site}`, grafana `{instance}`, mercury `{account}`, qbo `{realm}`). A new source
whose external_id is *not* install-namespaced will cross-tenant-collide and silently
drop one tenant's data.

---

## 6. Validation / acceptance

The release gate is `services/ingest/source_certification/`, backed by Provider
Lab and the canonical source contract. To add a source:

- add it once to the canonical source contract;
- implement a Provider Lab adapter (registry/catalog parity is fail-fast);
- attach versioned, pinned per-source certification evidence;
- record live-ingress, overlap, dedup, tampered-signature, and count evidence in
  that source's artifact;
- keep preflight coverage on the real fetcher→handler path, including non-null
  `external_id` and an `occurred_at` inside partition coverage.

**Migration shape** (every `NNNN_<source>.sql`): (1) `CREATE TABLE IF NOT EXISTS
<source>_installations` (`tenant_id` FK, `base_url`, `secret_ref`, optional
`webhook_secret_ref`, source-specific cols, `UNIQUE(tenant_id, base_url)`) + optional
child resource table; (2) `ENABLE`/`FORCE ROW LEVEL SECURITY` with a `tenant_isolation`
policy on `current_setting('app.current_tenant')::uuid`; (3) the **source-registry
CHECK widening** — drop+re-add the inline `source_check` on all **four** substrate
tables: `source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`,
`onboarding_triggers`. **Landmine:** the newest migration must list *every* prior
source (strict superset); integration tests re-running an older widening migration
must clean up the newest source first.

---

## 7. Per-source mapping template

```
SOURCE: <name>

Auth shape →            [API-token Bearer | OAuth2(+realm/query) | service-account Bearer |
                         OAuth bot-token | per-user OAuth | DWD+push | persistent-session | other]
                        token storage: secret_ref on <source>_installations | provider_installations
Install table →         <source>_installations (cols: base_url, secret_ref, webhook_secret_ref?, ___)
                        child resource table?: <source>_<resource> (shard targets) | none (org-wide)
Backfill cursor →       dimension: [offset | startposition | page_token | time-window-walk | snapshot-id]
                        high_water field: ___   incremental floor: ___   rate-limit-safe empty page: y/n
                        shard_kind: "<source>_<resource>"   one-shard | per-resource fan-out
Live mechanism →        [HMAC webhook→202 | Google-push→200 | shadow-write→200 |
                         gateway/direct-dispatch | inline-only | NONE/poll-only]
                        signature: header ___ format [sha256=hex | bare hex | Ed25519 | none]
                        tenant identifier in payload: ___ (extractor _extract_<source>)
New files →             fetchers/<s>.py · planners/<s>.py · handlers/<s>.py · signatures/<s>.py ·
                        _clients.py build/open · idempotency constructor · _load_install branch ·
                        router maps · tenant_resolver
Migration →             NNNN_<s>.sql: <s>_installations(+RLS)(+child) + source_check widening (4 tables)
Observation kind(s) →   signal: ___   state_change: ___   channel(s): "<s>:<channel>"
                        trust_tier: ___   external_id: <immutable | versioned-by-___>, namespaced by ___
Rate-limit risk →       API limits: ___   fan-out (records/resource): ___
Legal/ToS risk →        [API ToS permits server-side polling? token-rotation/consent model?
                         export-vs-API restriction? PII/secret redaction? per-user consent?]
Effort →                S | M | L  (+ why)
```

**Key files:**
`services/ingest/ingestion/fetchers/{__init__,_clients,mercury,jira,quickbooks,grafana}.py` ·
`services/ingest/ingestion/handlers/{__init__,mercury,jira,grafana}.py` ·
`services/ingest/ingestion/planners/{__init__,mercury}.py` ·
`services/ingest/ingestion/idempotency/__init__.py` ·
`services/ingest/ingestion/workflows/shard_fetch.py` · `services/ingest/ingestion/core.py` ·
`services/app/webhooks/{router,tenant_resolver,secrets,verifier,google_push,gmail_pubsub}.py` ·
`services/app/webhooks/signatures/{__init__,mercury,grafana,linear}.py` ·
`services/ingest/synthetic/validation_runs/{run_all_sources,preflight,composition}.py` ·
`db/migrations/{0001_foundation,0074_mercury,0081_grafana}.sql`
