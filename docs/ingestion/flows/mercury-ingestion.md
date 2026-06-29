# Mercury Ingestion — How Fyralis Pulls Mercury Data

This document explains, in detail, **how Mercury (banking / cash) data enters
Fyralis**: which Mercury REST APIs are called, with which token, and how the two
Mercury signal types — **transactions** (money movement) and **account balance
snapshots** (cash position) — are each ingested.

It deliberately stops at the point where a Mercury record becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

Mercury follows the **finance dedicated‑table archetype** (the same shape as
Jira / QuickBooks): a long‑lived Bearer API token, a per‑tenant
`mercury_installations` row with a child `mercury_accounts` table, an admin
"connect wizard" install surface, and HMAC‑signed live webhooks. Where it
differs from Jira is the fan‑out unit — Mercury fans out **per account** into a
transaction stream plus a balance snapshot, not per issue.

---

## 1. The three ways data arrives

Mercury data reaches Fyralis through **three paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical, PULL)** | Onboarding (`onboarding_triggers` → M6 chain) | Fyralis *pulls* full history via the Mercury **REST API** (`GET /accounts`, `GET /account/{id}/transactions`) | [planners/mercury.py](../../../services/ingest/ingestion/planners/mercury.py), [fetchers/mercury.py](../../../services/ingest/ingestion/fetchers/mercury.py) |
| **Poll (incremental, PULL)** | Reconciler reshare on gap | Same fetcher re‑run under `ingress_kind="poll"`, warm‑started at the transaction `createdAt` high‑water (`start=<date>`) | [fetchers/mercury.py:117‑195](../../../services/ingest/ingestion/fetchers/mercury.py#L117-L195), [reconcilers/mercury.py](../../../services/ingest/ingestion/reconcilers/mercury.py) |
| **Live (real‑time, PUSH)** | New activity in the bank | Mercury *pushes* an **HMAC‑signed webhook** (`transaction.created`, `account.updated`) to Fyralis's webhook edge | [webhooks/router.py](../../../services/app/webhooks/router.py), [signatures/mercury.py](../../../services/app/webhooks/signatures/mercury.py), [handlers/mercury.py](../../../services/ingest/ingestion/handlers/mercury.py) |

> The webhook PUSH path **does exist** and is wired end‑to‑end (verifier,
> tenant resolver, channel map, handler webhook branch — all present). What is
> *conditional* is whether the install ever registers it: the live edge row in
> `provider_installations` is seeded only when the connect wizard is given **both**
> an `organization_id` and a `webhook_secret`
> ([oauth.py:204‑212](../../../services/ingest/integrations/mercury/oauth.py#L204-L212)).
> Backfill (which needs only the API token) and the live edge are **seeded
> together but stay independent** — backfill can run with no webhook registered,
> and the webhook can be registered without it changing the backfill plan.

The channel‑mapping block confirms all three ingress kinds collapse onto one
channel ([channel_mapping.py:120‑122](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L120-L122)):

```python
("mercury", "backfill"): "mercury:transaction",
("mercury", "poll"):     "mercury:transaction",
("mercury", "webhook"):  "mercury:transaction",
```

### 1.1 The single dedup namespace

All three paths land on the **single** `mercury:transaction` handler
([handlers/mercury.py:299‑342](../../../services/ingest/ingestion/handlers/mercury.py#L299-L342)),
which branches on the record shape and derives the **same** `external_id` for the
same underlying object. The observations repo dedups on
`(source_channel, external_id)` **ignoring `occurred_at`**, so a record that is
both backfilled *and* delivered live collapses into **one** observation. The two
external_id families ([idempotency/__init__.py:144‑153](../../../services/ingest/ingestion/idempotency/__init__.py#L144-L153)):

```
transaction       external_id = "mercury:{account_id}:txn:{txn_id}:{status}"
account_snapshot  external_id = "mercury:{account_id}:balance:{YYYY-MM-DD}"
```

Two design notes are load‑bearing here:

- **The transaction id is VERSIONED by `status`.** A Mercury transaction is a
  *mutable* entity — its status walks `pending → sent → posted` or
  `pending → failed`. Because dedup ignores `occurred_at`, an unversioned id
  would silently drop every status transition after the first. Encoding `status`
  into the id means a `pending` row and its later `failed` row are **distinct**
  observations, so the state change is recorded rather than swallowed
  ([handlers/mercury.py:23‑28](../../../services/ingest/ingestion/handlers/mercury.py#L23-L28),
  [idempotency/__init__.py:144‑147](../../../services/ingest/ingestion/idempotency/__init__.py#L144-L147)).
- **The balance snapshot is keyed by day, not timestamp.** One cash‑position
  observation per account per calendar day — a re‑run on the same day dedups,
  a new day produces a fresh snapshot
  ([handlers/mercury.py:238‑239](../../../services/ingest/ingestion/handlers/mercury.py#L238-L239)).

---

## 2. Authentication & token model — long‑lived Bearer API token (no OAuth)

Mercury authenticates with a **single long‑lived API token** presented as a
**Bearer** token — the **Mercury / Jira archetype**, *not* an OAuth bot‑token
flow. There is no `code` exchange, no refresh token, and no per‑user token.

- The token is resolved **once** from the secret store (via the install's
  `secret_ref`) on first request and reused for the life of the client; in
  spammer mode it is preset
  ([client.py:96‑122](../../../services/ingest/integrations/mercury/client.py#L96-L122)).
- The outbound header is `Authorization: Bearer {token}`
  ([client.py:120‑122](../../../services/ingest/integrations/mercury/client.py#L120-L122)).
  (The module docstring notes Mercury also accepts the token as the Basic‑auth
  username with an empty password; the client uses the Bearer form
  ([client.py:6‑8](../../../services/ingest/integrations/mercury/client.py#L6-L8)).)
- **The API token and the auth header are never logged**
  ([client.py:18](../../../services/ingest/integrations/mercury/client.py#L18)).

### 2.1 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| API token (outbound reads) | secret store, label `mercury_api_token:{base_url}`; opaque `secret_ref` on `mercury_installations` | resolved lazily on first request ([oauth.py:181‑183](../../../services/ingest/integrations/mercury/oauth.py#L181-L183)) |
| Webhook signing secret (inbound HMAC) | secret store, label `mercury_webhook_secret:{base_url}`; opaque `secret_ref` on `provider_installations` (provider='mercury') | **per‑tenant**, resolved by the generic webhook secrets loader ([oauth.py:185‑189](../../../services/ingest/integrations/mercury/oauth.py#L185-L189), [onboarding.py:127‑149](../../../services/ingest/integrations/mercury/onboarding.py#L127-L149)) |
| Organization id (live edge key) | `provider_installations.installation_id` | the webhook tenant‑resolution key; matches `_extract_mercury` ([tenant_resolver.py:317‑328](../../../services/app/webhooks/tenant_resolver.py#L317-L328)) |

> **Contrast with GitHub / Notion.** GitHub and Notion use a single
> **App‑level** webhook secret read from an env var
> ([secrets.py:249‑263](../../../services/app/webhooks/secrets.py#L249-L263)).
> Mercury's webhook secret is **per‑tenant** — it flows through the generic
> `provider_installations.secret_ref` → secret‑store DB path (resolution step 2),
> exactly like Slack / Jira.

### 2.2 The admin "connect wizard" install flow

`services/ingest/integrations/mercury/oauth.py` implements a Bearer‑authed admin
connect wizard (mirroring Jira's), not an OAuth redirect handshake:

1. **`POST /integrations/mercury/connect/preflight`** (Bearer‑authed) — takes
   `{api_token, base_url?}`, calls `MercuryClient.list_accounts()` to **verify the
   token** and enumerate accounts for the selector UI; an auth failure returns a
   structured **400 with no secret stored**
   ([oauth.py:124‑144](../../../services/ingest/integrations/mercury/oauth.py#L124-L144)).
2. **`POST /integrations/mercury/connect/finalize`** — takes
   `{api_token, base_url?, account_ids?, organization_id?, webhook_secret?}`,
   **re‑verifies the creds before any write**, resolves the account set (all
   enumerated, or the `account_ids` subset), then
   ([oauth.py:147‑225](../../../services/ingest/integrations/mercury/oauth.py#L147-L225)):
   - persists the API token (and webhook secret, if given) into the secret store;
   - calls `finalize_install()` — UPSERT `mercury_installations` +
     `mercury_accounts` (one row per account) + an `onboarding_triggers` row
     (`source='mercury'`) so the **M6 backfill chain** fires
     ([onboarding.py:36‑124](../../../services/ingest/integrations/mercury/onboarding.py#L36-L124));
   - **iff** both `organization_id` and `webhook_secret` were supplied, calls
     `register_webhook_installation()` to seed the `provider_installations` row
     the live edge resolves tenant + HMAC secret from
     ([oauth.py:204‑212](../../../services/ingest/integrations/mercury/oauth.py#L204-L212),
     [onboarding.py:127‑149](../../../services/ingest/integrations/mercury/onboarding.py#L127-L149)).

   Credentials are verified **before** any secret is written, so an invalid token
   leaves no `encrypted_secrets` / install rows behind
   ([oauth.py:147‑158](../../../services/ingest/integrations/mercury/oauth.py#L147-L158)).

---

## 3. The Mercury REST API surface that is actually called

All read calls funnel through `MercuryClient._request`
([client.py:124‑184](../../../services/ingest/integrations/mercury/client.py#L124-L184)),
which:

- sets `Authorization: Bearer {token}` + `Accept: application/json`,
- honours Mercury's **`429` `Retry-After`** within a bounded budget
  (`MERCURY_RL_MAX_ATTEMPTS`=4, `MERCURY_RL_MAX_SLEEP_SEC`=30s), then surfaces
  `MercuryApiError(mercury_api_rate_limited)` once the budget is spent
  ([client.py:144‑167](../../../services/ingest/integrations/mercury/client.py#L144-L167)),
- maps any non‑2xx to a typed `MercuryApiError` (`401`/`403` →
  `mercury_api_unauthorized`, `404` → `mercury_api_not_found`, else
  `mercury_api_error`) ([client.py:180‑184](../../../services/ingest/integrations/mercury/client.py#L180-L184),
  [client.py:246‑277](../../../services/ingest/integrations/mercury/client.py#L246-L277)).

The endpoints invoked for ingestion:

| Mercury endpoint | Wrapper | Purpose | Code |
|------------------|---------|---------|------|
| `GET /accounts` | `list_accounts()` | enumerate accounts visible to the token (+ balances) — used at install/preflight and as the snapshot source | [client.py:190‑201](../../../services/ingest/integrations/mercury/client.py#L190-L201) |
| `GET /account/{id}` | `get_account()` | one account's current balance (the per‑shard snapshot probe) | [client.py:203‑205](../../../services/ingest/integrations/mercury/client.py#L203-L205) |
| `GET /account/{id}/transactions` | `list_transactions()` | one page of an account's transactions (`limit`+`offset`, optional `start=` window) | [client.py:207‑232](../../../services/ingest/integrations/mercury/client.py#L207-L232) |

### 3.1 Pagination — `limit` + `offset`

List endpoints return `{total, transactions:[…]}` (or `{total, accounts:[…]}`)
and accept `limit` + `offset`. `list_transactions` returns
`(transactions, next_offset, total)`; it computes `next_offset = offset + len(page)`
and signals terminal by returning **`next_offset is None`** when
`next_offset >= total` or the page is empty
([client.py:207‑232](../../../services/ingest/integrations/mercury/client.py#L207-L232)).
`list_accounts` returns the **full** list in one call (it also tolerates a bare
list response shape), so account enumeration is never silently truncated
([client.py:190‑201](../../../services/ingest/integrations/mercury/client.py#L190-L201)).

The page size defaults to **100** and is capped at Mercury's **500** ceiling,
overridable via `MERCURY_BACKFILL_PAGE_SIZE`
([client.py:37‑39](../../../services/ingest/integrations/mercury/client.py#L37-L39),
[fetchers/mercury.py:56‑60](../../../services/ingest/ingestion/fetchers/mercury.py#L56-L60)).

### 3.2 Rate limits — **no dedicated client‑side bucket**

Mercury has **no entry** in the client‑side token‑bucket registry — the only
buckets declared there are for Slack, GitHub, Gmail, and Discord
([rate_limit/buckets.py:82‑90](../../../services/ingest/ingestion/rate_limit/buckets.py#L82-L90)).
Mercury's rate‑limit safety is entirely the **server‑driven `429` `Retry-After`
loop** in `_request` (§3) — there is no pre‑emptive client‑side throttle. This
matches Jira (also bucket‑less).

---

## 4. Backfill scope — the shard families

The planner decomposes one install into **one shard per active account**, all of
`shard_kind = "mercury_account_txns"`
([planners/mercury.py:49‑81](../../../services/ingest/ingestion/planners/mercury.py#L49-L81)).
There is exactly **one shard family** — Mercury's only signal is account
activity.

`ctx.source_client` is **None**: the planner reads DB state only. The accounts
are pre‑aggregated into `ctx.install["accounts"]` by the SourceOnboarding loader's
JSON aggregation over `mercury_accounts` (so the planner stays stateless / no DB
I/O) ([planners/mercury.py:1‑15](../../../services/ingest/ingestion/planners/mercury.py#L1-L15),
[source_onboarding.py:341‑360](../../../services/ingest/ingestion/workflows/source_onboarding.py#L341-L360)):

```sql
SELECT mi.id, mi.tenant_id, mi.base_url, mi.secret_ref, mi.disabled_at,
       json_agg(json_build_object(
         'account_id',   ma.account_id,
         'account_name', ma.account_name,
         'account_kind', ma.account_kind,
         'txn_cursor',   ma.txn_cursor          -- the high-water warm-start
       ) ...) AS accounts
  FROM mercury_installations mi
  LEFT JOIN mercury_accounts ma
    ON ma.mercury_installation_id = mi.id AND ma.state = 'active'
 WHERE mi.tenant_id = $1 AND mi.disabled_at IS NULL
```

Each shard carries `account_id`, `account_name`, `installation_id`, and the
per‑account `txn_cursor` (the persisted transaction‑`createdAt` high‑water, used
to warm‑start incremental polls; `None` on first sync), at a baseline
`recency_score=1.0` ([planners/mercury.py:59‑75](../../../services/ingest/ingestion/planners/mercury.py#L59-L75)).

---

## 5. Fetch specifics — one shard kind, two sync modes

`fetch_page_mercury` ([fetchers/mercury.py:117‑195](../../../services/ingest/ingestion/fetchers/mercury.py#L117-L195))
takes one `(install, shard_identifier, cursor)` triple and returns one page of
records + the next cursor. ShardFetch calls it in a loop, persisting the cursor
between calls.

The same shard kind runs in two modes
([fetchers/mercury.py:8‑21](../../../services/ingest/ingestion/fetchers/mercury.py#L8-L21)):

- **FULL (initial backfill):** walk `GET /account/{id}/transactions` from
  `offset=0`, paginated.
- **INCREMENTAL (poll):** when the shard is warm‑started with a `txn_cursor`, the
  fetcher freezes it as the `incremental_floor` and passes `start=<date>` so only
  recent transactions return; the overlap re‑fetch dedups via the versioned
  `external_id` ([fetchers/mercury.py:133‑138](../../../services/ingest/ingestion/fetchers/mercury.py#L133-L138)).

### 5.1 The cursor

```python
class MercuryCursor(BaseModel):
    offset: int = 0                       # list-transactions pagination offset
    high_water_created: str | None = None # max txn `createdAt` seen — warm-start
                                          # lower bound AND reconciler reference
    incremental_floor: str | None = None  # frozen `start=` lower bound (None=FULL)
    txns_seen: int = 0                     # diagnostic
    seeded: bool = False                   # whether first-call setup ran
```

([fetchers/mercury.py:63‑84](../../../services/ingest/ingestion/fetchers/mercury.py#L63-L84)).
The high‑water advances over each transaction's `createdAt` (falling back to
`postedAt`) via `_bump_high_water`
([fetchers/mercury.py:110‑114](../../../services/ingest/ingestion/fetchers/mercury.py#L110-L114),
[fetchers/mercury.py:171‑177](../../../services/ingest/ingestion/fetchers/mercury.py#L171-L177)).
Mercury's `start=` is **date‑granular**, so `_iso_date` truncates the ISO floor
to `YYYY‑MM‑DD` ([fetchers/mercury.py:103‑107](../../../services/ingest/ingestion/fetchers/mercury.py#L103-L107)).

### 5.2 Fan‑out — one account → N records

The handler produces exactly **one observation per record**, so the fetcher
fans one account out into two record types
([fetchers/mercury.py:23‑33](../../../services/ingest/ingestion/fetchers/mercury.py#L23-L33)):

- **`account_snapshot`** — emitted **once per shard run**, on the first
  (un‑seeded) call. The fetcher calls `get_account(account_id)` and appends a
  record tagged `_fyralis_record_type="account_snapshot"` with the balance and an
  `as_of` timestamp. A `MercuryApiError` on that probe is swallowed (the snapshot
  is best‑effort; the transaction walk still proceeds)
  ([fetchers/mercury.py:132‑151](../../../services/ingest/ingestion/fetchers/mercury.py#L132-L151)).
- **`transaction`** — one record per transaction on the page, tagged
  `_fyralis_record_type="transaction"`
  ([fetchers/mercury.py:171‑177](../../../services/ingest/ingestion/fetchers/mercury.py#L171-L177)).

Both carry a private `_fyralis_account_id`; the handler branches on
`_fyralis_record_type` (§6).

### 5.3 Rate‑limit handling mid‑fetch

If `list_transactions` raises `mercury_api_rate_limited` mid‑walk, the fetcher
returns the records gathered so far with **`end_of_data=False`** — a recoverable,
non‑terminal page that ShardFetch retries next tick — rather than failing the run
([fetchers/mercury.py:160‑169](../../../services/ingest/ingestion/fetchers/mercury.py#L160-L169)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_mercury_transaction` ([handlers/mercury.py:299‑342](../../../services/ingest/ingestion/handlers/mercury.py#L299-L342))
is a **pure function** (no DB / network). It detects the path by shape:

- **LIVE WEBHOOK** — a raw Mercury webhook body carries a `type`
  (`transaction.*` → transaction builder; `account.*` → snapshot builder);
  unsupported types raise `ValidationError`
  ([handlers/mercury.py:306‑325](../../../services/ingest/ingestion/handlers/mercury.py#L306-L325)).
- **BACKFILL / POLL** — fetcher‑tagged records branch on `_fyralis_record_type`
  ∈ `{transaction, account_snapshot}` (with a tolerant fallback when a bare
  `transaction` key is present) ([handlers/mercury.py:327‑342](../../../services/ingest/ingestion/handlers/mercury.py#L327-L342)).

Both paths feed the **same** two draft builders, so a webhook‑delivered change
and its backfill twin dedup:

| Record type (`_fyralis_record_type` / webhook `type`) | Builder | `external_id` | `occurred_at` | `kind` | Trust tier |
|---|---|---|---|---|---|
| `transaction` (status pending/sent/posted) | `_transaction_draft` | `mercury:{account}:txn:{id}:{status}` | `postedAt` → `createdAt` → now | `signal` | **authoritative** |
| `transaction` (status failed/cancelled/declined) | `_transaction_draft` | ″ (versioned by status) | ″ | **`state_change`** (cash‑risk) | **authoritative** |
| `account_snapshot` / `account.*` | `_account_snapshot_draft` | `mercury:{account}:balance:{YYYY-MM-DD}` | `as_of` → now | `signal` | **authoritative** |

Highlights:

- **Trust tier is `authoritative`** for every Mercury observation — the bank is
  the system of record for cash ([handlers/mercury.py:46‑47](../../../services/ingest/ingestion/handlers/mercury.py#L46-L47),
  [handlers/mercury.py:29](../../../services/ingest/ingestion/handlers/mercury.py#L29)).
- **A failed/cancelled/declined transaction is the cash‑risk `state_change`** —
  and the failure *reason* (`reasonForFailure`) is surfaced inline in the content
  sentence so a bare failure becomes an actionable liquidity / counterparty‑risk
  signal ([handlers/mercury.py:51](../../../services/ingest/ingestion/handlers/mercury.py#L51),
  [handlers/mercury.py:181‑191](../../../services/ingest/ingestion/handlers/mercury.py#L181-L191)).
- **`source_actor_ref` is always `None`** — a bank transaction has no Fyralis
  actor ([handlers/mercury.py:223](../../../services/ingest/ingestion/handlers/mercury.py#L223),
  [handlers/mercury.py:276](../../../services/ingest/ingestion/handlers/mercury.py#L276)).
- **PII redaction.** `accountNumber` / `routingNumber` / `iban` inside a
  transaction's `details` sub‑tree are masked to the last 4 chars before they
  ever reach the reasoning layer / LLM context
  ([handlers/mercury.py:100‑118](../../../services/ingest/ingestion/handlers/mercury.py#L100-L118),
  [handlers/mercury.py:148‑152](../../../services/ingest/ingestion/handlers/mercury.py#L148-L152)).
- **`entities_hint`** carries a `mercury_account` ref and, for transactions, an
  `organization` counterparty ref
  ([handlers/mercury.py:193‑197](../../../services/ingest/ingestion/handlers/mercury.py#L193-L197),
  [handlers/mercury.py:251](../../../services/ingest/ingestion/handlers/mercury.py#L251)).

> **Trust‑map registration.** `mercury:transaction` is **not** a static entry in
> `CHANNEL_TRUST_MAP`; the handler module registers it at import via
> `CHANNEL_TRUST_MAP.setdefault("mercury:transaction", "authoritative")`
> ([handlers/mercury.py:345](../../../services/ingest/ingestion/handlers/mercury.py#L345)),
> and the handler package imports the module so the `@register` + setdefault fire
> ([handlers/__init__.py:175](../../../services/ingest/ingestion/handlers/__init__.py#L175)).

---

## 7. Live (real‑time) ingestion via HMAC‑signed webhooks

When activity occurs in the bank, Mercury **POSTs a signed webhook delivery** to
Fyralis's webhook edge (`/webhooks/mercury`). It is dispatched by the same
generic router as every other provider; the router maps provider `mercury` →
channel `mercury:transaction` ([router.py:448](../../../services/app/webhooks/router.py#L448)),
and `VERIFIERS["mercury"] = mercury.verifier`
([signatures/__init__.py:52](../../../services/app/webhooks/signatures/__init__.py#L52)).

### 7.1 Signature verification (HMAC‑SHA256, no timestamp)

Mercury signs the **raw request body** with HMAC‑SHA256 and presents the digest
in the **`Mercury-Signature`** header as `sha256=<hex>` (GitHub‑style). The
verifier requires the `sha256=` prefix, then tries each active per‑tenant secret
in turn with a **constant‑time** compare
([signatures/mercury.py:32‑74](../../../services/app/webhooks/signatures/mercury.py#L32-L74)).

> **No replay window.** Like GitHub and Jira, the digest is over the **body
> alone** — there is no timestamp envelope (contrast Slack's `v0:{ts}:{body}` +
> 300 s window), so the verifier returns `signed_timestamp=None`. Mercury's
> at‑least‑once retry semantics are made idempotent at the **ingestion layer**
> via the versioned `external_id`, not here
> ([signatures/mercury.py:9‑11](../../../services/app/webhooks/signatures/mercury.py#L9-L11),
> [signatures/mercury.py:69‑74](../../../services/app/webhooks/signatures/mercury.py#L69-L74)).
> There is **no provider‑specific replay cache** for Mercury either (contrast
> GitHub's `(installation, delivery)` cache).

### 7.2 Tenant resolution

The tenant is resolved from the webhook body's top‑level **`organizationId`**
(falling back to the legacy `accountId`) → the `provider_installations` row for
`(provider='mercury', installation_id=organizationId)`
([tenant_resolver.py:317‑328](../../../services/app/webhooks/tenant_resolver.py#L317-L328)).
The signing secret is loaded separately by the generic
`provider_installations.secret_ref` → secret‑store path
([secrets.py:255‑263](../../../services/app/webhooks/secrets.py#L255-L263)).
An unknown/disabled installation gets `401 unknown_installation`, deferred until
**after** signature verification so a tenant‑id prober sees signature failures
first ([router.py:917‑926](../../../services/app/webhooks/router.py#L917-L926)).

### 7.3 The `kafka_path_enabled` cutover

Mercury is a **cutover‑enabled** provider — present in both
`_PROVIDER_TO_SHADOW_SOURCE` and `_CUTOVER_ENABLED_PROVIDERS`
([router.py:130](../../../services/app/webhooks/router.py#L130),
[router.py:169](../../../services/app/webhooks/router.py#L169)). After signature
verification + tenant resolution, the router branches on the tenant's
`ingestion.kafka_path_enabled` flag ([router.py:1036‑1095](../../../services/app/webhooks/router.py#L1036-L1095)):

- **Flag ON (cutover):** the body is published to Kafka and the edge returns
  **`202 accepted`**; inline `ingest()` is **skipped** (the writer pool produces
  the observation downstream) ([router.py:1045‑1070](../../../services/app/webhooks/router.py#L1045-L1070)).
- **Flag OFF (or cutover failure → graceful fallback):** the router falls back to
  **inline** `ingest(channel, payload, …)` with
  `channel = _PROVIDER_CHANNEL["mercury"] = "mercury:transaction"`
  ([router.py:1095](../../../services/app/webhooks/router.py#L1095),
  [router.py:1110‑1120](../../../services/app/webhooks/router.py#L1110-L1120)),
  preserving the user‑visible 200/201 contract. The inline channel and the
  cutover source are the same destination by two routes.

---

## 8. Reconciliation — gap detection

Two reconcilers share the Mercury gap logic via `RECONCILER_DISPATCH["mercury"] =
reconcile_mercury`:

1. **At‑completion** — `reconcile_mercury` runs after a run's account shards
   settle ([reconcilers/mercury.py:131‑168](../../../services/ingest/ingestion/reconcilers/mercury.py#L131-L168)).
2. **Periodic** — the periodic reconciler re‑checks already‑reconciled runs on a
   rotation, reusing the same `RECONCILER_DISPATCH` entry
   ([periodic_reconciler.py:263](../../../services/ingest/ingestion/workflows/periodic_reconciler.py#L263)).

For each `done` account shard, `_check_one_shard_for_gap` loads the cursor's
`high_water_created` from the shard's persisted state, then probes the live
account cheaply with `list_transactions(account_id, limit=1, start=high_water[:10])`
([reconcilers/mercury.py:80‑113](../../../services/ingest/ingestion/reconcilers/mercury.py#L80-L113)).
If the newest returned `createdAt`/`postedAt` is **strictly greater** than the
high‑water, there is a gap and it reshares a `mercury_account_txns` shard at
**`recency_score=1.5`**, warm‑started with `txn_cursor = high_water` (so the
re‑walk runs in incremental mode and only re‑fetches the changed tail)
([reconcilers/mercury.py:115‑128](../../../services/ingest/ingestion/reconcilers/mercury.py#L115-L128)).

`external_id` parity (versioned by status) makes the re‑walk idempotent — only
genuinely new/changed transactions produce new observations. The reconciler
comment notes the probe is *pragmatic v1*: it can over‑reshare but never
under‑reshares, and a transient probe error is logged and treated as "no gap"
(best‑effort), never failing the run
([reconcilers/mercury.py:6‑13](../../../services/ingest/ingestion/reconcilers/mercury.py#L6-L13),
[reconcilers/mercury.py:98‑103](../../../services/ingest/ingestion/reconcilers/mercury.py#L98-L103)).

> The reconciler probe also doubles as the **~45‑day token keepalive** — Mercury
> deletes idle tokens, and the periodic probe keeps the token live
> ([reconcilers/mercury.py:12‑13](../../../services/ingest/ingestion/reconcilers/mercury.py#L12-L13)).

---

## 9. Revocation / recoverable‑error behaviour

> **No dedicated revocation chokepoint** (contrast GitHub's
> `_maybe_disable_on_revocation`, which disables the install on `401 Bad
> credentials`/`404`, and the Notion revocation chokepoint). The Mercury client
> maps `401`/`403` to `MercuryApiError(mercury_api_unauthorized)` and `404` to
> `mercury_api_not_found`
> ([client.py:246‑277](../../../services/ingest/integrations/mercury/client.py#L246-L277)),
> but **nothing in the fetcher, reconciler, or client toggles
> `mercury_installations.disabled_at` on those codes.** The only error the fetcher
> special‑cases is `mercury_api_rate_limited`, which it treats as recoverable
> (non‑terminal page, retry next tick — §5.3); a `401`/`403`/`404` raised by
> `list_transactions`/`get_account` propagates to ShardFetch as an ordinary fetch
> error. (The first‑page balance‑snapshot probe is the exception — *any*
> `MercuryApiError` there is swallowed and the snapshot skipped —
> [fetchers/mercury.py:140‑151](../../../services/ingest/ingestion/fetchers/mercury.py#L140-L151).)
>
> `disabled_at` is written only by **admin actions**: `finalize_install` clears it
> on re‑connect (`disabled_at = NULL`), and the reconciler simply *skips* installs
> where `disabled_at IS NOT NULL`
> ([onboarding.py:71‑77](../../../services/ingest/integrations/mercury/onboarding.py#L71-L77),
> [reconcilers/mercury.py:139‑149](../../../services/ingest/ingestion/reconcilers/mercury.py#L139-L149)).
> Recovery from a rejected token is therefore re‑running the connect wizard with a
> fresh API token (which re‑clears `disabled_at`), not an automatic chokepoint.

> **TODO(human):** The *why* of "no automatic disable‑on‑revocation for Mercury"
> is not stated in the code — it is consistent with the Jira archetype (admin
> re‑connect, not auto‑disable), but whether that is a deliberate finance‑source
> policy or simply not‑yet‑built is not recorded. Confirm and document the
> rationale (or file it as a known gap) rather than inferring intent.

---

## 10. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  connect wizard: api_token verified → finalize_install           │
                          │     └─► mercury_installations + mercury_accounts + onboarding     │
   PER ACTIVE ACCOUNT     │  planner: reads accounts from DB (ctx.source_client = None)       │
                          │     └─► one mercury_account_txns shard per account                │
                          │  fetcher (FULL): GET /account/{id}/transactions (limit+offset)    │
                          │     └─► first page also: GET /account/{id} → account_snapshot     │
                          │     └─► tag _fyralis_record_type (transaction | account_snapshot) │
                          └───────────────────────────────────────────────────────────────┬─┘
                          ┌──────────────────────── POLL (pull, incremental) ──────────────┐│
   RECONCILER reshare ───►  same fetcher, warm-started at high_water (start=<date>)         ││
   (gap on high-water)    │     └─► only the changed tail; dedups via versioned external_id ││
                          └───────────────────────────────────────────────────────────────┘│
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐│
   bank activity ─────────►  Mercury webhook ──HTTP POST──► /webhooks/mercury              ││
                          │     verify Mercury-Signature (HMAC-SHA256, no ts, per-tenant)  ││
                          │     resolve tenant via organizationId                          ││
                          │     kafka_path_enabled? 202 (Kafka) : inline mercury:transaction│
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_mercury_transaction      │
                                                            │  branch: webhook type /          │
                                                            │    _fyralis_record_type          │
                                                            │  txn → mercury:{acct}:txn:{id}:{status}
                                                            │  bal → mercury:{acct}:balance:{date}
                                                            │  → ObservationDraft (authoritative)│
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, poll, and webhook all land on
   `mercury:transaction`. The handler branches on `_fyralis_record_type` (or the
   webhook `type`) and derives the same `external_id`, so a backfilled record and
   its live twin dedup to one observation.
2. **The transaction id is versioned by `status`** (`…:txn:{id}:{status}`) — a
   `pending → failed` transition lands as a *new* observation (a cash‑risk
   `state_change`) instead of being silently swallowed by insert‑only dedup. The
   balance snapshot id is versioned by **day** (`…:balance:{YYYY‑MM‑DD}`).
3. **One credential model: a long‑lived Bearer API token.** No OAuth, no refresh,
   no per‑user token. The webhook signing secret is a separate **per‑tenant**
   secret on `provider_installations`.
4. **One shard family** (`mercury_account_txns`), fanning out **per account** into
   a transaction stream + one balance snapshot per run. The poll incremental floor
   is the transaction‑`createdAt` high‑water.
5. **No webhook replay window and no replay cache** (Mercury signs the body only);
   idempotency is the `external_id` dedup. **No automatic revocation chokepoint** —
   recovery is an admin re‑connect.

---

## 11. Configuration & compliance

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `MERCURY_RL_MAX_ATTEMPTS` | `4` | bounded `429` `Retry-After` retry budget in `_request` ([client.py:144](../../../services/ingest/integrations/mercury/client.py#L144)) |
| `MERCURY_RL_MAX_SLEEP_SEC` | `30` | max backoff (s) per `Retry-After` ([client.py:145](../../../services/ingest/integrations/mercury/client.py#L145)) |
| `MERCURY_BACKFILL_PAGE_SIZE` | `100` (capped at `500`) | transactions page size ([fetchers/mercury.py:56‑60](../../../services/ingest/ingestion/fetchers/mercury.py#L56-L60)) |
| `MERCURY_API_BASE_URL` | `https://api.mercury.com/api/v1` | canonical API host (endpoint resolver key `mercury_api`) ([endpoints.py:56](../../../lib/integrations/endpoints.py#L56), [endpoints.py:128](../../../lib/integrations/endpoints.py#L128)) |

The connect wizard's `base_url` defaults to `https://api.mercury.com/api/v1`
([oauth.py:56](../../../services/ingest/integrations/mercury/oauth.py#L56)). The
webhook signing secret is **per‑tenant** (secret store, not an env var).

### 11.2 Verified compliant

- **Auth** — long‑lived Bearer API token, resolved once, never logged. ✅
- **Webhook signing** — HMAC‑SHA256 `Mercury-Signature: sha256=<hex>`,
  constant‑time compare, per‑tenant secret. ✅
- **Pagination** — `limit` + `offset`, `total`‑driven terminal detection;
  accounts fully enumerated. ✅
- **Rate‑limit etiquette** — server‑driven `429` `Retry-After` honoured within a
  bounded budget (no dedicated client‑side bucket). ✅
- **PII least‑exposure** — `accountNumber` / `routingNumber` / `iban` masked to
  last‑4 before reaching observations / LLM context. ✅
- **Idempotency** — versioned `external_id` (status for transactions, day for
  balances); webhook + backfill twins collapse to one observation. ✅

### 11.3 Dev / spammer mode

For local testing against the mock source servers, `build_mercury_client` detects
spammer mode and **presets** the API token to `spam-mercury` (skipping any secret
store) and points `api_base_url` at the local spammer's `/mercury` sub‑path via
the endpoint resolver
([_clients.py:309‑334](../../../services/ingest/ingestion/fetchers/_clients.py#L309-L334),
[endpoints.py:158](../../../lib/integrations/endpoints.py#L158)). The mock server
serves `GET /accounts`, `GET /account/{id}`, and `GET /account/{id}/transactions`
with `limit`/`offset` paging and a `start=`‑driven incremental delta set
([mock_servers/mercury.py:60‑107](../../../services/ingest/synthetic/mock_servers/mercury.py#L60-L107)).
