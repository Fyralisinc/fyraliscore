# Brex Ingestion — How Fyralis Pulls Brex Data

This document explains, in detail, **how Brex data enters Fyralis**: which Brex
REST APIs are called, with which token, and how the Brex finance signal set —
**cash/card transactions and account balance snapshots** — is each ingested.

It deliberately stops at the point where a Brex transaction becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Caveat — verified-but-unconfirmed external surface.** Several Brex
> specifics (the real API host, the exact read paths, the transactions
> pagination model, the webhook signature scheme, and the webhook tenant-id
> field) are **CLONED from Mercury and not yet confirmed against Brex's docs**.
> The code carries these as explicit `TODO(human)` callouts; this doc surfaces
> every one of them as a `> **TODO(human):**` block rather than asserting the
> external contract is correct. What *is* fully verified is Fyralis's internal
> wiring — channel, dedup key, auth model, planner/fetcher/handler/reconciler
> shape — and that is described authoritatively below.

---

## 1. The three ways data arrives

Brex data reaches Fyralis through **three independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* history via the Brex **REST API** (`GET /account/{id}/transactions`) | [planners/brex.py](../../../services/ingest/ingestion/planners/brex.py), [fetchers/brex.py](../../../services/ingest/ingestion/fetchers/brex.py) |
| **Poll (incremental)** | The incremental driver re-runs the backfill fetcher under `ingress_kind="poll"` | Same fetcher, warm-started with the per-account **transaction high-water cursor** (`start=<date>`) | [fetchers/brex.py:138‑199](../../../services/ingest/ingestion/fetchers/brex.py#L138-L199) |
| **Live (real‑time)** | New transaction / account change in Brex | Brex *pushes* an **HMAC-signed webhook** delivery to Fyralis | [webhooks/router.py](../../../services/app/webhooks/router.py), [webhooks/signatures/brex.py](../../../services/app/webhooks/signatures/brex.py), [handlers/brex.py](../../../services/ingest/ingestion/handlers/brex.py) |

All three are wired in `channel_mapping.py` to the **single** `brex:transaction`
channel ([channel_mapping.py:171‑173](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L171-L173)):

```python
("brex", "backfill"): "brex:transaction",
("brex", "poll"):     "brex:transaction",
("brex", "webhook"):  "brex:transaction",
```

Crucially, **all three paths are parsed by the single `brex:transaction`
handler** ([handlers/brex.py:299‑342](../../../services/ingest/ingestion/handlers/brex.py#L299-L342)).
The handler branches on input shape — a live webhook body carries a `type`
field (`transaction.*` / `account.*`); a backfill/poll record carries a private
`_fyralis_record_type` tag set by the fetcher — and routes both onto the same
two draft builders. All paths derive the **same** dedup key
([idempotency/__init__.py:195‑205](../../../services/ingest/ingestion/idempotency/__init__.py#L195-L205)):

```
transaction       external_id = "brex:{account_id}:txn:{txn_id}:{status}"
account_snapshot  external_id = "brex:{account_id}:balance:{YYYY-MM-DD}"
```

So a transaction that is both backfilled *and* delivered live collapses into
**one** observation. This is the central design invariant of Brex ingestion.

> **Why the `:{status}` suffix?** The observations repo dedups on
> `(source_channel, external_id)` **ignoring `occurred_at`**. A transaction's
> status mutates over its lifetime (`pending → sent → posted`, or
> `pending → failed`). Versioning `external_id` by status means a status
> transition lands as a **new** observation rather than silently deduping
> against the earlier state ([handlers/brex.py:23‑28](../../../services/ingest/ingestion/handlers/brex.py#L23-L28),
> [idempotency/__init__.py:195‑198](../../../services/ingest/ingestion/idempotency/__init__.py#L195-L198)).
> Balance snapshots version by **as-of date** instead — one snapshot per account
> per day.

---

## 2. Authentication & token model

Brex ingestion uses a **single credential model: a long-lived API token
presented as a Bearer token** — the same posture as the Notion / Jira clients.

> **Resolving the stale comment.** The code comments are inconsistent: the
> `oauth.py` module is named "OAuth" and its docstring once mentioned an
> "OAuth/QuickBooks archetype", while `client.py` and `channel_mapping.py`
> describe a "Bearer/Mercury archetype". **The actual auth model is Bearer.**
> [client.py:104‑130](../../../services/ingest/integrations/brex/client.py#L104-L130)
> resolves a single token and presents it as `Authorization: Bearer {token}`;
> there is **no `code` exchange, no refresh, and no token-mint flow**. The
> `oauth.py` file is misnamed — it implements an admin *connect wizard* (a
> Bearer-token paste form), not an OAuth handshake
> ([oauth.py:1‑33](../../../services/ingest/integrations/brex/oauth.py#L1-L33)).

### 2.1 The token

- The API token is resolved **once** from the gateway secret store via
  `install['secret_ref']` and reused for the life of the client; there is no
  refresh (Bearer archetype) ([client.py:104‑130](../../../services/ingest/integrations/brex/client.py#L104-L130)).
- It is presented as `Authorization: Bearer {token}` on every read
  ([client.py:128‑130](../../../services/ingest/integrations/brex/client.py#L128-L130)).
- The token and the auth header are **never logged**
  ([client.py:26](../../../services/ingest/integrations/brex/client.py#L26)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| API token (read) | secret store, label `brex_api_token:{base_url}`; opaque ref in `brex_installations.secret_ref` | resolved lazily on first read ([oauth.py:185‑187](../../../services/ingest/integrations/brex/oauth.py#L185-L187)) |
| Webhook HMAC secret | secret store, label `brex_webhook_secret:{base_url}`; opaque ref in `provider_installations.secret_ref` (provider='brex') | only for the live edge ([oauth.py:189‑193](../../../services/ingest/integrations/brex/oauth.py#L189-L193), [onboarding.py:128‑150](../../../services/ingest/integrations/brex/onboarding.py#L128-L150)) |

Backfill reads `brex_installations`; the live webhook edge reads
`provider_installations`. **The two are seeded together but stay independent**
([oauth.py:31‑32](../../../services/ingest/integrations/brex/oauth.py#L31-L32)).

### 2.3 The connect-wizard install flow

`services/ingest/integrations/brex/oauth.py` implements a Bearer-authed admin
connect wizard (mirrors the Jira wizard) — **not** a redirect-based OAuth flow:

1. **`POST /integrations/brex/connect/preflight`** — takes `{api_token, base_url?}`,
   verifies the token by calling `BrexClient.list_accounts()`, and returns the
   normalized account set for the selector UI. On auth failure a structured
   `400` is returned and **no secret is stored**
   ([oauth.py:128‑148](../../../services/ingest/integrations/brex/oauth.py#L128-L148)).
2. **`POST /integrations/brex/connect/finalize`** — re-verifies the creds
   *before any write*, resolves the account set (all enumerated, or the
   `account_ids` subset), stores the token (+ optional webhook secret) in the
   secret store, then calls `finalize_install()`
   ([oauth.py:151‑229](../../../services/ingest/integrations/brex/oauth.py#L151-L229)).
3. `finalize_install()` UPSERTs a `brex_installations` row (keyed
   `(tenant_id, base_url)`), one `brex_accounts` row per account, and an
   `onboarding_triggers` row (`source='brex'`, `trigger_kind='install'`) so the
   M6 backfill chain fires — **all in one tenant-scoped transaction**
   ([onboarding.py:36‑125](../../../services/ingest/integrations/brex/onboarding.py#L36-L125)).
4. If **both** an `organization_id` and a `webhook_secret` were supplied,
   `register_webhook_installation()` seeds the `provider_installations` row
   (`provider='brex'`, `installation_id=organization_id`) the webhook edge
   resolves the tenant + HMAC secret from
   ([oauth.py:208‑216](../../../services/ingest/integrations/brex/oauth.py#L208-L216),
   [onboarding.py:128‑150](../../../services/ingest/integrations/brex/onboarding.py#L128-L150)).

> **TODO(human):** confirm the Brex API host. The default is
> `https://platform.brexapis.com`, carried as UNVERIFIED in both
> [oauth.py:58‑60](../../../services/ingest/integrations/brex/oauth.py#L58-L60)
> and [endpoints.py:70‑72](../../../lib/integrations/endpoints.py#L70-L72). *Why:
> the host isn't load-bearing for Fyralis's internal wiring but must be right
> before production traffic flows.*

---

## 3. The Brex REST API surface that is actually called

All read calls funnel through `BrexClient._request`
([client.py:132‑192](../../../services/ingest/integrations/brex/client.py#L132-L192)),
which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- honours `429` + `Retry-After` within a bounded budget
  (`BREX_RL_MAX_ATTEMPTS`=4, `BREX_RL_MAX_SLEEP_SEC`=30),
- lets transport errors propagate as `BrexApiError`, and maps any non-2xx to a
  typed `BrexApiError` (`401/403` → `brex_api_unauthorized`, `404` →
  `brex_api_not_found`, `429` → `brex_api_rate_limited`, else `brex_api_error`)
  ([client.py:254‑285](../../../services/ingest/integrations/brex/client.py#L254-L285)).

The endpoints invoked for ingestion:

| Brex endpoint | Wrapper | Purpose | Code |
|---------------|---------|---------|------|
| `GET /accounts` | `list_accounts()` | enumerate accounts + balances (seed/install time, selector UI) | [client.py:198‑209](../../../services/ingest/integrations/brex/client.py#L198-L209) |
| `GET /account/{id}` | `get_account()` | one account — the fetcher's balance-snapshot probe | [client.py:211‑213](../../../services/ingest/integrations/brex/client.py#L211-L213) |
| `GET /account/{id}/transactions?limit&offset&start` | `list_transactions()` | paginated transaction page (full or `start=`-bounded incremental) | [client.py:215‑240](../../../services/ingest/integrations/brex/client.py#L215-L240) |

> **TODO(human):** confirm the read surface + scopes. These three paths
> (`/accounts`, `/account/{id}`, `/account/{id}/transactions`) are **CLONED from
> Mercury and UNVERIFIED for Brex**; Brex's real paths are likely
> `/v2/accounts/cash`, `/v2/transactions/card`, etc.
> ([client.py:9‑16](../../../services/ingest/integrations/brex/client.py#L9-L16)).
> *Why: the internal record-shaping is correct regardless of path, but the URLs
> must be confirmed before the real API is hit.*

### 3.1 Pagination — offset / limit (UNVERIFIED)

`list_transactions` returns `(items, next_offset, total)`; `next_offset is None`
is terminal. It advances `offset += len(txns)` and stops when
`next_offset >= total or not txns`
([client.py:229‑240](../../../services/ingest/integrations/brex/client.py#L229-L240)).
The fetcher persists `offset` in its cursor and resumes next invocation. Page
size defaults to 100, capped at 500, overridable via `BREX_BACKFILL_PAGE_SIZE`
([fetchers/brex.py:63‑67](../../../services/ingest/ingestion/fetchers/brex.py#L63-L67)).

> **TODO(human):** confirm pagination model. The offset/limit contract is
> **CLONED from Mercury and UNVERIFIED**; Brex v2 may be cursor-token based, in
> which case `BrexCursor.offset` becomes an opaque page token and `start=`
> becomes whatever "created/posted since" filter the API exposes
> ([client.py:22‑24](../../../services/ingest/integrations/brex/client.py#L22-L24),
> [fetchers/brex.py:35‑40](../../../services/ingest/ingestion/fetchers/brex.py#L35-L40)).
> *Why: a wrong pagination assumption would silently truncate or loop backfill.*

### 3.2 Rate limits — no dedicated bucket

Unlike Slack and GitHub, Brex has **no client-side token bucket** in
`services/ingest/ingestion/rate_limit/buckets.py` (a grep for `brex` there is
empty). Rate-limit pressure is handled **only** by the per-request
`429` + `Retry-After` retry inside `BrexClient._request`
([client.py:171‑175](../../../services/ingest/integrations/brex/client.py#L171-L175)).

> **TODO(human):** confirm Brex's rate-limit signalling. The code assumes
> `429` + `Retry-After` (Mercury's scheme); Brex may instead signal via
> `X-RateLimit-Reset` ([client.py:18‑20](../../../services/ingest/integrations/brex/client.py#L18-L20)).
> *Why: if Brex uses a different header, the retry never sleeps the right amount.*

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per active account**, all of
`shard_kind = "brex_account_txns"`
([planners/brex.py:55‑87](../../../services/ingest/ingestion/planners/brex.py#L55-L87)).
There is a **single** shard family — one shard per account's transaction stream.

`ctx.source_client` is **None** — the planner reads accounts purely from DB
state. The `brex_accounts` rows (populated at seed time by
`BrexClient.list_accounts`) are JSON-aggregated into `ctx.install["accounts"]`
by the SourceOnboarding loader, so the planner stays stateless (no DB I/O) —
same precedent as Jira / Calendar / Gmail
([planners/brex.py:1‑20](../../../services/ingest/ingestion/planners/brex.py#L1-L20)).

Each shard carries `account_id`, `account_name`, `installation_id`, and the
warm-start `txn_cursor` (the high-water transaction `createdAt`, `None` on first
sync), at baseline `recency_score=1.0`
([planners/brex.py:69‑81](../../../services/ingest/ingestion/planners/brex.py#L69-L81)).

> **TODO(human):** confirm the resource taxonomy to shard on. This clones
> Mercury's one-shard-per-account model; Brex distinguishes cash vs card
> accounts (and possibly other entities), so the entity list (and the
> child-table seed in onboarding) may need extending
> ([planners/brex.py:16‑20](../../../services/ingest/ingestion/planners/brex.py#L16-L20)).
> *Why: the highest-signal cash/card flow works today, but other Brex entities
> would be silently un-ingested.*

---

## 5. The fetcher — one shard kind, two sync modes, fan-out to N records

`fetch_page_brex` ([fetchers/brex.py:124‑201](../../../services/ingest/ingestion/fetchers/brex.py#L124-L201))
takes one `(install, shard_identifier, cursor)` triple and returns one page of
records + the next cursor, persisted between calls by ShardFetch.

### 5.1 The cursor

```python
class BrexCursor:
    offset: int = 0                       # list-transactions pagination offset
    high_water_created: str | None        # max txn createdAt (ISO) — warm-start
                                          #   lower bound AND reconciler gap ref
    incremental_floor: str | None         # the start= lower bound frozen for the run
    txns_seen: int = 0                     # diagnostic
    seeded: bool = False                   # whether first-call setup (snapshot) ran
```

([fetchers/brex.py:70‑91](../../../services/ingest/ingestion/fetchers/brex.py#L70-L91)).
It round-trips through the opaque `workflow_states.state_data` dict.

### 5.2 Two sync modes

- **FULL (initial backfill):** walk `GET /account/{id}/transactions` from
  `offset=0`, paginated, with no `start=` filter
  ([fetchers/brex.py:8‑20](../../../services/ingest/ingestion/fetchers/brex.py#L8-L20)).
- **INCREMENTAL (poll):** when the shard is warm-started with a `txn_cursor`,
  the fetcher sets `incremental_floor`/`high_water_created` to that value on the
  first call and passes `start=<date>` (date-granular) so only recent
  transactions return; overlap re-fetch dedups via the versioned `external_id`
  ([fetchers/brex.py:140‑145](../../../services/ingest/ingestion/fetchers/brex.py#L140-L145),
  [160‑166](../../../services/ingest/ingestion/fetchers/brex.py#L160-L166)).

### 5.3 Fan-out: one account → N records

The handler produces ONE observation per record. The fetcher emits two record
types, each tagged with a private `_fyralis_record_type` the handler branches on
([fetchers/brex.py:22‑40](../../../services/ingest/ingestion/fetchers/brex.py#L22-L40)):

- **`account_snapshot`** — emitted **once per shard run**, on the first
  (un-seeded) call, from `get_account(account_id)`. A `BrexApiError` on the probe
  is swallowed (the snapshot is best-effort, not run-fatal)
  ([fetchers/brex.py:146‑158](../../../services/ingest/ingestion/fetchers/brex.py#L146-L158)).
- **`transaction`** — one per transaction on the page. After each, the cursor's
  `high_water_created` is bumped to the max of `createdAt`/`postedAt`
  ([fetchers/brex.py:178‑188](../../../services/ingest/ingestion/fetchers/brex.py#L178-L188)).

A `brex_api_rate_limited` error mid-page is treated as a **non-fatal pause**: the
fetcher returns the records collected so far with `end_of_data=False` so the run
resumes, rather than failing ([fetchers/brex.py:167‑176](../../../services/ingest/ingestion/fetchers/brex.py#L167-L176)).

---

## 6. The handler — shaping records into `ObservationDraft`

`handle_brex_transaction` ([handlers/brex.py:299‑342](../../../services/ingest/ingestion/handlers/brex.py#L299-L342))
is a pure function (no DB / network). It dispatches on input shape:

- **Live webhook** — `payload["type"]` present. `transaction.*` → transaction
  draft; `account.*` → snapshot draft; anything else → `ValidationError`
  ([handlers/brex.py:306‑325](../../../services/ingest/ingestion/handlers/brex.py#L306-L325)).
- **Backfill / poll** — `_fyralis_record_type` ∈ `{transaction,
  account_snapshot}` (or a bare `transaction` key) routes to the same two builders
  ([handlers/brex.py:327‑342](../../../services/ingest/ingestion/handlers/brex.py#L327-L342)).

The two builders produce:

| Record | Builder | `external_id` | `occurred_at` | `kind` | Trust tier |
|--------|---------|---------------|---------------|--------|------------|
| transaction (pending/sent/posted/…) | `_transaction_draft` | `brex:{account}:txn:{id}:{status}` | `postedAt` → `createdAt` → now | `signal` | **authoritative** |
| transaction (failed/cancelled/canceled/declined) | `_transaction_draft` | `brex:{account}:txn:{id}:{status}` | `postedAt` → `createdAt` → now | **`state_change`** (cash-risk) | **authoritative** |
| account balance snapshot | `_account_snapshot_draft` | `brex:{account}:balance:{YYYY-MM-DD}` | `as_of` → now | `signal` | **authoritative** |

Highlights:

- **Channel:** `brex:transaction`, registered via `@register(_CHANNEL)`
  ([handlers/brex.py:299](../../../services/ingest/ingestion/handlers/brex.py#L299)).
- **Trust tier:** `authoritative` — Brex is the bank's system of record for cash.
  Note the channel is registered into `CHANNEL_TRUST_MAP` at **import time** via
  `CHANNEL_TRUST_MAP.setdefault("brex:transaction", "authoritative")`
  ([handlers/brex.py:345](../../../services/ingest/ingestion/handlers/brex.py#L345)),
  not in the static dict in `handlers/__init__.py`. The `brex` handler module is
  imported in `handlers/__init__.py:179` so the registration always runs.
- **`source_actor_ref` is always `None`** — Brex transactions are
  system/bank-originated, not attributed to a human actor
  ([handlers/brex.py:226](../../../services/ingest/ingestion/handlers/brex.py#L226),
  [276](../../../services/ingest/ingestion/handlers/brex.py#L276)).
- **State-change statuses** (`failed`, `cancelled`, `canceled`, `declined`) flip
  `kind` to `state_change` — the cash-risk signal — and inline the
  `reasonForFailure` into `content_text`
  ([handlers/brex.py:51](../../../services/ingest/ingestion/handlers/brex.py#L51),
  [181‑191](../../../services/ingest/ingestion/handlers/brex.py#L181-L191)).
- **PII redaction:** `accountNumber`, `routingNumber`, `iban` in a transaction's
  `details` sub-tree are masked to the last 4 chars before they reach the
  reasoning layer / LLM context
  ([handlers/brex.py:100‑118](../../../services/ingest/ingestion/handlers/brex.py#L100-L118),
  [148‑152](../../../services/ingest/ingestion/handlers/brex.py#L148-L152)).
- **`entities_hint`** carries a `brex_account` ref and, for transactions, an
  `organization` (counterparty) ref
  ([handlers/brex.py:193‑197](../../../services/ingest/ingestion/handlers/brex.py#L193-L197)).

---

## 7. Live (real‑time) ingestion via HMAC-signed webhooks

When a transaction or account changes in Brex, Brex **POSTs an HMAC-signed
webhook delivery** to Fyralis's webhook edge. The router maps provider `brex` →
channel `brex:transaction` ([router.py:455](../../../services/app/webhooks/router.py#L455)).

### 7.1 Signature verification (HMAC-SHA256, no timestamp)

`BrexVerifier.verify` ([signatures/brex.py:50‑92](../../../services/app/webhooks/signatures/brex.py#L50-L92))
computes `HMAC-SHA256(secret, raw_body)`, prefixes it `sha256=`, and
constant-time compares against the `Brex-Signature` header. Each active secret is
tried in turn so a rotation (two valid secrets in flight) verifies
([signatures/brex.py:72‑85](../../../services/app/webhooks/signatures/brex.py#L72-L85)).
The verifier is registered as `VERIFIERS["brex"] = brex.verifier`
([signatures/__init__.py:55](../../../services/app/webhooks/signatures/__init__.py#L55)).

> **No replay window.** Like GitHub/Jira, the digest is over the body alone —
> there is no timestamp envelope, so there is no replay window here; idempotency
> is enforced at the ingestion layer via the versioned `external_id`
> ([signatures/brex.py:16‑18](../../../services/app/webhooks/signatures/brex.py#L16-L18)).
> There is also **no per-delivery replay cache** for Brex (contrast GitHub's
> `(installation, delivery)` cache); the `external_id` dedup is the sole backstop.

> **TODO(human):** confirm the webhook signature scheme. The header name
> (`Brex-Signature`), prefix (`sha256=`), and digest encoding (`hex`) are
> **CLONED from Mercury and UNVERIFIED**, exposed as the module constants
> `_HEADER_NAME` / `_PREFIX` / `_DIGEST_ENCODING` so confirming the real scheme
> is a one-line edit per knob
> ([signatures/brex.py:3‑9](../../../services/app/webhooks/signatures/brex.py#L3-L9),
> [37‑41](../../../services/app/webhooks/signatures/brex.py#L37-L41)).
> *Why: a wrong header/encoding rejects every real Brex delivery as a signature
> mismatch.*

### 7.2 Tenant resolution

The tenant is resolved by `_extract_brex` from the webhook body's top-level
`organizationId` (falling back to `accountId`) → the `provider_installations`
row for `(provider='brex', installation_id=…)`
([tenant_resolver.py:358‑372](../../../services/app/webhooks/tenant_resolver.py#L358-L372)).
The signing secret is loaded separately from `provider_installations.secret_ref`
for the **enabled** row ([secrets.py:170‑184](../../../services/app/webhooks/secrets.py#L170-L184)).

> **TODO(human):** confirm the Brex webhook tenant-id field —
> `organizationId` vs `accountId` vs an event-envelope path
> ([tenant_resolver.py:367‑368](../../../services/app/webhooks/tenant_resolver.py#L367-L368)).
> *Why: the synthetic harness sends `organizationId`, but if real Brex nests it,
> tenant resolution fails and the delivery 401s.*

### 7.3 `kafka_path_enabled` cutover — inline `brex:transaction` when off

Brex is in **both** the shadow-source map and the cutover-enabled map
([router.py:137](../../../services/app/webhooks/router.py#L137),
[router.py:176](../../../services/app/webhooks/router.py#L176)):

- **Flag ON** (`ingestion.kafka_path_enabled=TRUE` for the tenant): the verified
  body is published to Kafka and the edge returns **`202`**; inline `ingest()`
  is skipped — the writer pool produces the observation via the full pipeline
  ([router.py:1045‑1070](../../../services/app/webhooks/router.py#L1045-L1070)).
- **Flag OFF** (the kill-switch, also the default): the edge falls through to
  **inline `ingest("brex:transaction", payload, …)`** using the
  `_PROVIDER_CHANNEL["brex"] = "brex:transaction"` fallback channel
  ([router.py:455](../../../services/app/webhooks/router.py#L455),
  [router.py:1092‑1118](../../../services/app/webhooks/router.py#L1092-L1118)).
- If the Kafka publish *fails* while the flag is ON, the edge gracefully
  degrades to the same inline path (preserving the user-visible 200/201) and
  records a `fallback` metric ([router.py:1071‑1090](../../../services/app/webhooks/router.py#L1071-L1090)).

---

## 8. Reconciliation — gap detection

`reconcile_brex` ([reconcilers/brex.py:131‑168](../../../services/ingest/ingestion/reconcilers/brex.py#L131-L168))
re-checks **done** account shards for new activity. For each shard it loads the
stored `high_water_created` from the shard's cursor and issues a cheap probe —
`list_transactions(account_id, limit=1, offset=0, start=high_water[:10])`. If the
newest returned transaction's `createdAt`/`postedAt` is **strictly newer** than
the high-water, it emits a reshare
([reconcilers/brex.py:80‑128](../../../services/ingest/ingestion/reconcilers/brex.py#L80-L128)).

The reshare is a `brex_account_txns` shard at **`recency_score=1.5`**, warm-started
with `txn_cursor = high_water` so the re-walk only re-fetches the changed tail
(incremental mode). `external_id` parity makes re-walked transactions idempotent
— only genuinely new/changed transactions produce new observations. The reconciler
opens one shared client and resolves the **one enabled, non-disabled**
`brex_installations` row for the tenant
([reconcilers/brex.py:138‑151](../../../services/ingest/ingestion/reconcilers/brex.py#L138-L151)).

The probe doubles as the **~45-day token keepalive** — Brex deletes idle tokens,
and the periodic gap probe keeps the long-lived Bearer token warm
([reconcilers/brex.py:11‑13](../../../services/ingest/ingestion/reconcilers/brex.py#L11-L13)).
A probe failure is logged and skipped (best-effort, never run-fatal)
([reconcilers/brex.py:98‑103](../../../services/ingest/ingestion/reconcilers/brex.py#L98-L103)).

---

## 9. Revocation chokepoint — ABSENT

**There is no revocation chokepoint for Brex.** Unlike GitHub (which disables an
install on `401 Bad credentials` / specific `404`s) and Notion (which parks +
disables on token revocation), the Brex client maps `401/403` to a typed
`brex_api_unauthorized` `BrexApiError` and **does nothing else** — no install row
is disabled, no `disabled_at` is stamped, nothing is parked
([client.py:188‑192](../../../services/ingest/integrations/brex/client.py#L188-L192),
[259‑264](../../../services/ingest/integrations/brex/client.py#L259-L264)). A grep
for `disable` / `revoke` / `park` across the Brex integration finds only the
*unset* of `disabled_at` in the install UPSERT
([onboarding.py:76](../../../services/ingest/integrations/brex/onboarding.py#L76))
and the reconciler's `WHERE disabled_at IS NULL` filter
([reconcilers/brex.py:143](../../../services/ingest/ingestion/reconcilers/brex.py#L143)).

**Recoverable-error behavior is partial:** the *fetcher* treats a
`brex_api_rate_limited` (429) error as a non-fatal pause and resumes the run
([fetchers/brex.py:167‑176](../../../services/ingest/ingestion/fetchers/brex.py#L167-L176)),
and the snapshot probe swallows `BrexApiError`
([fetchers/brex.py:147‑150](../../../services/ingest/ingestion/fetchers/brex.py#L147-L150)).
But a `401/403` mid-backfill (a revoked or insufficient-scope token) **propagates
and fails the shard** — there is no recover-via-re-OAuth chokepoint and no
install-disable as Notion/GitHub have.

> **TODO(human):** decide whether Brex needs a revocation chokepoint. Today a
> revoked API token fails the run with no install-level park/disable and no
> recovery path; the only "keepalive" is the reconciler probe (§8). *Why: this is
> an intentional v1 gap to flag, not a bug to assert — the blueprint may consider
> the token long-lived enough that a chokepoint isn't worth it.*

---

## 10. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  connect wizard: BrexClient.list_accounts() (Bearer token)       │
   ACTIVE ACCOUNTS        │     └─► brex_installations + brex_accounts + onboarding_trigger  │
                          │  planner: one brex_account_txns shard per account (DB state)     │
                          │  fetcher (FULL):  GET /account/{id}/transactions  offset=0        │
                          │     └─► first call: GET /account/{id} → account_snapshot record   │
                          │     └─► each txn → "transaction" record (+ bump high-water)       │
                          └───────────────────────────────────────────────────────────────┬─┘
                          ┌──────────────────────── POLL (incremental) ───────────────────┐│
   warm txn_cursor   ─────►  same fetcher, ingress_kind="poll", start=<high-water date>   ││
                          │     └─► only the changed tail; dedup via versioned external_id  ││
                          └───────────────────────────────────────────────────────────────┘│
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐│
   txn/account change ────►  Brex webhook ──HTTP POST──► /webhooks/brex                    ││
                          │     verify Brex-Signature (HMAC-SHA256 hex, no ts) [UNVERIFIED] ││
                          │     tenant = organizationId|accountId → provider_installations  ││
                          │     flag ON → publish Kafka, 202 ; flag OFF → inline ingest()    ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_brex_transaction         │
                                                            │  webhook: branch on `type`        │
                                                            │  backfill: branch on _fyralis_*   │
                                                            │  txn  external_id =               │
                                                            │    brex:{acct}:txn:{id}:{status}  │
                                                            │  snap external_id =               │
                                                            │    brex:{acct}:balance:{date}     │
                                                            │  → ObservationDraft (authoritative)│
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, poll, and live webhook all land
   on `brex:transaction`. A backfilled transaction and its live twin dedup to one
   observation via `external_id = brex:{account}:txn:{id}:{status}`.
2. **Status-versioned dedup.** Because dedup ignores `occurred_at`, the mutable
   transaction status is part of `external_id` — a `pending → posted`/`declined`
   transition lands as a new observation, not a silent drop.
3. **One credential model — Bearer, no refresh.** A single long-lived API token
   reads everything (resolved once from the secret store). No OAuth handshake, no
   token mint. The `oauth.py` filename is a misnomer; it's a Bearer connect wizard.
4. **One shard family.** One `brex_account_txns` shard per account; the planner is
   stateless (`ctx.source_client is None`), reading accounts from DB state.
5. **Fan-out 1→N.** Each shard run emits one balance `account_snapshot` plus one
   `transaction` per transaction page row.
6. **No revocation chokepoint, no replay cache, no rate-limit bucket.** The 429
   `Retry-After` retry in the client and the `external_id` dedup are the only
   resilience mechanisms; a revoked token fails the run with no install-disable.

---

## 11. Configuration & compliance

> Brex's external contract is **not** verified against official Brex docs — every
> external-surface assumption is CLONED from Mercury and flagged `TODO(human)`
> above. Fyralis's internal wiring (channel, dedup, planner/fetcher/handler/
> reconciler) **is** verified against the code.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `BREX_API_BASE_URL` | `https://platform.brexapis.com` (via `endpoint("brex_api")`) | overrides the canonical Brex API host ([endpoints.py:70‑72](../../../lib/integrations/endpoints.py#L70-L72), [131](../../../lib/integrations/endpoints.py#L131)) |
| `BREX_BACKFILL_PAGE_SIZE` | `100` (capped at 500) | transactions page size ([fetchers/brex.py:63‑67](../../../services/ingest/ingestion/fetchers/brex.py#L63-L67)) |
| `BREX_RL_MAX_ATTEMPTS` | `4` | rate-limit (429) retry budget ([client.py:152](../../../services/ingest/integrations/brex/client.py#L152)) |
| `BREX_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:153](../../../services/ingest/integrations/brex/client.py#L153)) |

Plus the per-tenant `ingestion.kafka_path_enabled` flag (the webhook cutover
kill-switch, §7.3).

### 11.2 Verified (internal) ✅ / unverified (external) ⚠️

- **Auth model** — single Bearer API token, resolved once, never logged, no
  refresh. ✅ (internal)
- **One channel / one dedup key** — `brex:transaction`; status-versioned txn id,
  date-versioned balance. ✅ (internal)
- **Trust tier** — `authoritative`, registered via `setdefault` at import. ✅
- **PII redaction** — account/routing/IBAN masked to last 4 in `details`. ✅
- **Webhook body-only HMAC, no replay window** — confirmed in code. ✅ (internal)
- **No replay cache, no rate-limit bucket, no revocation chokepoint** —
  confirmed absent. ✅ (accurately documented)
- **API host / read paths / pagination model / webhook scheme / tenant field** —
  ⚠️ CLONED from Mercury, **UNVERIFIED** against Brex docs (see the five
  `TODO(human)` callouts).

### 11.3 Dev / spammer mode

For local testing against the mock source servers, `build_brex_client` detects
spammer mode and **presets the API token** to `spam-brex`, skipping the secret
store entirely; the API base then points at the local spammer's `/brex`
sub-path (`endpoint("brex_api")` → `/brex`)
([_clients.py:427‑452](../../../services/ingest/ingestion/fetchers/_clients.py#L427-L452),
[endpoints.py:161](../../../lib/integrations/endpoints.py#L161)).

The mock Brex server ([synthetic/mock_servers/brex.py](../../../services/ingest/synthetic/mock_servers/brex.py))
serves `GET /accounts`, `GET /account/{id}`, and
`GET /account/{id}/transactions?limit&offset&start` (full vs `start=`-bounded
incremental), so the **real** `BrexClient` + fetcher + reconciler can be driven
end-to-end with no Brex credentials. The path SUFFIX is matched so the `/brex`
prefix is irrelevant.
