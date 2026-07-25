# Notion Ingestion — How Fyralis Pulls Notion Data

This document explains, in detail, **how Notion data enters Fyralis**: which
Notion REST APIs are called, with which token, and how the Notion content set —
**pages, blocks, and comments**, across **databases** and **loose pages** — is
each ingested.

It deliberately stops at the point where a Notion object becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

---

## 1. The two ways data arrives

Notion data reaches Fyralis through **two independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* the workspace tree via the Notion **REST API** (`/v1/search`, `/v1/databases/{id}/query`, `/v1/blocks/{id}/children`, `/v1/comments`) | `services/ingest/ingestion/planners/notion.py`, `services/ingest/ingestion/fetchers/notion.py` |
| **Live (real‑time)** | A page changes in Notion | Notion *pushes* a **thin webhook** delivery; Fyralis fetches the full page back | `services/app/webhooks/router.py`, `services/app/webhooks/signatures/notion.py`, `services/ingest/integrations/notion/webhook.py` |

Unlike Slack and GitHub — where the live webhook payload is a complete event the
handler can shape directly — **Notion's live webhook is *thin*** (an entity id +
a dotted event type, no object body). So the live path is **fetch‑back**: the
webhook handler retrieves the full page via the bot token and **shadow‑writes**
it onto the same data plane backfill uses. Both paths therefore deliver the
**same record shape** to the **single** `notion:object` handler
([handlers/notion.py](../../../services/ingest/ingestion/handlers/notion.py)) — the raw
Notion object, carrying its own `object` field (`"page" | "block" | "comment"`).
Both derive the **same** dedup key per object:

```
page    → external_id = "notion:page:{id}"
block   → external_id = "notion:block:{id}"
comment → external_id = "notion:comment:{id}"
```

Because a Notion object id is identical whether the object arrived via backfill
or was fetched back from a webhook, an object that is both backfilled *and*
delivered live collapses into **one** observation. This is the central design
invariant of Notion ingestion
([handlers/notion.py:164](../../../services/ingest/ingestion/handlers/notion.py#L164),
[198](../../../services/ingest/ingestion/handlers/notion.py#L198),
[235](../../../services/ingest/ingestion/handlers/notion.py#L235)).

> Notion uses **REST only** (`Notion-Version: 2022-06-28`). No GraphQL. Real‑time
> is a **thin HTTP webhook** that triggers a **REST fetch‑back**; history is the
> **REST API**.

---

## 2. Authentication — one long‑lived bot token

Notion ingestion uses a **single credential model: a per‑workspace integration
("bot") token**, issued once at OAuth install. There are no per‑user tokens, no
PATs, and — unlike GitHub — **no per‑request token mint or JWT**. The token is
**long‑lived**: resolved once from the secret store (or preset in Provider Lab mode)
and reused for the life of the client
([client.py:1‑22](../../../services/ingest/integrations/notion/client.py#L1-L22),
[112‑134](../../../services/ingest/integrations/notion/client.py#L112-L134)).

### 2.1 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Bot token | secret store, label `notion_token:{workspace_id}`; pointed at by `provider_installations.secret_ref` | the **outbound** API credential; resolved lazily on first request and cached on the client instance |
| OAuth client id / secret | `NOTION_CLIENT_ID` / `NOTION_CLIENT_SECRET` (env) | used only during the install code‑exchange |
| Redirect URI | `NOTION_REDIRECT_URI` (env) | OAuth callback URL |
| Webhook verification token | `NOTION_WEBHOOK_VERIFICATION_TOKEN` (+ `…_PREV` for rotation) | **App‑level** HMAC secret for inbound webhook signature verification — a *different* secret from the bot token |

> **Contrast with GitHub.** GitHub mints a short‑lived installation token on
> every read from an App JWT; `provider_installations.secret_ref` is `NULL`.
> Notion stores the **long‑lived bot token** *in* `secret_ref` and reuses it —
> so the GitHub "per‑fetch re‑mint" and "stale‑token 401 mid‑run" failure classes
> do **not** apply. The only inbound secret (the webhook verification token) is
> App‑global, like GitHub's webhook secret.

### 2.2 The OAuth install flow (how a workspace gets registered)

`services/ingest/integrations/notion/oauth.py` implements Notion's OAuth handshake on
the same state‑token substrate as Slack/GitHub:

1. **`GET /integrations/notion/install`** (Bearer‑authed) — issues an
   HMAC‑signed `state` token bound to the session's `tenant_id` (never a
   client‑supplied param; the state helpers are reused from Slack), then `302`s
   to `https://api.notion.com/v1/oauth/authorize?…&state=…`
   ([oauth.py:74‑133](../../../services/ingest/integrations/notion/oauth.py#L74-L133)).
2. **`GET /integrations/notion/callback`** (public, state‑authed) — verifies the
   HMAC, atomically consumes the nonce, then **`POST`s `/v1/oauth/token`**
   (HTTP Basic `client_id:client_secret`) to exchange the `code` for
   `{access_token, workspace_id, workspace_name, bot_id, …}`
   ([oauth.py:140‑164](../../../services/ingest/integrations/notion/oauth.py#L140-L164),
   [276‑326](../../../services/ingest/integrations/notion/oauth.py#L276-L326)).
3. The bot token is stored under `notion_token:{workspace_id}`; **in one
   transaction** a `provider_installations` row keyed on
   `(provider='notion', installation_id=workspace_id)` is upserted with
   `secret_ref` pointing at that token, and an `onboarding_triggers` row
   (`install` vs `reinstall`) is emitted
   ([oauth.py:167‑225](../../../services/ingest/integrations/notion/oauth.py#L167-L225),
   [347‑360](../../../services/ingest/integrations/notion/oauth.py#L347-L360)). A
   cross‑tenant rebind is rejected with `installation_collision`, and the foreign
   `tenant_id` never appears in the response, redirect, or logs
   ([oauth.py:179‑199](../../../services/ingest/integrations/notion/oauth.py#L179-L199),
   [361‑367](../../../services/ingest/integrations/notion/oauth.py#L361-L367)).
4. There is **no separate webhook‑secret persistence** here. Notion's
   subscription verification token is App‑level and is captured out‑of‑band (§8.1),
   not per‑install ([oauth.py:18‑28](../../../services/ingest/integrations/notion/oauth.py#L18-L28)).

---

## 3. The Notion REST API surface that is actually called

All read calls funnel through `NotionClient._request`
([client.py:136‑190](../../../services/ingest/integrations/notion/client.py#L136-L190)),
which:

- sets `Authorization: Bearer {bot_token}`, `Notion-Version: 2022-06-28`
  (env‑overridable via `NOTION_API_VERSION`), and `Content-Type: application/json`,
- honours `Retry-After` on **`429`** within a bounded budget
  (`NOTION_RL_MAX_ATTEMPTS`=4, `NOTION_RL_MAX_SLEEP_SEC`=30),
- maps transport errors and any non‑2xx to a typed `NotionApiError`
  (`401`→`notion_api_unauthorized`, `404`→`notion_api_not_found`,
  `429`→`notion_api_rate_limited`, else `notion_api_error`)
  ([client.py:344‑386](../../../services/ingest/integrations/notion/client.py#L344-L386)).

The endpoints invoked for ingestion:

| Notion endpoint | Wrapper | Purpose | Code |
|-----------------|---------|---------|------|
| `POST /v1/search` (`filter=database`) | `search(object_filter="database")` | enumerate visible databases (planner) | [client.py:196‑211](../../../services/ingest/integrations/notion/client.py#L196-L211) |
| `POST /v1/search` (`filter=page`) | `search(object_filter="page")` | enumerate loose pages (fetcher) | ″ |
| `POST /v1/databases/{id}/query` | `query_database()` | rows (page objects) of a database | [client.py:213‑228](../../../services/ingest/integrations/notion/client.py#L213-L228) |
| `GET /v1/blocks/{id}/children` | `list_block_children()` | child blocks of a page/block | [client.py:230‑245](../../../services/ingest/integrations/notion/client.py#L230-L245) |
| `GET /v1/comments?block_id=` | `list_comments()` | comments on a page/block | [client.py:247‑260](../../../services/ingest/integrations/notion/client.py#L247-L260) |
| `POST /v1/databases/{id}/query` (`page_size=1`, sort desc) | `latest_database_edit()` | reconciler gap probe (newest row edit) | [client.py:262‑279](../../../services/ingest/integrations/notion/client.py#L262-L279) |
| `POST /v1/search` (`page`, sort desc) | `latest_page_edit()` | reconciler gap probe (newest loose page) | [client.py:281‑311](../../../services/ingest/integrations/notion/client.py#L281-L311) |
| `GET /v1/pages/{id}` | `retrieve_page()` | webhook fetch‑back of one page | [client.py:313‑315](../../../services/ingest/integrations/notion/client.py#L313-L315) |
| `GET /v1/users/me` | `retrieve_bot_user()` | identity / connectivity probe | [client.py:317‑321](../../../services/ingest/integrations/notion/client.py#L317-L321) |

### 3.1 Pagination — opaque cursor

Every list endpoint paginates the **same** way: Notion returns
`{results: […], next_cursor: str|null, has_more: bool}`; the client unwraps that
into the `(results, next_cursor, has_more)` triple
([_unwrap_list, client.py:328‑341](../../../services/ingest/integrations/notion/client.py#L328-L341)),
the caller passes `next_cursor` back as `start_cursor`, and **`has_more=false` is
the terminal signal**.

- The planner's `_paginate_search` loops **to completion** internally so the full
  database/page set is enumerated even for a large workspace
  ([planners/notion.py:66‑80](../../../services/ingest/ingestion/planners/notion.py#L66-L80)).
- The fetcher's list calls return **one page** plus the cursor to the work‑stack,
  which is persisted in the shard cursor and resumed next invocation (§5).

The cursor is **opaque** — never parsed, only round‑tripped — so there is no
ETag/`If-None-Match`/`updated_at`‑ordering machinery as in GitHub.

### 3.2 Rate limits — reactive only

Notion enforces **~3 requests/second per integration** and returns **`429` with
an integer‑seconds `Retry-After`** header. **There is no `notion` entry in
`BUCKET_DEFAULTS`** ([rate_limit/buckets.py:79‑91](../../../services/ingest/ingestion/rate_limit/buckets.py#L79-L91)),
and the live shard‑fetch path does not consult the Redis token bucket at all
(`RateLimiter` is wired only into the embedding‑backlog recovery path). So Notion
throttling is **purely reactive**: `_request` sleeps on `Retry-After` within its
bounded retry budget; when the budget is exhausted it surfaces a `429`
`NotionApiError`, and the fetcher re‑queues the work item and ends the round empty
so ShardFetch re‑enters next tick with the cursor preserved (§5.3).

> Contrast: GitHub declares a client‑side bucket and Slack declares per‑method
> tiers in `BUCKET_DEFAULTS`, but Notion relies on the reactive `Retry-After`
> handler alone.

---

## 4. Backfill scope — the shard families

The planner decomposes one workspace install into **two shard kinds**
([planners/notion.py:83‑128](../../../services/ingest/ingestion/planners/notion.py#L83-L128)):

| Shard kind | How many | Covers | REST entry |
|------------|----------|--------|------------|
| `notion_database` | **one per visible database** | that DB's rows → each row's blocks → each row's comments | `POST /v1/search?filter=database` enumerates them |
| `notion_page_tree` | **exactly one** | all **loose** pages (those *not* a row of a database) → each page's blocks + comments | `POST /v1/search?filter=page` inside the fetcher |

The split mirrors GitHub's per‑`(repo, event_type)` sharding: it bounds each
fetch unit and lets **recency ordering** run recently‑edited databases first.
Each `notion_database` shard carries a `recency_score = exp(-age_days/τ)` (τ = 7
days) from the database's `last_edited_time`; the single page‑tree shard is fixed
at `1.0` ([planners/notion.py:48‑63](../../../services/ingest/ingestion/planners/notion.py#L48-L63),
[100‑122](../../../services/ingest/ingestion/planners/notion.py#L100-L122)).

The planner requires a real `NotionClient` in its `PlannerContext` (API‑at‑plan
time enumeration); a `None` source client is a hard error
([planners/notion.py:86‑91](../../../services/ingest/ingestion/planners/notion.py#L86-L91)).

---

## 5. The fetcher — a resumable tree walk (the cursor *is* a work stack)

A Notion shard is a **tree** (database → rows → each row's blocks → each row's
comments), and every Notion list endpoint is itself paginated. The fetcher models
the whole walk as an explicit **work stack carried in the cursor**, popping
**exactly one work item (= one Notion API list call) per invocation** so the walk
is fully restorable under the ingestion N1 invariant
([fetchers/notion.py:1‑42](../../../services/ingest/ingestion/fetchers/notion.py#L1-L42)).

### 5.1 The cursor

```python
class WorkItem(BaseModel):
    kind: str            # db_rows | loose_pages | page_blocks | page_comments
    list_cursor: str|None # opaque Notion cursor for this list call
    page_id: str|None
    block_id: str|None    # parent block for page_blocks (page_id at root)
    depth: int = 0

class NotionCursor(BaseModel):
    stack: list[WorkItem] = []
    items_seen: int = 0
    last_edited_at: str|None = None   # high-water for the reconciler
    seeded: bool = False
```

([fetchers/notion.py:70‑87](../../../services/ingest/ingestion/fetchers/notion.py#L70-L87)).
On the first call the stack is seeded from the shard kind — `db_rows` for a
`notion_database` shard, `loose_pages` for the `notion_page_tree` shard
([_seed, fetchers/notion.py:106‑115](../../../services/ingest/ingestion/fetchers/notion.py#L106-L115)).
`end_of_data=True` exactly when the stack empties
([fetchers/notion.py:143‑144](../../../services/ingest/ingestion/fetchers/notion.py#L143-L144),
[258‑262](../../../services/ingest/ingestion/fetchers/notion.py#L258-L262)).

### 5.2 The four work‑item kinds

| Work item | API call | Emits | Pushes onto stack |
|-----------|----------|-------|-------------------|
| `db_rows` | `query_database(database_id, list_cursor)` | each row page object | per row: a `page_comments` + a `page_blocks` item; a `db_rows` continuation if `has_more` | 
| `loose_pages` | `search(filter="page", list_cursor)` | each page **not** owned by a database | per page: `page_comments` + `page_blocks`; a `loose_pages` continuation if `has_more` |
| `page_blocks` | `list_block_children(block_id, list_cursor)` | each child block | recurse into children‑with‑children up to the **depth cap** (D2); a continuation if `has_more` |
| `page_comments` | `list_comments(page_id, list_cursor)` | each comment | a continuation if `has_more` |

([fetchers/notion.py:155‑231](../../../services/ingest/ingestion/fetchers/notion.py#L155-L231)).

Two design points:

- **Database rows are skipped in the loose‑page sweep.** `loose_pages` drops any
  page whose `parent.type == "database_id"` (`_is_database_row`) — those are
  covered by their own `notion_database` shard, so the workspace is walked once,
  not twice ([fetchers/notion.py:126‑129](../../../services/ingest/ingestion/fetchers/notion.py#L126-L129),
  [176‑180](../../../services/ingest/ingestion/fetchers/notion.py#L176-L180)).
- **Block recursion has a depth cap.** `NOTION_BLOCK_DEPTH_CAP` (default 3) bounds
  the block tree; at the cap a child‑bearing block is stamped with a
  `_fyralis_truncated` marker and a `block_truncated` metric instead of recursing
  ([fetchers/notion.py:63‑67](../../../services/ingest/ingestion/fetchers/notion.py#L63-L67),
  [198‑210](../../../services/ingest/ingestion/fetchers/notion.py#L198-L210)).

### 5.3 Handler conformance + error handling

Each emitted record is the **raw Notion object** (it already carries its own
`object` field), so no synthetic header is injected — the handler branches on
`record["object"]` directly. The fetcher injects only two private keys:
`_fyralis_workspace_id` (entity grounding) and `_fyralis_truncated` (depth‑cap
marker) ([fetchers/notion.py:31‑42](../../../services/ingest/ingestion/fetchers/notion.py#L31-L42)).
The high‑water `last_edited_at` advances over every object walked (rows, blocks,
comments) for the reconciler baseline
([_bump_high_water, fetchers/notion.py:118‑123](../../../services/ingest/ingestion/fetchers/notion.py#L118-L123)).

Two `NotionApiError` cases are handled in‑walk rather than failing the shard
([fetchers/notion.py:233‑253](../../../services/ingest/ingestion/fetchers/notion.py#L233-L253)):

- **`429` (budget exhausted)** — re‑push the **same** work item (cursor
  unadvanced), record the `rate_limited` metric, and return an empty,
  `end_of_data=False` round so ShardFetch retries next tick.
- **`404` (object un‑shared / deleted mid‑walk)** — **skip** this item and keep
  walking the rest of the tree (`end_of_data` reflects whether the stack is now
  empty).
- Any other API error **propagates** to ShardFetch, which **parks** the shard
  (leaves it `in_progress` for the orphan‑scan) when the error is *recoverable*,
  or terminal‑fails it otherwise (§5.4).

### 5.4 Recoverable errors & the revocation chokepoint (IN‑14 hardening)

`NotionApiError` carries a `recoverable` flag; ShardFetch parks a shard
(`getattr(exc,"recoverable",False) → return`, stay `in_progress`) instead of
terminal‑failing the whole run when it is set
([shard_fetch.py:937‑959](../../../services/ingest/ingestion/workflows/shard_fetch.py#L937-L959)).
Classification ([client.py `_api_error_from_response`](../../../services/ingest/integrations/notion/client.py)):

| Status | `recoverable` | Rationale |
|--------|---------------|-----------|
| `401` unauthorized | **yes** | token revoked → the chokepoint disables the install on this same 401, so the parked shard re‑claim lands on a disabled install and parks **cheaply** at `_load_install` (no token‑hammering) until a re‑OAuth / re‑enable resumes it |
| `429` rate‑limited | **yes** | transient (also handled in‑walk, §5.3) |
| `5xx` / transport | **yes** | transient upstream / network blip |
| `404` / other `4xx` | no | genuine not‑found / client fault — fail fast |

**Revocation chokepoint.** On a `401`, `NotionClient._maybe_disable_on_revocation`
fires `_disable_installation_notion` (sets `provider_installations.enabled=FALSE`
for `(provider='notion', installation_id=workspace_id, tenant_id)`, idempotent,
+ audit row) **before** the error is raised
([client.py](../../../services/ingest/integrations/notion/client.py),
[uninstall.py](../../../services/ingest/integrations/notion/uninstall.py)). This mirrors
GitHub's `_maybe_disable_on_revocation`. Unlike GitHub, Notion has **no inbound
`installation.unsuspend` webhook**, so re‑enable is via **re‑OAuth** (the install
callback upserts `enabled=TRUE` + emits a fresh onboarding trigger) or an
operator; once re‑enabled, the orphan‑scan resumes the parked shards. The net
effect: **a revoked token parks the backfill and never fails the run** — contrast
the pre‑IN‑14 behavior where one `401` terminal‑failed the shard and the entire
source run with no auto‑recovery.

---

## 6. The handler — shaping objects into `ObservationDraft`

`handle_notion_object` ([handlers/notion.py:248‑263](../../../services/ingest/ingestion/handlers/notion.py#L248-L263))
reads the Notion object's native `object` field and dispatches to one of three
shapers. There is **one channel** (`notion:object`, decision D3); object
granularity lives in `content.object_type` + `kind`. All Notion objects carry the
**`attested_agent`** trust tier — human‑authored via an authenticated integration,
the same tier as `slack:message`; Notion declares *intent*, it does not verify
*reality* ([handlers/notion.py:11‑17](../../../services/ingest/ingestion/handlers/notion.py#L11-L17),
[32‑33](../../../services/ingest/ingestion/handlers/notion.py#L32-L33)).

| `object` | Shaper | `external_id` | `occurred_at` | `kind` |
|----------|--------|---------------|---------------|--------|
| `page` | `_shape_page` | `notion:page:{id}` | `last_edited_time`/`created_time` | `state_change` if the page is a DB row carrying a `status`/`select` property, else `signal` |
| `block` | `_shape_block` | `notion:block:{id}` | `last_edited_time`/`created_time` | `signal` |
| `comment` | `_shape_comment` | `notion:comment:{id}` | `created_time` | `signal` |

Highlights:

- **`source_actor_ref`** is `notion:{user_id}` from `last_edited_by` (pages/blocks)
  or `created_by` (comments) ([handlers/notion.py:80‑87](../../../services/ingest/ingestion/handlers/notion.py#L80-L87)).
- **`entities_hint`** carries typed refs — `notion_page` (incl. the page id and
  any `relation`‑property edges), `notion_database`, and `notion_user`/`notion_page`
  mentions extracted from rich‑text `mention` spans
  ([handlers/notion.py:90‑108](../../../services/ingest/ingestion/handlers/notion.py#L90-L108),
  [136‑145](../../../services/ingest/ingestion/handlers/notion.py#L136-L145)).
- A **DB row with a `status`/`select` workflow property** is a tracked work item →
  `kind="state_change"` ([handlers/notion.py:126‑131](../../../services/ingest/ingestion/handlers/notion.py#L126-L131)).
- An unknown `object` value is rejected with a `ValidationError` listing the
  supported set ([handlers/notion.py:257‑262](../../../services/ingest/ingestion/handlers/notion.py#L257-L262)).

The normalizer routes **all** Notion ingress kinds to this one channel:
`("notion", "backfill")`, `("notion", "poll")`, and `("notion", "webhook")` →
`notion:object` ([channel_mapping.py:70‑72](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L70-L72)).

---

## 7. Live (real‑time) ingestion via the thin webhook

Notion subscriptions deliver a **thin change event** — `{entity:{id,type}, type,
workspace_id}` — **not** the object body. The webhook edge verifies + resolves +
**fetches the page back**, then shadow‑writes it onto the data plane. The router
short‑circuits Notion to `notion_webhook.handle_notion_event` *before* the
inline/cutover dispatch block (there is no `notion` inline channel)
([router.py:842‑854](../../../services/app/webhooks/router.py#L842-L854)).

### 7.1 The one‑time verification handshake (unsigned, intercepted pre‑verify)

Notion's first delivery is an **unsigned** subscription‑verification POST whose
body is `{"verification_token":"secret_…"}` — the token *is* the secret later
events are signed with, so there is nothing to verify against yet. The router
detects it (`is_verification_handshake`) **before** tenant resolution and
signature verification, logs the token at WARNING for the operator to copy into
`NOTION_WEBHOOK_VERIFICATION_TOKEN`, and returns `200 {"handled":"verification"}`
([router.py:678‑686](../../../services/app/webhooks/router.py#L678-L686),
[webhook.py:69‑103](../../../services/ingest/integrations/notion/webhook.py#L69-L103)).

### 7.2 Signature verification (HMAC‑SHA256, no timestamp)

Steady‑state events are verified against `X-Notion-Signature` —
`sha256=` + hex `HMAC-SHA256(verification_token, raw_body)`, constant‑time
compared; the bare hex is also accepted for header‑format drift. Each active
token is tried in turn (1–2 during rotation)
([signatures/notion.py:39‑82](../../../services/app/webhooks/signatures/notion.py#L39-L82)).
The verification token is an **App‑level** secret loaded from
`NOTION_WEBHOOK_VERIFICATION_TOKEN` (+ `…_PREV`), **not** from
`provider_installations.secret_ref` (that holds the bot token)
([secrets.py:288‑325](../../../services/app/webhooks/secrets.py#L288-L325)).

> **No replay window.** Like GitHub, Notion signs the body alone — there is no
> timestamp envelope and no replay window (contrast Slack's `v0:{ts}:{body}` +
> 300 s). Idempotency is the `external_id` dedup, not the edge
> ([signatures/notion.py:81](../../../services/app/webhooks/signatures/notion.py#L81)).
> There is currently **no** dedup/replay cache for Notion deliveries (GitHub has a
> `(installation, delivery)` cache).

### 7.3 Tenant resolution

The tenant is resolved from the payload's top‑level `workspace_id` → the
`provider_installations` row for `(provider='notion', installation_id=workspace_id)`
([tenant_resolver.py:289‑296](../../../services/app/webhooks/tenant_resolver.py#L289-L296),
[362](../../../services/app/webhooks/tenant_resolver.py#L362)). Unknown/disabled
installations get `401 unknown_installation`, deferred until *after* signature
verification so a tenant‑id prober sees signature failures first
([router.py:806‑815](../../../services/app/webhooks/router.py#L806-L815)).

### 7.4 Fetch‑back + shadow‑write

`handle_notion_event` ([webhook.py:143‑216](../../../services/ingest/integrations/notion/webhook.py#L143-L216)):

1. **Scope = pages only (v1).** If `entity.type != "page"` (or no id), it acks
   `200 {"handled":"ignored","reason":"unsupported_entity"}` without a write —
   databases/blocks/comments and un‑fetchable objects are covered by backfill on
   its cadence ([webhook.py:40‑46](../../../services/ingest/integrations/notion/webhook.py#L40-L46),
   [158‑168](../../../services/ingest/integrations/notion/webhook.py#L158-L168)).
2. **Fetch the full page** via a per‑workspace `NotionClient`
   (`build_notion_client`, identical bot‑token resolution + base URL as backfill)
   and `retrieve_page(entity_id)`. A `404`/`401`/rate‑limit‑exhausted is logged and
   acked `200 {"handled":"ignored","reason":"fetch_failed"}` — Notion retries
   non‑2xx, and a fetch miss is not worth a retry storm
   ([webhook.py:123‑196](../../../services/ingest/integrations/notion/webhook.py#L123-L196)).
3. **Shadow‑write** the page (with `_fyralis_workspace_id` injected to mirror the
   fetcher) via `shadow_write_raw(source="notion", ingress_kind="webhook")` onto
   the Notion‑scoped producer + S3 client at `app.state.notion_data_plane`:
   S3 PutIfAbsent → Kafka `ingestion.raw.notion` → normalizer
   (`("notion","webhook") → notion:object`) → observation_writer
   ([webhook.py:198‑283](../../../services/ingest/integrations/notion/webhook.py#L198-L283)).
   The producer is **flushed (≤10 s)** before the `200` so a gateway restart can't
   lose the event in librdkafka's local queue
   ([webhook.py:255‑269](../../../services/ingest/integrations/notion/webhook.py#L255-L269)).

The observation lands only once the tenant's `ingestion.kafka_path_enabled` flag
is on (the observation_writer full‑mode gate) — the same gate backfill lives
behind ([webhook.py:33‑39](../../../services/ingest/integrations/notion/webhook.py#L33-L39)).
The data plane is wired by `_wire_ingestion_data_plane` in the gateway; when
`KAFKA_BOOTSTRAP_SERVERS` is unset it is a no‑op and the handler reports
`shadow_write=false` ([main.py:529‑597](../../../services/app/gateway/main.py#L529-L597)).

> **`handle_notion_event` always returns `200`.** Every branch — unsupported
> entity, fetch failure, data‑plane unwired, flush incomplete — acks 200, because
> Notion retries non‑2xx and backfill/reconcile is the correctness backstop.

---

## 8. Reconciliation — gap detection

`reconcile_notion` ([reconcilers/notion.py:153‑190](../../../services/ingest/ingestion/reconcilers/notion.py#L153-L190))
re‑checks completed shards for new activity. After a shard completes, its cursor
carries `last_edited_at` (the high‑water of everything walked); the reconciler
probes the **live latest edit** for that shard's scope and compares
([reconcilers/notion.py:101‑150](../../../services/ingest/ingestion/reconcilers/notion.py#L101-L150)):

- `notion_database` → `latest_database_edit(db_id)` (newest row edit, one 1‑row
  descending query).
- `notion_page_tree` → `latest_page_edit()` (newest **loose** page edit; a 50‑row
  descending `/v1/search` that **skips database rows**, mirroring the page‑tree
  fetcher's coverage).

On `latest > high_water`, it re‑shares the shard at **`recency_score=1.5`** with
`gap_baseline_edited_at` recorded, so the re‑walk lands ahead of remaining
low‑recency backfill. `external_id` parity means re‑walked objects dedup against
what backfill already wrote — only genuine new/changed objects produce new
observations ([reconcilers/notion.py:139‑150](../../../services/ingest/ingestion/reconcilers/notion.py#L139-L150)).

**Two convergence guards (the IN‑14 runaway‑loop fixes):**

1. `latest_page_edit()` **excludes database‑row pages** (`parent.type ==
   "database_id"`). Without this, the probe returns a DB‑row timestamp the
   page‑tree walk never records as its high‑water → `latest > high_water` forever
   ([client.py:281‑311](../../../services/ingest/integrations/notion/client.py#L281-L311)).
2. `if high_water is None: return None` — a shard that walked **zero** objects (a
   workspace whose pages are all DB rows) has no reference point; re‑sharing it
   would re‑walk an empty scope forever. This mirrors the calendar reconciler's
   guard ([reconcilers/notion.py:107‑115](../../../services/ingest/ingestion/reconcilers/notion.py#L107-L115)).

These match the prior reconciler‑convergence findings (see the
`reconciler-convergence-bugs` note) — both are present in this tree.

> The channel mapping also reserves a **`("notion","poll")`** ingress for an
> incremental driver that re‑runs the same fetcher on a cadence
> ([channel_mapping.py:53‑72](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L53-L72));
> in the current tree the **reconciler** is the wired live‑gap mechanism. Both
> share the one fetcher + one handler, so `external_id` parity holds across them.

---

## 9. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  long-lived bot token (resolved once from secret_ref)            │
   VISIBLE DATABASES      │  planner: POST /v1/search?filter=database (fully paged)          │
                          │     └─► one notion_database shard per database                   │
   LOOSE PAGES            │  planner: + one notion_page_tree shard                           │
                          │  fetcher (work-stack, 1 API call/invocation):                    │
                          │     db_rows  → /v1/databases/{id}/query                          │
                          │     loose    → /v1/search?filter=page  (skip DB-row pages)        │
                          │     blocks   → /v1/blocks/{id}/children (depth cap)               │
                          │     comments → /v1/comments?block_id=                             │
                          │     └─► raw page|block|comment object (object field intact)       │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐│
   page changes ─────────►  thin Notion webhook ──HTTP POST──► /webhooks/notion           ││
                          │  one-time {verification_token} → 200 (unsigned, pre-verify)    ││
                          │  verify X-Notion-Signature (HMAC-SHA256, no ts)                ││
                          │  resolve workspace_id → tenant ; entity.type==page only        ││
                          │  GET /v1/pages/{id} (fetch-back) → shadow_write_raw(webhook)    ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_notion_object            │
                                                            │  branch on object field           │
                                                            │  external_id=notion:{object}:{id} │
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, webhook fetch‑back, and the
   reserved poll path all land on `notion:object` with
   `external_id="notion:{object}:{id}"`. A backfilled object and its live twin
   dedup to one observation. Object granularity is `content.object_type` + `kind`.
2. **One credential model.** A single long‑lived **bot token** per workspace reads
   everything — no per‑request mint, no per‑user tokens. The only inbound secret
   is the App‑level webhook verification token.
3. **Backfill is a resumable tree walk.** The cursor *is* a work stack; one Notion
   API call per fetcher invocation; `end_of_data` exactly when the stack empties.
4. **The live webhook is thin → fetch‑back.** Notion pushes only an entity id +
   type; the handler retrieves the full page and shadow‑writes it onto the same
   data plane (pages only in v1).
5. **Opaque‑cursor pagination, reactive‑only rate limiting.** `has_more`/
   `next_cursor` everywhere; `Retry-After` honoured on 429 within a bounded budget,
   with no proactive client‑side bucket.

---

## 10. Configuration & compliance

Verified against Notion's official docs (integration auth, API versioning,
pagination, rate limits, webhook signing).

### 10.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `NOTION_API_BASE_URL` | `https://api.notion.com` | outbound API base (set explicitly to Provider Lab in dev) |
| `NOTION_API_VERSION` | `2022-06-28` | pinned `Notion-Version` header |
| `NOTION_CLIENT_ID` / `NOTION_CLIENT_SECRET` | — (required for install) | OAuth app credentials |
| `NOTION_REDIRECT_URI` | — (required for install) | OAuth callback URL |
| `NOTION_WEBHOOK_VERIFICATION_TOKEN` (+ `…_PREV`) | — | App‑level HMAC secret(s) for webhook verification |
| `NOTION_BLOCK_DEPTH_CAP` | `3` | block‑tree recursion depth cap |
| `NOTION_RL_MAX_ATTEMPTS` | `4` | 429 retry budget |
| `NOTION_RL_MAX_SLEEP_SEC` | `30` | max sleep per `Retry-After` |

### 10.2 Verified compliant

- **Integration auth** — `Authorization: Bearer {bot_token}`, long‑lived, resolved
  once; token never logged (workspace id hashed in logs). ✅
- **API version pinned** — `Notion-Version: 2022-06-28` on every call. ✅
- **Pagination** — `next_cursor`/`has_more` opaque cursor everywhere; full
  enumeration loops to `has_more=false`. ✅
- **Rate limits** — `429 Retry-After` (integer seconds) honoured within a bounded
  budget; fetcher re‑queues on exhaustion (cursor preserved). ✅
- **Webhook signing** — HMAC‑SHA256 `X-Notion-Signature` over the raw body keyed by
  the App‑level `verification_token`, constant‑time compare, rotation overlap. ✅
- **Least secret surface** — bot token in `secret_ref` (outbound), App‑level
  verification token (inbound); two distinct secrets. ✅

### 10.3 Dev / Provider Lab mode

For local testing, `build_notion_client` detects `PROVIDER_LAB_URL` and
**presets** the bot token to
`spam-notion::{workspace_id}`, skipping secret‑store resolution; the base URL
is supplied explicitly as `NOTION_API_BASE_URL=<lab>/notion`. The Notion
adapter serves API version `2022-06-28` and authenticates a Bearer integration
token.
