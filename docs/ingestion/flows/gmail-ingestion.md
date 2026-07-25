# Gmail Ingestion — How Fyralis Pulls Gmail Data

This document explains, in detail, **how Gmail data enters Fyralis**: which
Google REST APIs are called, with which token, and how the Gmail signal —
**individual email messages across every mailbox in a Workspace domain** — is
ingested both historically (backfill) and in near‑real‑time (Pub/Sub push +
history poll).

It deliberately stops at the point where a Gmail message becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, thread‑canonicalization post‑processing, the Memory Fabric) is out
of scope.

Gmail is unusual among Fyralis sources in two ways, and both shape this whole
document:

1. **It is the only source whose live ingress is a Google Pub/Sub *push*, not a
   direct content webhook.** The push payload is a *notification*
   (`{emailAddress, historyId}`), **not** a message — so it has no direct
   handler. Live ingestion is a fetch‑on‑notify + poll model (§8).
2. **Its credential model is Domain‑Wide Delegation (DWD) with a service
   account**, not per‑user OAuth refresh tokens and not a per‑install bot token
   (§2).

---

## 1. The two ways data arrives

Gmail data reaches Fyralis through **two independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* the whole mailbox via the **Gmail REST API** (`users.messages.list` → `users.messages.get`) | [planners/gmail.py](../../../services/ingest/ingestion/planners/gmail.py), [fetchers/gmail.py](../../../services/ingest/ingestion/fetchers/gmail.py) |
| **Live (near‑real‑time)** | New mail in a watched mailbox | Google **Pub/Sub push** notifies Fyralis, which then *fetches* the new message resources via `users.history.list` → `users.messages.get`; a periodic **history poller** is the safety net | [gmail_pubsub.py](../../../services/app/webhooks/gmail_pubsub.py), [push_handler.py](../../../services/ingest/integrations/gmail/push_handler.py), [history_poller.py](../../../services/ingest/integrations/gmail/history_poller.py), [fetcher.py](../../../services/ingest/integrations/gmail/fetcher.py) |

Crucially, **both paths produce the exact same record shape** — a bare
"handler‑conformant Gmail record" wrapping a **Gmail API message resource** —
and both are parsed by the **single** `gmail:` handler
([handlers/gmail.py](../../../services/ingest/ingestion/handlers/gmail.py)).
Both derive the **same** dedup key:

```
external_id = "gmail:{gmail_installation_id}:{rfc5322_message_id}"
```

The key is composed by `idempotency.gmail_message`
([idempotency/__init__.py:44‑47](../../../services/ingest/ingestion/idempotency/__init__.py#L44-L47)).
Note the id is the RFC‑5322 **`Message-ID` header**, *not* the Gmail API
message id — and it is namespaced by `gmail_installation_id` so the same
Message‑ID seen by two tenants stays distinct. The handler reads it from the
message headers ([handlers/gmail.py:191‑195](../../../services/ingest/ingestion/handlers/gmail.py#L191-L195))
and a message with no `Message-ID` is rejected.

So a message that is both backfilled *and* delivered live collapses into **one**
observation. This is the central design invariant of Gmail ingestion — the
backfill fetcher and the live fetch path both build the **same** record envelope
([fetchers/gmail.py:212‑235](../../../services/ingest/ingestion/fetchers/gmail.py#L212-L235),
[fetcher.py:71‑77](../../../services/ingest/integrations/gmail/fetcher.py#L71-L77))
so the one handler treats both identically.

> Gmail uses **REST only** here (`gmail.googleapis.com/gmail/v1`). Real‑time is
> a **Pub/Sub push notification** that triggers a REST fetch; history is the
> **same REST API**. There is **no inbound content webhook** — Gmail is absent
> from the webhook signature `VERIFIERS` registry
> ([signatures/__init__.py:44‑68](../../../services/app/webhooks/signatures/__init__.py#L44-L68)),
> and the webhook router explicitly notes "Gmail enters via Pub/Sub, not this
> webhook router".

---

## 2. Authentication — Domain‑Wide Delegation (service account), not OAuth

Unlike Slack (per‑team/per‑user OAuth tokens) or GitHub (a per‑installation App
token), Gmail ingestion uses a **single credential model: a Google service
account with Domain‑Wide Delegation (DWD)**. There are **no per‑user OAuth
refresh tokens** and **no `code` exchange** — the user never bounces through
Google for consent. Instead, a Workspace **super‑admin** pre‑grants the service
account the right to impersonate users in their domain at admin‑chosen scopes
(Admin Console → Security → API controls → Domain‑Wide Delegation)
([dwd.py:1‑29](../../../services/ingest/integrations/gmail/dwd.py#L1-L29)).

### 2.1 The JWT‑bearer (impersonation) mint flow

At ingest time, the `DwdTokenMinter` mints a **per‑user, scope‑bound** bearer
token via the JWT‑bearer grant
([dwd.py:162‑248](../../../services/ingest/integrations/gmail/dwd.py#L162-L248)):

1. **Sign an RS256 JWT** with the service‑account private key
   ([dwd.py:205‑225](../../../services/ingest/integrations/gmail/dwd.py#L205-L225)).
   The payload is Google's contract: `iss=service_account_email`,
   `sub=impersonated_user_email`, `scope=<space‑separated scopes>`,
   `aud=https://oauth2.googleapis.com/token`, `iat`/`exp` (lifetime **50 min**,
   under Google's 1 h cap, [dwd.py:49‑50](../../../services/ingest/integrations/gmail/dwd.py#L49-L50)).
2. **Exchange** it at `POST https://oauth2.googleapis.com/token` with
   `grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion=<jwt>`; the
   response yields `{access_token, expires_in}`
   ([dwd.py:227‑248](../../../services/ingest/integrations/gmail/dwd.py#L227-L248)).
3. **In‑process cache.** Tokens are cached by
   `(service_account_email, user_email, frozenset(scopes))`, re‑minted **5 min**
   before expiry, guarded by a per‑key `asyncio.Lock` to prevent a mint
   stampede ([dwd.py:174‑195](../../../services/ingest/integrations/gmail/dwd.py#L174-L195)).
   Tokens live in memory only — never persisted to disk or DB.
4. **Read calls** then carry `Authorization: Bearer {token}`. On a downstream
   `401`, the HTTP client invalidates the cached token and retries **once**
   ([client.py:110‑113](../../../services/ingest/integrations/gmail/client.py#L110-L113),
   [dwd.py:197‑200](../../../services/ingest/integrations/gmail/dwd.py#L197-L200)).

The service‑account key is loaded **once per process** and **never logged** (the
token‑exchange error path deliberately omits the assertion, which carries the
impersonated identity, [dwd.py:235‑242](../../../services/ingest/integrations/gmail/dwd.py#L235-L242)).
Every code path that needs a token reaches for the process‑wide singleton
`get_minter()` ([dwd.py:277‑281](../../../services/ingest/integrations/gmail/dwd.py#L277-L281)).

### 2.2 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| Service‑account JSON | `GMAIL_SERVICE_ACCOUNT_JSON_FILE` (path) **or** `GMAIL_SERVICE_ACCOUNT_JSON` (inline) | exactly one; file is expected to be a KMS‑mounted secret in prod ([dwd.py:81‑121](../../../services/ingest/integrations/gmail/dwd.py#L81-L121)) |
| DWD client ID | `GMAIL_SERVICE_ACCOUNT_CLIENT_ID` | numeric; surfaced to admins so they can authorize scopes in their Admin Console ([oauth.py:412‑415](../../../services/ingest/integrations/gmail/oauth.py#L412-L415)) |
| Pub/Sub push OIDC audience | `GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE` (or `…_ENDPOINT`) | expected `aud` of the push OIDC token ([gmail_pubsub.py:128‑131](../../../services/app/webhooks/gmail_pubsub.py#L128-L131)) |
| Pub/Sub push SA email | `GMAIL_PUBSUB_PUSH_OIDC_SA` | expected `email` claim — proves the push came from our subscription ([gmail_pubsub.py:134‑135](../../../services/app/webhooks/gmail_pubsub.py#L134-L135)) |

> **Contrast with Slack/GitHub.** There is **no per‑install secret in the secret
> store** for Gmail and **no `provider_installations.secret_ref`** — auth is the
> single service‑account key plus the admin's DWD grant. The
> `gmail_installations` row stores only `service_account_email`, `scope`
> (`gmail.metadata` | `gmail.readonly`), `workspace_domain`, and the
> `inclusion_spec` ([oauth.py:169‑186](../../../services/ingest/integrations/gmail/oauth.py#L169-L186)).

### 2.3 The connect wizard (how an install gets registered)

`services/ingest/integrations/gmail/oauth.py` implements a **first‑party admin
connect wizard** (not an OAuth redirect dance — there is no state token because
there is no consent bounce, [oauth.py:1‑31](../../../services/ingest/integrations/gmail/oauth.py#L1-L31)):

1. **`POST /integrations/gmail/connect/preflight`** (Bearer‑authed) — impersonate
   the admin at directory scopes and enumerate domain users/groups/org‑units for
   the selector UI. **If the DWD grant is missing/mis‑scoped**, it returns a
   structured `dwd_grant_invalid` error carrying the **exact client ID + scope
   strings** to paste into the Admin Console
   ([oauth.py:90‑149](../../../services/ingest/integrations/gmail/oauth.py#L90-L149)).
2. **`POST /integrations/gmail/connect/finalize`** — in **one transaction**,
   upsert a `gmail_installations` row (idempotent on
   `(tenant_id, workspace_domain)`), write a `gmail.install` audit row, and emit
   an `onboarding_triggers` row (`ON CONFLICT DO NOTHING` so reinstalls/refreshes
   produce at most one trigger) — this is the F4 retrofit that fires the M6
   backfill chain on every completed install
   ([oauth.py:152‑218](../../../services/ingest/integrations/gmail/oauth.py#L152-L218)).
3. A **background task** then provisions out‑of‑band: provision the tenant's
   Pub/Sub topic + subscription, `resolve_inclusion` (expand the inclusion spec
   into concrete mailbox emails, minus opt‑outs), upsert a `pending` watch per
   mailbox, then call `users.watch` per mailbox to activate. Every step is
   idempotent and the `watch_scheduler` reconciles partial failures
   ([oauth.py:315‑409](../../../services/ingest/integrations/gmail/oauth.py#L315-L409)).

---

## 3. The Gmail REST API surface that is actually called

All read calls funnel through `GoogleHttpClient.request`
([client.py:88‑115](../../../services/ingest/integrations/gmail/client.py#L88-L115)),
which:

- asks the `DwdTokenMinter` for an impersonated bearer token per call (passing
  the `user_email` + scope to impersonate),
- sets `Authorization: Bearer {token}` + `Accept: application/json`,
- on **`401`** (attempt 1) invalidates the cached token and retries once,
- maps non‑2xx to typed errors via `_raise_for_error`
  ([client.py:156‑189](../../../services/ingest/integrations/gmail/client.py#L156-L189)):
  `429` / `5xx` (and `403` with reason `quotaExceeded` / `userRateLimitExceeded`
  / `rateLimitExceeded`) → `GoogleRateLimited` (carrying `Retry-After`); any
  other non‑2xx → `GoogleApiError`. Request bodies/headers are **never logged**
  (they carry bearer tokens, [client.py:185](../../../services/ingest/integrations/gmail/client.py#L185)).

The endpoints invoked for ingestion (base URL resolved via
`lib.integrations.endpoints.endpoint("gmail_api")` so it can be pointed at a
Provider Lab, [client.py:201‑204](../../../services/ingest/integrations/gmail/client.py#L201-L204)):

| Gmail endpoint | Wrapper | Purpose | Code |
|----------------|---------|---------|------|
| `GET users/me/messages` | `GmailClient.messages_list()` | list message stubs (`{id, threadId}`) — backfill paging | [client.py:233‑267](../../../services/ingest/integrations/gmail/client.py#L233-L267) |
| `GET users/me/messages/{id}` | `GmailClient.get_message()` | hydrate one message resource | [client.py:292‑307](../../../services/ingest/integrations/gmail/client.py#L292-L307) |
| `GET users/me/history` | `GmailClient.history_list()` | `messageAdded` deltas from a `startHistoryId` — live + gap‑fill | [client.py:269‑290](../../../services/ingest/integrations/gmail/client.py#L269-L290) |
| `GET users/me/profile` | `GmailClient.get_profile()` | read the mailbox's current `historyId` (watermark / gap probe) | [client.py:309‑315](../../../services/ingest/integrations/gmail/client.py#L309-L315) |
| `POST users/me/watch` | `GmailClient.watch()` | register the mailbox to publish to the Pub/Sub topic | [client.py:206‑223](../../../services/ingest/integrations/gmail/client.py#L206-L223) |
| `POST users/me/stop` | `GmailClient.stop()` | drop the watch (pause / opt‑out) | [client.py:225‑231](../../../services/ingest/integrations/gmail/client.py#L225-L231) |

The `DirectoryClient` (Admin Directory API: `users`, `groups`,
`groups/{key}/members`, `orgunits`) is used at **install / provision time** to
enumerate and resolve the mailbox inclusion list, **not** during steady‑state
ingestion ([client.py:323‑424](../../../services/ingest/integrations/gmail/client.py#L323-L424)).

### 3.1 The metadata‑vs‑full scope toggle

`get_message` requests `format=full` only when the install scope is
`gmail.readonly`; under `gmail.metadata` it requests `format=metadata`
(headers only) ([client.py:300](../../../services/ingest/integrations/gmail/client.py#L300)).
The handler mirrors this: it only attempts plain‑text body extraction when
`scope_used == "gmail.readonly"`
([handlers/gmail.py:205‑207](../../../services/ingest/ingestion/handlers/gmail.py#L205-L207)).

### 3.2 Pagination — `pageToken`

Every list endpoint pages the same way: pass `pageToken`, read `nextPageToken`,
stop when it's absent. `messages_list` returns **one page** of stubs plus its
`nextPageToken` to the fetcher, which persists it in the shard cursor and
resumes next invocation ([client.py:256‑267](../../../services/ingest/integrations/gmail/client.py#L256-L267));
`history_list` pages the same way over `messageAdded` events
([client.py:278‑290](../../../services/ingest/integrations/gmail/client.py#L278-L290)).

### 3.3 Rate limits

A single client‑side token bucket models Gmail's **per‑user quota**
([rate_limit/buckets.py:89](../../../services/ingest/ingestion/rate_limit/buckets.py#L89)):

```
("gmail", "per-user"): capacity 200, refill 200.0/s
```

On top of the bucket, the backfill fetcher/reconciler wrap each network call in
`retry_with_backoff_on_429(retry_on=GoogleRateLimited)` so a Gmail 429 / 403‑quota
response backs off rather than failing the shard
([fetchers/gmail.py:318‑326](../../../services/ingest/ingestion/fetchers/gmail.py#L318-L326),
[97‑102 doc](../../../services/ingest/ingestion/fetchers/gmail.py#L93-L106)).

---

## 4. Backfill scope — the shard families

The planner decomposes one Gmail install into **one shard per ACTIVE mailbox**,
all of `shard_kind = "gmail_mailbox_window"`
([planners/gmail.py:111‑145](../../../services/ingest/ingestion/planners/gmail.py#L111-L145)).
The planner is stateless: it does **no DB/API I/O at plan time** — the active
mailbox list is JSON‑aggregated onto `ctx.install["mailboxes"]` by M6.2a's
install loader (a `LEFT JOIN` against `gmail_mailbox_watches WHERE state='active'`),
and the planner just orjson‑decodes it
([planners/gmail.py:19‑39](../../../services/ingest/ingestion/planners/gmail.py#L19-L39),
[92‑108](../../../services/ingest/ingestion/planners/gmail.py#L92-L108)). Mailboxes in
`pending`/`paused`/`opted_out`/`errored` state are excluded by the loader.

There are **two** Gmail `shard_kind`s, but only the first is planner‑created:

| `shard_kind` | Created by | Gmail API | Fetch path |
|--------------|------------|-----------|------------|
| `gmail_mailbox_window` | the **planner** (this file) | `users.messages.list` → `users.messages.get` | `_fetch_page_mailbox_window` |
| `gmail_history_gap` | the **reconciler** (§9), not the planner | `users.history.list` → `users.messages.get` | `_fetch_page_history_gap` |

Each `gmail_mailbox_window` shard carries `mailbox_email`, the Gmail
`user_id`, and the install‑time `initial_history_id` (a watermark reference for
the reconciler; may be `NULL` if the watch was still `pending` at plan time), at
a baseline `recency_score=1.0`
([planners/gmail.py:119‑145](../../../services/ingest/ingestion/planners/gmail.py#L119-L145)).
If no mailboxes are active the planner returns an empty list (a clean run with
`pass_count=0`).

---

## 5. Backfill fetch — `gmail_mailbox_window` (full‑mailbox scan)

The fetcher dispatches on `shard_identifier["shard_kind"]`
([fetchers/gmail.py:272‑297](../../../services/ingest/ingestion/fetchers/gmail.py#L272-L297));
for the planner's shard it runs `_fetch_page_mailbox_window`
([fetchers/gmail.py:303‑399](../../../services/ingest/ingestion/fetchers/gmail.py#L303-L399)),
which fetches **one page** per call (the N1 invariant — ShardFetch owns the
publish‑then‑persist round, [fetchers/gmail.py:50‑67](../../../services/ingest/ingestion/fetchers/gmail.py#L50-L67)).

### 5.1 Cursor

```python
class GmailCursor(BaseModel):
    page_token: str | None = None        # nextPageToken (messages.list or history.list)
    messages_seen: int = 0               # diagnostic running count
    final_history_id: str | None = None  # stamped on the LAST page via users.getProfile
    start_history_id: str | None = None  # gap shards only — range lower bound
    end_history_id: str | None = None    # gap shards only — range upper bound
```

([fetchers/gmail.py:161‑180](../../../services/ingest/ingestion/fetchers/gmail.py#L161-L180)).

### 5.2 The message fan‑out

`messages.list` returns only stubs (`{id, threadId}`), so each page **fans out**:
for every stub, `get_message` hydrates the full message resource
([fetchers/gmail.py:328‑368](../../../services/ingest/ingestion/fetchers/gmail.py#L328-L368)).
An individual `get_message` failure (e.g. a deleted‑then‑listed message returns
404) is logged and **skipped** — the page still advances — so one bad message
never fails the shard ([fetchers/gmail.py:346‑359](../../../services/ingest/ingestion/fetchers/gmail.py#L346-L359)).

### 5.3 The watermark stamp

On the **last** page (no `nextPageToken`), the fetcher calls `users.getProfile`
and stamps the mailbox's current `historyId` into `cursor.final_history_id`
([fetchers/gmail.py:380‑391](../../../services/ingest/ingestion/fetchers/gmail.py#L380-L391)).
This is the reconciler's reference point for gap detection (§9); without it the
reconciler can't tell whether mail arrived between the last list call and
reconciliation. It is `NULL` until the last page — reading it before
`end_of_data` is incorrect.

### 5.4 Record envelope

Each hydrated message becomes a record via `_build_record`
([fetchers/gmail.py:212‑235](../../../services/ingest/ingestion/fetchers/gmail.py#L212-L235)):

```python
{
  "message_resource": <Gmail API message resource>,
  "mailbox_email": "alice@acme.com",
  "scope_used": "gmail.metadata" | "gmail.readonly",
  "gmail_installation_id": "<uuid>",
  "read_path": "poll",      # <-- normalised, see below
}
```

The genuine producing path (`"backfill"` vs `"reconciliation_gap"`) is passed in
for readability but is **normalised to `"poll"`** before it reaches the record,
because the `gmail:` handler only accepts `read_path in ("push","poll")` and the
backfill‑vs‑gap distinction must **not** affect `external_id`
([fetchers/gmail.py:202‑235](../../../services/ingest/ingestion/fetchers/gmail.py#L202-L235)).
This is what keeps the dedup key identical to the live‑poll twin (§8).

---

## 6. The handler — shaping a message into `ObservationDraft`

`handle_gmail` ([handlers/gmail.py:164‑252](../../../services/ingest/ingestion/handlers/gmail.py#L164-L252))
is registered via `@register("gmail:")` and is the **single** registered handler
for the `gmail:` channel ([handlers/gmail.py:61‑65](../../../services/ingest/ingestion/handlers/gmail.py#L61-L65),
[164](../../../services/ingest/ingestion/handlers/gmail.py#L164)).

> **`gmail:` vs `email:inbound`.** A separate handler module
> `handlers/email.py` owns the `email:inbound` channel (Postmark/SendGrid
> inbound webhooks) — a **different** channel and dedup namespace. Gmail
> ingestion never touches it. The registry confirms both as distinct
> `attested_agent` channels
> ([handlers/__init__.py:43‑44](../../../services/ingest/ingestion/handlers/__init__.py#L43-L44)).

The handler validates the envelope (`message_resource`, `mailbox_email`,
`scope_used ∈ {metadata,readonly}`, `read_path ∈ {push,poll}`,
`gmail_installation_id`), then derives:

| Field | Value | Source |
|-------|-------|--------|
| `source_channel` | `gmail:` | constant |
| `external_id` | `gmail:{install}:{message_id}` | RFC‑5322 `Message-ID` header (required) ([191‑195](../../../services/ingest/ingestion/handlers/gmail.py#L191-L195), [246](../../../services/ingest/ingestion/handlers/gmail.py#L246)) |
| `occurred_at` | message `internalDate` (ms epoch → UTC) | [113‑118](../../../services/ingest/ingestion/handlers/gmail.py#L113-L118), [247](../../../services/ingest/ingestion/handlers/gmail.py#L247) |
| `content_text` | `Subject: …` + snippet + (readonly only) truncated body | [145‑161](../../../services/ingest/ingestion/handlers/gmail.py#L145-L161) |
| `source_actor_ref` | `email:{from_email}` (parsed from `From`), or `None` | [197‑204](../../../services/ingest/ingestion/handlers/gmail.py#L197-L204), [248](../../../services/ingest/ingestion/handlers/gmail.py#L248) |
| `entities_hint` | `{kind:"email"}` for each To/Cc/From address | [234‑238](../../../services/ingest/ingestion/handlers/gmail.py#L234-L238) |
| `kind` | `signal` (the `ObservationDraft` default) | [handlers/__init__.py:99](../../../services/ingest/ingestion/handlers/__init__.py#L99) |
| `trust_tier` | **`attested_agent`** | [handlers/gmail.py:65](../../../services/ingest/ingestion/handlers/gmail.py#L65) |

`content` also stashes `thread_id_gmail`, labels, `internal_date_ms`, recipients,
and a `_gmail_thread_canonical_id` surfaced by the inline dispatcher (§8.2).

---

## 7. Reconciliation — gap detection (`gmail_history_gap`)

After all `gmail_mailbox_window` shards complete, `reconcile_gmail`
([reconcilers/gmail.py:333‑388](../../../services/ingest/ingestion/reconcilers/gmail.py#L333-L388))
decides CLEAN vs RE‑SHARE **per mailbox**. For each `done` shard
(`reconciliation_resharded` and `failed` shards are skipped,
[reconcilers/gmail.py:343‑345](../../../services/ingest/ingestion/reconcilers/gmail.py#L343-L345)):

1. Read the shard's `final_history_id` from its N1 cursor in `workflow_states`
   ([reconcilers/gmail.py:206‑223](../../../services/ingest/ingestion/reconcilers/gmail.py#L206-L223)).
2. Call `users.getProfile` **now** to read the mailbox's **current** `historyId`
   ([reconcilers/gmail.py:268‑288](../../../services/ingest/ingestion/reconcilers/gmail.py#L268-L288)).
3. If `current > final` (numeric compare), a gap exists → emit one
   `gmail_history_gap` shard for the range `[final, current]`, marked as a child
   via `parent_shard_id`, at **`recency_score=1.5`**
   ([reconcilers/gmail.py:306‑327](../../../services/ingest/ingestion/reconcilers/gmail.py#L306-L327)).

The gap shard is then drained by `_fetch_page_history_gap`
([fetchers/gmail.py:405‑517](../../../services/ingest/ingestion/fetchers/gmail.py#L405-L517)),
which pages `users.history.list` from `start_history_id`, extracts
`messagesAdded` ids, hydrates each via `get_message`, and terminates when there's
no `nextPageToken` **or** the response `historyId` ≥ `end_history_id`
([fetchers/gmail.py:492‑499](../../../services/ingest/ingestion/fetchers/gmail.py#L492-L499)).

**NULL handling.** If a shard's `final_history_id` is `NULL` (the fetcher
end‑of‑data'd on the first page without `getProfile`, or the watch was `pending`
at plan time), there's no reference point, so the reconciler conservatively
returns `has_gaps=False` for that mailbox — a documented limitation
([reconcilers/gmail.py:54‑65](../../../services/ingest/ingestion/reconcilers/gmail.py#L54-L65),
[255‑258](../../../services/ingest/ingestion/reconcilers/gmail.py#L255-L258)). A failed
`getProfile` is likewise treated as "can't determine gap" → clean for this pass
([reconcilers/gmail.py:275‑286](../../../services/ingest/ingestion/reconcilers/gmail.py#L275-L286)).

---

## 8. Live (near‑real‑time) ingestion — Pub/Sub push + history poll

Gmail's live path is the one that breaks the "webhook → handler" shape every
other source follows. **The Pub/Sub notification has no direct handler.**

### 8.1 The notification ingress (`/webhooks/gmail/pubsub`)

Google Pub/Sub delivers a push envelope to a **dedicated** endpoint
([gmail_pubsub.py:1‑25](../../../services/app/webhooks/gmail_pubsub.py#L1-L25)),
mounted separately from the generic `/webhooks/{provider}` router
([ceo_view_wiring.py:142‑153](../../../services/app/gateway/ceo_view_wiring.py#L142-L153)).
The body is a **notification**, not a message:

```
{ "message": { "data": "<base64 of {emailAddress, historyId}>", "messageId": ... },
  "subscription": "projects/.../subscriptions/gmail-{tenant}-sub" }
```

Verification is a **Google‑signed OIDC JWT** in the `Authorization: Bearer`
header (RS256 against Google's JWKS; `iss ∈ {accounts.google.com, …}`,
`aud == GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE`, `email == GMAIL_PUBSUB_PUSH_OIDC_SA`,
`email_verified == true`, `exp` with 60 s leeway)
([gmail_pubsub.py:176‑184](../../../services/app/webhooks/gmail_pubsub.py#L176-L184),
[google_oidc.py:113‑173](../../../services/app/webhooks/signatures/google_oidc.py#L113-L173)).
This is **not** an HMAC body signature (so Gmail is not in the `VERIFIERS`
registry). When the OIDC env is unset the route returns `503 not_configured`
(retryable) rather than verifying garbage
([gmail_pubsub.py:138‑170](../../../services/app/webhooks/gmail_pubsub.py#L138-L170)).
The endpoint **always returns 200** on transient failures so Pub/Sub doesn't
enter a retry storm — the history poller is the safety net
([gmail_pubsub.py:22‑24](../../../services/app/webhooks/gmail_pubsub.py#L22-L24)).

### 8.2 Fetch‑on‑notify — `handle_push` → `drain_mailbox_history`

Because the notification carries no content, `handle_push` resolves
`subscription_name → (tenant_id, gmail_installation_id)` from
`gmail_pubsub_topics`, then drains the mailbox
([push_handler.py:66‑143](../../../services/ingest/integrations/gmail/push_handler.py#L66-L143)).
The actual fetch is `drain_mailbox_history`
([fetcher.py:100‑318](../../../services/ingest/integrations/gmail/fetcher.py#L100-L318)),
shared verbatim by the **history poller** (the safety net that re‑drains each
active mailbox every ~10 min, [history_poller.py:1‑11](../../../services/ingest/integrations/gmail/history_poller.py#L1-L11),
[38‑70](../../../services/ingest/integrations/gmail/history_poller.py#L38-L70)). It:

1. Loads the mailbox's last‑known `history_id` + the install scope.
2. Pages `users.history.list(historyTypes=['messageAdded'])` collecting new ids
   ([fetcher.py:169‑193](../../../services/ingest/integrations/gmail/fetcher.py#L169-L193)).
3. For each new id: `get_message`, then **either** publish to `ingestion.raw`
   (cutover) **or** dispatch inline.
4. Advances the mailbox `history_id` + stamps `last_push_at`/`last_poll_at`
   ([fetcher.py:283‑310](../../../services/ingest/integrations/gmail/fetcher.py#L283-L310)).

### 8.3 The cutover — why the live message is published under `ingress_kind="poll"`

When the shadow deps are wired and `ingestion.kafka_path_enabled` is **not**
killed for the tenant (kafka‑first default), each fetched message is published
to `ingestion.raw` under **`ingress_kind="poll"`** instead of being ingested
inline ([fetcher.py:119‑142](../../../services/ingest/integrations/gmail/fetcher.py#L119-L142),
[214‑246](../../../services/ingest/integrations/gmail/fetcher.py#L214-L246)). The published
record is **byte‑shaped identically** to the M6.3 backfill fetcher's record
([fetcher.py:43‑97](../../../services/ingest/integrations/gmail/fetcher.py#L43-L97)),
so the normalizer dispatches it through the same `gmail:` handler and derives
the **same** `external_id`. A publish failure falls back to inline dispatch — the
message is **never dropped** ([fetcher.py:241‑246](../../../services/ingest/integrations/gmail/fetcher.py#L241-L246)).

The channel map encodes exactly this: **both** `("gmail","backfill")` and
`("gmail","poll")` resolve to `"gmail:"`, while the raw Pub/Sub notification
ingress (`ingress_kind="pubsub"`) is **intentionally absent** from the map —
"that payload is a notification … NOT a Gmail message resource, so it has no
direct handler"
([channel_mapping.py:39‑52](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L39-L52)).
The raw notification *is* shadow‑written for replay/debug under
`ingress_kind="pubsub"`, but that record is never normalized into an observation
([gmail_pubsub.py:54‑114](../../../services/app/webhooks/gmail_pubsub.py#L54-L114)).

The net effect: **a backfilled message and its live‑poll twin collapse to one
observation** via the identical `gmail:{install}:{message_id}` key. (The inline
dispatch path adds RFC‑5322 thread canonicalization before ingest and stamps
`observations.thread_canonical_id` afterward,
[handlers/gmail.py:260‑346](../../../services/ingest/ingestion/handlers/gmail.py#L260-L346);
the cutover path defers thread linkage to downstream.)

### 8.4 Watch lifecycle

A mailbox publishes to Pub/Sub only while it has an active `users.watch`. Watches
expire ~7 days out; `watch_scheduler` renews anything within 24 h of expiry
([watch_scheduler.py:1‑5](../../../services/ingest/integrations/gmail/watch_scheduler.py#L1-L5)).
`activate_watch` persists the returned `historyId` (the drain bookmark) +
expiration and flips the row to `active`; on error it stamps `errored` for the
scheduler to retry; `stop_watch` transitions to `paused`/`opted_out`
([watch.py:72‑184](../../../services/ingest/integrations/gmail/watch.py#L72-L184)).

---

## 9. Revocation / recoverable‑error behavior

There is **no dedicated revocation chokepoint** like GitHub's
`_maybe_disable_on_revocation`. (inferred) Because auth is a domain‑wide service
account rather than a per‑install token, a *single* mailbox's token‑mint `401`
is handled locally — `GoogleHttpClient.request` invalidates the cached token and
retries once ([client.py:110‑113](../../../services/ingest/integrations/gmail/client.py#L110-L113)) —
and persistent failures degrade gracefully rather than disabling the whole
install:

- **Backfill / reconcile:** `GoogleRateLimited` is retried with backoff; a
  `getProfile`/`get_message` `GoogleApiError` is logged and treated as "skip /
  no gap" rather than failing the run
  ([fetchers/gmail.py:346‑359](../../../services/ingest/ingestion/fetchers/gmail.py#L346-L359),
  [reconcilers/gmail.py:275‑286](../../../services/ingest/ingestion/reconcilers/gmail.py#L275-L286)).
- **Poller:** a mailbox accumulates `consecutive_poll_failures`; at **5** the
  watch row flips to `errored` (which excludes it from leasing) — a per‑mailbox
  circuit breaker, not an install‑wide one
  ([history_poller.py:41](../../../services/ingest/integrations/gmail/history_poller.py#L41),
  [95‑113](../../../services/ingest/integrations/gmail/history_poller.py#L95-L113)).
- **Whole‑install teardown** is operator‑driven: `POST /integrations/gmail/uninstall`
  stops every watch, tears down the Pub/Sub topic/subscription, and disables the
  install row (idempotent; RLS‑scoped)
  ([oauth.py:253‑274](../../../services/ingest/integrations/gmail/oauth.py#L253-L274)).

> **TODO(human):** confirm the intended behavior when the **DWD grant itself**
> is revoked in the Admin Console (every mint 401s for every mailbox). The code
> degrades per‑mailbox (each errors after 5 poll failures) but there is no
> single signal that disables the install or alerts the operator the way the
> GitHub `Bad credentials` chokepoint does. The *why* (deliberate, or a gap) is
> not stated in the code.

---

## 10. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
   DWD service account    │  sign RS256 JWT (iss=SA, sub=user) ─► POST oauth2/token          │
   (one key, no per-user  │     └─► per-(SA,user,scopes) bearer token (cached, 5m pre-expiry) │
    OAuth, no bot token)  │  planner: one gmail_mailbox_window shard per ACTIVE mailbox       │
                          │  fetcher: GET users/me/messages (stubs) → users/me/messages/{id}  │
                          │     └─► hydrate resource → record {message_resource, …, poll}     │
                          │  last page: GET users/me/profile → cursor.final_history_id        │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────────────── LIVE (push + poll) ───────────────────┐│
   new mail in a watched  │  Google Pub/Sub ──push {emailAddress, historyId}──► /webhooks/ ││
   mailbox                │     gmail/pubsub   (verify Google OIDC JWT; NO content handler)││
                          │  history poller (every ~10m)  ─── safety net ───┐              ││
                          │  → drain_mailbox_history: users/me/history (messageAdded)      ││
                          │       → users/me/messages/{id} → record {…, read_path}         ││
                          │       publish ingestion.raw ingress_kind="poll" (cutover) OR   ││
                          │       inline dispatch (thread-canonicalize + ingest)           ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_gmail  (@register gmail:)│
                                                            │  external_id =                    │
                                                            │   gmail:{install}:{Message-ID}    │
                                                            │  trust_tier = attested_agent      │
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, reconciliation gap‑fill, and
   live drain all build the same record envelope and land on `gmail:` with
   `external_id = gmail:{install}:{rfc5322_message_id}`. A backfilled message
   and its live twin collapse to one observation. The genuine path
   (backfill/gap/poll/push) is preserved in the cursor/shard, **not** in the
   dedup key.
2. **The Pub/Sub notification is NOT an observation.** Its payload is
   `{emailAddress, historyId}` with no message body, so it has no direct handler
   and `("gmail","pubsub")` is intentionally absent from the channel map. Live
   ingestion is **fetch‑on‑notify**: the notification triggers a
   `users.history.list` + `users.messages.get` fetch that publishes a real
   message under `ingress_kind="poll"`.
3. **One credential model: DWD service account.** A single service‑account key +
   the admin's domain‑wide grant mints per‑user, scope‑bound tokens on demand.
   No per‑user OAuth refresh tokens, no per‑install secret, no `secret_ref`.
4. **Two shard kinds.** `gmail_mailbox_window` (planner, full‑mailbox
   `messages.list`) and `gmail_history_gap` (reconciler, `history.list` over a
   `[final, current]` range at `recency_score=1.5`).
5. **`getProfile` watermark drives gap detection.** The last backfill page
   stamps `final_history_id`; the reconciler compares it against the mailbox's
   live `historyId`. A `NULL` watermark means "no reference point → clean".

---

## 11. Configuration & compliance

Verified against Google's official docs (DWD JWT‑bearer grant, Gmail
`users.{messages,history,watch,getProfile}`, Pub/Sub push OIDC).

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `GMAIL_SERVICE_ACCOUNT_JSON_FILE` / `GMAIL_SERVICE_ACCOUNT_JSON` | — (exactly one required) | service‑account key (file path preferred — KMS‑mounted) |
| `GMAIL_SERVICE_ACCOUNT_CLIENT_ID` | — | numeric DWD client id surfaced to admins for scope authorization |
| `GMAIL_PUBSUB_PUSH_OIDC_AUDIENCE` (or `GMAIL_PUBSUB_PUSH_ENDPOINT`) | — | expected `aud` of the push OIDC token; if unset the ingress returns `503 not_configured` |
| `GMAIL_PUBSUB_PUSH_OIDC_SA` | — | expected `email` claim of the push OIDC token |
| `GMAIL_API_BASE_URL` / `GOOGLE_DIRECTORY_BASE_URL` | Google prod | explicit Gmail / Directory base override ([endpoints.py](../../../lib/integrations/endpoints.py)) |
| `GOOGLE_TOKEN_URI` | Google OAuth | explicit DWD token endpoint override |
| `PROVIDER_LAB_URL` | unset | test-only Provider Lab origin; does not override production endpoints by itself |
| `ingestion.kafka_path_enabled` (tenant flag, not env) | `TRUE` (kafka‑first) | kill‑switch: `FALSE` forces the live drain to ingest inline instead of publishing to `ingestion.raw` ([fetcher.py:134‑142](../../../services/ingest/integrations/gmail/fetcher.py#L134-L142)) |

### 11.2 Verified compliant

- **DWD auth** — RS256 JWT (`iss=SA`, `sub=user`, `scope`, `aud=token uri`,
  ≤ 1 h lifetime) → JWT‑bearer grant; token cached + re‑minted pre‑expiry; key
  never logged. ✅
- **Pub/Sub push OIDC** — RS256 against Google JWKS; issuer/audience/email/
  email_verified/exp all checked; forced JWKS refresh on unknown `kid`. ✅
- **Least‑privilege scope** — `gmail.metadata` (headers only) vs `gmail.readonly`
  (body) drives both the `get_message` `format` and the handler's body
  extraction. ✅
- **Pagination** — `pageToken`/`nextPageToken` on `messages.list` +
  `history.list`. ✅
- **Rate‑limit etiquette** — `429`/`403‑quota` → `GoogleRateLimited` with
  `Retry-After`, wrapped in backoff; per‑user bucket cap 200, refill 200/s. ✅
- **No secret sprawl** — single service‑account key + admin DWD grant; no
  per‑install token, no `secret_ref`. ✅

### 11.3 Dev / Provider Lab mode

Gmail does **not** have a `build_*` entry in
`services/ingest/ingestion/fetchers/_clients.py` (unlike Slack/GitHub/Notion/
finance sources) — it builds its own client via `get_minter()` +
`GoogleHttpClient` in the fetcher/reconciler `_open_gmail_client` hooks
([fetchers/gmail.py:256‑266](../../../services/ingest/ingestion/fetchers/gmail.py#L256-L266)).
So there is **no `spam-gmail::…` token preseed** in `_clients.py`. A lab run
sets the three explicit overrides `GMAIL_API_BASE_URL`,
`GOOGLE_DIRECTORY_BASE_URL`, and `GOOGLE_TOKEN_URI`; the helper
`provider_lab_endpoint_overrides()` produces all three from
`PROVIDER_LAB_URL`. This keeps the Gmail and DWD token calls on the loopback
lab without adding a single-host fallback to the production resolver.
