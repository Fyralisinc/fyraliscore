# Miro Ingestion — How Fyralis Pulls Miro Data

This document explains, in detail, **how Miro data enters Fyralis**: which Miro
REST APIs are called, with which token, and how the one Miro signal —
**board items** (sticky notes, shapes, frames, cards, text, connectors) — is
ingested off a collaborative whiteboard.

It deliberately stops at the point where a Miro board item becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

> **Archetype + verification status.** Miro is a clone of the **Brex Bearer-token
> archetype** ([integrations/miro/__init__.py](../../../services/ingest/integrations/miro/__init__.py)).
> Several external-API details were cloned and are **UNVERIFIED**; they are
> reproduced as `TODO(human)` callouts below rather than asserted. The single
> most consequential verified fact: **Miro discontinued its experimental webhooks
> on 2025-12-05** ([signatures/miro.py:4‑17](../../../services/app/webhooks/signatures/miro.py#L4-L17)),
> so the "live push" path described in stale comments **does not exist in
> production** — the webhook edge that is wired is a *synthetic gate stand-in*,
> and the real live story is **poll-only**. This doc reports the code as written
> and flags every place where a comment contradicts it.

---

## 1. The ways data arrives

The Miro *code* contemplates **three** ingress kinds — but only two are real:

| Path | Trigger | Mechanism | Real? | Code |
|------|---------|-----------|-------|------|
| **Backfill (historical)** | Onboarding (`onboarding_triggers source='miro'`) | Fyralis *pulls* board items via the Miro **REST API** (`GET /boards/{id}/items`, opaque cursor) | ✅ | [planners/miro.py](../../../services/ingest/ingestion/planners/miro.py), [fetchers/miro.py](../../../services/ingest/ingestion/fetchers/miro.py) |
| **Poll (incremental)** | Reconciliation cadence | Same fetcher, **warm-started** from the per-board item high-water cursor (`ingress_kind="poll"`) | ✅ | [fetchers/miro.py:8‑39](../../../services/ingest/ingestion/fetchers/miro.py#L8-L39), [reconcilers/miro.py](../../../services/ingest/ingestion/reconcilers/miro.py) |
| **Webhook (live push)** | New / changed item | Miro *would* POST `board_item.created/updated/deleted` to the webhook edge | ⚠️ **synthetic gate only** — Miro killed webhooks 2025-12-05 | [signatures/miro.py](../../../services/app/webhooks/signatures/miro.py), [handlers/miro.py:239‑257](../../../services/ingest/ingestion/handlers/miro.py#L239-L257) |

All three converge on the **single** `miro:item` handler
([handlers/miro.py:232‑270](../../../services/ingest/ingestion/handlers/miro.py#L232-L270)).
The channel map makes this explicit — every ingress kind routes to one channel
([normalizer/channel_mapping.py:232‑234](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L232-L234)):

```python
("miro", "backfill"): "miro:item",
("miro", "poll"):     "miro:item",
("miro", "webhook"):  "miro:item",
```

All paths derive the **same** dedup key, **versioned** because a board item
mutates (a sticky note's text/position is edited in place)
([idempotency/__init__.py:257‑261](../../../services/ingest/ingestion/idempotency/__init__.py#L257-L261)):

```
external_id = "miro:{org_id}:item:{item_id}:{version}"
```

So a backfilled item and its (synthetic) live twin at the **same version**
collapse into **one** observation; a *new version* of the same item lands as a
**new** observation (the edit re-observes rather than silently dedup'ing). This
is the central design invariant of Miro ingestion.

> ⚠️ **Stale comment.** The `channel_mapping` block above still narrates a live
> "HMAC-signed webhooks (`board_item.created/updated/deleted`)" surface
> ([channel_mapping.py:223‑231](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L223-L231)).
> That comment, the `__init__.py` summary, and the router-map comments
> ([router.py:143‑147](../../../services/app/webhooks/router.py#L143-L147)) are
> **contradicted** by the verified changelog note in
> [signatures/miro.py](../../../services/app/webhooks/signatures/miro.py) and the
> fetcher header ([fetchers/miro.py:37‑38](../../../services/ingest/ingestion/fetchers/miro.py#L37-L38)):
> Miro has no live webhook in production. The "webhook" row exists to keep the
> ingress gate exercising the `webhook → 202 → handler` plumbing.

---

## 2. Authentication & token model

Miro authenticates with a **single credential: a long-lived, org-level app
Bearer token** ([client.py:1‑31](../../../services/ingest/integrations/miro/client.py#L1-L31),
[0102_miro.sql:3‑6](../../../db/migrations/0102_miro.sql#L3-L6)). This is the
Brex/Jira posture, **not** a per-user OAuth code-exchange flow:

- The token is resolved **once** from the secret store (or preset in Provider Lab
  mode) and reused for the life of the client — there is **no refresh**
  ([client.py:109‑135](../../../services/ingest/integrations/miro/client.py#L109-L135)).
- Read calls carry `Authorization: Bearer {token}`; the token and the auth
  header are **never logged**
  ([client.py:133‑135](../../../services/ingest/integrations/miro/client.py#L133-L135), [30‑31](../../../services/ingest/integrations/miro/client.py#L30-L31)).

### 2.1 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| API Bearer token | `encrypted_secrets`, label `miro_api_token:{base_url}` | only an opaque `secret_ref` reaches `miro_installations.secret_ref` ([oauth.py:185‑187](../../../services/ingest/integrations/miro/oauth.py#L185-L187)) |
| Webhook signing secret | `encrypted_secrets`, label `miro_webhook_secret:{base_url}` | **optional**; ref stored on `miro_installations.webhook_secret_ref` and (if an org id is also given) on the `provider_installations` row ([oauth.py:188‑216](../../../services/ingest/integrations/miro/oauth.py#L188-L216)) |
| Org id | `miro_installations.org_id` | namespaces every `external_id`; also the webhook tenant key ([0102_miro.sql:49‑52](../../../db/migrations/0102_miro.sql#L49-L52)) |
| API base URL | `miro_installations.base_url` | per-install override; default resolves via `endpoint("miro_api")` |

> **No code-exchange OAuth.** Despite the file named `oauth.py`, there is **no
> authorization-code redirect, no token endpoint call, and no refresh-token
> handling** in the implementation. `oauth.py` is an **admin connect wizard**:
> the operator pastes an already-minted API token, Fyralis verifies it, and
> persists it ([oauth.py:1‑31](../../../services/ingest/integrations/miro/oauth.py#L1-L31)).

> **(inferred)** A `CONFIRMED` comment in
> [oauth.py:56‑59](../../../services/ingest/integrations/miro/oauth.py#L56-L59)
> and [endpoints.py:85‑87](../../../lib/integrations/endpoints.py#L85-L87) notes
> that real Miro *does* offer OAuth2 with `authorization_code` + `refresh_token`
> grants (access 60 min, refresh 60 days) and a `boards:read` scope. The code
> does **not** implement that — it treats the token as long-lived. This is a
> recognized gap, not a claim that no-refresh is correct against the live API.

### 2.2 The connect/install flow

[`oauth.py`](../../../services/ingest/integrations/miro/oauth.py) exposes two
Bearer-authed admin routes:

1. **`POST /integrations/miro/connect/preflight`** — verifies the pasted
   `api_token` by calling `MiroClient.list_boards()` and returns the boards for
   the selector UI. On auth failure it returns a structured `400` and **stores no
   secret** ([oauth.py:128‑148](../../../services/ingest/integrations/miro/oauth.py#L128-L148)).
2. **`POST /integrations/miro/connect/finalize`** — re-verifies creds *before any
   write*, resolves the board set (all enumerated, or the requested `board_ids`
   subset), persists the token (+ optional webhook secret), then calls
   `finalize_install()` ([oauth.py:151‑229](../../../services/ingest/integrations/miro/oauth.py#L151-L229)).

`finalize_install()` ([onboarding.py:36‑125](../../../services/ingest/integrations/miro/onboarding.py#L36-L125))
does, in **one tenant-scoped transaction**:

- UPSERT `miro_installations` keyed on `(tenant_id, base_url)` (re-finalize
  clears `disabled_at`);
- INSERT one `miro_boards` row per board (UPSERT on `(miro_installation_id, board_id)`);
- emit an `onboarding_triggers` row `source='miro', trigger_kind='install'` so the
  existing **M6 backfill chain** (`oauth_poller → tenant_onboarding →
  source_onboarding → shard_fetch → reconciler`) fires.

Then, **only if both a webhook secret and an org id were supplied**,
`register_webhook_installation()` seeds a `provider_installations` row
(`provider='miro', installation_id=org_id, enabled=TRUE`) for the webhook edge
([onboarding.py:128‑150](../../../services/ingest/integrations/miro/onboarding.py#L128-L150)).
Backfill (`miro_installations`) and the live edge (`provider_installations`) are
**seeded together but stay independent**.

---

## 3. The Miro REST API surface that is actually called

All read calls funnel through `MiroClient._request`
([client.py:137‑197](../../../services/ingest/integrations/miro/client.py#L137-L197)),
which:

- sets `Authorization: Bearer {token}` and `Accept: application/json`,
- retries **`429`** within a bounded budget honouring `Retry-After`
  (`MIRO_RL_MAX_ATTEMPTS`=4, `MIRO_RL_MAX_SLEEP_SEC`=30),
- lets transport errors and any non-2xx map to a typed `MiroApiError`
  ([client.py:264‑295](../../../services/ingest/integrations/miro/client.py#L264-L295)).

The endpoints invoked for ingestion:

| Miro endpoint | Wrapper | Purpose | Code |
|---------------|---------|---------|------|
| `GET /boards` | `list_boards()` | enumerate all boards visible to the org token (seed/install time) | [client.py:203‑216](../../../services/ingest/integrations/miro/client.py#L203-L216) |
| `GET /boards/{id}` | `get_board()` | single-board metadata probe (not on the hot path) | [client.py:218‑220](../../../services/ingest/integrations/miro/client.py#L218-L220) |
| `GET /boards/{id}/items` | `list_items()` | one **cursor-paged** page of board items (backfill + poll + reconciler probe) | [client.py:222‑250](../../../services/ingest/integrations/miro/client.py#L222-L250) |

> **TODO(human): confirm Miro API host + read endpoints/scopes.** The read
> surface (`/boards`, `/boards/{id}`, `/boards/{id}/items`) and page cap (50) are
> **CLONED from Brex and UNVERIFIED**
> ([client.py:9‑16](../../../services/ingest/integrations/miro/client.py#L9-L16), [49‑52](../../../services/ingest/integrations/miro/client.py#L49-L52)).
> A later `CONFIRMED` note in the fetcher header
> ([fetchers/miro.py:31‑38](../../../services/ingest/ingestion/fetchers/miro.py#L31-L38))
> states the real paths are `GET /v2/boards/{id}/items` (cursor, `limit` 10–50)
> and `GET /v2/boards` (**offset**-paginated, a *different* paginator) under scope
> `boards:read` — so the client's "everything is the same paginator" assumption
> needs the verified split applied.

### 3.1 Pagination — opaque cursor

`list_items` returns `(items, next_cursor, total)`; **`next_cursor is None` is
terminal**. The cursor is whatever opaque string Miro returns under the response
`cursor` key, and the fetcher **round-trips it verbatim** — not an offset/limit
scheme ([client.py:222‑250](../../../services/ingest/integrations/miro/client.py#L222-L250),
[fetchers/miro.py:68‑89](../../../services/ingest/ingestion/fetchers/miro.py#L68-L89)).

> **TODO(human): confirm the cursor shape.** The opaque-cursor handling is
> "CLONED-and-adapted from Brex's offset scheme to a cursor scheme but
> UNVERIFIED for Miro" ([client.py:22‑28](../../../services/ingest/integrations/miro/client.py#L22-L28)).
> The fetcher header now marks the **items** endpoint cursor handling as
> `CONFIRMED` correct, while flagging that the parent **`/boards`** listing is
> offset-paginated ([fetchers/miro.py:31‑38](../../../services/ingest/ingestion/fetchers/miro.py#L31-L38)).

### 3.2 Rate limits — no dedicated client-side bucket

There is **no Miro entry in the ingestion token-bucket registry**
(`services/ingest/ingestion/rate_limit/buckets.py` has no `("miro", …)` key).
Rate limiting is **purely reactive**: the client honours `429` + `Retry-After`
within its retry budget ([client.py:176‑180](../../../services/ingest/integrations/miro/client.py#L176-L180)).

> **TODO(human): confirm Miro rate-limit signalling.** The client assumes
> `429` + `Retry-After` (Brex's scheme) ([client.py:18‑21](../../../services/ingest/integrations/miro/client.py#L18-L21)),
> while a later comment says Miro signals **credit-based** limits via
> `X-RateLimit-*` headers ([fetchers/miro.py:37‑38](../../../services/ingest/ingestion/fetchers/miro.py#L37-L38)).
> These two claims disagree; the `X-RateLimit-*` path is **not** handled today.

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per board**, all of
`shard_kind = "miro_board_items"`
([planners/miro.py:54‑97](../../../services/ingest/ingestion/planners/miro.py#L54-L97)).

`ctx.source_client` is **`None`** — boards are read from DB state (the
`SourceOnboarding` loader JSON-aggregates `miro_boards` into `ctx.install["boards"]`),
so the planner does **no network I/O**
([planners/miro.py:1‑19](../../../services/ingest/ingestion/planners/miro.py#L1-L19)).

Each shard carries `board_id`, `board_name`, `org_id`, `installation_id`, and the
warm-start `item_cursor` (the high-water item-`modifiedAt`, `None` on first sync),
at a baseline `recency_score=1.0` ([planners/miro.py:78‑91](../../../services/ingest/ingestion/planners/miro.py#L78-L91)).

The **`org_id`** namespaces every observation's `external_id`; when the org id was
not resolved at seed time it **falls back to the install id** (still install-unique,
so the namespacing invariant holds) ([planners/miro.py:62‑71](../../../services/ingest/ingestion/planners/miro.py#L62-L71)).

> **TODO(human): confirm the Miro resource taxonomy to shard on.** The
> one-shard-per-board model is cloned from Brex's one-shard-per-account
> ([planners/miro.py:16‑19](../../../services/ingest/ingestion/planners/miro.py#L16-L19)).
> If a single board's item count is unbounded, a finer shard (per item-type or
> per frame) may be needed.

---

## 5. Fetching board items — one shard kind, two sync modes

`fetch_page_miro` ([fetchers/miro.py:122‑196](../../../services/ingest/ingestion/fetchers/miro.py#L122-L196))
takes one `(install, shard_identifier, cursor)` triple, returns one page of
records + the next cursor, and is called in a loop by ShardFetch (N1 contract:
one HTTP fetch per call).

Unlike the Brex archetype (which emits a per-shard balance snapshot), Miro emits
**ONE observation per board item and NO extra board-snapshot record** — so a
board with N items yields exactly N backfill observations
([fetchers/miro.py:13‑18](../../../services/ingest/ingestion/fetchers/miro.py#L13-L18), [144‑147](../../../services/ingest/ingestion/fetchers/miro.py#L144-L147)).

The shard cursor (`MiroCursor`, persisted in `workflow_states.state_data`)
([fetchers/miro.py:68‑89](../../../services/ingest/ingestion/fetchers/miro.py#L68-L89)):

```python
class MiroCursor(BaseModel):
    page_cursor: str | None = None          # opaque Miro list cursor; None=first page
    high_water_modified: str | None = None   # max item modifiedAt seen (poll + reconciler baseline)
    incremental_floor: str | None = None     # modified-since floor frozen for this run (None in FULL)
    items_seen: int = 0                       # diagnostic
    seeded: bool = False                      # first-call setup done
```

- **FULL (initial backfill):** walk `GET /boards/{id}/items` from the start via
  the opaque cursor; `next_cursor is None` ends the shard.
- **INCREMENTAL (poll):** when warm-started with an `item_cursor`, the fetcher
  freezes it as the `incremental_floor` / `high_water_modified` and resumes; the
  overlap re-fetch **dedups via the versioned `external_id`**
  ([fetchers/miro.py:148‑153](../../../services/ingest/ingestion/fetchers/miro.py#L148-L153)).

Each item becomes a fetcher-tagged record the handler branches on
([fetchers/miro.py:172‑178](../../../services/ingest/ingestion/fetchers/miro.py#L172-L178)):

```python
records.append({
    "_fyralis_record_type": "item",
    "_fyralis_board_id": board_id,
    "_fyralis_org_id": org_id,
    "item": item,
})
```

The high-water (`max item modifiedAt`) is bumped over the page; it is the **only**
incremental anchor — Miro has **no "modified since" filter** on items, so the
high-water rides the cursor purely as the reconciler's gap reference
([fetchers/miro.py:109‑119](../../../services/ingest/ingestion/fetchers/miro.py#L109-L119), [37‑38](../../../services/ingest/ingestion/fetchers/miro.py#L37-L38)).

A `429` whose retry budget is exhausted returns a **non-terminal** empty page
(cursor preserved) so the shard resumes rather than failing
([fetchers/miro.py:161‑169](../../../services/ingest/ingestion/fetchers/miro.py#L161-L169)).

---

## 6. The handler — shaping items into `ObservationDraft`

`handle_miro_item` ([handlers/miro.py:232‑270](../../../services/ingest/ingestion/handlers/miro.py#L232-L270))
is a **pure function** (no DB / network). It branches on the input shape:

- **LIVE WEBHOOK** (synthetic gate only): a raw body with an `event` /`type` of
  `board_item.*` (or `item.*`). It pulls the item out of `payload.item`/`.data`
  and maps a `.deleted`/`.removed` suffix → a removal
  ([handlers/miro.py:239‑257](../../../services/ingest/ingestion/handlers/miro.py#L239-L257)).
- **BACKFILL / POLL**: a fetcher-tagged record (`_fyralis_record_type == "item"`)
  ([handlers/miro.py:259‑265](../../../services/ingest/ingestion/handlers/miro.py#L259-L265)).

Both feed the **same** `_item_draft` builder
([handlers/miro.py:145‑198](../../../services/ingest/ingestion/handlers/miro.py#L145-L198)),
producing exactly one observation:

| Field | Value | Source |
|-------|-------|--------|
| `source_channel` | `miro:item` | constant |
| `external_id` | `miro:{org_id}:item:{item_id}:{version}` | [idempotency:257‑261](../../../services/ingest/ingestion/idempotency/__init__.py#L257-L261) |
| `occurred_at` | item `modifiedAt` → `updatedAt` → `createdAt` → now | [handlers/miro.py:155‑160](../../../services/ingest/ingestion/handlers/miro.py#L155-L160) |
| `kind` | `state_change` if **deleted/removed**, else `signal` | [handlers/miro.py:193](../../../services/ingest/ingestion/handlers/miro.py#L193) |
| `trust_tier` | `authoritative` (Miro is system-of-record for its boards) | [handlers/miro.py:52‑53](../../../services/ingest/ingestion/handlers/miro.py#L52-L53) |
| `source_actor_ref` | `None` (item author rides in `entities_hint`, not the actor ref) | [handlers/miro.py:195](../../../services/ingest/ingestion/handlers/miro.py#L195) |
| `content_text` | `"{item_type} {updated|removed}: {text}"` (or `"… on board {board_id}"`) | [handlers/miro.py:163‑168](../../../services/ingest/ingestion/handlers/miro.py#L163-L168) |
| `entities_hint` | `{type:"miro_board", id:board_id}` + a `person` author when `createdBy` present | [handlers/miro.py:170‑175](../../../services/ingest/ingestion/handlers/miro.py#L170-L175) |

The `.deleted`/`.removed` → `state_change` mapping is the board-state signal: an
item was removed from the board ([handlers/miro.py:16‑18](../../../services/ingest/ingestion/handlers/miro.py#L16-L18), [55‑56](../../../services/ingest/ingestion/handlers/miro.py#L55-L56)).
Note this is reachable **only** via the (synthetic) webhook event branch — the
backfill/poll fetcher never tags a record as deleted, so in production no
`state_change` is produced.

The channel's trust tier is registered **dynamically** at import time
(`CHANNEL_TRUST_MAP.setdefault("miro:item", "authoritative")`,
[handlers/miro.py:273](../../../services/ingest/ingestion/handlers/miro.py#L273)) —
it is **not** in the static `CHANNEL_TRUST_MAP` literal in `handlers/__init__.py`.

> **TODO(human): confirm the Miro item version field.** `external_id` is versioned
> on `item.version` → `modifiedAt` → `updatedAt` → `createdAt` → `"none"`
> ([handlers/miro.py:82‑94](../../../services/ingest/ingestion/handlers/miro.py#L82-L94)).
> If Miro exposes a monotonic version counter, prefer it over the timestamp.

---

## 7. Live (real-time) ingestion — synthetic gate stand-in, not production

There is **no production live path.** Miro's experimental webhooks were
**discontinued on 2025-12-05** ([signatures/miro.py:4‑17](../../../services/app/webhooks/signatures/miro.py#L4-L17)):
event transmission stopped, the subscription endpoints were removed, there is no
replacement, and Miro's webhooks **never had an HMAC signature scheme** (the only
authenticity check was a `challenge`-echo handshake).

What *is* wired is a **synthetic-gate stand-in** that keeps the
`webhook → 202 → handler` plumbing exercised:

### 7.1 Signature verification (synthetic HMAC-SHA256)

`MiroVerifier` ([signatures/miro.py:55‑97](../../../services/app/webhooks/signatures/miro.py#L55-L97))
verifies a synthetic `X-Miro-Signature: sha256={hex HMAC-SHA256(secret, body)}`,
looping over all active secrets so a rotation verifies, constant-time compared.
It is registered as `VERIFIERS["miro"] = miro.verifier`
([signatures/__init__.py:61](../../../services/app/webhooks/signatures/__init__.py#L61)).

> This is **not a real Miro scheme** — it is explicitly a gate stand-in
> ([signatures/miro.py:41‑46](../../../services/app/webhooks/signatures/miro.py#L41-L46)).
> There is **no replay/timestamp window**; `signed_timestamp` is `None`.

### 7.2 Tenant resolution

The router maps provider `miro → channel miro:item` for the inline path
(`_PROVIDER_CHANNEL["miro"] = "miro:item"`, [router.py:462](../../../services/app/webhooks/router.py#L462)).
The tenant is resolved from the body's top-level **`organizationId`** (falling
back to `orgId`) against the `provider_installations` row keyed
`(provider='miro', installation_id=org_id)`
([tenant_resolver.py:450‑462](../../../services/app/webhooks/tenant_resolver.py#L450-L462)).

> **TODO(human): confirm the Miro webhook tenant-id field** (`organizationId` vs
> `orgId`) — moot in production since the webhook is dead, but the synthetic
> harness sends `organizationId` explicitly
> ([tenant_resolver.py:457‑458](../../../services/app/webhooks/tenant_resolver.py#L457-L458)).

### 7.3 Kafka cutover vs inline

`miro` **is** in `_CUTOVER_ENABLED_PROVIDERS`
([router.py:182‑184](../../../services/app/webhooks/router.py#L182-L184)). When the
resolved tenant's `ingestion.kafka_path_enabled` flag is TRUE, the verified body
is published to Kafka and the edge returns `202`; the writer pool produces the
observation off the data plane ([router.py:1026‑1070](../../../services/app/webhooks/router.py#L1026-L1070)).
When the flag is **off** (the kill-switch), the edge falls back to **inline
`ingest("miro:item", …)`** ([router.py:1092‑1119](../../../services/app/webhooks/router.py#L1092-L1119)).
Either way the same `miro:item` handler runs.

---

## 8. Reconciliation — gap detection

`reconcile_miro` ([reconcilers/miro.py:133‑170](../../../services/ingest/ingestion/reconcilers/miro.py#L133-L170))
loads the tenant's enabled `miro_installations` row and re-checks every **done**
board shard for new activity ([reconcilers/miro.py:84‑130](../../../services/ingest/ingestion/reconcilers/miro.py#L84-L130)):

1. Load the shard's persisted `high_water_modified` (max item `modifiedAt` walked).
   No reference point (empty board) → skip.
2. Probe the live board's **first page** (`list_items(board_id, limit=1)`) and read
   the newest item's `modifiedAt`. A failure is logged and the shard is skipped
   (best-effort).
3. If that timestamp is **strictly newer** than the high-water → emit a reshare of
   the same `miro_board_items` shard at **`recency_score=1.5`**, warm-started with
   `item_cursor = high_water` (incremental mode), tagged with `parent_shard_id` and
   `gap_baseline_modified`.

`external_id` parity makes re-walks idempotent: re-fetched unchanged items dedup;
only genuinely new/changed items (a new version) produce new observations. This
is "pragmatic v1" — it can **over-reshare but never under-reshares**
([reconcilers/miro.py:1‑12](../../../services/ingest/ingestion/reconcilers/miro.py#L1-L12)).

This reconciler **is** the "poll" path — there is no separate poller; gap
detection on a cadence is how Miro stays current absent a live webhook.

---

## 9. Revocation chokepoint — **absent**

Unlike GitHub (which has `_maybe_disable_on_revocation` that flips an install to
`enabled=FALSE` on `401`/`404`), the Miro client has **no revocation chokepoint
and no auto-disable on token failure**. A `401`/`403` is recorded as an
`unauthorized` metric and raised as a typed `MiroApiError`
([client.py:193‑197](../../../services/ingest/integrations/miro/client.py#L193-L197), [264‑274](../../../services/ingest/integrations/miro/client.py#L264-L274));
nothing in `client.py` / `fetchers/miro.py` / `reconcilers/miro.py` writes
`disabled_at`.

Recovery is **operator-driven**: re-run the connect wizard
(`/integrations/miro/connect/finalize`), which UPSERTs the install with a fresh
token and **clears `disabled_at`** ([onboarding.py:71‑77](../../../services/ingest/integrations/miro/onboarding.py#L71-L77)).
The connect-wizard preflight itself surfaces an auth failure as a structured
`400` without storing anything ([oauth.py:98‑116](../../../services/ingest/integrations/miro/oauth.py#L98-L116)).

> **(inferred)** A revoked/expired token therefore causes the backfill shard to
> raise on the next read rather than parking gracefully — there is no
> recoverable-error / park-and-disable behavior like the Notion 401 fix. This is
> a gap relative to the hardened sources.

---

## 10. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  connect wizard: paste org Bearer token → verify via            │
   ORG-VISIBLE BOARDS     │     MiroClient.list_boards()  (GET /boards)                      │
                          │  finalize_install: miro_installations + miro_boards +           │
                          │     onboarding_triggers(source='miro')  → M6 chain              │
                          │  planner: one miro_board_items shard per board (org_id-namespaced)│
                          │  fetcher: GET /boards/{id}/items  (opaque cursor, page<=50)      │
                          │     └─► one tagged "item" record per board item                 │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────────────── POLL (incremental) ───────────────────┐│
                          │  reconciler: per done shard, probe newest item modifiedAt vs   ││
                          │     stored high_water; if newer → reshare (recency 1.5,        ││
                          │     warm-start item_cursor=high_water)                          ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                          ┌────────────────── LIVE (push) — SYNTHETIC GATE ONLY ───────────┐│
   (no production         │  Miro webhooks DISCONTINUED 2025-12-05; no real edge.          ││
    webhook)              │  synthetic: POST /webhooks/miro → X-Miro-Signature HMAC256      ││
                          │     tenant from body organizationId; board_item.* → handler    ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_miro_item                │
                                                            │  one channel: miro:item          │
                                                            │  external_id =                   │
                                                            │   miro:{org}:item:{id}:{version} │
                                                            │  .deleted/.removed → state_change│
                                                            │  → ObservationDraft (authoritative)│
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill, poll, and the synthetic
   webhook all land on `miro:item` with `external_id =
   miro:{org_id}:item:{item_id}:{version}`. A backfilled item and its same-version
   twin dedup to one observation; a new version re-observes (the edit is *not*
   silently dropped).
2. **One credential model.** A single long-lived **org-app Bearer token** reads
   everything — no per-user OAuth, no refresh, no code-exchange. (Real Miro OAuth2
   with refresh is a noted, un-implemented gap.)
3. **One shard family.** One `miro_board_items` shard per board; the planner does
   no network I/O (boards pre-loaded from `miro_boards`).
4. **Opaque-cursor pagination, reactive rate limits.** `GET /boards/{id}/items` is
   cursor-paged (`next_cursor is None` terminal); there is **no client-side token
   bucket** — only `429 + Retry-After` retries.
5. **Poll-only liveness.** Miro has no production webhook; the reconciler's
   high-water gap probe is the real incremental mechanism. The webhook edge is a
   synthetic gate stand-in.
6. **No revocation chokepoint.** Token failures raise; recovery is re-running the
   connect wizard (which clears `disabled_at`).

---

## 11. Configuration & compliance

> **Verification posture.** Miro's external-API specifics are **partly
> unverified** (see the `TODO(human)` callouts in §§2–3, §6). The verified facts
> are: REST base `https://api.miro.com/v2`, scope `boards:read`, items endpoint
> cursor-paged, boards listing offset-paged, OAuth2 with refresh available, and
> **webhooks discontinued 2025-12-05** ([oauth.py:56‑59](../../../services/ingest/integrations/miro/oauth.py#L56-L59), [fetchers/miro.py:31‑38](../../../services/ingest/ingestion/fetchers/miro.py#L31-L38), [signatures/miro.py:4‑17](../../../services/app/webhooks/signatures/miro.py#L4-L17)).

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `MIRO_API_BASE_URL` | `https://api.miro.com/v2` (via `endpoint("miro_api")`) | overrides the API host (Provider Lab/test) ([endpoints.py:88](../../../lib/integrations/endpoints.py#L88), [136](../../../lib/integrations/endpoints.py#L136)) |
| `MIRO_BACKFILL_PAGE_SIZE` | `50` (capped at 50) | board-items page size ([fetchers/miro.py:61‑65](../../../services/ingest/ingestion/fetchers/miro.py#L61-L65)) |
| `MIRO_RL_MAX_ATTEMPTS` | `4` | `429` retry budget ([client.py:157](../../../services/ingest/integrations/miro/client.py#L157)) |
| `MIRO_RL_MAX_SLEEP_SEC` | `30` | max backoff per `Retry-After` ([client.py:158](../../../services/ingest/integrations/miro/client.py#L158)) |

Per-install (not env): `base_url`, `org_id`, `secret_ref`, `webhook_secret_ref`
on `miro_installations` ([0102_miro.sql:41‑59](../../../db/migrations/0102_miro.sql#L41-L59)).

### 11.2 Verified compliant

- **One channel / one dedup namespace** — `miro:item`, versioned `external_id`. ✅
- **Tenant isolation** — `miro_installations` / `miro_boards` ENABLE ROW LEVEL
  SECURITY with a `tenant_isolation` policy keyed on `app.current_tenant`
  ([0102_miro.sql:96‑118](../../../db/migrations/0102_miro.sql#L96-L118)). ✅
- **Secret hygiene** — only opaque `secret_ref`s reach the DB; the Bearer token /
  auth header are never logged; creds verified before any write. ✅
- **Idempotent install + migration** — UPSERTs keyed on `(tenant_id, base_url)` /
  `(install, board_id)`; migration is append-only + `IF NOT EXISTS`. ✅
- **Opaque-cursor pagination on items** — `next_cursor is None` terminal,
  round-tripped verbatim. ✅ *(items endpoint CONFIRMED; `/boards` offset paginator
  not yet split out — see §3 TODO.)*

### 11.3 Dev / Provider Lab mode

`build_miro_client` detects Provider Lab mode and **presets** the token to `spam-miro`,
skipping the secret store entirely, and points `api_base_url` at the local
Provider Lab's `/miro` URL via `endpoint("miro_api")`
([_clients.py:570‑593](../../../services/ingest/ingestion/fetchers/_clients.py#L570-L593)).
The fetcher's `_open_miro_client` is a rebindable test seam so a unit test
can inject a fake ([fetchers/miro.py:102‑107](../../../services/ingest/ingestion/fetchers/miro.py#L102-L107)).
The executable HTTP conformance surface is the canonical
[Provider Lab Miro adapter](../../../services/ingest/synthetic/provider_lab/wave_b.py);
deterministic data remains in
[fixtures/miro_generator.py](../../../services/ingest/synthetic/fixtures/miro_generator.py).
