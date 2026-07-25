# Telegram Ingestion — How Fyralis Pulls Telegram Data

This document explains, in detail, **how Telegram data enters Fyralis**: which
MTProto methods are called, with which credential, and how a single
conversational surface — **messages in dialogs (private chats, basic groups, and
channels/supergroups)** — is ingested both historically and live.

It deliberately stops at the point where a Telegram message becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
the normalizer, observation_writer, the Memory Fabric) is out of scope.

The architectural decision behind this design is recorded in
[ADR-0003](../../adr/0003-telegram-mtproto-user-account-ingestion.md); a short
source-level reference is [sources/telegram.md](../sources/telegram.md). This
page is the **deep** how-it-works narrative — read those for the *why* and the
spec.

---

## 1. The two ways data arrives

Telegram data reaches Fyralis through **two independent paths that converge on
one handler** — but unlike Slack or GitHub, **neither path is an HTTP webhook or
a poll**. Telegram is consumed through the **MTProto user-account API**, and its
live surface is a **persistent push connection** (a gateway, like Discord), not
a webhook:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* each dialog's history via MTProto **`messages.getHistory`** (cursored on `offset_id`) under `ingress_kind="backfill"` | [planners/telegram.py](../../../services/ingest/ingestion/planners/telegram.py), [fetchers/telegram.py](../../../services/ingest/ingestion/fetchers/telegram.py) |
| **Live (real-time)** | New activity on the account | A long-running worker holds a **persistent MTProto updates connection**; each `updateNewMessage` is shadow-written under `ingress_kind="gateway"` | [integrations/telegram/gateway/worker.py](../../../services/ingest/integrations/telegram/gateway/worker.py), [integrations/telegram/gateway/dispatch.py](../../../services/ingest/integrations/telegram/gateway/dispatch.py), [handlers/telegram.py](../../../services/ingest/ingestion/handlers/telegram.py) |

Crucially, **both paths produce the exact same record shape** — the canonical
message record built by **one** function,
`build_message_record`
([records.py:40-66](../../../services/ingest/integrations/telegram/records.py#L40-L66)) —
and both are parsed by the **single** `telegram:message` handler
([handlers/telegram.py:45-101](../../../services/ingest/ingestion/handlers/telegram.py#L45-L101)).
Both derive the **same** dedup key through the central idempotency constructor
([idempotency/__init__.py:175-191](../../../services/ingest/ingestion/idempotency/__init__.py#L175-L191)):

```
external_id = "telegram:{installation_id}:{dialog_id}:{message_id}:{edit_date|none}"
```

So a message that is both backfilled *and* delivered live collapses into **one**
observation. This is the central design invariant of Telegram ingestion — and
because the `external_id` is **install-namespaced** and **edit-versioned**, the
same `(dialog, message)` seen by two tenants stays distinct, while an edit
(`updateEditMessage`, fresh `edit_date`) deliberately re-observes as a new signal
([idempotency/__init__.py:179-191](../../../services/ingest/ingestion/idempotency/__init__.py#L179-L191)).

The channel-mapping table confirms both ingress kinds collapse onto one channel
([channel_mapping.py:148-161](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L148-L161)):

```python
("telegram", "gateway"):  "telegram:message"   # live persistent connection
("telegram", "backfill"): "telegram:message"   # messages.getHistory pages
```

> There is **no `webhook` or `poll` ingress kind** for Telegram. The HTTP Bot
> API (token + webhook) cannot read history, so it is not used at all (ADR-0003);
> the MTProto user API has no HTTP webhook. Gap-recovery is `updates.getDifference`
> **inside the live worker** (§8.2), not a poll re-fetch.

---

## 2. Authentication & token model — a persisted MTProto session

Telegram does **not** authenticate with an OAuth bot token. The durable credential
is a **persisted MTProto session** — a Telethon `StringSession` string that wraps
the per-data-center **`auth_key`** (a 2048-bit key negotiated once via
Diffie-Hellman, never sent over the wire). It is stored in the secret store and
referenced by an install row, alongside the application's `api_id`/`api_hash`
(ADR-0003 §3).

### 2.1 The client wrapper

`TelegramClient` ([client.py:66-262](../../../services/ingest/integrations/telegram/client.py#L66-L262))
is a thin async wrapper over **Telethon** (the pure-Python MTProto user-account
library — ADR-0003 §2). Telethon is an **optional** dependency: the import is
deferred to `_connect()`
([client.py:105-141](../../../services/ingest/integrations/telegram/client.py#L105-L141)),
so importing the ingestion package never requires it (the synthetic harness
monkeypatches the `_open_telegram_client` seam and never connects).

On connect, the client resolves the session string from the secret store (or a
preset, §12.3), opens the Telethon connection, and asserts
`is_user_authorized()`; a revoked/unauthorized session raises
`TelegramApiError(code="telegram_api_unauthorized")`
([client.py:127-141](../../../services/ingest/integrations/telegram/client.py#L127-L141)).

### 2.2 Two sessions per account (Topology B)

A single `auth_key` cannot be safely shared across Fyralis's process-separated
workers, so per ADR-0003 §6 **two independent logins are minted on the same
account** (legitimate — like being logged in on phone + desktop):

| Session | Stored as | Owned by | Used for |
|---------|-----------|----------|----------|
| **Live session** | `telegram_installations.session_secret_ref` | the gateway worker | the persistent updates connection (§8) |
| **Backfill session** | `telegram_installations.backfill_session_secret_ref` | the per-account backfill worker | `messages.getHistory` fan-out (§5–6) |

The backfill client builder prefers the dedicated backfill session and falls back
to the live session ref only if a separate backfill session was not minted
([_clients.py:395-424](../../../services/ingest/ingestion/fetchers/_clients.py#L395-L424)).
The two sessions isolate connections, **not** quota — they share the
account-wide `FLOOD_WAIT` budget (ADR-0003 §6, Consequences).

### 2.3 Where credentials live

| Credential | Where | Notes |
|-----------|-------|-------|
| `api_id` | `telegram_installations.api_id` | MTProto application id (string-typed) |
| `api_hash` | secret store via `api_hash_secret_ref` | resolved on connect |
| Live session | secret store via `session_secret_ref` | `StringSession` (wraps `auth_key`) |
| Backfill session | secret store via `backfill_session_secret_ref` | second authorization (Topology B) |

> **Contrast with Slack/GitHub.** There is no `provider_installations` row and no
> server-side OAuth redirect. Login is **interactive** (phone → SMS/app code →
> optional 2FA), driven by an operator connect wizard out of band, then the
> resulting session string is persisted (ADR-0003 §3). Telegram lives in its own
> `telegram_installations` table, which is why `shard_fetch` has a dedicated
> install-load branch for it
> ([shard_fetch.py:457-469](../../../services/ingest/ingestion/workflows/shard_fetch.py#L457-L469),
> [713-714](../../../services/ingest/ingestion/workflows/shard_fetch.py#L713-L714)).

### 2.4 How an install gets registered

`finalize_install` ([onboarding.py:35-131](../../../services/ingest/integrations/telegram/onboarding.py#L35-L131))
mirrors the Jira/Mercury dedicated-table shape. In **one tenant-scoped
transaction** it:

1. UPSERTs a `telegram_installations` row keyed on `(tenant_id, account_label)`,
   storing both session refs + api credentials and clearing `disabled_at`
   ([onboarding.py:55-75](../../../services/ingest/integrations/telegram/onboarding.py#L55-L75));
2. INSERTs one `telegram_dialogs` row per dialog to back up (the per-dialog
   backfill cursor home), idempotent on `(install, dialog_id)`
   ([onboarding.py:77-98](../../../services/ingest/integrations/telegram/onboarding.py#L77-L98));
3. seeds an empty `telegram_update_state` row (the live `pts/qts/seq/date` cursor,
   one row per install)
   ([onboarding.py:100-109](../../../services/ingest/integrations/telegram/onboarding.py#L100-L109));
4. emits an `onboarding_triggers` row (`source='telegram'`) so the existing M6
   backfill chain fires
   ([onboarding.py:111-125](../../../services/ingest/integrations/telegram/onboarding.py#L111-L125)).

The dialog set is enumerated at connect time via `TelegramClient.iter_dialogs`
or supplied as an operator inclusion list (see the open question in §5).

---

## 3. The MTProto API surface actually called

All reads funnel through `TelegramClient`, which connects lazily and guards every
Telethon coroutine with `_guard_flood`
([client.py:151-165](../../../services/ingest/integrations/telegram/client.py#L151-L165)).
The methods invoked for ingestion:

| MTProto method (via Telethon) | Wrapper | Purpose | Code |
|-------------------------------|---------|---------|------|
| `messages.getHistory` (`client.get_messages(peer, limit, offset_id, min_id)`) | `get_history()` | one backward page of a dialog's history | [client.py:167-199](../../../services/ingest/integrations/telegram/client.py#L167-L199) |
| `messages.getDialogs` (`client.get_dialogs`) | `iter_dialogs()` | enumerate the account's dialogs at onboarding | [client.py:201-224](../../../services/ingest/integrations/telegram/client.py#L201-L224) |
| `messages.getHistory` (`get_messages(peer, limit=1, min_id=…)`) | `has_history_since()` | reconciler 1-row gap probe (§9) | [client.py:226-242](../../../services/ingest/integrations/telegram/client.py#L226-L242) |
| `users.getFullUser` (`client.get_me`) | `me()` | connectivity + credential probe | [client.py:244-252](../../../services/ingest/integrations/telegram/client.py#L244-L252) |

The peer for each call is built from the shard's stored `(dialog_id, access_hash,
dialog_kind)` into an `InputPeerChannel` / `InputPeerUser` / `InputPeerChat`
([client.py:143-149](../../../services/ingest/integrations/telegram/client.py#L143-L149)).
Each Telethon `Message` is flattened to a plain dict by `_message_to_dict`
(`id`, `date`/`edit_date` as epoch seconds, `message` text, `out`, `from_id`→
`{"user_id": …}`) so the backfill fetcher and live worker share one record
contract ([client.py:43-63](../../../services/ingest/integrations/telegram/client.py#L43-L63)).

### 3.1 Pagination — the `offset_id` cursor

`messages.getHistory` pages **backward** from the newest message. `get_history`
requests up to `limit` messages older than `offset_id` (`0` = start at newest)
and bounded below by `min_id` (`0` = no floor), then returns
`(messages, next_offset_id, is_last)` where **`next_offset_id` is the MIN message
id in the page** — the cursor for the next, older page — and `is_last` is True on
a short page ([client.py:167-199](../../../services/ingest/integrations/telegram/client.py#L167-L199)).
The page size caps at **100** (`messages.getHistory`'s limit;
`_DEFAULT_PAGE_SIZE` [client.py:40](../../../services/ingest/integrations/telegram/client.py#L40)).

### 3.2 Rate limits — `FLOOD_WAIT`, not a client bucket

There is **no dedicated client-side token bucket for Telegram** — the
`BUCKET_DEFAULTS` table in
[rate_limit/buckets.py](../../../services/ingest/ingestion/rate_limit/buckets.py)
declares no `("telegram", …)` entry (verified: a search for `telegram` in that
file returns nothing). Instead the protocol's own backpressure is honoured:
`FLOOD_WAIT` (MTProto RPC error **420**) carries the **server-returned `seconds`**.
Telethon raises `FloodWaitError`, which `_guard_flood` maps to
`TelegramApiError(code="telegram_api_flood_wait")` with the wait on
`context["retry_after"]`
([client.py:151-165](../../../services/ingest/integrations/telegram/client.py#L151-L165)).
The caller waits the server's value — the backoff is **not** client-chosen
([client.py:23-26](../../../services/ingest/integrations/telegram/client.py#L23-L26)).

> Concrete numeric `FLOOD_WAIT` durations / per-method limits are **not**
> verifiable from primary sources; only the mechanism (error 420 + server
> `seconds`) is. See the spike `TODO(human)` in ADR-0003 / sources/telegram.md.

---

## 4. Backfill scope — one shard per dialog

The planner decomposes one install into **one shard per dialog**, all of
`shard_kind = "telegram_dialog_history"`
([planners/telegram.py:50-84](../../../services/ingest/ingestion/planners/telegram.py#L50-L84)).
There is exactly **one shard family** — Telegram has a single conversational
signal (messages), unlike GitHub's six event types.

The planner is **stateless** (`ctx.source_client is None`): the dialog list was
written into `telegram_dialogs` at install time and is JSON-aggregated onto
`ctx.install["dialogs"]` by the SourceOnboarding loader, exactly like Jira's
project aggregation
([planners/telegram.py:1-15](../../../services/ingest/ingestion/planners/telegram.py#L1-L15),
[33-47](../../../services/ingest/ingestion/planners/telegram.py#L33-L47)).

Each shard carries the identity the fetcher needs to rebuild the MTProto peer,
plus the warm-start cursor, at baseline `recency_score=1.0`
([planners/telegram.py:64-78](../../../services/ingest/ingestion/planners/telegram.py#L64-L78)):

```python
shard_identifier = {
    "shard_kind": "telegram_dialog_history",
    "dialog_id": dialog_id,
    "dialog_kind": "user" | "chat" | "channel",
    "access_hash": …,
    "dialog_title": …,
    "installation_id": install_id,
    "offset_id_cursor": …,   # per-dialog high-water; None on first full sync
}
```

---

## 5. The backfill fetcher — backward-paged history

`fetch_page_telegram` ([fetchers/telegram.py:105-194](../../../services/ingest/ingestion/fetchers/telegram.py#L105-L194))
fetches one page, advances the cursor, and builds one record per message.
ShardFetch calls it in a loop, persisting the returned cursor between calls
(the N1/A16 invariant: the cursor is opaque to ShardFetch).

### 5.1 Cursor

```python
class TelegramCursor(BaseModel):
    offset_id: int = 0          # next (older) page boundary; 0 = start at newest
    min_id: int = 0             # incremental floor (warm-start high-water); 0 = full
    high_water_max_id: int = 0  # MAX id seen this run — the reconciler's gap ref
    messages_seen: int = 0      # diagnostic
    seeded: bool = False        # first-call warm-start setup has run
```

([fetchers/telegram.py:67-86](../../../services/ingest/ingestion/fetchers/telegram.py#L67-L86)).

Two backfill modes share this cursor
([fetchers/telegram.py:122-141](../../../services/ingest/ingestion/fetchers/telegram.py#L122-L141)):

- **FULL (initial)** — `offset_id=0, min_id=0`; page backward until a short/empty
  page (`is_last`), i.e. the start of history.
- **INCREMENTAL (reconciler re-walk)** — warm-started from the dialog's
  `offset_id_cursor` high-water, used as `min_id` so the walk is bounded to
  messages **newer** than the high-water (only the changed tail comes back).

### 5.2 One message → one record

Unlike Jira (which fans an issue into multiple records), **one Telegram message
is one record**. For each message with a positive id the fetcher tracks
`high_water_max_id` (the MAX id, the reconciler's reference) and calls
`build_message_record` — the **same** builder the live worker uses — then
advances `offset_id` to the oldest id in the page
([fetchers/telegram.py:159-177](../../../services/ingest/ingestion/fetchers/telegram.py#L159-L177)).

### 5.3 `FLOOD_WAIT` handling

On `TelegramApiError(code="telegram_api_flood_wait")` the fetcher **leaves the
cursor unadvanced** and ends the round empty with `end_of_data=False`, so
ShardFetch re-enters on the next tick — the canonical Telegram backoff (wait the
server's value)
([fetchers/telegram.py:142-157](../../../services/ingest/ingestion/fetchers/telegram.py#L142-L157)).
Any other `TelegramApiError` (including `telegram_api_unauthorized` and
`telegram_api_error`) propagates to ShardFetch's normal error handling.

---

## 6. The handler — shaping a message into `ObservationDraft`

The canonical record (built identically by backfill and live) is parsed by
`parse_message_record`
([records.py:103-153](../../../services/ingest/integrations/telegram/records.py#L103-L153))
and shaped by `handle_telegram`
([handlers/telegram.py:45-101](../../../services/ingest/ingestion/handlers/telegram.py#L45-L101)),
a **pure function** (no DB / network). The dialog context (`dialog_kind`,
`dialog_title`, `installation_id`) rides on the record under reserved
`_fyralis_*` keys so the handler needs no lookup
([records.py:60-66](../../../services/ingest/integrations/telegram/records.py#L60-L66)).

| Field | Source | Notes |
|-------|--------|-------|
| `source_channel` | `telegram:message` | the one channel for both ingress kinds |
| `external_id` | `idempotency.telegram_message(...)` | `telegram:{install}:{dialog}:{message_id}:{edit\|none}` |
| `occurred_at` | message `date` (epoch s → UTC) | unparseable date → `ValidationError` → DLQ ([records.py:122-126](../../../services/ingest/integrations/telegram/records.py#L122-L126)) |
| `kind` | `"signal"` | a conversational message |
| `source_actor_ref` | `telegram:user:{from_id.user_id}` or `None` | `None` for self-sent / channel-broadcast (no `from_id`) ([records.py:93-100](../../../services/ingest/integrations/telegram/records.py#L93-L100)) |
| `content_text` | `[{dialog_label}] {who}: {text}` | text truncated to 600 chars ([handlers/telegram.py:41-63](../../../services/ingest/ingestion/handlers/telegram.py#L41-L63)) |
| `entities_hint` | `telegram_dialog` (channel) + `telegram_user` (actor) | [handlers/telegram.py:65-74](../../../services/ingest/ingestion/handlers/telegram.py#L65-L74) |
| `trust_tier` | **`attested_agent`** | a human conversational channel like Slack/Discord ([handlers/telegram.py:38](../../../services/ingest/ingestion/handlers/telegram.py#L38)) |

The handler is registered with `@register("telegram:message")` and seeds
`CHANNEL_TRUST_MAP[telegram:message] = attested_agent`
([handlers/telegram.py:45-46](../../../services/ingest/ingestion/handlers/telegram.py#L45-L46),
[104](../../../services/ingest/ingestion/handlers/telegram.py#L104)); the
authoritative trust map lives in
[handlers/__init__.py:41-54](../../../services/ingest/ingestion/handlers/__init__.py#L41-L54).
A malformed record (missing id, missing `_fyralis_dialog_id`, unparseable date)
raises `ValidationError` → DLQ rather than crashing
([records.py:109-126](../../../services/ingest/integrations/telegram/records.py#L109-L126)).

---

## 7. Live ingestion — a persistent updates connection (NOT a webhook)

When activity occurs on the account, MTProto **pushes** an update
(`updateNewMessage`, …) over a **long-lived connection** — there is no HTTP
webhook for the user API. Telegram is therefore modelled as a **gateway source
like Discord**: a single long-running worker holds the connection and shadow-writes
each live update onto `ingestion.raw.telegram` so live flows through the *same*
normalizer → observation_writer chain as backfill (ADR-0003 §4).

### 7.1 The gateway worker

`TelegramGatewayWorker` ([gateway/worker.py:36-143](../../../services/ingest/integrations/telegram/gateway/worker.py#L36-L143))
holds **one** install's live MTProto connection. It registers a Telethon
`events.NewMessage` handler that builds the update dict (`message` via the shared
`_message_to_dict`, plus `dialog_id`/`dialog_kind`/`dialog_title` resolved from a
prebuilt dialog index) and calls `handle_update`
([gateway/worker.py:79-95](../../../services/ingest/integrations/telegram/gateway/worker.py#L79-L95)),
then runs `run_until_disconnected()`
([gateway/worker.py:109](../../../services/ingest/integrations/telegram/gateway/worker.py#L109)).

**Single-instance lease.** A Telegram authorization may be driven by only one
live connection at a time, so the launcher acquires the
`gateway:telegram:leader_lock` Redis lease **before** constructing the worker
(reusing Discord's `leader_lock`)
([worker.py:16-19](../../../services/ingest/integrations/telegram/gateway/worker.py#L16-L19),
[run_telegram_gateway_worker.py:6-8](../../../scripts/run_telegram_gateway_worker.py#L6-L8)).

### 7.2 The dispatch — cutover to the same pipeline

`handle_update` ([gateway/dispatch.py:144-185](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L144-L185))
is the Discord-gateway `handle_message_create` analog, simplified: a worker holds
ONE account's session = ONE tenant's install, so the **tenant is known by
construction** (carried on `DispatchDeps`) — there is no per-update tenant
resolution ([gateway/dispatch.py:1-22](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L1-L22)).

1. `_update_to_record` builds the canonical record with the **same**
   `build_message_record`, **skipping the account's own outgoing messages**
   (`message["out"] is True`)
   ([gateway/dispatch.py:65-89](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L65-L89)).
2. **Cutover:** if `ingestion.kafka_path_enabled` for the tenant (the kafka-first
   default), `shadow_write_raw(source="telegram", ingress_kind="gateway")`
   publishes the record to `ingestion.raw.telegram` and returns — the normalizer
   + observation_writer produce the observation, concurrently with any in-flight
   backfill ([gateway/dispatch.py:106-127](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L106-L127),
   [150-163](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L150-L163)).
3. **Inline fallback:** otherwise `core.ingest("telegram:message", record, …)`,
   then a best-effort M2 shadow audit when `SHADOW_WRITE_ENABLED`
   ([gateway/dispatch.py:165-185](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L165-L185)).

The raw body **is** the canonical record (byte-stable `orjson`, sorted keys), so
the normalizer feeds `handle_telegram` identically on both paths and content_hash
dedup / replay-from-raw hold
([gateway/dispatch.py:92-103](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L92-L103)).

### 7.3 No signature verifier (gateway, not webhook)

There is **no HMAC signature gate**. The trust boundary is the authenticated
MTProto connection itself (as with Discord's gateway and Gmail Pub/Sub) —
explicitly stated in the dispatch module
([gateway/dispatch.py:20-22](../../../services/ingest/integrations/telegram/gateway/dispatch.py#L20-L22))
and in ADR-0003 §4. Accordingly, **Telegram is not in the `VERIFIERS` registry**
([signatures/__init__.py:44-68](../../../services/app/webhooks/signatures/__init__.py#L44-L68))
— verified: the registry contains slack/github/linear/stripe/discord/notion/jira/
mercury/quickbooks/grafana/brex/ramp/gusto/deel/fireflies/miro/figma/hibob/ashby,
and no `telegram` entry. There is no `services/.../webhooks/signatures/telegram.py`.

---

## 8. Live gap recovery — `updates.getDifference` inside the worker

### 8.1 The live cursor

Live updates cursor on the MTProto **update-state** (`pts`/`qts`/`seq`/`date`,
plus per-channel `pts`), stored one row per install in `telegram_update_state`
(ADR-0003 §5, sources/telegram.md). The worker periodically persists the
advancing state via `client.get_state()` → `save_state(pts, qts, seq, date)`
after handling each update
([gateway/worker.py:111-123](../../../services/ingest/integrations/telegram/gateway/worker.py#L111-L123)).

### 8.2 Gap recovery is native, not a poll

On startup (and after any disconnect — including a long backfill sweep on the
separate backfill session) the worker calls Telethon's `catch_up()`, which issues
`updates.getDifference` / `updates.getChannelDifference` under the hood to
reconcile any update missed while the connection was down
([gateway/worker.py:104-107](../../../services/ingest/integrations/telegram/gateway/worker.py#L104-L107),
[10-14](../../../services/ingest/integrations/telegram/gateway/worker.py#L10-L14)).
This is **protocol-native gap-recovery**, not a poll re-fetch — a stronger
reconciler than the polling reconcilers other sources use (ADR-0003,
Consequences).

---

## 9. Reconciliation — the backfill-completeness safety net

`reconcile_telegram` ([reconcilers/telegram.py:148-177](../../../services/ingest/ingestion/reconcilers/telegram.py#L148-L177))
re-checks **completed** dialog shards for new history. For each done shard it
loads the stored `high_water_max_id` from the shard's saved cursor
([reconcilers/telegram.py:81-89](../../../services/ingest/ingestion/reconcilers/telegram.py#L81-L89))
and issues a **1-row** `has_history_since(min_id=high_water_max_id)` probe; a
non-empty result means there is a newer message → a gap
([reconcilers/telegram.py:92-118](../../../services/ingest/ingestion/reconcilers/telegram.py#L92-L118),
[client.py:226-242](../../../services/ingest/integrations/telegram/client.py#L226-L242)).

On a gap it reshares a `telegram_dialog_history` shard at **`recency_score=1.5`**,
warm-started at the high-water (`offset_id_cursor = high_water`) so the re-walk
only re-fetches the newer tail (the fetcher's incremental mode, §5.1)
([reconcilers/telegram.py:120-136](../../../services/ingest/ingestion/reconcilers/telegram.py#L120-L136)).
`external_id` parity makes the re-walk idempotent — it can over-reshare but never
under-reshares ([reconcilers/telegram.py:13-17](../../../services/ingest/ingestion/reconcilers/telegram.py#L13-L17)).

> **Two reconcilers, one for each path.** This DB-side reconciler is the
> *backfill-completeness* safety net (it catches anything that arrived between the
> backfill sweep and the live connection coming up). The **live** path's native
> reconciler is `updates.getDifference` inside the worker (§8.2). They are
> independent and never contend at the state level — `offset_id` lives on
> per-dialog rows; `pts/qts/seq/date` lives on the per-install update-state row
> ([reconcilers/telegram.py:19-22](../../../services/ingest/ingestion/reconcilers/telegram.py#L19-L22), ADR-0003 §5).

---

## 10. Revocation / recoverable-error behavior

The connect path is the single chokepoint that detects a dead session: a
non-authorized session raises `TelegramApiError(code="telegram_api_unauthorized")`
([client.py:127-141](../../../services/ingest/integrations/telegram/client.py#L127-L141)),
and the live worker raises a `RuntimeError` if the live session is not authorized
on connect ([gateway/worker.py:97-99](../../../services/ingest/integrations/telegram/gateway/worker.py#L97-L99)).

- **`telegram_api_flood_wait`** is the only error the backfill fetcher treats as
  *recoverable in place* — it ends the round empty (`end_of_data=False`) without
  advancing the cursor, so ShardFetch re-enters next tick
  ([fetchers/telegram.py:142-157](../../../services/ingest/ingestion/fetchers/telegram.py#L142-L157)).
- **`telegram_api_unauthorized` / `telegram_api_error`** propagate to ShardFetch's
  standard error handling.

> **TODO(human):** Unlike GitHub (which has an explicit
> `_maybe_disable_on_revocation` chokepoint that flips `enabled=FALSE`) and the
> Notion park-on-revocation flow, this codebase has **no Telegram-specific
> install-disable wiring** on `telegram_api_unauthorized` — the only `disabled_at`
> writes for Telegram are the manual/onboarding ones
> ([onboarding.py:70](../../../services/ingest/integrations/telegram/onboarding.py#L70),
> with `shard_fetch`/`reconcile` filtering on `disabled_at IS NULL`
> [shard_fetch.py:467](../../../services/ingest/ingestion/workflows/shard_fetch.py#L467)).
> Recovery from a revoked session is re-running the interactive connect wizard to
> mint a fresh session (ADR-0003 §3). *Confirm whether a revocation-disable
> chokepoint is intended before production* — this is a genuine gap, not an
> inference about existing code.

---

## 11. End-to-end summary

```
                          ┌──────────────────────── BACKFILL (pull) ─────────────────────────┐
                          │  credential: persisted MTProto StringSession (backfill session)   │
   ACCOUNT DIALOGS        │  install: telegram_dialogs (one row per dialog) + onboarding_trig │
   (user|chat|channel)    │  planner: one telegram_dialog_history shard per dialog            │
                          │  fetcher: messages.getHistory(offset_id) — backward-paged         │
                          │     └─► next_offset_id = MIN id in page ; warm-start on min_id     │
                          │     └─► build_message_record  (ingress_kind="backfill")            │
                          └────────────────────────────────────────────────────────────────┬─┘
                                                                                             │
                          ┌──────────────────────── LIVE (push) ───────────────────────────┐│
   ANY account activity ──►  persistent MTProto updates connection (gateway worker)        ││
   (updateNewMessage)     │     single-instance Redis lease ; NO HMAC gate (not a webhook)  ││
                          │     skip own `out` messages                                      ││
                          │     build_message_record → shadow_write_raw(ingress=gateway)     ││
                          │     gap recovery: catch_up() → updates.getDifference (in-worker)  ││
                          └────────────────────────────────────────────────────────────────┘│
                                                                                             │
                                                            ┌────────────────────────────────▼─┐
                                                            │  handle_telegram                  │
                                                            │  external_id = telegram:{inst}:   │
                                                            │     {dialog}:{msg_id}:{edit|none} │
                                                            │  trust_tier = attested_agent      │
                                                            │  → ObservationDraft               │
                                                            └───────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill and the live gateway both build
   the canonical record with `build_message_record` and land on `telegram:message`
   with `external_id = telegram:{installation_id}:{dialog_id}:{message_id}:{edit|none}`.
   A backfilled message and its live `updateNewMessage` twin dedup to one
   observation; an edit re-observes via a fresh `edit_date`.
2. **Persistent connection, not a webhook or poll.** Live ingress is a long-lived
   MTProto updates connection (gateway, Discord analog); there is **no HTTP
   webhook** and **no signature verifier** for Telegram — the trust boundary is
   the authenticated connection itself.
3. **MTProto user-account API, persisted session credential.** History needs
   `messages.getHistory` (users-only), so the credential is a persisted Telethon
   `StringSession`, not a token. Two authorizations (Topology B) isolate the live
   and backfill connections; they share the account-wide `FLOOD_WAIT` budget.
4. **Two cursors, two reconcilers, no contention.** Backfill cursors per-dialog on
   `offset_id` (oldest id → next page), reconciled by `reconcile_telegram`'s
   `has_history_since` probe; live cursors per-install on `pts/qts/seq/date`,
   reconciled natively by `updates.getDifference` inside the worker.
5. **Protocol-native backpressure.** No client-side rate bucket; `FLOOD_WAIT`
   (error 420) is honoured with the server-returned `seconds` (waited, not
   client-chosen).

---

## 12. Configuration & compliance

Verified against Telegram's primary MTProto/API spec + the Telethon docs via the
adversarially-verified research pass behind ADR-0003 (23/23 surviving claims
passed 3-0).

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `TELEGRAM_BACKFILL_PAGE_SIZE` | `100` | per-page `messages.getHistory` limit, clamped to 1–100 ([fetchers/telegram.py:60-64](../../../services/ingest/ingestion/fetchers/telegram.py#L60-L64)) |
| `DATABASE_URL` | — (required) | Postgres DSN (gateway launcher) |
| `REDIS_URL` | — (required) | single-instance lease store (gateway launcher) |
| `KAFKA_BOOTSTRAP_SERVERS` | — (optional) | wire the data plane for the kafka-first cutover; absent → inline `ingest()` ([run_telegram_gateway_worker.py:13-14](../../../scripts/run_telegram_gateway_worker.py#L13-L14)) |
| `TELEGRAM_INSTALLATION_ID` | — (optional) | which `telegram_installations` row the worker runs; absent → first active install ([run_telegram_gateway_worker.py:15-16](../../../scripts/run_telegram_gateway_worker.py#L15-L16)) |
| `PROVIDER_LAB_URL` | unset | presence outside production enables deterministic lab credentials |

Tenant-scoped feature flags (`ingestion.kafka_path_enabled`, `SHADOW_WRITE_ENABLED`)
gate the live cutover vs inline path (§7.2); they are shared ingestion flags, not
Telegram-specific.

### 12.2 Verified compliant

- **API choice** — MTProto user API (`messages.getHistory` is users-only); the
  Bot API is not used because it cannot read history. ✅
- **Credential** — persisted `StringSession` (wraps the DH-negotiated `auth_key`),
  reused across reconnects; never logged. ✅
- **Pagination** — `offset_id`/`min_id`, oldest returned id → next page, limit ≤ 100. ✅
- **Backpressure** — `FLOOD_WAIT` (error 420) honoured with the server's `seconds`
  (no client-chosen quota). ✅
- **Live ingress** — persistent updates connection (no webhook), single-instance
  lease, no HMAC gate (authenticated connection = trust boundary). ✅
- **Gap recovery** — native `updates.getDifference` (`catch_up`) on the live path;
  `has_history_since` re-probe on the backfill path. ✅

### 12.3 Dev / Provider Lab mode

For local testing there are **two** distinct seams (the live path does **not**
reuse the backfill mock):

- **Backfill** — `build_telegram_client` detects `PROVIDER_LAB_URL` and presets
  the session string to
  `spam-telegram`, skipping the secret store and the real Telethon connect
  ([_clients.py:407-423](../../../services/ingest/ingestion/fetchers/_clients.py#L407-L423)).
  In-process tests instead rebind the `_open_telegram_client` seam
  ([fetchers/telegram.py:98-102](../../../services/ingest/ingestion/fetchers/telegram.py#L98-L102))
  to a `MockTelegramClient`, which mirrors the real `messages.getHistory` backward
  paging and raises the production `TelegramApiError` codes
  ([mock_clients/telegram.py:39-160](../../../services/ingest/synthetic/mock_clients/telegram.py#L39-L160)).
- **Live** — the `TelegramGatewayGenerator` drives the **production**
  `gateway/dispatch.handle_update` in-process with synthetic `updateNewMessage`
  deltas (unique ids ≥ 1_000_000, current-window timestamps), so live observations
  land via the real cutover with genuine cross-path dedup — there is no HTTP status
  to assert (Discord-gateway style)
  ([live_generators/telegram_gateway.py:1-26](../../../services/ingest/synthetic/live_generators/telegram_gateway.py#L1-L26)).

Telethon is an **optional** dependency (`pip install 'fyraliscore[telegram]'`),
import-guarded so the synthetic gate runs without it (ADR-0003 §7;
[client.py:111-119](../../../services/ingest/integrations/telegram/client.py#L111-L119)).
