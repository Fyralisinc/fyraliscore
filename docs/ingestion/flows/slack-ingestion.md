# Slack Ingestion — How Fyralis Pulls Slack Data

This document explains, in detail, **how Slack data enters Fyralis**: which Slack
Web APIs are called, with which tokens, and how the three distinct conversation
surfaces — **company/public channels**, **one‑to‑one DMs**, and **multi‑person
group DMs** — are each ingested.

It deliberately stops at the point where a Slack message becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope.

---

## 1. The two ways data arrives

Slack data reaches Fyralis through **two independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* history via the Slack **Web API** (`conversations.history`) | `services/ingestion/planners/slack.py`, `services/ingestion/fetchers/slack.py` |
| **Live (real‑time)** | New message in Slack | Slack *pushes* an **Events API** webhook to Fyralis | `services/gateway/main.py`, `services/webhooks/`, `services/ingestion/handlers/slack.py` |

Crucially, **both paths produce the exact same record shape** — Slack's Events
API `event_callback` envelope — and both are parsed by the **single**
`slack:message` handler ([handlers/slack.py](../../../services/ingestion/handlers/slack.py)).
Both derive the **same** dedup key:

```
external_id = "{channel_id}:{ts}"
```

So a message that is both backfilled *and* delivered live collapses into **one**
observation. This is the central design invariant of Slack ingestion.

> Slack does **not** use Socket Mode or RTM here. Real‑time is the **Events
> API over HTTP webhooks** only; history is the **Web API** only.

---

## 2. Authentication — two kinds of token

Slack ingestion uses **two different token types**, because of a hard Slack
constraint: **a bot token can never read human↔human DMs.** Only a user token
(granted by a consenting participant) can.

### 2.1 Bot token (`xoxb`) — for channels

- **Label in secret store:** `slack_bot_token:{team_id}`
- **Used by:** `SlackClient` ([client.py:85‑113](../../../services/integrations/slack/client.py#L85-L113))
- **Reads:** public channels (and optionally private channels) the bot is a member of.

### 2.2 User token (`xoxp`) — for DMs and group DMs

- **Label in secret store:** `slack_user_token:{team_id}:{user_id}`
- **Used by:** `SlackUserClient` ([client.py:319‑420](../../../services/integrations/slack/client.py#L319-L420))
- **Reads:** the *consenting user's own* `im` (1:1 DM) and `mpim` (group DM)
  conversations, under that user's grant.

`SlackUserClient` subclasses `SlackClient` and overrides only **(a)** token
resolution and **(b)** `conversations_list` (it requests `types=im,mpim`). All
the rate‑limiting, retry, and pagination machinery is shared verbatim
([client.py:319‑340](../../../services/integrations/slack/client.py#L319-L340)).

### 2.3 OAuth scopes requested at install

The authorize URL requests scopes split **by token type** — bot scopes in the
`scope` param, user scopes in the `user_scope` param
([oauth.py](../../../services/integrations/slack/oauth.py)). DM scopes are deliberately
**not** requested as bot scopes (a bot token can't read human DMs anyway —
least privilege):

```
# scope  (bot token)
channels:read, channels:history,
groups:read,   groups:history,
users:read,    team:read

# user_scope  (per-user xoxp token — the only way to read DMs)
im:read,   im:history,
mpim:read, mpim:history
```

| Scope | Token | Grants |
|-------|-------|--------|
| `channels:read` / `channels:history` | bot | list / read **public** channels |
| `groups:read` / `groups:history` | bot | list / read **private** channels |
| `im:read` / `im:history` | user | list / read **1:1 DMs** |
| `mpim:read` / `mpim:history` | user | list / read **group DMs** |
| `users:read` | bot | resolve user profiles |
| `team:read` | bot | workspace metadata |

### 2.4 How the tokens get stored — the OAuth flow

`services/integrations/slack/oauth.py` implements the standard Slack OAuth v2
flow:

1. **`GET /integrations/slack/install`** (Bearer‑authed) — issues an
   HMAC‑signed `state` token bound to the session's `tenant_id` (never a
   client‑supplied param), persists a single‑use nonce, then `302`s to
   `https://slack.com/oauth/v2/authorize` with the scopes above
   ([oauth.py:261‑324](../../../services/integrations/slack/oauth.py#L261-L324)).
2. **`GET /integrations/slack/callback`** (public, state‑authed) — verifies the
   HMAC, atomically consumes the nonce, then **`POST`s `oauth.v2.access`** to
   exchange the `code` for tokens
   ([oauth.py:331‑351](../../../services/integrations/slack/oauth.py#L331-L351)).
3. The response yields `access_token` (the **bot** token) and, because
   `user_scope` was requested, `authed_user.access_token` (the **user** token)
   plus `authed_user.id`. The bot token is stored under
   `slack_bot_token:{team}`; the user token under the **per-user** label
   `slack_user_token:{team}:{user}` — exactly what `SlackUserClient` resolves.
   The **signing secret** (for inbound webhook verification) is stored too
   (`_persist_secrets`).
4. A `provider_installations` row is upserted (keyed on
   `(provider='slack', installation_id=team_id)`), with a cross‑tenant rebind
   guard returning `409 installation_collision`. **In the same transaction**, if
   a user token was granted, the consenting user is registered in
   `slack_dm_installations` (`_upsert_dm_installation`) so the planner shards
   that user's DM windows — this is what wires **production** DM ingestion.

---

## 3. The Slack Web API surface that is actually called

All Web API calls funnel through `SlackClient._call`
([client.py:125‑212](../../../services/integrations/slack/client.py#L125-L212)), which:

- sets `Authorization: Bearer {token}` (bot or user, resolved lazily),
- honours Slack's `429 Retry-After` header within a bounded retry policy,
- retries short cooldowns inline, while a `Retry-After` above the independent
  inline ceiling returns `RetryLater` so the scheduler can persist
  `next_attempt_at` and release the worker,
- retries transport errors with exponential backoff,
- raises `SlackApiError` on `ok=false` (Slack errors are not retried — they're
  permanent for a given input).

The endpoints invoked for ingestion:

| Slack method | Wrapper | Purpose | Token | Code |
|--------------|---------|---------|-------|------|
| `conversations.list` | `SlackClient.conversations_list()` | enumerate **public/private channels** | bot | [client.py:244‑288](../../../services/integrations/slack/client.py#L244-L288) |
| `conversations.list` | `SlackUserClient.conversations_list(types="im,mpim")` | enumerate a user's **DMs + group DMs** | user | [client.py:379‑420](../../../services/integrations/slack/client.py#L379-L420) |
| `conversations.history` | `conversations_history()` | fetch one **page of messages** in a conversation | bot or user | [client.py:290‑316](../../../services/integrations/slack/client.py#L290-L316) |
| `conversations.info` | `conversations_info()` | channel metadata | bot | [client.py:231‑236](../../../services/integrations/slack/client.py#L231-L236) |
| `users.info` | `users_info()` | user profile | bot | [client.py:226‑229](../../../services/integrations/slack/client.py#L226-L229) |
| `oauth.v2.access` | `_exchange_code_for_tokens()` | exchange OAuth code → tokens | — | [oauth.py:331‑351](../../../services/integrations/slack/oauth.py#L331-L351) |

> Note: `conversations.replies` (thread fetching) is **not** called by Fyralis
> core — backfill reads only top‑level conversation history. Live thread replies
> still arrive as ordinary `message` events with a `thread_ts`.

### 3.1 Pagination — cursor based

Every list/history endpoint paginates the same way: read
`response_metadata.next_cursor`, pass it back as `cursor`, stop when it is
empty.

- `conversations_list` loops **to completion** internally (it returns the full
  channel/DM set), so a workspace with >1000 channels is never silently
  truncated ([client.py:266‑288](../../../services/integrations/slack/client.py#L266-L288)).
- `conversations_history` returns **one page** plus its `next_cursor` to the
  caller; the fetcher persists that cursor in shard state and resumes on the
  next invocation ([client.py:290‑316](../../../services/integrations/slack/client.py#L290-L316)).

### 3.2 Rate limits

Client‑side token buckets are declared per method
([rate_limit/buckets.py:35‑37](../../../services/ingestion/rate_limit/buckets.py#L35-L37)):

```
("slack", "conversations.history"): capacity 40, refill 0.67/s
("slack", "conversations.list"):    capacity 40, refill 0.67/s
("slack", "users.info"):            capacity 40, refill 0.67/s
```

---

## 4. Ingesting **company / public channels** (bot token)

This is the original backfill path (the planner calls it the
`slack_channel_window` shard family).

### 4.1 Enumerate channels

The planner asks the bot client for every visible channel
([planners/slack.py:64‑90](../../../services/ingestion/planners/slack.py#L64-L90)):

```python
channels = await ctx.source_client.conversations_list()   # bot token
```

Internally this is `conversations.list` with
`types="public_channel"` by default. Two env knobs widen it
([client.py:255‑264](../../../services/integrations/slack/client.py#L255-L264)):

- `SLACK_BACKFILL_INCLUDE_PRIVATE=1` → adds `private_channel` (needs
  `groups:read`).
- `SLACK_BACKFILL_CHANNEL_TYPES="..."` → sets the comma‑separated types
  explicitly.

One **shard** is emitted per channel, carrying `channel_id`, `channel_name`,
`team_id`, `installation_id`.

### 4.2 Read each channel's history

For each channel shard, the fetcher calls `conversations.history`
([fetchers/slack.py:97‑105](../../../services/ingestion/fetchers/slack.py#L97-L105)):

```python
client, close = await _open_slack_client(install)        # bot token
messages, next_cursor = await client.conversations_history(
    channel=channel_id, cursor=cur.next_cursor,
)
```

**Bot‑not‑in‑channel handling:** a bot is rarely a member of every channel. If
Slack returns `not_in_channel` or `channel_not_found`, the fetcher logs it and
treats that channel as a **terminal empty page** rather than failing the whole
run ([fetchers/slack.py:106‑123](../../../services/ingestion/fetchers/slack.py#L106-L123)).
Live coverage for such a channel begins once the bot is invited and its
`message.*` events start flowing.

### 4.3 Conform to the handler shape

`conversations.history` messages carry `ts` but **not** `channel` (it was the
request param). The fetcher **injects `channel`** so the derived
`external_id="{channel}:{ts}"` matches the live‑webhook twin exactly
([fetchers/slack.py:132‑142](../../../services/ingestion/fetchers/slack.py#L132-L142)):

```python
def _event(m):
    ev = {**m, "channel": channel_id}
    if channel_type is not None:        # only set for DM shards
        ev["channel_type"] = channel_type
    return ev

records = [{"type": "event_callback", "team_id": ..., "event": _event(m)}
           for m in messages]
```

Channel shards leave `channel_type` unset (the handler stamps `None`).

---

## 5. Ingesting **DMs and group DMs** (user token, consent‑based)

DMs cannot be read by a bot. So DM coverage is **consent‑shaped**: it exists only
for users who granted an `xoxp` user token. Those users are rows in the
`slack_dm_installations` table. The planner calls this the `slack_dm_window`
shard family.

### 5.1 Who consented? — `slack_dm_installations`

The planner loads every consenting user for the tenant
([planners/slack.py:43‑48](../../../services/ingestion/planners/slack.py#L43-L48)):

```sql
SELECT id, team_id, user_id, base_url
  FROM slack_dm_installations
 WHERE tenant_id = $1 AND disabled_at IS NULL
 ORDER BY user_id
```

A row is created two ways, both storing the xoxp token under
`slack_user_token:{team_id}:{user_id}`:

- **Production:** the OAuth callback, when the installing user grants the
  `user_scope` DM scopes, registers them via `_upsert_dm_installation`
  (§2.4 step 4).
- **Dev/testing:** `POST /slack/{user_id}/install` records the consent row with a
  mock token ([slack_router.py:368‑381](../../../services/gateway/slack_router.py#L368-L381)).

### 5.2 Enumerate that user's DMs + group DMs

For each consenting user, the planner builds a `SlackUserClient` (their token)
and lists their conversations
([planners/slack.py:111‑116](../../../services/ingestion/planners/slack.py#L111-L116)):

```python
client = await _open_slack_user_client(tenant_id, team_id, user_id, base_url)
conversations = await client.conversations_list(types="im,mpim")
```

`SlackUserClient.conversations_list` classifies each conversation from Slack's
flags ([client.py:401‑413](../../../services/integrations/slack/client.py#L401-L413)):

```python
ctype = ("im"   if c.get("is_im")
         else "mpim" if c.get("is_mpim")
         else c.get("channel_type"))   # mock/Provider Lab convenience
out.append({
    "id": c["id"],
    "channel_type": ctype,
    "user": c.get("user"),   # the OTHER human in a 1:1; None for group DMs
    "name": c.get("name"),
    "team_id": ...,
})
```

| Type | Slack flag | `channel_type` | `user` field | What it is |
|------|-----------|----------------|--------------|------------|
| 1:1 DM | `is_im=true` | `"im"` | the counterpart's user id | direct message between two people |
| Group DM | `is_mpim=true` | `"mpim"` | *(absent)* | multi‑person direct message |

### 5.3 One DM shard per conversation

Each conversation becomes a `slack_dm_window` shard
([planners/slack.py:130‑145](../../../services/ingestion/planners/slack.py#L130-L145)),
carrying the identity needed for the fetcher to re‑resolve the right user token:

```python
shard_identifier = {
    "shard_kind": "slack_dm_window",
    "channel_id": cid,
    "channel_type": conv["channel_type"],   # im | mpim
    "consenting_user_id": user_id,           # whose xoxp token reads this
    "counterpart_user_id": conv.get("user"), # the other human (im only; None for mpim)
    "team_id": ...,
    "installation_id": ...,
}
# DMs get a higher recency_score (1.25) than bulk channel history.
```

**Partial‑failure isolation:** if one user's token is revoked or errors, that
user's DMs are logged and skipped — the remaining users *and* the channel shards
still proceed ([planners/slack.py:117‑125](../../../services/ingestion/planners/slack.py#L117-L125)).

### 5.4 Read the DM history under the user token

The fetcher detects a DM shard by `shard_kind`, opens the **user** client (not
the bot), and stamps `channel_type` onto each event so live and backfill twins
match ([fetchers/slack.py:93‑99](../../../services/ingestion/fetchers/slack.py#L93-L99)):

```python
is_dm = shard_identifier.get("shard_kind") == "slack_dm_window"
if is_dm:
    client, close = await _open_slack_user_client(install, shard_identifier)
    channel_type  = shard_identifier.get("channel_type")   # im | mpim
else:
    client, close = await _open_slack_client(install)
    channel_type  = None
```

From there the read is identical to channels: `conversations.history` with
cursor pagination ([fetchers/slack.py:103‑105](../../../services/ingestion/fetchers/slack.py#L103-L105)).
The user token, identified by `(team_id, consenting_user_id)`, is resolved by
`SlackUserClient._resolve_token` from `slack_user_token:{team}:{user}`
([client.py:347‑377](../../../services/integrations/slack/client.py#L347-L377)).

---

## 6. Live (real‑time) ingestion via the Events API

When a new message is posted in Slack, Slack **POSTs an `event_callback`** to
Fyralis's webhook edge. DMs, group DMs, and channel messages all arrive on the
**same** webhook and the **same** `slack:message` handler — the only difference
is the `channel_type` field (`im` / `mpim` / `channel`) and the channel‑id prefix
(`D…` for DMs, `C…`/`G…` for channels/group DMs).

### 6.1 Signature verification (replay‑safe HMAC)

Before any parsing, the inbound body is verified against Slack's **v0** HMAC
protocol ([handlers/slack.py:59‑99](../../../services/ingestion/handlers/slack.py#L59-L99)):

```
basestring = "v0:{X-Slack-Request-Timestamp}:{raw_body}"
expected   = "v0=" + hex(hmac_sha256(signing_secret, basestring))
compare(expected, X-Slack-Signature)   # constant-time
```

A request whose timestamp is older than **300 s** (`SLACK_MAX_TIMESTAMP_AGE_S`)
is rejected as a replay. Verification has **one** implementation, called from two
places:

- the gateway's `/ingest/slack:message` route
  ([gateway/main.py:978‑990](../../../services/gateway/main.py#L978-L990)), and
- the webhook subsystem's `SlackVerifier`
  ([webhooks/signatures/slack.py:39‑108](../../../services/webhooks/signatures/slack.py#L39-L108)),
  which tries each candidate signing secret and short‑circuits on
  "too old" / "malformed timestamp".

The webhook router maps provider `slack` → channel `slack:message`
([webhooks/router.py:349](../../../services/webhooks/router.py#L349)).

### 6.2 Parsing the event → `ObservationDraft`

`handle_slack_message` ([handlers/slack.py:158‑267](../../../services/ingestion/handlers/slack.py#L158-L267))
unwraps the `event` envelope and extracts:

- **`text`** → `content_text`
- **`ts`** → `occurred_at` (parsed from Slack's fractional‑epoch string,
  [handlers/slack.py:102‑114](../../../services/ingestion/handlers/slack.py#L102-L114))
- **`user`** → `source_actor_ref = "slack:{user_id}"`
- **`channel` + `ts`** → `external_id = "{channel}:{ts}"`
- **entities** — `@mentions`, `#channel` refs, and URLs are regex‑extracted into
  `entities_hint` ([handlers/slack.py:117‑145](../../../services/ingestion/handlers/slack.py#L117-L145)):
  - `<@U01ABC>` → `{"type":"slack_user","id":"U01ABC"}`
  - `<#C01ENG>` → `{"type":"slack_channel","id":"C01ENG"}`
  - `<https://…>` → `{"type":"url","id":"https://…"}`
- **`channel_type`** (`im`/`mpim`/`channel`) is preserved on `content` — this is
  the *only* thing distinguishing a DM observation from a channel observation;
  they share one channel and one dedup namespace.

### 6.3 Message subtypes

Two mutation subtypes get special handling
([handlers/slack.py:176‑214](../../../services/ingestion/handlers/slack.py#L176-L214)):

- **`message_deleted`** → rejected cleanly (no content to ingest; deletion
  tracking is out of scope).
- **`message_changed`** → the real text lives in the nested `message` object.
  Since dedup is insert‑only, reusing the original `ts` would silently drop the
  edit, so the edit is keyed on its **own edit timestamp** (`edited_ts`) and
  linked back via `content.original_ts`.

Other system events (channel joins, bot adds, etc.) carry no `text` and are
rejected with a 400.

---

## 7. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │                                                                   │
  PUBLIC/PRIVATE CHANNELS │  planner: conversations.list (bot)                               │
                          │     └─► one slack_channel_window shard per channel               │
                          │  fetcher: conversations.history (bot, cursor-paged)              │
                          │     └─► inject `channel` → event_callback                        │
                          │                                                                   │
  1:1 DMs  +  GROUP DMs   │  planner: per consenting user in slack_dm_installations:         │
  (im)        (mpim)      │     conversations.list(types=im,mpim) (user xoxp token)          │
                          │       └─► one slack_dm_window shard per conversation             │
                          │  fetcher: conversations.history (user token, cursor-paged)       │
                          │     └─► inject `channel` + `channel_type` → event_callback       │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
                          ┌──────────────────────── LIVE (push) ──────────────────────────┐│
   ANY new message  ──────►  Slack Events API  ──HTTP POST──► /webhooks/slack/events       ││
   (channel/im/mpim)      │     verify v0 HMAC (replay-safe) → event_callback              ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_slack_message            │
                                                            │  (one handler, one dedup space)  │
                                                            │  external_id = "{channel}:{ts}"  │
                                                            │  → ObservationDraft              │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Channels, DMs, and group DMs all land on
   `slack:message` with `external_id="{channel}:{ts}"`. A backfilled message and
   its live twin dedup to a single observation. DM‑vs‑channel is just the
   `content.channel_type` attribute (`im` / `mpim` / `channel`).
2. **Two tokens, by necessity.** Channels use the workspace **bot** token; DMs
   and group DMs use a **per‑user** token, because Slack forbids bots from
   reading direct messages.
3. **Consent‑shaped DM coverage.** DM ingestion exists only for users who granted
   a user token (rows in `slack_dm_installations`); one user's revoked token
   never breaks the rest of the plan.
4. **Cursor pagination + bounded retries everywhere**, with `429 Retry-After`
   honoured without retaining a worker through a long provider cooldown.

---

## 8. Compliance with Slack's rules & configuration

Verified against Slack's official docs (rate limits, OAuth, scopes, signing).

### 8.1 Rate-limit tiers — `SLACK_API_TIER`

On **2025-05-29** Slack moved `conversations.history` / `conversations.replies`
from **Tier 3 (50+/min, ≤1000 obj)** to **Tier 1 (1/min, ≤15 obj)** for *new
commercially-distributed apps that are not Marketplace-approved* and for *new
installs* of such apps. Marketplace and internal apps keep Tier 3. An app cannot
detect its own listing status at runtime, so the tier is operator config:

| Env var | Default | Meaning |
|---------|---------|---------|
| `SLACK_API_TIER` | `3` | Tier for `conversations.history`/`.replies`. Set to **`1`** for a non-Marketplace ("unlisted") distributed app; `3` for Marketplace/internal. Accepts `1`–`4`. |
| `SLACK_RETRY_WALL_BUDGET_S` | tier-derived (`75` if tier 1, else `30`) | Total wall-clock budget for one operation, including attempts and short inline waits. |
| `SLACK_MAX_INLINE_RETRY_AFTER_S` | `30` | Maximum provider cooldown that may be waited inline. A longer delay becomes `RetryLater` even when it fits inside the total wall budget. |

For example, the approximately 60-second Tier-1 cooldown is published to the
shared quota coordinator and returned as `RetryLater`; it does not sleep the
fetch worker. A short transient cooldown remains an inline retry.

`conversations.list` (Tier 2) and `users.info` (Tier 4) are unaffected by the
2025 change and carry their canonical tiers
([rate_limit/buckets.py](../../../services/ingestion/rate_limit/buckets.py)).

### 8.2 Verified compliant

- **Signing** — Slack v0 HMAC (`v0:{ts}:{body}`), constant-time compare, 300 s
  replay window. ✅
- **OAuth v2** — `scope` (bot) + `user_scope` (user) split; bot/user tokens read
  from `access_token` / `authed_user.access_token`. ✅
- **Least privilege** — DM scopes (`im/mpim:*`) are requested only as *user*
  scopes, not bot scopes. ✅
- **Pagination** — `response_metadata.next_cursor` everywhere. ✅
- **Reconciler `oldest`** — Slack's `oldest` is exclusive by default, so the
  gap-probe `conversations.history(oldest=newest_seen_ts, limit=1)` correctly
  returns only strictly-newer messages (no false positives). ✅
