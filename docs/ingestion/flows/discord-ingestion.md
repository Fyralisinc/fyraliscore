# Discord Ingestion — How Fyralis Pulls Discord Data

This document explains, in detail, **how Discord data enters Fyralis**: which
Discord REST APIs are called, with which token, and how Discord's signal set —
**channel messages** (`MESSAGE_CREATE`) and **slash‑command interactions** — is
each ingested.

Discord is the **only source with three ingress surfaces**: a historical
**backfill PULL** over the REST API, a persistent **Gateway PUSH** (a WSS bot
connection that streams live messages), and a thin **HTTP webhook PUSH** (Discord
posts slash‑command interactions). The first two converge on **one** handler; the
webhook lands on a **second** handler in a distinct channel.

It deliberately stops at the point where a Discord event becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope. (`docs/ingestion/sources/discord.md`
is the short reference card; this is the deep version.)

---

## 1. The two ways data arrives

Discord data reaches Fyralis through **three paths over two transports**, mapping
onto exactly **two** handlers / two dedup channels:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* channel history via the **REST API** (`GET /channels/{id}/messages`) | [planners/discord.py](../../../services/ingest/ingestion/planners/discord.py), [fetchers/discord.py](../../../services/ingest/ingestion/fetchers/discord.py) |
| **Live messages (gateway)** | New `MESSAGE_CREATE` in a guild channel | Fyralis holds a **persistent WSS Gateway** connection; Discord streams op‑0 DISPATCH frames | [gateway/client.py](../../../services/ingest/integrations/discord/gateway/client.py), [gateway/dispatch.py](../../../services/ingest/integrations/discord/gateway/dispatch.py), [handlers/discord.py](../../../services/ingest/ingestion/handlers/discord.py) |
| **Live interactions (webhook)** | A user runs a slash command | Discord *pushes* an **HTTP interaction** to Fyralis's webhook edge | [webhooks/router.py](../../../services/app/webhooks/router.py), [signatures/discord.py](../../../services/app/webhooks/signatures/discord.py), [handlers/discord.py](../../../services/ingest/ingestion/handlers/discord.py) |

These resolve to **two channels** via the `(source, ingress_kind)` →
channel map ([normalizer/channel_mapping.py:27‑32](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L27-L32)):

```
("discord", "gateway")  → "discord:message"      # IN-12 MESSAGE_CREATE
("discord", "backfill") → "discord:message"      # M6.7 — SAME handler as gateway
("discord", "webhook")  → "discord:interaction"  # IN-09 slash commands
```

So **backfill and the Gateway share the `discord:message` handler**
([handlers/discord.py:213‑273](../../../services/ingest/ingestion/handlers/discord.py#L213-L273)),
while the HTTP webhook is the *only* feeder of the `discord:interaction` handler
([handlers/discord.py:116‑159](../../../services/ingest/ingestion/handlers/discord.py#L116-L159)).
Both handlers derive the **same shape** of dedup key
([idempotency/__init__.py:51‑54](../../../services/ingest/ingestion/idempotency/__init__.py#L51-L54)):

```
external_id = "discord:{snowflake}"
    discord:message      → snowflake = message id
    discord:interaction  → snowflake = interaction id
```

Because Discord snowflakes are immutable, a message that is both **backfilled**
*and* delivered **live over the Gateway** collapses into **one** observation. The
backfill fetcher exists precisely to **reshape REST message objects into the
`MESSAGE_CREATE` shape** so the one `discord:message` handler treats both
identically ([fetchers/discord.py:8‑19](../../../services/ingest/ingestion/fetchers/discord.py#L8-L19)).

> A cross‑channel collision between an interaction id and a message id is
> impossible even if two snowflakes coincided: the unique index is
> `(source_channel, external_id, occurred_at)`, and `source_channel` differs
> (`discord:interaction` vs `discord:message`)
> ([handlers/discord.py:15‑19](../../../services/ingest/ingestion/handlers/discord.py#L15-L19)).

> **No single dedup namespace across all of Discord** (contrast Slack/GitHub).
> There are **two** namespaces, one per channel — interactions and messages are
> genuinely different object kinds and never need to dedup against each other.

---

## 2. Authentication & token model

Discord ingestion uses **one credential to read** and a **separate, asymmetric
key to verify inbound webhooks** — they are not the same secret:

| Credential | Where | Used by | Notes |
|-----------|-------|---------|-------|
| **Bot token** | `DISCORD_BOT_TOKEN` (env) | `DiscordClient` (REST) + Gateway worker | App‑level, **one per Discord app**, not per‑installation. Sent as `Authorization: Bot {token}` |
| App **Ed25519 public key** | `WEBHOOK_SECRET_DISCORD` (env), mirrored per‑guild into `encrypted_secrets` as `discord_public_key:{guild_id}` | the webhook verifier | Discord signs interactions with the matching *private* key; verification is **asymmetric** |
| OAuth `access_token` | `encrypted_secrets` as `discord_bot_token:{guild_id}`, referenced by `provider_installations.secret_ref` | (not used to authorize bot calls) | A *user* Bearer; reserved for future refresh flows |

### 2.1 The bot token is app‑scoped, not per‑installation

`DiscordClient._resolve_bot_token` reads `DISCORD_BOT_TOKEN` from the environment
on first use and caches it on the instance
([client.py:84‑107](../../../services/ingest/integrations/discord/client.py#L84-L107)).
A missing/empty value raises `DiscordApiError(code="discord_secret_unavailable")`.
The rationale is explicit in the docstring: a Discord **bot token is one per app
in the Developer Portal**, so env resolution is the correct model — the OAuth
`access_token` returned at install time is a *user* Bearer that **cannot**
authorize bot‑scope API calls
([client.py:1‑23](../../../services/ingest/integrations/discord/client.py#L1-L23),
[84‑96](../../../services/ingest/integrations/discord/client.py#L84-L96)).

> **Contrast with GitHub/Slack.** GitHub mints a per‑installation token; Slack
> stores per‑team/per‑user tokens. Discord's *read* path uses a **single
> app‑global** bot token (env), while the per‑guild `discord_bot_token:{gid}`
> rows hold the OAuth user Bearer for a *future* refresh flow, not for reads.

### 2.2 The interaction signature is asymmetric (public key, not HMAC)

Interactions are verified with **Ed25519**, not HMAC. The "secret" stored is the
application's **public** key; Discord signs with the private key it holds. This
is why the verifier uses `pynacl`'s `VerifyKey.verify` rather than a constant‑time
HMAC compare ([signatures/discord.py:1‑20](../../../services/app/webhooks/signatures/discord.py#L1-L20)).
See §8.1.

### 2.3 The OAuth install flow (how a guild gets registered)

`services/ingest/integrations/discord/oauth.py` mirrors the Slack OAuth shape with
Discord‑specific differences ([oauth.py:1‑34](../../../services/ingest/integrations/discord/oauth.py#L1-L34)):

1. **`GET /integrations/discord/install`** (Bearer‑authed) — issues an
   HMAC‑signed `state` token bound to the session's `tenant_id` (it reuses the
   provider‑agnostic Slack state helpers, passing `provider="discord"`), then
   `302`s to `https://discord.com/oauth2/authorize` with
   `scope="applications.commands bot"` and `permissions="3072"`
   (`send_messages` + `view_channel`)
   ([oauth.py:73‑77](../../../services/ingest/integrations/discord/oauth.py#L73-L77),
   [119‑181](../../../services/ingest/integrations/discord/oauth.py#L119-L181)).
2. **`GET /integrations/discord/callback`** (public, state‑authed) — verifies the
   HMAC, then **`POST`s `https://discord.com/api/v10/oauth2/token`** to exchange
   the `code`. Discord requires **HTTP Basic** `(client_id:client_secret)` for
   the exchange (not body params like Slack)
   ([oauth.py:188‑199](../../../services/ingest/integrations/discord/oauth.py#L188-L199)).
3. `_persist_secrets` stores the OAuth `access_token` under
   `discord_bot_token:{guild_id}` **and** mirrors the app public key
   (`WEBHOOK_SECRET_DISCORD`) under `discord_public_key:{guild_id}` — so the
   DB‑backed `load_secrets` path resolves the verifier key uniformly via
   `provider_installations.secret_ref`
   ([oauth.py:247‑281](../../../services/ingest/integrations/discord/oauth.py#L247-L281)).
4. The callback **upserts** a `provider_installations` row keyed on
   `(provider='discord', installation_id=guild_id)`, emits an
   `onboarding_triggers` row in the **same transaction**, rejects a cross‑tenant
   rebind with `installation_collision`, and then **registers the `/fyralis`
   global slash command** as a side effect of install
   ([oauth.py:288‑319](../../../services/ingest/integrations/discord/oauth.py#L288-L319),
   [546‑586](../../../services/ingest/integrations/discord/oauth.py#L546-L586)).

---

## 3. The Discord API surface actually called

All outbound REST funnels through `DiscordClient._request`
([client.py:131‑280](../../../services/ingest/integrations/discord/client.py#L131-L280)),
which:

- sets `Authorization: Bot {token}` (unless `require_bot_token=False`),
- honours Discord's **`Retry-After`** on `429` within a bounded budget
  (`≤3 attempts`, `≤30 s` wall) — `Retry-After` is parsed as **float seconds**
  (often `<1`) ([client.py:210‑227](../../../services/ingest/integrations/discord/client.py#L210-L227),
  [398‑405](../../../services/ingest/integrations/discord/client.py#L398-L405)),
- retries transport errors with exponential backoff inside the same wall budget,
- triggers the **bot‑kick chokepoint** on `401` / `403 code=50001` (§10), and
- maps other non‑2xx to `DiscordApiError`. Crucially, the structured log records
  the **unsubstituted** endpoint template, never the raw IDs — the raw
  `guild_id` is **never logged** (SC‑006)
  ([client.py:200‑208](../../../services/ingest/integrations/discord/client.py#L200-L208)).

The endpoints invoked for ingestion:

| Discord endpoint | Wrapper | Purpose | Code |
|------------------|---------|---------|------|
| `GET /users/@me/guilds` | `list_guilds()` | enumerate the bot's guilds (planner shard source) | [client.py:336‑361](../../../services/ingest/integrations/discord/client.py#L336-L361) |
| `GET /guilds/{id}/channels` | `list_guild_channels()` | a guild's channels (planner filters to text) | [client.py:363‑371](../../../services/ingest/integrations/discord/client.py#L363-L371) |
| `GET /channels/{id}/messages` | `get_messages()` | one page of channel history (backfill + reconciler probe) | [client.py:373‑395](../../../services/ingest/integrations/discord/client.py#L373-L395) |
| `POST /webhooks/{app}/{interaction_token}` | `post_followup_message()` | async follow‑up to a slash command (not ingestion) | [client.py:286‑301](../../../services/ingest/integrations/discord/client.py#L286-L301) |
| `GET /guilds/{gid}/members/{uid}` | `get_guild_member()` | actor enrichment | [client.py:303‑308](../../../services/ingest/integrations/discord/client.py#L303-L308) |
| `POST /applications/{app}/commands` | `post_register_global_command()` | register `/fyralis` at install | [client.py:317‑327](../../../services/ingest/integrations/discord/client.py#L317-L327) |

### 3.1 Pagination — snowflake cursors

Discord pages by **snowflake**, not page numbers or opaque cursors. Snowflakes
are sortable integers, so "older than X" and "newer than X" are direct comparisons:

- **`list_guilds`** paginates with `after=<last id>` (Discord caps the page at
  **200**), looping to completion so a bot in >200 guilds is never silently
  truncated ([client.py:336‑361](../../../services/ingest/integrations/discord/client.py#L336-L361)).
- **`get_messages`** takes `before` / `after` / `limit` and returns **one page**
  (newest‑first); the fetcher persists the snowflake cursor and resumes
  ([client.py:373‑395](../../../services/ingest/integrations/discord/client.py#L373-L395)).
- **`list_guild_channels`** is a single unpaginated call (a guild's channel list
  is bounded) ([client.py:363‑371](../../../services/ingest/integrations/discord/client.py#L363-L371)).

### 3.2 Rate limits

A single client‑side token bucket covers Discord message reads
([rate_limit/buckets.py:90](../../../services/ingest/ingestion/rate_limit/buckets.py#L90)):

```
("discord", "channels_messages"): capacity 30, refill 5.0/s
```

There is one logical bucket (`channels_messages`) plus the `Retry-After`‑aware
retry in the client.

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per *sampled* channel**, all
of `shard_kind = "discord_channel_window"`
([planners/discord.py:64‑97](../../../services/ingest/ingestion/planners/discord.py#L64-L97)).

There is **a single shard family** (no Class‑A/Class‑B split like GitHub):

| Class | Signal | REST shape | Fetch path |
|-------|--------|-----------|------------|
| Channel window | guild text messages | `GET /channels/{id}/messages` (newest‑first) | `fetch_page_discord` |

### 4.1 Enumerate guilds → channels → **5% sparse sample**

```python
guilds   = await ctx.source_client.list_guilds()            # GET /users/@me/guilds
channels = await ctx.source_client.list_guild_channels(gid) # per guild
text     = [c for c in channels if c.get("type") == 0]      # GUILD_TEXT only
sampled  = _sampled_channels(tenant_str, text)              # deterministic 5%
```

The planner does **not** backfill every channel. Per LLD §3.4 it takes a
**deterministic 5% sparse sample** of each guild's text channels
([planners/discord.py:21‑61](../../../services/ingest/ingestion/planners/discord.py#L21-L61)):

- **`SAMPLING_RATE`** defaults to `0.05`, overridable via
  **`DISCORD_BACKFILL_SAMPLING_RATE`** (set `1.0` for full coverage against a
  mock) ([planners/discord.py:30](../../../services/ingest/ingestion/planners/discord.py#L30)).
- The sample is **stable across runs and process restarts**: channels are sorted
  by id, then `random.sample` is seeded from a **SHA‑256** digest of
  `(tenant_id, SAMPLING_VERSION)`. Python's salted builtin `hash()` was rejected
  precisely because it gave a different set every restart
  ([planners/discord.py:33‑61](../../../services/ingest/ingestion/planners/discord.py#L33-L61)).
- `k = max(1, int(len(channels) * rate))` — every non‑empty guild yields at least
  one channel ([planners/discord.py:60](../../../services/ingest/ingestion/planners/discord.py#L60)).

Each shard carries `guild_id`, `channel_id`, `channel_name`, `is_sampled=True`,
`sampling_version`, `installation_id`, at a baseline `recency_score=1.0`
([planners/discord.py:84‑96](../../../services/ingest/ingestion/planners/discord.py#L84-L96)).

---

## 5. Backfill fetch — channel window (the `discord:message` path)

`fetch_page_discord` ([fetchers/discord.py:63‑108](../../../services/ingest/ingestion/fetchers/discord.py#L63-L108))
fetches one page, advances the snowflake cursor, and reshapes each REST message
into the `MESSAGE_CREATE` shape the `discord:message` handler consumes.

### 5.1 Cursor

```python
class DiscordCursor(BaseModel):
    before_snowflake: str | None = None       # next page goes OLDER
    oldest_seen_snowflake: str | None = None
    newest_seen_snowflake: str | None = None   # gap baseline for the reconciler
    messages_seen: int = 0
```

([fetchers/discord.py:39‑45](../../../services/ingest/ingestion/fetchers/discord.py#L39-L45)).
The fetcher walks **backward in time**: each page sets `before` to the oldest
snowflake seen so far. End‑of‑data is a short page (`< 100`)
([fetchers/discord.py:73‑102](../../../services/ingest/ingestion/fetchers/discord.py#L73-L102)).

### 5.2 Reshape REST message → `MESSAGE_CREATE` body (external‑id parity)

The `discord:message` handler **requires** a `guild_id`, but the REST
`/channels/{id}/messages` objects **omit** it (the guild was implicit in the
path). So the fetcher **injects `guild_id` from the shard** and ensures
`channel_id` is present, so the derived `external_id="discord:{id}"` matches the
live Gateway twin exactly ([fetchers/discord.py:79‑87](../../../services/ingest/ingestion/fetchers/discord.py#L79-L87)):

```python
records = [{
    **m,
    "guild_id": guild_id,                       # REST omits it; handler requires it
    "channel_id": m.get("channel_id", channel_id),
} for m in messages]
```

Unlike GitHub, Discord carries **no load‑bearing webhook headers** for messages,
so **no `webhook_metadata`** is attached ([fetchers/discord.py:17‑19](../../../services/ingest/ingestion/fetchers/discord.py#L17-L19)).

---

## 6. The handlers — shaping events into `ObservationDraft`

Both handlers live in [handlers/discord.py](../../../services/ingest/ingestion/handlers/discord.py)
and both stamp `kind="signal"` and `trust_tier="attested_agent"` (from
`CHANNEL_TRUST_MAP` ([handlers/__init__.py:49‑51](../../../services/ingest/ingestion/handlers/__init__.py#L49-L51))).

| Channel | Handler | `external_id` | `occurred_at` | Trust tier |
|---------|---------|---------------|---------------|------------|
| `discord:message` (gateway + backfill) | `handle_discord_message` | `discord:{message_id}` | message `timestamp` (ISO‑8601), else `now()` | `attested_agent` |
| `discord:interaction` (HTTP webhook) | `handle_discord_webhook` | `discord:{interaction_id}` | **`now()`** | `attested_agent` |

### 6.1 `discord:message` — `handle_discord_message`

([handlers/discord.py:213‑273](../../../services/ingest/ingestion/handlers/discord.py#L213-L273))

- `content_text` = `message.content` **verbatim** (no markdown strip).
- `source_actor_ref` = `discord:{author.id}` (top‑level `author`)
  ([handlers/discord.py:189‑194](../../../services/ingest/ingestion/handlers/discord.py#L189-L194)).
- `occurred_at` parses Discord's ISO‑8601 `timestamp`, falling back to `now(UTC)`
  if missing/unparseable — the timestamp is metadata, not load‑bearing for dedup
  ([handlers/discord.py:174‑186](../../../services/ingest/ingestion/handlers/discord.py#L174-L186)).
- **Privacy (SC‑006):** the raw `guild_id` is **never** put in `content.metadata`
  — only a non‑reversible 8‑byte BLAKE2b `short_guild_hash`; metadata also carries
  `channel_id`, `mention_user_ids`, `attachment_count`
  ([handlers/discord.py:162‑171](../../../services/ingest/ingestion/handlers/discord.py#L162-L171),
  [249‑257](../../../services/ingest/ingestion/handlers/discord.py#L249-L257)).
- **Required fields:** a missing string `id` or `guild_id` raises `ValidationError`
  — a guild‑less (DM) message should have been filtered upstream
  ([handlers/discord.py:237‑247](../../../services/ingest/ingestion/handlers/discord.py#L237-L247)).
- The handler **does not** re‑apply the bot/webhook filter (§7.2); doing so would
  mask a worker bug rather than fix it
  ([handlers/discord.py:217‑223](../../../services/ingest/ingestion/handlers/discord.py#L217-L223)).

### 6.2 `discord:interaction` — `handle_discord_webhook`

([handlers/discord.py:116‑159](../../../services/ingest/ingestion/handlers/discord.py#L116-L159))

- `content_text` = the **primary string option's value** for an
  ApplicationCommand — walking `data.options[0].value` (or a nested subcommand
  option) ([handlers/discord.py:70‑99](../../../services/ingest/ingestion/handlers/discord.py#L70-L99)).
- `source_actor_ref` = `discord:{member.user.id}` (guild context) or
  `discord:{user.id}` (DM context) ([handlers/discord.py:102‑113](../../../services/ingest/ingestion/handlers/discord.py#L102-L113)).
- **Credential stripping:** `content.metadata` is the full payload **minus the
  per‑interaction `token`** — a follow‑up credential that is **never persisted**
  (plus defensive stripping of `member.user.token` / `user.token`)
  ([handlers/discord.py:49‑67](../../../services/ingest/ingestion/handlers/discord.py#L49-L67), [138‑139](../../../services/ingest/ingestion/handlers/discord.py#L138-L139)).
- `entities_hint` carries `discord_application`, `discord_guild`,
  `discord_channel` typed refs ([handlers/discord.py:130‑136](../../../services/ingest/ingestion/handlers/discord.py#L130-L136)).
- `occurred_at` is `now(UTC)` — the interaction payload carries no authoritative
  event time ([handlers/discord.py:148](../../../services/ingest/ingestion/handlers/discord.py#L148)).

---

## 7. Live ingestion #1 — the Gateway (persistent WSS, `ingress_kind="gateway"`)

Unlike an HTTP webhook, the Gateway is a **persistent outbound WSS connection** the
worker holds open; Discord **streams** live events down it. This is the same
shadow‑write/`ingress_kind="gateway"` shape used by Telegram/Signal (push‑less
sources), not an HTTP edge.

### 7.1 The connection

`DiscordGatewayClient` connects to `wss://…/gateway/bot`, sends **IDENTIFY**
(op 2) with an intent bitmask, and processes op‑0 DISPATCH frames
([gateway/client.py:45‑63](../../../services/ingest/integrations/discord/gateway/client.py#L45-L63)).
The intents are `GUILDS | GUILD_MESSAGES | MESSAGE_CONTENT`
(`= 33281`); `MESSAGE_CONTENT` is a **privileged** intent required to populate the
`content` field ([gateway/client.py:49‑53](../../../services/ingest/integrations/discord/gateway/client.py#L49-L53)).
Because two pods sharing one bot token would cause duplicate delivery and
"Already authenticated" rejections, a **Redis lease (`leader_lock`)** ensures a
single connected leader; non‑holders wait
([gateway/leader_lock.py:1‑20](../../../services/ingest/integrations/discord/gateway/leader_lock.py#L1-L20)).

### 7.2 Dispatch + the bot/webhook filter chain

Every DISPATCH frame routes through `handle_dispatch`; only `MESSAGE_CREATE` is
ingested ([gateway/dispatch.py:76‑97](../../../services/ingest/integrations/discord/gateway/dispatch.py#L76-L97)).
`handle_message_create` applies a filter chain **before any DB hit**
([gateway/dispatch.py:100‑143](../../../services/ingest/integrations/discord/gateway/dispatch.py#L100-L143)):

1. `author.bot is True` → drop (`filtered_bot_total{source=self|other_bot}`).
2. `webhook_id is not None` → drop (`filtered_bot_total{source="webhook"}`) — a
   Discord *channel webhook* message, distinct from Fyralis's HTTP webhook path.
3. No `guild_id` (a DM) → drop (out of scope for v1).
4. Resolve tenant via the shared `TenantResolver` (`guild_id` → installation).
   `UnknownInstallation` / `PayloadMissing` → drop, logging only the
   `short_guild_hash` (never the raw `guild_id`).
5. Otherwise → `ingest("discord:message", message, …)` — the **same** path
   backfill uses, idempotent on `external_id`
   ([gateway/dispatch.py:187‑211](../../../services/ingest/integrations/discord/gateway/dispatch.py#L187-L211)).

### 7.3 Cutover + shadow write

The Gateway path mirrors the webhook router's Kafka cutover. When the tenant's
`kafka_path_enabled` flag is on (default‑on, kill‑switch‑off model), the frame is
published to `ingestion.raw` under `source="discord", ingress_kind="gateway"` and
the function returns; on publish failure it **falls back to inline `ingest()`** so
the message is never dropped ([gateway/dispatch.py:145‑185](../../../services/ingest/integrations/discord/gateway/dispatch.py#L145-L185)).
When the flag is off, after a successful inline ingest the worker performs a
best‑effort **shadow write** of the canonical raw body (orjson sorted keys, so
WSS retransmissions of the same `message_id` are byte‑identical), also under
`ingress_kind="gateway"` ([gateway/dispatch.py:213‑349](../../../services/ingest/integrations/discord/gateway/dispatch.py#L213-L349)).
The raw body **never** contains the raw `guild_id` — only its short hash
([gateway/dispatch.py:233‑254](../../../services/ingest/integrations/discord/gateway/dispatch.py#L233-L254)).

---

## 8. Live ingestion #2 — the interaction webhook (`ingress_kind="webhook"`)

When a user runs a slash command, Discord **POSTs an interaction** to Fyralis's
webhook edge. The router maps provider `discord` → channel `discord:interaction`
([webhooks/router.py:437‑442](../../../services/app/webhooks/router.py#L437-L442)),
and `VERIFIERS["discord"]` is the Ed25519 verifier
([signatures/__init__.py:44‑49](../../../services/app/webhooks/signatures/__init__.py#L44-L49)).

### 8.1 Signature verification (Ed25519, asymmetric)

`DiscordVerifier.verify` ([signatures/discord.py:55‑138](../../../services/app/webhooks/signatures/discord.py#L55-L138)):

```
message  = X-Signature-Timestamp || raw_body
verify(public_key, signature=X-Signature-Ed25519, message)   # pynacl VerifyKey
```

- The signature header is **hex** (`bytes.fromhex`); a non‑hex header is a
  `malformed_signature_header`.
- The "secret" is the **app public key** (asymmetric — Discord holds the private
  key); each active key is tried in turn so a key rotation overlaps cleanly. A
  non‑hex *configured* key is treated as a non‑match (config error, not an attack)
  rather than short‑circuiting the rotation
  ([signatures/discord.py:104‑131](../../../services/app/webhooks/signatures/discord.py#L104-L131)).
- **Replay window:** Discord signs a timestamp; Fyralis enforces a **300 s**
  window (`DISCORD_MAX_TIMESTAMP_AGE_S`) so a captured request can't be replayed
  ([signatures/discord.py:36](../../../services/app/webhooks/signatures/discord.py#L36),
  [83‑90](../../../services/app/webhooks/signatures/discord.py#L83-L90)).

### 8.2 The PING handshake

Discord's interaction endpoint must answer a **PING** (`type=1`) with a PONG
(`{"type": 1}`) — including the bootstrap ping at endpoint registration, which
predates any customer install. `_is_discord_ping`
([webhooks/router.py:504‑506](../../../services/app/webhooks/router.py#L504-L506))
detects it, and the router answers `200 {"type": 1}` **after** signature
verification but **before** unknown‑installation enforcement
([webhooks/router.py:868‑869](../../../services/app/webhooks/router.py#L868-L869)).

### 8.3 The one intentional inline exception (synchronous response shape)

Discord requires a **synchronous response body** for an interaction. For an
ApplicationCommand (`type=2`), the router emits a
`CHANNEL_MESSAGE_WITH_SOURCE` (`{"type": 4, "data": {…, "flags": 64}}`,
ephemeral) so the user sees an acknowledgement instead of "the application didn't
respond in time" ([webhooks/router.py:1201‑1232](../../../services/app/webhooks/router.py#L1201-L1232)).
Because the async `202` cutover contract can't carry that shape, Discord webhooks
are deliberately **not** in the cutover provider set and stay **inline**
([webhooks/router.py:154‑164](../../../services/app/webhooks/router.py#L154-L164)).
Tenant resolution is from `guild_id` (guild interactions) → `application_id` (DM /
global commands) ([tenant_resolver.py:281‑287](../../../services/app/webhooks/tenant_resolver.py#L281-L287)).

> **Gateway vs HTTP webhook, at a glance.** The Gateway is a long‑lived WSS pull
> the worker holds open (`ingress_kind="gateway"`, channel `discord:message`); the
> interaction webhook is a classic HTTP push (`ingress_kind="webhook"`, channel
> `discord:interaction`) that must return a Discord‑shaped body synchronously.

---

## 9. Reconciliation — gap detection (sampling‑aware)

`reconcile_discord` ([reconcilers/discord.py:122‑159](../../../services/ingest/ingestion/reconcilers/discord.py#L122-L159))
re‑checks completed shards for new activity. It is **sampling‑aware**: only shards
with `is_sampled=True` are probed — the unsampled 95% of channels are out of scope
by definition ([reconcilers/discord.py:68‑78](../../../services/ingest/ingestion/reconcilers/discord.py#L68-L78)).

Per shard, it loads the cursor's `newest_seen_snowflake` and issues a cheap probe
`get_messages(channel_id, after=<newest>, limit=1)`. Any returned message means a
gap; the reconciler reshares a `discord_channel_window` shard at
**`recency_score=1.5`**, carrying `parent_shard_id` and `gap_baseline_snowflake`
([reconcilers/discord.py:86‑119](../../../services/ingest/ingestion/reconcilers/discord.py#L86-L119)).
A probe exception is logged and treated as "no gap" (it does not fail the run)
([reconcilers/discord.py:92‑97](../../../services/ingest/ingestion/reconcilers/discord.py#L92-L97)).

> Discord snowflakes encode time, so `after=<newest_seen>` returns strictly newer
> messages — no `updated_at`/ETag machinery is needed (contrast GitHub/Slack).

---

## 10. Revocation chokepoint (bot kick / lost access)

The outbound client is the single chokepoint that disables an installation on a
documented authorization failure, via `_trigger_chokepoint` →
`_disable_and_zeroize_discord`
([client.py:120‑129](../../../services/ingest/integrations/discord/client.py#L120-L129),
[229‑250](../../../services/ingest/integrations/discord/client.py#L229-L250)):

- **`401`** (bad/revoked credentials), or
- **`403` with body `{"code": 50001}`** (Missing Access — i.e. the bot was kicked
  / lost channel access).

Either flips `provider_installations.enabled=FALSE` (idempotent) **and** deletes
the guild's `encrypted_secrets` rows (zeroize), so the next inbound interaction
resolves to `unknown_installation`
([uninstall.py:40‑145](../../../services/ingest/integrations/discord/uninstall.py#L40-L145)).
A `429` is **not** a chokepoint — it's retried within budget; other 4xx/5xx are a
plain `DiscordApiError` ([client.py:210‑269](../../../services/ingest/integrations/discord/client.py#L210-L269)).

> Per Clarifications, the IN‑09 **outbound‑401 chokepoint** is the canonical
> kick‑detection path — the Gateway's `GUILD_DELETE` frame is *metric‑only* and
> does not itself disable the install
> ([gateway/dispatch.py:18‑20](../../../services/ingest/integrations/discord/gateway/dispatch.py#L18-L20),
> [89‑93](../../../services/ingest/integrations/discord/gateway/dispatch.py#L89-L93)).

---

## 11. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  bot token (env DISCORD_BOT_TOKEN) → Authorization: Bot {token}  │
   ALL BOT GUILDS         │  planner: GET /users/@me/guilds → GET /guilds/{id}/channels      │
                          │     └─► deterministic 5% sample of GUILD_TEXT channels           │
                          │     └─► one discord_channel_window shard per sampled channel     │
   channel history        │  fetcher: GET /channels/{id}/messages (before=<oldest>, snowflake)
                          │     └─► reshape REST msg → MESSAGE_CREATE shape (inject guild_id) │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
   LIVE MESSAGES          ┌──────────────── GATEWAY (push, persistent WSS) ────────────────┐│
   (ingress=gateway) ─────►  bot WSS connect → IDENTIFY (intents 33281) → op-0 DISPATCH     ││
                          │     filter author.bot / webhook_id / DM → resolve tenant         ││
                          │     cutover→ingestion.raw OR inline ingest() (+shadow write)     ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            ├─► discord:message
                                                            ┌───────────────────────────────▼─┐  external_id="discord:{msg_id}"
                                                            │  handle_discord_message          │
                                                            └──────────────────────────────────┘
                                                                                            │
   LIVE INTERACTIONS      ┌──────────────────── WEBHOOK (push, HTTP) ─────────────────────┐│
   (ingress=webhook) ─────►  Discord interaction ──POST──► /webhooks/discord               ││
                          │     verify Ed25519 (X-Signature-Ed25519 over ts||body, 300s)   ││
                          │     PING type=1 → 200 {"type":1} ; type=2 → inline (sync shape) ││
                          └───────────────────────────────────────────────────────────────┘│
                                                            ┌───────────────────────────────▼─┐  discord:interaction
                                                            │  handle_discord_webhook          │  external_id="discord:{int_id}"
                                                            │  → ObservationDraft               │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **Two handlers, two dedup namespaces.** Backfill + Gateway both land on
   `discord:message` (`external_id="discord:{message_id}"`) — a backfilled
   message and its live Gateway twin dedup to one observation. The HTTP webhook
   is the only feeder of `discord:interaction` (`external_id="discord:{interaction_id}"`).
   `source_channel` keeps the two namespaces disjoint.
2. **One read credential.** A single **app‑level bot token** (`DISCORD_BOT_TOKEN`)
   reads everything (REST + Gateway). The OAuth user Bearer is *not* used to
   authorize bot calls.
3. **Asymmetric webhook verification.** Interactions are verified with **Ed25519**
   against the app **public** key (not HMAC), with a 300 s replay window.
4. **Sparse, deterministic backfill.** 5% per‑guild sampling, stable across
   process restarts (SHA‑256 seed); the reconciler only gap‑checks sampled shards.
5. **Privacy by construction (SC‑006).** The raw `guild_id` never appears in
   logs, metadata, entities, or shadow‑write bodies — only a BLAKE2b short hash.
6. **One intentional inline exception.** Discord interactions stay inline (never
   on the Kafka cutover) because Discord requires a synchronous response body.

---

## 12. Configuration & compliance

Verified against Discord's official docs (OAuth2, interactions security/Ed25519,
Gateway intents, REST rate limits).

### 12.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `DISCORD_BOT_TOKEN` | — (required to read) | app‑level bot token; `Authorization: Bot {token}` for REST + Gateway |
| `DISCORD_CLIENT_ID` / `DISCORD_CLIENT_SECRET` | — (required for install) | OAuth2 app credentials (Basic auth on token exchange) |
| `DISCORD_REDIRECT_URI` | — (required for install) | OAuth2 redirect target |
| `WEBHOOK_SECRET_DISCORD` | — (required for interactions) | app **Ed25519 public key** (hex); mirrored per‑guild into `encrypted_secrets` |
| `DISCORD_MAX_TIMESTAMP_AGE_S` | `300` | interaction signature replay window |
| `DISCORD_BACKFILL_SAMPLING_RATE` | `0.05` | fraction of each guild's text channels to backfill (`1.0` = full) |

### 12.2 Verified compliant

- **OAuth2** — `applications.commands bot` scope, `permissions=3072`, **HTTP Basic**
  token exchange (Discord requirement). ✅
- **Interaction signing** — Ed25519 `VerifyKey.verify` over
  `X-Signature-Timestamp || body`, hex signature, 300 s replay window, PONG to
  PING. ✅
- **Gateway** — privileged `MESSAGE_CONTENT` intent declared; single‑leader Redis
  lease prevents dual‑connect. ✅
- **Pagination** — snowflake `before`/`after`; `/users/@me/guilds` loops past the
  200‑guild page cap. ✅
- **Rate limits** — `Retry-After` (float seconds) honoured on `429` within a
  bounded `≤3‑attempt / ≤30 s` budget. ✅
- **Privacy (SC‑006)** — raw `guild_id` never logged or persisted; only its
  BLAKE2b short hash. ✅

### 12.3 Dev / spammer mode

For local testing against the mock source servers, `build_discord_client` detects
spammer mode (`SYNTHETIC_SOURCE_API_BASE` set) and **presets** the bot token to
`spam-bot::{guild_id}`, skipping the real bot‑token resolution; the client's API
base then points at the local spammer (`:7008` for Discord) rather than
`discord.com` ([_clients.py:14‑22](../../../services/ingest/ingestion/fetchers/_clients.py#L14-L22),
[232‑249](../../../services/ingest/ingestion/fetchers/_clients.py#L232-L249)). The
path‑keyed read endpoints (guilds, channels, messages) key on globally‑unique
snowflakes, so only the token‑scoped auth needs the preseed.

> **TODO(human):** the spammer Discord mock port (`:7008` above) is **inferred**
> from the sibling source‑mock convention (GitHub `:7003`, Notion `:7006`) — the
> port is not hard‑coded in this code path (it resolves through
> `lib.integrations.endpoints`). Confirm and correct the port if it differs.
