# Signal Ingestion — How Fyralis Pulls Signal Data

This document explains, in detail, **how Signal data enters Fyralis**: which
Signal surface is read, with which credential, and how Signal's one signal set —
**conversation messages** in the linked account's threads (1:1 *direct* + *group*) —
is ingested.

Signal is a **linked‑device messaging surface**, not a bot/webhook API: there is
**no OAuth, no HTTP webhook, and no poll**. So it has exactly **two ingress
paths** — a historical **backfill PULL** (each thread's history, paged backward),
and a **persistent linked‑device receive loop** (the "gateway") that streams live
messages — and **both converge on one handler**, exactly like Telegram. This is
the ADR‑0003 *Topology B* archetype, cloned from Telegram
([integrations/signal/__init__.py:1‑27](../../../services/ingest/integrations/signal/__init__.py#L1-L27)).

It deliberately stops at the point where a Signal message becomes an
`ObservationDraft`. Everything downstream of that (the ingestion core, Kafka,
embeddings, the Memory Fabric) is out of scope. (`docs/ingestion/sources/` has no
`signal.md` short card — this is the only Signal flow doc.)

---

## 1. The two ways data arrives

Signal data reaches Fyralis through **two independent paths that converge on one
handler**:

| Path | Trigger | Mechanism | Code |
|------|---------|-----------|------|
| **Backfill (historical)** | Onboarding / reconciliation | Fyralis *pulls* each thread's history via the linked‑device session (`get_history`, paged backward on `offset_id`) | [planners/signal.py](../../../services/ingest/ingestion/planners/signal.py), [fetchers/signal.py](../../../services/ingest/ingestion/fetchers/signal.py) |
| **Live (gateway)** | A new message in a thread the linked account is in | Fyralis holds a **persistent linked‑device receive loop**; Signal pushes each message down it | [gateway/worker.py](../../../services/ingest/integrations/signal/gateway/worker.py), [gateway/dispatch.py](../../../services/ingest/integrations/signal/gateway/dispatch.py), [handlers/signal.py](../../../services/ingest/ingestion/handlers/signal.py) |

There is **no HTTP webhook** and **no poll** — the live path is the persistent
session itself. Both ingress kinds resolve to the **single** `signal:message`
channel via the `(source, ingress_kind)` → channel map
([normalizer/channel_mapping.py:247‑258](../../../services/ingest/ingestion/normalizer/channel_mapping.py#L247-L258)):

```
("signal", "gateway")  → "signal:message"   # live persistent linked-device loop
("signal", "backfill") → "signal:message"   # SAME handler as the gateway
```

Both paths build the **exact same record shape** through the **one** canonical
builder `build_message_record`
([records.py:40‑66](../../../services/ingest/integrations/signal/records.py#L40-L66)),
and both are parsed by the **single** `signal:message` handler
([handlers/signal.py:45‑101](../../../services/ingest/ingestion/handlers/signal.py#L45-L101)),
which derives the dedup key through the central `idempotency.signal_message`
constructor ([idempotency/__init__.py:273‑288](../../../services/ingest/ingestion/idempotency/__init__.py#L273-L288)):

```
external_id = "signal:{installation_id}:{thread_id}:{message_id}:{edit_ts|none}"
```

The key is **install‑namespaced** (the global `observations` UNIQUE has no
`tenant_id`) and **versioned** by edit slot. Edits are unsupported in v1, so the
edit slot is **always `none`** — i.e. `signal:{install}:{thread}:{message_id}:none`
([records.py:139‑142](../../../services/ingest/integrations/signal/records.py#L139-L142)).
Because the backfill fetcher and the live gateway worker call the **same**
builder, a message that is both backfilled *and* delivered live collapses into
**one** observation. This is the central design invariant of Signal ingestion.

> **One handler, one dedup namespace.** Unlike Discord (two channels), Signal has
> a single `signal:message` channel for both ingress kinds — there is no
> interaction/webhook surface to keep disjoint.

---

## 2. Authentication & token model — a linked‑device session (no OAuth)

Signal authenticates with a **persisted linked‑device registration** — the
libsignal identity/session store — **not** an OAuth token and **not** an HMAC
webhook secret ([client.py:10‑18](../../../services/ingest/integrations/signal/client.py#L10-L18),
[onboarding.py:1‑23](../../../services/ingest/integrations/signal/onboarding.py#L1-L23)).
A Signal authorization is a **linked device on a single account** (the service's
own number / a companion device); it sees **only the threads that account
participates in** — own/linked‑account self‑coverage, the same posture as
Telegram's user‑account session ([integrations/signal/__init__.py:9‑13](../../../services/ingest/integrations/signal/__init__.py#L9-L13)).

| Credential | Where | Used by | Notes |
|-----------|-------|---------|-------|
| **Live linked‑device session** | `signal_installations.session_secret_ref` → secret store | the gateway worker (live) | the LIVE receive loop's device registration |
| **Backfill linked‑device session** | `signal_installations.backfill_session_secret_ref` → secret store | `SignalClient` (backfill + reconciler probe) | a **second** linked device on the **same** account |

### 2.1 Two devices, one account (Topology B)

Per ADR‑0003 §6, the install holds **two** session refs so the live loop and the
backfill walk never share one device registration across processes
([onboarding.py:15‑22](../../../services/ingest/integrations/signal/onboarding.py#L15-L22)):
`session_secret_ref` is the live gateway worker's device;
`backfill_session_secret_ref` is a **second** linked device the backfill fetcher
uses. `build_signal_client` resolves the **backfill** ref, falling back to the
live ref only if a dedicated backfill device wasn't minted
([_clients.py:652‑684](../../../services/ingest/ingestion/fetchers/_clients.py#L652-L684)).
Unlike Telegram there are **no MTProto app credentials** (no `api_id`/`api_hash`)
([_clients.py:660‑662](../../../services/ingest/ingestion/fetchers/_clients.py#L660-L662)).

The `SignalClient` resolves its session once from the secret store via
`session_secret_ref` (or a preset string for Provider Lab/tests), and a missing
session raises `SignalApiError(code="signal_api_unauthorized")`
([client.py:126‑163](../../../services/ingest/integrations/signal/client.py#L126-L163)).
The session is **never logged**.

### 2.2 No HTTP webhook, no signature verifier — by design

Signal is **not** in the webhook `VERIFIERS` registry
([signatures/__init__.py:44‑68](../../../services/app/webhooks/signatures/__init__.py#L44-L68)) —
there is no inbound HTTP edge to sign. The live dispatch has **no HMAC / signature
gate**; the trust boundary is the **authenticated linked‑device session itself**,
exactly as for Telegram's and Discord's gateways
([gateway/dispatch.py:20‑22](../../../services/ingest/integrations/signal/gateway/dispatch.py#L20-L22)).

### 2.3 The install flow (how a linked account gets registered)

There is **no OAuth handshake**. `finalize_install`
([onboarding.py:40‑127](../../../services/ingest/integrations/signal/onboarding.py#L40-L127))
provisions an install in **one tenant‑scoped transaction**, mirroring the
Telegram/Jira dedicated‑table shape:

1. **UPSERT a `signal_installations` row** keyed on `(tenant_id, account_label)`,
   carrying `session_secret_ref` + `backfill_session_secret_ref`; a re‑install
   clears `disabled_at` (re‑enable) ([onboarding.py:56‑74](../../../services/ingest/integrations/signal/onboarding.py#L56-L74)).
2. **INSERT one `signal_threads` row per thread** to shard, keyed on
   `(signal_installation_id, thread_id)` and idempotent (re‑activates + refreshes
   title on conflict). The thread set is enumerated via `SignalClient.iter_threads`
   at connect time, or an operator inclusion list
   ([onboarding.py:76‑94](../../../services/ingest/integrations/signal/onboarding.py#L76-L94)).
3. **Seed an empty `signal_update_state` row** (one per install) — the live sync
   cursor the gateway worker advances ([onboarding.py:96‑105](../../../services/ingest/integrations/signal/onboarding.py#L96-L105)).
4. **Emit an `onboarding_triggers` row** (`source='signal'`, admitted by migration
   0100) so the existing M6 backfill chain (`oauth_poller → tenant_onboarding →
   source_onboarding → shard_fetch → reconciler`) fires
   ([onboarding.py:107‑121](../../../services/ingest/integrations/signal/onboarding.py#L107-L121)).

---

## 3. The Signal surface actually called

All backfill reads funnel through `SignalClient`, a thin async wrapper over the
linked‑device surface ([client.py:97‑104](../../../services/ingest/integrations/signal/client.py#L97-L104)).
The "endpoints" here are **linked‑device methods**, not HTTP routes:

| Linked‑device method | Wrapper | Purpose | Code |
|----------------------|---------|---------|------|
| thread history (older than `offset_id`) | `get_history()` | one **page** of a thread's messages (backfill) | [client.py:175‑207](../../../services/ingest/integrations/signal/client.py#L175-L207) |
| thread / group listing | `iter_threads()` | enumerate the linked account's threads at onboarding | [client.py:209‑230](../../../services/ingest/integrations/signal/client.py#L209-L230) |
| newest‑above‑high‑water (1 row) | `has_history_since()` | reconciler **gap probe** | [client.py:232‑244](../../../services/ingest/integrations/signal/client.py#L232-L244) |
| identity/connectivity | `me()` | cheap auth/connectivity probe | [client.py:246‑257](../../../services/ingest/integrations/signal/client.py#L246-L257) |

### 3.1 Pagination — backward on an `offset_id` cursor

Like Telegram's `messages.getHistory`, Signal's thread history pages **backward**
from the newest message ([client.py:175‑207](../../../services/ingest/integrations/signal/client.py#L175-L207)):
`get_history(offset_id=0)` starts at the newest; each call returns up to `limit`
messages **older than** `offset_id` (and newer than the incremental `min_id`
floor); the **MIN message id** in the page becomes the next (older) page's
`offset_id`. `is_last` is True when the page comes back short (no older history)
([client.py:204‑207](../../../services/ingest/integrations/signal/client.py#L204-L207)).
The fetcher persists that `offset_id` in the shard cursor and resumes on the next
invocation (§5).

### 3.2 Rate limits — provider transport, no invented Signal quota

Every JSON-RPC operation runs through the shared `ProviderTransport` binding.
HTTP `429` responses parse `Retry-After`; JSON-RPC errors carrying
`retryAfter`/`retry_after` or an explicit rate-limit message become
`ProviderRateLimited`. Shared transport policy applies concurrency, retry
budget, jitter, and durable `RetryLater` handling rather than a client-local
sleep loop.

Signal publishes no official service quota for this unofficial client boundary,
so Fyralis does not invent a fixed requests-per-second value. The certified
signal-cli version and observed disposable-account behavior must supply the
deployment policy; Provider Lab supplies deterministic quota and recovery tests.

---

## 4. Backfill scope — the shard family

The planner decomposes one install into **one shard per active thread**, all of
`shard_kind = "signal_thread_history"`
([planners/signal.py:50‑83](../../../services/ingest/ingestion/planners/signal.py#L50-L83)).
There is **a single shard family** (no Class‑A/Class‑B split like GitHub, no
sampling like Discord):

| Class | Signal | Shape | Fetch path |
|-------|--------|-------|------------|
| Thread window | one thread's message history (1:1 *direct* or *group*) | backward‑paged `get_history` (cursored on `offset_id`) | `fetch_page_signal` |

### 4.1 Threads come from DB state, not a live enumeration

`ctx.source_client` is **None** for Signal — the planner reads DB state only and
does **no I/O**, exactly like Telegram/Jira. The thread list was populated at
install time (`SignalClient.iter_threads` → `signal_threads` via `finalize_install`,
§2.3) and JSON‑aggregated onto `ctx.install["threads"]` by the SourceOnboarding
loader ([planners/signal.py:1‑15](../../../services/ingest/ingestion/planners/signal.py#L1-L15)).
The planner decodes that column ([planners/signal.py:33‑47](../../../services/ingest/ingestion/planners/signal.py#L33-L47))
and emits one shard per thread with an `int` `thread_id`.

Each shard carries `thread_id`, `thread_kind` (`direct`/`group`, default `direct`),
`thread_title`, `installation_id`, and the per‑thread `offset_id_cursor`
high‑water (None on first full sync), at a baseline `recency_score=1.0`
([planners/signal.py:64‑77](../../../services/ingest/ingestion/planners/signal.py#L64-L77)).

---

## 5. Backfill fetch — thread window (the `signal:message` path)

`fetch_page_signal` ([fetchers/signal.py:109‑196](../../../services/ingest/ingestion/fetchers/signal.py#L109-L196))
fetches one page of a thread's history, advances the backward‑paging cursor, and
builds one canonical record per message. ShardFetch calls it in a loop, persisting
the returned cursor between calls (N1: cursor opaque to the fetcher, advanced by
ShardFetch).

### 5.1 Cursor

```python
class SignalCursor(BaseModel):
    offset_id: int = 0          # next (older) page boundary; 0 = start at newest
    min_id: int = 0             # incremental floor (warm-start high-water); 0 = full
    high_water_max_id: int = 0  # MAX id seen this run — the reconciler's gap ref
    messages_seen: int = 0      # diagnostic
    seeded: bool = False        # whether first-call warm-start setup has run
```

([fetchers/signal.py:71‑89](../../../services/ingest/ingestion/fetchers/signal.py#L71-L89)).
Two modes ([fetchers/signal.py:17‑24](../../../services/ingest/ingestion/fetchers/signal.py#L17-L24)):

- **FULL** (initial backfill): `offset_id=0`, `min_id=0`; page backward until a
  short/empty page (`is_last`), i.e. the start of obtainable history.
- **INCREMENTAL** (reconciler re‑walk): on the first call, the shard's
  `offset_id_cursor` high‑water seeds `min_id` (and `high_water_max_id`), so the
  walk is bounded to messages **newer** than the high‑water — only the changed
  tail comes back ([fetchers/signal.py:126‑131](../../../services/ingest/ingestion/fetchers/signal.py#L126-L131)).

The cursor advances to the **oldest id in this page** (`next_offset_id`); the
**MAX id seen this run** (`high_water_max_id`) is the reconciler's gap reference
point ([fetchers/signal.py:166‑179](../../../services/ingest/ingestion/fetchers/signal.py#L166-L179)).

### 5.2 Rate‑limit defer

On `SignalApiError(code="signal_api_rate_limited")`, the fetcher logs the
server‑returned `retry_after`, **leaves the cursor unadvanced**, and returns an
empty page with `end_of_data=False` so ShardFetch re‑enters on the next tick — the
retry budget is deferred to ShardFetch, not slept inline
([fetchers/signal.py:144‑159](../../../services/ingest/ingestion/fetchers/signal.py#L144-L159)).

### 5.3 One message → one canonical record (external‑id parity)

For each message the fetcher calls `build_message_record` — the **same** builder
the live gateway worker uses — injecting the thread context (`thread_id`,
`thread_kind`, `thread_title`, `installation_id`) under reserved `_fyralis_*` keys
so the pure handler can derive the install‑namespaced `external_id` with **no DB
lookup** ([fetchers/signal.py:161‑174](../../../services/ingest/ingestion/fetchers/signal.py#L161-L174),
[records.py:40‑66](../../../services/ingest/integrations/signal/records.py#L40-L66)).
Because both paths use this builder, a backfilled message and its live twin derive
the identical `signal:{install}:{thread}:{id}:none` key and collapse to one
observation.

The backward page size is bounded to ≤100 and overridable via
**`SIGNAL_BACKFILL_PAGE_SIZE`** ([fetchers/signal.py:61‑68](../../../services/ingest/ingestion/fetchers/signal.py#L61-L68)).

> **History is inherently shallow (confirmed limitation).** A linked device cannot
> fetch arbitrary deep thread history — signal‑cli surfaces messages going
> *forward* (the `receive` notification) plus what syncs at link time. So
> `get_history` backfill is bounded to the own/linked‑account recent sync, not the
> full conversation archive ([client.py:32‑36](../../../services/ingest/integrations/signal/client.py#L32-L36)).

---

## 6. The handler — shaping a message into `ObservationDraft`

`handle_signal` ([handlers/signal.py:45‑101](../../../services/ingest/ingestion/handlers/signal.py#L45-L101))
is a **pure function** (no DB / network): it calls `parse_message_record` to
validate + normalize the canonical record, then emits exactly **one**
`ObservationDraft`. A malformed record (missing integer id, unparseable date,
missing `_fyralis_thread_id`) raises `ValidationError` → DLQ, not a crash
([records.py:103‑156](../../../services/ingest/integrations/signal/records.py#L103-L156)).

| Channel | Handler | `external_id` | `occurred_at` | `kind` | Trust tier |
|---------|---------|---------------|---------------|--------|------------|
| `signal:message` (gateway + backfill) | `handle_signal` | `signal:{install}:{thread}:{msg_id}:none` | message `date` (epoch s → UTC) | `signal` | `attested_agent` |

Highlights:

- **`content_text`** = `[{thread_title|thread N}] {sender_username|sender_ref|"someone"}: {text}`,
  the body truncated at 600 chars ([handlers/signal.py:41‑63](../../../services/ingest/ingestion/handlers/signal.py#L41-L63)).
- **`source_actor_ref`** = `signal:user:{sender_id}`, or `None` for a self‑sent /
  group‑system message with no `from_id` ([handlers/signal.py:56‑58](../../../services/ingest/ingestion/handlers/signal.py#L56-L58),
  [records.py:93‑100](../../../services/ingest/integrations/signal/records.py#L93-L100)).
- **`occurred_at`** is the message `date` (epoch seconds → UTC); an unparseable
  date is a `ValidationError`, not a `now()` fallback
  ([records.py:84‑90](../../../services/ingest/integrations/signal/records.py#L84-L90),
  [124‑128](../../../services/ingest/integrations/signal/records.py#L124-L128)).
- **`entities_hint`** carries a `signal_thread` channel ref and (when present) a
  `signal_user` actor ref ([handlers/signal.py:65‑74](../../../services/ingest/ingestion/handlers/signal.py#L65-L74)).
- **Trust tier** is `attested_agent` — Signal is a human conversational channel
  like Telegram/Slack ([handlers/signal.py:18‑20](../../../services/ingest/ingestion/handlers/signal.py#L18-L20),
  [38](../../../services/ingest/ingestion/handlers/signal.py#L38)). The channel is
  registered via `@register(CHANNEL)` and stamped into `CHANNEL_TRUST_MAP` at
  import (`setdefault`), rather than statically listed in `handlers/__init__.py`
  ([handlers/signal.py:45](../../../services/ingest/ingestion/handlers/signal.py#L45),
  [104](../../../services/ingest/ingestion/handlers/signal.py#L104)).

---

## 7. Live ingestion — the persistent linked‑device receive loop (gateway)

Signal's live path is **not** an HTTP webhook — it is a **persistent outbound
linked‑device session** the worker holds open; Signal **streams** each incoming
message down it. This is the same shadow‑write/`ingress_kind="gateway"` shape used
by Telegram/Discord for push‑less sources, **not** an HTTP edge
([gateway/__init__.py:1‑16](../../../services/ingest/integrations/signal/gateway/__init__.py#L1-L16)).

### 7.1 The worker

`SignalGatewayWorker` holds **one** install's live linked‑device session and
drives `dispatch.handle_update` for each incoming message
([gateway/worker.py:40‑66](../../../services/ingest/integrations/signal/gateway/worker.py#L40-L66)).
A live device should be driven by exactly **one** receive loop, so the launcher
requires both `SIGNAL_TENANT_ID` and `SIGNAL_INSTALLATION_ID`, loads the active
row using that exact pair, and acquires
`gateway:signal:{tenant_id}:{installation_id}:leader_lock` **before**
constructing the worker. Different installations therefore neither block each
other nor inherit one process-global credential/state binding
([run_signal_gateway_worker.py](../../../scripts/run_signal_gateway_worker.py)).

The worker consumes signal-cli's HTTP SSE stream through
`SignalJsonRpcTransport`, flattens each receive envelope to the canonical
message dict, attaches thread context, and calls `handle_update`. A finite
stream or recoverable transport failure reconnects after a bounded delay;
provider cooldowns escape as `RetryLater` so the launcher releases its
installation lease instead of hot-looping.

### 7.2 Gap recovery lives in the live worker

There is no Signal server-side history cursor. signal-cli owns the linked
device's local receive queue. Fyralis acknowledges an update only after raw or
inline ingestion is durable, then persists its timestamp as the exact
installation's `signal_update_state.sync_cursor`. That cursor is an
audit/reconciliation checkpoint; it is not sent to Signal as an invented replay
token. On disconnect, the worker reconnects to the daemon's receive stream, and
the backfill reconciler remains the independent safety net.

### 7.3 Dispatch + cutover (no signature gate)

`handle_update` is the load‑bearing, test‑drivable bridge from a live message to
the pipeline ([gateway/dispatch.py:144‑185](../../../services/ingest/integrations/signal/gateway/dispatch.py#L144-L185)).
A Signal gateway worker holds **one linked account = one tenant's install**, so
the tenant is known by construction (carried on `DispatchDeps`) — there is **no
per‑update tenant resolution** ([gateway/dispatch.py:5‑9](../../../services/ingest/integrations/signal/gateway/dispatch.py#L5-L9)).
The flow:

1. **Build the canonical record** via the **same** `build_message_record` the
   backfill fetcher uses → identical `external_id` → cross‑path dedup
   ([gateway/dispatch.py:65‑89](../../../services/ingest/integrations/signal/gateway/dispatch.py#L65-L89)).
   The linked account's **own outgoing** messages (`out is True`) are skipped
   ([gateway/dispatch.py:80‑82](../../../services/ingest/integrations/signal/gateway/dispatch.py#L80-L82)).
2. **Cutover branch** (kafka‑first default): if `tenant_flags.kafka_path_enabled`
   for the tenant, **shadow‑write** the canonical body to `ingestion.raw.signal`
   under `ingress_kind="gateway"` and return — the normalizer + observation_writer
   produce the observation, concurrently with any in‑flight backfill
   ([gateway/dispatch.py:106‑128](../../../services/ingest/integrations/signal/gateway/dispatch.py#L106-L128),
   [150‑163](../../../services/ingest/integrations/signal/gateway/dispatch.py#L150-L163)).
   On publish failure it **falls through to inline** so the message is never
   dropped.
3. **Inline path / fallback**: `core.ingest("signal:message", record, …)`, then a
   best‑effort M2 shadow audit when `SHADOW_WRITE_ENABLED`
   ([gateway/dispatch.py:165‑185](../../../services/ingest/integrations/signal/gateway/dispatch.py#L165-L185)).

The canonical raw body is byte‑stable `orjson` (sorted keys), so retransmissions
of the same message id are byte‑identical and `content_hash` dedup / replay‑from‑raw
hold ([gateway/dispatch.py:92‑103](../../../services/ingest/integrations/signal/gateway/dispatch.py#L92-L103)).

> **No HMAC / signature gate.** There is no inbound HTTP request to verify; the
> trust boundary is the authenticated linked‑device session
> ([gateway/dispatch.py:20‑22](../../../services/ingest/integrations/signal/gateway/dispatch.py#L20-L22)).

---

## 8. Reconciliation — gap detection

`reconcile_signal` ([reconcilers/signal.py:148‑177](../../../services/ingest/ingestion/reconcilers/signal.py#L148-L177))
re‑checks **completed** (`state == "done"`) thread shards for new activity. Per
shard, it loads the cursor's `high_water_max_id` and issues a **cheap 1‑row probe**
`has_history_since(min_id=high_water)`; any newer message means a gap
([reconcilers/signal.py:93‑136](../../../services/ingest/ingestion/reconcilers/signal.py#L93-L136)).

On a gap it reshares a `signal_thread_history` shard at **`recency_score=1.5`**,
carrying `parent_shard_id`, `gap_baseline_max_id`, and `offset_id_cursor` warm‑set
to the high‑water so the re‑walk only re‑fetches the newer tail (incremental mode
in the fetcher) ([reconcilers/signal.py:123‑136](../../../services/ingest/ingestion/reconcilers/signal.py#L123-L136)).
`external_id` parity means re‑walked messages dedup against what backfill already
wrote — only genuinely new messages produce new observations; the probe can
over‑reshare but never under‑reshares. A probe exception is logged and treated as
"no gap" (it does not fail the run) ([reconcilers/signal.py:107‑118](../../../services/ingest/ingestion/reconcilers/signal.py#L107-L118)).

> The **primary** live path is signal-cli's local receive queue plus the
> installation worker's reconnect loop (§7.2). This DB-side reconciler is the
> **backfill-completeness safety net**—it catches anything that arrived between
> the backfill sweep and the live session coming up
> ([reconcilers/signal.py:20‑23](../../../services/ingest/ingestion/reconcilers/signal.py#L20-L23)).

The reconciler opens the **same** backfill `SignalClient` (via the shared
`_open_signal_client` seam) and resolves the install with `disabled_at IS NULL`
([reconcilers/signal.py:139‑160](../../../services/ingest/ingestion/reconcilers/signal.py#L139-L160)).

---

## 9. Revocation / recoverable‑error behavior

Signal's authorization is a **linked device** that can be **unlinked** on the
account. The error surface is `SignalApiError` with `code` values
`signal_api_unauthorized` (missing/revoked session), `signal_api_rate_limited`
(server rate‑limit; see §5.2), and `signal_api_error` (other failures)
([client.py:142‑163](../../../services/ingest/integrations/signal/client.py#L142-L163)).

The install row carries a `disabled_at` chokepoint column: `finalize_install`
clears it on re‑install (re‑enable), and the reconciler/loader only select installs
with `disabled_at IS NULL` ([onboarding.py:64‑69](../../../services/ingest/integrations/signal/onboarding.py#L64-L69),
[reconcilers/signal.py:139‑144](../../../services/ingest/ingestion/reconcilers/signal.py#L139-L144)).

Unlike GitHub's `_maybe_disable_on_revocation` or Discord's
`_trigger_chokepoint`, there is currently **no outbound auto-disable
chokepoint** for Signal. HTTP/RPC authentication and daemon failures are typed,
but signal-cli has no stable official revocation contract that Fyralis can map
to `disabled_at` without risking a false disable. Recovery remains
operator-side: re-link the device and re-run `finalize_install`, which
re-enables the row.

---

## 10. End‑to‑end summary

```
                          ┌──────────────────────── BACKFILL (pull) ────────────────────────┐
                          │  linked-device session (backfill_session_secret_ref, 2nd device) │
   LINKED-ACCOUNT THREADS │  planner: read signal_threads from DB (ctx.source_client = None) │
   (own/linked coverage)  │     └─► one signal_thread_history shard per active thread        │
   thread history         │  fetcher: get_history(offset_id, min_id) — paged BACKWARD        │
                          │     └─► build_message_record (inject _fyralis_* thread context)  │
                          └───────────────────────────────────────────────────────────────┬─┘
                                                                                            │
   LIVE MESSAGES          ┌──────────── GATEWAY (persistent linked-device loop) ───────────┐│
   (ingress=gateway) ─────►  worker holds live session → receive loop → handle_update       ││
                          │     NO HTTP webhook, NO signature gate (session is the trust bnd)││
                          │     skip own out=True ; ACK only after ingestion durability      ││
                          │     cutover→ingestion.raw.signal OR inline ingest() (+shadow)    ││
                          └───────────────────────────────────────────────────────────────┘│
                                                                                            │
                                                            ┌───────────────────────────────▼─┐
                                                            │  handle_signal (one handler)     │
                                                            │  external_id =                   │
                                                            │   signal:{install}:{thread}:     │
                                                            │   {message_id}:none              │
                                                            │  → ObservationDraft              │
                                                            └──────────────────────────────────┘
```

**Key invariants**

1. **One handler, one dedup namespace.** Backfill and the live gateway loop both
   land on `signal:message` with `external_id="signal:{install}:{thread}:{id}:none"`.
   A backfilled message and its live twin dedup to a single observation; the key is
   built by the **one** `build_message_record` + `idempotency.signal_message`.
2. **A linked‑device session, not OAuth/webhook.** Auth is a persisted
   linked‑device registration; there is **no OAuth, no HTTP webhook, no signature
   verifier** (Signal is absent from `VERIFIERS`).
3. **Two devices, one account (Topology B).** The live gateway worker uses
   `session_secret_ref`; the backfill fetcher uses a **second** device
   (`backfill_session_secret_ref`), so live and backfill never share a registration
   across processes.
4. **Backward `offset_id` pagination; no client‑side rate bucket.** History pages
   backward (oldest id → next `offset_id`); rate limiting is server‑driven, with
   the fetcher deferring the retry budget to ShardFetch on `signal_api_rate_limited`.
5. **Durability-gated live cursor; DB reconciler is the safety net.** signal-cli
   owns its local receive queue; the worker checkpoints only after durable
   ingestion, while `reconcile_signal` is the reshare-safe backstop.
6. **Self/linked coverage + shallow history.** A linked device sees only its own
   account's threads, and cannot fetch arbitrary deep history (confirmed signal‑cli
   limitation).

---

## 11. Configuration & compliance

> **Compliance caveat.** Signal has **no official server API** and **no maintained
> pure-Python client**. Fyralis pins the unofficial signal-cli `0.14.4.1`
> boundary. Link signal-cli as a secondary device and run
> `signal-cli -a <number> daemon --http HOST:PORT`; Fyralis calls its HTTP
> `/api/v1/rpc` endpoint and consumes `/api/v1/events` over SSE. The pinned
> version, account-consent posture, and real-daemon canary are part of Signal
> certification.

### 11.1 Environment knobs

| Env var | Default | Meaning |
|---------|---------|---------|
| `SIGNAL_TENANT_ID` | none; required | UUID of the tenant that owns this worker binding. |
| `SIGNAL_INSTALLATION_ID` | none; required | Exact active `signal_installations.id`; there is no first/latest fallback. |
| `SIGNAL_JSONRPC_ENDPOINT` | none; required | HTTP(S) signal-cli RPC URL, normally `http://host:port/api/v1/rpc`. |
| `SIGNAL_SSE_ENDPOINT` | derived for native endpoint | SSE URL. Native `/api/v1/events` is derived; custom Provider Lab paths set it explicitly. |
| `SIGNAL_CLI_VERSION` | `0.14.4.1` | Must equal the certified pinned signal-cli version. |
| `SIGNAL_CLI_MULTI_ACCOUNT` | `0` | Explicit opt-in for a daemon serving multiple accounts; otherwise deploy one daemon/account per installation. |
| `SIGNAL_BACKFILL_PAGE_SIZE` | `100` (capped ≤100) | backward history page size for `get_history` ([fetchers/signal.py:64‑68](../../../services/ingest/ingestion/fetchers/signal.py#L64-L68)) |

> The numerous `SIGNAL_KIND_*` / `SIGNAL_TOPIC` env vars elsewhere in the repo are
> the **ingestion‑pipeline Kafka workflow‑signal** constants (run/shard lifecycle
> events) — **unrelated** to the Signal messaging source.

### 11.2 Verified compliant

- **Single handler / single dedup namespace** — both ingress kinds map to
  `signal:message`; one `build_message_record` + `idempotency.signal_message`. ✅
- **External‑id parity** — backfill and gateway derive the identical
  `signal:{install}:{thread}:{id}:none` key (install‑namespaced, edit slot `none`). ✅
- **No HTTP webhook surface** — Signal absent from `VERIFIERS`; live path is the
  persistent session, dispatch has no signature gate. ✅
- **Backward pagination** — `offset_id` cursor (oldest id → next page), `is_last`
  on a short page. ✅
- **Reconciler reshare safety** — 1‑row `has_history_since` probe, warm‑started
  re‑walk, idempotent via `external_id` dedup. ✅

### 11.3 Dev / Provider Lab mode

For local testing, `build_signal_client` detects `PROVIDER_LAB_URL` and
**presets** the session to the literal
`"spam-signal"`, skipping real secret‑store resolution
([_clients.py](../../../services/ingest/ingestion/fetchers/_clients.py)). The
synthetic backfill harness rebinds the fetcher's/reconciler's `_open_signal_client`
seam to a **`MockSignalClient`**, which implements `get_history` (backward‑paged),
`has_history_since`, `iter_threads`, and `me` with the production
`SignalApiError` `code` values so the fetcher branches exactly as against the real
client ([fetchers/signal.py:102‑107](../../../services/ingest/ingestion/fetchers/signal.py#L102-L107),
[synthetic/mock_clients/signal.py:1‑30](../../../services/ingest/synthetic/mock_clients/signal.py#L1-L30),
[126‑158](../../../services/ingest/synthetic/mock_clients/signal.py#L126-L158)).
The **live** path does not use the mock — the synthetic `SignalGatewayGenerator`
drives the production `gateway.dispatch.handle_update` directly (Telegram‑gateway
style), so a live event flows through the same normalizer → observation_writer
chain as backfill ([synthetic/live_generators/signal_gateway.py:1‑26](../../../services/ingest/synthetic/live_generators/signal_gateway.py#L1-L26)).

---

## 12. Implemented boundary and remaining certification

The production client implements the pinned signal-cli HTTP JSON-RPC/SSE
surface used by Fyralis: `listGroups`, finite `receive`,
`subscribeReceive`/`unsubscribeReceive`, native event streaming, envelope
mapping, typed HTTP/RPC errors, provider-transport execution, and durable cursor
acknowledgement. The launcher is exact-installation-only and Compose/Helm carry
that identity explicitly.

This does **not** create an official Signal API or deep server-side history.
Signal remains a shallow, forward-from-link source whose release certification
must pin signal-cli, exercise a disposable real linked device, and record the
unofficial-client/account-standing risk. A real-provider canary—not merely
synthetic fixtures—is still required before marking the source certified.
