# ADR-0003: Telegram ingestion uses the MTProto user-account API, with a two-session backfill+live topology

- **Status:** Proposed <!-- Proposed | Accepted | Superseded by ADR-XXXX | Deprecated -->
- **Date:** 2026-06-07
- **Deciders:** Ingestion / platform
- **Related:** [ADR-0001](0001-kafka-first-ingestion-default.md) (Kafka-first ingestion), the Telegram source spec ([../ingestion/sources/telegram.md](../ingestion/sources/telegram.md)), the eleven existing sources under `services/ingest/integrations/`.

## Context

Telegram is being added as the **12th ingestion source**. Unlike the existing
eleven, Telegram exposes *two* developer surfaces with very different
capabilities, and the choice between them is load-bearing for the whole design:

- The **HTTP Bot API** is simple (token + webhook) but **cannot read historical
  message history** — a bot only receives updates for messages sent *after* it
  joins a chat, and there is no method to page backwards. Backfill — a hard
  requirement for every Fyralis source — is impossible on it.
- The **MTProto client (user-account) API** is the full protocol that official
  clients speak. `messages.getHistory` — the method that pages a dialog's
  history — is explicitly **"Only users can use this method"**
  ([core.telegram.org/method/messages.getHistory](https://core.telegram.org/method/messages.getHistory)).
  So historical backfill *requires* a user-account ("userbot") session.

This forces a set of coupled decisions: which API, which client library, how the
long-lived credential is modelled and operated, how live ingress works given
**MTProto has no HTTP webhook** (live is a persistent push connection), and how
backfill and live run *concurrently* against one account. The findings below are
drawn from a cited, adversarially-verified research pass over Telegram's own
primary protocol/API spec and the Telethon docs (23/23 surviving claims passed
3-0 verification); the full report is summarized in the
[source spec](../ingestion/sources/telegram.md).

## Decision

**1. Use the MTProto user-account API, not the Bot API.** Backfill is
non-negotiable and only the user API can serve `messages.getHistory`. *Rejected:
Bot API* — no history access; would make Telegram a live-only source, breaking
the uniform backfill+live contract every other source honours.

**2. Use Telethon as the client library.** It is the pure-Python asyncio MTProto
client, with `StringSession` persistence, native `updates.getDifference`
gap-recovery, and `FloodWaitError` handling — the natural fit for our async
ingestion service. *Rejected: TDLib* (C++ binding, heavier integration),
*GramJS* (JS), *MadelineProto* (PHP). **(Inference, not a verified ranking:** the
research found no surviving head-to-head comparative claim; this choice follows
from the Python-async requirement, not a benchmarked verdict. Revisit if Telethon
proves limiting.)

**3. Model the credential as a persisted MTProto session, not a token.** The
durable secret is the per-data-center **`auth_key`** (a 2048-bit key negotiated
once via Diffie-Hellman, never sent over the wire, identified on-wire by
`auth_key_id`). We persist it as a Telethon `StringSession` string in
`encrypted_secrets`, referenced by `telegram_installations.session_secret_ref`,
alongside the app's `api_id`/`api_hash`. Login is **interactive** (phone →
SMS/app code → optional SRP 2FA via `auth.sendCode`/`auth.signIn`/
`auth.checkPassword`), so the install is operator-mediated (a connect wizard that
completes the login out-of-band and writes the session), *not* a server-side OAuth
redirect like Slack/GitHub. This mirrors the operator-mediated install posture of
the finance sources (Mercury/QuickBooks), but with a session string rather than an
API token.

**4. Live ingress is gateway-style (a persistent updates connection), not a
webhook.** MTProto pushes updates (`updateNewMessage`, …) over a long-lived
connection; there is no HTTP webhook for the user API. Telegram is therefore
modelled as a **gateway source like Discord**: a single long-running worker holds
the connection and shadow-writes each live update onto `ingestion.raw.telegram`
(`ingress_kind="gateway"`), so live flows through the *same*
normalizer→observation_writer chain as backfill. **There is no HMAC signature
gate** — the trust boundary is the authenticated MTProto connection itself (as
with Discord's gateway and Gmail Pub/Sub). The worker must hold a **single-instance
lease** (reuse Discord's Redis `leader_lock`): a Telegram authorization may be
driven by only one live updates connection at a time.

**5. Two cursor families, two state primitives.** Backfill cursors per dialog on
**`offset_id`** (`messages.getHistory`'s `offset_id`/`add_offset`/`limit`; the
oldest returned message id becomes the next page's `offset_id`). Live cursors on
the **`pts`/`qts`/`seq`/`date`** update-state, reconciled by
`updates.getDifference` (common: private chats + basic groups) and
`updates.getChannelDifference` (per-channel/supergroup, auto-triggered by
`updateChannelTooLong`). These are independent and never contend at the state
level — `offset_id` lives on per-dialog rows; `pts/qts/seq/date` live on a
per-install update-state row.

**6. Run backfill and live concurrently via TWO authorizations (Topology B).**
A single `auth_key` cannot be safely shared across our process-separated workers
(Telethon corrupts/invalidates a session used from two places at once), and the
"one 64-bit session hosts multiple concurrent sub-sessions" idea was **explicitly
refuted** in research (0-3). So we mint **two independent logins on the same
account** (two `auth_key`s — legitimate, the same as being logged in on phone +
desktop): a **live session** owned solely by the gateway worker, and a **backfill
session** owned by one per-account backfill worker that multiplexes
`messages.getHistory` across that account's dialog shards on its single
connection. They share the account-wide FLOOD_WAIT budget but have no
connection-level head-of-line blocking, so live latency stays clean under heavy
backfill, and `getDifference` reconciles any update the live connection missed
while busy. *Rejected: one shared connection (Topology A)* — idiomatic to MTProto
and how real clients behave, but it does not fit our worker-per-process model
(backfill could not fan out across `shard_fetch` workers without sharing the
credential). Topology A remains the fallback if operating two sessions proves
problematic.

**7. The deliverable is a synthetic test environment, not a live account.** As
with the other eleven sources, the acceptance gate is the in-process synthetic
harness: a `MockTelegramClient` drives the production seams (the backfill
`_open_telegram_client` fetcher seam and the live worker's client), and the
all-sources concurrent **backfill-in-progress-while-live-arrives** overlap gate
is extended to include Telegram as source #12. No real Telegram credentials,
network, or Telethon runtime are required to run the gate; Telethon is an
**optional** dependency, import-guarded so the test environment runs without it.

## Consequences

**Easier / now possible.** Telegram backfill + live ingestion through the
existing Kafka full-pipeline, with per-dialog cursoring and protocol-native live
gap-recovery (`getDifference` is a stronger reconciler than the polling
reconcilers other sources use). The synthetic gate proves concurrent
backfill+live overlap without any external dependency.

**Harder / new constraints.**

- **Account-ban risk.** *"All accounts that log in using unofficial Telegram API
  clients are automatically put under observation"*; flooding/spam → permanent
  ban ([core.telegram.org/api/obtaining_api_id](https://core.telegram.org/api/obtaining_api_id)).
  The ingestion service account is a real phone-numbered account at risk — a
  materially different operational posture than our OAuth apps. Operations must
  keep request patterns conservative and honour every `FLOOD_WAIT`.
- **Interactive install.** No headless OAuth redirect; the connect flow needs a
  phone code (and possibly a 2FA password) entered once to mint the session.
- **Single-instance liveness.** The live updates connection requires the Redis
  lease guard (like Discord) — two live connections on one authorization conflict.
- **Shared rate budget.** Two authorizations do **not** double the rate limit;
  they isolate connections, not quota. Backfill `getHistory` and live-side
  `getChannelDifference` draw from the same per-account FLOOD_WAIT budget.
- **Unknowns to close before production.** (a) Concrete numeric rate limits /
  `FLOOD_WAIT` durations are **not** verifiable from primary sources — only the
  mechanism (error 420, server-returned `seconds`) is. (b) The single-connection
  concurrency claim (Topology A) is high-confidence *inference*, not a cited
  fact. **TODO(human):** run a throwaway Telethon spike on a real test account —
  a multi-thousand-message backfill while asserting live `updateNewMessage`
  events keep landing — to (i) confirm the concurrency property end-to-end and
  (ii) measure real `FLOOD_WAIT` behaviour, before enabling a production tenant.

**How this is revisited / falsified.** If the spike shows two authorizations are
unnecessary or harmful, collapse to Topology A (one connection serving both) —
the cursor primitives and pipeline wiring are unchanged; only the worker topology
moves. If Telethon proves limiting at scale, ADR-supersede with a TDLib decision.
