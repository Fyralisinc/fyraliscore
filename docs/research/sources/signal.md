# Signal — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (7/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: does not fit standard archetypes cleanly · can-we-gather: yes (conditional — own/consented accounts only, no org token) · effort: M (leaning M-minus on code, heavy on compliance/ops).**

---

## TL;DR

Signal has no official B2B API, OAuth, or webhook surface — the only practical ingestion path is the community CLI `signal-cli` (AsamK), which links as a secondary device to an account the operator controls and exposes a local JSON-RPC daemon. Inbound messages arrive as `dataMessage` envelopes; outbound as `syncMessage.sentMessage`; live receive is a subscription/streaming pull via `subscribeReceive` — there is no webhook and no HMAC gate. The closest pipeline analogue is the just-landed Telegram MTProto gateway (ADR-0003): operator-mediated install, persistent gateway-style connection, no org-wide token, per-account grain. The decisive divergence from every other source is backfill: Signal is end-to-end encrypted with no server-side message archive, so a newly linked device receives only traffic from the moment it links forward — deep historical backfill is architecturally impossible, not a tuning choice, and breaks the uniform backfill+live contract the rest of the pipeline assumes.

---

## What companies use it for — and what signal lives there

Signal is used in enterprise contexts precisely *because* it is E2E-encrypted and off-record — the same property that makes it valuable to users is what makes it hard to ingest. The signal that lives there is high-sensitivity, high-value, and otherwise invisible to a company-intelligence platform.

- **Company ops/support line on Signal** — Support, trust-and-safety, journalists' tip lines, or security teams who give out a Signal number for confidential inbound. Captures inbound customer/source messages (`dataMessage`) + the team's replies (`syncMessage.sentMessage`) — a full conversational record of confidential intake that bypasses corporate Slack/email entirely.
- **Executive / deal-making back-channel** — Founders, execs, BD/partnerships who negotiate sensitive deals or discuss M&A/legal matters over Signal. Captures high-value relationship and commitment signals: who is talking to which counterparty, cadence, and (with consent) the substance of negotiations that never land in Jira or email but drive outcomes.
- **Incident / on-call coordination bridge** — Security/SRE teams using a Signal group for sensitive incident comms when corporate channels are considered compromised or too public. Group `dataMessage`s timestamped during incidents correlate with Grafana alerts and GitHub deploys already in the pipeline.
- **Field / vendor / regional comms where Signal is the norm** — Teams in regions or industries where Signal is the default business messenger, coordinating with suppliers/contractors. Captures operational vendor/supplier coordination otherwise absent from the system of record.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| Inbound direct/group message (`dataMessage`) | A received Signal message envelope delivered to our linked device. **Verified** (claim [4]). | `source`, `sourceNumber`, `sourceUuid`, `sourceName`, `sourceDevice`, `timestamp` (ms), `dataMessage.message`, `groupInfo` | Core conversational signal: who said what to our org account and when. Maps alongside Slack/Discord/Telegram channel messages. Captures coordination, customer/partner comms, and informal decisions made over an E2E channel invisible elsewhere. |
| Outbound / own-device sent message (`syncMessage.sentMessage`) | When the linked account sends from any of its devices, the daemon receives a SyncMessage wrapping a `sentMessage`. **Verified** (claim [5]). Must be normalized on a separate parse path from inbound. | `syncMessage.sentMessage.destination`, `destinationNumber`, `destinationUuid`, `timestamp`, `message` | Outbound side of the conversation — completes both halves of each thread (request+reply, promise+confirmation) essential for relationship/commitment intelligence. |
| Sender / contact identity | Per-message sender identity. UUID is the stable actor key; phone numbers can change. | `sourceUuid` (stable), `sourceNumber` (phone), `sourceName` (profile display name) | Actor dimension — powers entity resolution / relationship graph. UUID-keyed `actor_ref`, analogous to `telegram:user:{id}`. |
| Group / conversation context | Signal group metadata identifying the conversation. **Inferred** from Signal's group-message model — confirm exact field names (`groupInfo` vs `groupId`) in open questions before implementation. | `groupId` / `groupInfo`, group membership (where exposed) | Channel/dialog dimension (analogous to `telegram_dialog`) — lets us scope conversations and cluster signals by conversation. |
| Reactions / typing / read receipts / edits | Signal delivers engagement sub-types within the envelope family. **Inferred** — signal-cli likely surfaces as `receiptMessage` / `typingMessage` / `reaction` / `editMessage`; confirm exact shapes. | `reaction.emoji` + `targetTimestamp`, `receiptMessage`, `editMessage` | Low-density engagement/acknowledgement signals. Likely v2 scope; primary value is message bodies. |
| Attachments (files / images) | Files/media exchanged over Signal; signal-cli can persist to disk on receive. **Inferred** (not verified from primary sources). | `attachments[].id`, `contentType`, `filename`, `size` | Documents/contracts/screenshots shared off-channel. Higher PII/compliance sensitivity; treat as metadata-only initially and gate as v2. |

---

## API & authentication

**API style:** No official API exists. `signal.org/docs` is a cryptographic-spec index (XEdDSA/VXEdDSA, X3DH, PQXDH, Double Ratchet, Sesame, ML-KEM Braid) and `libsignal` is a Rust crypto protocol library — neither exposes REST, HTTP endpoints, OAuth, webhooks, rate limits, or retention windows (verified [0][1][2][3]). The practical surface is `signal-cli`'s JSON-RPC daemon: a subscription/streaming PULL surface, NOT REST and NOT a webhook.

**Key endpoints (signal-cli JSON-RPC):**

| Endpoint | Verification status |
|---|---|
| `subscribeReceive` — start streaming receive, returns subscription id | VERIFIED [6] |
| `unsubscribeReceive` — stop streaming | VERIFIED [6] |
| `receive` — one-shot drain in manual/daemon mode | UNVERIFIED (prior knowledge; confirm) |
| `link` / `register` — device linking and registration during install | UNVERIFIED (prior knowledge; confirm exact method names) |
| Envelope shapes: `dataMessage` (inbound), `syncMessage.sentMessage` (outbound) | VERIFIED [4][5] |
| `send` / `sendReaction` — outbound; NOT needed for ingestion | UNVERIFIED |

**Auth mechanism:** Device linking, not a token or OAuth. `signal-cli` either registers as a primary device (own phone number + SMS/voice verification) or links as a **secondary device** to an existing Signal account by consuming a linking QR/URI — the same mechanism Signal Desktop uses. The durable credential is the per-account linked-device identity/session persisted by `signal-cli` on disk, analogous to Telegram's `StringSession` auth_key. There is no bearer token, no OAuth redirect, no signing secret.

**Org vs. per-user:** Strictly per-account. There is NO org-wide or admin token, no central tenant grant, no directory-wide consent. Each Signal account to be ingested must be individually device-linked by an operator. An org can only cover accounts it owns/controls (e.g. a company support/ops number, or employee accounts with explicit consent and a manual device-link step).

**Scopes:** No scope model. A linked device receives the full message stream the account is entitled to after link time — all-or-nothing for that account, no read-only or per-resource scoping.

**Admin requirements:** Operator-mediated install via a connect wizard that performs the QR/URI device-link or phone+code registration out-of-band, then persists the `signal-cli` account state into `encrypted_secrets`. No headless server-side OAuth. Mirrors Telegram's interactive/operator-mediated install posture (ADR-0003 §3), but using Signal's device-linking mechanism rather than phone+SMS code.

---

## Backfill (historical pull)

**Supported:** Effectively NO for true historical backfill. This is the hard divergence from every other source in the pipeline.

Signal is end-to-end encrypted with no server-side message archive; the relay holds undelivered messages only transiently. A freshly linked device receives messages from the moment it links forward — there is no `getHistory` equivalent (no server has the plaintext; Signal deliberately does not sync full history to a newly linked device beyond limited recent context the primary may push). Signal cannot honor the uniform backfill contract that Telegram (`messages.getHistory`), Jira, Mercury, and every other current source satisfies.

**Mechanism (closest approximation):**
1. Drain whatever the relay has queued at link time via a one-shot `receive` call.
2. Optionally consume the limited recent-history/contacts sync the primary device pushes to a newly linked device.

Neither is a paged, cursored, deep historical walk.

**History depth:** Approximately zero for messages predating the device link, with at best a small, non-guaranteed recent-context sync from the primary device. Exact sync behavior is unverified — see open questions.

**Pagination:** None. No server-side history means no page cursor. The only cursor concept is a live high-water (last-received message timestamp) used to dedup/resume the stream across daemon restarts.

**Rate limits:** Not documented for the unofficial path; not present in any primary source ([2][3] confirm no published rate limits/retention). Treat as unknown and conservative.

**Maps to our pipeline:** Signal does NOT fit the `ShardFetch` pull-loop model. There is no `offset_id`, `next_page_token`, or time-window cursor to walk. The planner would emit at most one shard per install (or zero), and the fetcher would only perform the thin link-time queue drain before marking `end_of_data=True`. The `workflow_states` cursor reduces to a live high-water timestamp rather than a meaningful backfill position. Model Signal as **live-only** (or live plus thin link-time catch-up) — the backfill machinery fires once and completes immediately. This is a new contract precedent requiring an ADR analogous to ADR-0003.

---

## Live ingestion (real-time)

**Mechanism:** Subscription/streaming PULL via `signal-cli` JSON-RPC `subscribeReceive` (verified [6]). A single long-running gateway-style worker holds the JSON-RPC connection to the `signal-cli` daemon, receives decrypted envelopes, and shadow-writes each onto the raw tier with `ingress_kind='gateway'`, flowing through the normalizer → observation_writer chain. There is NO HTTP webhook (verified [2][6]).

**Events:**

| Event | Verification |
|---|---|
| `dataMessage` (inbound direct/group message) | VERIFIED [4] |
| `syncMessage.sentMessage` (outbound/own-device message, separate envelope shape) | VERIFIED [5] |
| `receiptMessage` / `typingMessage` / `reaction` / `editMessage` (engagement sub-types) | UNVERIFIED — inferred from Signal protocol surface; prior knowledge; confirm for v2 |

**Signature scheme:** None at the application layer. There is NO HMAC/signature gate — the trust boundary is the authenticated, E2E-decrypted Signal connection held by our own linked device. Message authenticity is guaranteed by the Signal Protocol itself, not by a webhook signature we verify. Same posture as Discord gateway and Telegram live session (ADR-0003 §4: "no HMAC signature gate").

**Notes:** Requires a single-instance lease (Redis `leader_lock`, the Discord/Telegram pattern): one active receiving device-connection per Signal account at a time to avoid receipt/decryption contention. The worker must persist a last-received high-water timestamp to dedup across restarts. Unlike Telegram's `updates.getDifference`, there is no server-side replay for missed windows — a gap while the worker is down is genuinely and permanently lost. This is a weaker recovery story than any current source and must be explicitly documented and accepted.

**Maps to our pipeline:** Path **(d) — gateway/direct-dispatch (no HTTP)**. Same path as Discord gateway and Telegram MTProto (ADR-0003). `_EXPECTED_LIVE_STATUS[signal] = set()`. No webhook verifier file, no tenant resolver extractor, no router entry needed — the gateway worker dispatches directly to the handler.

---

## Can we gather this? — feasibility

**Verdict: Yes, but only narrowly.**

We can gather Signal traffic for an account we own/control by linking `signal-cli` as a device (or registering it as primary). There is no admin/org token and no tenant-wide grant — coverage is strictly per-account, operator-mediated, and from link time forward only.

**Access model:** Per-account device-link via `signal-cli` (secondary-device linking or primary registration). Operator-mediated connect wizard persists `signal-cli` account state as the durable secret. No OAuth, no admin API, no org directory integration.

**Legal/ToS:** Material risk. There is no official, sanctioned ingestion API; `signal-cli` is an unofficial community client. Signal's terms and the unofficial-client posture mean automated/sustained use risks account standing. The research refuted the stronger claim that this is "explicitly disqualified" — the official Signal org does not publish a prohibition against signal-cli — but the absence of any sanctioned B2B API ([0][1][2][3] confirmed) is the real constraint. Ingest only accounts the org owns or has explicit, documented written consent for. Do not bulk-send.

**Compliance/PII:** High-sensitivity surface. The point of Signal is E2E encryption; ingesting it deliberately removes that protection at our linked device — decrypted PII and confidential content (legal matters, deal negotiations, source-protected communications) lands in plaintext in `observations` and the Kafka/S3 raw tier. Strong consent, retention policy, access control, and possibly redaction requirements apply. Attachments/media are especially sensitive; treat as metadata-only initially. Capturing only org-owned accounts mitigates but does not eliminate counterparty-PII concerns.

**Blockers:**
1. No deep historical backfill — breaks the uniform backfill+live contract; Signal is live-only. This needs an explicit ADR.
2. Unofficial client (`signal-cli`) with account-standing/ToS risk and no SLA or official support guarantee.
3. Per-account, operator-mediated linking does not scale to org-wide coverage without per-user consent and a manual device-link step per employee.
4. Weak live recovery: worker downtime = permanently lost message window. No server-side `getDifference`/replay. This is a lower source-quality floor than any currently wired source.
5. Compliance/consent overhead for decrypting E2E content into a persistent pipeline.

**Confidence level:** High (feasibility assessment is grounded in verified primary sources). **Legal risk: High.**

---

## How it maps onto our pipeline

```
SOURCE: signal

Auth shape →            persistent-session (closest to telegram MTProto archetype)
                        NOT OAuth/token; device-linking via signal-cli QR/URI or phone+code.
                        token storage: signal-cli account session state persisted in
                        encrypted_secrets, referenced as session_secret_ref on signal_installations
                        (mirrors telegram_installations.session_secret_ref).
                        No bearer token, no realm, no OAuth redirect, no HMAC secret.

Install table →         signal_installations (cols: tenant_id FK, account_phone_or_uuid,
                        session_secret_ref, enabled, created_at)
                        child resource table: optional signal_groups (conversation labels) — v2
                        One install row per (tenant, Signal account/phone)

Backfill cursor →       dimension: N/A — does not fit. No server-side history; no offset or
                        page-token cursor. Only cursor is live high-water last-received-timestamp
                        (per install) for dedup/resume across daemon restarts.
                        high_water field: last_received_at   incremental floor: link_time (not
                        configurable)   rate-limit-safe empty page: N/A
                        shard_kind: "signal_account"   one-shard per install (trivial / immediate
                        end_of_data — the planner emits one shard; the fetcher drains the
                        link-time queue once and marks done)

Live mechanism →        gateway/direct-dispatch (no HTTP) — path (d) from the contract.
                        signal-cli JSON-RPC subscribeReceive subscription held by a long-running
                        signal_gateway_worker; envelopes shadow-written with ingress_kind='gateway'.
                        NO webhook, NO router entry, NO tenant_resolver entry.
                        signature: none — trust = authenticated E2E-decrypted linked-device connection.
                        tenant identifier in payload: N/A (gateway worker is install-scoped)
                        _EXPECTED_LIVE_STATUS[signal] = set()

New files →             integrations/signal/client.py (signal-cli JSON-RPC wrapper, import-guarded)
                        integrations/signal/onboarding.py (finalize_install: link/register + UPSERT
                          signal_installations + onboarding_trigger)
                        integrations/signal/records.py (build_message_record shared by both ingress paths)
                        integrations/signal/gateway/{worker,dispatch,client,session_state}.py
                        fetchers/signal.py (trivial — link-time queue drain only, immediate end_of_data;
                          no meaningful page cursor; NOTE: nearly empty vs other fetchers)
                        planners/signal.py (single-or-no shard planner, one shard per install)
                        handlers/signal.py (@register('signal:message') — branches on dataMessage
                          vs syncMessage.sentMessage shapes)
                        idempotency/__init__.py — signal_message constructor
                        channel_mapping.py — ('signal','gateway') -> signal:message
                        envelope.py — SourceLiteral += 'signal'
                        progress/events.py + workflows VALID_SOURCES
                        synthetic harness: fixtures/signal_generator.py, mock_clients/signal.py,
                          live_generators/signal_gateway.py, run_all_sources entry as source #13
                        NO signatures/<s>.py (no HMAC/signature gate)
                        NO meaningful fetchers/<s>.py cursor loop (no server history)
                        NO tenant_resolver extractor (gateway is install-scoped)
                        NO router maps entry (direct dispatch)

Migration →             NNNN_signal.sql: signal_installations (+RLS)(+optional signal_groups child)
                        + source_check widening on all 4 tables (source_onboarding_runs,
                        onboarding_shards, ingestion_failures, onboarding_triggers) carrying every
                        prior source including telegram (0094). Landmine: must list ALL prior sources
                        as strict superset — see migration-source-CHECK re-run landmine memory.
                        signal-cli daemon is an external runtime dependency (like Telethon for
                        Telegram) — import-guard so synthetic gate runs without it.

Observation kind(s) →   kind: signal (all message observations — both dataMessage + syncMessage
                          shapes; neither is a state_change/status transition)
                        channel: "signal:message"
                        trust_tier: "attested_agent" (human conversational channel, same as
                          Telegram/Slack/Discord)
                        external_id: versioned by timestamp (messages are immutable but edits
                          produce new shapes); namespaced by install:
                          signal:{installation_id}:{sourceUuid|groupId}:{timestamp}
                          (install_id in namespace is MANDATORY — global observations UNIQUE has
                          no tenant_id, so without it cross-tenant collision drops data silently)
                        source_actor_ref: signal:user:{sourceUuid}

Rate-limit risk →       Unknown/unquantified — no published limits on the unofficial path [2][3].
                        Risk is account-standing/ban risk from sustained automated use (the
                        Telegram 'accounts under observation' analogue), not throughput. Keep
                        request patterns conservative; never bulk-send via the daemon.

Legal/ToS risk →        High. Unofficial client (signal-cli), no sanctioned B2B API, E2E-
                        decryption of PII/confidential content into plaintext pipeline. Strictly
                        own/consented accounts only. Heavy consent + retention + access-control
                        requirements. Attachments: metadata-only initially.

Effort →                M (leaning M-minus on code volume, heavy on compliance/ops lift)
```

**What a realistic integration actually looks like vs. the standard archetypes:**

The code surface is genuinely a near-clone of the Telegram gateway (ADR-0003) and is simpler in two ways: there is no backfill fetcher to build (the "fetcher" is a one-shot drain that completes immediately), and there is no webhook verifier, tenant resolver, or router entry. The gateway worker, leader-lock, records module, handler, and idempotency constructor are all structurally identical to Telegram's.

Where Signal materially diverges from every archetype:

1. **The backfill contract is broken by design.** Every current source satisfies a cursor-walked historical pull (even Grafana, which time-walks backward). Signal cannot. The planner emits one shard; the fetcher drains whatever is in the relay queue at link time and immediately returns `end_of_data=True`. This is not a limitation to work around — it is the architecture. An ADR is required (like ADR-0003 for Telegram, but specifically addressing the live-only precedent and what "no backfill" means for source quality and the overlap-gate harness in `run_all_sources.py`).

2. **The `_EXPECTED[signal]` count in the validation harness will be near-zero** (or a small fixed number equal to the link-time drain, not a meaningful historical corpus). The preflight and overlap-gate harness need to accommodate a source where `backfill_count ≈ 0` is a passing result, not a failure.

3. **The install wizard is more operationally complex than Telegram.** Signal's device-linking is interactive (QR scan or URI) and requires the primary device to approve. The wizard must guide an operator through this step and capture the resulting `signal-cli` session state into `encrypted_secrets`. This is the single highest operational-complexity element.

4. **Live recovery is weaker than any current source.** Discord has the gateway and can re-fetch recent messages; Telegram has `updates.getDifference`. Signal has neither — a restart gap is permanently gone. This downgrade in source quality must be documented and explicitly accepted by product.

---

## Open questions

- Exact `signal-cli` envelope field names for groups (`groupInfo` vs `groupId`), reactions, edits, and receipts — verified claims cover `dataMessage` sender fields, `syncMessage.sentMessage`, and `subscribeReceive` [4][5][6] but NOT the full group/reaction/edit shapes. Must confirm from `signal-cli-jsonrpc.5.adoc` before handler implementation.
- How much recent context, if any, does a newly linked `signal-cli` device receive from the primary device (contacts/group sync; any recent-message replay)? This determines whether a "thin link-time catch-up" pass is worth implementing or whether Signal is purely live-from-link-time with zero initial corpus.
- Register `signal-cli` as a PRIMARY device (own phone number, full receive) or link as a SECONDARY device to an existing account (less intrusive, but secondary-device delivery/sync semantics differ)? This affects the install wizard design, the account custody model, and which employees/numbers can be covered.
- Real rate limits / pacing thresholds and concrete account-ban risk for sustained `signal-cli` daemon use — not derivable from primary sources ([2][3] confirm none published). Would require a throwaway-account spike similar to the Telegram `TODO(human)` in ADR-0003.
- Compliance/legal sign-off on decrypting E2E content into the pipeline: consent model for employee accounts, counterparty-PII handling (messages from non-consenting external parties land in plaintext), retention window, and whether attachments/media are ingested at all vs. metadata-only. This is a human/legal decision, not an engineering one.
- Live-recovery gap policy: with no server-side `getDifference`/replay, what is the acceptable data-loss window during gateway-worker downtime, and does that meet the source-quality bar other sources are held to? Does Signal qualify as "authoritative" or should it be explicitly gated as "best-effort/experimental"?
- Whether maintaining an unofficial `signal-cli` runtime dependency is acceptable long-term given no SLA, no official Anthropic/Signal relationship, and potential client-version churn as the Signal protocol evolves. Should this source be provisionally marked as experimental rather than production-grade?
- ADR scope: does the live-only precedent set by Signal warrant its own ADR, or should it extend ADR-0003 (Telegram) to cover the class of "gateway-only, no server-history" sources?

---

## Sources

- `https://signal.org/docs/` (primary) — cryptographic spec index confirming no REST/API/webhook/OAuth surface [0][1][2][3]
- `https://github.com/AsamK/signal-cli/blob/master/man/signal-cli-jsonrpc.5.adoc` (primary) — JSON-RPC envelope reference; verified `dataMessage` sender fields, `syncMessage.sentMessage` destination fields, `subscribeReceive`/`unsubscribeReceive` [4][5][6]
- `https://github.com/signalapp/libsignal` (primary) — confirms libsignal is a crypto protocol library, not an integration point [1][2][3]
- `https://github.com/AsamK/signal-cli` (secondary) — community CLI project; overall architecture, device-linking model, daemon mode [6]
- `https://signald.org/` (primary) — alternative Signal daemon; cross-reference for envelope shapes and JSON-RPC patterns [6]
- `https://github.com/bbernhard/signal-cli-rest-api` (primary) — REST wrapper around signal-cli; secondary reference for envelope shapes and available methods [6]
- `https://bbernhard.github.io/signal-cli-rest-api/` (primary) — REST API docs for the signal-cli-rest-api wrapper [6]
