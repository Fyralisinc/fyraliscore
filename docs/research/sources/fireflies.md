# Fireflies.ai — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (8/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict:** Clones the Jira/Mercury/Grafana API-token Bearer archetype · can-we-gather: **yes** (conditional on plan tier for org-wide coverage) · effort: **M**.

---

## TL;DR

Fireflies.ai exposes a **GraphQL API** (single POST endpoint) that returns meeting transcripts, AI-generated summaries, action items, and participant rosters. We mint one static Bearer API key per tenant, store it as a `secret_ref` in a dedicated `fireflies_installations` table, and drive backfill via a date-windowed offset pager (`fromDate`/`toDate` + `skip`/`limit=50`). Live ingestion arrives via an HMAC-SHA256 webhook that carries only a thin `meetingId` payload — the handler must issue a `transcript(id:)` hydrate read before emitting an observation. The principal catch is a **very tight rate-limit budget** (50/day Free, 500/day Pro, 60/min Business+): deep backfill on lower-tier plans is slow, and every live hydrate read competes for the same quota. Verbatim `sentences` content is the heaviest PII surface in the pipeline — default to summary + action items + participant metadata, with full verbatim gated behind an explicit per-tenant opt-in.

---

## What companies use it for — and what signal lives there

Fireflies.ai is a meeting-intelligence platform: it auto-joins calls, records audio/video, generates verbatim transcripts, and then applies AI to produce summaries, action items, and topic keywords. Four dominant usage patterns emerge in practice:

- **Sales and CS external meeting recording** (`whoUses`: revenue org, sales, CS, RevOps consuming summaries) — `signalWeCapture`: who from our side met which customer or prospect (participant emails → org resolution), meeting cadence and recency per account, and AI action items + keywords as deal-momentum and commitment signals.
- **Internal recurring meetings** (standups, 1:1s, all-hands, planning) (`whoUses`: eng, product, ops teams, managers) — `signalWeCapture`: internal collaboration graph (who meets whom, how often), meeting load and cadence per team, topic drift, action items as internal commitment signals.
- **Hiring and interview panel recording** (`whoUses`: recruiting and hiring managers) — `signalWeCapture`: interview activity and panel composition; contains candidate PII; high compliance weight — likely keyword- or summary-only ingestion with verbatim suppressed.
- **Leadership decision and commitment tracking** (`whoUses`: leadership, chief of staff, PMO) — `signalWeCapture`: AI overview + action items as decision records (what was decided, by whom, owed by when) without parsing full transcripts.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| Transcript meeting | Per-meeting record; participant email is the cross-source join key for entity resolution | `id`, `title`, `date`, `host_email`, `participants`, `transcript_url` | Who met whom and when; meeting cadence; strongest cross-source join key is participant email |
| Sentences / verbatim text | Verbatim utterances with speaker attribution | `sentences.text`, `speaker_name`, `start_time` | Richest signal but heaviest PII; gate verbatim behind per-tenant opt-in; store raw for audit |
| AI summary / action items | Generated overview, action items, keywords, meeting type | `summary.overview`, `action_items`, `keywords` | Pre-distilled decision and commitment signals, low noise, no raw verbatim required |
| Participants / attendees | Organizer and attendees by email and display name | `participants.emails`, `organizer_email`, `host_email` | High value for entity resolution to internal people and external orgs |
| User account | Users under the account from the `users` query | `user_id`, `email`, `name`, `num_transcripts` | Coverage map; signals whether ingestion is org-wide or per-seat |
| Media URLs | `transcript_url`, `audio_url`, `video_url` | `audio_url`, `video_url`, `transcript_url` | Low; reference and audit link; **signed URL expiry is an open question** (see Open Questions) |

---

## API & authentication

**API style:** GraphQL, single endpoint `POST https://api.fireflies.ai/graphql` — not REST. All queries are document-POSTs; there is no REST resource hierarchy. This makes Fireflies the **first GraphQL-native source** in the repo: the client must construct and POST GraphQL documents and parse the `data` / `errors` response shape rather than calling REST paths.

**Key operations (VERIFIED):**

| Operation | Purpose |
|---|---|
| `transcripts(fromDate, toDate, limit, skip)` | Backfill and windowed incremental reads |
| `transcript(id: $id)` | Single-record hydrate; used by the live webhook path after receiving a thin `meetingId` |
| `users` | Roster probe; coverage diagnostic for org-wide vs. per-seat visibility |

**Docs:** `https://docs.fireflies.ai/graphql-api/query/transcripts`

**Auth mechanism:** Static account Bearer API key — `Authorization: Bearer <key>`. No OAuth, no scopes, no per-request token mint. The key is resolved once from the secret store at install time. This is the same shape as Jira, Mercury, and Grafana (the API-token Bearer/Basic archetype), with the simplification that there is no Basic-auth base64 wrapping and no per-site `base_url` — the endpoint is fixed.

**Scopes:** None documented. Access is all-or-nothing per key; server-side visibility is determined by the key owner's account membership and plan tier. Scope enforcement is implicit.

**Org-token vs. per-user (OPEN — see Open Questions):** A workspace or Business/Enterprise admin key likely spans all org members (true org-wide token); a standard user key is per-seat only. The exact plan tier required for org-wide key coverage is **unverified**. If org-wide keys are plan-gated, per-seat keys (analogous to our Slack per-user `xoxp` path) may be required for full coverage.

**Admin requirements:** Account owner mints the key from the Fireflies dashboard. A Business+ plan and workspace-admin role is likely required for an org-wide key (unverified). The webhook signing secret (16–32 char HMAC secret) is also set by an admin.

---

## Backfill (historical pull)

**Supported:** Yes. The `transcripts` query is a documented backfill interface.

**Mechanism:** Repeated `transcripts` calls with `fromDate`, `toDate`, `limit` (max 50), and `skip`. Walk ISO 8601 date windows and page within each window by advancing `skip += page_len` until a short or empty page indicates the window is exhausted, then step the date window forward.

**Pagination:** Offset-based (`skip`). No opaque continuation token. Skip-paging a mutating set can drop or duplicate records, so the strategy is narrow date windows + `external_id` dedup at ingestion. This mirrors the Jira `startAt` offset pager.

**History depth:** Bounded by Fireflies-side data retention, which is **not documented** (exact retention horizon is open). The practical depth is also limited by the rate-limit budget (see below).

**Rate limits:**

| Plan | Limit |
|---|---|
| Free | 50 requests/day |
| Pro | 500 requests/day |
| Business / Enterprise | 60 requests/minute |

With a 50-record page cap and a 50 req/day Free budget, one day of Free tier exhausts in a single page. Deep backfill requires a Business+ account or multi-day chunking. Token-bucket pacing with 429 + `Retry-After` handling (matching Jira's request backoff pattern) is mandatory.

**Maps to our pipeline:** The date-windowed skip cursor maps directly onto the `workflow_states.state_data["cursor"]` slot. Cursor shape: `{ "from_date": ISO, "to_date": ISO, "skip": int, "high_water_date": ISO }`. `shard_kind = "fireflies_transcript"`. Because Fireflies is account-wide (no child resource enumeration required), the planner emits **one shard per install** (org-wide, same as `grafana`). The `high_water_date` becomes the reconciler's warm-start floor. The N1 invariant (S3 write → publish → flush → advance cursor) applies unchanged.

---

## Live ingestion (real-time)

**Mechanism:** HMAC-signed webhook → `202` → Kafka → hydrate read. Fireflies fires a webhook on `Transcription complete`. The thin payload carries `meetingId`, `eventType`, `clientReferenceId` — **not the transcript body** — so the fetcher must issue a `transcript(id: meetingId)` GraphQL read to hydrate the full record before emitting an observation. This hydrate read consumes the rate-limit budget.

**Events:**

| Event | Meaning |
|---|---|
| `Transcription complete` | Transcript ready for viewing; the primary live signal |

**Signature scheme:** HMAC-SHA256 over the raw request body, delivered in the `x-hub-signature` header with a `sha256=` prefix — the same `sha256=<hex>` shape as GitHub, Jira, and Mercury. The shared secret is 16–32 characters. Verification: constant-time `hmac.compare_digest` over the literal body bytes, stripping the `sha256=` prefix to obtain the hex digest.

> **OPEN:** It is not yet confirmed whether the HMAC is computed over the raw body alone or a timestamp+body envelope. No replay-window or anti-replay timestamp header is documented, but this should be confirmed before implementation (see Open Questions).

**Tenant resolution:** From `clientReferenceId` in the payload, or fallback to the `provider_installations` row via the install key. A `_extract_fireflies(payload, headers)` extractor in `tenant_resolver.py` pulls this identifier.

**Maps to our pipeline:** Live path **(a) — HMAC webhook → Kafka cutover → 202** (the default for HMAC token sources). Add `fireflies` to `_HMAC_SOURCES`, `_CUTOVER_ENABLED_PROVIDERS`, and `_PROVIDER_TO_SHADOW_SOURCE` in `router.py`. The two-step webhook-then-hydrate is a new pattern in the pipeline (no current source does this), but it is mechanically straightforward: the handler receives the thin webhook body, extracts `meetingId`, calls `transcript(id:)` on the client, and constructs the `ObservationDraft` from the hydrated response.

---

## Can we gather this? — feasibility

**Verdict: yes** — for our own Fireflies account, this is well-supported. We mint an API key, store it as a `secret_ref`, and the existing token-auth backfill chain plus a GraphQL client pulls transcripts; webhooks deliver live.

**Access model:** Account or admin API key. Org-wide coverage requires a key that spans all member transcripts — likely a Business+ admin key (plan tier unverified). If only per-seat keys are available on lower plans, coverage degrades analogously to our Slack-DM per-user token discussion.

**Legal / ToS:** Standard SaaS API usage of our own subscription data. The GraphQL API + webhooks are documented, publicly available, and clearly the intended programmatic access pattern. No scraping gray area. Recording consent law (two-party consent jurisdictions) is a **capture-time** concern that the ingested content inherits — we did not record the meetings, Fireflies did — but PII handling obligations attach to us once we store the content.

**Compliance:** This is the **heaviest PII surface** of the token-auth sources in the pipeline. Transcripts contain verbatim human conversation; content may include financial, strategic, HR, or candidate data. The API returns plaintext to a valid key — there is no end-to-end encryption at the API layer. Recommended default: ingest `summary`, `action_items`, `keywords`, and participant metadata. Gate full `sentences` verbatim behind a per-tenant opt-in flag. Store raw under existing audit handling. Mirror Fireflies-side deletes via the reconciler (GDPR data-subject delete support is an open question — see below).

**Blockers:**
- No hard technical blocker for own-account ingestion.
- Soft blockers: tight rate-limit budget makes deep backfill slow on Free/Pro; org-wide coverage may require a Business/Enterprise admin key (unverified plan requirement); compliance review is likely required before ingesting verbatim transcript bodies at scale.

**Confidence: high** for core feasibility; **medium** for org-wide coverage scope and compliance sign-off path.

---

## How it maps onto our pipeline

```
SOURCE: fireflies

Auth shape →            API-token Bearer, per-tenant
                        token storage: secret_ref on fireflies_installations
                        no OAuth, no realm, no per-request mint
                        closest twin: Jira/Mercury/Grafana archetype;
                        plain Bearer (no Basic base64 wrapping, no per-site base_url)

Install table →         fireflies_installations
                          cols: tenant_id FK, secret_ref (Bearer key),
                                webhook_secret_ref (HMAC 16–32 char),
                                UNIQUE(tenant_id)
                        child resource table: none — org-wide, one shard per install
                          (same as grafana_installations)

Backfill cursor →       dimension: date-window-walk + offset (skip)
                        high_water field: high_water_date (max transcript date seen)
                        incremental floor: earliest desired fromDate (config or onboarding default)
                        rate-limit-safe empty page: yes (short/empty page = window exhausted)
                        shard_kind: "fireflies_transcript"
                        one-shard (org-wide, planner emits 1 shard per install)

Live mechanism →        HMAC webhook → 202 (Kafka cutover path)
                        signature: x-hub-signature header, format sha256=<hex>
                          (same shape as GitHub, Jira, Mercury)
                        HMAC computed over raw body bytes, constant-time compare
                        NOTE: thin payload (meetingId + eventType only) →
                          handler must call transcript(id: meetingId) to hydrate
                          before emitting ObservationDraft
                        tenant identifier in payload: clientReferenceId
                          (extractor: _extract_fireflies in tenant_resolver.py)

New files →             services/ingest/integrations/fireflies/
                          __init__.py · client.py · onboarding.py
                        services/ingest/ingestion/fetchers/fireflies.py
                          (SHARD_KIND_TRANSCRIPT = "fireflies_transcript",
                           GraphQL document POST client, date-window+skip cursor,
                           rate-limit-aware 429 backoff, _fyralis_account_id namespace tag)
                        services/ingest/ingestion/planners/fireflies.py
                          (one shard per install, org-wide)
                        services/ingest/ingestion/handlers/fireflies.py
                          (@register("fireflies:transcript"), hydrate-then-draft logic)
                        services/app/webhooks/signatures/fireflies.py
                          (Verifier: x-hub-signature sha256=<hex>, HMAC-SHA256,
                           register in signatures/__init__.py VERIFIERS)
                        services/ingest/ingestion/idempotency/__init__.py
                          (fireflies_transcript constructor — versioned if summaries
                           are mutable, immutable on transcript id otherwise)
                        services/ingest/ingestion/workflows/shard_fetch.py
                          (_LOAD_FIREFLIES_INSTALL_SQL + _load_install branch)
                        services/app/webhooks/tenant_resolver.py
                          (_extract_fireflies, PROVIDER_EXTRACTORS entry)
                        services/app/webhooks/router.py
                          (_PROVIDER_TO_SHADOW_SOURCE, _CUTOVER_ENABLED_PROVIDERS,
                           _HMAC_SOURCES additions)
                        services/ingest/ingestion/fetchers/_clients.py
                          (build_fireflies_client + open_fireflies_client)
                        synthetic: mock client + mock GraphQL server + tests

Migration →             0095_fireflies.sql
                          (1) CREATE TABLE fireflies_installations (RLS + tenant_isolation policy)
                          (2) source_check widening on all 4 substrate tables:
                              source_onboarding_runs, onboarding_shards,
                              ingestion_failures, onboarding_triggers
                          NOTE: 0095 is the next free slot (0094 = telegram);
                          integration tests adding fireflies MUST clean up after
                          themselves so prior widening migration reruns are not poisoned

Observation kind(s) →   kind: signal (meetings are point events, not state transitions)
                        channel: "fireflies:transcript"
                        trust_tier: authoritative (Fireflies is the system of record)
                        add "fireflies:transcript" to CHANNEL_TRUST_MAP
                        optional: kind signal, object_type "action_item" per item
                        external_id: versioned if AI summaries are mutable post-generation
                          (fireflies:{account_id}:transcript:{id}:{updated_at});
                          immutable if summaries are fixed after Transcription-complete
                          (fireflies:{account_id}:transcript:{id})
                        namespaced by: account_id (from install row) — required for
                          global UNIQUE(source_channel, external_id, occurred_at)
                          dedup without cross-tenant collision

Rate-limit risk →       HIGH and defining.
                        Free: 50 req/day; Pro: 500 req/day; Business+: 60 req/min.
                        50-record page cap means Free exhausts in one page.
                        Live hydrate reads compete for the same budget as backfill.
                        Mitigate: token-bucket pacing, honor 429 + Retry-After
                        (mirror Jira's request backoff), narrow date windows, default
                        to summary-level field selection, assume Business+ for prod tenants.

Legal/ToS risk →        LOW at access level (documented public API, own-data use).
                        MEDIUM–HIGH at content level: verbatim PII, possible financial/
                        HR/candidate data, recording consent law inheritance.
                        Gate verbatim sentences behind per-tenant opt-in flag.
                        Compliance review recommended before production verbatim ingestion.

Effort →                M.
                        Token auth + HMAC webhook verifier + dedicated install table
                        is fully precedented by Jira/Grafana/Mercury — most wiring is
                        copy-shaped. New work: a GraphQL client (first GraphQL source
                        in the repo, POSTs documents, parses data/errors), a
                        date-window + skip pager with rate-limit-aware backoff, and the
                        webhook-then-hydrate two-step (thin payload requires a second
                        API call before emitting). No new infra. Not L because there is
                        no OAuth dance, no realm, no per-resource child tables required.
```

### Prose walk-through

**Auth archetype.** Fireflies clones the **Jira/Mercury/Grafana API-token Bearer** archetype: one long-lived static key per tenant, resolved once from the secret store via `build_fireflies_client(install, *, pool)` in `fetchers/_clients.py`. Unlike Jira (Basic base64 wrapping) and unlike Grafana (per-instance `base_url`), the Fireflies endpoint is fixed and the header is plain `Authorization: Bearer <key>` — the simplest variant of the archetype.

**Install table.** `fireflies_installations` holds `tenant_id`, `secret_ref` (Bearer key), and `webhook_secret_ref` (HMAC secret). No child resource table is needed: the API is account-wide and the planner emits exactly one shard per install, mirroring `grafana_installations`. The `_LOAD_FIREFLIES_INSTALL_SQL` branch in `shard_fetch.py::_load_install` is mandatory — missing it parks the shard forever.

**Backfill cursor.** The cursor is a `FirefliesCursor` Pydantic model (`extra="forbid"`) carrying `from_date`, `to_date`, `skip`, and `high_water_date`. Each tick advances `skip += page_len`; on a short or empty page the date window steps forward. The fetcher tags every emitted record with `_fyralis_account_id` (from the install row) for external_id namespacing. The N1 invariant (publish → flush → advance) applies unchanged; a 429 returns `FetchResult(records=[...fetched so far], next_cursor=<unadvanced>, end_of_data=False)`.

**Live mechanism.** Path **(a): HMAC webhook → Kafka cutover → 202**. The `signatures/fireflies.py` verifier implements the `Verifier` protocol: read `x-hub-signature`, strip the `sha256=` prefix, HMAC-SHA256 over the raw body bytes, constant-time compare. The secret is loaded from the install row's `webhook_secret_ref`. The thin payload means the handler cannot construct an `ObservationDraft` from the webhook body alone — it must call `transcript(id: meetingId)` to hydrate and then build the draft. This two-step is new in the pipeline but mechanically straightforward. The hydrate call consumes rate-limit budget; at high live volume on lower plans, queuing or circuit-breaking may be required.

**Observation kinds and channels.** One channel: `fireflies:transcript`, `kind=signal` (meetings are point events, not state transitions), `trust_tier=authoritative`. Optionally a per-action-item `signal` observation of `object_type=action_item`. Add `"fireflies:transcript": "authoritative"` to `CHANNEL_TRUST_MAP`.

**external_id strategy.** Namespace by `account_id` (from the install row) to prevent cross-tenant collision on the global `UNIQUE(source_channel, external_id, occurred_at)` index. Whether the id is versioned depends on whether AI summaries are mutable after initial generation (open question): if mutable, use `fireflies:{account_id}:transcript:{id}:{updated_at}` (Jira pattern); if immutable after `Transcription complete`, use `fireflies:{account_id}:transcript:{id}`.

**Migration.** Next free slot is `0095_fireflies.sql` (0094 = telegram). The migration must widen all four source-registry `CHECK` constraints (`source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`) to include `fireflies` as a strict superset of every prior source. Integration tests adding Fireflies must clean up after themselves or older widening migration reruns will be poisoned (the source-CHECK rerun landmine).

---

## Open questions

- Does a workspace or admin API key return ALL org members' transcripts (true org token), or only the key owner's meetings (per-seat)? This decides org vs. per-user coverage and whether per-seat keys are needed analogously to the Slack DM path.
- Is org-wide key access gated behind Business or Enterprise plan, and what admin role is required to mint it?
- What is the exact `transcripts` webhook payload schema, and is the `x-hub-signature` value `sha256=<hex>` (GitHub-style prefixed) or bare hex (Grafana-style)? This determines whether the verifier strips a prefix.
- Is the HMAC computed over the raw body only, or a timestamp+body envelope? Is there any anti-replay header or timestamp in the webhook? No replay window is documented — confirm whether any such field exists.
- What is the Fireflies-side data retention horizon for `fromDate` backfill depth? Are `transcript_url`, `audio_url`, `video_url` time-limited signed URLs that expire, affecting later media re-fetch?
- Are AI summaries and action items mutable after initial generation (e.g., via re-summarization)? This determines whether `external_id` must be versioned on `updated_at` (Jira pattern) vs. immutable on `transcript_id`.
- Beyond `Transcription complete`, are there webhook events for deletion, sharing, or edits? These are needed for reconcile and GDPR data-subject delete mirroring.
- Does the `transcripts` query support any stable server-side cursor beyond `skip` offset? If only `skip` exists, narrow date windows + external_id dedup are the sole defense against drop/duplicate on a mutating set.
- Is there an account-level enumeration that lists all transcripts across all users in one pass, or must we iterate per `user_id`? Per-user iteration multiplies the already tight rate-limit budget.

---

## Sources

- `https://docs.fireflies.ai/graphql-api/query/transcripts` (primary — 6 claims)
- `https://docs.fireflies.ai/getting-started/introduction` (primary — 6 claims)
- `https://docs.fireflies.ai/fundamentals/concepts` (primary — 3 claims)
- `https://docs.fireflies.ai/fundamentals/introspection` (primary — 6 claims)
- `https://docs.fireflies.ai/llms-full.txt` (primary — 6 claims)
- `https://docs.fireflies.ai/getting-started/quickstart` (primary — 6 claims)
- `https://docs.fireflies.ai/fundamentals/authorization` (primary — 5 claims)
