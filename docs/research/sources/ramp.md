# Ramp — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (8/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: clones the Mercury/QuickBooks finance-source archetype · can-we-gather: yes · effort: M (Medium).**

---

## TL;DR

Ramp is an org-owned corporate-card, bill-pay, and reimbursement platform whose Developer API (`docs.ramp.com/developer-api/v1`) is a documented first-party integration surface built for machine-to-machine access by the account owner. We authenticate with a single OAuth2 client-credentials credential (no per-user consent), backfill the three primary list endpoints (transactions, bills, reimbursements) with cursor pagination, and subscribe to HMAC-signed webhooks for live events across the same resources. The signal is high-value, authoritative spend intelligence: real money movement, vendor relationships, forward cash-flow obligations, and card declines as a cash-risk state-change. The main catch is that three specs remain unverified before coding begins — the exact OAuth2 token endpoint and grant mode, the webhook signature header/encoding, and the pagination field names — none of which are hard blockers, only confirmation work.

---

## What companies use it for — and what signal lives there

Companies issue Ramp cards, route AP invoices through Ramp Bill Pay, and process employee expense reimbursements, all synced to their accounting system. Fyralis ingests the result.

- **Card spend for SaaS, cloud, travel, ads** — finance/ops admins set per-team budgets; every cardholder spends. Signal: per-vendor and per-team burn rate, the actual SaaS/infra/tooling stack the company pays for, headcount-correlated spend, and card declines as a cash-risk/control signal. Direct ground-truth on what the company actually buys.
- **Accounts payable via Ramp Bill Pay** — AP/finance team approves and pays; department heads approve. Signal: vendor obligations and payment cadence, approval-workflow throughput (created→approved→paid cycle time), upcoming `due_at` amounts as forward cash-flow/runway indicators, and `sync_status` into the GL as an accounting-hygiene signal.
- **Employee expense reimbursements** — employees submit out-of-pocket expenses; managers/finance approve. Signal: out-of-pocket spend by employee and category, approval latency, and total reimbursement liability — completes expense coverage beyond card spend.
- **Accounting sync reconciliation** — controllers track which transactions/bills are reconciled into QuickBooks/NetSuite. Signal: financial-operations maturity via `transactions.synced` and bills `sync_status` (NOT\_SYNCED vs BILL\_AND\_PAYMENT\_SYNCED) — a data-completeness signal for downstream reasoning.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| **Transaction (card spend)** [VERIFIED] | Settled corporate-card spend. `GET /developer/v1/transactions` (list, filter by card/`limit_id`/fund) + `GET .../{transaction_id}` | `id`, `amount`, `merchant`/vendor, `card_id`, `limit_id`, user/cardholder, `state` (cleared/declined), accounting category/`sk_category`, `memo`, `synced`, occurred/cleared timestamps | Highest-value: real money movement, vendor/tool spend, per-team burn, cardholder activity, declines as cash-risk `state_change` |
| **Bill (accounts payable)** [VERIFIED] | Vendor invoices and payment lifecycle. `GET /developer/v1/bills` (list) + `GET .../{bill_id}` | `id`, `amount`, `vendor`, `status` (OPEN\|PAID), `approval_status`, `sync_status` (BILL\_AND\_PAYMENT\_SYNCED\|BILL\_SYNCED\|NOT\_SYNCED), `due_at`, `issued_at`, `invoice_number`, `line_items`, `payment`, `entity_id`, `memo` | AP-side spend, vendor obligations, forward cash-flow signal from `due_at`; lifecycle steps are distinct versioned observations |
| **Reimbursement** [scope VERIFIED; endpoint shape inferred] | Employee out-of-pocket expense reimbursement requests. Read via `reimbursements:read` scope. | `id`, employee/user, `amount`, `status`, merchant/category, submitted/approved/paid timestamps (field names inferred, UNVERIFIED) | Per-employee spend behavior, approval throughput, out-of-pocket cost categories |
| **User / Department / Entity** [resource exists per scope docs; endpoints inferred] | Ramp users (employees), departments, legal entities. | `user_id`, `name`, `email`, `role`, `department`, `manager`, `entity_id` (field names inferred) | Entity-attribution backbone — resolves transactions/bills/reimbursements to real people, teams, and legal entities |
| **Card / Limit (budget/fund)** [limit\_id/fund VERIFIED as transaction filter; card/limit endpoints inferred] | Virtual/physical cards and spend limits/budgets. `limit_id` is the fund that transactions filter on. | `card_id`, `limit_id`, budget amount, owner, status, associated vendor/category lock (inferred) | Spend-control topology: budget allocation and utilization, vendor-locked cards, over-limit pressure detection |
| **Vendor** [referenced via bills.vendor; dedicated `vendors:read` resource inferred] | Counterparties the company pays (bill-pay vendors/merchants). | `vendor_id`, `name`, payment details (bank PII to be redacted), `category` | Counterparty/org graph: which companies we transact with, spend concentration, vendor onboarding over time |

---

## API & authentication

**API style:** REST/JSON over HTTPS, versioned under `/developer/v1`. Cursor/page-based list endpoints. HMAC-signed webhooks for live events. Docs: `https://docs.ramp.com/developer-api/v1`.

**Key endpoints:**

| Endpoint | Status |
|---|---|
| `GET /developer/v1/transactions` (list; filter by card, `limit_id`/fund) | VERIFIED |
| `GET /developer/v1/transactions/{transaction_id}` (fetch one) | VERIFIED |
| `GET /developer/v1/bills` (list) | VERIFIED |
| `GET /developer/v1/bills/{bill_id}` (fetch one) | VERIFIED |
| `POST/PATCH/DELETE /developer/v1/bills` + draft-bill & attachment subroutes (write — not needed) | VERIFIED (not used) |
| `GET /developer/v1/reimbursements` (list) | scope VERIFIED; endpoint shape inferred |
| Webhook events: `transactions.cleared`, `transactions.declined`, `transactions.synced`, `bills.created`, `bills.approved`, `bills.paid`, `bills.rejected`, `bills.archived`, `bills.ready_to_sync` | VERIFIED (event names) |
| OAuth2 token endpoint — likely `/developer/v1/token` or `api.ramp.com/developer/v1/token` | UNVERIFIED — must confirm |

**Auth mechanism:** OAuth2. Ramp uses a resource:permission scope model (VERIFIED). For ingesting our own org's data the correct grant is OAuth2 **client-credentials** (machine-to-machine: `client_id` + `client_secret` → short-lived bearer token, no per-user consent required). Ramp also supports authorization-code flow for acting on behalf of a Ramp user — not needed for read ingestion. The client-credentials grant is conservative prior knowledge; confirmation is required (see Open Questions).

**Scopes (read-only ingestion):**

| Scope | Status |
|---|---|
| `transactions:read` | VERIFIED |
| `bills:read` | VERIFIED |
| `reimbursements:read` | VERIFIED |
| `users:read`, `cards:read`, `limits:read`, `departments:read`, `vendors:read`, `accounting:read` | Inferred from resource:permission pattern — exact strings UNVERIFIED |

**Org-token vs per-user:** Org-level. One Ramp business creates one developer client app in the Ramp dashboard, grants read scopes, and ingestion runs against the whole org with one credential set. No per-user OAuth. Closest existing analogs: Mercury (single org token) and QuickBooks (OAuth2 app, token refresh), NOT the Slack/Telegram per-user model.

**Admin requirements:** A Ramp business admin (or developer/owner role) creates the client app in the Ramp developer dashboard, enables the client-credentials grant, and configures allowed read scopes. The `client_secret` is stored in our `encrypted_secrets` and referenced by `secret_ref` — same trust handoff as the Mercury API token and QuickBooks OAuth app.

---

## Backfill (historical pull)

**Supported:** Yes. All three primary list endpoints support full historical pull with pagination and filters, fully analogous to the existing Mercury/QuickBooks backfill fetchers.

**Mechanism:** Per-resource list walk. Initial FULL backfill pages through `GET /developer/v1/transactions`, `GET /developer/v1/bills`, `GET /developer/v1/reimbursements` from the beginning of the account. INCREMENTAL re-walks filtered by an `updated`/`created` high-water cursor with window overlap, relying on versioned `external_id` to dedup lifecycle transitions.

**Pagination:** Cursor/page-token based. Ramp returns a `page`/`next`-style cursor on list endpoints — exact field names UNVERIFIED. Transaction list filters by `card` and `limit_id`/fund are VERIFIED. Our cursor struct stores `(page_cursor, high_water_updated, incremental_floor)` mirroring `MercuryCursor`/`QuickBooksCursor`.

**History depth:** Expected to cover the full life of the Ramp account (all settled transactions, all bills, all reimbursements). No documented hard history cap found — UNVERIFIED; treat as account-lifetime until confirmed.

**Rate limits:** Ramp enforces API rate limits (standard for fintech REST APIs). Documented limits not captured — UNVERIFIED. Mitigate with the same backoff-and-resume pattern the Mercury fetcher uses: on 429, return the partial page + current cursor with `end_of_data=False`. Tune page size via env `RAMP_BACKFILL_PAGE_SIZE`.

**Maps to our pipeline:** Each primary resource (`ramp_transaction`, `ramp_bill`, `ramp_reimbursement`) becomes a `shard_kind`. The planner fans out one shard per resource type (or per-account/card sub-resource if Ramp exposes a `cards`/`accounts` child list analogous to `mercury_accounts`). Cursor advances follow the N1 invariant: S3-write → publish → flush → advance cursor in `workflow_states.state_data["cursor"]`. The `high_water_updated` field becomes the reconciler's warm-start reference. Bills additionally version the `external_id` by `status+sync_status` (OPEN, PAID, BILL\_SYNCED, BILL\_AND\_PAYMENT\_SYNCED are VERIFIED) so each lifecycle step lands as a new observation rather than dedup-collapsing.

---

## Live ingestion (real-time)

**Mechanism:** HMAC-signed webhooks. Ramp emits events for transactions and bills; reimbursement live events likely exist by analogy but are UNVERIFIED.

**Events:**

| Event | Status |
|---|---|
| `transactions.cleared` | VERIFIED |
| `transactions.declined` | VERIFIED |
| `transactions.synced` | VERIFIED |
| `bills.created` | VERIFIED |
| `bills.approved` | VERIFIED |
| `bills.paid` | VERIFIED |
| `bills.rejected` | VERIFIED |
| `bills.archived` | VERIFIED |
| `bills.ready_to_sync` | VERIFIED |
| `reimbursements.*` | Inferred, UNVERIFIED |

**Signature scheme:** Ramp signs webhook deliveries — most likely HMAC-SHA256 over the raw body with a per-app webhook signing secret in a Ramp signature header analogous to Mercury's `Mercury-Signature: sha256=<hex>`. Exact header name, encoding (hex vs base64), and whether a timestamp/replay envelope is included are UNVERIFIED — must confirm before coding the verifier. Both shapes are already implemented in the pipeline (Mercury hex+prefix, QuickBooks base64), so either is a drop-in.

**Degradation note:** If webhooks require a paid Ramp plan tier or are not enabled for a given org, fall back to incremental polling on the same cursor (Mercury reconciler probe pattern). Live is degradable, never a hard blocker.

**Maps to our pipeline:** Live path **(a) HMAC webhook → Kafka cutover → 202**. This is the default path for token/HMAC sources already in `_HMAC_SOURCES` (`jira`, `mercury`, `quickbooks`, `grafana`). Ramp joins this set: raw body → 1 MB precheck → `VERIFIERS["ramp"]` (signature verification, 401 before tenant enforcement) → tenant resolve → enforce tenant → if `kafka_path_enabled` → `_attempt_kafka_path` (S3 PutIfAbsent → publish `RawEnvelope(ingress_kind="webhook")` → flush → 202). Handler deduplicates against the backfill twin via the versioned `external_id`. `_EXPECTED_LIVE_STATUS["ramp"] = {202}`.

---

## Can we gather this? — feasibility

**Verdict: Yes.** As the Ramp account owner we provision one developer client app in the Ramp dashboard, grant read-only scopes (`transactions:read`, `bills:read`, `reimbursements:read` — all VERIFIED), and authenticate machine-to-machine (OAuth2 client-credentials — one org credential, no per-user consent). Backfill the list endpoints with cursor pagination; subscribe to HMAC-signed webhooks for live events (polling fallback if webhooks are unavailable).

**Access model:** Org-level single-credential (`client_id`/`client_secret` → bearer token), stored in `encrypted_secrets` via `secret_ref`. Not per-user OAuth. Closest existing analog: QuickBooks (OAuth2 app, token refresh) without `realm_id`, or Mercury (single org token) if a static-token mode is also offered.

**Legal/ToS:** Ramp's Developer API is a first-party, documented integration surface intended for the account owner to programmatically access their own org's data. Automated ingestion of our own account is squarely within the intended use. No scraping, no ToS gray area. Standard fintech API terms apply (no reselling raw cardholder data, respect rate limits).

**Compliance/PII:** Financial and PII data: cardholder names, merchant/vendor counterparties, amounts, and potentially bank/account identifiers on bills/payments. Apply the Mercury redaction precedent — mask account/routing/IBAN-style identifiers to last 4 before they reach observations/LLM context. No end-to-end encryption to contend with (server-side REST). Data is authoritative (Ramp is system-of-record) → `trust_tier=authoritative`. Tenant isolation via RLS on the new tables.

**Blockers:** No hard blockers for the owning org. Soft items: (1) webhook delivery and/or higher API rate tiers may depend on Ramp plan — degrade to polling; (2) exact OAuth token endpoint, webhook signature scheme, and pagination fields are UNVERIFIED and must be confirmed against live docs/sandbox before coding the verifier and client.

**Confidence: high.**

---

## How it maps onto our pipeline

```
SOURCE: ramp

Auth shape →            OAuth2 client-credentials (machine-to-machine), org-scoped single
                        credential (client_id + client_secret → short-lived bearer token,
                        token mint + refresh). If Ramp offers a static long-lived token,
                        degrades to API-token Bearer (Mercury shape). No realm_id.
                        token storage: secret_ref + refresh_secret_ref on ramp_installations
Install table →         ramp_installations (cols: base_url, client_id, secret_ref,
                        refresh_secret_ref, webhook_secret_ref)
                        child resource table?: ramp_accounts or ramp_entities if Ramp
                        exposes sub-accounts; otherwise single org-wide row (org-wide
                        → one install row, per-resource shard fan-out)
Backfill cursor →       dimension: page_cursor (field name UNVERIFIED) + high_water_updated
                        (incremental floor for updated/created filter)
                        high_water field: high_water_updated   incremental floor: account_created_at
                        rate-limit-safe empty page: y (return partial + cursor, end_of_data=False on 429)
                        shard_kind: "ramp_transaction" | "ramp_bill" | "ramp_reimbursement"
                        per-resource fan-out (3 shards per install)
Live mechanism →        HMAC webhook → 202 (path a; joins _HMAC_SOURCES)
                        signature: header UNVERIFIED (likely Ramp-Signature or X-Ramp-Signature)
                        format: sha256=hex (Mercury shape) OR base64 (QuickBooks shape) — UNVERIFIED
                        timestamp/replay envelope: UNVERIFIED
                        tenant identifier in payload: UNVERIFIED (need the field that maps
                        to ramp_installations.tenant_id; extractor _extract_ramp)
New files →             fetchers/ramp.py · planners/ramp.py · handlers/ramp.py
                        (channels: ramp:transaction, ramp:bill, ramp:reimbursement) ·
                        signatures/ramp.py (HMAC verifier) ·
                        _clients.py: build_ramp_client + open_ramp_client (OAuth2 token mint
                        + refresh, SYNTHETIC_SOURCE_API_BASE seam) ·
                        idempotency/__init__.py: ramp_transaction / ramp_bill / ramp_reimbursement
                        key constructors ·
                        _load_install branch in shard_fetch.py (_LOAD_RAMP_INSTALL_SQL) ·
                        router.py: add ramp to _PROVIDER_TO_SHADOW_SOURCE,
                        _CUTOVER_ENABLED_PROVIDERS, _PROVIDER_CHANNEL ·
                        tenant_resolver.py: ResolverProvider Literal + _extract_ramp +
                        PROVIDER_EXTRACTORS entry ·
                        mock client + mock server + fixtures/synthetic + planner/fetcher/
                        handler/verifier tests mirroring Mercury
Migration →             NNNN_ramp.sql: ramp_installations (tenant_id FK, base_url,
                        client_id, secret_ref, refresh_secret_ref, webhook_secret_ref,
                        UNIQUE(tenant_id, base_url)) + ENABLE/FORCE ROW LEVEL SECURITY
                        + tenant_isolation policy; optional ramp_entities child table;
                        source-registry CHECK widening on all four substrate tables
                        (source_onboarding_runs, onboarding_shards, ingestion_failures,
                        onboarding_triggers) listing every prior source + 'ramp'
Observation kind(s) →   signal: cleared transactions, OPEN/PAID bills, reimbursements
                          (submitted/approved/paid), balance/budget snapshots
                        state_change: transactions.declined (cash-risk), bills.rejected/
                          archived (payment failure/cancellation)
                        channels: "ramp:transaction", "ramp:bill", "ramp:reimbursement"
                        trust_tier: authoritative (Ramp is system-of-record)
                        external_id: VERSIONED — transaction by state (cleared/declined/synced),
                          bill by status+sync_status (OPEN, PAID, BILL_SYNCED,
                          BILL_AND_PAYMENT_SYNCED), reimbursement by status;
                          namespaced by install/org identifier to prevent cross-tenant
                          collision on the global UNIQUE (source_channel, external_id, occurred_at)
Rate-limit risk →       Medium. Fintech REST limits apply (documented limits UNVERIFIED).
                        Backfill across 3 resources for a large org is the hottest path —
                        reuse Mercury on-429 backoff; tune via RAMP_BACKFILL_PAGE_SIZE
Legal/ToS risk →        Low. First-party documented API for the account owner accessing
                        its own org data — not scraping. Standard fintech-data handling
                        obligations apply (PII/financial redaction per Mercury precedent,
                        no resale of raw cardholder data). Per-user consent: not required.
Effort →                M — mechanical reuse of the finance source contract end-to-end;
                        only above-Mercury work is OAuth2 token mint/refresh (already
                        solved for QuickBooks) and confirming three UNVERIFIED specs
                        (token endpoint, webhook signature, pagination fields). No new
                        substrate concepts.
```

**Auth archetype:** Clones **QuickBooks** (OAuth2 app, token mint + refresh, encrypted `secret_ref` + `refresh_secret_ref`) without the `realm_id` dimension, degrading to **Mercury** (single org bearer token) if Ramp offers a static long-lived token. The install row holds `base_url`, `client_id`, `secret_ref`, and `refresh_secret_ref`. The `open_ramp_client` builder in `_clients.py` must implement the `SYNTHETIC_SOURCE_API_BASE` seam (set a recognizable `spam-ramp` token and override `api_base_url` via `lib.integrations.endpoints::endpoint("ramp_api")`) so it is testable in the synthetic harness without real credentials.

**Install table and sharding:** `ramp_installations` is a dedicated per-tenant install table (not `provider_installations`), consistent with the other finance sources. The planner fans out three shards per install — `ramp_transaction`, `ramp_bill`, `ramp_reimbursement` — each with its own `workflow_states` cursor. If Ramp exposes sub-account or legal-entity resources, a `ramp_entities` child table can be added mirroring `mercury_accounts`.

**Backfill cursor:** `RampCursor` Pydantic model with `extra="forbid"`, carrying `page_cursor` (UNVERIFIED field name), `high_water_updated`, and `incremental_floor`. FULL backfill pages from account inception; INCREMENTAL re-walks from `high_water_updated` with window overlap. Bills version the `external_id` by `status+sync_status` (both VERIFIED), so the OPEN→PAID and NOT\_SYNCED→BILL\_AND\_PAYMENT\_SYNCED transitions each emit a fresh observation rather than dedup-collapsing.

**Live mechanism:** Path **(a) HMAC webhook → Kafka cutover → 202**. Add `"ramp"` to `_HMAC_SOURCES`, `_CUTOVER_ENABLED_PROVIDERS`, and `_PROVIDER_TO_SHADOW_SOURCE` in `router.py`. The `signatures/ramp.py` verifier implements the `Verifier` protocol; the exact header name and encoding (Mercury-style `sha256=<hex>` or QuickBooks-style base64) must be confirmed against Ramp docs before writing the verifier. The tenant extractor `_extract_ramp` in `tenant_resolver.py` pulls the org identifier from the webhook payload (field name UNVERIFIED — see Open Questions).

**`external_id` strategy:** Versioned by mutable status, namespaced by install-scoped org identifier (analogous to `mercury:{account}:txn:{id}:{status}` and `qbo:{realm}:bill:{id}:{sync_token}`). The `external_id` must be globally unique across tenants because the `UNIQUE (source_channel, external_id, occurred_at)` index carries no `tenant_id`. The ramp org identifier (derived from the install row) provides this namespace.

**Migration note:** Ramp is a new source and must become the newest migration. The migration SQL must list every prior source plus `"ramp"` in all four `source_check` CHECK constraints (`source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`). Any source migrated after Ramp must carry `"ramp"` forward. Beware the source-CHECK re-run landmine (noted in MEMORY): integration tests re-running an older widening migration must clean up the newest source first.

**Observation kinds:** `kind=signal` for cleared/posted transactions, OPEN/PAID bills, and submitted/approved/paid reimbursements. `kind=state_change` for `transactions.declined` and `bills.rejected`/`bills.archived` (cash-risk / control-failure / payment-failure transitions). `trust_tier=authoritative`. `entities_hint` carries `ramp_account`/`card` (budget/fund), `organization` (role=vendor/counterparty), and `user` (employee/cardholder).

**Rate-limit risk:** Medium. Backfill across three resources for a large org is the hottest path. Reuse Mercury's on-429 `return partial page + cursor, end_of_data=False` pattern; expose `RAMP_BACKFILL_PAGE_SIZE` env for tuning. Exact per-minute/per-day limits are UNVERIFIED — confirm before sizing concurrency.

**Legal risk:** Low. First-party documented API for the account owner accessing its own org data. Apply Mercury redaction precedent for PII and financial identifiers. No per-user consent required.

**Effort: M.** Entirely mechanical reuse of the finance source contract. The only work above Mercury is OAuth2 token mint/refresh (already solved for QuickBooks) and confirming three UNVERIFIED specs. No new substrate concepts required.

---

## Open questions

- **OAuth2 token endpoint and grant confirmation.** Exact URL (`/developer/v1/token` or `api.ramp.com/developer/v1/token`) and formal confirmation that Ramp supports the client-credentials grant for org-owned read access (vs. requiring authorization-code/user-consent flow). [auth grant is conservative prior knowledge; scope model is VERIFIED — confirm against live docs/sandbox before coding the token minter]
- **Webhook signature scheme.** Exact header name, hex vs base64 encoding, and whether a timestamp/replay envelope is included. [UNVERIFIED — we already have both Mercury-hex and QuickBooks-base64 shapes; pick the right one once confirmed]
- **Webhook org/tenant identifier.** How a Ramp webhook payload identifies the org/tenant (analogous to Mercury `organizationId` / QuickBooks `realmId`) for tenant resolution at ingress via `_extract_ramp`. [UNVERIFIED]
- **List-endpoint pagination contract.** Cursor/page-token field names and whether an `updated_since`/`created_since` filter exists for incremental polling. (Transaction `card`/`limit_id` filters are VERIFIED.) [UNVERIFIED]
- **Transaction webhook coverage.** Whether `transactions.cleared`/`declined`/`synced` fully covers live needs or polling is still required for some states. [partially VERIFIED — 2-1 vote on the cleared/declined/synced set]
- **Reimbursements endpoint shape and live events.** Exact list endpoint path, response field names, and whether `reimbursements.*` webhook events exist. [scope `reimbursements:read` VERIFIED; everything else inferred/UNVERIFIED]
- **Webhook and rate-limit tier dependency.** Whether webhook delivery and/or higher API rate limits require a specific Ramp plan tier, which would force the polling fallback for some customers. [UNVERIFIED]
- **Documented API rate limits.** Per-minute/per-day limits for sizing `RAMP_BACKFILL_PAGE_SIZE` and fetch concurrency. [UNVERIFIED]
- **Backfill history depth.** Whether full account-lifetime transaction/bill history is retrievable or whether a retention/window cap applies. [UNVERIFIED; treat as account-lifetime until confirmed]

---

## Sources

- `https://docs.ramp.com/developer-api/v1/api/transactions` (primary) — transactions endpoint, filter params, field names
- `https://docs.ramp.com/developer-api/v1/reference/rest/bills` (primary) — bills CRUD, status/sync\_status VERIFIED values, lifecycle events
- `https://docs.ramp.com/developer-api/v1/api/reimbursements` (primary) — reimbursements resource, scope
- `https://docs.ramp.com/developer-api/v1/guides/accounting` (primary) — accounting sync, sync\_status semantics
- `https://docs.ramp.com/llms-api.txt` (primary) — full API surface overview
- `https://support.ramp.com/hc/en-us/articles/46681939909907-Accessing-the-Developer-API` (primary) — developer app setup, scope configuration
- `https://docs.ramp.com/developer-api/v1/guides/oauth` (primary) — OAuth2 scope model, client-credentials grant reference
