# Deel — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (6/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: clones Mercury/QuickBooks archetype · can-we-gather: yes · effort: M**

---

## TL;DR

Deel exposes a verified REST `/rest/v2` API plus a dedicated HRIS developer API covering People/Workers, Contracts, and Invoices — the three authoritative HR and financial signal types for an org using Deel as its global payroll or EOR platform. Access is org-scoped (one admin API key or OAuth2 grant per tenant, no per-user fan-out), making it a clean single-install source shaped almost identically to Mercury (static Bearer key) or QuickBooks (OAuth2 + refresh). Backfill is a polling fetcher with incremental `updated-since` floors, one shard per resource type; live ingestion targets HMAC webhooks to Kafka cutover (path a), though the signature scheme and pagination contract are unverified and must be confirmed before implementation. The main catches are mandatory PII and compensation redaction before any reasoning/LLM path, and several critical API details (pagination, full scope list, webhook header) that are not yet confirmed from primary docs.

---

## What companies use it for — and what signal lives there

Deel is widely used as a global payroll, EOR (Employer of Record), and contractor management platform. Companies run their entire international headcount through it, making it one of the highest-fidelity sources of HR and financial ground truth available from a single API.

- **EOR/payroll for international hires** — headcount roster + geo distribution; hire and termination events as `state_change` signals (growth and attrition).
- **Pay contractors via Deel invoices** — payroll outflow amounts; invoice status transitions as cash-commitment and liquidity signals; burn-rate trend input.
- **Manage compliance + contract lifecycle** — contract state changes, compensation figures, and jurisdiction codes as compliance footprint and engagement-risk signals.
- **Reconcile vs Mercury + QuickBooks** — Deel payroll commitments cross-reference Mercury bank outflows and QBO general ledger for burn/runway modelling.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| People / Workers (HRIS) | Worker records by internal Deel ID or external ref (VERIFIED) | internal id, external_ref, status, geo | Headcount; hire/termination = growth/attrition; external-ref joins to graph actors |
| Contracts | Active and historical contractor/employee contracts — **field set REFUTED, full schema uncertain** (VERIFIED endpoint, uncertain fields) | id, state, country_code, compensation (unverified), start/end dates | Lifecycle `state_change`; comp = burn-rate input; `country_code` = compliance footprint |
| Invoices | Payroll and contractor invoices under `accounting:read` scope (VERIFIED) | id, status, amount, currency, issued_at, paid_at | Payroll outflow; status transitions (issued/paid/overdue) = cash-commitment and liquidity |
| Payments / Payslips | Payroll runs and individual payouts — **PRIOR-KNOWLEDGE UNVERIFIED** | run_id, payout_id, amount, paid_at | Realized cash-out; burn-rate and runway | 

> **NOTE:** The Contracts field set was explicitly refuted during verification — treat any assumed field names as provisional until confirmed from primary Deel documentation. Payments/Payslips were not confirmed from a primary source.

---

## API & authentication

**API style:** REST JSON over `/rest/v2` (VERIFIED) plus the HRIS developer API at `developer.deel.com` (VERIFIED). No GraphQL or streaming transport.

**Key endpoints (as verified):**

| Endpoint | Status |
|---|---|
| `HRIS People CRUD` (by internal Deel ID or external ref) | VERIFIED |
| `POST /rest/v2/contracts` | VERIFIED (field set uncertain — see above) |
| `GET /rest/v2/invoices` (requires `accounting:read` scope) | VERIFIED |

Pagination contract (offset/limit vs cursor, `updated-since` floor syntax) is **UNVERIFIED**. Rate-limit ceilings and bulk/export endpoints are **UNVERIFIED**.

**Authentication mechanism:**

Two auth shapes are both evidenced; the production choice must be confirmed:

- **Static API-key Bearer** (`Authorization: Bearer $DEEL_API_KEY`) — VERIFIED. Shaped identically to Mercury. Simpler rotation story; no refresh cycle.
- **OAuth2 with `accounting:read` scope** — VERIFIED for invoices. Shaped identically to QuickBooks. Required for the invoices endpoint; may or may not cover HRIS/contracts.

**Full scope list is UNVERIFIED** — only `accounting:read` is confirmed. Scopes required for People/HRIS and Contracts endpoints are unknown.

**Org vs per-user:** Org/admin single credential covering the whole account. One install per tenant; no per-user fan-out. The org admin or owner must mint the API key or authorize the OAuth grant.

**Admin requirements:** Org admin/owner access to create a key or grant OAuth; not a delegated user-level credential.

---

## Backfill (historical pull)

**Supported:** Yes. Verified REST list/retrieve endpoints are accessible with the org token and cover the three primary resource types.

**Mechanism:** Polling fetcher per resource, performing a full paginated walk on first run then switching to incremental `updated-since` floor (same model as QuickBooks and Mercury). One shard per resource type.

**Pagination:** **UNVERIFIED.** Assumed to be offset/limit or cursor-based; specific query parameters and response envelope are not confirmed. The cursor model should carry `high_water_updated` + an `incremental_floor` date, returning `end_of_data=False` with unadvanced cursor on 429.

**Rate limits:** **UNVERIFIED.** Per-token 429 assumed. No confirmed ceiling or burst allowance. Volume for payroll sources is bounded well below chat/messaging volumes.

**History depth:** UNVERIFIED. No confirmed retention window for historical records.

**Maps to our pipeline:**

Three shard kinds: `deel_people`, `deel_contracts`, `deel_invoices` — the same entity-shard fan-out pattern used by QuickBooks (`qbo_customer`, `qbo_invoice`, etc.). The planner reads the install row and emits one `Shard` per resource kind (`shard_kind="deel_<resource>"`). The fetcher owns a `DeelCursor` Pydantic model (`extra="forbid"`) carrying `offset_or_page_token`, `high_water_updated`, and `incremental_floor`. On the first call the cursor seeds from a shard hint; on 429 the cursor is returned unadvanced with `end_of_data=False` per the N1 invariant. Each emitted record is tagged `_fyralis_record_type` and `_fyralis_org_id` (the Deel org ID) for external_id namespacing and dedup. The `_LOAD_DEEL_INSTALL_SQL` branch in `shard_fetch.py` is mandatory — without it shards park forever.

---

## Live ingestion (real-time)

**Mechanism:** HMAC webhook delivery to the router; 202 Kafka cutover (contract path a).

**Events (documented, verification status not specified in profile):**

- `worker.created`, `worker.updated`, `worker.terminated`
- `contract.signed`, `contract.terminated`
- `invoice.issued`, `invoice.paid`, `invoice.overdue`
- `payment.failed`

**Signature scheme:** **UNVERIFIED.** Assumed HMAC-SHA256; header name and format (whether `sha256=<hex>` Mercury-style or bare lowercase hex Grafana-style) must be confirmed from Deel webhook documentation before the `signatures/deel.py` verifier can be written.

**Tenant resolution:** The Deel org ID must be present in each webhook payload for `_extract_deel` to resolve the install. Whether Deel includes this field (and under what key) is **UNVERIFIED** and is a hard prerequisite for live ingestion.

**Fallback:** If webhooks are unavailable or signature scheme cannot be verified, fall back to poll-only with dedup via versioned `external_id` (no live path wired). If webhook payloads are thin (entity id + type only, Notion-style), shadow-write pattern applies: fetch full object via REST on receipt.

**Maps to our pipeline:**

Path **(a) HMAC webhook → Kafka cutover → 202.** Add `deel` to `_HMAC_SOURCES`, `_CUTOVER_ENABLED_PROVIDERS`, and `_PROVIDER_CHANNEL` in `router.py`. The `signatures/deel.py` verifier implements `async def verify(*, body, headers, secrets, now) -> VerifiedContext`, registered in `signatures/__init__.py::VERIFIERS`. The `_extract_deel` extractor in `tenant_resolver.py` pulls the Deel org ID from the payload and maps `(provider="deel", installation_id=<org_id>)` → tenant via `deel_installations`. `_EXPECTED_LIVE_STATUS["deel"] = {202}` in the validation harness.

---

## Can we gather this? — feasibility

**Verdict: yes** (high confidence, with soft blockers).

**Access model:** Org/admin token; static API-key Bearer (VERIFIED) or OAuth2+refresh (VERIFIED for `accounting:read`). Single install per tenant; no per-user OAuth; no bot approval or marketplace listing required for private org token.

**Legal / ToS:** Polling with the org's own credential against documented public API endpoints is sanctioned use. Rate-limit details, token-rotation requirements, and whether OAuth vs API-key is required for production use are **UNVERIFIED** and should be confirmed before launch.

**Compliance / PII:** **HIGH risk.** Worker records contain personal data (names, addresses, employment status, compensation). Invoice and contract data contains financial figures tied to identifiable individuals. PII and compensation fields must be redacted before any reasoning or LLM path — reuse the Mercury `_redact_routing` pattern. RLS tenant isolation is mandatory on `deel_installations`. GDPR and data-residency obligations likely apply; right-to-erasure handling should be scoped before GA.

**Hard blockers:** None. The API is publicly documented and accessible via org token.

**Soft blockers (resolve before implementation):**

1. Webhook signature scheme and header format (UNVERIFIED) — required for `signatures/deel.py`.
2. Pagination contract (UNVERIFIED) — required for correct `DeelCursor` implementation.
3. Deel org ID in webhook payloads (UNVERIFIED) — required for `_extract_deel`.
4. Contract field set (REFUTED during verification) — reconfirm from primary docs.
5. Full OAuth scope list (UNVERIFIED) — determines auth shape (key vs OAuth2).
6. Legal sign-off on PII retention depth and GDPR erasure handling.

**Confidence: high** — all primary endpoints verified; unverified items are implementation details, not go/no-go questions.

---

## How it maps onto our pipeline

```
SOURCE: deel

Auth shape →            API-token Bearer (Mercury-shaped) OR OAuth2+refresh (QBO-shaped)
                        UNVERIFIED which is required for all scopes in production
                        token storage: secret_ref (+ optional refresh_secret_ref) on deel_installations
Install table →         deel_installations (cols: tenant_id, base_url, secret_ref,
                        refresh_secret_ref? [if OAuth], webhook_secret_ref, deel_org_id,
                        UNIQUE(tenant_id, deel_org_id))
                        child resource table: none (org-wide, resource shards from planner)
Backfill cursor →       dimension: offset-or-page-token (UNVERIFIED) + updated-since floor
                        high_water field: high_water_updated   incremental floor: incremental_floor
                        rate-limit-safe empty page: y (unadvanced cursor + end_of_data=False on 429)
                        shard_kind: "deel_people" | "deel_contracts" | "deel_invoices"
                        per-resource fan-out (3 shards per install)
Live mechanism →        HMAC webhook → 202 Kafka cutover (path a)
                        signature: header UNVERIFIED  format [sha256=hex | bare hex — UNVERIFIED]
                        tenant identifier in payload: deel_org_id (field name UNVERIFIED)
                        extractor: _extract_deel
New files →             fetchers/deel.py · planners/deel.py · handlers/deel.py ·
                        signatures/deel.py · _clients.py build_deel_client/open_deel_client ·
                        idempotency constructor (deel_person, deel_contract, deel_invoice) ·
                        _LOAD_DEEL_INSTALL_SQL + _load_install branch in shard_fetch.py ·
                        router maps (_HMAC_SOURCES, _CUTOVER_ENABLED_PROVIDERS, _PROVIDER_CHANNEL) ·
                        tenant_resolver (_extract_deel, ResolverProvider Literal)
Migration →             0095_deel.sql (after 0094_telegram):
                        deel_installations(tenant_id FK, base_url, secret_ref,
                        refresh_secret_ref?, webhook_secret_ref, deel_org_id,
                        UNIQUE(tenant_id, deel_org_id)) + ENABLE/FORCE RLS tenant_isolation +
                        source_check widening on 4 tables (superset including telegram)
Observation kind(s) →   signal: worker/contract/invoice creates and snapshots
                        state_change: worker status transitions (hire/termination),
                          contract state transitions (signed/terminated),
                          invoice status transitions (issued/paid/overdue/failed)
                        channel(s): "deel:people" | "deel:contract" | "deel:invoice"
                        trust_tier: authoritative
                        external_id: VERSIONED, namespaced by deel_org_id
                          deel:{org_id}:person:{id}:{status}
                          deel:{org_id}:contract:{id}:{state}
                          deel:{org_id}:invoice:{id}:{status}
Rate-limit risk →       Medium-unknown; per-token 429 ceiling unverified; volume bounded
                        well below chat sources; manageable with 429 handling + Redis FetchRateLimiter
Legal/ToS risk →        Medium; org polling own credential = sanctioned; HIGH PII+financial
                        (worker names/comp/status + invoice amounts); mandatory redaction before
                        LLM; GDPR/data-residency/erasure apply; token-rotation/OAuth-vs-key unverified
Effort →                M: Mercury/QBO templates mechanical (fetcher/planner/handler/migration near
                        copy-paste); above S for 3 resource types + versioned external_id +
                        mandatory PII redaction + 4 unverified items requiring primary-doc
                        confirmation before code; not L (no novel transport, no per-user fan-out,
                        no multi-tenant child table fan-out)
```

**Auth archetype — Mercury or QuickBooks clone.** If only an API key is needed (static Bearer), this is a near-exact Mercury clone: `build_deel_client(install, *, pool)` reads `secret_ref` from `deel_installations` and constructs a Bearer-auth HTTP client. Provider Lab mode uses `PROVIDER_LAB_URL` for the recognizable `spam-deel` credential and an explicit `DEEL_API_BASE_URL` for routing. If `accounting:read` requires OAuth2 with a refresh token, the auth shape pivots to QuickBooks: `secret_ref` + `refresh_secret_ref` with token-refresh logic before expiry. This decision is gated on confirming the full required scope list.

**Install table.** `deel_installations` is a new dedicated table (not `provider_installations`), keyed by `(tenant_id, deel_org_id)` with a UNIQUE constraint. The `deel_org_id` column doubles as the webhook tenant identifier extracted by `_extract_deel`. RLS `tenant_isolation` policy on `current_setting('app.current_tenant')::uuid` is mandatory given the PII content.

**Backfill cursor.** The planner emits three shards (`deel_people`, `deel_contracts`, `deel_invoices`) from a single install. The fetcher dispatches on `shard_kind`, initializes `DeelCursor(offset_or_page_token=0, high_water_updated=None, incremental_floor=<floor>)` on first call, pages until `end_of_data=True`, and emits records tagged `_fyralis_record_type` + `_fyralis_org_id=<deel_org_id>` for namespacing. The N1 invariant applies: publish all records, flush, advance cursor only on `flush==0`.

**Live mechanism.** Path (a). The `signatures/deel.py` verifier must confirm the HMAC header name and format before it can be written — this is the single hardest prerequisite. Once confirmed, wiring is identical to Mercury: raw body hash comparison, 401 before tenant enforcement, 202 Kafka cutover via `_attempt_kafka_path`. The handler at `handlers/deel.py` registers `@register("deel:people")`, `@register("deel:contract")`, `@register("deel:invoice")` and branches on `_fyralis_record_type` (backfill path) vs raw webhook body shape (live path).

**external_id strategy.** Versioned, namespaced by `deel_org_id`. Status/state is encoded in the key so a real transition (invoice `issued`→`paid`) lands a new observation rather than collapsing into the original. Because the `UNIQUE (source_channel, external_id, occurred_at)` index has no `tenant_id`, the `deel_org_id` namespace is the only cross-tenant collision guard — it is non-optional.

**PII redaction.** Worker PII (name, address, employment details) and compensation figures must be stripped before the record reaches any reasoning or LLM worker. Reuse and extend the Mercury `_redact_routing` pattern. The redaction boundary should be the handler output — `content["_raw"]` may retain full payload for audit, but `content_text` and top-level `content` fields must not expose PII.

**Migration note.** `0095_deel.sql` lands after `0094_telegram`. The source-check widening on `source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, and `onboarding_triggers` must be a strict superset of the telegram widening (include all 12 prior sources + `deel`). The standard landmine applies: integration tests re-running an older widening migration must clean up `deel` rows first.

**Validation harness additions.** `_EXPECTED["deel"]`, `_scen_params("deel", slug)` with `deel_org_id` embedded in fixture params, `_EXPECTED_LIVE_STATUS["deel"] = {202}`, a `deel` branch in `_dispatch_one`, and a `preflight.py` assertion that `occurred_at` lands within the live partition window.

---

## Open questions

- **Webhook signature scheme** — which header carries the HMAC, and is the format `sha256=<hex>` (Mercury-style) or bare lowercase hex (Grafana-style)? What is the replay-window tolerance?
- **Pagination contract** — offset/limit or cursor token? What query parameter drives `updated-since` incremental floor? What does an empty page look like?
- **Full OAuth scope list** — is `accounting:read` sufficient for invoices only, or are additional scopes required for HRIS People and Contracts? Does production use require OAuth2 or is a static API key acceptable?
- **Webhook payload shape** — are payloads full entity objects (inline-verify sufficient) or thin identifiers (shadow-write required)? Does every payload include the Deel org ID, and under what key?
- **Contract field set** — the verification round refuted the assumed field set; must be reconfirmed from primary Deel contractor API documentation before the handler can be written.
- **Rate-limit ceilings + bulk/export** — are there burst limits, daily quotas, or bulk endpoints that affect large-org backfill strategy?
- **Deel org/tenant ID in webhook payloads** — confirm the field name carrying the org ID needed by `_extract_deel` to resolve the install.
- **PII/data-residency + retention** — GDPR right-to-erasure scope (which fields, which tables); data-residency requirements for EU-domiciled orgs; redaction depth before LLM (field-level vs record-level exclusion).

---

## Sources

- <https://developer.deel.com/api/hris/introduction> (primary) — HRIS People CRUD, worker records by internal ID and external ref
- <https://developer.deel.com/api/contractors/introduction> (primary) — contractor API introduction
- <https://developer.deel.com/api/reference/endpoints/accounting/retrieve-invoices> (primary) — `GET /rest/v2/invoices`, `accounting:read` scope confirmation
- <https://developer.deel.com/api/reference/endpoints/legal-entities/get-legal-entity> (primary) — legal entity endpoints
- <https://developer.deel.com/docs/deel-it-api> (primary) — Deel IT/integration API overview
- <https://developer.deel.com/api/authentication> (primary) — API-key Bearer auth, OAuth2 mechanism
- <https://www.getknit.dev/blog/deel-api-directory-NAWaPL> (blog) — third-party API directory summary; not a primary source, used for triangulation only
