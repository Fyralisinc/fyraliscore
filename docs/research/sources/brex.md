# Brex — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (8/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: clones Mercury archetype · can-we-gather: yes · effort S–M (card-only first cut: S).**

---

## TL;DR

Brex is a corporate card, spend-management, and banking platform used by startups and growth-stage companies to manage employee cards, vendor payments, and cash. The API exposes card transactions, cash transactions, accounts/statements, expenses, payments, and budgets via REST+cursor-paginated endpoints secured by a long-lived Bearer token (`bxt_`). We gather data by enumerating accounts as backfill shards (cursor-per-account), then subscribing to HMAC webhooks for live transaction events — identical in shape to Mercury. The main catch is the 90-day inactivity expiry on the Bearer token: without an explicit keep-alive, credentials silently die and both backfill resumes and live delivery stop.

---

## What companies use it for — and what signal lives there

Brex-enabled companies centralize all corporate spending on a single platform, giving Fyralis a high-fidelity window into operational burn and cash posture.

- **Cards — per-employee/merchant spend, policy compliance, budget burn.** Every swipe is a timestamped, merchant-attributed transaction; budget utilization telegraphs runway risk before any ERP or payroll signal does.
- **Payments — vendor graph, failures as cash-risk signals.** Outbound payment attempts and failures map directly to a `state_change` observation (initiated → failed/cleared) and expose the supplier dependency graph.
- **Cash/accounting — runway and burn-rate, GL classification.** Cash deposits and withdrawals, alongside statement snapshots, give a period-over-period burn view useful for trend reasoning and anomaly detection.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| Card transactions | Primary spend ledger (`GET /v2/transactions/card/primary`) | `id`, `amount`, `merchant`, `status`, `posted_at`, `card_id`, `budget_id` | Burn rate, merchant counterparty, budget attribution — core spend signal |
| Cash transactions | Distinct Transactions API resource (separate endpoint) | `id`, `amount`, `direction`, `posted_at`, `description` | Runway; deposits as positive cash events |
| Accounts + statements | Account enumeration + statement objects | `id`, `balance`, `currency`, `statement_period` | Cash position; account list drives shard creation |
| Card expenses + receipts | `expenses.card.readonly` scope | `id`, `amount`, `merchant`, `status`, `employee_id`, `department_id` | Employee/dept/budget attribution; policy-compliance gaps |
| Vendors + payments | Payments API | `id`, `counterparty`, `amount`, `status`, `initiated_at` | Supplier graph; payment failures = cash-risk `state_change` |
| Budgets | Budget objects | `id`, `name`, `limit`, `spent`, `period` | Spend-control signals; budget saturation early-warning |
| Accounting objects + users | *(inferred, unverified)* GL codes, employee directory | unknown | Secondary — do not implement until scope confirmed |

The final row (accounting objects + users) is **inferred and unverified**; endpoints and scopes are unknown. Defer until confirmed.

---

## API & authentication

**API style:** REST/JSON, cursor-paginated, split product APIs (Transactions, Expenses, Payments, Budgets are separate API families under `https://platform.brexapis.com`). No sandbox environment exists; the synthetic mock harness is required for local/CI testing.

**Key endpoints (VERIFIED):**
- `GET /v2/transactions/card/primary` — primary card transaction list; `cursor`+`limit` pagination; `transactions.card.readonly` scope.
- `GET /v2/transactions/cash` — cash transactions; same pagination shape.
- Accounts/statements — `accounts.card.readonly` + `statements.card.readonly`.
- Expenses — `expenses.card.readonly`.

**Key endpoints (UNVERIFIED — scopes/paths not confirmed):**
- Payments API — exact path + scope unknown.
- Budgets API — `budgets.readonly` scope named in docs but full path unconfirmed.
- Users/Accounting endpoints — inferred; treat as unknown.

**Auth mechanism:** Long-lived Bearer token with `bxt_` prefix. Tokens are minted by an org admin in the Brex dashboard, scoped to a specific set of read-only permissions. The token is passed as `Authorization: Bearer <bxt_token>` on every request.

**Scopes (VERIFIED):** `transactions.card.readonly`, `transactions.cash.readonly`, `accounts.card.readonly`, `statements.card.readonly`, `expenses.card.readonly`, `budgets.readonly`.

**Org-token vs per-user:** Org/admin-scoped — one credential per tenant install. No per-user OAuth or consent flow is required. Admin mints a read-only token once; operator pastes it into the install record (QBO connect UX pattern).

**Expiry / keep-alive (VERIFIED):** The token expires after 90 days of inactivity. There is no automatic OAuth refresh. An explicit keep-alive call (a lightweight authenticated request on a schedule) must be implemented or the credential silently dies. This is the most operationally significant difference from Mercury's token model.

**Admin requirements:** The user minting the token must have org-admin or API-admin privileges in Brex.

---

## Backfill (historical pull)

**Supported:** Yes — confirmed for card transactions; assumed equivalent for cash/expenses/payments.

**Mechanism:** Cursor pagination: each response includes a `next_cursor` opaque string; the fetcher passes `cursor=<next_cursor>&limit=<N>` on subsequent pages. This is **not** Mercury's offset+total model — the cursor is truly opaque and must be stored verbatim.

**History depth:** Uncaptured from docs; assume full account history is available from account open date. Verify before implementation.

**Rate limits:** Uncaptured. Honor `429` + `Retry-After` header. Treat as medium-low risk given the single-tenant, read-only access model (no multi-tenant fan-out at the API layer).

**Maps to our pipeline:** The opaque `next_cursor` becomes a `BrexCursor` Pydantic model stored in `workflow_states.state_data["cursor"]` — directly analogous to `MercuryCursor`. One shard per account (`shard_kind = "brex_account_txns"`): the planner reads the account list from `brex_accounts` (child table populated at install time) and emits one `Shard` per account, identical to Mercury's `mercury_accounts` fan-out. Optional additional shard kinds (`brex_cash_txns`, `brex_expenses`, `brex_payments`) can be added later without architectural change. The `posted_at` field serves as the incremental floor, encoded in the cursor and used as the `high_water_posted_at` for the reconciler warm-start. The N1 invariant applies: S3-write → publish → flush → cursor advance; a flush failure leaves the shard `in_progress` and the orphan scan resumes.

---

## Live ingestion (real-time)

**Mechanism (VERIFIED):** HMAC webhook delivery to our existing gateway edge. The provider posts to a registered webhook URL; the gateway's `_extract_brex` extracts the install identifier, `BrexVerifier` validates the signature, the router emits a `RawEnvelope(ingress_kind="webhook")` onto the `brex:transaction` Kafka topic, and responds 202. A polling fallback is available for tenants where webhook registration is not possible.

**Signature scheme (UNVERIFIED):** Assumed HMAC-SHA256 over the raw body, in the style of Mercury and GitHub (`Mercury-Signature: sha256=<hex>` or similar header). The exact header name, algorithm confirmation, and whether a timestamp field is included for replay-window protection are **not yet confirmed** — this is the top open question before implementation.

**Webhook event catalog (UNVERIFIED):** The full set of subscribable event types is not captured. At minimum, transaction-posted events are expected. Payment failure events would be high-value for `state_change` observations.

**Tenant identifier in payload (UNVERIFIED):** The field name used to route a webhook to the correct tenant install is unknown. Mercury uses `organizationId`; Brex likely uses a similar field. Must be confirmed to implement `_extract_brex` in `tenant_resolver.py`.

**Maps to our pipeline:** Live path **(a) — HMAC webhook → Kafka cutover → 202**. This is the same path as Mercury, Jira, QuickBooks, and Grafana. Brex's source-certification artifact records the expected 202 live ingress and HMAC/tamper evidence; Provider Lab coverage is derived from the canonical catalog.

---

## Can we gather this? — feasibility

**Verdict: Yes.** The access model is entirely operator-mediated: an admin mints a read-only Bearer token and pastes it into the install flow. No per-user OAuth, no consent screens, no third-party approval. The pipeline integration is well-understood — Mercury is the direct exemplar.

**Access model:** Org/admin single Bearer token per tenant. First-party access to the tenant's own financial data.

**Legal / ToS:** First-party data under the Brex Developer ToS. No web scraping, no re-export, no third-party data brokerage. Low legal risk.

**Compliance / PII:** Financial PII is present: bank account last-4, routing numbers, IBAN fragments may appear in cash/payment records. Reuse Mercury's last-4 redaction pattern. All credentials stored as encrypted `secret_ref`. Tenant-scoped RLS enforced at the DB layer. No E2E blocker identified.

**Blockers (soft):**
1. **90-day keep-alive** — the token silently expires without an authenticated call every 90 days. A scheduled lightweight ping must be implemented (not present in Mercury, which has no expiry).
2. **Webhook scheme unconfirmed** — signature header, algorithm, and replay-window details must be extracted from docs or Brex support before `BrexVerifier` can be written.
3. **Webhook event catalog not captured** — must enumerate subscribable events before handler branching logic can be written.
4. **No sandbox** — all integration testing uses the synthetic mock harness; real-API validation requires a live Brex account.

**Confidence: high** — the auth shape and backfill mechanics are fully understood (Mercury clone); the open questions are implementation details, not architectural unknowns.

---

## How it maps onto our pipeline

```
SOURCE: brex

Auth shape →            API-token Bearer (bxt_ prefix, long-lived, 90-day inactivity expiry)
                        token storage: secret_ref on brex_installations
                        webhook secret: webhook_secret_ref on brex_installations
Install table →         brex_installations (cols: tenant_id, secret_ref, webhook_secret_ref)
                        child resource table: brex_accounts (account_id, account_kind, label)
                        — one row per account; planner fans out one shard per row
Backfill cursor →       dimension: opaque cursor (next_cursor string, not offset/total)
                        model: BrexCursor { cursor: str | None, high_water_posted_at: str }
                        high_water field: high_water_posted_at
                        incremental floor: posted_at of earliest desired history
                        rate-limit-safe empty page: y (next_cursor=None → end_of_data=True)
                        shard_kind: "brex_account_txns"  (per-account fan-out)
                        optional later: "brex_cash_txns", "brex_expenses", "brex_payments"
Live mechanism →        HMAC webhook → 202 (path a; same as mercury/jira/grafana)
                        signature: header UNVERIFIED, format sha256=hex (assumed; not confirmed)
                        tenant identifier in payload: UNVERIFIED (likely organizationId or similar)
                        extractor: _extract_brex in tenant_resolver.py
New files →             fetchers/brex.py · planners/brex.py · handlers/brex.py
                        signatures/brex.py (BrexVerifier) · _clients.py build_brex_client / open_brex_client
                        idempotency: brex_transaction(account_id, txn_id, status) constructor
                        _load_install: _LOAD_BREX_INSTALL_SQL branch in shard_fetch.py
                        router maps: _PROVIDER_TO_SHADOW_SOURCE, _CUTOVER_ENABLED_PROVIDERS, _PROVIDER_CHANNEL
                        tenant_resolver: _extract_brex + ResolverProvider literal widening
Migration →             0095_brex.sql (or next available after telegram=0094):
                        brex_installations(+RLS) + brex_accounts(+RLS)
                        + source_check widening on all 4 substrate tables
                        (strict superset of all prior sources incl. telegram)
Observation kind(s) →   signal: new transaction posted (card, cash, expense)
                        state_change: payment status transition (initiated→failed/cleared),
                                      transaction status flip (pending→posted)
                        channel(s): "brex:transaction", optionally "brex:payment"
                        trust_tier: authoritative
                        external_id: versioned-by-status, namespaced by account_id
                        pattern: brex:{account_id}:txn:{txn_id}:{status}
                        (mirrors mercury:{account}:txn:{id}:{status})
Rate-limit risk →       API limits: uncaptured; assumed medium-low (single-tenant token)
                        fan-out: one API call per account per backfill page; manageable
Legal/ToS risk →        API ToS permits server-side polling — first-party data; low risk
                        token rotation: manual (90-day keep-alive required); no OAuth refresh
                        PII: financial last-4/routing; redact via Mercury pattern
                        per-user consent: not required (org-admin token)
Effort →                S–M overall; card-only first cut is S
                        S deltas: opaque cursor vs Mercury offset; 90-day keep-alive;
                                  webhook scheme confirmation
                        M deltas: multi-API fan-out (cash, expenses, payments, budgets)
```

**Auth archetype — Mercury clone.** Brex is the closest possible analogue to Mercury in the existing source set: long-lived Bearer token, org/admin-scoped, per-tenant install table with a child accounts table, cursor-paginated backfill sharded by account, HMAC webhook live edge. The implementation should start from `services/ingest/integrations/mercury/` and apply the diffs described below.

**Install table.** `brex_installations` mirrors `mercury_installations`: `tenant_id FK`, `secret_ref` (encrypted token), `webhook_secret_ref`, `enabled`. The child `brex_accounts` table mirrors `mercury_accounts`: one row per account enumerated at install time, providing the shard targets for the planner. The planner (`planners/brex.py`) reads `ctx.install["accounts"]` and emits one `Shard(shard_kind="brex_account_txns", shard_identifier={"account_id": ...})` per row — identical to `planners/mercury.py`.

**Backfill cursor.** `BrexCursor` holds the opaque `next_cursor` string plus a `high_water_posted_at` ISO timestamp. Unlike `MercuryCursor`'s `offset`+`incremental_floor`, there is no integer offset — the cursor is entirely opaque. On `next_cursor=None` from the API, the fetcher sets `end_of_data=True`. The `_fyralis_account_id` tag on each record provides the namespacing dimension for external_id construction.

**Live mechanism.** `BrexVerifier` in `signatures/brex.py` implements the `Verifier` protocol (HMAC-SHA256 over raw body — **exact header name and algorithm must be confirmed before writing**). `_extract_brex` in `tenant_resolver.py` extracts the install identifier from the webhook payload — **field name is unverified and is the top implementation blocker**. Add `"brex"` to `_HMAC_SOURCES`, `_CUTOVER_ENABLED_PROVIDERS`, and `_PROVIDER_CHANNEL` in `router.py`.

**New files:** `fetchers/brex.py`, `planners/brex.py`, `handlers/brex.py`, `signatures/brex.py`, client builder additions to `_clients.py`, `idempotency/__init__.py` extension, `_load_install` branch in `shard_fetch.py`, router/resolver wiring, `db/migrations/0095_brex.sql`.

**Migration note.** The migration number follows telegram (`0094`). The `source_check` widening on all four substrate tables (`source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, `onboarding_triggers`) must list every prior source as a strict superset — including `telegram`. The integration-test landmine applies: any test that re-runs an older widening migration must clean up `brex` from those tables first.

**Observation kinds and external_id.** Card and cash transactions are `signal` on first appearance; a status flip (pending → posted, or payment initiated → failed) is `state_change`. The versioned external_id pattern `brex:{account_id}:txn:{txn_id}:{status}` mirrors Mercury exactly and ensures a status change lands a new observation rather than silently deduplicating. The `{account_id}` namespace makes the key globally unique across tenants — satisfying the dedup-invariant requirement (the `UNIQUE (source_channel, external_id, occurred_at)` index has no `tenant_id` column).

**Rate-limit risk.** Medium-low. The token is per-tenant and read-only; Brex's documented limits are uncaptured but assumed comparable to Mercury. Honor `429` + `Retry-After`; the existing `FetchRateLimiter` in `shard_fetch.py` handles this transparently.

**Legal risk.** Low. First-party data under Brex Developer ToS; no scraping; no per-user consent required; financial PII handled by Mercury's existing redaction pattern.

**Effort.** Card-only first cut (card transactions + account enumeration + webhook live edge) is **S**: it is mechanically a Mercury clone with an opaque cursor substitution and one unconfirmed webhook detail. Expanding to the full multi-API fan-out (cash, expenses, payments, budgets) and implementing the 90-day keep-alive is **M**.

---

## Open questions

- **Webhook signature scheme** — what is the exact header name, signing algorithm, and whether a timestamp/nonce is included for replay-window protection? This is the top blocker for `BrexVerifier`.
- **Webhook event catalog** — what event types can be subscribed to? Are payment failure events available? Are there separate subscriptions per resource type?
- **Tenant identifier in webhook payload** — what field name identifies the org/install in a webhook POST body? (`organizationId`? Something else?) Required to implement `_extract_brex`.
- **Exact paths + scopes for cash, accounts, statements, expenses, payments, budgets** — only `GET /v2/transactions/card/primary` and `transactions.card.readonly` are verified; all other resource paths must be confirmed against OpenAPI specs before implementation.
- **History/backfill depth** — how far back does the Transactions API return results? Is there a hard date floor?
- **Rate limits** — requests/minute or requests/day per token? Burst limits? Confirm before fan-out design is finalized.
- **90-day keep-alive implementation** — is a lightweight authenticated endpoint (`GET /v2/user/me` or similar) suitable, and should this be a periodic reconciler task or a sidecar ping?
- **No sandbox** — confirm the synthetic mock harness is sufficient for CI; document the real-API validation path (requires a live Brex account).
- **Token rotation / revocation** — is there an operator-facing token rotation flow that needs a UI touchpoint, or is re-paste of a new token into the install record sufficient?
- **Users/employees list and Accounting objects** — inferred to exist; scopes and paths are entirely unknown. Descope from first cut; verify before adding.

---

## Sources

- <https://developer.brex.com/> (primary) — API overview, auth model, developer portal
- <https://developer.brex.com/openapi/transactions_api> (primary) — card and cash transactions endpoints, pagination, scopes
- <https://developer.brex.com/openapi/expenses_api> (primary) — expenses endpoint + `expenses.card.readonly` scope
- <https://developer.brex.com/openapi/payments_api> (primary) — payments API surface
- <https://developer.brex.com/examples/transactions_examples> (primary) — cursor pagination usage examples
- <https://developer.brex.com/guides/authentication> (primary) — Bearer token format, 90-day inactivity expiry, scope assignment
- <https://www.brex.com/support/brex-api> (secondary) — operator-facing token minting, admin requirements
