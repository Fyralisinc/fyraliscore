# Gusto — ingestion source research

> **Status:** Pre-implementation research/scoping — NOT built. Grounded in the [Source Integration Contract](_integration-contract.md). Web-researched + adversarially verified (8/8 claims survived 3-vote verification). Date: 2026-06-08.

**Verdict: clones the QuickBooks (QBO) OAuth2+realm archetype · can-we-gather: Yes (for a company that owns its Gusto account) · effort: M (Medium).**

---

## TL;DR

Gusto is a payroll, benefits, and HR platform that exposes its full resource hierarchy (Companies → Employees → Jobs → Compensations, plus Payrolls, Company Benefits, Contractors, Bank Accounts, Locations, Departments) via a clean versioned REST API (Embedded Payroll API, `/v1`). Access uses OAuth 2.0 Bearer with a rotating refresh token scoped to a `company_uuid` — a near-exact structural match to our existing QuickBooks source (`realm_id` → `company_uuid`, `SyncToken` versioning → `effective_date` versioning). Live updates arrive as HMAC-signed webhooks organized into resource-scoped event categories (Employee, Payroll, Contractor, Company, Company Benefit, Bank Account, Location Events), which map directly onto our HMAC webhook → Kafka 202 live path. The principal catch is PII sensitivity: Gusto is the heaviest-PII source we would add (SSNs, bank/routing numbers, individual salaries), mandating a careful field-minimization design and security/legal sign-off before any build begins.

---

## What companies use it for — and what signal lives there

Gusto is the payroll and HR system of record for a large share of US startups and SMBs. For ingestion purposes it is the single source of truth for headcount, compensation structure, labor burn, and benefits cost — company-intelligence signals that are otherwise scattered across finance and people systems.

- **~80-FTE semi-monthly payroll + benefits (health, 401k) administered in Gusto.** Finance/People Ops admin owns the OAuth grant. We capture per-run gross payroll totals and cadence (labor burn over time), benefit fixed costs, and the full active-employee roster by department — the canonical headcount + labor-cost baseline.

- **Fast-scaling startup: frequent hires, terminations, comp adjustments post-funding.** HR/founder is the account admin. Employee Events webhooks fire on each hire/termination; comp changes arrive as new compensation records. We capture hire velocity and net headcount delta (growth signal), termination clusters (restructuring/risk signal), and raise/promotion events via `most-recent-effective_date` compensations (compensation-trajectory + retention-investment signal).

- **Company shifting between contractors and FTEs as it matures.** Ops/finance admin. Contractor Events + contractor-payment records alongside employee hires. We capture the FTE-vs-contractor mix and contractor spend trend — a strategy/maturity + cost-flexibility signal, plus recurring contractor/vendor relationships from payment patterns.

- **Company opening a second office or expanding to a new state.** People Ops admin. Location Events + new Departments + new-state employee tax setup. We capture geographic expansion and org-design changes (new location, new department) — an expansion/scale signal that corroborates headcount growth.

---

## Data we can fetch

| Entity | What it is | Key fields | Signal value |
|---|---|---|---|
| Company | VERIFIED: the Companies endpoint root, with sub-resources Locations, Bank accounts, Company Benefits, Departments. | `company_uuid`, `name`, `locations[]`, `bank_accounts[]`, `departments[]` | Org-scope anchor (our `realm_id` analog). Locations/departments give org-structure + geographic footprint; bank accounts hint at funding/cash-out rails. |
| Employee | VERIFIED: `GET /v1/employees/{employee_id}` (UUID); scope `employees:read`. Each employee belongs to the company and carries jobs. | `employee_id` (UUID), `full_name`, `department`, `jobs[]`, `onboarded`, `terminated`, `has_ssn` **(PII — minimize)** | Headcount is the strongest company-intelligence signal: hires = growth, terminations/attrition = risk/restructuring, department distribution = where the org is investing. |
| Compensation | VERIFIED: `GET /v1/compensations/{compensation_id}`; scope `compensations:read`. A job has many compensations but only one active — the one with the most recent `effective_date`. | `compensation_id` (UUID), `job_uuid`, `rate`, `payment_unit` (hourly/salary), `flsa_status`, `effective_date` | Comp changes are a premium signal: raises/promotions, total payroll cost trajectory, salary-band shifts. `effective_date` is the natural version key for our mutable-source dedup. |
| Payroll (Payroll Events) | VERIFIED (via webhook category): a payroll-run resource with totals, pay periods, check dates. Exact REST path to confirm (see Open Questions). | `payroll_uuid`, `pay_period` (start/end), `check_date`, gross pay total, `processed` status, `off_cycle` flag | Payroll run amounts + cadence = direct cash-burn / labor-cost-velocity signal; off-cycle runs (bonuses/corrections) are anomaly-worthy. |
| Contractor & Contractor Payments | VERIFIED (via webhook category): Gusto handles 1099 contractor onboarding and payments. Exact REST paths to confirm. | `contractor_uuid`, `type` (individual/business), `wage_type`, payment amounts, payment dates | Contractor spend is the variable-labor signal: ramp-up of contractors vs FTEs signals scaling strategy / cost flexibility. |
| Company Benefit | VERIFIED: `POST/GET /v1/companies/{company_uuid}/company_benefits`; webhook category Company Benefit Events. | `company_benefit_uuid`, `benefit_type`, `active`, `description`, employee enrollments | Benefits richness = culture/maturity + fixed-cost signal; benefit additions/changes are org-investment signals; enrollment counts cross-check headcount. |
| Bank Account / Location / Department | VERIFIED (sub-resources of Company with their own webhook categories). Bank account numbers **must not be persisted** (PII). | `bank_account_uuid` **(PII — minimize)**, `location_uuid` + address, `department_uuid` + name | Locations = geographic expansion signal; departments = org-design signal; bank-account changes = treasury/funding-rail changes (handle as metadata only). |

---

## API & authentication

**API style:** Versioned `/v1` REST, JSON over HTTPS. Resource-nested paths: `companies → employees → jobs → compensations`; plus `company_benefits`, `bank_accounts`, `locations`, `departments`, `payrolls`, `contractors`, `contractor_payments`.

**Key endpoints:**

| Endpoint | Status |
|---|---|
| `GET /v1/companies/{company_uuid}` + sub-resources (Locations, Bank accounts, Departments) | VERIFIED |
| `GET /v1/employees/{employee_id}` | VERIFIED |
| `GET /v1/compensations/{compensation_id}` | VERIFIED |
| `GET /v1/companies/{company_uuid}/company_benefits` | VERIFIED |
| Payroll and contractor-payment REST paths | UNVERIFIED — inferred from webhook event categories; confirm before build |
| Production API host `api.gusto.com` | UNVERIFIED — demo host `api.gusto-demo.com` is VERIFIED; production host is prior knowledge, confirm |

**Auth mechanism:** OAuth 2.0 authorization-code with Bearer access tokens and a rotating refresh token. Access tokens are short-lived; refresh tokens rotate on use — same operational profile as our QBO source (short access token + rotating refresh owned by an `oauth_poller`).

**Scopes:** Granular per-resource read scopes. VERIFIED: `employees:read`, `compensations:read`. Scope names for payroll, contractor, contractor-payment, company-benefit, bank-account, and location reads are **unverified** — must confirm before build (see Open Questions). Always request least-privilege read-only scopes; never request write scopes for an intelligence pipeline.

**Org-token vs per-user:** Org/company-scoped, not per-end-user. One OAuth grant (company admin or partner/embedded server-to-server credential) yields a token that reads all company data scoped to `company_uuid` (VERIFIED). This matches our admin/org-token archetype (QBO realm, Jira install) — not per-user OAuth (Slack xoxp / Google DWD).

**Admin requirements:** The OAuth grant must be authorized by a company admin (or via Gusto's embedded/partner flow if we act as an integration partner). Webhook subscriptions are configured at the app level and signed with an app verifier/signing token, resolved per-tenant from the install table (same pattern as `quickbooks_installations.webhook_secret_ref`).

---

## Backfill (historical pull)

**Supported:** Yes — paginated GET reads per resource type, identical in shape to our QBO entity-shard backfill (one shard per resource type for the company).

**Mechanism:** One shard per resource type (`employees`, `compensations`, `payrolls`, `company_benefits`, `contractors`, `contractor_payments`) scoped to `company_uuid`. Full backfill = list each resource type and walk pages; incremental poll = re-list and dedup. Compensations dedup on `most-recent effective_date` (VERIFIED) — the active compensation is the version key, mirroring QBO's `SyncToken` versioning so a raise lands as a new observation.

**Pagination:** Gusto uses page/per-page pagination (prior knowledge — exact param names and whether a server-side `updated-since`/`modified-since` filter exists for true incremental polls are unverified; see Open Questions). The cursor persists page position and a per-resource high-water timestamp (`updated_at` or `effective_date`), directly analogous to `QuickBooksCursor` (`start_position` + `high_water_updated` + `incremental_floor`).

**History depth:** Payroll and compensation history is retained for the life of the company in Gusto (multi-year). Bound by a configurable lookback window, consistent with all other sources.

**Rate limits:** Gusto enforces per-app rate limits (prior knowledge — exact RPM and 429 `Retry-After` semantics unverified; see Open Questions). Treat like QBO: on rate-limit, the fetcher returns `end_of_data=False` with the cursor unadvanced.

**Maps to our pipeline:** Each resource type maps to a distinct `shard_kind` (e.g. `gusto_employee`, `gusto_compensation`, `gusto_payroll`), one shard per resource type per company — the same `plan_shards_quickbooks` fan-out pattern. The cursor is a `GustoCursor` Pydantic model carrying `page`, `per_page`, `high_water_updated` (or `effective_date` for compensations), and `incremental_floor`, directly modeled on `QuickBooksCursor`. The N1 invariant (S3-write → publish → flush → advance) applies unchanged. Out-of-range `occurred_at` values must land inside the live `observations` partition window; `preflight.py` is the gate.

---

## Live ingestion (real-time)

**Mechanism:** HMAC-signed webhooks, organized into resource-scoped event categories (VERIFIED): Payroll Events, Employee Events, Contractor Events, Company Events, Company Benefit Events, Bank Account Events, Location Events.

**Events:**

| Category | Trigger | Signal |
|---|---|---|
| Employee Events | Hire / onboard / terminate | Headcount change (growth / risk / restructuring) |
| Payroll Events | Payroll processed / cancelled | Cash-burn / labor-cost-velocity |
| Contractor Events | Contractor onboard / payment | Variable-labor spend |
| Company Benefit Events | Benefit added / changed | Org-investment / fixed-cost |
| Bank Account Events | Bank account changed | Treasury/funding-rail change |
| Location Events | New location / location changed | Geographic expansion / org-design |

**Signature scheme:** HMAC over the raw body with an app signing/verifier token. Exact header name, HMAC algorithm, and hex-vs-base64 encoding are **unverified** (QBO uses base64 in `intuit-signature`; Mercury/GitHub use `sha256=<hex>`; Gusto's specific scheme must be confirmed — see Open Questions). Add a `GustoVerifier` mirroring `quickbooks.py`/`mercury.py` and register it in `signatures/__init__.py::VERIFIERS`.

**Notes:** Gusto webhooks require a one-time subscription-verification handshake (echo a verification token); the verifier/onboarding must handle it. Tenant resolution keys off `company_uuid` in the payload (analog of QBO `realmId`). Fall back to poll-only for any unsigned event types.

**Maps to our pipeline:** Live path **(a): HMAC webhook → Kafka cutover → 202.** Webhooks are thin-change events (resource type + UUID); the handler emits a thin-change observation and the next poll re-fetches the full body and dedups — copying the QBO `thin_change_draft` branch exactly. Add `gusto` to `_HMAC_SOURCES`, `_CUTOVER_ENABLED_PROVIDERS`, and `_PROVIDER_TO_SHADOW_SOURCE` in `router.py`. `_EXPECTED_LIVE_STATUS["gusto"] = {202}`.

---

## Can we gather this? — feasibility

**Verdict: Yes**, for a company that owns its Gusto account.

**Access model:** A single company-admin OAuth grant (or partner/embedded server-to-server credential) yields a Bearer token scoped to `company_uuid` that reads employees, compensations, payrolls, benefits, contractors, locations, and departments programmatically (VERIFIED endpoints/scopes). This is the org/admin-token access model we already support for QBO/Jira — not per-user OAuth. Live updates arrive via HMAC webhooks across the verified event categories.

**Legal/ToS:** Reading your own company's data via your own authorized OAuth app is within Gusto's intended API use (the Embedded Payroll API exists for this). If we act as an embedded/partner platform on behalf of customers, Gusto partner terms and per-customer consent apply — partner-program eligibility and data-use terms must be confirmed before building a multi-tenant deployment (see Open Questions). First-party API only; no scraping.

**Compliance/PII:** This is the highest-sensitivity source we would add. Payroll data includes SSNs/EINs, bank account and routing numbers, individual salaries, and possibly DOB/home addresses. We **must** field-minimize at the handler: store only signal-bearing fields (`department`, `comp rate`/`effective_date`, `payroll totals`, headcount deltas); **never persist SSNs or raw bank/routing numbers** — store boolean flags (`has_ssn`) or masked references. Salary data is highly confidential and must be access-controlled inside the customer org with strict RLS (using the `jira_*` tenant-isolation template) and consideration of extra at-rest controls. A PII/financial-data DPA and retention policy are required before production.

**Blockers:** No hard technical blocker. Soft blockers: (1) confirming OAuth scope names for payroll/contractor reads (only `employees:read` + `compensations:read` are VERIFIED); (2) PII-minimization design must be reviewed and approved before any data is stored; (3) if embedding on behalf of customers, Gusto partner-program approval is required.

**Legal risk:** Medium-high (sensitive financial PII), mitigated by first-party API access + aggressive field minimization + RLS.

**Confidence: high.**

---

## How it maps onto our pipeline

```
SOURCE: gusto

Auth shape →            OAuth2(+realm) — OAuth2 Bearer + rotating refresh token +
                        per-company scoping (company_uuid == QBO realm_id)
                        token storage: secret_ref + refresh_secret_ref on gusto_installations

Install table →         gusto_installations
                          cols: id, tenant_id, company_uuid (UNIQUE per tenant),
                                base_url (api.gusto.com), secret_ref, refresh_secret_ref,
                                token_expires_at, webhook_secret_ref
                        child resource table: none (all shards keyed by company_uuid directly)

Backfill cursor →       dimension: page/per_page position + high_water (updated_at |
                                   effective_date for compensations) + incremental_floor
                        high_water field: updated_at (employees/payrolls/benefits/contractors);
                                          effective_date (compensations)
                        incremental floor: configurable lookback (same as QBO)
                        rate-limit-safe empty page: y (return end_of_data=False, cursor unadvanced)
                        shard_kinds: "gusto_employee" | "gusto_compensation" | "gusto_payroll" |
                                     "gusto_company_benefit" | "gusto_contractor" |
                                     "gusto_contractor_payment"
                        per-resource fan-out (one shard per resource type per company)

Live mechanism →        HMAC webhook → Kafka cutover → 202
                        (path (a) in the contract; thin-change webhooks → poll re-fetch → dedup)
                        signature: header [UNVERIFIED — confirm name + algorithm + hex-vs-base64];
                                   HMAC over raw body with app signing token
                        tenant identifier in payload: company_uuid
                                   (extractor _extract_gusto in tenant_resolver.py)
                        subscription-verification handshake required at onboarding

New files →             fetchers/gusto.py (FETCHER_DISPATCH['gusto'])
                        planners/gusto.py (PLANNER_DISPATCH['gusto'], one shard per resource type)
                        handlers/gusto.py (channel 'gusto:object'; @register; branch
                                           backfill-record vs webhook-thin-change; PII minimization)
                        signatures/gusto.py (GustoVerifier + registry entry in signatures/__init__.py)
                        fetchers/_clients.py — build_gusto_client + open_gusto_client
                        integrations/gusto/client.py
                        idempotency/__init__.py — gusto_entity / gusto_change constructors
                        workflows/shard_fetch.py — _LOAD_GUSTO_INSTALL_SQL + _load_install branch
                        webhooks/router.py — _PROVIDER_TO_SHADOW_SOURCE, _CUTOVER_ENABLED_PROVIDERS,
                                            _PROVIDER_CHANNEL
                        webhooks/tenant_resolver.py — _extract_gusto + PROVIDER_EXTRACTORS entry
                        synthetic mock_client / mock_server / fixtures / tests
                        docs/architecture/ingest.md update + new ADR

Migration →             NNNN_gusto.sql:
                          gusto_installations (+ RLS jira_* tenant_isolation template)
                          source_check widening on all 4 substrate tables
                          (source_onboarding_runs, onboarding_shards, ingestion_failures,
                           onboarding_triggers) carrying EVERY prior source forward + 'gusto'

Observation kind(s) →   signal: employee record, payroll run totals, compensation record,
                                 contractor payment, benefit enrollment, location/department
                        state_change: employee termination, payroll cancelled/off-cycle,
                                      compensation change (raise/promotion), bank-account change
                        channel(s): "gusto:object"  (single channel; object_type in content JSONB
                                    distinguishes employee/compensation/payroll/benefit/contractor)
                        trust_tier: authoritative (Gusto is the payroll system of record)
                        external_id: versioned-by-effective_date (compensations) /
                                     versioned-by-updated_at (employees, payrolls, benefits,
                                     contractors)
                        namespaced by: company_uuid (global UNIQUE index has no tenant_id —
                                       external_id MUST be namespaced to prevent cross-tenant
                                       collision; e.g. gusto:{company_uuid}:employee:{id}:{updated_at})

Rate-limit risk →       Medium — per-app limits across 6+ resource types + deep first backfill;
                        mitigate with QBO-style rate-limit-aware fetcher (preserve cursor,
                        return end_of_data=False on 429) and bounded page sizes.
                        Confirm exact RPM/429 semantics (unverified).

Legal/ToS risk →        Medium-high — most sensitive PII of any source (SSNs, bank/routing
                        numbers, salaries). Mandatory field minimization at the handler + strict
                        RLS + DPA/retention review before production.
                        First-party-API-only (no scraping) keeps ToS risk low for self-owned
                        data; embedded/partner use needs Gusto partner approval + per-customer
                        consent.

Effort →                M (Medium). Same end-to-end shape as QBO (already exists), so the
                        fetcher/handler/planner/verifier/migration scaffolding is a known
                        template. The M (not S) cost: more resource types than QBO
                        (employees + comps + payrolls + benefits + contractors + locations +
                        departments), confirming non-verified payroll/contractor scopes +
                        pagination + webhook signature scheme + subscription-verification
                        handshake, and — the biggest item — a careful, reviewed PII-minimization
                        design with extra at-rest/RLS controls.
```

**Auth archetype — clones QuickBooks.** The auth shape is a near-exact match: OAuth2 Bearer with rotating refresh token, per-company scoping via `company_uuid` (`realm_id` analog), and a `webhook_secret_ref` for HMAC verification. Reuse the `quickbooks_installations` table shape and the `oauth_poller` for refresh rotation verbatim. This is NOT the Jira/Mercury static-token archetype and NOT per-user OAuth.

**Install table.** `gusto_installations` (own table, not `provider_installations`) with `tenant_id` FK, `company_uuid` (the shard-scoping key and external-id namespace anchor), `base_url`, `secret_ref`, `refresh_secret_ref`, `token_expires_at`, and `webhook_secret_ref`. RLS uses the `jira_*` tenant-isolation template (`ENABLE ROW LEVEL SECURITY; FORCE ROW LEVEL SECURITY; CREATE POLICY tenant_isolation ...`).

**`_load_install` branch.** Add `_LOAD_GUSTO_INSTALL_SQL` and a corresponding branch in `shard_fetch.py::_load_install`. This is the easiest step to miss — a missing branch parks shards forever.

**Backfill cursor.** Planner emits one shard per resource type (6 shard kinds), mirroring `plan_shards_quickbooks`. Fetcher owns a `GustoCursor` Pydantic model (`extra="forbid"`) with `page`, `per_page`, `high_water_updated`/`high_water_effective_date`, `incremental_floor`, and `seeded`. Compensations dedup on `most-recent effective_date` (VERIFIED): a raise creates a new compensation record with a later `effective_date` — the new record gets a new `external_id` (versioned by `effective_date`) and lands as a new observation, never overwriting the prior comp record. This is the exact lesson QBO taught us with `SyncToken`.

**Live mechanism.** Path (a): HMAC webhook → Kafka cutover → 202. Thin-change webhook payloads (resource type + UUID) emit a thin-change observation with `kind='state_change'` for transitions (termination, payroll cancellation, raise) or `kind='signal'` for creations/snapshots. The handler's `thin_change_draft` branch fires a downstream incremental poll (same as QBO) that re-fetches the full body and dedups via `external_id`. Add `gusto` to `_HMAC_SOURCES` in `router.py` and in the validation harness.

**Handler + PII minimization.** `handlers/gusto.py` registers `@register("gusto:object")`, branches on `_fyralis_record_type` (backfill) vs raw webhook payload (live), and applies field minimization before constructing `ObservationDraft`: `content` carries `object_type`, `department`, `comp_rate`, `payment_unit`, `effective_date`, `payroll_totals`, headcount fields — **never SSNs, raw bank account/routing numbers, home addresses, or DOB**. Store `has_ssn=True/False` boolean rather than the SSN itself.

**`external_id` strategy.** Versioned (the mutable-source lesson). Namespace by `company_uuid` to satisfy the global UNIQUE index (no `tenant_id` column):
- Employees: `gusto:{company_uuid}:employee:{employee_id}:{updated_at}` (new record on status change)
- Compensations: `gusto:{company_uuid}:comp:{compensation_id}:{effective_date}` (new record on raise)
- Payrolls: `gusto:{company_uuid}:payroll:{payroll_uuid}:{processed_status}` (new record on state change)
- Other resources: `gusto:{company_uuid}:{resource}:{uuid}:{updated_at}`

The backfill fetcher tags records with `_fyralis_company_uuid`; the webhook handler derives `company_uuid` from the payload — both paths must produce identical `external_id` values (guarded by `test_backfill_external_id_parity.py`).

**Migration note.** The new `NNNN_gusto.sql` must DROP and re-ADD all four `source_check` constraints across `source_onboarding_runs`, `onboarding_shards`, `ingestion_failures`, and `onboarding_triggers`, carrying every prior source forward plus `'gusto'`. This is the source-CHECK re-run landmine: if any integration test re-runs an older widening migration, it must clean up `gusto` first or the constraint will reject it.

**Rate-limit risk.** Medium. Six or more resource types + a deep initial backfill for long-tenured companies. Mitigate with bounded page sizes, QBO-style `end_of_data=False` on 429, and a `FetchRateLimiter` in the fetch loop. Exact RPM ceiling and `Retry-After` semantics are unverified.

**Legal risk.** Medium-high. Highest-PII source added to date. Mitigated by first-party API access, handler-level field minimization, strict RLS, and a required DPA/retention policy review before any production deployment. Partner/embedded-platform use adds Gusto partner approval and per-customer consent requirements.

**Effort.** M (Medium). The end-to-end scaffolding is a known template from QBO. The extra cost is: more resource types, unverified scope names and pagination scheme requiring confirmation, unverified webhook signature scheme + subscription handshake, and a PII-minimization design that must pass security and legal review before the build starts.

---

## Open questions

- Exact OAuth scope names for payroll, contractor, contractor-payment, company-benefit, bank-account, and location reads — only `employees:read` and `compensations:read` are VERIFIED.
- Production API host (assumed `api.gusto.com` vs the VERIFIED demo host `api.gusto-demo.com`) and whether production requires a Gusto partner/app review before issuing tokens.
- Pagination scheme specifics: `page`/`per_page` param names, total-count headers, and whether a server-side `updated-since`/`modified-since` filter exists for true incremental polls (vs full re-list + dedup).
- Webhook signature details: exact header name, HMAC algorithm, and hex-vs-base64 encoding (QBO = base64 `intuit-signature`; Mercury = `sha256=<hex>`); and the exact subscription-verification handshake contract (token echo format + endpoint).
- Rate limits: requests-per-minute ceiling and 429 `Retry-After` semantics for backfill sizing.
- First-party app (our own Gusto account) vs embedded/partner platform on behalf of customers — determines partner-program eligibility, consent UX, and data-use terms.
- PII policy sign-off: which fields are permitted to persist (proposed: `department`, `comp rate`/`effective_date`, payroll totals, headcount deltas) and explicit prohibition of SSNs / raw bank+routing numbers / home addresses — needs security + legal review before build.
- Exact payroll and contractor-payment resource paths and response shapes (inferred from Payroll Events and Contractor Events webhook categories (VERIFIED) but REST endpoint docs not directly verified).

---

## Sources

- <https://docs.gusto.com/embedded-payroll/docs/companies-intro> (primary) — Companies endpoint, sub-resources (Locations, Bank accounts, Departments), `company_uuid` scoping.
- <https://docs.gusto.com/embedded-payroll/reference/get-v1-employees> (primary) — Employee resource, UUID keying, `employees:read` scope.
- <https://docs.gusto.com/embedded-payroll/reference/get-v1-compensations-compensation_id> (primary) — Compensation resource, `compensations:read` scope, `most-recent effective_date` dedup rule.
- <https://docs.gusto.com/embedded-payroll/docs/create-company-benefits> (primary) — Company benefit resource + POST/GET endpoints.
- <https://docs.gusto.com/embedded-payroll/reference/put-v1-companies-company_id-payrolls> (primary) — Payroll resource (partial — webhook categories used to infer event taxonomy).
- <https://docs.gusto.com/app-integrations/docs/oauth2> (primary) — OAuth 2.0 authorization-code flow, scope model, token rotation.
- <https://docs.gusto.com/embedded-payroll/docs/contractors-intro> (unreliable — fetched but zero confirmed claims; contractor REST paths remain unverified).
