# Ingestion Hardening — Phase 2 (Real-World Integrations) Report

**Date:** 2026-06-09 · **Branch:** `main` · **Scope:** close the *synthetic-fidelity
gap* — make ingestion correct against **real provider payloads**, not just the
synthetic mocks that mirror our own code.

**Method:** a real-provider **contract-test layer** + a **DOC-HUNTER** research
swarm pulling official webhook/signature/casing specs from public developer docs,
then doc-verified fixes (no schema guessing — every drift fix is backed by a
cited official source or halted for a real fixture).

---

## 1. Executive Summary

Phase 1 hardened the platform; Phase 2 attacks the gap Phase 1 surfaced: the
all-25 synthetic gate's mock clients emit field shapes that match our code, so
real-provider drift (camelCase vs snake_case, REST vs GraphQL, wrong signature
scheme, wrong pagination) is invisible to it.

**Delivered & validated this phase:**
- A strict, self-validating **contract-test framework** (`tests/contract/`) that
  loads physical real-payload JSON fixtures and asserts our verifiers /
  resolvers / handlers parse them — distinct from the synthetic mocks.
- **Six drift/auth fixes**, each doc-verified or code-grounded: Deel #4, AWS #6,
  Notion #29 (design), **Gusto #17 (full real-shape)**, **Ramp #35 (full
  real-shape)**, and **HiBob #20 (confirmed false-positive)**.
- **The three architectural integrations — now IMPLEMENTED & gate-validated**
  (were the headline "remaining" items): **R1 QuickBooks multi-tenant fan-out**,
  **R2 Figma `webhook_id` install model**, **R3 Ashby per-install-endpoint
  resolution**. Each is a coherent change across resolver / router / handler /
  generator / onboarding + a dedicated contract suite. See §4 (F7–F9).
- **DOC-HUNTER** researched 6 providers against official docs; 2 deep-dives
  (Gusto, Ramp) resolved all ambiguities from public sources.
- **The all-25 gate is `READY` ✅** — and now fires the *real* Gusto/Ramp wire
  shapes AND exercises R1/R2/R3 end-to-end (the synthetic generators + install
  seeding were corrected in lockstep). 358 observations, zero duplicates, 14/14
  tampered signatures rejected, every source overlap-tested, all subprocesses
  `rc=0`. The log shows Ashby resolving via its real per-install endpoint URL
  (`POST /webhooks/ashby/{installId} → 202`), Figma via `webhook_id`, and QBO
  fanning out through the 202 cutover.

**Remaining (scoped in §6):** per-source OAuth-refresh lifecycle
(QBO/Ramp/Gusto/Carta), confirming the Ramp `X-Ramp-Signature` encoding against a
real delivery, and the Phase-1 report-only items (Fireflies REST-vs-GraphQL,
page-1-only pagination, Gmail `history_id`, Jira non-status changelog).

---

## 2. The Contract-Test Layer (Phase-1 deliverable of this program)

`tests/contract/` — `framework.py` (strict loader; rejects un-`sanitized` or
malformed fixtures so a bad fixture fails the build), `registry.py` (the
coverage checklist mapping each contract to its Phase-1 finding), `README.md`
(capture/sanitize rules), and the `contract` pytest marker.

`pytest -m contract -rs` prints a **live `AWAITING FIXTURE` checklist** — every
outstanding provider fixture with the exact uncertainty it must resolve. Landed
contract suites: **Gusto (6), Ramp (4), HiBob (3), QuickBooks (5), Figma (5),
Ashby (6)** + framework self-tests (**37 passed / 13 skipped**), all driving the
**real production** verifier/resolver/handler against doc-sourced fixtures.

> Fixtures are marked `_meta.source: doc:<url>` when derived from official docs
> (real key names/casing, placeholder values) vs a production capture — never
> conflated.

---

## 3. DOC-HUNTER Research (official-doc verification)

| Provider | Verdict | Key finding |
|---|---|---|
| QuickBooks | clear | `eventNotifications[].realmId` + `entities[]` both **plural** (multi-tenant, multi-entity); `intuit-signature` base64 |
| Figma | clear | **No HMAC** — plaintext `body.passcode` equality; tenant = `webhook_id`, not `team_id` |
| HiBob | clear | Real V2 **does** carry numeric `companyId` in-band → **finding #20 false positive** |
| Ashby | clear | **No org id in body** — tenant by per-install endpoint URL / signing-secret; `Ashby-Signature` sha256=+hex |
| Gusto | clear (via deep-dive) | `resource_uuid` = company on **every** event; `X-Gusto-Signature` lowercase **hex**; no replay |
| Ramp | clear (via deep-dive) | Flat event, **root** `business_id`; `X-Ramp-Signature` HMAC-SHA256 (hex-vs-base64 undocumented) |

All claims carry official-doc citations (recorded in the swarm output).

---

## 4. Implemented Fixes (validated)

### F1 — Deel install crash *(CRITICAL #4)*
`db/migrations/0122_deel_contracts_metadata.sql` adds `contract_name`/`contract_type`
to `deel_contracts` — columns the **live** `finalize_install` path (OAuth finalize
+ finance connect wizard) and `finance_router` already reference, but `0098`
never created. The synthetic gate masked it (it seeds Deel via the fetcher, not
`finalize_install`). **Validated:** 2 install tests green.

### F2 — AWS fetcher credential opener *(CRITICAL #6)*
`fetchers/aws.py::_open_aws_client` hardcoded `secret_store=None`, so
`resolve_credentials` raised before the first CloudTrail call. Now delegates to
the shared `_clients.open_aws_client` (matching every other fetcher); fixes the
reconciler transitively. **Validated:** 8 tests green (incl. a no-DB test proving
the real `secret_store` reaches the client).

### F3 — Notion verification-token *(HIGH #29, design)*
Design doc (`docs/validation/notion-verification-token-secret-store-design.md`)
with a **dual-read rollout** (verifier reads store→env during cutover) so the
plaintext log is removed only *after* the replacement retrieval is live. Design
only — the naive log removal would break onboarding.

### F4 — Gusto real-shape *(CRITICAL #17, full)*
6-file coherent fix: verifier (`X-Gusto-Signature`/hex), resolver (`resource_uuid`),
handler (flat thin-notification branch), **synthetic generator → real shape**,
contract fixture + 6 tests. **Validated:** 213 webhooks+contract tests green **and
the all-25 gate (gusto ✅ [202], overlap×3).**

### F5 — Ramp real-shape *(HIGH #35, full)*
Same pattern: resolver reads **root** `business_id` (drops the QBO-clone
`eventNotifications` wrapper), handler flat-event branch (versioned by the stable
event `id`), generator → real shape, fixture + 4 contract tests. **Validated:** 78
tests + the gate (ramp ✅ [202], overlap×3). *Open:* `X-Ramp-Signature` hex-vs-base64
is undocumented upstream — generator+verifier kept in lockstep (base64); confirm
against a real delivery before production.

### F6 — HiBob *(HIGH #20, false positive)*
Official docs show real Bob V2 carries numeric `companyId` in every delivery, and
`_str_or_none` already stringifies it — the code is **correct**. Finding #20 is
rejected-on-real-docs; a 3-test contract suite locks the real shape in.

### F7 — QuickBooks multi-tenant fan-out *(CRITICAL #7, R1, full)*
A single Intuit delivery batches `eventNotifications[]`, each with its own
`realmId` (a connected company = a **different tenant**) × multiple
`entities[]`. The generic single-resolve→single-ingest tail dropped every realm
past the first and every entity past the first. The ingress now **fans out**:
`router.py::_ingest_quickbooks_fanout` splits the delivery into per-`(realmId,
entity)` units, **re-resolves each realm to its own tenant**, and processes each
unit through the same cutover-or-inline tail. `intuit-signature` is an
app-level secret, so the single up-front verification authenticates the whole
batch (spec-confirmed). Safety: a single-realm/single-entity delivery fans out
to exactly one unit and yields a **byte-identical** observation + the same 202
cutover status — so the gate is invariant. **Validated:** 5-test contract suite
(`test_quickbooks_contract.py`, 2-realm/3-entity fixture) + router 23/23 +
handler 14/14 + **the gate (quickbooks ✅ [202], 14/14)**.

### F8 — Figma `webhook_id` install model *(#C, R2, full)*
Real Figma Webhooks V2 carry a Figma-assigned **`webhook_id`** (the install
scope) and **no `team_id`** in the body, and **no stable event id**. The
pre-fix code resolved by `team_id` (absent → live resolution always failed) and
keyed the external_id on an event `id` (absent → the handler raised). The fix
keys `provider_installations` by `webhook_id` (resolver reads it; onboarding
captures it from `POST /v2/webhooks`), and the handler namespaces the
external_id by `webhook_id` with `(file_key, timestamp)` as the discriminator
(`figma:{webhook_id}:event:{file_key}:{timestamp}`). Verification stays
passcode-in-body (already correct). Generator + gate install-seeding corrected
in lockstep. **Validated:** 5-test contract suite (`test_figma_contract.py`) +
handler 10/10 + generators 40/40 + **the gate (figma ✅ [202], 14/14)**.

### F9 — Ashby per-install-endpoint resolution *(#28, R3, full)*
Real Ashby deliveries carry **no org id in the body** — the tenant is named by
the **per-install endpoint URL** (`/webhooks/ashby/{installId}`, each with its
own signing secret). The resolver now accepts the request **subpath** and, for
Ashby (`_PATH_RESOLVED_PROVIDERS`), resolves the tenant from the path segment
first (body `organizationId` is a legacy fallback). The router threads the
subpath into `resolve()`; the generator posts to the per-install endpoint. The
`Ashby-Signature` (sha256=+hex) verifier was already correct. **Validated:**
6-test contract suite (`test_ashby_contract.py`, no-org-body + path-segment
resolution end-to-end) + the 4 stub-resolver signatures updated to mirror the
new interface + **the gate (ashby ✅ [202], 16/16) with the live log showing
`POST /webhooks/ashby/ashby-org-…-0 → 202`**.

**Aggregate:** the all-25 gate is `READY` (run6, R1/R2/R3 + real Gusto/Ramp
shapes), plus the Phase-1 277-test regression, the contract suite
(**37 passed / 13 skipped**), and the handler/verifier/resolver/router suites —
all green.

---

## 5. Documentation vs Implementation Discrepancies (new)

Every Phase-2 fix corrected a code/real-world discrepancy the synthetic gate hid:
Deel migration columns; AWS credential wiring; Gusto/Ramp QBO-clone webhook
shapes + wrong signature schemes; and two findings that were actually *false
positives* against real docs (HiBob `companyId`; Figma "no HMAC" is by design).

---

## 6. Remaining Risks (the three architectural integrations are now DONE — §4 F7–F9)

The three architectural integrations that headlined this section in the prior
revision (QuickBooks fan-out, Figma `webhook_id`, Ashby per-endpoint) are
**implemented and gate-validated** — see §4 F7–F9. What remains:

### Open — per-source OAuth-refresh lifecycle *(#24/#26/#38/#40)*
QBO / Ramp / Gusto / Carta have **no refresh-token exchange** implemented, so a
long-lived poll install stops fetching once the access token expires. The
contract registry has the four `oauth_token` fixtures outstanding
(`pytest -m contract -rs` lists them). This is the **main remaining blocker for
full PRODUCTION_READY** — it is auth-lifecycle work, not a shape fix, and is
invisible to the gate (which seeds warm credentials).

### Open — Ramp signature encoding (hex vs base64)
`X-Ramp-Signature` is HMAC-SHA256 over the raw body, but the hex-vs-base64
encoding is **undocumented upstream**. Generator + verifier are kept in lockstep
(base64); confirm against a real Ramp delivery before production (see F5).

### Report-only (from Phase 1, unchanged)
Fireflies REST-vs-GraphQL client; Brex/Miro/Notion page-1-only pagination; Gmail
`history_id` overwrite; Jira non-status changelog drop. Each carries file:line
evidence + remediation in the Phase-1 report.

---

## 7. Production Readiness (updated from Phase 1's 72/100)

| Dimension | Phase 1 | 79-rev | Now | Why |
|---|:---:|:---:|:---:|---|
| Security | 7 | 8 | 9 | Gusto/Ramp signature schemes corrected & gate-verified; **Ashby per-endpoint-URL tenant binding (R3)** removes the body-trust stand-in; HiBob/Figma confirmed; secret rotation (P1) |
| Data integrity | 7 | 8 | 9 | **QBO multi-tenant fan-out (R1)** + **Figma webhook_id keying (R2)** make live tenant-resolution + dedup correct against real shapes across all webhook sources; gate zero-dup (358 obs) |
| Test coverage | 6 | 8 | 9 | Real-provider contract layer now covers QBO/Figma/Ashby/Gusto/Ramp/HiBob (37 passed); gate fires real shapes + per-install Ashby endpoint |
| Reliability | 7 | 7 | 7 | (P1 breaker/reconciler fixes); **per-source OAuth auth-refresh still open** (the main remaining blocker) |
| Scalability / Observability / Recovery | 8/7/8 | 8/7/8 | 8/7/8 | unchanged |

**Score: 88 / 100** (79-rev: 79; Phase 1: 72). **Verdict: CONDITIONALLY_READY.**

The contract layer + the corrected Gusto/Ramp/Deel/AWS paths + **the three
architectural integrations (R1 QBO fan-out, R2 Figma `webhook_id`, R3 Ashby
per-endpoint) — all now implemented and gate-validated** — close the
highest-leverage real-provider tenant-resolution and dedup gaps. The all-25 gate
is `READY` with R1/R2/R3 exercised end-to-end. Full `PRODUCTION_READY` now turns
primarily on the **per-source OAuth-refresh lifecycle** (QBO/Ramp/Gusto/Carta)
and confirming the Ramp signature encoding against a real delivery — both scoped
in §6.

*Supported by: the `READY` all-25 gate run (run6 — quickbooks ✅ [202] 14/14,
figma ✅ [202] 14/14, ashby ✅ [202] 16/16 via `/webhooks/ashby/{installId}`, 358
observations zero-dup, 14/14 tampered rejected, all 7 subprocesses rc=0), the
contract-test suites (37 passed / 13 skipped), the Phase-1 277-test regression,
and DOC-HUNTER's official-doc citations.*
