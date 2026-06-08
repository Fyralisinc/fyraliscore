# Candidate ingestion sources — research

Pre-implementation research/scoping for **10 candidate ingestion sources** not yet
wired into the pipeline. Each doc answers: *what data can we fetch, how, can we
legally gather it, and how does it map onto our backfill + live pipeline?*

- **Method:** per-source fan-out web search → fetch → **3-vote adversarial
  verification** → synthesis, then mapped onto our real pipeline. Generated
  2026-06-08.
- **Grounding:** every doc maps onto the [Source Integration Contract](_integration-contract.md)
  — the code-grounded "how we wire a source" reference (auth archetypes, the
  dual-edge model, backfill/live mechanics, dedup invariants, validation gate).
- **Status:** these are **scoping docs, not built features.** Claims flagged
  *unverified/inferred* are tentative; each doc ends with open questions to resolve
  before building.
- These pages are kept on disk but `exclude_docs`-ed from the published MkDocs site
  (internal research, not architecture reference). Promote into `nav` if we decide
  to publish.

Already-built sources (for reference, **not** in this folder): GitHub, Slack,
Discord, Gmail, Google Calendar, Google Drive, Jira, Notion, Mercury, QuickBooks,
Grafana — plus Telegram in-flight.
v
## Decision matrix

| Source | Group | Clones archetype | Backfill | Live path | Can we gather? | Effort | Conf. |
|---|---|---|---|---|---|---|---|
| [Brex](brex.md) | Finance | **Mercury** (Bearer token) | cursor, per-account shard | HMAC webhook → 202 | ✅ Yes | **S–M** | high |
| [Ramp](ramp.md) | Finance | QBO / finance (OAuth2 client-creds) | cursor + time filter | HMAC webhook → 202 | ✅ Yes | M | high |
| [Gusto](gusto.md) | Payroll | **QuickBooks** (OAuth) | list + paginate | HMAC webhook → 202 | ✅ Yes (owner) | M | high |
| [Deel](deel.md) | Payroll | Mercury / QBO (org API key or OAuth) | per-resource cursor | HMAC webhook → 202 | ✅ Yes | M | high |
| [Fireflies.ai](fireflies.md) | Comms | token + HMAC webhook | cursor list | HMAC webhook → 202 + hydrate | ✅ Yes | M | high |
| [Signal](signal.md) | Comms | **Telegram** (gateway session) | linked-device sync | gateway persistent conn | ⚠️ Narrow (own/linked acct only) | M | high |
| [AWS](aws.md) | Infra | *novel* — IAM/SigV4, closest to Grafana | time-window per service/account | SQS/EventBridge **poll** | ✅ Yes (own/consenting acct) | **M–L** | high |
| [Miro](miro.md) | Design | token + HMAC webhook | opaque cursor | HMAC webhook → 202 | ✅ Yes (org app) | M | high |
| [Figma](figma.md) | Design | token + HMAC webhook | cursor | HMAC webhook → 202 (passcode-in-body verifier) | ✅ Yes (org/team) | M | high |
| [Carta](carta.md) | Cap table | **QuickBooks** (OAuth + scope-id) | OAuth list | **Poll** (no webhook) | ⚠️ Conditional (API gating) | M | medium |

*Effort: S = small (near-clone of an existing slice), M = medium (full source
contract, mechanical), L = large (new auth/transport primitives).*

## Cross-cutting observations

- **The finance/payroll cluster is the cheapest, highest-confidence win.** Brex,
  Ramp, Gusto, and Deel all clone the existing Mercury/QuickBooks finance archetype
  almost exactly: per-tenant token/OAuth install, cursor backfill sharded per
  resource, HMAC-webhook → Kafka 202 live edge, `authoritative` trust tier,
  versioned `external_id`. Brex is the closest to a drop-in Mercury clone.
- **Most sources fit the standard HMAC-webhook live path.** Eight of ten map onto
  path (a) (HMAC webhook → 202). The two exceptions: **AWS** (no webhook — poll via
  SQS/EventBridge, closest to Grafana's time-window backfill) and **Carta**
  (poll-only, no webhook edge).
- **Two sources are edge cases on access, not on plumbing:**
    - **Signal** — the pipeline scaffolding is a near-clone of Telegram's gateway
      session, but we can only gather traffic for an account/linked-device *we
      control*; there is no org-wide admin API. Coverage is inherently narrow.
    - **Carta** — cleanly clones the QBO OAuth archetype, but **API access is gated**
      (not self-serve for all customers) and the data is highly sensitive equity/PII.
      Feasibility is conditional on confirming our account's API entitlement.
- **`external_id` namespacing matters for every one** — the global (no-`tenant_id`)
  `observations` UNIQUE means each source must namespace its key by a per-tenant
  install identifier (Brex account, Ramp business, Gusto company, AWS account/region,
  Carta firm/issuer). See the contract §5.

## Suggested sequencing

1. **Brex** (S–M) — closest Mercury clone; fastest finance win.
2. **Ramp** + **Gusto** (M) — you flagged both as near-term ("soon" / "soon
   upgrading"); both reuse the finance/QBO archetype end-to-end.
3. **Deel** (M) — completes the finance/payroll cluster.
4. **Fireflies** (M) — clean token + HMAC-webhook source; adjacent to the
   call-transcript signal goal.
5. **Miro** / **Figma** (M) — standard HMAC-webhook sources; second-tier value.
6. **AWS** (M–L) — higher value but needs new SigV4/poll primitives; do after the
   webhook-shaped sources land.
7. **Signal** / **Carta** — gate on the open access questions first (linked-device
   coverage; Carta API entitlement) before committing build effort.
