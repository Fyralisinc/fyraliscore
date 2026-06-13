# ADR-0004: Interface/extension platform — developer-hosted third-party extensions on a governed host boundary

- **Status:** Proposed <!-- Proposed | Accepted | Superseded by ADR-XXXX | Deprecated -->
- **Date:** 2026-06-13
- **Deciders:** Core / platform team <!-- TODO(human): confirm named deciders -->
- **Related:** [Interfaces & Extensions](../architecture/interfaces.md) · [ADR-0001 (Kafka-first ingestion)](0001-kafka-first-ingestion-default.md) · `CODEBASE-MANAGEMENT.md` (layering/monorepo decisions)

## Context

Several capabilities are layered on the core substrate as de-facto "interfaces":
`github_intel` + `code_intel` (since **extracted** to `Fyralisinc/github-intel` as the
first step of this plan), the finance sources, and the CEO view.
Each attaches a **different** way — an inline `if channel == "github:webhook"` hook
hardcoded in `services/ingest/ingestion/core.py`, a source-handler registry, and a
direct-import gateway scheduler — and two of the three make the **core import the
interface**, the exact coupling the existing `company_os.*` entry-point seams were
built to avoid.

The forcing decision: the team wants to **invite external developers to build
extensions on fyraliscore** as a product. That raises requirements the current ad-hoc
wiring cannot meet — a stable contract external code can bind to, a way for untrusted
parties to read/write tenant data safely, per-tenant consent, and a marketplace.

Constraints shaping the decision: a **small** engineering team (no dedicated platform
org yet); **sensitive multi-tenant data** (employee communications + finance); a
target of a **curated, paid marketplace** plus **private per-tenant** extensions; and
external research (VS Code, Backstage) showing the same convergent pattern — a host
owning the substrate and exposing it only through a manifest of capability-scoped
extension points, a version-pinned API hiding internals, lazy activation, and
ownership-scoped data access, with isolation strength calibrated to blast-radius.

## Decision

We will introduce a **unified interface/extension layer**: the core stops importing
interfaces and instead exposes a fixed **host boundary** of six offerings — a closed
contribution-point set, a declarative manifest + discovery registry, a stable
versioned host API (read *projections*, not raw tables; SemVer `engines` pin;
stable/proposed split), governed data channels (a capability-filtered/redacted egress
event stream + an authenticated edge-ingest endpoint), an identity + capability +
consent model (`extension_grants` enforced by an RLS-scoped role), and a
per-tenant lifecycle + marketplace.

We will run **third-party extension code developer-hosted** (the Stripe / GitHub Apps
model): extensions run on the developer's own infrastructure, authenticate to Fyralis,
consume the filtered stream (read), and call the host API + edge-ingest endpoint
(write). Fyralis never executes third-party code. Third parties may **read
(capability-scoped) and write only at the ingestion edge** — never into the
reasoning/synthesis loop, which stays host-owned/first-party. The manifest, capability
model, and host API are kept **transport-agnostic** so a Fyralis-hosted sandbox tier
can be added later without re-architecture.

Rejected alternatives:

- **Keep ad-hoc per-interface wiring** — cannot admit third parties safely (no stable
  contract, no capability boundary, core-imports-interface coupling).
- **Build a Fyralis-hosted code sandbox now** (Shopify Functions / Snowflake Native
  Apps) — infeasible for a small team (a multi-quarter secure-runtime build with a
  permanent maintenance tax) and unnecessary for read + edge-ingest, which need no
  in-process execution.
- **Open, free, self-publish ecosystem** — rejected in favour of a curated review
  pipeline given the sensitivity of the data extensions can reach.
- **Let extensions write into the synthesis loop** — rejected; the host must own what
  becomes belief (mirrors Backstage's entity-provider-at-the-edge vs host-owned
  processor split).

Delivery is **phased** so each phase ships standalone value and the expensive
third-party security/marketplace spend is deferred until real demand: first-party
consolidation (manifest + generalize the inline hook into a registered enricher) →
versioned host API → capability model → developer foundation (public SDK + local dev
harness + docs) → trust + data plane → curated marketplace → commerce/scale.

## Consequences

**Easier / now possible:** one consistent interface model and a single place to see
every interface; an external developer ecosystem; auditable, capability-scoped
substrate access for *all* interfaces (first-party included); host refactors stop
silently breaking interfaces (the versioned API ends the lockstep fragility).

**Harder / new work:** four genuinely new components to build and operate — the
versioned host API, the filtered/redacted egress stream, the `extension_grants`
capability store, and the edge-ingest endpoint — plus a security-review/signing
pipeline, per-extension observability, and developer ToS + Data Processing Agreements
(third parties touching employee comms + finance is a real compliance surface).

**New constraints / forbidden:** third-party extensions are **async-only** (no inline,
in-hot-path enrichment — that stays first-party); third parties cannot mutate
Models/Acts/Resources; edge-ingested third-party observations are written at a
constrained trust tier.

**Revisited / falsified when:** a class of integrations genuinely needs synchronous,
in-loop, low-latency participation (→ add the deferred hosted-sandbox tier), or a
verified partner needs deeper access than the developer-hosted boundary allows (→
the verified-partner sidecar tier). Open questions tracked on the
[Interfaces & Extensions](../architecture/interfaces.md) page.
