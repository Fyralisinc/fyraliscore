# ADR-0004: Interface/extension platform — developer-hosted third-party extensions on a governed host boundary

- **Status:** Accepted — E0–E2 + DP-1 implemented 2026-06-13 (first-party/verified governance + the developer foundation are production-grade; E3/E4 — the third-party data plane + marketplace — remain demand-gated). <!-- Proposed | Accepted | Superseded by ADR-XXXX | Deprecated -->
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

**Governance specifics (resolved 2026-06-13).** Four points the proposal had left open
are now decided (rationale grounded in how the substrate actually behaves — `trust_tier`
is a continuous *weight* in retrieval/edge scoring, not a binary gate):

- **Egress redaction.** The filtered stream emits versioned read *projections*
  (`ObservationView`), never raw `observations` rows; each streamed channel ships a
  redaction projection authored *alongside its handler*, default-stripping the raw
  payload (`content["_raw"]`) and actor-identity fields. The channel/source owner
  defines "sensitive"; a tenant admin may **tighten, never loosen**.
- **Edge-ingest trust ceiling.** Third-party edge-ingested observations **default to
  `inferential_external`** (extensions *derive*, they do not witness a system of record)
  and are **capped at `attested_agent`** — granted only with a recorded re-attestation
  justification. `authoritative` / `authoritative_external` are **unreachable** for any
  non-first-party; over-ceiling writes are **rejected, not silently downgraded**;
  unreviewed/private extensions floor at `unvetted`. Because trust is a weight, this
  ceiling — not a gate — is the primary limiter on how much third-party signal can move
  synthesis.
- **Review rigor scales with blast radius (tenants exposed), not code trust.** Both
  private and public extensions pass an automated gate (manifest lint + scope
  justification + callback-domain verification); **manual review + signing is required
  only for *public listing***. Private per-tenant extensions are self-attested with a
  louder consent screen; the data-processing/legal gate applies to **both**.
- **Reasoning-substrate isolation.** Ownership-scoped isolation alone is **insufficient**
  for a synthesis substrate — an edge observation still influences shared inferences via
  trust-weighted scoring. Keeping **E5 (reasoning writes) first-party is therefore the
  load-bearing containment**, not conservatism; together with the trust-weight discount
  above it is what stops a third party authoring a belief. Models materially driven by a
  single third-party extension are tagged and surfaced as **contestable**.

Delivery is **phased** so each phase ships standalone value and the expensive
third-party security/marketplace spend is deferred until real demand: first-party
consolidation (manifest + generalize the inline hook into a registered enricher) →
versioned host API → capability model → developer foundation (public SDK + local dev
harness + docs) → trust + data plane → curated marketplace → commerce/scale. The
story-level breakdown, dependencies, and trust tiers are in the
[Interface platform roadmap](../architecture/interface-platform-roadmap.md). The
first-party consolidation has begun: `github_intel` + `code_intel` are now **extracted**
to a separate repo and the inline hook is **removed** from `services/ingest/ingestion/core.py`;
the generalized draft-enricher seam it re-attaches through is the first roadmap step.

## Consequences

!!! note "Implementation status (2026-06-13): runtime-complete for first-party/verified extensions"
    Beyond the discovery seam + host API + capability *store* (E0–E2, DP-1), the platform
    is now wired at runtime for **any** extension: (a) a generic **background-worker**
    contribution point (`company_os.workers` + the `lib.extensions.run_workers`
    supervisor / `extension_workers` compose service) — declared workers actually run;
    (b) **per-tenant capability enforcement applied** at the ingest seam
    (`access.enricher_allowed` gating `run_enrichers` via each contribution's
    `manifest_id`) — a non-granted tenant gets the raw signal; (c) a tenant
    **install/enable lifecycle** (`services/platform/extensions/lifecycle.py` +
    `scripts/manage_extension.py`); (d) **extension-owned schema** via the
    `company_os.migrations` group + a per-extension migration ledger. `github-intel`
    consumes all four as the first interface; `scripts/demo_extension_e2e.py` exercises
    install → enforce → supervise → index → observe end-to-end against a throwaway DB
    (observable in pgAdmin). E3/E4 (third-party data plane + marketplace) remain
    demand-gated and unchanged.

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
Models/Acts/Resources (E5 first-party, indefinitely); edge-ingested third-party
observations are written at a constrained trust tier (default `inferential_external`,
ceiling `attested_agent`, never `authoritative` — see *Governance specifics* above).

**Revisited / falsified when:** a class of integrations genuinely needs synchronous,
in-loop, low-latency participation (→ add the deferred hosted-sandbox tier), or a
verified partner needs deeper access than the developer-hosted boundary allows (→
the verified-partner sidecar tier). **Precondition:** because the reasoning substrate is
contained by *E5-first-party + the trust-weight discount* rather than by hard isolation,
any move to relax E5-first-party or raise the trust ceiling **must** re-open the
"does the reasoning substrate need a stronger sandbox?" question before shipping.
Remaining open questions tracked on the
[Interfaces & Extensions](../architecture/interfaces.md) page.
