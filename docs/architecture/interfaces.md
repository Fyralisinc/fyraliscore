# Interfaces & Extensions

> Source: cross-cutting — `services/ingest/integrations/*` (finance),
> `services/product/greeting` (CEO view); the attach seams in
> `services/ingest/ingestion/handlers/`, `services/reasoning/think/hooks.py`, and
> `services/app/gateway/extensions.py`. The `github_intel` + `code_intel` interface
> was **extracted** to a separate repo (`Fyralisinc/github-intel`) as the first step
> of building this layer. Part of the [architecture overview](index.md).

**One-line:** an *interface* (extension) is a self-contained capability layered on
the core substrate — it consumes core signals and/or contributes derived ones.
The finance sources and the CEO view are the first in-core interfaces; `github_intel`
+ `code_intel` were the proof-of-concept, now **extracted** to `Fyralisinc/github-intel`
to return as the first true external interface. This page documents how interfaces
attach **today** and the **proposed** unified, externally-extensible interface layer
([ADR-0004](../adr/0004-interface-extension-platform.md)).

## What is an interface

Fyralis is a **core substrate** (ingestion → [observations](../glossary.md)
system-of-record → reasoning/synthesis → product) with **interfaces** built on top —
the editor-and-extensions analogy: one core, many capabilities layered on it. An
interface reads core signals (and reasoning outputs) and/or writes derived signals
back, without being part of the core.

## Today — how interfaces attach

Three different mechanisms have been in use. The inconsistency — in particular that
two of the three made the **core import the interface** — is what the proposed layer
resolves; the `github_intel` case has already been cut out of core (see the extracted row).

| Interface | Attachment mechanism | Code |
|-----------|----------------------|------|
| `github_intel` + `code_intel` *(extracted)* | **was** an inline draft-enrichment hook in `core.py` + an out-of-band FSM worker; **extracted** to `Fyralisinc/github-intel` (the inline seam is now removed from core) — returns as the first external interface | `Fyralisinc/github-intel` (was `services/ingest/github_intel/`) |
| Finance sources | **source-handler registry** — `@register("mercury:api")` + fetcher/planner/reconciler dispatch + OAuth routers | `services/ingest/ingestion/handlers/__init__.py`, `services/ingest/integrations/{mercury,quickbooks,…}/` |
| CEO view | **gateway surface** — routers + an in-process scheduler mounted in the gateway lifespan | `services/product/greeting/`, `services/app/gateway/` |

## Existing extensibility primitives

The platform already ships the building blocks an extension layer needs — about half
of the proposed boundary is *composing* these:

- **Entry-point plugin discovery** — the `company_os.reasoning_augmentors`
  (`services/reasoning/think/hooks.py`) and `company_os.gateway_extensions`
  (`services/app/gateway/extensions.py`) groups, discovered via `importlib.metadata`
  and failure-isolated. The `fyralis_demo` overlay already attaches through them
  *without the core importing it*.
- **Per-tenant feature flags** — `TenantFlags`
  (`services/ingest/ingestion/feature_flags/client.py`), the activation/enablement
  primitive (e.g. `ingestion.kafka_path_enabled`) and a kill-switch.
- **Tenant isolation** — `tenant_transaction` + RLS (`lib/shared/tenant_context.py`)
  on every tenant-scoped table.
- **Five-layer access control** — `can_read`
  (`services/platform/access_control/checks.py`), whose `source_channel` /
  `resource_kind` discriminators are the vocabulary a capability model reuses.

## Proposed — a unified, externally-extensible layer

!!! note "Status: proposed (ADR-0004), not yet implemented"
    The model below is the target architecture for opening Fyralis to **external
    developers** (a curated marketplace of developer-hosted extensions). It is
    design, not current reality.

The core **inverts its dependency**: instead of importing interfaces, it exposes a
fixed **host boundary** that extensions bind to and are *discovered* through. The
host offers six things:

1. **Contribution surface** — a *closed* set of extension points. External (async,
   network-isolated) extensions use only the safe subset: **stream-consume**,
   **edge-ingest**, **product-surface**.
2. **Manifest + discovery registry** — an extension declares identity, contributions,
   activation events, and requested scopes; the host discovers + validates it
   (generalizing the existing entry-point groups).
3. **Stable versioned host API** — internals-hiding read *projections* (not raw
   tables) + a write API, with a SemVer `engines` pin and a stable/proposed split.
4. **Governed data channels** — *egress*: a capability-filtered, redacted,
   per-`(extension, tenant)` event stream; *ingress*: an authenticated,
   rate-limited edge-ingest endpoint at a constrained [trust tier](../glossary.md).
5. **Identity + capability + consent** — each extension is an OAuth2 principal;
   scopes are declared then granted (`extension_grants`) through an admin consent
   flow; enforced by an RLS-scoped role + rate limits + audit + a kill-switch.
6. **Lifecycle + catalog** — per-tenant install/enable/disable, versioning with
   re-consent on new scopes, and a marketplace/registry.

The genuinely **new** surface is just four pieces: the versioned host API, the
filtered/redacted egress stream, the `extension_grants` capability store, and the
edge-ingest endpoint. The rest is composing the primitives above.

## How it's wired (proposed target)

```mermaid
graph TD
  subgraph EXT["External Extension — developer-hosted"]
    EMAN["Manifest<br/>id · contributes · activation · scopes"]
    ESVC["Extension service<br/>(developer infra)"]
  end
  subgraph BOUND["Host Boundary — what Fyralis offers"]
    REG["Registry + manifest loader"]
    API["Versioned Host API<br/>(stable / proposed)"]
    IDP["Extension identity (OAuth2)"]
    GRANTS["Capability store<br/>extension_grants + consent"]
    STREAM["Egress: filtered + redacted stream"]
    INGEST["Ingress: authed edge-ingest"]
    ENF["Enforcement<br/>RLS · quotas · audit · kill-switch"]
  end
  subgraph CORE["Fyralis Core"]
    ING["Ingestion pipeline"]
    OBS[("observations")]
    THINK["Reasoning / Think"]
    PROD["Product / CEO view"]
    KAFKA{{"Kafka data plane"}}
  end
  ADMIN["Tenant admin"]

  EMAN --> REG --> API
  ADMIN -->|install + consent| GRANTS
  OBS --> KAFKA --> STREAM
  STREAM -->|granted tenants only| ESVC
  ESVC -->|read projections| API --> OBS
  ESVC -->|POST derived signals| INGEST --> ING --> OBS --> THINK --> PROD
  IDP -. authn .-> ESVC
  GRANTS -. authz .-> STREAM
  GRANTS -. authz .-> INGEST
  ENF -. guard .-> STREAM
  ENF -. guard .-> INGEST
```

> The diagram is the **proposed target**, not current wiring. Per the
> [diagram conventions](index.md#diagram-conventions), dotted edges here are the
> authorization / enforcement relationships.

## Trust tiers

| Tier | Isolation | Who | May contribute |
|------|-----------|-----|----------------|
| First-party | in-process | core team | all points, incl. inline enrich + reasoning writes |
| Verified-partner | separate worker / sidecar | reviewed partners | sources, workers, post-commit actions, product surfaces |
| Third-party | network boundary (**developer-hosted**) | marketplace developers | read via stream + **write only at the ingestion edge** — never the synthesis loop |

External (third-party) extensions are **developer-hosted**: they run on the
developer's own infrastructure and interact only through the boundary — never the
database or the ingest hot path. They therefore operate **asynchronously** (consume
stream → compute → write back via edge-ingest), which is why, e.g., a third-party
"GitHub Code Intel" would be the out-of-band worker model rather than the inline
enricher.

## Key seams & primitives

| Concern | Path | Role |
|---------|------|------|
| Inline enrich hook (removed) | `services/ingest/ingestion/core.py` | the former `github_intel` attach point — now cut from core; to be reintroduced as a registered **draft-enricher** seam |
| Handler registry | `services/ingest/ingestion/handlers/__init__.py` | `register(channel)` source-handler seam |
| Entry-point discovery | `services/reasoning/think/hooks.py`, `services/app/gateway/extensions.py` | the `company_os.*` plugin groups (reasoning augmentors, gateway extensions) to generalize |
| Feature flags | `services/ingest/ingestion/feature_flags/client.py` | per-tenant activation/enablement + kill-switch |
| Tenant isolation | `lib/shared/tenant_context.py` | RLS + `tenant_transaction` — the isolation substrate |
| Access control | `services/platform/access_control/checks.py` | `can_read` — the capability vocabulary |

## Design rationale

The full proposal — the developer-hosted ruling (vs a Fyralis-hosted sandbox), the
menu of candidate interfaces (Finance Intelligence, Persona views, Sales/CRM, People/HR),
the external research (VS Code / Backstage extensibility patterns), the
capability/isolation governance, and the phased rollout — is in
[ADR-0004](../adr/0004-interface-extension-platform.md). The first-party
consolidation it builds on (generalizing the inline hook into a registered enricher)
is the same `ingest_from_draft` path documented in [Ingest](ingest.md).

> **TODO(human):** resolve before this is linked from a public developer portal —
> redaction granularity in the egress stream; whether third-party edge-ingested
> observations are capped below the `authoritative` trust tier; the marketplace
> commercial terms (revenue share / review SLA); and whether a *reasoning* substrate
> needs stronger isolation than the ownership-scoped (capability + RLS) model.
