# Implementation Plan: IN-06 — Webhook Gateway Router

**Branch**: `IN-06-webhook-gateway-router` | **Date**: 2026-05-13 | **Spec**: [spec.md](./spec.md)
**Input**: Feature specification from `specs/IN-06-webhook-gateway-router/spec.md`

## Summary

Real webhook sources (Slack, GitHub, Linear, Stripe, Discord) authenticate
with cryptographic signatures, not Bearer tokens. The current
`/ingest/{channel}` endpoint is Bearer-protected and therefore cannot
accept any of them — this blocks every real customer integration.

The plan delivers a new unauthenticated-at-transport ingress path
`/webhooks/{provider}/...` that verifies each request with the
originating provider's published signature scheme, enforces a
per-provider replay window, distinguishes failure reasons in structured
errors, supports zero-downtime secret rotation via a per-tenant
secret store with overlap, and routes verified payloads through the
existing ingestion pipeline so Observations land with the correct
`source_channel`, `trust_tier`, and `tenant_id` — preserving every
downstream invariant.

The Slack verifier in
[services/ingestion/handlers/slack.py](../../services/ingestion/handlers/slack.py)
is reused as-is (cryptographic semantics are extracted, not rewritten);
the path layout is the only Slack-side change. Four new verifiers
(GitHub, Linear, Stripe, Discord) join under a common `Verifier`
contract behind a thin router. One new external dependency (`pynacl`)
enters `pyproject.toml` for Discord's ed25519.

## Technical Context

**Language/Version**: Python 3.11+ (matches `pyproject.toml` `requires-python = ">=3.11"`)
**Primary Dependencies**: FastAPI ≥0.110, asyncpg ≥0.29, Pydantic v2, structlog ≥24.1, `hmac`/`hashlib` from stdlib, **new: `pynacl` ≥1.5 for Discord ed25519**
**Storage**: Postgres 16 (pgvector image) — no new tables in this feature. Secret material lives in env / out-of-band secrets manager; per-(provider, tenant) mapping reads existing tenant config. Verified webhooks produce `observations` rows via the existing handler pipeline.
**Testing**: pytest + pytest-asyncio (existing markers: `integration`, `slow`); `respx` for any LLM-adjacent boundary (not used here); real Postgres via `db_pool` / `fresh_db` fixtures; vendor-sample-payload unit tests per provider
**Target Platform**: Linux containers under the existing `docker-compose.yml` topology; same image as `gateway`/`think_worker`/`post_commit_worker`
**Project Type**: Web service (FastAPI app), single repo. No frontend change.
**Performance Goals**: Spoofed-request rejection under 50ms p95 (success criterion SC-005). Verified-path overhead added by signature check ≤5ms p95 on the existing `/ingest` latency budget. No new DB round-trips before verification.
**Constraints**: Verification operates on **raw request bytes** — the router must capture the body before any JSON decode. The path MUST NOT shadow `/ingest/{channel}` (Bearer path stays). Body-size precheck from IN-01 applies before verification. Failure logs MUST NOT include the body or candidate signature.
**Scale/Scope**: Five providers at launch (Slack, GitHub, Linear, Stripe, Discord). One process can serve N tenants; per-(provider, tenant) secret count is small (1–2 during rotation). Webhook RPS is bounded by provider delivery rates — no horizontal-scaling problem at MVP.

## Constitution Check

*GATE: Must pass before Phase 0 research. Re-check after Phase 1 design.*

The constitution at `.specify/memory/constitution.md` v1.0.0 has ten core
principles. Evaluating each:

| # | Principle | Status | Notes |
|---|-----------|--------|-------|
| I | Four Foundations are epistemically distinct | **PASS** | No new foundation; this feature is plumbing that produces existing `Observation` rows via the existing pipeline. `born_from_event_id` invariant is preserved because Models still come from Observations downstream — unchanged. |
| II | Schema is append-only, migrations idempotent | **PASS** | No new migrations. If a per-tenant secret table is added in Phase 1 design, it MUST be a new numbered idempotent migration with `tenant_id` + FK + RLS per Principle III. Currently planned: secrets live out-of-band (env + secrets manager), so no migration. Decision deferred to Phase 1 (see Research item R3). |
| III | Tenant isolation is structural | **PASS (with Phase 1 obligation)** | Verified payloads MUST be ingested under `tenant_transaction(tenant_id)` so RLS bites downstream. Tenant resolution for an incoming webhook (Slack `team_id` → `tenant_id`, GitHub installation, etc.) is a Phase 1 design question; the fallback `DEFAULT_TENANT_ID` env shortcut used by `/ingest/{channel}` is NOT acceptable here. |
| IV | Integration tests use a real database | **PASS** | Per-provider unit tests against vendor sample payloads do NOT need a DB. End-to-end tests (verify → ingest → `observations` row) use `db_pool` fixture against real Postgres. No Postgres mocks anywhere. |
| V | Reasoning is separated from rendering | **N/A** | This feature touches neither Think nor RND. |
| VI | Trust, confidence, falsifiers are first-class | **PASS** | Each verified webhook produces an Observation whose `trust_tier` comes from `CHANNEL_TRUST_MAP` (already defines `slack:message`, `github:webhook`, `linear:webhook`, `stripe:webhook` — Discord channel name to be confirmed in Phase 1). No bypass of trust rules. |
| VII | Determinism, idempotency, audit trails | **PASS** | Dedup continues to be enforced by the existing `external_id` contract per handler. No new queue. No new audit table. |
| VIII | Errors carry structured context | **PASS** | New error classes (one per failure reason, OR a single `WebhookVerificationError` with a `reason` field) derive from `CompanyOSError`. HTTP response shape matches `to_dict()`: `{code, message, context}`. |
| IX | Substrate changes are dual-write until proven | **N/A** | No substrate-shape change. |
| X | Simplicity, YAGNI, no premature abstraction | **PASS** | The `Verifier` Protocol earns its keep because we have 5 backends (≥ the §13.2 `Embedder` bar). No plugin registry, no DI framework — verifiers are imported and dispatched from a static map keyed on provider name. |

**Stack-constraint check**:

- New dep `pynacl` is justified (Discord's ed25519 is a vendor protocol; we don't roll our own crypto).
- No `print()`, no `uuid.uuid4()` in service code; reuse existing `structlog` + `uuid7()`.
- New router factory follows the existing `build_*_router()` pattern; no module-level globals.

**Workflow check**:

- The `.specify/extensions.yml` `before_plan` / `after_plan` git hooks fire automatically; existing CI gates (`ruff`, `pytest -m integration`, `check_schema_drift.py`) all run as a matter of course. No new gates introduced.

**Outcome**: PASS. No constitution exceptions are required for the planned scope. Two Phase 1 design questions (R3 secret store choice, R5 tenant-resolution mechanism) carry constitution-derived obligations rather than exemptions.

*(Re-check after Phase 1 design completes; record any new violations in Complexity Tracking below.)*

## Project Structure

### Documentation (this feature)

```text
specs/IN-06-webhook-gateway-router/
├── source.md           # Original prompt (already present)
├── spec.md             # Feature specification (already present)
├── plan.md             # This file (/speckit-plan output)
├── research.md         # Phase 0 output — resolve unknowns (R1–R6)
├── data-model.md       # Phase 1 output — Verifier contract, secret-store shape, error taxonomy
├── quickstart.md       # Phase 1 output — local dev guide for adding a new provider
├── contracts/          # Phase 1 output
│   ├── webhook-router.openapi.yaml
│   └── verifier-protocol.md
└── tasks.md            # Phase 2 output (/speckit-tasks — NOT created here)
```

### Source Code (repository root)

```text
services/
├── webhooks/                       # NEW — owns the unauthenticated webhook ingress
│   ├── __init__.py
│   ├── router.py                   # FastAPI router factory: build_webhooks_router(deps)
│   ├── verifier.py                 # Verifier Protocol + WebhookVerificationError taxonomy
│   ├── secrets.py                  # Per-(provider, tenant) secret resolution + rotation overlap
│   ├── tenant_resolution.py        # Provider-specific tenant lookup (e.g. Slack team_id → tenant_id)
│   ├── signatures/
│   │   ├── __init__.py
│   │   ├── slack.py                # Thin adapter — reuses services/ingestion/handlers/slack.verify_slack_signature
│   │   ├── github.py               # HMAC SHA-256 over body, X-Hub-Signature-256
│   │   ├── linear.py               # HMAC SHA-256 over body, Linear-Signature
│   │   ├── stripe.py               # HMAC SHA-256 over t={ts}.{body}, Stripe-Signature
│   │   └── discord.py              # ed25519 over X-Signature-Timestamp + body (pynacl)
│   └── tests/
│       ├── __init__.py
│       ├── conftest.py             # Vendor-sample fixtures (recorded payloads + test keys)
│       ├── test_router_paths.py    # Path routing, no-bearer, coexistence with /ingest
│       ├── test_slack.py           # Vendor sample + tamper + replay tests
│       ├── test_github.py
│       ├── test_linear.py
│       ├── test_stripe.py
│       ├── test_discord.py
│       ├── test_rotation.py        # Old + new secret accepted; remove old → rejected
│       ├── test_observability.py   # Metric increments + structured log shape
│       └── test_e2e_ingest.py      # @integration — verify → observations row (real Postgres)
│
├── ingestion/handlers/             # EXISTING — unchanged in semantics
│   └── slack.py                    # verify_slack_signature reused via services/webhooks/signatures/slack.py
│
└── gateway/
    └── main.py                     # MODIFIED — mount build_webhooks_router(); confirm Bearer middleware skips /webhooks/

pyproject.toml                      # MODIFIED — add pynacl >=1.5

specs/IN-06-webhook-gateway-router/ # this directory
```

**Structure Decision**: A new top-level service module `services/webhooks/`
owns the entire webhook ingress, parallel to `services/ingestion/`. Two
reasons drive this rather than placing it under `services/ingestion/`:

1. **Different authentication contract.** `services/ingestion/` is
   Bearer-protected and called via `/ingest/{channel}`. `services/webhooks/`
   is signature-protected at the transport layer. Keeping them as
   sibling modules makes the boundary obvious in the file tree.
2. **Different lifetime.** The webhook router is expected to outlive
   `/ingest/{channel}` (that path is a legacy Bearer endpoint for
   internal callers — simulation harness, tests — and may be retired
   for the five providers in a follow-up). Co-locating would force a
   later rename.

Verifiers under `services/webhooks/signatures/` each export a single
function or class matching the `Verifier` Protocol from
`services/webhooks/verifier.py`. The Slack module is a thin adapter
that delegates to the existing
`services.ingestion.handlers.slack.verify_slack_signature` — the
crypto semantics are NOT duplicated.

## Workflow Phases (executed by /speckit-plan)

The plan template references three workflow phases. Outputs are
written to the files listed under "Documentation" above.

### Phase 0 — Research (output: `research.md`)

Six open questions to resolve before design:

- **R1. Header set per provider.** Confirm the exact request headers
  carried by each provider's signed delivery as of 2026 (Slack's
  `X-Slack-Signature` + `X-Slack-Request-Timestamp`; GitHub's
  `X-Hub-Signature-256`; Linear's `Linear-Signature` and any sibling
  timestamp header; Stripe's `Stripe-Signature` parsed for `t=` and
  `v1=`; Discord's `X-Signature-Ed25519` + `X-Signature-Timestamp`).
  Note any historical aliases (GitHub's deprecated `X-Hub-Signature`
  SHA-1, which we MUST NOT accept).

- **R2. Replay window per provider.** Default 300s (constitution-
  aligned with the Slack precedent already in the codebase). Confirm
  vendor docs for Stripe (300s default), GitHub (no documented window
  — decide if we set one anyway), Linear (confirm), Discord
  (timestamp present, but vendor does not document a window — we set
  one).

- **R3. Secret store shape.** Pick one of:
  (a) env-var-only (e.g. `WEBHOOK_SECRET_SLACK_<TENANT_ID>=...`);
  (b) a new Postgres table `webhook_secrets` with `tenant_id`,
  `provider`, `secret`, `active_from`, `active_until` (rotation as
  row lifecycle, FK + RLS per Principle III);
  (c) external secrets-manager handle. Recommendation in `research.md`,
  decision in `data-model.md`. Constraint: FR-010 requires rotation
  without process restart.

- **R4. Channel-name mapping per provider.** Slack already uses
  `slack:message`. GitHub events (PR opened, issue opened, etc.) need
  to map to `github:<event_type>` or a single `github:webhook` —
  `CHANNEL_TRUST_MAP` already has `github:webhook` as `authoritative`;
  confirm we use the consolidated form or fan out by event type. Same
  question for Linear and Stripe. Discord needs a new
  `CHANNEL_TRUST_MAP` entry (proposed: `discord:interaction`).

- **R5. Tenant resolution mechanism.** For each provider, where does
  `tenant_id` come from?
  - Slack: `team_id` in the event payload → tenant config lookup.
  - GitHub: `installation.id` → tenant config lookup.
  - Linear: organizationId → tenant config lookup.
  - Stripe: account/connect_account → tenant config lookup.
  - Discord: application_id → tenant config lookup.
  Output: a single contract `resolve_tenant(provider, payload) → UUID | None`
  with provider-specific resolvers. Phase 1 deliverable.

- **R6. Slack URL-verification handshake.** Slack sends a
  `url_verification` event on app installation that must echo back
  a challenge. Confirm any other provider has a similar one-time
  handshake (Discord PING, etc.) and design the special-case path.

### Phase 1 — Design (outputs: `data-model.md`, `contracts/`, `quickstart.md`)

**`data-model.md`** specifies (no implementation, just shapes):

- The `Verifier` Protocol: `verify(provider: str, body: bytes,
  headers: Mapping[str, str], secrets: Sequence[Secret], *, now: float
  | None = None) -> VerifiedContext | raises WebhookVerificationError`.
- `VerifiedContext`: provider, payload-as-bytes (the literal bytes,
  re-handed to the ingestion pipeline), parsed timestamp (if the
  provider includes one), secret-id that matched (for rotation
  observability).
- `Secret`: provider, tenant_id, value, active_from, active_until
  (the rotation overlap window).
- `WebhookVerificationError` hierarchy under `CompanyOSError` —
  per FR-005, six distinct failure reasons. Decide: one class with a
  `reason: Literal[...]` field, or six subclasses. Recommendation:
  single class with literal reason, so callers can branch on
  `err.reason` without isinstance and metrics labels come straight
  from the field.

**`contracts/webhook-router.openapi.yaml`** — minimal OpenAPI spec
covering:

- `POST /webhooks/slack/events` (and `/webhooks/slack/interactions`
  if R1 finds a second Slack endpoint shape)
- `POST /webhooks/github/{event_type}` or `POST /webhooks/github`
  (per R4 decision)
- `POST /webhooks/linear`
- `POST /webhooks/stripe`
- `POST /webhooks/discord/interactions`
- All return 200 on verified + ingested, 401 on verification failure
  with `{code, message, context: {provider, reason, ...}}`, 413 on
  oversize body (IN-01).

**`contracts/verifier-protocol.md`** — the Protocol shape from
`data-model.md` rendered as a markdown contract that any future
verifier (Twilio, Shopify, …) must satisfy. One page.

**`quickstart.md`** — how to add a sixth provider:
1. Register a new `<provider>.py` under `services/webhooks/signatures/`
   that satisfies the Verifier Protocol.
2. Add a `CHANNEL_TRUST_MAP` entry.
3. Add a tenant-resolver entry.
4. Add a `dev:` secret in `.env.example`.
5. Drop a vendor-sample payload into `services/webhooks/tests/samples/<provider>/`.

### Phase 2 — Tasks (output: `tasks.md`, produced by `/speckit-tasks` — NOT in this PR)

Tasks will be enumerated story-by-story from the spec, with
acceptance-test mappings to the FRs and SCs. Out of scope here.

## Test Strategy

Aligned with Constitution Principle IV (real DB, not mocks):

- **Unit tests per verifier** (`test_slack.py`, `test_github.py`,
  `test_linear.py`, `test_stripe.py`, `test_discord.py`) — vendor-
  sample payloads recorded into `services/webhooks/tests/samples/`,
  signed with a known test secret. Each test file exercises: happy
  path, tampered body, tampered signature, missing header, malformed
  header, expired timestamp, secret-not-configured. No DB.

- **Router tests** (`test_router_paths.py`) — path routing, Bearer
  middleware skip on `/webhooks/*`, body-size precheck integration
  (IN-01), URL-verification handshakes.

- **Rotation test** (`test_rotation.py`) — configure two
  simultaneously-active secrets, hit endpoint with each, swap, hit
  again. No DB unless R3 chooses option (b).

- **Observability tests** (`test_observability.py`) — assert metric
  counter incremented with `{provider, reason}` labels on every
  failure path; assert structured log emits `{code, provider, reason,
  tenant_id?}` and NEVER `{body, signature}` (Principle VIII +
  FR-016).

- **End-to-end integration** (`test_e2e_ingest.py`, marked
  `@pytest.mark.integration`) — real Postgres via `db_pool` fixture;
  send a verified Slack request through the new router; assert an
  `observations` row exists with the right `tenant_id`,
  `source_channel`, `trust_tier`, and `external_id`; assert
  RLS-correct (`tenant_transaction(other_tid)` cannot see it).
  Repeat once for at least one HMAC provider (GitHub) and once for
  Discord to exercise the ed25519 path.

- **Determinism** — replays of recorded vendor samples MUST be
  deterministic; clock-sensitive tests use a fixed `now` injected
  through the verifier signature (the existing Slack verifier already
  takes a `now` kwarg).

- **Coverage gate** — all six failure reasons (FR-005) must have at
  least one negative-path test per provider.

## Migration & Coexistence Plan

The new path coexists with `/ingest/{channel}` for the lifetime of
this feature. No `/ingest/{channel}` behavior changes.

| Stage | Work | Gate |
|-------|------|------|
| A | Land `services/webhooks/` with all five verifiers + router + tests; mount in `services/gateway/main.py` | All Phase 1 tests pass; SC-001..SC-008 demonstrated locally |
| B | Cut over the dogfood Slack app to `/webhooks/slack/events` | Real Slack workspace produces Observations through the new path for ≥7 days |
| C | Per-provider rollouts for GitHub / Linear / Stripe / Discord as integrations land | One soak week per provider |
| D | (Separate plan) Retire `/ingest/{channel}` for the five providers; keep it for internal callers (simulation harness) | Out of scope of IN-06 |

## Complexity Tracking

> **Fill ONLY if Constitution Check has violations that must be justified**

No constitution violations are required for the planned scope. The
two Phase 1 design questions (R3 secret store, R5 tenant resolution)
do not introduce exceptions; they carry forward Principle III
(tenant isolation) and Principle II (migration discipline) as
constraints on the design choice.

If Phase 1 resolution of R3 selects option (b) (Postgres-backed
`webhook_secrets` table), that migration MUST satisfy Principle II
(idempotent, append-only, FK + RLS per Principle III, `tenant_id`-
prefixed indexes) — same bar as any other tenant-scoped table. No
exception requested in advance; the migration itself will pass or
fail the principle check at PR time.

| Violation | Why Needed | Simpler Alternative Rejected Because |
|-----------|------------|-------------------------------------|
| _(none)_  | _(n/a)_    | _(n/a)_                              |
