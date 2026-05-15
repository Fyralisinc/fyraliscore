# Phase 0 Research: GitHub Production Integration

This document resolves the technical decisions that drive the implementation plan. All five spec-level clarifications are locked in `spec.md` Clarifications; the items below are smaller plan-level confirmations (dependency presence, exact verbs, exact GitHub error shapes, exact module surfaces) that the plan relies on.

## R1 — PyJWT dependency

**Decision**: Add `pyjwt[crypto]>=2.8` as a direct dependency in `pyproject.toml`. The `[crypto]` extra pulls in the `cryptography` library's RSA-SHA256 backend which PyJWT uses for `algorithm='RS256'`.

**Rationale**: GitHub App authentication requires minting JWTs signed RS256 with a PEM private key. Implementing JWT manually (header.payload.signature, base64url, JSON-canonical, ASN.1 PKCS#1-v1_5 signature) is non-trivial and easy to get subtly wrong. PyJWT 2.8+ has a stable API, no known CVEs in our supported Python range (3.11+), and is the conventional choice in the Python ecosystem for GitHub App integrations. The transitive dep `cryptography` is already in the project (used by `lib/shared/secrets/fernet.py`); we're not pulling in a new runtime tree.

**Alternatives considered**:
- **`python-jose`** — broader algorithm support but with three CVEs in 2023-24 around algorithm confusion and a less-maintained release cadence. Rejected.
- **Manual implementation** — possible (the JWT spec is short) but high cost-to-value; rejected on YAGNI grounds (Constitution §X — copy the bar of the existing `Embedder` Protocol: don't abstract until there are two backends; here we have one signing primitive and PyJWT is the canonical implementation).
- **Re-using `cryptography` directly without PyJWT** — drops one indirection but keeps us responsible for base64url + JSON canonical + payload framing. Net cost > benefit.

## R2 — GitHub error response shapes for the chokepoint

**Decision**: Trigger `_disable_installation_github` from the outbound chokepoint on EITHER of these two GitHub response shapes:

1. HTTP 401 with response JSON body containing `{"message": "Bad credentials", ...}`. This is GitHub's response when the supplied JWT or installation access token is no longer valid for the named App/installation pair — the canonical "App has been uninstalled or the key was rotated" signal.
2. HTTP 404 with response JSON body containing a `documentation_url` field ending in `/rest/apps/apps#get-an-installation` OR `/rest/apps/installations` (depending on the specific endpoint). This is GitHub's response when the installation no longer exists from GitHub's view — uninstalled or transferred.

Any other 4xx/5xx response is treated as transient and does NOT trigger the chokepoint (preserves retry budget; matches IN-09 posture).

**Rationale**: These are GitHub's documented responses (per their REST API docs) for "this installation is gone." A broader trigger (e.g., any 4xx) would risk false-positive disables on rate-limit-adjacent 403s or input-validation 422s. A narrower trigger (e.g., 401 only) would miss the 404 case that fires when the install record is genuinely removed.

**Alternatives considered**:
- **Match on `documentation_url` substring alone** — fragile across documentation reorgs; better to match either the 401 message OR the 404+doc_url combination.
- **Always 401** — misses uninstalled-but-token-still-cached case.

**Source signal**: The two GitHub response shapes are stable per their changelog and have been consistent across the 2022–2026 window. We do NOT depend on response body shape for non-trigger paths — those use HTTP status alone.

## R3 — App private key loading mechanism

**Decision**: Support two operator-facing env vars, exactly one of which MUST be set at startup under `FYRALIS_ENV=prod`:

- `GITHUB_APP_PRIVATE_KEY` — multi-line PEM literal (works in `.env` files via the shell's `$'...'` escape or in Kubernetes Secret env injection).
- `GITHUB_APP_PRIVATE_KEY_PATH` — absolute filesystem path to a `.pem` file (the typical Kubernetes Secret → file projection pattern).

Both unset OR both set → `_assert_prod_safety_invariants` fails at startup with a clear error.

`mint_app_jwt()` reads whichever is set on EVERY call (no in-process cache). Parse cost (`load_pem_private_key`) is ~1 ms on CPython on the supported deployment hardware; acceptable per the performance constraints in plan.md.

**Rationale**: Operators have a strong preference between the two patterns based on their secret-management story (env-injection vs. file-mounted). Supporting both is one extra `if` in `jwt.py` — trivial cost, real operational benefit. Reading-on-every-mint is what makes rotation a no-op deploy: when the operator rotates the key in their secret-management system and recycles the env / file mount, the next mint picks up the new key without any application restart.

**Alternatives considered**:
- **Secret-store-backed key (DB-backed Fernet-encrypted PEM)** — over-engineered for v1. The App private key is operator-scoped not customer-scoped; per-deployment rather than per-tenant. The DB-backed secret store is the right home for per-tenant secrets (which IN-08 / IN-09 needed) but adds rotation latency for App-level secrets. Deferred to a follow-up if rotation cadence demands it.
- **Cache the parsed key object across mints** — would block rotation in the 99% case where the key hasn't changed; we'd save ~1ms per call at the cost of a rotation deploy. Rotation is the harder operational scenario; we optimize for it.

## R4 — `oauth_install_states` schema confirmation

**Decision**: Confirm — `oauth_install_states` has a `provider TEXT` column (added by migration 0040 in IN-08) and the state-token verification path keys on `(provider, nonce)`. The GitHub callback path SHALL pass `provider='github'` when verifying / consuming the nonce.

**Rationale**: Without this column, an attacker could mint a state token via the Slack install endpoint and replay it against the GitHub callback (or vice-versa). The provider scoping is the structural defense. This was added in IN-09's plan as a regression-test concern; we re-verify it as a precondition here.

**Verification step (Slice 1 of plan)**: Read the column list of `oauth_install_states` from the live `db_pool` fixture and assert `provider TEXT NOT NULL` is present.

## R5 — `installation_audit_log` action vocabulary

**Decision**: The `installation_audit_log.action` column is a free-form `TEXT` (not a CHECK-constrained enum) per IN-08's migration 0041. New actions used by IN-13:

- `install` — fresh OAuth callback success
- `reinstall` — same `installation_id`, same tenant, prior `enabled=FALSE`
- `update` — `setup_action='update'` callback (admin changed repo selection)
- `uninstall` — inbound `installation.deleted` OR outbound chokepoint
- `suspend` — inbound `installation.suspend`
- `unsuspend` — inbound `installation.unsuspend`
- `repo_change` — inbound `installation_repositories.added` / `.removed`
- `rejected_collision` — cross-tenant rebind attempt
- `repository_fetch_failed` — non-fatal: callback completed but `GET /installation/repositories` failed (selected_repositories=NULL with `context.selected_repositories_unknown=true`)

Each row carries a `context JSONB` field per IN-08. The plan uses `context.{added, removed}` for repo_change events.

**Rationale**: Reusing the existing audit log shape (no schema changes) and aligning vocabulary with IN-08 / IN-09 minimizes operator cognitive load when querying across providers.

## R6 — Replay-cache implementation choice

**Decision**: In-process `OrderedDict`-backed TTL LRU, modeled directly on `services/webhooks/tenant_resolver.py::InstallationCache`. Singleton at `services/integrations/github/replay_cache.py::REPLAY_CACHE` (factory-injected via app state, not module-global — see FR-016 of Constitution §V analogue). Max 4096 entries, 5-minute TTL.

**Rationale**: Constitution §X — copy the bar of the existing `InstallationCache` rather than introducing a new abstraction. The replay-cache is a defense-in-depth layer (FR-014), never a correctness gate (observation-layer dedup remains the backstop), so process-local state is acceptable. A cross-process cache (Redis) would add a dependency for a feature that doesn't need it.

**Alternatives considered**:
- **Redis-backed** — cross-process replay protection; adds Redis as a runtime dep we don't otherwise have. Rejected for v1.
- **Postgres-backed via a `webhook_deliveries_seen` table** — persistent across restarts, but requires a TTL job + write amplification on every delivery. The observation-layer dedup already covers correctness; we don't need persistence. Rejected for v1.

## R7 — Trust tier matrix (re-confirmation)

**Decision**: The existing trust-tier matrix in `services/ingestion/handlers/github.py` is correct and stable. IN-13 does NOT modify the per-event shaping. The matrix is:

| Event | Action | trust_tier | kind |
|---|---|---|---|
| `pull_request` | `closed` AND `merged=true` | `authoritative` | `state_change` |
| `pull_request` | `opened` / `reopened` | `inferential` | `signal` |
| `pull_request` | `closed` AND `merged=false` | `inferential` | `state_change` |
| `pull_request` | other actions | `inferential` | `signal` |
| `push` | (any) | `authoritative` | `signal` |
| `issues` | `opened` | `authoritative` | `signal` |
| `issues` | `closed` / `reopened` | `authoritative` | `state_change` |
| `issues` | other | `authoritative` | `signal` |
| `issue_comment` | (any) | `inferential` | `signal` |
| `pull_request_review` | `state='approved'` | `authoritative` | `state_change` |
| `pull_request_review` | other states | `inferential` | `signal` |
| `check_run` | `status='completed'` | `authoritative` | `state_change` |
| `check_run` | other | `authoritative` | `signal` |

**Rationale**: The existing handler shapes these correctly. No need to re-design. Plan Slice 5 verifies this by exercising US1's acceptance scenarios via `test_router_github_integration.py::test_verified_pull_request_lands_as_observation` and its siblings.

## R8 — Pagination of `GET /installation/repositories`

**Decision**: Read up to 3 pages (90 repos at GitHub's default `per_page=30`); if a 4th page would be required, persist the first 90 repos and write an audit-log context field `selected_repositories_truncated=true, total_available=<count_from_link_header>`. Operators can manually re-trigger via the update flow.

**Rationale**: 99% of GitHub orgs grant the App access to fewer than 90 repos. The handful that grant more (large monorepos with org-wide grant, where `selected_repositories` is `NULL` and "all" applies, OR enterprise orgs with hundreds of repos and explicit selection) get a documented degraded path rather than an unbounded fetch. For v1 simplicity, hard-cap rather than infinite-loop with backoff.

**Alternatives considered**:
- **Unbounded pagination** — risks slow install callbacks; rejected for v1 latency budget.
- **Async background fetch after callback** — adds a worker dep + race window where webhooks arrive before the allowlist is populated. Rejected.

## R9 — Webhook URL path consistency

**Decision**: The GitHub App webhook URL is `https://<gateway-host>/webhooks/github/events`. The existing webhook router at `services/webhooks/router.py` handles `/webhooks/{provider}/{subpath:path}` — `/webhooks/github/events` matches this pattern with `provider='github', subpath='events'`. No new route is mounted; the router branch on `provider='github'` is the modification point.

**Rationale**: Symmetric with the existing Slack `/webhooks/slack/events` and Discord `/webhooks/discord/events` paths. Operators have one mental model.

## R10 — `repositories` field on `installation_repositories` payloads

**Decision**: GitHub's `installation_repositories` webhook payload carries:
- `installation.id` — for tenant routing
- `action ∈ {added, removed}` — for dispatch
- `repositories_added: list[{full_name, ...}]` (on `action=added`)
- `repositories_removed: list[{full_name, ...}]` (on `action=removed`)

The lifecycle handler extracts `full_name` from each repo object. The `repository_selection` field at the payload root (`"selected" | "all"`) indicates whether the installation is in "all-repos" or explicit-selection mode; if it flips from `selected` to `all`, the handler MUST set `selected_repositories=NULL` (meaning "all"). Conversely, flipping from `all` to `selected` MUST set `selected_repositories=[]` initially with the next `added` event populating it.

**Rationale**: Per GitHub's documented webhook payload schema. Both transitions are user actions in GitHub's admin UI and must be honored on the Fyralis side.

**Edge case (handled)**: An `installation_repositories.added` event arriving for a `selected_repositories=NULL` installation (admin previously selected "all repos") is a no-op semantically — the new repo is already covered by "all". The handler writes the audit row but does NOT change `selected_repositories` to a non-NULL list.
