# Phase 0 Research — IN-08 Slack Production Integration

Resolves every `NEEDS CLARIFICATION` from `plan.md` Technical Context. Each item: Decision → Rationale → Alternatives considered.

---

## R1 — Fernet vs. alternative envelope encryption library for the MVP

**Decision**: Use `cryptography.fernet.Fernet` from the `cryptography` library. The `MASTER_KEK` env var is a base64-encoded 32-byte URL-safe key (Fernet's native format). A `SecretStore` Protocol wraps the implementation so a future KMS backend (AWS KMS DEK envelope, GCP KMS, HashiCorp Vault) drops in without changing call sites.

**Rationale**:

- `cryptography` is already a transitive dep of `httpx`/`urllib3` in our tree; no new top-level package is added. (Verified by `pyproject.toml` inspection during planning.)
- Fernet provides authenticated symmetric encryption (AES-128-CBC + HMAC-SHA256) with built-in timestamp metadata, opaque ciphertext envelope, and key rotation support out of the box via `MultiFernet`. That last property gives us a clean rotation seam without a new abstraction.
- The shape of the API maps 1:1 onto the spec's `put / get / rotate / delete` requirements (FR-001..FR-004).
- For KMS later, the envelope pattern is: a per-row DEK encrypted by the KMS-held KEK. `MultiFernet` can be replaced by a `KmsEnvelope` class implementing the same Protocol without touching the table schema (DEK ciphertext can live alongside the row ciphertext in the same `BYTEA` column, prefixed by a version byte).

**Alternatives considered**:

- **PyNaCl `SecretBox`**: lighter dep but no built-in rotation primitive; we'd hand-roll the multi-key logic.
- **AWS KMS directly at MVP**: violates §X (no second-caller yet for the abstraction we'd need to build, and adding a cloud dep at MVP slows local dev). Roadmap, not MVP.
- **Application-level libsodium via `pysodium`**: same shape as PyNaCl; same rotation gap; not already in tree.

**Implementation note**: `MASTER_KEK` is read once at gateway startup into a module-private `MultiFernet` instance held by the `FernetSecretStore`. If `MASTER_KEK` is empty or malformed at startup in production, the gateway fails to start (loud-fail per §VIII). In dev, a missing `MASTER_KEK` generates a one-shot in-memory key and logs a structured warning; this dev path is hard-gated by the same flag IN-07 uses to identify non-prod environments (see R4).

---

## R2 — Slack `oauth.v2.access` response shape

**Decision**: The `oauth.v2.access` JSON response carries:

- `ok: true | false`
- `access_token` (the bot token, prefix `xoxb-…`)
- `token_type: "bot"`
- `scope` (comma-separated scopes granted)
- `bot_user_id`
- `app_id`
- `team: { id, name }`
- `enterprise: { id, name } | null`
- `authed_user: { id, scope, access_token, token_type }` (the user token block, `xoxp-…`, only when user scopes are requested; **may be absent** when only bot scopes are granted)

The **signing secret is NOT returned by `oauth.v2.access`** — it is a per-app constant configured in the Slack App dashboard and read from `SLACK_SIGNING_SECRET` env var at gateway startup. (The spec's edge-case language about "signing secret in the OAuth response" is a misreading of the Slack docs and is corrected here.)

**Rationale**: Source: Slack API docs for `oauth.v2.access`. Verified by hitting the endpoint manually during planning.

**Implication for the spec**: FR-012(c) is updated to clarify that "signing secret" persisted via the secret store is the **per-app signing secret** (constant, but stored once in `encrypted_secrets` so the verification path reads it from the same source as bot tokens), not a per-workspace value. The per-workspace bot/user tokens are what the OAuth response yields.

**Alternatives considered**:

- Treating the signing secret as per-workspace: rejected — Slack doesn't model it that way; we'd be inventing a fiction.
- Persisting only the bot token (skipping the user token): rejected — the user token enables future user-scoped Acts (IN-10). Store-it-now-or-prompt-later trade-off favors store-now.

---

## R3 — Slack 429 Retry-After parsing

**Decision**: Slack returns standard `Retry-After: <seconds>` for 429 responses, with the integer-seconds form. The outbound client honors `Retry-After` strictly, with an absolute cap (configurable, default 30 s per attempt and 3 attempts total).

**Rationale**: Slack docs state Retry-After is in integer seconds. Slack also publishes a Tier 1–4 budget per method; we don't pre-throttle at the client (would require sliding-window accounting per method per workspace) — instead we react to 429s. This is the same posture the existing IN-06 inbound verifier uses for the `x-slack-retry-num` header.

**Alternatives considered**:

- **Pre-throttle via token bucket per (workspace, tier)**: rejected for MVP — three sized-buckets per workspace would balloon state. Reactive is simpler and Slack's 429s are not catastrophic.
- **Retry on any 5xx**: rejected for `chat.postMessage` (idempotency unclear without a `client_msg_id` we'd have to generate). Only retry on 429 and transport-level errors.

---

## R4 — Production environment detection mechanism

**Decision**: The `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW` flag is consulted directly, with NO production-env auto-detection. The flag defaults to `0` (disabled). Production deployments set nothing; dev/test deployments explicitly set `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`. A separate `assert_prod_safety_invariants()` startup hook (new helper inside `services/webhooks/secrets.py`) inspects `os.environ.get("FYRALIS_ENV", "dev")` and, when its value is `"prod"`, fails-startup if `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW` is also set. This way prod can never accidentally enable the env-var fallback, but dev doesn't need to know the prod-env detection mechanism.

**Rationale**:

- Default-deny is the right posture for a security feature. An operator who needs the dev fallback opts in explicitly.
- The startup-fail is a belt-and-braces defense — if someone misconfigures prod's `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW=1`, the gateway refuses to boot rather than silently exposing tenant secrets via env-var fallback.
- `FYRALIS_ENV` is the existing env identifier used elsewhere in the codebase for dogfood/demo/prod separation (verified by grep during planning). Reusing it avoids a new config knob (§X).

**Alternatives considered**:

- **Auto-detect prod via hostname / IP**: too brittle.
- **Single boolean `IS_PROD`**: equivalent but requires a new env var.
- **Hard-disable the fallback entirely in this PR**: would break local-dev workflows that haven't migrated yet. Phase-gated removal is cleaner.

---

## R5 — Install / uninstall metric names and labels

**Decision**: New metrics:

- `slack_install_outcomes_total{outcome="success" | "state_invalid" | "state_expired" | "state_consumed" | "slack_oauth_error" | "installation_collision" | "secret_store_unavailable"}` — counter, labeled only by outcome.
- `slack_uninstall_outcomes_total{outcome="success" | "unknown_team" | "error"}` — counter, labeled only by outcome.
- `slack_install_duration_seconds` — histogram, no labels (one observation per OAuth callback round-trip).

`tenant_id` and `team_id` are NOT in label sets — labels are bounded cardinality. This matches IN-07's `webhook_resolver_outcomes_total` convention.

**Rationale**:

- Bounded cardinality (8 distinct values × 1 metric) keeps Prometheus storage proportional.
- The label set covers every spec acceptance scenario branch, so dashboards can be built directly from the spec.
- The duration histogram lets us track the SC-001 "30 s from install to first Observation" target indirectly (we measure the install half; the observation half is already covered by existing ingestion metrics).

**Alternatives considered**:

- **Label by `tenant_id`**: unbounded cardinality in a multi-tenant deployment — explicit anti-pattern.
- **Single counter, label by phase**: harder to graph "success vs. failure by reason" without re-labeling. Two counters is clearer.

---

## R6 — OAuth callback redirect target hosts

**Decision**: The callback handler emits **path-relative** 302 redirects: `/integrations/slack/installed?team=…` and `/integrations/slack/install-error?reason=…`. The browser resolves these against the same origin that served the callback, which is the Fyralis app's public host. No new `APP_BASE_URL` env var is introduced.

**Rationale**:

- Browsers handle relative `Location:` headers correctly per RFC 7231.
- Avoids a new config knob (§X).
- The UI routes for these paths are not in scope for this backend feature — when the UI ships them, they slot in under the same origin. Until they ship, hitting these URLs gives the standard SPA fallback (404 or app-shell), which is acceptable for the install MVP since automated tests assert on the redirect target string, not the rendered page.

**Alternatives considered**:

- **Absolute URLs from `APP_BASE_URL`**: more explicit but requires a new env var that must be kept in sync per deployment.
- **JSON response on callback**: rejected via clarification Q4.

---

## R7 — Existing webhooks router test fixtures and IN-07 wiring

**Decision**: Confirmed via repository inspection during planning — `app.state.tenant_resolver` is wired by the gateway lifespan as of IN-07 (see `services/gateway/main.py` lifespan handler and `services/webhooks/tests/conftest.py`). The router refactor in Phase 2 is therefore mechanical: replace the `from services.webhooks.tenant_resolution import resolve_tenant` import with a `request.app.state.tenant_resolver` lookup, and translate the IN-07 outcomes to HTTP via a small match statement.

**Rationale**: IN-07 already paid the wiring cost; this feature reuses it. The risk that emerged in IN-07 (forged `team_id` leaking into logs) was already mitigated there and is preserved by deferring the resolver call until after signature verification (existing pattern at `services/webhooks/router.py:217-226`).

**Alternatives considered**: None — this is observed state, not a choice.

---

## R8 — `oauth_install_states` sweep mechanism

**Decision**: Hybrid — **read-time TTL filter** as the correctness guarantee plus a **lazy lifespan-task sweep** for storage hygiene. The callback `SELECT` filters `WHERE consumed_at IS NULL AND expires_at > now()`; this is the only contract surface (a stale row that hasn't been swept is invisible to readers). Separately, a single lightweight `asyncio.Task` started by the gateway lifespan wakes every 5 min and runs `DELETE FROM oauth_install_states WHERE expires_at < now() - INTERVAL '1 hour' OR (consumed_at IS NOT NULL AND consumed_at < now() - INTERVAL '1 hour')`. The DELETE is bounded by `LIMIT 1000` per sweep so a sudden backlog cannot cause a long transaction.

**Rationale**:

- Correctness lives in the read-time filter, so the sweep can be lossy/late without breaking the security property.
- A separate worker process would be overkill for the row volume (≤100 installs/day × ~10 min TTL ≈ ~1 row at any time at GA scale, plus a margin for retries).
- The 5-min cadence matches the Greeting scheduler's polling rhythm so operational cognitive load doesn't change.

**Alternatives considered**:

- **Periodic worker process**: too heavy for this workload.
- **DB trigger / event-driven**: Postgres has no native scheduled-job primitive; pg_cron is not deployed in our topology.
- **Read-time-only with no sweep**: rows accumulate forever; eventually `oauth_install_states` becomes a dust pile. Sweep is hygiene, not correctness.

---

## R9 (added during research) — `oauth_install_states` `nonce` uniqueness scope

**Decision**: `nonce` is `UNIQUE` globally (not `UNIQUE (tenant_id, nonce)`). The nonce is generated with `secrets.token_urlsafe(32)` which gives 256 bits of entropy — collision across tenants is astronomically improbable, and a global unique constraint is simpler and gives us defense-in-depth against any future bug where `tenant_id` is mis-attributed during issuance.

**Rationale**: Cryptographic randomness makes the tenant prefix redundant; the global unique catches replay across any tenant; matches how session tokens are typically unique-scoped.

**Alternatives considered**: `UNIQUE (tenant_id, nonce)` — works but the row size for the unique index is larger, and any latent issuance bug would re-use nonces silently if the bug also got `tenant_id` wrong. Global unique is strictly safer.

---

## Summary of Constitution-relevant decisions

- R1 confirms the `SecretStore` Protocol is a §X-compliant abstraction (real second backend on the roadmap).
- R4 confirms the `WEBHOOK_SECRETS_ENV_FALLBACK_ALLOW` gating is fail-safe-prod by construction.
- R5 confirms metric label cardinality is bounded.
- R8 confirms no new worker process is introduced; the sweep is a lifespan-attached lightweight task.

No `NEEDS CLARIFICATION` remains. Proceeding to Phase 1.
