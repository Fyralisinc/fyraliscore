# Fyralis Production Readiness Master Checklist

Last reviewed from `main` on 2026-06-24.

This is the single working checklist for making Fyralis a production-grade
software product before treating BYOC, enterprise onboarding, or large customer
rollout as the main project. It consolidates the current hardening backlog,
feature-status audit, architecture docs, and operational readiness gates into a
prioritized execution plan.

This document is not a replacement for the detailed source audits. Use it as
the productization punch list, then use the referenced docs for evidence and
implementation detail:

- [Engineering hardening backlog](../hardening-backlog.md)
- [Production readiness gates](production-readiness-gates.md)
- [Data retention, backup, and recovery policy](data-retention-backup-recovery.md)
- [Feature status](../status/feature-status.md)
- [Wiring gaps](../status/wiring-gaps.md)
- [Architecture overview](../architecture/index.md)

## Readiness Definition

Fyralis is production ready when all P0 and P1 items in this document are
closed, the automated readiness gates pass on a staging environment with
production-like data volume, and the team can safely operate, upgrade, observe,
and roll back the product without manually inspecting code or databases during
normal customer operation.

Priority meanings:

- P0: blocks any production customer data.
- P1: blocks production launch, except tightly scoped internal dogfood.
- P2: blocks GA-quality maturity, but may follow a controlled pilot if tracked.
- P3: polish and long-term maintainability.

Every checklist item is done only when:

- Code is implemented.
- Tests cover the failure mode.
- Operational metric or runbook exists where relevant.
- The change is included in CI or release gates.
- The old unsafe path is removed or explicitly fails closed.

## Launch Gates

### P0 Gate - No Customer Data Until Closed

- [ ] Strict tenant isolation is enforced by both application code and database
  policy under the same DB role used in production.
- [ ] All externally reachable routes have correct authentication,
  authorization, and audit behavior.
- [ ] No raw secrets, bearer tokens, webhook signatures, prompts, source
  payloads, or customer text can leave through logs, metrics, errors, or debug
  artifacts.
- [ ] Webhook and source ingress paths verify signatures/OIDC or are explicitly
  bearer-authenticated internal paths.
- [ ] LLM, embedding, source API, HTTP rendering, and object-storage calls are
  not made inside database transactions.
- [ ] Durable queues have idempotent application, leases, heartbeat, orphan
  recovery, and dead-letter visibility.
- [ ] Production startup fails closed for unsafe env/config combinations.
- [ ] The product has a rollback path that does not delete or corrupt customer
  data.

### P1 Gate - Production Launch Quality

- [ ] Product-critical worker fabric is deployed or deliberately disabled by
  product decision.
- [ ] Access control is applied consistently to product routes and realtime
  delivery.
- [ ] CI blocks merges on tests, architecture ratchets, env contracts, schema
  drift checks, and privacy probes.
- [ ] Observability covers gateway, ingestion, reasoning, workers, database,
  queue depth, DLQs, source lag, LLM cost, and product errors.
- [ ] Staging can run a full migration rehearsal, load/soak test, release, and
  rollback rehearsal.
- [ ] Product surfaces have no fixture-backed or demo-only data paths in
  production mode.
- [ ] User-facing errors are safe, actionable, and do not expose internals.

### P2 Gate - GA Maturity

- [ ] Data retention, backup/restore, disaster recovery, and audit retention
  policies are documented and tested.
- [ ] Per-source and per-tenant scaling limits are known from load tests.
- [ ] Cost and rate-limit budgets are enforced, not merely observed.
- [ ] Admin/operator surfaces exist for all routine actions.
- [ ] Security scans, dependency updates, and release signing are part of the
  standard release process.

## 1. Security, Privacy, And Tenancy

### 1.1 Strict Tenant Isolation

Current state:

- Most domain rows carry `tenant_id`.
- `TenantContext` exists and binds `app.current_tenant`.
- RLS migrations exist, but the current policy shape has a permissive branch
  when no tenant is bound so older code paths keep working.
- CI has ingestion RLS tests under a non-superuser role.
- Shared DB startup guards now reject production startup when the connected
  role is `SUPERUSER`/`BYPASSRLS`, when tenant-scoped tables lack
  enabled/forced RLS, when policies are missing, or when policies contain the
  known unbound-tenant bypass shape. Gateway lifespan startup runs this guard
  in production, including when a pool is injected.
- Migration `0164_tenant_rls_coverage_gaps.sql` closes the remaining outright
  RLS coverage gaps on tenant-scoped tables. The remaining strict-RLS work is
  now the compatibility policy branch that permits reads/writes when
  `app.current_tenant` is unset.
- Architecture ratchets now block any post-`0164` migration from adding a new
  unbound-tenant RLS bypass (`current_tenant IS NULL` / `NULLIF(... ) IS NULL`)
  while legacy policy cleanup proceeds deliberately.
- Reasoning substrate extraction no longer hardcodes a customer internal-domain
  list or customer-name suppression rule. Internal email domains are supplied
  through `FYRALIS_INTERNAL_EMAIL_DOMAINS` or explicit test/caller input, and a
  guardrail test scans production code for legacy customer-specific tokens.

Must solve:

- [ ] Migrate production repositories and workers to `TenantContext` or an
  equivalent explicit tenant-bound transaction.
- [ ] Remove the permissive no-tenant RLS branch from production policies.
- [x] Add a startup check that fails if the application DB role is superuser or
  has `BYPASSRLS`.
- [x] Add a startup check that verifies strict RLS policy shape on all
  tenant-scoped tables.
- [ ] Add an automated test suite that tries cross-tenant reads/writes through
  gateway, workers, repositories, and realtime paths.
- [ ] Make resolver/setup paths that genuinely require cross-tenant lookup use a
  separate audited service role or explicit safe registry table, not the main
  tenant data role.
- [ ] Keep application-level `WHERE tenant_id = $1` filtering as defense in
  depth after strict RLS is active.
- [x] Remove hardcoded customer/internal-domain assumptions from production
  reasoning paths.

Acceptance evidence:

- `pytest` privacy/RLS suite passes under non-superuser role.
- Startup fails on superuser/BYPASSRLS role.
- A static policy check proves every tenant-scoped table has strict RLS.
- No production DB connection can read tenant B after binding tenant A.
- Production-code guardrail blocks legacy customer-specific tokens in
  non-test paths.

### 1.2 Authentication And Authorization

Current state:

- Bearer auth middleware validates `actor_sessions`.
- Several route families are public or prefix-bypassed for health, metrics,
  webhooks, CEO view, rendering, debug, and overlay routes.
- The access-control engine exists, but the `@requires_access` decorator is not
  broadly applied.
- Legacy substrate list endpoints (`/observations`, `/models`, `/commitments`,
  `/goals`, `/decisions`, `/resources`) now apply per-row `can_read` filtering
  and audit override reads.
- Model page and `/v1/model/*` trace surfaces now gate seed models, aggregate
  lists, relationship neighbors, synthesized neighbors, and trace chains through
  actor-scoped model `can_read` decisions.
- CEO Map `/map/*` surfaces now gate map nodes, graph edges, mixed
  neighborhoods, topology events, model stories, and story activity rows through
  actor-scoped model `can_read` decisions. `/map/refresh_projection` now
  requires tenant-scoped `admin` or `leadership` before mutating the
  tenant-wide projection cache.
- Dashboard `/dashboard/*` surfaces now gate revenue-at-risk customers,
  capacity resources, goal trees, customer details, served commitments,
  deployments, and non-apportionable aggregates through actor-scoped
  `can_read` decisions.
- Contestability `/contest/*` now gates target models through actor-scoped
  model `can_read` decisions before applying contestation mutations.
- Today `/today/*` surfaces now filter target-scoped decision deltas,
  evidence, summaries, next-card pointers, and mutation endpoints through
  actor-scoped target-entity `can_read` decisions.
- Legacy artifact drawers under `/v1/artifacts/*` now gate direct artifact
  reads through actor-scoped `can_read` decisions, restrict actor drawers to
  self/admin/leadership, audit override reads, and suppress nested drawer links
  that point at hidden substrate entities.
- Structure `/v1/structure/*` surfaces now gate commitment overlays, recent
  structure graphs, resource aggregates, resource overlays, visible-only
  deployment counts, and nested overlay evidence through actor-scoped
  `can_read` decisions.
- History `/v1/history*` surfaces now filter state-change events,
  model-derived prediction/pattern events, predictions, arcs, calibration, and
  summary counters through actor-scoped target/model/observation `can_read`
  decisions.
- Forecasts `/v1/forecasts*` surfaces now filter target-linked prediction
  lists, details, page payloads, patterns, ask context, summary counters,
  upcoming resolutions, risk exposure, accuracy bins, recent resolutions, and
  calibration summaries through actor-scoped target-entity `can_read`
  decisions. Targetless prediction rows now carry `created_by_actor_id` and
  `scope_actors` metadata and are visible only to the creator, explicit scope
  actors, or admin/leadership roles. Forecast creation now also gates any
  submitted `target_node_kind`/`target_node_id` through actor-scoped
  `can_read_by_id` before insert, preventing users from anchoring new
  predictions to hidden substrate entities.
- Startup-mounted card conversations under `/v1/cards/{card_id}/conversation`
  and `/v1/cards/{card_id}/probe` now gate the card id through actor-scoped
  model `can_read` decisions before reading, creating, clearing, or appending
  conversation exchanges.
- Generic realtime `/stream` and CEO `/view/ceo/stream` no longer accept
  query-string bearer tokens when `WEBSOCKET_QUERY_TOKEN_AUTH_ENABLED=0`;
  production settings and the env contract require that disabled value, and
  non-production settings now default to disabled unless explicitly opted in.
- Generic realtime `/stream`, CEO `/view/ceo/stream`, and CEO browser HTTP
  surfaces now fail closed when embedded without `gateway_settings` or with a
  partial settings object: query-string bearer tokens are accepted only when
  the app explicitly sets `websocket_query_token_auth_enabled=True`.
- Generic realtime `/stream`, CEO `/view/ceo/stream`, and CEO browser HTTP
  surfaces now accept the configured `WEBSOCKET_SESSION_COOKIE_NAME`
  (`fyralis_session` by default), so browser clients can use an HttpOnly session
  cookie while production query-token auth remains disabled.
- Architecture ratchets now reject first-party browser/client assets that store
  auth/session tokens in `localStorage`/`sessionStorage` or construct
  token-bearing query-string URLs.
- Core currently ships no first-party browser UI source; the production
  readiness item for localStorage migration remains open for the separate
  `fyraliscore-demo` overlay/customer UI deployment.
- Production settings and the env contract now require `GATEWAY_MOUNT_SIM=0`;
  `GATEWAY_MOUNT_SIM=1` fails gateway startup in production.
- Legacy `/v1/today` now applies row-level `can_read` checks before emitting
  recommendation cards, target entities, card evidence, supporting models,
  financial-resource metrics, recent-signal feed rows, and just-updated model
  banners. Admin/leadership override paths used by these aggregate widgets
  write `access_override_log` rows.
- Legacy `/v1/today/brand` is now restricted to tenant-scoped `admin` or
  `leadership` actors before it mutates the tenant-wide brand resource, and its
  update statement includes `tenant_id` as a defense-in-depth predicate.
- `scripts/manage_actor_roles.py` provides a production operator CLI for
  listing, granting, and revoking `actor_roles` with explicit tenant/actor
  scope validation.
- `/api/admin/dead-letters*` is classified as `admin-only` in the executable
  route inventory, requires gateway actor auth plus tenant-scoped admin role in
  route code, sanitizes listed payloads, and writes `operator_action_log` rows
  for list/retry/quarantine actions.
- Generic realtime `/stream` dispatch and replay now apply fail-closed
  `can_read_by_id` checks before queueing entity frames. Missing/unhydratable
  entities are dropped, and admin/leadership override deliveries write
  `access_override_log` rows.
- CEO-view static tenant tokens are now an explicit dev/dogfood-only path:
  production settings and the env contract require
  `VIEW_CEO_STATIC_TOKENS_ENABLED=0` and reject `VIEW_CEO_STATIC_TOKENS`.
  `/view/ceo/home` and `/view/ceo/stream` also accept normal gateway
  actor-session bearer tokens.
- `services.platform.access_control.audit.record_override_if_needed` centralizes
  override audit writes, and the architecture ratchet now fails production code
  that calls `can_read`/`can_read_by_id` without an override-audit path.
- `/v1/decision_deltas/*` now filters target-linked list rows and gates detail,
  accept, delegate, contest, add-context, and recommendation-promotion flows
  through actor-scoped target-entity/model `can_read_by_id` decisions.
  Targetless deltas remain visible only as explicit targetless rows, and
  admin/leadership override reads write `access_override_log` rows.
- `/v1/recommendations/*` now filters model-backed recommendation and
  hypothesis lists through actor-scoped model and target-entity
  `can_read_by_id` decisions. Act, dismiss, ratify, watch, unwatch, and triage
  endpoints enforce the same direct-access checks before mutating state, and
  admin/leadership override reads write `access_override_log` rows.
- `/v1/resolution_threads/*` now filters target-linked operational tracker
  lists and gates create, detail, status/step/signal updates, signal
  observations, and evaluation runs through actor-scoped target-entity
  `can_read_by_id` decisions. Targetless threads are creator-scoped, with
  admin/leadership override reads written to `access_override_log`.
- `/v1/clarifications/*` now filters clarification lists and gates answer/
  dismiss mutations through actor-scoped source observation, model, target
  object, or substrate-candidate evidence `can_read_by_id` decisions.
  Unanchored clarification rows fail closed except for audited admin/leadership
  override reads.

Must solve:

- [x] Inventory every gateway route and classify it as public, bearer-auth,
  provider-signed, internal-only, or admin-only.
- [x] Remove `/debug/*`, `/api/debug/*`, finance dev panels, Slack dev panels,
  simulation, and synthetic injection from production by default.
- [x] Replace env-name-only debug gating with explicit
  `DEBUG_ENDPOINTS_ENABLED=0` fail-closed behavior.
- [ ] Apply access-control checks to all remaining routes that read substrate
  entities: any remaining untargeted artifact/feed surfaces that need explicit
  row-level scope metadata.
- [x] Ensure realtime/WebSocket delivery applies the same `can_read` contract as
  HTTP.
- [x] Add role-management production surfaces or an admin CLI for
  `actor_roles`.
- [x] Ensure `access_override_log` records every admin/leadership override.
- [x] Cross-check `/auth/session` body `tenant_id` against `X-Tenant-Id` when
  present.
- [x] Remove WebSocket auth tokens from query strings; production disables
  query-token auth and browser-facing streams accept the configured session
  cookie instead.
- [ ] Move browser auth/session tokens out of localStorage in production
  overlays and into `HttpOnly`, `Secure`, `SameSite` cookies or customer IdP
  session infrastructure.

Acceptance evidence:

- Route inventory is committed.
- Tests show unauthorized users receive 401/403 on every sensitive surface.
- Realtime tests prove tenant/user scope filtering.
- Access override audit rows appear when override paths are used.
- Logs and HTTP access lines contain no tokens in URLs.

### 1.3 Secrets And Credential Handling

Current state:

- `encrypted_secrets` plus `FernetSecretStore` exists.
- `lib.shared.secrets.provider_contract.SecretProviderConfig` now defines the
  production secret backend and `MASTER_KEK` provider contract. The
  compatibility backend is `SECRET_STORE_BACKEND=fernet`; the wrapping key can
  be resolved from env, AWS Secrets Manager, GCP Secret Manager, or HashiCorp
  Vault through `MASTER_KEK_PROVIDER`.
- The production env contract now requires `SECRET_STORE_BACKEND` and
  `MASTER_KEK_PROVIDER` with allowed provider values.
- Production runtime validation now rejects `MASTER_KEK_PROVIDER=env`; the
  checked-in production template and env-contract gate require
  `MASTER_KEK_SECRET_REF` plus managed-provider configuration and forbid raw
  `MASTER_KEK`.
- Gateway safe-error tests now inject fake bearer/refresh tokens into exception
  messages and assert neither HTTP responses nor captured error logs expose
  them. Structured log redaction tests also cover fake emails, channel names,
  payload text, prompts, and token-like strings.
- WhatsApp webhook credentials now have `app_secret_ref`, `verify_token_ref`,
  and `access_token_ref` columns backed by `encrypted_secrets`; new debug
  registrations write refs, clear legacy plaintext columns, and route
  verification resolves refs before accepting GET challenges or POST
  signatures.
- WhatsApp local env fallbacks (`WHATSAPP_APP_SECRET`,
  `WHATSAPP_VERIFY_TOKEN`, `WHATSAPP_ALLOW_UNSIGNED`) are ignored in
  production and are forbidden by the production env-template contract.
- The production env-template contract now also requires raw provider/app
  secret placeholders such as `CODEX_API_KEY`, `AWS_SECRET_ACCESS_KEY`,
  `GITHUB_APP_PRIVATE_KEY`, source client secrets, and webhook HMAC secrets to
  stay blank in `.env.production.example`; real values must be injected by the
  runtime secret mechanism or migrated to `secret_ref`.
- Google Workspace DWD production config now requires the mounted
  `GMAIL_SERVICE_ACCOUNT_JSON_FILE` path and explicitly forbids inline
  `GMAIL_SERVICE_ACCOUNT_JSON` private-key material in the production env
  contract.
- Architecture ratchets now reject post-`0166` migrations that add new
  plaintext credential-shaped columns such as `access_token`, `app_secret`, or
  `refresh_token`; future migrations must use refs, hashes, ciphertext, or
  metadata columns.
- `scripts/migrate_whatsapp_secret_refs.py` provides an idempotent rollout tool
  to move legacy WhatsApp `app_secret`, `verify_token`, and `access_token`
  plaintext values into `encrypted_secrets`, clear the plaintext columns, and
  support dry-run/count-limited operation.
- `scripts/uninstall_whatsapp_installation.py` provides an idempotent
  operator-grade WhatsApp uninstall path that disables the installation, deletes
  secret refs, clears legacy plaintext, and writes a sanitized
  `installation_audit_log` row.
- Many production template keys are still env-shaped.
- Webhook env fallback is disabled in production by contract.

Must solve:

- [x] Define the production secret provider contract:
  `SecretStore` interface, cloud/Vault backend, and Fernet compatibility
  backend.
- [ ] Remove production dependence on plaintext app secrets in `.env` wherever
  a managed secret provider is available.
- [x] Load `MASTER_KEK` or equivalent wrapping key from KMS/Vault, not from a
  static file or committed env template.
- [ ] Rotate all source credentials and webhook secrets through a tested
  rotation flow.
- [ ] Ensure all provider tokens are stored as opaque `secret_ref` values.
- [ ] Add secret zeroization/uninstall tests for each integration.
- [ ] Confirm every source-specific client resolves secrets at call time or via
  short-lived in-memory cache with TTL.
- [x] Add a secret-leak test that scans captured logs and HTTP responses for
  known fake tokens.

Acceptance evidence:

- No real production credential is required directly in process env except
  provider references and non-secret configuration.
- Secret rotation test passes for at least one OAuth source and one HMAC
  webhook source.
- Uninstall path deletes or disables all related secret refs.

### 1.4 Log, Error, And Telemetry Privacy

Current state:

- `safe_headers` and structlog header redaction exist.
- Route-template HTTP metrics avoid raw path labels.
- Prometheus label policy forbids tenant IDs in metrics.
- `DEBUG_ARTIFACT_CAPTURE=0` is required by the production env contract, and
  Think debug capture now refuses to write `think_run_artifacts` in production
  even if an unsafe override is accidentally present.
- Gateway uncaught exceptions are converted to safe JSON 500 responses
  containing only `error=internal_server_error` plus the request ID; focused
  tests assert stack traces, raw SQL, and secret-looking text do not reach the
  client.
- Structlog redaction now covers header bags, nested JSON-body-shaped fields,
  token/secret/signature/private-key keys, OAuth codes, bank fields, emails,
  prompts, source payloads, channel/source-channel names, and secret-looking
  substrings while preserving safe numeric usage fields such as LLM token
  counters.
- Gateway `HTTPException` responses are sanitized before returning 4xx/5xx
  bodies, including prompt/body-shaped detail objects and token-like strings.
- Gateway request-validation failures now use a dedicated safe 422 handler that
  returns only validation locations/types plus the request ID, never the raw
  submitted body or PII-bearing invalid input.
- `think_run_artifacts` now has a default-enabled housekeeper retention job
  with env-controlled TTL, batch size, dry-run mode, schedule interval, and
  bounded Prometheus metrics. Partitioning and retention coverage for other
  debug-heavy tables still remain open.
- Shared Prometheus families reject forbidden label names, unsafe label values,
  and opt-in per-family label value allowlists now reject undeclared free-form
  values.
- Core gateway-owned HTML debug pages now render through a trusted static HTML
  response helper with nonce-based Content-Security-Policy, no-store caching,
  no-sniff, frame denial, no-referrer, and permissions-policy headers.

Must solve:

- [x] Extend redaction from headers to JSON bodies and structured log context
  keys that commonly hold tokens, private keys, OAuth codes, signatures, bank
  fields, emails, and PII.
- [x] Add tests proving user-facing 4xx/5xx responses never include stack
  traces, raw provider responses, raw prompts, SQL values, or secrets.
- [x] Make `RequestContextMiddleware` preserve the original exception even if
  logging fails.
- [x] Disable debug artifact capture in production by default and fail closed on
  unsafe production overrides.
- [ ] Partition and expire `think_run_artifacts` and other debug-heavy tables.
- [x] Build a telemetry allowlist for exported metrics and reject free-form
  labels.
- [x] Add a PII egress test that tries to emit user emails, channel names,
  payload text, and tokens through logs/metrics and verifies rejection or
  redaction.

Acceptance evidence:

- Captured log sample contains no bearer tokens, webhook signatures, OAuth
  tokens, private keys, raw source text, or raw prompts.
- Production env contract sets `DEBUG_ARTIFACT_CAPTURE=0`.
- Metrics linter fails if forbidden labels are added.

## 2. Data Integrity And Persistence

### 2.1 Schema, Migration, And Drift Safety

Current state:

- Migration prefix uniqueness is checked in CI and by
  `scripts/check_architecture_ratchets.py`, which rejects malformed migration
  names and duplicate four-digit prefixes.
- `scripts/check_schema_drift.py` exists and is now run in the CI test job
  after applying migrations with `scripts/apply_db_migrations.py`.
- The release-promotion readiness harness includes
  `schema_drift_migration_rehearsal`; it passes against a configured
  `DATABASE_URL` and becomes a manual gate when the staging clone DSN is absent.
- `customer_commitments.revenue_at_risk_usd` now has an explicit
  `NUMERIC(14,2)` migration and schema-drift precision check so cent values are
  preserved.
- `models.status` and `models.archive_reason` now have database CHECK
  constraints matching the bounded application literals; schema drift verifies
  those constraint names are present.
- Schema drift now verifies extensions, expected indexes, partitioned parents,
  selected CHECK constraints, numeric precision/scale, and enabled + forced RLS
  for every live tenant-scoped table, including tables not yet captured in the
  hand-authored schema lock.
- `schema_drift_monitor` now runs the same drift/RLS check continuously in the
  production stack, exposes bounded Prometheus metrics
  (`schema_drift_check_status`, `schema_drift_findings`), and feeds the
  `SchemaRLSDriftDetected` Grafana alert without exporting table/column names
  as labels.
- [Migration release runbook](migration-release-runbook.md) now defines the
  expand/contract policy, release artifacts, rollback versus forward-fix
  decision tree, and backup/snapshot evidence required for destructive
  migrations.
- `scripts/check_architecture_ratchets.py` now rejects new destructive SQL
  migrations unless they carry a `destructive-migration-approved:` marker with
  backup, rollback, and owner evidence. Historical destructive migrations are
  explicitly baselined.
- Migrations are mostly idempotent, but staging-clone rollback rehearsals are
  not yet a routine gate.

Must solve:

- [x] Keep migration prefix uniqueness in CI.
- [x] Run schema drift checks in CI and release promotion.
- [x] Define expand/contract migration policy:
  - add before read,
  - dual-write or compatibility read,
  - remove only after one release boundary.
- [x] Add migration rollback or forward-fix runbooks for every release.
- [ ] Rehearse migrations against a staging clone before release.
- [x] Require backup/snapshot verification before destructive migrations.
- [x] Add constraints for app-enforced invariants that matter for corruption
  prevention, including archive reasons and money precision.
- [x] Fix `revenue_at_risk_usd` precision so cents are preserved.
- [x] Add schema checks for RLS, indexes, partitions, and extensions.
- [x] Add continuous production alerting for schema/RLS drift.

Acceptance evidence:

- Migration rehearsal report attached to each release.
- `check_schema_drift.py` passes after migrations.
- `schema_drift_monitor` is present in the production process manifest, compose
  stack, Prometheus scrape config, and Grafana alert rules.
- Money precision test preserves decimal cents.
- Constraint tests reject invalid archive/status values where appropriate.

### 2.2 Queue And Idempotency Correctness

Current state:

- Durable queues exist for Think and post-commit.
- Think trigger queue polling uses `FOR UPDATE SKIP LOCKED`, stale-lock
  timeout reclamation, per-trigger heartbeat extension, and startup recovery
  for orphaned locks.
- `applied_triggers` now uses an atomic `INSERT ... ON CONFLICT DO NOTHING
  RETURNING` claim, with concurrent same-trigger coverage.
- Think worker metrics now expose aggregate stale-lock and retry-exhaustion
  signals without tenant, trigger, error, or payload labels.
- Post-commit enqueue now treats `(tenant, trigger, action_kind)` as a
  historical idempotency key, and migrations include the tenant-scoped pending
  index for per-tenant workers.
- `lib.shared.backoff` now owns the internal exponential retry math used by
  Think, post-commit, ingestion workflow helpers, the observation writer, and
  Discord gateway retry loops.
- Source idempotency constructors now cover all composed source keys found in
  the audited handlers, and adopted-verbatim keys are explicitly registered in
  `ADOPTED_VERBATIM_KEYS`.
- Admin-only DLQ operator APIs now cover post-commit actions, model re-eval
  sidecars, and exhausted Think triggers at
  `/api/admin/dead-letters`, with sanitized list output, retry, quarantine,
  and durable `operator_action_log` audit rows.
- Exhausted Think trigger rows now persist `last_error`, while successful
  completion clears it so the trigger queue itself can serve as the trigger
  dead-letter record without a side table.

Must solve:

- [x] Replace `applied_triggers` SELECT-then-INSERT with atomic
  `INSERT ... ON CONFLICT ... RETURNING` behavior.
- [x] Add concurrent idempotency test with many workers processing the same
  trigger.
- [x] Add lease timeout and heartbeat for `think_trigger_queue`.
- [x] Recover orphaned `think_trigger_queue` locks on worker startup.
- [x] Add alerting for stale locks and retry exhaustion.
- [x] Add `tenant_id` to hot post-commit pending indexes.
- [x] Fix post-commit dedup race between insert and processed update.
- [x] Standardize backoff formulas across Think, post-commit, ingestion, and
  source workflows.
- [x] Ensure every source dedup key is defined in the central idempotency module
  or explicitly documented as adopted-verbatim.
- [x] Add DLQ admin read/list/retry/quarantine surfaces with audit.

Acceptance evidence:

- Worker kill/restart test proves orphan recovery.
- Concurrent trigger test produces one applied row and no false dead letters.
- Post-commit duplicate dispatch test fails before fix and passes after fix.
- DLQ counts appear in local metrics and operator view.
- DLQ operator tests prove non-admin denial, sanitized listing, post-commit
  retry, model re-eval retry, trigger quarantine, and audit writes.

### 2.3 Data Retention, Backup, And Recovery

Current state:

- `think_run_artifacts` retention is enforced by the housekeeper worker through
  `think_run_artifact_retention`, defaulting to a 30-day TTL and 5,000-row
  delete batches (`THINK_RUN_ARTIFACT_RETENTION_DAYS`,
  `THINK_RUN_ARTIFACT_RETENTION_BATCH_SIZE`,
  `THINK_RUN_ARTIFACT_RETENTION_DRY_RUN`,
  `HOUSEKEEPER_THINK_ARTIFACT_RETENTION_INTERVAL_S`).
- The retention delete is bounded and tested against Postgres so large debug
  artifact tables cannot be removed in one unbounded transaction.
- Retention metrics expose only bounded labels:
  `housekeeper_retention_rows_total`,
  `housekeeper_retention_eligible_rows`, and
  `housekeeper_retention_last_run_timestamp_seconds`.
- Backup and restore automation can now report into the deployment-global
  `backup_recovery_status` contract through
  `scripts/record_backup_recovery_status.py`; the default-enabled housekeeper
  `backup_recovery_metrics` job exports freshness, attempt, age, and
  `fresh|stale|missing|failed` gauges with bounded labels.
- Full retention policy, partitioning, backup automation, and restore rehearsal
  are tracked in
  [data-retention-backup-recovery.md](data-retention-backup-recovery.md);
  backup automation and restore rehearsal remain open.

Must solve:

- [x] Define retention policy for observations, raw payloads, blobs/chunks,
  debug artifacts, audit events, telemetry, source install audit, and logs.
- [ ] Partition high-volume append-only tables by time where needed.
- [ ] Implement automated backup for Postgres and object storage.
- [ ] Test point-in-time restore.
- [ ] Test object-store restore or replication recovery.
- [x] Document restore order: database, object raw tier, broker offsets/state,
  secrets, application config.
- [x] Add backup freshness and restore-test metrics.
- [ ] Ensure rollback never deletes customer data.

Acceptance evidence:

- Restore rehearsal succeeds in staging.
- Backup freshness alert exists and is backed by
  `backup_recovery_health_status`.
- Retention jobs are observable and have dry-run mode.

## 3. Runtime Reliability

### 3.1 Transaction Scope And Pool Safety

Current state:

- Model insertion now precomputes missing embeddings before opening
  repo-owned transactions. Callers that pass an already-transactional
  connection must provide a precomputed embedding; otherwise `ModelsRepo`
  fails before calling the embedder.
- Inferential Think triggers now default to the split transaction shape:
  retrieval/planning and LLM reasoning run outside an explicit DB transaction,
  then validation, apply, cascade, and post-commit enqueue run inside the short
  mutation transaction. `THINK_NARROW_INFERENTIAL_TX=1` is enforced by the
  production env contract; setting it to `0` is reserved for emergency rollback.
- Think debug artifact capture now defers payloads produced inside the mutation
  transaction and flushes them on a fresh connection after commit. Production
  still forces `DEBUG_ARTIFACT_CAPTURE=0`.
- DB pool metrics exist.
- DB pool saturation is alertable through scrape-time `db_pool_*` gauges and
  `db_pool_acquire_wait_seconds`; long transaction age is now alertable through
  housekeeper-refreshed `db_longest_transaction_age_seconds` and
  `db_long_transactions_over_threshold`.
- pgbouncer-compatible pool wiring now covers gateway, Think, post-commit,
  housekeeper, maintenance workers, source gateways, and source schedulers via
  `lib.shared.db.asyncpg_pool_runtime_kwargs`. `POSTGRES_PGBOUNCER_COMPATIBLE`
  enables global transaction-pooling compatibility, with process-specific
  overrides available for mixed routing.
- Gateway/shared/vector/ingestion/extension worker pools now apply
  `statement_timeout`, `lock_timeout`, and
  `idle_in_transaction_session_timeout` on connection initialization via
  `lib.shared.db.configure_connection_timeouts`; production env templates and
  contract checks require positive millisecond values.
- Production pool budgets are explicit positive env-contract values for the
  gateway, Think, post-commit, housekeeper, maintenance workers, source
  gateways, source schedulers, ingestion writers/workers, and extension workers.
- CI architecture ratchets now include a conservative AST check that rejects
  direct HTTP, object-storage, LLM/rendering, and embedding calls placed
  lexically inside `conn.transaction()` or `tenant_transaction()` blocks.

Must solve:

- [x] Split inferential Think into retrieval/planning outside the mutation
  transaction, LLM call outside the mutation transaction, and short
  validation/apply transaction.
- [x] Precompute model embeddings outside insert transactions.
- [x] Move debug artifact writes outside the apply transaction where possible.
- [x] Add static check that rejects known network calls inside
  `conn.transaction()` or `transaction()` scopes.
- [x] Add statement timeout, lock timeout, and idle-in-transaction timeout for
  production roles.
- [x] Define pool budgets per process type and enforce via env/config contract.
- [x] Make gateway and worker pools pgbouncer-compatible where routed through
  transaction pooling.
- [x] Alert on DB pool saturation and long transaction age.

Acceptance evidence:

- Static no-network-in-transaction test passes.
- Production launcher pgbouncer wiring ratchet passes.
- Load test with delayed LLM/Ollama does not exhaust DB pool.
- DB p95 acquire wait remains under target during staging soak.

### 3.2 Horizontal Scaling And Leader Election

Current state:

- Live source workers are either singleton-protected (Discord/Telegram/Signal
  Redis leader leases) or process work through row/record leasing. Realtime
  dispatchers are safe to run per gateway replica because they only fan out
  Postgres notifications to clients connected to that process.
- Gateway greeting scheduler background loops now run behind a pgbouncer-safe
  Postgres row lease in `scheduler_leases`. Standby gateway replicas keep
  retrying, the active holder refreshes TTL, and tests cover single-leader and
  stop/handoff behavior.
- CEO cache refreshes now acquire a per-tenant `scheduler_leases` row lease
  before composing/rendering cache payloads. This avoids duplicate refreshes
  across gateway replicas without holding a DB transaction through rendering.
- OAuth install-state sweeping now runs behind `gateway:oauth_state_sweeper`
  row lease, and maintenance/housekeeper jobs use pgbouncer-safe per-job row
  leases with TTL refresh instead of session advisory locks.
- Production runtime manifest tests now reject horizontally scalable Compose
  services that pin `container_name`; singleton services remain explicit in
  `services.platform.runtime.process_manifest`.
- Multi-replica regression coverage now pins scheduler leader handoff, Think
  trigger/model-reeval queue splitting, post-commit `SKIP LOCKED` dispatch,
  and Gmail/Google Calendar/Google Drive source live leases.
- Maintenance jobs cancel the protected job body when row-lease refresh is
  lost, and Signal/Telegram stateful gateway launchers now return a transient
  exit code on Redis lease loss and release held leases during cleanup.

Must solve:

- [x] Add leader election for `GreetingScheduler`.
- [x] Add per-tenant row lease around CEO cache refresh to avoid duplicate
  refresh during failover.
- [x] Ensure OAuth sweeper, realtime dispatcher side effects, maintenance jobs,
  and source live workers are either safe to run N times or protected by leases.
- [x] Add multi-replica tests for scheduler, Think worker, post-commit worker,
  and source live workers.
- [x] Ensure all leases have TTL, refresh, loss detection, and graceful
  handoff.
- [x] Make deployment manifests scale all stateless services horizontally while
  keeping singleton jobs explicit.

Acceptance evidence:

- Two gateway replicas produce one scheduler leader.
- Killing the leader transfers leadership within the expected TTL.
- Cache refresh count does not multiply by replica count.

### 3.3 Dependency Resilience

Current state:

- LLM provider has error classification, retry policy, and circuit breaker seam.
- Ingestion workflows have retry helpers and source rate limiter.
- Rendering/query HTTP adapters use shared retry classification with
  exponential backoff and jitter for transient `httpx` failures and 408/409/425
  /429/5xx responses.
- Rendering/query HTTP adapters now lazily reuse one owned long-lived
  `httpx.AsyncClient` per adapter instance when a test/special client is not
  injected, and gateway CEO-view shutdown closes the owned greeting/query
  rendering clients.
- Rendering/query HTTP adapters now raise structured
  `DependencyUnavailableError` after retry exhaustion. Ask maps rendering
  dependency exhaustion to HTTP 503 with a non-sensitive structured body.
- Rendering/query HTTP adapters now run retried calls through a shared async
  circuit breaker. Retry-exhausted dependency failures count toward the breaker,
  permanent caller faults such as non-retryable 4xx responses do not, and open
  breakers fast-fail as structured `DependencyUnavailableError` responses with
  `circuit_open=true`. Breaker state is exported via the shared Prometheus
  metrics registry.
- SDK-backed LLM transports now log `llm.usage_missing` and record
  conservative estimated nonzero token usage when provider responses omit
  usage fields, instead of silently writing zero-cost calls.
- Think worker daily LLM budget enforcement now supports per-tenant spend,
  token, and request ceilings. Exhausted tenants are unlocked and deferred to
  the next UTC day without incrementing attempts, running Think, or
  dead-lettering.
- Embedding backends now combine their existing retry/backoff behavior with
  backend-level batch fan-out semaphores (`OLLAMA_EMBED_MAX_CONCURRENCY`,
  `OPENAI_EMBED_MAX_CONCURRENT_REQUESTS`), so batch callers cannot bypass the
  live embedding worker's concurrency budget.
- Embedding backends now run retried backend calls through the shared async
  circuit breaker. Ollama 5xx/unreachable failures and OpenAI 429/5xx/
  unreachable failures count toward the breaker, non-retryable 4xx caller
  faults do not, and open breakers fail fast through the existing embedder
  error hierarchy with `circuit_open=true`.
- Source API errors now share a default recoverability classifier: rate-limit
  codes, HTTP 429, upstream 5xx, and transport failures are recoverable, while
  auth/object/caller 4xx failures remain permanent unless a source explicitly
  overrides the outcome. Slack's client now maps all HTTP failures to
  `SlackApiError` instead of leaking raw `httpx.HTTPStatusError`.
- Source API clients constructed through the shared fetcher/reconciler builder
  now receive a per-source async circuit-breaker proxy around public async read
  methods. Recoverable source errors count toward breaker state; permanent 4xx
  failures do not; an open breaker fast-fails before calling the upstream
  client.
- ShardFetch declares `REDIS_URL`, `SHARD_FETCH_RATE_LIMIT=1`, and
  `SHARD_FETCH_RATE_LIMIT_MAX_WAIT_SEC` in the production env contract, so
  primary source page fetches consume token-bucket capacity before upstream
  calls. Integration clients expose bounded 429/`Retry-After` budgets in
  `.env.production.example`, and an audit test fails when a client adds an
  undocumented `_RL_` budget env.

Must solve:

- [x] Add retry with exponential backoff and jitter to rendering/query HTTP
  adapters.
- [x] Reuse long-lived `httpx.AsyncClient` instances instead of per-call
  clients.
- [x] Raise structured unavailable errors after retries instead of silently
  returning placeholders.
- [x] Add circuit breakers for rendering, source APIs, embeddings, and LLMs
  where an outage can cascade.
- [x] Add per-tenant LLM token, spend, and request ceilings.
- [x] Make missing token-usage fields loud in cost tracking.
- [x] Add shared embedding backoff/semaphore behavior so batch embedding does
  not thundering-herd Ollama or the selected embedder.
- [x] Ensure each source API client classifies recoverable vs permanent errors.

Acceptance evidence:

- respx/httpx tests cover transient 5xx, timeout, rate limit, and permanent
  errors.
- Cost budget exhaustion returns controlled degraded behavior.
- Circuit breaker open state appears in metrics.

## 4. Ingestion And Integration Readiness

### 4.1 Source Ingress Security

Must solve:

- [x] Verify every webhook source has signature/OIDC negative tests.
- [x] Ensure `/ingest/{channel}` remains bearer-authenticated internal/dev
  intake and is not used as public provider ingress.
- [x] Keep env-var webhook secret fallback disabled in production.
- [x] Ensure provider tenant resolution does not reveal install existence
  across tenants.
- [x] Ensure replay/dedup behavior is source-specific and tested for retries.
- [x] Ensure all public webhook responses follow provider retry semantics
  without leaking internals.

Acceptance evidence:

- Contract tests for Slack, GitHub, Discord, Gmail Pub/Sub, Google push,
  WhatsApp, and finance webhooks as applicable.
- Failed signature attempts increment metrics and do not write observations.

### 4.2 Full Pipeline And Source Isolation

Current state:

- Kafka-first ingestion and per-source topic isolation are documented and mostly
  implemented.
- Circuit breaker trips tenant back to inline path on sustained lag.
- The shared OAuth refresh core now covers every dedicated source table that
  stores OAuth refresh-token material: QuickBooks, Gusto, and LinkedIn. Ramp and
  Carta are explicitly modeled as client-credentials re-mint flows rather than
  refresh-token grants. LinkedIn refresh-on-401 is wired through the fetch
  client, persists returned access/refresh refs, and is pinned by unit and
  provider-contract fixture tests.
- Gmail, Google Calendar, and Google Drive DWD install routers expose explicit
  preflight endpoints that impersonate the admin against Directory APIs,
  enumerate selectable users/groups/org units, and return the exact service
  account client ID plus OAuth scopes required in Workspace Admin Console when
  the grant is missing or mis-scoped.

Must solve:

- [ ] Run full pipeline in staging with all production source families enabled.
- [ ] Validate one noisy source cannot block another source.
- [x] Verify producer/consumer topic provisioning on every deployment.
- [x] Verify partition count used by traffic-signal metrics matches broker
  topic partition count.
- [x] Add per-source worker scaling runbooks.
- [x] Test circuit breaker trip and re-enable paths against a live broker.
- [x] Add raw-tier object retention and integrity checks.
- [x] Add DLQ replay/quarantine workflow.

Acceptance evidence:

- Source-isolation load report with lag and throughput per source.
- Circuit breaker smoke test passes on real broker.
- Per-source dashboards show raw, normalized, written, embedded, and DLQ rates.

### 4.3 Integration Install And Lifecycle

Current state:

- Generic `provider_installations` lifecycle operations are covered by
  `scripts/manage_source_installations.py`.
- Dedicated OAuth/admin-paste/API-key install tables for QuickBooks, Gusto,
  Ramp, Carta, LinkedIn, Jira, Mercury, Brex, Deel, Fireflies, Miro, Grafana,
  Figma, HiBob, Ashby, AWS, Telegram, and Signal are now covered by
  `scripts/manage_dedicated_source_installations.py` for status, pause, resume,
  credential rotation, and uninstall. The tool binds `app.current_tenant`,
  writes bounded operator audit rows, zeroizes access/refresh/webhook secret
  refs and live/backfill session refs on uninstall, handles AWS account/region
  selectors, and disables matching webhook resolver rows for webhook-capable
  sources.
- Architecture ratchets now block new plaintext credential columns in
  migrations and block production integration code from passing raw
  credential-looking variables into `secret_ref`/`*_secret_ref` install
  parameters.
- Admin-paste install routers now have explicit bad-credential write-order
  coverage for QuickBooks, Jira, Mercury, Brex, Deel, Fireflies, Gusto, Ramp,
  Miro, Figma, Carta, and LinkedIn: invalid credentials return a structured
  400 and leave no install rows, `encrypted_secrets`, onboarding triggers, or
  webhook resolver rows behind.

Must solve:

- [ ] Every production source has a production install, status, pause, resume,
  uninstall, and credential rotation path.
- [ ] Install flows verify credentials before writing install rows.
- [x] Install flows store credentials as secret refs only.
- [x] OAuth refresh worker covers every OAuth source with refresh tokens.
- [x] DWD/service-account sources have explicit admin preflight and scope
  display.
- [x] Dedicated finance/people OAuth source tables have operator lifecycle and
  credential-zeroization coverage.
- [x] Dedicated single-scope API-key source tables have operator lifecycle and
  credential-zeroization coverage.
- [x] Dedicated AWS and Telegram/Signal session source tables have operator
  lifecycle and credential-zeroization coverage.
- [x] Source onboarding progress events are emitted once per state transition.
- [ ] Uninstall stops watches/subscriptions, disables install rows, and removes
  or disables secrets. Generic `provider_installations` uninstall now disables
  rows and removes the generic `secret_ref`; dedicated QuickBooks, Gusto, Ramp,
  Carta, LinkedIn, Jira, Mercury, Brex, Deel, Fireflies, Miro, Grafana, Figma,
  HiBob, Ashby, AWS, Telegram, and Signal uninstall now disables source install
  rows and clears per-install secret refs. External provider webhook
  deregistration/watch teardown remains open.
- [x] Customer/admin UI or CLI exposes install health and last successful sync.

Acceptance evidence:

- End-to-end install/uninstall tests for each production source family.
- Status endpoint or CLI reports source health without raw payload leakage.

## 5. Worker Fabric And Background Jobs

Current state:

- Feature-status docs show several workers resolved through housekeeper, but
  anomaly processing, entity resolution, and some expensive jobs still need
  deliberate deployment decisions.
- The runtime process manifest lists production gateway/workers with compose
  service names and healthcheck metadata; the manifest test now requires every
  production process with a compose service to declare a healthcheck, and the
  gateway compose service probes `/healthz` directly.
- Post-commit realtime/metric/anomaly-published actions now emit transactional
  `view_ceo_refresh` NOTIFYs that the CEO-view scheduler consumes, with the
  existing poll loop retained as a fallback for missed notifications.
- Post-commit prediction scheduling now validates that payloads include
  `evaluate_at` and emits the same transactional CEO-view refresh notification
  for product surfaces.
- Housekeeper now schedules `access_matview_refresh` daily so
  `actor_visible_*` access-control materialized views refresh in the production
  worker fabric.
- `anomaly_processor_worker` is now a first-class production worker with script
  launcher, compose service, healthcheck, Prometheus scrape target, runtime
  manifest entry, and metrics for detected/enqueued/debounced/subthreshold
  anomaly work.
- `entity_resolver_worker` is now a first-class production worker with bounded
  polling, LLM budget controls, script launcher, compose service, healthcheck,
  Prometheus scrape target, runtime manifest entry, and terminal phrase cleanup
  so completed unresolved phrases are not reprocessed indefinitely.
- Worker liveness is now manifest-backed: every production compose process has
  a healthcheck and Prometheus scrape target, while the `WorkerHeartbeatStale`
  alert uses the shared `fyralis:worker_heartbeat_age_seconds` recording rule
  that merges ingestion and worker heartbeat metrics.
- Worker launch scope is now explicit in
  `services.platform.runtime.worker_launch_policy`: every `services/workers/*`
  package is classified as a production process, a default housekeeper job, a
  flag-gated housekeeper job, or dogfood-only. Tests assert all worker packages
  are classified and that flag-gated jobs are disabled by default.
- The [worker fabric runbook](worker-fabric-runbook.md) documents selected
  workers, gated jobs, health/metrics checks, restart/scale guidance, and
  release verification commands.

Must solve:

- [x] Decide which workers are product-critical for launch:
  anomaly processor, entity resolver, precipitation, topology sweeper,
  housekeeper, deadline resolver, calibration updater, edge drift, maintenance.
- [x] Deploy every selected worker as a first-class process with health,
  metrics, config, and runbook.
- [x] Explicitly disable and document every non-selected worker so dormant
  tables/features do not look production-ready by accident.
- [x] Wire entity resolver if unresolved phrases materially affect product
  quality.
- [x] Wire anomaly processor or remove anomaly promises from product surfaces.
- [x] Ensure actor-visible materialized views refresh on schedule if access
  control depends on them.
- [x] Ensure remaining post-commit dispatchers are real where product depends
  on them.
- [x] Add worker heartbeat and stale heartbeat alerts for every long-running
  process.

Acceptance evidence:

- Runtime process manifest lists every production worker.
- `/healthz` and `/metrics` exist for every worker.
- Feature-status page no longer contains high-severity "implemented but
  not-wired" items for launch-critical behavior.

## 6. Product Readiness

### 6.1 Remove Demo, Fixture, And Mock Behavior From Production

Current state:

- Greeting close-line calibration no longer uses a hardcoded demo percentage:
  `SnapshotComposer` reads tenant-level aggregate calibration from
  `calibration_stats`, carries sample count in the snapshot, and the rendering
  adapter reports an explicit `calibration warming` state when no resolved
  samples exist.
- Gateway extension routers and public auth-bypass prefixes are skipped in
  production unless the extension explicitly sets `production_enabled=True`,
  preventing installed demo/simulation overlays from mounting by accident.
- Legacy substrate list endpoints now report `source=substrate` and
  `stub=false`; a gateway ratchet test fails if production router code returns
  `stub=true`.
- Spec seed routes are isolated to `spec_routes.py` and remain unmounted in
  production through `SPEC_DEMO_ROUTES_ENABLED=0`; a gateway ratchet fails if
  `/v1/spec/*` seed endpoints appear outside that demo router.
- CEO Map snapshots now expose `degraded_reasons` such as
  `no_visible_models`, `projection_warming`, and `topology_warming`; model
  overview sparse-state coverage verifies empty categories remain renderable.
- `scripts/audit_gateway_route_access.py --production --check` now audits the
  production-mounted route inventory plus source-level stub/spec leak
  invariants; the current production inventory passes with 112 routes.
- Ask and legacy CEO-query routes now reject default tenant/viewer fallbacks
  and raw `X-Tenant-Id` / `X-Actor-Id` identity headers in production unless
  `request.state.auth` has been populated by gateway actor-session auth.
  `DEFAULT_ACTOR_ID`, `DEFAULT_TENANT_ID`, and `COMPANY_OS_TENANT_ID` are all
  forbidden by production runtime settings and the env-template contract.

Must solve:

- [x] Audit every production route for in-code seed payloads, fixture-backed
  responses, mock rendering, in-memory cache, and demo-overlay assumptions.
- [x] Ensure `GRT_RENDERING_BASE_URL`, `QUERY_RENDERING_BASE_URL`, and
  `QUERY_CACHE_BACKEND=pg` fail closed in production.
- [x] Keep `/v1/spec/*` seed-payload routes unmounted in production by default
  with `SPEC_DEMO_ROUTES_ENABLED=0` enforced by runtime settings and the
  production environment contract.
- [x] Replace spec/v2 surfaces that return seed payloads with substrate-backed
  responses or hide them from production.
- [x] Ensure CEO Map and model surfaces degrade transparently when topology data
  is unavailable, or wire the topology pipeline required to populate them.
- [x] Ensure simulation and synthetic injection cannot run in production.

Acceptance evidence:

- Production-mode test suite boots the gateway and asserts no demo/mock/sim
  routes are mounted.
- Product route tests verify data comes from tenant-scoped substrate rows.

### 6.2 Core User Workflows

Current state:

- Decision-delta review actions now write tenant/actor-scoped
  `product_action_audit_log` rows for accept, delegate, contest, add-context,
  and recommendation-promotion actions. Audit metadata is bounded operational
  context only: status transitions, target IDs, request IDs, counts, and
  related object IDs; raw notes and contest reasons remain out of the audit
  metadata. Migration `0168_product_action_audit_log.sql` enables forced RLS and
  the schema-drift lock tracks the table, indexes, and bounded action check.
- Recommendation review actions now use the same audit table for act, dismiss,
  ratify, watch, unwatch, and triage flows. Migration
  `0169_recommendation_product_action_audit.sql` widens the bounded action set;
  route tests assert raw notes, reasons, predicates, and explanations are not
  copied into audit metadata.
- Clarification answer/dismiss actions now use the same audit table for user
  decisions over autonomous clarification prompts. Migration
  `0177_clarification_product_action_audit.sql` widens the bounded action set;
  route tests assert raw answer notes and dismissal reasons are not copied into
  audit metadata.

Must solve:

- [x] Define the production-critical workflows:
  - user login/session,
  - source install,
  - first successful backfill,
  - Today/CEO view,
  - Ask/query,
  - model detail/map,
  - recommendation or decision-delta action,
  - forecast/prediction review if exposed,
  - source pause/uninstall,
  - admin role change.
- [ ] Add end-to-end tests for those workflows with real Postgres and realistic
  source fixtures.
- [x] Add latency targets and error budgets per workflow.
- [ ] Make degraded states explicit in UI/API responses.
- [ ] Ensure user-facing copy does not expose implementation details.
- [x] Add audit trails for user actions that mutate or accept autonomous output.

Acceptance evidence:

- Product E2E report covers all launch workflows.
- SLO table exists for every launch workflow.
- Customer-facing errors include request ID and safe remediation text.

### 6.3 Frontend And Overlay Readiness

This repo is backend-only, but Fyralis as a product is not production ready
unless the overlay UI is also ready.

Core note: backend-owned debug HTML pages are now nonce-CSP hardened and tested.
Backend LLM-rendered HTML fragments now pass through an allowlist sanitizer in
`RenderingService` before returning `body_html`, `response_html`,
`reasoning_html`, or evidence `body_html` to the overlay. The remaining items
below are for the separate production overlay UI.

Must solve in the overlay repo:

- [ ] Typecheck with strict shared API contracts.
- [ ] Remove auth tokens from URL query strings and localStorage.
- [x] Sanitize backend-rendered HTML fragments before returning them to the
  overlay.
- [ ] Add overlay-side defense-in-depth sanitization if the overlay renders any
  HTML not supplied by the backend rendering service.
- [ ] Fix polling races and stale response overwrites.
- [ ] Add accessibility checks for interactive elements.
- [ ] Add Playwright or equivalent workflow tests against the production-mode
  backend.
- [ ] Add error and empty-state coverage for source onboarding, Today, Ask, and
  admin flows.

Acceptance evidence:

- Overlay CI is required for product release.
- Cross-repo contract tests pass against `contracts/http-routes.json` or an
  updated OpenAPI/schema artifact.

## 7. Observability And Operations

### 7.1 Metrics, Dashboards, And Alerts

Current state:

- Prometheus/Grafana stack exists.
- Worker health and many metrics are implemented.
- Docker Compose healthchecks now cover the gateway and every production
  process listed in `services.platform.runtime.process_manifest`.
- Prometheus scrape coverage is now manifest-backed: every production process
  with a compose service must have a target in
  `observability/prometheus/prometheus.yml` with a stable `worker` label.
- Grafana alert provisioning is tested as code for worker heartbeat/scrape,
  infra scrape, DLQ depth, Kafka lag, webhook signature spikes, embedding
  failure ratio, Think queue/stale-lock/retry exhaustion, DB pool saturation,
  backup/restore health, and LLM spend burn.
- Admin-only dead-letter APIs and the
  [durable dead-letter admin runbook](dead-letter-admin-runbook.md) now cover
  post-commit, model re-eval, and Think trigger list/retry/quarantine actions.
  Ingestion DLQ replay/quarantine remains covered separately by
  [ingestion-dlq-replay-quarantine-runbook.md](ingestion-dlq-replay-quarantine-runbook.md).
- Shared Prometheus `Counter`/`Gauge`/`Histogram` families now reject
  forbidden label names (`tenant_id`, raw IDs, emails, URLs, paths, prompts,
  payloads, queries) and unsafe label values (UUIDs, emails, URLs/query
  strings, control characters, secret-looking values, and oversized strings).
  The architecture ratchet also fails static metric declarations that introduce
  forbidden label names.
- Product workflow SLO burn recording rules now aggregate user-facing gateway
  routes only (Today, Ask, recommendations, forecasts, model/map, CEO view,
  decision/review surfaces) into request rate, 5xx ratio, p95 latency, and
  normalized burn gauges. `ProductSLOBurnHigh` alerts when 5xx or p95 burn is
  materially above first-pass launch budgets.
- Gateway middleware now emits bounded `product_workflow_requests_total` and
  `product_workflow_request_duration_seconds` families by workflow
  (`today`, `ask`, `recommendations`, `forecasts`, `decision_review`,
  `model_map`, `ceo_view`, `source_onboarding`, `dashboard`, `substrate`,
  `rendering`, `history`) and status class. Product SLO recording rules use
  these bounded series instead of route-template regexes.
- Product workflows now also emit bounded `product_workflow_events_total`
  counters for recommendation actions, dismissals, watches, triage,
  hypothesis ratification, forecast creation, forecast detail review, forecast
  accuracy review, forecast Ask answers, source install completion/failure,
  source onboarding start/completion/failure, source status checks, and source
  uninstall events. Labels are restricted to allowlisted
  workflow/event/outcome enums.
- Source install metrics are emitted from the shared Slack/Discord/GitHub/
  Notion OAuth callback wrapper and Gmail's DWD management routes. Durable
  source onboarding metrics are emitted from the ingestion workflow progress
  path (`source.onboarding.started`, `source.onboarding.complete`) and from the
  idempotent terminal failure signal only when the signal row is newly
  inserted. Outcomes are coarse (`success`, `bad_request`, `forbidden`,
  `not_found`, `conflict`, `error`) and do not include workspace domains,
  emails, install IDs, raw status payloads, or provider response bodies.
- Grafana now provisions `product-workflow-health.json` alongside the core
  system, ingestion, webhook, embedding, reasoning/cost, and infrastructure
  dashboards.

Must solve:

- [x] Ensure every long-running process is scraped in production.
- [x] Ensure dashboards exist for system health, ingestion funnel, webhook
  ingress, embeddings, reasoning/LLM cost, data-plane infrastructure, and
  product workflow health.
- [x] Add product workflow metrics: onboarding completion, source health, Today
  load, Ask latency, recommendation actions, forecast review, and user-visible
  error rates.
- [x] Add dead-letter admin endpoint and runbook.
- [x] Add alerts for:
  - [x] worker heartbeat stale,
  - [x] scrape down,
  - [x] DB pool saturation,
  - [x] queue backpressure,
  - [x] stale locks,
  - [x] retry exhaustion,
  - [x] DLQ rows present,
  - [x] source lag / Kafka consumer lag,
  - [x] webhook signature spike,
  - [x] LLM spend burn,
  - [x] embedding failure ratio,
  - [x] backup stale / missing / failed,
  - [x] schema/RLS drift,
  - [x] product SLO burn.
- [ ] Tune thresholds after staging soak.
- [x] Ensure Prometheus labels are bounded and privacy-safe.

Acceptance evidence:

- Staging dashboard review completed.
- Alert fire drill proves notifications reach the operator channel.
- Product SLO dashboard exists.

### 7.2 Runbooks And Operator Tools

Current state:

- Role grant/revoke operations have `scripts/manage_actor_roles.py`, write
  `operator_action_log` rows with bounded `role.list`/`role.grant`/
  `role.revoke` actions, require an authorized tenant `admin`/`leadership`
  operator for normal operations, and allow only an explicit first-admin
  bootstrap grant before any operator exists. They are documented in
  [admin-role-management-guide.md](admin-role-management-guide.md).
- Tenant support diagnostics can be exported with
  `scripts/export_support_bundle.py`; the bundle includes only bounded counts,
  states, timestamps, and backup status fields, requires an operator actor, and
  writes a bounded `support_bundle.export` audit row. It is documented in
  [product-workflow-support-guide.md](product-workflow-support-guide.md).
- Source installation status, pause, resume, and generic uninstall operations have
  `scripts/manage_source_installations.py`; it is tenant-scoped, returns
  sanitized install state, and writes bounded `operator_action_log` rows for
  `source_installation.status`, `source_installation.pause`, and
  `source_installation.resume`, and `source_installation.uninstall`. Generic
  uninstall disables the row, deletes/clears the generic `secret_ref` by
  default, supports a `--keep-secret-ref` escape hatch for shared app-level
  secrets, and records that source-specific provider cleanup is still required.
  The `status` command also reports bounded source health, latest onboarding
  status timestamps, and `last_successful_sync_at` from `source_onboarding_runs`
  without exposing raw failure text or provider payloads.
- Source credential rotation is handled by
  `scripts/manage_source_installations.py rotate-secret`; it accepts new secret
  material only through env/file/stdin, preserves the stable `secret_ref`,
  rotates encrypted secret material in place, returns sanitized output, and
  writes a bounded `source_installation.secret.rotate` audit row.
- Dedicated source lifecycle operations for QuickBooks, Gusto, Ramp, Carta,
  LinkedIn, Jira, Mercury, Brex, Deel, Fireflies, Miro, Grafana, Figma, HiBob,
  Ashby, AWS, Telegram, and Signal are handled by
  `scripts/manage_dedicated_source_installations.py`.
  The CLI binds the tenant before touching strict-RLS source tables, can pause
  and resume matching webhook resolver rows, rotates access/refresh/webhook refs
  and live/backfill session refs by stable ref, handles AWS account/region
  selectors, and uninstalls by disabling the source row, clearing deleted refs,
  writing `operator_action_log` plus `installation_audit_log`, and keeping raw
  secret values and source scope IDs out of audit metadata.
- Queue-depth inspection has `scripts/inspect_queue_depth.py`; it returns
  bounded tenant-scoped counts for Think, model re-eval, post-commit,
  ingestion failures, and source onboarding queues, and writes a
  `queue_depth.inspect` audit row.
- Ingestion DLQ retry/quarantine is covered by
  `scripts/manage_ingestion_dlq.py`; Kafka-path re-enable is covered by
  `scripts/reenable_kafka_path.py` with tenant `admin`/`leadership`
  authorization and `kafka_path.reenable` audit rows; health validation is
  covered by `scripts/run_operational_readiness_gates.py`; sanitized customer
  diagnostics are covered by `scripts/export_support_bundle.py`.
- `services.platform.operator_auth.require_tenant_operator` is now shared by
  source-installation management, queue-depth inspection, support-bundle export,
  and ingestion DLQ list/replay/quarantine, requiring the operator actor to
  exist in the tenant and hold tenant-wide `admin` or `leadership`.
- [operator-runbook-index.md](runbook-index.md) maps every required operator
  scenario to a primary runbook and first verification signal. A coverage test
  fails when one of the required scenarios is dropped from the index.

Must solve:

- [x] Create runbooks for:
  - deploy,
  - rollback,
  - migration failure,
  - queue backlog,
  - DLQ replay/quarantine,
  - webhook verification spike,
  - source API outage,
  - LLM provider outage,
  - DB saturation,
  - Redis/broker/object storage outage,
  - tenant isolation incident,
  - secret rotation,
  - backup restore,
  - customer support diagnostics.
- [x] Add operator CLIs or admin routes for common actions:
  - pause source,
  - re-enable Kafka path,
  - retry DLQ,
  - inspect queue depth,
  - rotate secret,
  - run health validation,
  - export sanitized support bundle.
- [ ] Ensure every admin action is authenticated, authorized, audited, and
  tenant-scoped.

Acceptance evidence:

- On-call rehearsal follows runbooks without code/database spelunking.
- Admin action audit rows are queryable by tenant and actor.

## 8. Performance, Scalability, And Cost

Current state:

- Think has tenant-scoped concurrency (`THINK_MAX_CONCURRENCY_PER_TENANT`) and
  daily LLM spend/token/request deferral. When
  `THINK_DAILY_BUDGET_ENFORCEMENT=1` and `LLM_DAILY_BUDGET_USD_PER_TENANT`,
  `LLM_DAILY_TOKEN_BUDGET_PER_TENANT`, or
  `LLM_DAILY_REQUEST_BUDGET_PER_TENANT` is exceeded, the worker releases the
  trigger lock, reschedules the trigger for the next UTC day, and does not
  increment attempts or dead-letter the trigger. Focused tests cover spend,
  token, and request budget paths.
- Source page fetches use the Redis-backed fetch rate limiter in production,
  and source clients have bounded 429/`Retry-After` retry budgets documented in
  `.env.production.example`.

Must solve:

- [x] Define target tenant sizes for launch:
  - sources enabled,
  - users,
  - observations/day,
  - backfill volume,
  - active models,
  - Think triggers/day,
  - object/blob size,
  - Ask requests/day.
- [ ] Build synthetic datasets and load generators that match target sizes.
- [ ] Run load/soak tests for gateway, ingestion, Think, post-commit, source
  onboarding, and product reads.
- [ ] Verify noisy-source isolation.
- [ ] Verify one tenant cannot starve another tenant in shared workers.
- [ ] Add per-tenant concurrency controls for Think and expensive workers.
- [x] Add LLM token, spend, and request ceilings.
- [x] Add source API rate limits and backoff budgets.
- [ ] Add query performance budgets and index review for every hot endpoint.
- [ ] Verify p95/p99 latency and queue drain targets.
- [ ] Produce a cost model for compute, DB, broker, object storage, embeddings,
  and LLM calls.

Acceptance evidence:

- Staging soak report meets beta and GA thresholds from
  [production-readiness-gates.md](production-readiness-gates.md).
- Cost report identifies per-tenant and per-source drivers.
- No queue grows unbounded during target load.

## 9. Release Engineering And CI/CD

Current state:

- CI has ruff, import-linter, architecture ratchets, production env contract,
  migration prefix uniqueness, strict MkDocs build, and unit/integration tests.
- CI now has a targeted `mypy` gate over critical backend contracts: secret
  provider resolution, secret store factory surface, HTTP/log redaction, safe
  gateway error handling, production env contract checks, and architecture
  ratchets.
- CI now includes a security/supply-chain job that runs `pip-audit`, Trivy
  filesystem scanning, a local Docker image build plus Trivy image scan, and
  uploads source/image SBOM artifacts.
- The production env contract now requires `GRAFANA_ADMIN_PASSWORD` in the
  production template and rejects the compose/local fallback
  `fyralis-admin`, preventing the observability profile from shipping with a
  known weak admin password.
- The CI security/supply-chain job now generates SBOM checksums, signs source
  and image SBOM artifacts plus the checksum file with Sigstore/cosign
  keyless signing, verifies those signatures in CI, and uploads the signed
  bundles. The production deploy workflow downloads the signed SBOM artifact
  set from the triggering/requested CI run and verifies signatures plus
  checksums before the SSH deploy step.
- A dedicated staging deploy workflow now triggers from successful CI on
  `main` or a manual `ci_run_id`, verifies the signed SBOM/checksum artifact
  set with the `main` Sigstore identity, and then deploys to the GitHub
  `staging` environment over staging-specific SSH secrets.
- Staging and production deploy workflows now capture the previously deployed
  SHA before reset/build, and automatically roll back to that SHA if the
  gateway `/healthz` check does not recover within the deploy window.
- `mkdocs build --strict` passes locally and is now wired as a dedicated CI job.
- Release notes template exists under Operations and requires migration,
  feature-flag/config, rollback-risk, and observability sections.
- Real LLM tests run nightly.
- Production deployment now triggers from a successful `CI` workflow run on the
  `production` branch instead of racing CI on direct push, and the deploy job is
  bound to the GitHub `production` environment for repository-side approvals.
- A dedicated `Promote to Production` workflow now performs the staging-to-
  production branch promotion. It is manual-only, requires the target `main`
  SHA, the successful `Deploy to Staging` run id for that SHA, reviewed release
  notes, an explicit staging-validation checkbox, and the GitHub `production`
  environment approval before pushing the SHA to the `production` branch.
- Staging and production deploy workflows now run
  `scripts/check_product_slo_gate.py` after gateway health recovery. The gate
  queries only aggregate product workflow error/latency burn recording rules
  and rolls back to the previous SHA when burn exceeds configured thresholds.

Must solve:

- [ ] Ensure CI status checks are required for merge to `main`.
- [x] Add type-checking gate for critical backend modules.
- [x] Add security scanning for dependencies and container images.
- [x] Add SBOM generation for release artifacts.
- [x] Add image/artifact signing and verification in deploy path.
- [x] Add staging deploy automation.
- [x] Add promotion process from staging to production with explicit approval.
- [x] Add release notes template that calls out migrations, feature flags,
  rollback risk, and observability changes.
- [ ] Add canary/blue-green rollout strategy for gateway and workers.
- [x] Add automatic rollback or pause criteria for SLO breaches.
- [ ] Add real-provider contract fixtures for every integration.
- [x] Add docs build to CI and keep navigation strict.

Acceptance evidence:

- A release can be cut, deployed to staging, validated, promoted, and rolled
  back using documented commands.
- Release artifact provenance is verifiable.
- Branch protection includes the required checks.

## 10. Compliance And Governance

Current state:

- [integration-data-classification.md](integration-data-classification.md)
  classifies every documented ingestion flow by PII, financial, HR, security,
  infrastructure, communications, IP, and generated-reasoning data classes. A
  coverage test fails when a new ingestion flow lacks a classification row.
- [subprocessor-data-flow.md](subprocessor-data-flow.md) defines the external
  service/data boundaries for Codex/OpenAI reasoning, optional OpenAI
  embeddings, local Ollama embeddings, raw object storage, managed secret
  providers, source APIs/webhooks, CI scanners, and observability backends.
- [llm-prompt-content-use-policy.md](llm-prompt-content-use-policy.md) defines
  what customer-derived content may be sent to external LLM/embedding providers,
  prohibited prompt data, required redaction/budget/transaction controls, and
  provider enablement steps.
- [integration-security-review-register.md](integration-security-review-register.md)
  defines the security review checklist, risk tier, status, and production
  enablement gate for every documented ingestion source. A coverage test fails
  if a new ingestion flow lacks a security-review row.
- Local sandbox, synthetic, E2E, and real-LLM scenario fixtures now use reserved
  `.example`/`.test` identities. `scripts/tests/test_fixture_data_guardrails.py`
  scans tracked fixture/demo files for customer-shaped domains, personal email
  providers, and legacy customer-specific tokens.
- [customer-launch-evidence/](customer-launch-evidence/README.md) defines the
  template-only DPA, security questionnaire, source approval, and launch
  evidence folder shape. The repository-level folder ignores customer-specific
  evidence under `customers/`, and a regression test verifies the templates and
  no-commit policy exist.
- Admin and governance audit coverage now spans `operator_action_log` for
  operator/admin actions, role changes, source lifecycle operations, sanitized
  support bundles, Kafka re-enable, and completed customer data-export records;
  `installation_audit_log` for source install/uninstall lifecycle events;
  `product_action_audit_log` for autonomous-output acceptance/mutation flows;
  and `access_override_log` for admin/leadership access overrides.

Must solve:

- [x] Classify data types handled by each integration: PII, financial, HR,
  security, infrastructure, communications, and generated reasoning.
- [x] Define data retention and deletion policy.
- [x] Define customer data export and deletion workflow.
- [x] Define subprocessors and third-party provider data flow, especially LLM
  and embedding providers.
- [x] Add audit logs for admin actions, source installs, role changes,
  autonomous write acceptance, overrides, and data exports.
- [x] Define incident response process for privacy/security events.
- [x] Add security review for every new integration before production enablement.
- [x] Add DPA/security questionnaire evidence folder.
- [x] Ensure local development fixtures do not contain real customer data.
- [x] Add a policy for prompt/content use with external LLM providers.

Acceptance evidence:

- Data-flow inventory exists for every production source.
- Audit log coverage report exists.
- Fixture data guardrail test passes.
- Integration security review register coverage test passes.
- Security review is part of release checklist.

## 11. Documentation Required Before Launch

Must solve:

- [x] Production architecture overview.
- [x] Deployment runbook.
- [x] Rollback runbook.
- [x] Migration runbook.
- [x] Backup/restore runbook.
- [x] Source onboarding runbook per production source.
- [x] Admin/role management guide.
- [x] Observability and alert guide.
- [x] Incident response guide.
- [x] Data retention and deletion guide.
- [x] Product workflow guide for customer-facing support.
- [x] Known limitations and feature flags.

Acceptance evidence:

- Docs build passes strict mode.
- Every runbook has an owner and last-reviewed date.

## Execution Order

### Sprint 1 - Stop Production Blockers

- Strict RLS migration plan and production DB role startup gate.
- Access route inventory and debug/demo/synthetic production disablement.
- Session tenant cross-check and WebSocket token removal design.
- Log/body redaction tests.
- `applied_triggers` idempotency fix.
- Queue lease heartbeat/orphan recovery.
- No-network-in-transaction static check.
- Think/model transaction split implementation plan.

### Sprint 2 - Runtime Stability

- Implement Think/model transaction split.
- Scheduler leader election and advisory refresh lock.
- Post-commit dedup/index fixes.
- Rendering/query adapter retries and circuit breakers.
- Dead-letter admin endpoint, metrics, and runbook.
- Worker process manifest and heartbeat coverage.

### Sprint 3 - Product Surface Integrity

- Access-control enforcement on product routes.
- Product route production-mode tests.
- Remove fixture/spec/demo paths from production.
- Core workflow E2E suite.
- Overlay auth/storage/sanitization/polling fixes.
- Product SLO metrics.

### Sprint 4 - Operations And Release

- Migration rehearsal and rollback process.
- Backup/restore rehearsal.
- Load/soak test.
- Source install lifecycle completion.
- Security/dependency scanning and artifact signing.
- Staging deploy and promotion automation.

### Sprint 5 - GA Hardening

- Retention/partitioning for debug and high-volume tables.
- Cost ceilings and budget breakers.
- Compliance/data-flow inventory.
- Customer support diagnostics.
- Full docs/runbook strict build.

## Readiness Scorecard

Use this table in weekly readiness reviews.

| Area | P0 complete | P1 complete | Evidence link | Owner |
| --- | --- | --- | --- | --- |
| Tenant isolation | no | no | TBD | TBD |
| Authz and route safety | no | no | TBD | TBD |
| Secret handling | no | no | TBD | TBD |
| Log/error privacy | no | no | TBD | TBD |
| Schema/migrations | partial | no | TBD | TBD |
| Queue correctness | no | no | TBD | TBD |
| Runtime reliability | no | no | TBD | TBD |
| Ingestion pipeline | partial | no | TBD | TBD |
| Source lifecycle | partial | no | TBD | TBD |
| Worker fabric | partial | no | TBD | TBD |
| Product workflows | no | no | TBD | TBD |
| Frontend/overlay | no | no | TBD | TBD |
| Observability | partial | no | TBD | TBD |
| Runbooks/operator tools | no | no | TBD | TBD |
| Performance/cost | no | no | TBD | TBD |
| Release engineering | partial | no | TBD | TBD |
| Compliance/governance | no | no | TBD | TBD |
