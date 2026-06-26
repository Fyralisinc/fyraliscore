# Source Onboarding Runbook

Owner: Integrations Engineering.
Last reviewed: 2026-06-24.

This runbook defines the production operator flow for installing, validating,
pausing, resuming, and uninstalling data sources. It covers Fyralis core
behavior; source-specific OAuth screens and customer-facing UI are owned by the
overlay product.

In BYOC deployments, every source validation call, provider credential exchange,
secret write/read probe, raw payload write, and queue/object-store health check
runs inside the customer data plane. The Fyralis control plane may track only
bounded onboarding state, source-family enablement, aggregate health, and
sanitized failure codes; it must not receive source credentials, raw payloads,
prompts, logs, or PII.

Before enabling the first source in a BYOC deployment, run
`scripts/validate_byoc_permissions_manifest.py` against the customer permission
manifest and AWS/IAM skeleton used for the deployment, then run
`scripts/run_byoc_post_deploy_validation.py --require-live` from inside the
customer data plane with the local gateway URL, worker health URLs, production
database DSN, broker endpoint, and object-store endpoint. Source onboarding
should remain paused until required checks pass and the data-plane agent has
successfully completed the `fyralis.byoc.agent.enrollment.v1` registration
contract and submitted a privacy-safe `fyralis.byoc.agent.heartbeat.v1`
heartbeat. The heartbeat must report only bounded component status codes,
validation state, and aggregate telemetry-contract flags.

## Source Coverage

| Source | Production source doc | Credential style | Production launch note |
| --- | --- | --- | --- |
| Slack | [Slack](../ingestion/sources/slack.md) | OAuth | Must verify webhook signing secret and bot scopes before enabling watches. |
| Gmail | [Gmail](../ingestion/sources/gmail.md) | OAuth or domain-wide delegation | Must verify Pub/Sub push identity and refresh-token coverage. |
| Google Calendar | [Google Calendar](../ingestion/sources/google-calendar.md) | OAuth or domain-wide delegation | Must verify watch renewal worker and calendar scope. |
| Google Drive | [Google Drive](../ingestion/sources/google-drive.md) | OAuth or domain-wide delegation | Must verify object-store write path and large-object retention tags. |
| GitHub | [GitHub](../ingestion/sources/github.md) | App installation or token | Must verify webhook signature and installation tenant mapping. |
| Discord | [Discord](../ingestion/sources/discord.md) | Bot token/webhook | Must verify gateway worker health before launch. |
| Telegram | [Telegram](../ingestion/sources/telegram.md) | MTProto session | Must verify session storage and operator-controlled pause/uninstall. |
| Jira | [Jira](../ingestion/sources/jira.md) | OAuth | Must verify site URL, webhook signature, and backfill limits. |
| Grafana | [Grafana](../ingestion/sources/grafana.md) | Service account token | Must verify token secret reference and API rate budget. |
| Notion | [Notion](../ingestion/sources/notion.md) | OAuth | Must verify workspace mapping and page/database access. |

## Standard Install Flow

1. Confirm tenant and actor context.
2. Collect the source-specific customer authorization.
3. Validate credentials against the provider before writing an enabled install
   row.
4. Store secrets only as `secret_ref` values.
5. Create or update the provider installation row with the tenant, source,
   external account/workspace identity, scopes, status, and secret references.
6. Register watches, subscriptions, or polling schedules.
7. Start initial backfill with bounded batch size and source rate limits.
8. Emit one onboarding progress event per state transition.
9. Verify source health, first successful sync, and queue lag.
10. Declare install successful only after post-install validation passes.

## Required Validation Checks

| Check | Purpose | Failure action |
| --- | --- | --- |
| Credential exchange or API ping | Proves the token/session is valid | Do not write enabled install row. |
| Scope check | Proves required least-privileged scopes are present | Return safe remediation text to customer. |
| Secret-provider write/read | Proves `secret_ref` can be resolved at call time | Keep install disabled. |
| Webhook signature/OIDC check | Proves ingress can authenticate provider calls | Keep webhook inactive. |
| Backfill dry run | Proves pagination and rate limits are configured | Lower batch size or request missing scope. |
| Object-store write check | Proves raw payload/blob persistence | Keep source paused. |
| Queue health check | Proves the source lane drains | Scale lane or pause before user traffic. |

## Pause And Resume

Pause a source when provider rate limits, bad payloads, customer request, or
incident containment requires stopping new ingestion.

Generic provider-installation status, pause, resume, credential rotation, and
uninstall are available through `scripts/manage_source_installations.py`. The
CLI is tenant-scoped, requires the operator actor to have tenant-wide `admin`
or `leadership`, writes `operator_action_log` rows, and returns sanitized
installation state only (`has_secret_ref`, never the secret value).

List source installation status:

```bash
python scripts/manage_source_installations.py status \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --provider slack
```

Status output includes `source_health`, `latest_onboarding_status`,
`latest_onboarding_*_at` timestamps, and `last_successful_sync_at` from the
bounded onboarding rollup tables. It exposes only booleans such as
`has_secret_ref`; raw secrets, provider payloads, and raw failure text are not
returned.

Pause one installation:

```bash
python scripts/manage_source_installations.py pause \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --provider slack \
  --installation-id "$PROVIDER_INSTALLATION_ID" \
  --reason "provider outage"
```

Resume one installation:

```bash
python scripts/manage_source_installations.py resume \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --installation-row-id "$PROVIDER_INSTALLATION_ROW_ID" \
  --reason "provider recovered"
```

Rotate one installation credential:

```bash
NEW_SOURCE_SECRET="..." \
python scripts/manage_source_installations.py rotate-secret \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --provider slack \
  --installation-id "$PROVIDER_INSTALLATION_ID" \
  --new-secret-env NEW_SOURCE_SECRET \
  --reason "customer token rotation"
```

The rotation command preserves the stable `provider_installations.secret_ref`,
updates the encrypted secret material in place, and writes a
`source_installation.secret.rotate` audit row. Operators must pass the
replacement value through `--new-secret-env`, `--new-secret-file`, or
`--new-secret-stdin`; raw secrets must not be placed in command-line arguments,
logs, support bundles, or ticket comments.

Generic uninstall:

```bash
python scripts/manage_source_installations.py uninstall \
  --tenant "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --provider slack \
  --installation-id "$PROVIDER_INSTALLATION_ID" \
  --reason "customer requested uninstall"
```

By default the generic uninstall action disables the
`provider_installations` row, deletes the referenced `encrypted_secrets` row,
clears `provider_installations.secret_ref`, and writes a
`source_installation.uninstall` audit row. Use `--keep-secret-ref` only for
known shared app-level secrets that must outlive one installation. Generic
uninstall does not prove provider-side webhook/watch deletion; run the
source-specific uninstall checklist below before declaring cleanup complete.

Dedicated source install tables are managed with
`scripts/manage_dedicated_source_installations.py` for status, pause, resume,
rotation where the table owns secret refs, and uninstall. Google Workspace DWD
sources (`gmail`, `google_calendar`, and `google_drive`) use this dedicated
path too; their install rows do not carry per-install secret refs, so uninstall
disables the row and related shards without requiring secret-store deletion for
the install row itself. Gmail uninstall additionally stops mailbox watches,
attempts Pub/Sub teardown, marks the local Pub/Sub topic torn down, and clears
local watch cursors and expirations so stale pushes cannot resolve after the
source is disabled. WhatsApp also uses this dedicated path; it maps the
`enabled` install flag into the same operator status/pause/resume/uninstall
contract and rotates its `app_secret_ref`, `verify_token_ref`, and
`access_token_ref` fields. Dedicated webhook-capable source uninstall also
disables the matching local `provider_installations` resolver row when present
and reports `webhook_cleanup_status` plus `webhook_cleanup_complete` in the
operator output and audit context. Treat `provider_row_missing` as an
investigation item before declaring cleanup complete.

Pause requirements:

- Mark the install paused or disabled in the provider installation table.
- Stop or disable provider watches where the provider supports it.
- Keep existing raw payloads and durable queue rows intact.
- Emit an operator/customer-visible reason.
- Verify no new source events are accepted except explicit replay.

Resume requirements:

- Re-check credential validity and scopes.
- Re-register watches if needed.
- Resume from provider cursors or durable checkpoints.
- Verify queue drain and first successful sync.

## Uninstall

Uninstall must be reversible until the customer explicitly requests data
deletion.

1. Disable install row and set terminal/uninstalled status.
2. Stop watches/subscriptions and polling leases.
3. Remove provider-side webhooks when applicable.
4. Revoke or delete source credentials in the secret provider.
5. Keep audit rows and uninstall metadata.
6. Quarantine in-flight source work that cannot safely complete.
7. Record uninstall status and last successful cleanup step.
8. For webhook sources, verify the local resolver row is disabled and the
   uninstall result reports `webhook_cleanup_complete=true`.

Data deletion is separate from uninstall. Follow
[Data retention, backup, and recovery](data-retention-backup-recovery.md) for
export/deletion workflow.

## Health Signals

Operators should check:

- source install status and last successful sync
- provider refresh-token or session renewal status
- webhook verification failures by source
- per-source Kafka lag and DLQ rate
- backfill progress and retry budget
- secret resolution failures
- object-store write failures

Use [Per-source worker scaling](per-source-worker-scaling-runbook.md) for noisy
lanes and [Ingestion DLQ replay/quarantine](ingestion-dlq-replay-quarantine-runbook.md)
for failed records.

## Current Open Gaps

The production checklist still tracks implementation gaps separately from this
runbook:

- every source install flow must block enabled rows until credential validation
  passes
- every source must store credentials only as `secret_ref`
- OAuth refresh coverage must be proven for every OAuth source
- uninstall tests must prove watches/subscriptions and secrets are cleaned up
