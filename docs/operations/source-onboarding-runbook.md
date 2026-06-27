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
manifest and AWS/IAM skeleton used for the deployment, run
`scripts/generate_byoc_aws_iac_package.py --check-package` against the AWS IaC
package scaffold, placeholder component modules, and referenced manifests, run
the Terraform validation report CLI to archive a contract-only report with no
raw command output or plan JSON, run
`scripts/verify_byoc_bootstrap_bundle.py --verify-local-files` against the
signed bundle manifest used by the bootstrap runner, run
`scripts/generate_byoc_bootstrap_plan.py --check-plan` against the dry-run plan
used by the customer-side bootstrap runner, run
`scripts/run_byoc_bootstrap_runner.py --json` to archive a sanitized local
evidence report with no raw command output or artifact refs, run
`scripts/run_byoc_aws_live_preflight.py --json` from inside the customer AWS
execution context to verify the selected account/profile against the
permissions manifest, then run
`scripts/run_byoc_post_deploy_validation.py --require-live` from inside the
customer data plane with the local gateway URL, worker health URLs, production
database DSN, broker endpoint, and object-store endpoint.

For customer handoff or support triage, the local preflight bundle wraps the
non-live checks above into one sanitized aggregate report:

```bash
scripts/run_byoc_preflight_bundle.py --json \
  --env-file <customer-byoc.env> \
  --output <byoc-preflight-report.json>
```

The preflight bundle is a summary only. Keep using the individual commands when
operators need to diagnose a specific failing section.

For the final backend/core handoff check, compose the local preflight,
evidence-package contract, and source-onboarding gate into one sanitized
go/no-go report:

```bash
scripts/run_byoc_customer_handoff.py --json \
  --env-file <customer-byoc.env> \
  --evidence-package <package> \
  --evidence-ledger <ledger.yaml> \
  --output <byoc-customer-handoff-report.json>
```

This report is safe for customer support and release-review handoff. It
contains only aggregate status, bounded failure codes, and counts; it does not
embed child reports, package bodies, command output, account IDs, ARNs, URLs,
artifact refs, credentials, source payloads, prompts, logs, or PII.

For the first real AWS credential test, run the read-only live preflight inside
the customer boundary. The basic command verifies STS caller identity against
the account contract in the permissions manifest:

```bash
scripts/run_byoc_aws_live_preflight.py --json \
  --dataplane-manifest <customer-dataplane.yaml> \
  --permissions-manifest <customer-permissions.yaml> \
  --iam-template <customer-iam-skeleton.yaml> \
  --output <aws-live-preflight-report.json>
```

For a deeper customer-side permissions check, add read-only describe/list
probes and IAM policy simulation for the bootstrap role. The simulation
principal ARN is used only for the local AWS API call and is not serialized:

```bash
scripts/run_byoc_aws_live_preflight.py --json \
  --dataplane-manifest <customer-dataplane.yaml> \
  --permissions-manifest <customer-permissions.yaml> \
  --iam-template <customer-iam-skeleton.yaml> \
  --run-readonly-api-probes \
  --run-iam-policy-simulation \
  --simulation-principal-arn <customer-bootstrap-role-arn> \
  --output <aws-live-preflight-report.json>
```

The AWS live-preflight report contains only bounded status, booleans, and
counts. It must not include account IDs, ARNs, profile names, endpoint URLs,
policy documents, command output, credentials, source payloads, prompts, logs,
or PII. For a one-command handoff report, the aggregate preflight may include
this section with `--run-aws-live-preflight`; do not use
`--skip-aws-live-preflight-aws` for customer readiness because that flag is only
for CI/report-contract smoke tests.

For tomorrow's real credential rehearsal, prefer the artifact-pipeline command
from inside the customer boundary. It writes sanitized AWS-preflight,
evidence-ledger, and evidence-package artifacts, then gates the generated
package:

```bash
scripts/run_byoc_live_credential_rehearsal.py --json \
  --output-dir <customer-rehearsal-artifacts-dir> \
  --env-file <customer-byoc.env> \
  --require-live-aws-api-calls \
  --output <live-credential-rehearsal-summary.json>
```

Add `--run-readonly-api-probes`, `--run-iam-policy-simulation`, and
`--simulation-principal-arn <customer-bootstrap-role-arn>` when the customer
wants deeper permission proof. Use `--skip-live-aws` only in CI or local
contract smoke tests; when `--require-live-aws-api-calls` is set, the command
fails if no real AWS API call was executed.

Where the hosted control-plane intake is enabled, sign and submit the preflight
summary with the evidence intake key:

```bash
FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY="<local-signing-material>" \
scripts/submit_byoc_preflight_report.py \
  --preflight-report <byoc-preflight-report.json> \
  --agent-id <agent-id> \
  --agent-version <agent-version> \
  --key-ref <evidence-intake-key-ref> \
  --submit-url https://<control-plane>/byoc/control-plane/preflight-reports
```

The backend stores only a scalar preflight receipt with report digest, status,
section counts, and safe execution flags; it does not store the preflight report
body or child reports.

```bash
scripts/run_byoc_terraform_plan_validation.py --json \
  --output <terraform-validation-report.json>
```

When Terraform is installed inside the customer data-plane execution
environment, operators may add `--run-terraform-init`,
`--run-terraform-validate`, and `--terraform-bin <terraform>` to run
`terraform init -backend=false` followed by `terraform validate` against the
scaffold root. The report still excludes stdout, stderr, plan JSON, provider
credentials, and raw Terraform output; it records only bounded status metadata
and exit code. If init is requested and fails, validate is not run.

If the live validator writes a JSON report, set
`FYRALIS_BYOC_EVIDENCE_SIGNING_SECRET` locally and summarize it with:

```bash
scripts/generate_byoc_evidence_ledger.py \
  --aws-live-preflight-report <aws-live-preflight-report.json> \
  --terraform-validation-report <terraform-validation-report.json> \
  --post-deploy-report <report.json> \
  --post-deploy-envelope <envelope.json>
```

Then run `scripts/generate_byoc_evidence_ledger.py --check-ledger <ledger>` to
verify the ledger contract, and build the customer handoff package with:

```bash
scripts/generate_byoc_evidence_package.py \
  --ledger <ledger.yaml> \
  --post-deploy-envelope <envelope.json>
```

Run `scripts/generate_byoc_evidence_package.py --check-package <package>` before
sharing the package. The package contains the sanitized ledger, source manifest
digests, AWS IaC package digest, and signed-envelope metadata only; do not
include the raw validator report. Where the hosted control-plane intake is
enabled, submit only a signed
`fyralis.byoc.evidence_package_submission.v1` payload to
`POST /byoc/control-plane/evidence-packages`; the backend stores only durable
sanitized scalar receipt metadata and must not store the package body or raw
validator report. Receipt lookup and list automation must use signed read
headers, and list queries must include `deployment_id` or `customer_id`. The
submission/read signing `key_ref` values must resolve through
`FYRALIS_BYOC_EVIDENCE_*_SIGNING_KEY_SECRET_REF` managed secrets in production;
raw signing-key env values are local/test only. Source onboarding
should remain paused until required checks pass and the data-plane agent has
successfully completed the `fyralis.byoc.agent.enrollment.v1` registration
contract, pulled metadata-only desired state through
`fyralis.byoc.agent.desired_state_poll.v1`, and submitted a privacy-safe
`fyralis.byoc.agent.heartbeat.v1` heartbeat. The heartbeat must report only
bounded component status codes, validation state, and aggregate
telemetry-contract flags.

Immediately before enabling the first source, run the source-onboarding gate.
For a customer credential/live-readiness handoff, require AWS live-preflight
evidence and live post-deploy evidence:

```bash
scripts/check_byoc_source_onboarding_gate.py --json \
  --evidence-package <package> \
  --require-aws-live-preflight \
  --require-live-post-deploy
```

Add `--require-signed-post-deploy` for production cutover packages that must
prove the live validator report was signed before import. The gate consumes
only the sanitized package or ledger and emits bounded pass/fail metadata.
Apply the same strict requirements to the customer handoff report by adding
`--require-aws-live-preflight --require-live-post-deploy`.

Where the hosted control-plane agent endpoint is enabled, enrollment uses
`POST /byoc/agent/enroll` with the signed
`fyralis.byoc.agent.enrollment.v1` request, and heartbeat uses
`POST /byoc/agent/heartbeat` with the `fyralis.byoc.agent.heartbeat.v1`
payload. Production enrollment resolves the request `key_ref` through
`FYRALIS_DATA_PLANE_AGENT_INSTALL_TOKEN_SECRET_REF`; raw install tokens are
local/test only. The backend persists only sanitized registration metadata and
latest heartbeat aggregate counts. Agents pull revision/config intent through
`POST /byoc/agent/desired-state` with the signed
`fyralis.byoc.agent.desired_state_poll.v1` request; the response contains only
desired revision, rollout action, poll cadence, telemetry contract, and config
epoch metadata.

Backend automation advances an enrolled agent through
`POST /byoc/control-plane/agent-desired-state` with a signed
`fyralis.byoc.agent.desired_state_update.v1` request. Use the BYOC evidence
intake signing key reference for this route until a dedicated desired-state
key is introduced. The hosted backend persists only scalar rollout intent in
`byoc_agent_registrations`: desired revision, config epoch,
evidence-package-required flag, reason code, requester code, and accepted
timestamp. Do not submit artifact refs, config bodies, endpoint URLs, raw
tokens, logs, payloads, prompts, embeddings, or PII through this route.

To produce the signed request locally without network access, omit
`--submit-url`; to send it to the hosted backend, provide the full route URL:

```bash
FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY="<local-signing-material>" \
scripts/update_byoc_agent_desired_state.py \
  --deployment-id <dep_...> \
  --customer-id <cus_...> \
  --agent-id <agt_...> \
  --desired-revision <revision> \
  --config-epoch <epoch> \
  --reason-code rollout_rehearsal \
  --requested-by ops_backend \
  --key-ref <control-plane/byoc/evidence-intake-key-ref> \
  --submit-url https://<control-plane>/byoc/control-plane/agent-desired-state
```

Backend automation can inspect sanitized enrolled-agent state with signed read
headers against `GET /byoc/control-plane/agents`. Always include
`deployment_id` or `customer_id`; responses contain only agent identity,
revision/config epoch intent, evidence-required flag, and aggregate heartbeat
status/counts. They do not include install-token refs, signatures, request
bodies, endpoint URLs, logs, payloads, prompts, embeddings, or PII.

Backend automation/control-panel consumers can also read
`GET /byoc/control-plane/deployment-overview` with the same signed read
headers. Include `deployment_id` and, where available, `customer_id`; the
response summarizes deployment status, next action, agent health counts,
evidence-package receipt counts, and latest accepted timestamps only. Use the
overview for customer-facing health state and the agent fleet endpoint for
per-agent metadata.

To print a signed GET request without network access, omit `--list-url`; to
run the read against the hosted backend, provide the full route URL:

```bash
FYRALIS_BYOC_EVIDENCE_READ_SIGNING_KEY="<local-read-signing-material>" \
scripts/list_byoc_agents.py \
  --deployment-id <dep_...> \
  --limit 50 \
  --key-ref <control-plane/byoc/evidence-read-key-ref> \
  --list-url https://<control-plane>/byoc/control-plane/agents
```

For local contract proof or customer handoff before live agent endpoint wiring,
run the mock-backed probe from inside the customer data-plane context:

```bash
FYRALIS_BYOC_INSTALL_TOKEN="<managed-secret-value-loaded-locally>" \
scripts/run_byoc_agent_probe.py --json --output <agent-probe-report.json>
```

For bounded local runner proof, run the loop skeleton with an explicit
iteration cap. To exercise non-mutating apply-plan evidence, provide a local
mock desired revision that differs from the manifest artifact revision:

```bash
FYRALIS_BYOC_INSTALL_TOKEN="<managed-secret-value-loaded-locally>" \
scripts/run_byoc_agent_runner.py --json --iterations 2 \
  --mock-desired-revision 2026.06.26-2 --mock-config-epoch 1 \
  --bootstrap-bundle deploy/byoc/bootstrap-bundle.next.example.yaml \
  --verify-local-bundle-files \
  --output <agent-runner-report.json>
```

Before rehearsing live token rotation, generate the plan-only install-token
rotation report. This validates dual-ref overlap without writing customer cloud
secrets, mutating the hosted control plane, serializing secret refs, or
including raw token material:

```bash
scripts/run_byoc_agent_token_rotation_plan.py --json \
  --next-install-token-secret-ref <customer-next-install-token-secret-ref> \
  --overlap-seconds 3600 \
  --activation-epoch <next-agent-config-epoch> \
  --output <agent-token-rotation-plan.json>
```

Archive only the generated report. Do not archive shell history, raw install
tokens, live control-plane URLs, desired-state bodies, or request/response
bodies. The runner report may include apply-plan evidence, but that evidence is
`plan_only` and must have a zero mutating-step count. Artifact verification
evidence may include artifact roles, kinds, SHA-256 digests, and counts only;
do not archive artifact refs, Sigstore bundle refs, raw verification output, or
secret-ref paths. The token-rotation plan may include salted secret-ref
digests only.
Where hosted control-plane intake is enabled, derive a signed
`fyralis.byoc.runner_evidence_submission.v1` payload from the report and submit
it to `POST /byoc/control-plane/runner-evidence` using the evidence intake
signing key reference. Submit only the derived runner summary, not the raw
runner report. The backend stores a scalar
`fyralis.byoc.runner_evidence_receipt.v1` receipt with deployment/agent
identity, revision intent, pass/fail status, and aggregate counts only.

```bash
FYRALIS_BYOC_EVIDENCE_INTAKE_SIGNING_KEY="<local-signing-material>" \
scripts/submit_byoc_runner_evidence.py \
  --runner-report <agent-runner-report.json> \
  --key-ref <control-plane/byoc/evidence-intake-key-ref> \
  --submit-url https://<control-plane>/byoc/control-plane/runner-evidence
```

Before a real AWS credential window, run the offline readiness check. Without
`--require-aws-access`, it validates manifests, IAM skeletons, operator
scripts, and local AWS-access prerequisites without making AWS calls; missing
AWS access is reported as the next manual action. Add `--require-aws-access`
when a profile or temporary env credentials should already be configured:

```bash
scripts/check_byoc_live_test_readiness.py --json \
  --aws-profile <profile-name> \
  --aws-region <region> \
  --require-aws-access
```

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
