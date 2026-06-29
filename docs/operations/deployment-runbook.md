# Deployment Runbook

This runbook covers the current GitHub Actions deployment path for Fyralis
Core. It assumes the production and staging hosts already contain a checked-out
`fyraliscore` repo, Docker Compose, and their environment files.

## Release Shape

| Environment | Source | Workflow | GitHub environment |
| --- | --- | --- | --- |
| Staging | `main` | `Deploy to Staging` | `staging` |
| Production promotion | `main` SHA validated in staging | `Promote to Production` | `production` |
| Production | `production` | `Deploy to Production` | `production` |

Both deploy workflows verify signed CI artifacts before SSH:

- `fyraliscore-source.spdx.json`
- `fyraliscore-image.cdx.json`
- `fyraliscore-sboms.SHA256SUMS`
- the matching `.sigstore` bundles

Staging accepts artifacts signed by `.github/workflows/ci.yml@refs/heads/main`.
Production accepts artifacts signed by `.github/workflows/ci.yml@refs/heads/production`.

## Staging Deploy

Automatic path:

1. Merge to `main`.
2. Wait for the `CI` workflow to finish successfully.
3. `Deploy to Staging` starts from the successful `CI` `workflow_run`.
4. The workflow downloads `fyraliscore-sboms`, verifies signatures and checksums,
   SSHes to the staging host, resets the repo to `origin/main`, rebuilds, and
   waits for `http://localhost:8000/healthz`.

Manual path:

1. Open `Deploy to Staging`.
2. Run `workflow_dispatch` with a successful `CI` run id from `main`.
3. Confirm the `staging` GitHub environment approval if configured.

Required staging secrets:

- `STAGING_LIGHTSAIL_HOST`
- `STAGING_LIGHTSAIL_USER`
- `STAGING_LIGHTSAIL_SSH_KEY`
- `STAGING_APP_DIR`

## Production Promotion

1. Confirm staging is healthy and acceptance checks are complete.
2. Open `Promote to Production`.
3. Provide:
   - `target_sha`: the full `main` commit SHA that was deployed to staging.
   - `staging_deploy_run_id`: the successful `Deploy to Staging` workflow run
     for that exact SHA.
   - `release_notes_path`: the reviewed release notes file for this promotion.
   - `confirm_staging_validation`: checked only after dashboards, readiness
     gates, migration evidence, and rollback plan are reviewed.
4. The `production` GitHub environment approval gate must be satisfied before
   the promotion job can push `target_sha` to the `production` branch.
5. Wait for `CI` on `production` to finish successfully.
6. `Deploy to Production` starts from the successful `CI` `workflow_run`.
7. The `production` GitHub environment approval gate must be satisfied before
   the job deploys.

Manual production deploys require a successful `CI` run id from `production`;
this prevents bypassing signed artifact verification.

Required production secrets:

- `LIGHTSAIL_HOST`
- `LIGHTSAIL_USER`
- `LIGHTSAIL_SSH_KEY`

## Preflight Checklist

- `scripts/check_production_env_contract.py` passes.
- `mkdocs build --strict` passes in CI.
- `security-supply-chain` produced signed SBOM artifacts.
- Schema and migration readiness gates are green.
- Staging dashboard shows healthy scrape targets and no product SLO burn.
- Release notes include migration, feature flag, rollback, and observability
  sections.
- Product SLO gate thresholds are correct for the target environment:
  `PRODUCT_SLO_GATE_PROMETHEUS_URL`, `PRODUCT_SLO_GATE_ERROR_BURN_MAX`,
  `PRODUCT_SLO_GATE_LATENCY_BURN_MAX`, `PRODUCT_SLO_GATE_WAIT_SECONDS`, and
  `PRODUCT_SLO_GATE_INTERVAL_SECONDS`.

## Canary And Worker Rollout

Both staging and production deploy workflows call
`scripts/deploy_compose_release.sh` after resetting the host checkout to the
target branch. The helper implements the single-host Compose release strategy:

1. Pull/build the target release images.
2. Start a no-traffic `gateway` canary container with
   `GATEWAY_START_GRT_SCHEDULER=0`, so startup checks and `/healthz` run without
   taking user traffic or duplicating scheduler ownership.
3. Promote the real `gateway` service and wait for `/healthz`.
4. Recreate production worker services from
   `services.platform.runtime.process_manifest` one at a time and require each
   healthcheck to settle before moving to the next service.
5. Reconcile the Compose project, remove orphans, and run the product SLO gate.

Useful rollout environment flags:

- `DEPLOY_GATEWAY_CANARY=0` skips the no-traffic canary only for break-glass
  recovery.
- `DEPLOY_WORKER_ROLLOUT=0` skips health-gated worker rollout and falls back to
  final Compose reconciliation only.
- `DEPLOY_WORKER_ROLLOUT_SERVICES="svc_a svc_b"` limits the worker rollout set.
- `DEPLOY_RUN_PRODUCT_SLO_GATE=0` skips the SLO gate only when Prometheus is
  unavailable and an operator has approved manual verification.

## Post-Deploy Checks

Run on the target host:

```bash
docker compose ps
curl -fsS http://localhost:8000/healthz
curl -fsS http://localhost:8000/readyz
```

Then check Grafana:

- System Health: all expected targets up.
- Product Workflow Health: request rate present, 5xx ratio within budget, p95
  latency within budget.
- Data Plane Infrastructure: Postgres, Kafka, Redis, MinIO exporters healthy.

## Built-In Rollback

The staging and production deploy workflows capture the previously deployed git
SHA before resetting to the target branch. `scripts/deploy_compose_release.sh`
rolls back to that SHA if the gateway canary fails, if the promoted gateway or
any health-gated worker does not become healthy, or if
`scripts/check_product_slo_gate.py` sees product workflow error/latency burn
above the configured rollback thresholds. The helper resets back to the prior
SHA, rebuilds the compose stack, waits for health again, prints
`docker compose ps`, and fails the workflow.

The product SLO gate reads bounded aggregate Prometheus recording rules only:
`fyralis:product_workflow_error_budget_burn:5m` and
`fyralis:product_workflow_latency_budget_burn:5m`. No tenant, actor, route,
prompt, source payload, or object identifiers are queried or exported.

Use [rollback-runbook.md](rollback-runbook.md) for manual rollback or partial
service recovery.
