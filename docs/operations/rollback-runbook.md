# Rollback Runbook

Use this when a deploy creates a user-visible outage, data-plane failure, or
operator-approved rollback condition. Prefer forward-fix only when the fix is
small, already tested, and safer than returning to the previous known-good SHA.

## Automatic Rollback

Both deploy workflows capture the target host's current SHA:

```bash
PREV_SHA="$(git rev-parse HEAD)"
```

Before touching the live gateway, the deploy helper starts a no-traffic gateway
canary with schedulers disabled. After promotion, the workflow waits for:

```bash
curl -sf http://localhost:8000/healthz
```

If the canary fails, if health does not recover within the deploy window, if a
health-gated worker fails during rollout, or if the product SLO gate breaches
the configured rollback thresholds, the workflow resets to `PREV_SHA`,
rebuilds, waits for health again, prints compose status, and exits failed. Treat
that failed workflow as a rollback incident and inspect logs.

## Manual Rollback

On the affected host:

```bash
cd ~/fyraliscore
git rev-parse --short HEAD
git log --oneline -5
```

Choose the last known-good SHA, then:

```bash
git fetch origin
git reset --hard <known-good-sha>
docker compose up -d --build --remove-orphans
timeout 60 bash -c 'until curl -sf http://localhost:8000/healthz; do sleep 3; done'
docker compose ps
```

If only one worker is affected, prefer a targeted restart:

```bash
docker compose up -d --no-deps --force-recreate <service-name>
```

## Verification

After rollback:

```bash
curl -fsS http://localhost:8000/healthz
curl -fsS http://localhost:8000/readyz
docker compose ps
docker compose logs --tail=200 gateway
```

Check Grafana:

- `WorkerScrapeDown` and `WorkerHeartbeatStale` are clear.
- `ProductSLOBurnHigh` is clear or trending down.
- `SchemaRLSDriftDetected` is clear.
- `BackupRecoveryUnhealthy` is clear before any subsequent promotion.

## Migration Rollback

Do not roll back a destructive or contract migration with git alone. Use
[migration-release-runbook.md](migration-release-runbook.md):

- prefer forward-fix for already-applied irreversible migrations;
- restore from a verified snapshot only after explicit approval;
- run `scripts/check_schema_drift.py` after the rollback or forward-fix.

## Communication

Record the incident in release notes or the incident tracker with:

- failed SHA and rolled-back SHA;
- reason for rollback;
- affected services and user-visible symptoms;
- verification evidence after rollback;
- follow-up issue for the root cause.
