# Production Readiness Gates

This runbook is the promotion checklist for the learning-loop and ingestion
runtime after the structural feedback-loop work. The current gate is scoped to
checks that can run from this repo and database without staging credentials.

## Command

Run the umbrella gate harness from the repo root:

```bash
uv run python scripts/run_operational_readiness_gates.py
```

The harness writes:

- `tests/real_llm/reports/runs/<run-id>/operational_readiness_report.json`
- `tests/real_llm/reports/runs/<run-id>/operational_readiness_summary.md`

To inspect the expected production process inventory from the canonical runtime
manifest:

```bash
uv run python scripts/render_runtime_process_manifest.py production --format markdown
```

## Automated Gates

The runner executes these gates locally when the required database and test
environment are present:

| Gate | Production question |
| --- | --- |
| Production env contract | Does `.env.production.example` still expose the fail-closed production settings operators must fill before deploy, reject known weak defaults such as the local Grafana password, and keep raw app/provider secret placeholders blank? |
| GitHub main required checks | Does the live `main` branch protection or active repository ruleset require every checked-in CI gate listed in `.github/main-required-checks.json`? |
| Feedback-loop gap harness | Do archival cleanup, evidence attachment, no-op proof, graph context use, and question-policy learning still work? |
| 50-batch storyline report | Did the last 50-batch run meet product, calibration, and queue-drain thresholds? |
| Schema drift | Does the live migrated database match the schema lock expectations? |
| Shadow and cutover tests | Is webhook shadow/cutover behavior safe under success, fallback, and misconfiguration paths? |
| Synthetic load generator smoke | Does the M-Load sender still sign, skew, duplicate, and report traffic correctly? |
| Calibration suite | Does calibration compute, write, and read back bounded offsets? |
| Permission/privacy probes | Do tenant isolation and resolver security checks prevent cross-tenant reads? |

The GitHub gate needs `GITHUB_REPOSITORY` plus `GITHUB_TOKEN` or `GH_TOKEN`
with read access to branch protection/rulesets. Without that token, the gate is
reported as `manual_required` rather than silently passing.

## SLO And Alert Thresholds

These are beta thresholds. Tighten them after a week of dogfood telemetry.

| Surface | Beta gate | GA target | Alert |
| --- | ---: | ---: | --- |
| Think run failures | 0 in promotion harness | < 0.1 percent per hour | `think_runs_failed_total` burn alert |
| Validation errors | 0 in promotion harness | < 0.1 percent per hour | Think validation drop alert |
| Pending Think triggers after drain | 0 | 0 sustained for rollout tenant | trigger queue depth alert |
| Pending post-commit actions after drain | 0 | 0 sustained for rollout tenant | post-commit queue depth alert |
| Dead-lettered rows | 0 | 0 | `DeadLetterRowsPresent` |
| Calibration ECE | <= 0.25 with n >= 100 | <= 0.20 with n >= 500 | calibration drift panel |
| Company-intelligence score | >= 0.85 | >= 0.90 | benchmark regression alert |
| Product-value score | >= 0.70 | >= 0.85 | benchmark regression alert |
| Webhook end-to-end p95 | < 30 seconds | < 15 seconds | gateway/ingest latency alert |
| Duplicate observations | 0 in dry run | 0 sustained | duplicate-write alert |
| Privacy incidents | 0 | 0 | access override / cross-tenant alert |

## Migration Rehearsal

Before a release, rehearse migrations against a staging clone. Follow the full
[migration release runbook](migration-release-runbook.md) for expand/contract,
destructive migration approval, backup evidence, and rollback versus
forward-fix decisions.

1. Restore the latest production-like snapshot into the staging clone.
2. Apply `db/migrations/*.sql` exactly as the release process will apply them.
3. Run `uv run python scripts/check_schema_drift.py` with the staging clone DSN.
4. Run `uv run python scripts/run_production_readiness_gap_harness.py`.
5. Run the umbrella gate harness and attach its report to the release record.

Rollback is not considered rehearsed unless the release owner has also verified
that feature flags can pause autonomous writes without dropping queued work.

## Rollback Playbook

Use rollback for sustained SLO breach, privacy incident, migration drift, or
rising dead-letter rows.

1. Pause rollout traffic at the edge or feature flag layer.
2. Disable autonomous Think write paths for the affected tenant cohort.
3. Stop or scale down Think workers only after noting queue depth.
4. Leave read-only inspection paths online where possible.
5. Snapshot queue counts for `think_trigger_queue`, `model_reeval_queue`,
   `think_obligations`, and `pending_post_commit_actions`.
6. Revert the application build or disable the release flag.
7. Drain or quarantine queues only after the owner has classified whether work
   is safe to replay.
8. Re-run the feedback-loop gap harness and schema drift check before resuming.

Do not delete tenant data as a rollback mechanism. Quarantine bad autonomous
outputs through status/flag fields and preserve auditability.

## Deferred Checks

The staging cutover soak and shadow-mode customer-like traffic report are
temporarily deferred and are not part of the current automated readiness gate.
Keep these checks available for a later hardening pass.

### Shadow Mode

When re-enabled, shadow mode should run on real customer-like traffic.

Required report fields:

```json
{
  "duration_hours": 24,
  "canonical_mismatch_rate": 0.0,
  "shadow_write_error_rate": 0.0,
  "duplicate_observations": 0,
  "privacy_incidents": 0
}
```

For GA, prefer at least 72 hours of shadow traffic and compare against
representative tenant sizes, source mixes, and peak-hour volume.

### Soak And Load

The current harness only proves that the load generator works. The real soak is
the staging M-Load dry run documented in `docs/ingestion/m-load-runbook.md`;
it is not a current readiness blocker.

Release promotion requires:

- one full default-duration staging run, or an explicitly approved shorter run
  for emergency rollback validation
- throughput within 10 percent of configured QPS
- end-to-end p95 below 30 seconds
- zero duplicate observations after writer drain
- circuit breaker behavior observed during the injected lag window

## Permission And Privacy Audit

Before external rollout, review:

- tenant isolation tests in the umbrella harness
- RLS policy shape for tenant-scoped tables
- secret storage and webhook resolver tests
- access override logs for unexpected first-person/admin overrides
- logs and benchmark reports for raw customer text leakage

Any cross-tenant read, unexpected raw payload exposure, or secret-resolution
regression blocks production promotion.
