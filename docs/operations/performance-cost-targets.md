# Performance, Scale, And Cost Targets

Owner: Platform Engineering.
Last reviewed: 2026-06-24.

This page defines the launch tenant profiles that staging load, soak, and cost
tests must simulate. These are engineering targets, not customer contractual
SLOs.

## Tenant Profiles

| Dimension | Beta design partner | GA launch tenant |
| --- | ---: | ---: |
| Users | 50 | 250 |
| Enabled sources | 6 | 12 |
| Active source installs | 10 | 30 |
| Historical backfill window | 180 days | 365 days |
| Historical observations | 250,000 | 2,000,000 |
| New observations/day | 25,000 | 150,000 |
| Peak webhook events/minute | 500 | 2,500 |
| Largest single object/blob | 25 MB | 100 MB |
| Daily object/blob ingest | 10 GB | 100 GB |
| Active models | 50,000 | 500,000 |
| Relationship edges | 500,000 | 5,000,000 |
| Think triggers/day | 5,000 | 50,000 |
| Ask requests/day | 1,000 | 10,000 |
| Today/CEO view requests/day | 2,500 | 25,000 |
| Recommendation/decision actions/day | 250 | 2,500 |
| Source pause/resume/uninstall actions/day | 10 | 100 |

## Latency And Drain Targets

| Surface | Beta target | GA target |
| --- | ---: | ---: |
| Gateway product read p95 | <= 2.0s | <= 1.5s |
| Gateway product read p99 | <= 5.0s | <= 3.0s |
| Ask p95 | <= 8.0s | <= 6.0s |
| Webhook accept p95 | <= 500ms | <= 300ms |
| Webhook to observation p95 | <= 30s | <= 15s |
| Source backfill small tenant | <= 24h | <= 12h |
| Think queue steady-state drain | <= 15m | <= 5m |
| Post-commit queue drain | <= 5m | <= 2m |
| DLQ unresolved target | 0 sustained | 0 sustained |

## Cost Budgets

| Budget | Beta limit | GA limit |
| --- | ---: | ---: |
| Think LLM spend/tenant/day | $25 | $100 |
| Think LLM tokens/tenant/day | 5,000,000 | 25,000,000 |
| Think LLM requests/tenant/day | 5,000 | 50,000 |
| Embedding spend/tenant/day | $10 | $50 |
| Object storage growth/tenant/month | 300 GB | 3 TB |
| Postgres storage growth/tenant/month | 50 GB | 500 GB |
| Source API calls/source/day | Provider-specific | Provider-specific |

Think currently enforces daily spend/token/request deferral when
`THINK_DAILY_BUDGET_ENFORCEMENT=1`. Source page fetches have token-bucket
rate-limit gates and bounded 429 retry budgets. Embedding spend and a full
provider-specific source API call-ceiling model still need enforcement before
GA.

To estimate launch costs from the same beta/GA usage profiles, run:

```bash
uv run python scripts/estimate_production_cost_profile.py beta
uv run python scripts/estimate_production_cost_profile.py ga \
  --object-storage-usd-per-gb-month 0.023 \
  --postgres-storage-usd-per-gb-month 0.115
```

The estimator keeps provider pricing configurable. Attach the rendered JSON to
soak reports with the exact unit prices used for that environment.

## Required Load Data Shape

Synthetic datasets must include:

- multiple source families in the same tenant
- at least one noisy source lane with bursty webhook traffic
- duplicated and out-of-order events
- large objects and attachment metadata
- source-specific rate-limit and retry responses
- models with dense relationship edges and sparse evidence
- stale credentials and revoked-provider cases
- mixed product reads while ingestion and Think are active

## Executable Load Profiles

The machine-readable launch profiles live in
`services.platform.performance.load_profiles` and can be rendered with:

```bash
uv run python scripts/plan_production_load_profile.py beta
uv run python scripts/plan_production_load_profile.py ga --scale 0.01 --duration-s 300
```

To produce the environment variables for the staging M-Load sender:

```bash
uv run python scripts/plan_production_load_profile.py beta --format env
```

The generated plan includes:

- synthetic dataset counts for users, source installs, observations, active
  models, relationship edges, blob ingest, and product actions
- average and peak rates for ingestion, Think, Ask, Today, and recommendation
  actions
- M-Load settings for `services.ingest.synthetic.cutover_load`, including
  `CUTOVER_DRYRUN_PROVIDER_WEIGHTS` so noisy webhook sources can be weighted
  deliberately

## Load Test Acceptance

A load or soak report is valid only if it records:

- profile used: beta or GA
- dataset seed and source mix
- service versions and deployment SHA
- database size before and after
- object-store growth
- gateway p95/p99 by product workflow
- queue depth and drain time by worker family
- DLQ counts and failure kinds
- provider/API rate-limit events
- per-tenant LLM spend and token totals
- CPU, memory, DB connection pool, and disk saturation
- noisy-source isolation result

The first launch may use the beta profile only. GA requires passing the GA
profile or explicitly reducing the supported tenant-size claim.
