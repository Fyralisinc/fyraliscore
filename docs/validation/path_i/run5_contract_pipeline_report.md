# Run 5 — Contract Pipeline Matrix

Date: 2026-07-27

## Result

The final working-tree matrix executed every canonical source through its
generated `local_correctness` binding against dedicated loopback PostgreSQL,
Kafka, moto/S3, Redis, and Provider Lab services.

- 27/27 pipeline probes passed.
- 133/133 applicable pipeline scenarios passed.
- 2,370 expected Observations were persisted and identity-checked.
- 26 historical sources proved two tenants × two exact installations × two
  worker replicas.
- WhatsApp proved its three applicable live-only scenarios. Its contract
  intentionally declares no historical fetch.
- Every run verified raw S3 evidence, raw and normalized Kafka records,
  Observation persistence, exactly one same-tenant T1 trigger, and replay
  idempotency.
- No pipeline error or tenant-cleanup failure was reported.

The scenario ledger contains another 225 source-specific scenarios in the
blocked state. This run does not promote those scenarios and is not a
substitute for quota-aware throughput, fault/soak, verified provider evidence,
or real-provider canaries.

## Per-source results

| Source | Mode | Passed scenarios | Expected Observations | Replicas | Seconds |
|---|---:|---:|---:|---:|---:|
| Ashby | history | 5 | 96 | 2 | 13.95 |
| AWS | history | 5 | 12 | 2 | 9.16 |
| Brex | history | 5 | 20 | 2 | 10.80 |
| Carta | history | 5 | 16 | 2 | 10.59 |
| Deel | history | 5 | 20 | 2 | 10.69 |
| Discord | history | 5 | 480 | 2 | 38.76 |
| Facebook Pages | history | 5 | 24 | 2 | 11.41 |
| Figma | history | 5 | 20 | 2 | 11.54 |
| Fireflies | history | 5 | 16 | 2 | 10.22 |
| GitHub | history | 5 | 800 | 2 | 59.38 |
| Gmail | history | 5 | 40 | 2 | 11.68 |
| Google Calendar | history | 5 | 24 | 2 | 10.87 |
| Google Drive | history | 5 | 12 | 2 | 8.75 |
| Grafana | history | 5 | 20 | 2 | 11.03 |
| Gusto | history | 5 | 8 | 2 | 7.98 |
| HiBob | history | 5 | 16 | 2 | 9.76 |
| Jira | history | 5 | 12 | 2 | 10.22 |
| LinkedIn | history | 5 | 12 | 2 | 8.31 |
| Mercury | history | 5 | 20 | 2 | 9.92 |
| Miro | history | 5 | 16 | 2 | 10.13 |
| Notion | history | 5 | 12 | 2 | 11.89 |
| QuickBooks | history | 5 | 16 | 2 | 9.52 |
| Ramp | history | 5 | 16 | 2 | 10.05 |
| Signal | history | 5 | 20 | 2 | 10.84 |
| Slack | history | 5 | 600 | 2 | 40.45 |
| Telegram | history | 5 | 20 | 2 | 10.50 |
| WhatsApp | live only | 3 | 2 | 1 | 6.88 |

The sum of per-source pipeline durations was 385.28 seconds. Sources were run
sequentially to prevent shared consumer groups or database fixture resets from
masking ownership and attribution defects.

## Correctness defects closed during the run

- The normalizer now requires broker acknowledgement before committing the
  raw input offset.
- Kafka generation changes after normalized output or Observation persistence
  are treated as replayable at-least-once handoffs instead of worker crashes.
- Replay evidence selects one deterministic raw delivery per unique,
  successfully normalized S3 parent, so pre-existing at-least-once duplicates
  and deliberate DLQ inputs cannot distort the expected growth.
- The replicated harness waits for every durable workflow replica before
  releasing exact onboarding triggers and fails immediately if a required
  subprocess exits.
- Tenant onboarding consumes terminal orphan completion signals safely after a
  tenant/run deletion.
- Figma certification provisions both the raw-evidence and durable-blob S3
  buckets.

## Reproduction boundary

The matrix used:

```text
PostgreSQL  postgresql://company_os@127.0.0.1:55444/fyralis_certification_pipeline
Kafka      127.0.0.1:59092
S3         http://127.0.0.1:5601
Raw bucket fyralis-certification-raw
```

For each source, the generated plan hash from
`services/ingest/source_certification/execution_bindings/<source>.json` was
passed to:

```bash
python -m services.ingest.source_certification.execution_driver \
  --source <source> \
  --stage local_correctness \
  --plan-sha256 <generated-plan-hash>
```

This is local working-tree evidence, not a signed release artifact from a
clean commit. Release promotion remains fail-closed until the remaining
scenario, evidence, quota, throughput, soak, and live-canary gates pass.
