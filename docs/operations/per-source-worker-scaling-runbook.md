# Per-Source Worker Scaling Runbook

This runbook describes how to scale Fyralis ingestion workers when one source
is noisy without letting it block other source families.

## Scope

Applies to the Kafka-first ingestion data plane:

- `normalizer`
- `observation_writer`
- `embedding_worker`
- `summarization_worker`

The per-source topology is generated from `RawEnvelope.SourceLiteral` by
`scripts/gen_per_source_compose.py` into `docker-compose.per-source.yml`. Each
service is pinned with `INGESTION_SOURCE=<source>` and consumes only that
source's Kafka lane.

## Invariants

- Kafka auto-create stays disabled. `kafka-init` must complete successfully
  before any ingestion worker starts.
- Each source has isolated topics:
  `ingestion.raw.<source>`, `ingestion.normalized.<source>`,
  `ingestion.embedding.<source>`, `ingestion.summarization.<source>`, and
  `ingestion.dlq.<source>`.
- Per-source workers join per-source consumer groups through
  `topics.consumer_group(stage_group, source)`.
- Do not mix all-source singleton workers with per-source workers for the same
  stage in production. Scale the all-source services to zero when using the
  per-source overlay.

## Signals To Watch

Use the worker `/metrics` endpoint and Kafka consumer lag:

| Stage | Primary symptom | First knob |
|---|---|---|
| Raw to normalized | `normalizer.consumer_lag_seconds_last` rising for one source | Add `normalizer_<source>` replicas or raise `NORMALIZER_MAX_CONCURRENCY` |
| Normalized to observations | `writer.messages_consumed` flat while lag rises | Add `observation_writer_<source>` replicas or raise `WRITER_POSTGRES_POOL_SIZE` carefully |
| Embedding backlog | `embedding_worker` lag or embedding backlog pending grows | Add `embedding_worker_<source>` replicas or raise `EMBEDDING_MAX_CONCURRENCY` within Ollama/OpenAI limits |
| Summarization backlog | `summarization_worker` lag grows | Add `summarization_worker_<source>` replicas and verify LLM spend ceilings |
| DLQ growth | `*.dlq_publish.success` increasing | Pause scale-up; inspect DLQ cause before adding throughput |

## Compose Procedure

Regenerate the overlay after adding or removing a source:

```bash
uv run python scripts/gen_per_source_compose.py > docker-compose.per-source.yml
```

Start the per-source topology and disable all-source singleton workers:

```bash
docker compose -f docker-compose.yml -f docker-compose.per-source.yml up -d \
  --scale normalizer=0 \
  --scale observation_writer=0 \
  --scale summarization_worker=0 \
  --scale embedding_worker=0
```

Scale a noisy source lane:

```bash
docker compose -f docker-compose.yml -f docker-compose.per-source.yml up -d \
  --scale normalizer_slack=3 \
  --scale observation_writer_slack=2 \
  --scale embedding_worker_slack=2
```

Scale back down only after lag has stayed near zero for at least one retention
window of the alerting dashboard:

```bash
docker compose -f docker-compose.yml -f docker-compose.per-source.yml up -d \
  --scale normalizer_slack=1 \
  --scale observation_writer_slack=1 \
  --scale embedding_worker_slack=1
```

## Kubernetes Procedure

For Kubernetes, model each source/stage as a separate Deployment with:

- `INGESTION_SOURCE=<source>`
- one Deployment per stage/source pair
- independent HPA or KEDA scaling by Kafka consumer lag
- resource requests sized per source family
- `kafka-init` or equivalent topic-provisioning Job as a required pre-deploy
  hook

Recommended initial bounds:

| Worker | Min | Max | Scale metric |
|---|---:|---:|---|
| `normalizer_<source>` | 1 | 8 | Raw topic consumer lag |
| `observation_writer_<source>` | 1 | 6 | Normalized topic consumer lag and DB pool saturation |
| `embedding_worker_<source>` | 1 | 4 | Embedding topic lag and provider concurrency budget |
| `summarization_worker_<source>` | 0 | 4 | Summarization topic lag and LLM spend budget |

## Safety Checks Before Scaling

1. Confirm the source is actually lagging, not failing:
   - lag increasing
   - worker health is 200
   - DLQ rate is not rising for the same source
2. Confirm downstream dependencies have headroom:
   - Postgres pool utilization
   - Ollama/OpenAI embedding concurrency
   - LLM spend and token ceilings
   - S3 read/write latency
3. Confirm `kafka_path_enabled` has not tripped off for the tenant. If the
   circuit breaker has disabled Kafka path, scale-up will not help live ingress.
4. Prefer scaling the earliest lagging stage first. Do not scale downstream
   workers while upstream is the actual bottleneck.

## Rollback

If scale-up causes dependency saturation:

1. Reduce the newest replicas first.
2. Re-check DLQ and retry metrics for the affected source.
3. If customer-facing live ingestion is affected, disable Kafka path for the
   tenant so live webhooks fall back to inline ingestion.
4. Re-enable Kafka path only after the noisy lane drains and `kafka-init`
   verification is still passing:

```bash
python scripts/reenable_kafka_path.py "$TENANT_ID" \
  --operator-actor "$OPERATOR_ACTOR_ID" \
  --note "lane drained and kafka-init verification passed"
```

The operator actor must have tenant-wide `admin` or `leadership`; the command
writes a `kafka_path.reenable` row to `operator_action_log`.

## Completion Criteria

A scaling change is complete when:

- source-specific lag is back inside the SLO window
- other source lanes show no lag regression
- DLQ rate is not elevated
- worker health endpoints are 200
- cost ceilings for embedding and summarization providers remain under budget
