# Ingestion source isolation

**Status:** in progress (branch `feature/ingestion-source-isolation`, off `integration/ingestion-hardening`)
**Author:** ingestion pipeline hardening
**Related:** [03-low-level-design.md](03-low-level-design.md), [05-lld-amendments.md](05-lld-amendments.md)

## Problem

Every source (`slack`, `github`, `discord`, `gmail`, `notion`, `jira`,
`google_calendar`, `google_drive`) shares one physical pipeline:

```
ingress ─▶ ingestion.raw ─▶ normalizer ─▶ ingestion.normalized
        ─▶ observation-writer ─▶ ingestion.embedding ─▶ embedding-worker
        (failures) ─▶ ingestion.dlq ─▶ dlq-writer
```

Two structural facts couple the sources together so they **cannot operate
independently**:

1. **One topic per stage, keyed by `tenant_id` only.** Every producer keys
   its message by `str(tenant_id)`, so a tenant's slack + github + … messages
   all hash to the **same partition**. The normalizer consumes a partition
   **strictly serially** (`worker.py` reads one message, `await`s an S3 GET +
   handler, commits, then reads the next). A slow or large message for one
   source therefore **head-of-line blocks every other source** behind it on
   that partition. See `docs/ingestion/source-isolation.md` "Lag examples".

2. **Shared downstream resources.** The observation-writer, embedding-worker,
   and dlq-writer each run as a single process with one asyncpg pool and (for
   embedding) one Ollama endpoint. One source's backfill burst can exhaust the
   shared pool / embedder queue and starve other sources.

### Where the lag surfaces (motivating examples)

| Symptom | Real cause | Mechanism |
|---|---|---|
| Slack lands late, slack input flat | a github message ahead of it on the same partition | serial per-partition loop + slow S3 GET |
| brief lag spike + `parse_failure` ticks | poison message from any source | inline DLQ publish in the loop |
| writer latency up, no input spike | another source's backfill holding pool conns | shared asyncpg pool |
| `embedding IS NULL` lingers | another source flooding the embedder | single Ollama endpoint / one consumer group |

In every case the source that *experiences* the lag is healthy and the source
*causing* it is invisible from the victim's vantage point.

## Goals

- Each source ingests on its own physical lane end-to-end; lag, failures, and
  backpressure in one source do not affect another.
- Per-source horizontal scaling: a noisy source can be given more workers
  without touching the others.
- Preserve the existing correctness invariants: at-least-once delivery,
  per-`(tenant, source)` ordering, S3/observation idempotency, the PRIME
  DIRECTIVE (shadow/embedding/dlq publish never crash the caller), and Path B
  (normalizer touches no DB).

## Non-goals

- Per-tenant physical isolation (tenants still share a source's lane; tenant
  fairness within a source's partitions is a separate concern).
- Changing the envelope schema or the handler registry.

## Design

### 1. Per-source topics

Split each data-plane stage into one topic per source:

```
ingestion.raw.{source}          e.g. ingestion.raw.slack
ingestion.normalized.{source}
ingestion.embedding.{source}
ingestion.dlq.{source}
```

Control-plane topics (`ingestion.tenant_traffic_signal`, progress) stay
single — they are per-tenant signals, not per-source data.

A single module — `services/ingestion/kafka/topics.py` — is the **one source
of truth** for topic naming. Producers, consumers, the provisioner, and the
circuit-breaker lag probe all derive names from it. The canonical source list
is `RawEnvelope`'s `SourceLiteral` so the two can never drift.

Within a source's topic, messages stay **keyed by `tenant_id`** — this keeps
per-`(tenant, source)` ordering and the "this tenant's queue is stuck on
partition N" debuggability the LLD calls out.

### 2. Per-source workers

Each worker reads an optional `INGESTION_SOURCE` env var:

- **set** (e.g. `INGESTION_SOURCE=slack`): the worker subscribes only to that
  source's topic and joins a per-source consumer group
  (`normalizer.slack`, `observation-writer.slack`, …). This is the production
  deployment shape — one (or more) worker processes per source, each with its
  own asyncpg pool and its own lag/offset.
- **unset**: the worker subscribes to **all** per-source topics for its stage
  and uses the base group (`normalizer`, …). This is the dev / single-process
  shape and the backward-compatible default for the sandbox and tests.

Because production runs one process per source, **per-source pool budgets come
for free** (each process owns its pool). The pool size stays env-tunable
(`POSTGRES_POOL_SIZE`, and a new `WRITER_POSTGRES_POOL_SIZE` for the
observation-writer).

### 3. Per-source embedder concurrency budget

Ollama is the one downstream resource a per-source process can still oversubscribe
when many embedding workers share one endpoint. The embedding worker gains an
`EMBEDDING_MAX_CONCURRENCY` budget (a bounded `asyncio.Semaphore`) so a single
source's worker cannot monopolize the shared model server.

### 4. Concurrent, order-preserving normalizer

The normalizer's serial S3 GET is the dominant in-lane latency. We replace the
one-at-a-time loop with a **bounded, per-tenant-ordered concurrent** loop:

- pull a batch (`getmany`),
- group the batch by `tenant_id`,
- process tenants **concurrently** (their S3 GETs overlap) but messages within
  a tenant **serially** (preserves per-tenant ordering),
- commit the batch offset only after all complete (at-least-once preserved;
  reprocessing is idempotent via S3 PutIfAbsent + the observations unique index).

Concurrency is bounded by `NORMALIZER_MAX_CONCURRENCY` so a burst cannot
exhaust S3 connections.

### 5. Partition-count drift fix

`services/webhooks/router.py::_kafka_partition_for_tenant` hardcodes
`num_partitions=32`, but topics are provisioned with `KAFKA_TOPIC_PARTITIONS`
(default 12). The partition label it computes is therefore wrong. We read the
real count from `KAFKA_TOPIC_PARTITIONS` so the metric matches the broker.

## Correctness arguments

- **At-least-once.** Offsets commit only after work completes (normalizer:
  after the whole batch; writers: per their existing post-process commit).
  Crash mid-batch ⇒ reprocess ⇒ idempotent (S3 PutIfAbsent, observations
  unique index, DLQ upsert unique index).
- **Per-(tenant, source) ordering.** Each source has its own topic; within it,
  tenant-keyed partitioning + per-tenant-serial processing preserve order.
  Cross-tenant reordering inside a batch is acceptable (tenants are independent).
- **Path B.** The normalizer still imports no DB modules; concurrency uses only
  asyncio + the existing S3/producer clients.
- **PRIME DIRECTIVE.** Producer-routing changes only change the *topic name*
  argument; the surrounding try/except that protects the caller is untouched.

## Rollout

Topic names change, so this is **not** a hot in-place swap. Procedure:

1. Deploy the provisioner change ⇒ new per-source topics are created
   (old topics remain).
2. Let the old single-topic consumers drain to zero lag.
3. Deploy producers + consumers (this PR) ⇒ traffic flows on per-source topics.
4. After a retention window, delete the now-empty legacy topics.

The dev/sandbox path (no `INGESTION_SOURCE`) keeps working throughout because
the all-sources fallback subscribes to every per-source topic.

## Phase map (implementation)

| Phase | Change |
|---|---|
| 1 | `kafka/topics.py` registry + tests |
| 2 | producers route to per-source topics |
| 3 | consumers subscribe per-source (env-driven) + per-source groups |
| 4 | provisioner generates per-source topics from the registry |
| 5 | docker-compose per-source worker services |
| 6 | normalizer concurrent, order-preserving processing |
| 7 | per-source pool + embedder concurrency budgets |
| 8 | partition-count drift fix |

Each phase is a self-contained commit.

## Known follow-ups (deliberately deferred)

- **Circuit breaker per-source lag.** `services/ingestion/feature_flags/
  circuit_breaker.py` measures consumer lag on a single `raw_topic`. With
  per-source raw topics, a tenant can lag on one source's lane while healthy on
  another. The breaker should measure lag across every
  `topics_for_stage("raw")` lane (per-source consumer groups) and trip on the
  worst-case lane. Left at the legacy `raw_topic` it fails SAFE (never
  false-trips) but is inert. It's a cutover-era safety net, separate from
  steady-state isolation — deferred, not forgotten.
- **Per-source topic auto-provisioning on source addition.** Adding a source to
  `SourceLiteral` makes its four topics appear in the registry automatically,
  but the provisioner must be re-run on deploy to create them (auto-create is
  off). Documented in the rollout section.
