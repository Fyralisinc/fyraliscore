# Local Dev Setup — M2 Shadow Path

This page covers the local infrastructure introduced by the ingestion
M2 work order. It does NOT cover the pre-existing Postgres + Ollama
dev stack (the `dogfood_up.sh` workflow); that stack stays as-is.

## Prerequisites

| Tool       | Version | Why |
|---         |---      |--- |
| Docker     | 24+     | Kafka + moto-s3 containers |
| Docker Compose | v2 (`docker compose`) | New file is `docker-compose.dev.yml` |
| Python     | 3.12    | `.venv` setup; project-wide |
| Postgres   | 16 (pgvector) | Project's existing dev DB on `localhost:5433` — outside this stack |

The Postgres + Ollama stack is unchanged from earlier milestones; this
doc only describes the new M2 surface.

## Start the M2 dev stack

```bash
# One-shot: brings up Kafka + moto-s3, waits for health, creates topics.
bash scripts/dev/m2-up.sh
```

After it returns, the following are reachable:

| Service | Endpoint | Verify |
|---|---|---|
| Kafka broker | `localhost:9092` (PLAINTEXT) | `docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list` |
| moto S3 mock | `http://localhost:5001` | `curl -sS http://localhost:5001/moto-api/` returns JSON |

`m2-up.sh` is idempotent — running it twice is safe; existing topics
stay (Kafka topic creation uses `--if-not-exists`).

## Lifecycle helpers

| Action | Command | Notes |
|---|---|---|
| Bring up | `bash scripts/dev/m2-up.sh` | Compose + health-wait + topics |
| Stop (preserve volumes) | `bash scripts/dev/m2-down.sh` | `docker compose down` |
| Logs (tail) | `bash scripts/dev/m2-logs.sh` | `-f` follows; pass service name to filter |
| Nuke and restart | `bash scripts/dev/m2-reset.sh` | `down -v` then `up` — Kafka data lost |

## Topics on `ingestion.raw` and `ingestion.normalized`

M2 creates exactly two topics:

| Topic | Partitions (dev / prod) | Retention | Compression |
|---|---|---|---|
| `ingestion.raw` | 4 / 64 | 7 days | zstd |
| `ingestion.normalized` | 4 / 64 | 7 days | zstd |

Production partition count comes from LLD §10; dev is reduced to keep
local startup fast while still exercising cooperative-sticky
rebalances (which need ≥2 partitions per consumer to be meaningful).

`ingestion.dlq` and `ingestion.embedding` are NOT created in M2 —
those land in M3.

## Inspect Kafka from the host

```bash
# List topics
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --list

# Topic config / partition layout
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --describe --topic ingestion.raw

# Tail messages from a topic (newest first via `--from-beginning` omitted)
docker exec -it fyralis_dev_kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic ingestion.raw

# Consumer group lag (useful for verifying the normalizer is keeping up)
docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --all-groups
```

## Inspect S3 (moto) from the host

```bash
# moto's admin API
curl -sS http://localhost:5001/moto-api/

# List buckets via awscli (any creds — moto ignores them)
AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy aws \
    --endpoint-url http://localhost:5001 \
    s3 ls

# Inspect a raw-tier object after the shadow path writes
AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy aws \
    --endpoint-url http://localhost:5001 \
    s3 ls s3://fyralis-raw/dev/slack/ --recursive
```

The bucket `fyralis-raw` is created by the first shadow write (M2.1+);
moto auto-creates buckets on first PUT.

## Run M2 services locally

These commands assume the M2 dev stack is up (`m2-up.sh` ran cleanly)
and the project's Postgres is reachable at `localhost:5433`.

### Configuration via env vars

The M2 services read configuration from environment. The standard
mapping for local dev:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export S3_ENDPOINT_URL=http://localhost:5001
export AWS_ACCESS_KEY_ID=dummy
export AWS_SECRET_ACCESS_KEY=dummy
export AWS_REGION=us-east-1
export S3_RAW_BUCKET=fyralis-raw
export INGESTION_ENV=dev    # used in the S3 key prefix per LLD §5.1
```

Put these in `.env.dev` alongside the existing `.env`; `dogfood_up.sh`
already does an `if [ -f .env.dogfood ]; then source` pattern that can
be extended.

### Webhook router with shadow path (lands in M2.1)

The gateway runs as today via `dogfood_up.sh`; the M2.1 changes are
shadow-path additions and activate automatically when the env above is
set. The feature flag `ingestion.shadow_write_enabled` in `tenant_flags`
gates per-tenant; default is on.

### Normalizer worker (lands in M2.3)

```bash
python -m services.ingestion.normalizer
```

Consumes `ingestion.raw`, produces to `ingestion.normalized`. POOL_SIZE
defaults to `os.cpu_count()`; override via `POOL_SIZE=2 python -m …`
for predictable rebalance behaviour during local testing.

### Observation writer (lands in M2.4 — no-op mode)

```bash
python -m services.ingestion.writers.observation_writer
```

Consumes `ingestion.normalized`, logs `shadow_write_event` per message.
**Does not insert observations in M2.** Dual-mode insertion lands in M5.

## Running M2 unit + integration tests

```bash
# Unit tests — no docker stack required
.venv/bin/python -m pytest services/ingestion/ services/webhooks/ -v

# Integration tests that need Kafka + moto S3 (mark = integration AND
# requires_docker). Bring the stack up first.
bash scripts/dev/m2-up.sh
.venv/bin/python -m pytest -m "requires_docker" -v
```

`@pytest.mark.requires_docker` skips cleanly if Docker isn't running.

## When things go wrong

| Symptom | Likely cause | Fix |
|---|---|---|
| `m2-up.sh` hangs at "Waiting for services to report healthy" | Old Kafka volume from a different cluster id | `bash scripts/dev/m2-reset.sh` |
| `Topic 'ingestion.raw' already exists` error | Re-running create-topics on an existing topic | Expected — the script uses `--if-not-exists`; the line is just diagnostic |
| Worker can't reach Kafka from a container | Worker on the compose network needs `kafka:9092`, not `localhost:9092` | Override `KAFKA_BOOTSTRAP_SERVERS` per environment |
| S3 PUTs fail with `SignatureDoesNotMatch` | moto needs `AWS_REGION` set; some boto3 paths sign with the real region | Set `AWS_REGION=us-east-1` |
