#!/usr/bin/env bash
# scripts/dev/create-kafka-topics.sh — idempotent topic provisioning
# for the ingestion dev stack.
#
# Per ingestion LLD §10 (production uses 64 partitions; dev uses 4 —
# enough for cooperative-sticky rebalancing tests without long startup
# costs). Retention varies per topic.
#
# Surfaces:
#   ingestion.raw         — M2: Kafka envelope (raw bytes pointer)        [7 day retention]
#   ingestion.normalized  — M2: ObservationDraft JSON                     [7 day retention]
#   ingestion.dlq         — M3.1: DLQ envelope → ingestion_failures       [30 day retention]
#   ingestion.embedding   — M3.2: embedding-needed signal (per obs)        [7 day retention]
#
# Why per-topic retention: LLD §1.3 — DLQ retention must outlive ops's
# typical triage window (incidents are flagged within hours but full
# RCA + replay decisions may take days). 30 days is the smallest
# retention that survives a long weekend + a sick operator without
# the source bytes evaporating.
#
# Idempotent via `--if-not-exists`. Safe to run repeatedly.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-fyralis_dev_kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
PARTITIONS_DEV="${PARTITIONS_DEV:-4}"

# Per-topic retention (milliseconds).
RETENTION_7D_MS="${RETENTION_7D_MS:-604800000}"     # 7 days
RETENTION_30D_MS="${RETENTION_30D_MS:-2592000000}"  # 30 days

if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
  echo "ERROR: kafka container '${KAFKA_CONTAINER}' is not running."
  echo "  Run: docker compose -f docker-compose.dev.yml up -d kafka"
  exit 1
fi

# Topic table: name|retention_ms.
declare -a TOPIC_SPECS=(
  "ingestion.raw|${RETENTION_7D_MS}"
  "ingestion.normalized|${RETENTION_7D_MS}"
  "ingestion.dlq|${RETENTION_30D_MS}"
  "ingestion.embedding|${RETENTION_7D_MS}"
)

for spec in "${TOPIC_SPECS[@]}"; do
  topic="${spec%%|*}"
  retention="${spec##*|}"
  echo "  + ${topic} (retention ${retention}ms)"
  docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS_DEV}" \
    --replication-factor 1 \
    --config "retention.ms=${retention}" \
    --config "compression.type=zstd" \
    >/dev/null
done

echo ""
echo "Topics present:"
docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" --list \
  | grep -E "^ingestion\." | sed 's/^/  /'
