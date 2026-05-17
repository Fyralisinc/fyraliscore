#!/usr/bin/env bash
# scripts/dev/create-kafka-topics.sh — idempotent topic provisioning
# for the M2 shadow-path dev stack.
#
# Per ingestion LLD §10 (production uses 64 partitions; dev uses 4 —
# enough for cooperative-sticky rebalancing tests without long startup
# costs). Retention 7 days, compression zstd.
#
# M2 surfaces only:
#   ingestion.raw         — Kafka envelope (small JSON pointer)
#   ingestion.normalized  — ObservationDraft JSON
#
# M3+ will add ingestion.dlq and ingestion.embedding. Do NOT create
# them here.
#
# Idempotent via `--if-not-exists`. Safe to run as part of
# `scripts/dev/m2-up.sh` after compose comes up.
set -euo pipefail

KAFKA_CONTAINER="${KAFKA_CONTAINER:-fyralis_dev_kafka}"
BOOTSTRAP="${KAFKA_BOOTSTRAP:-localhost:9092}"
PARTITIONS_DEV="${PARTITIONS_DEV:-4}"
RETENTION_MS="${RETENTION_MS:-604800000}"   # 7 days

if ! docker ps --format '{{.Names}}' | grep -q "^${KAFKA_CONTAINER}$"; then
  echo "ERROR: kafka container '${KAFKA_CONTAINER}' is not running."
  echo "  Run: docker compose -f docker-compose.dev.yml up -d kafka"
  exit 1
fi

declare -a TOPICS=(
  "ingestion.raw"
  "ingestion.normalized"
)

for topic in "${TOPICS[@]}"; do
  echo "  + ${topic}"
  docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server "${BOOTSTRAP}" \
    --create --if-not-exists \
    --topic "${topic}" \
    --partitions "${PARTITIONS_DEV}" \
    --replication-factor 1 \
    --config "retention.ms=${RETENTION_MS}" \
    --config "compression.type=zstd" \
    >/dev/null
done

echo ""
echo "Topics present:"
docker exec "${KAFKA_CONTAINER}" /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server "${BOOTSTRAP}" --list \
  | grep -E "^ingestion\." | sed 's/^/  /'
