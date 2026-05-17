#!/usr/bin/env bash
# scripts/dev/m2-up.sh — bring up the M2 shadow-path dev stack.
#
# Brings up Kafka (KRaft single broker) + moto-s3 mock, waits for both
# health checks, then provisions the M2 Kafka topics.
#
# Convention: bash scripts (matching `dogfood_up.sh`). No Makefile in
# this project.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.dev.yml}"

echo "==> Starting M2 dev stack ($COMPOSE_FILE)..."
docker compose -f "$COMPOSE_FILE" up -d

echo ""
echo "==> Waiting for services to report healthy..."
for svc in fyralis_dev_kafka fyralis_dev_moto_s3; do
  for i in $(seq 1 60); do
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$svc" 2>/dev/null || echo none)"
    if [ "$status" = "healthy" ]; then
      echo "  $svc: healthy"
      break
    fi
    sleep 1
    if [ "$i" = "60" ]; then
      echo "  $svc: still $status after 60s — check 'docker logs $svc'"
      exit 1
    fi
  done
done

echo ""
echo "==> Provisioning Kafka topics..."
bash "$ROOT/scripts/dev/create-kafka-topics.sh"

cat <<EOF

=== M2 dev stack up ===
  Kafka broker:   localhost:9092 (PLAINTEXT)
  Kafka mgmt:     docker exec fyralis_dev_kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
  S3 (moto):      http://localhost:5001  (use --endpoint-url with awscli)

  Stop:           scripts/dev/m2-down.sh
  Reset (nuke):   scripts/dev/m2-reset.sh
  Logs:           docker compose -f docker-compose.dev.yml logs -f

  Next: run normalizer/writer/webhook with the env from docs/dev-setup.md.
EOF
