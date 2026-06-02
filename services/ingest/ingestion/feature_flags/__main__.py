"""CLI entry for the ingestion cutover circuit breaker.

Run with: `python -m services.ingest.ingestion.feature_flags`

A single instance measures lag on EVERY per-source raw lane
(`ingestion.raw.<source>` vs. group `<BREAKER_NORMALIZER_GROUP_BASE>.<source>`)
and trips each tenant on its worst lane — so run ONE breaker, not one
per source.

Env vars:
  DATABASE_URL                       — Postgres DSN (required)
  KAFKA_BOOTSTRAP_SERVERS            — Kafka brokers (default localhost:9092)
  BREAKER_INSTANCE_NAME              — instance name for state row (default "default")
  BREAKER_TICK_INTERVAL_SEC          — seconds between ticks (default 60)
  BREAKER_THRESHOLD_SEC              — lag threshold for breach (default 60)
  BREAKER_WINDOW_TICKS               — consecutive ticks to trip (default 5)
  BREAKER_NORMALIZER_GROUP_BASE      — normalizer consumer-group base (default "normalizer")
  INGESTION_ALERT_WEBHOOK_URL        — optional ops-alert webhook for trips
  CIRCUIT_BREAKER_LOG_LEVEL          — logging level (default INFO)

Test-injection env vars (M5.1 subprocess test pattern):
  M5_BREAKER_FAKE_LAG_PARTITIONS     — JSON {"<source>": {"<partition>": <lag_seconds>}}
  M5_BREAKER_FAKE_ACTIVE_TENANTS     — JSON {"<tenant_uuid>": {"<source>": <partition>}}

Both must be set together for synthetic mode; otherwise production
Kafka lag and signal-topic readers are used.
"""
from __future__ import annotations

from services.ingest.ingestion.feature_flags.circuit_breaker import main


if __name__ == "__main__":
    main()
