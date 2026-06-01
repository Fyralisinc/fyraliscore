"""CLI: `python -m services.ingestion.writers.dlq_writer`.

Per M3.1 work order. Env vars:
  KAFKA_BOOTSTRAP_SERVERS  — Kafka brokers (default localhost:9092)
  DATABASE_URL             — Postgres DSN (required)
  POSTGRES_POOL_SIZE       — asyncpg max pool size (default 5)
  DLQ_WRITER_LOG_LEVEL     — log level (default INFO)
"""
from __future__ import annotations

from services.ingestion.writers.dlq_writer.dlq_writer import main


if __name__ == "__main__":
    main()
