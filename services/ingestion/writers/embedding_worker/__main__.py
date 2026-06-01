"""CLI: `python -m services.ingestion.writers.embedding_worker`.

Per M3.2 work order. Env vars:
  KAFKA_BOOTSTRAP_SERVERS  — Kafka brokers (default localhost:9092)
  DATABASE_URL             — Postgres DSN (required)
  POSTGRES_POOL_SIZE       — asyncpg max pool size (default 5)
  OLLAMA_URL               — Ollama base URL (default localhost:11434)
  OLLAMA_EMBED_MODEL       — Ollama embedding model (default nomic-embed-text)
  EMBEDDING_WORKER_LOG_LEVEL — log level (default INFO)
"""
from __future__ import annotations

from services.ingestion.writers.embedding_worker.embedding_worker import main


if __name__ == "__main__":
    main()
