"""CLI: `python -m services.ingestion.recovery.embedding_backlog`.

Per M3.3 work order. Env vars:
  DATABASE_URL                — Postgres DSN (required)
  REDIS_URL                   — Redis DSN for the rate limiter
                                (default redis://localhost:6379/0)
  OLLAMA_URL                  — Ollama base URL (default localhost:11434)
  OLLAMA_EMBED_MODEL          — Ollama model (default nomic-embed-text)
  BACKFILL_OLLAMA_QPS         — rate limit (refill_per_sec) for the
                                Lua bucket; 0 means "paused" via the
                                -1 sentinel. Default 10.
  BACKFILL_INSTANCE_NAME      — cursor row key (default "default").
  BACKFILL_BATCH_SIZE         — max rows per SELECT (default 50).
  EMBEDDING_BACKLOG_LOG_LEVEL — log level (default INFO).

The service handles SIGTERM by completing its current iteration
(at most one row) and then exiting cleanly. Cursor state is
persisted after every advance, so a SIGTERM mid-flight does not
re-process the row.
"""
from __future__ import annotations

from services.ingestion.recovery.embedding_backlog.embedding_backlog import main


if __name__ == "__main__":
    main()
