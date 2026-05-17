"""services/ingestion/recovery/embedding_backlog — backlog drainer.

Per ingestion LLD §12.1 + LLD-amendment A4 (reshape from "one-shot
script" to long-running rate-limited service). M3.3.

This package contains the embedding backlog drainer:
  - Scans `observations WHERE embedding_pending = TRUE` ordered by
    `(ingested_at, id)`.
  - Acquires one token from the M1.3 Lua bucket per Ollama call.
  - Persists its scan cursor in `embedding_backlog_state` so a
    SIGTERM + restart resumes from the same point.
  - When BACKFILL_OLLAMA_QPS=0, the bucket returns the -1 sentinel
    (see services/ingestion/rate_limit/scripts/acquire.lua) and the
    service stalls indefinitely without thrashing — the operator's
    pause switch.

This is the second Path A surface in the new pipeline (first was
the M3.1 DLQ writer; M3.2 embedding worker added the third).
"""
from services.ingestion.recovery.embedding_backlog.embedding_backlog import (
    BACKLOG_BUCKET_KEY,
    BacklogConfig,
    get_metrics,
    main,
    reset_metrics,
    run_backlog_service,
)

__all__ = [
    "BACKLOG_BUCKET_KEY",
    "BacklogConfig",
    "get_metrics",
    "main",
    "reset_metrics",
    "run_backlog_service",
]
