"""services/code_intel/embed.py — best-effort code-RAG embedding fill.

Mirrors the ingestion `embedding_worker` pattern: the indexer writes
`code_embeddings` rows with `embedding_pending=TRUE`; this pass batches them
through the shared embedder (Ollama nomic-embed-text, 768-d) and flips the flag.
Fully optional — if the embedder is down, the graph stays queryable for blast
radius (which needs edges, not vectors); embeddings just stay pending.
"""
from __future__ import annotations

from typing import Any
from uuid import UUID

from lib.shared.tenant_context import tenant_transaction


def _vec(embedding: list[float]) -> str:
    return "[" + ",".join(repr(float(x)) for x in embedding) + "]"


async def fill_pending_embeddings(
    *,
    pool: Any,
    tenant_id: UUID,
    snapshot_id: UUID | None = None,
    embedder: Any | None = None,
    batch_size: int = 64,
    max_batches: int = 1000,
) -> int:
    """Embed pending code chunks. Returns the number filled. Never raises on
    embedder failure — leaves rows pending for a later pass."""
    if embedder is None:
        try:
            from lib.embeddings.factory import make_embedder
            embedder = make_embedder()
        except Exception:  # noqa: BLE001
            return 0

    filled = 0
    model_name = getattr(embedder, "model_name", None)
    for _ in range(max_batches):
        async with tenant_transaction(tenant_id, pool=pool) as ctx:
            if snapshot_id is not None:
                rows = await ctx.fetch(
                    "SELECT id, content_text FROM code_embeddings "
                    "WHERE embedding_pending=TRUE AND snapshot_id=$1 LIMIT $2",
                    snapshot_id, batch_size,
                )
            else:
                rows = await ctx.fetch(
                    "SELECT id, content_text FROM code_embeddings "
                    "WHERE embedding_pending=TRUE AND tenant_id=$1 LIMIT $2",
                    tenant_id, batch_size,
                )
            if not rows:
                break
            texts = [r["content_text"] for r in rows]
            try:
                vectors = await embedder.embed_batch(texts)
            except Exception:  # noqa: BLE001 — embedder down: stop, leave pending
                break
            updates = [
                (rows[i]["id"], _vec(vectors[i]), model_name)
                for i in range(min(len(rows), len(vectors)))
            ]
            await ctx.conn.executemany(
                # Bind the vector as text and cast in SQL ($2::text::vector) so the
                # write works whether or not the pgvector codec is registered on
                # the connection (the gateway pool registers it; raw pools don't).
                "UPDATE code_embeddings SET embedding=$2::text::vector, "
                "embedding_pending=FALSE, model_name=$3 WHERE id=$1",
                updates,
            )
            filled += len(updates)
            if len(rows) < batch_size:
                break
    return filled
