"""Provider-free orchestration for the CF2 core fast-path population.

This module is deliberately gold-blind.  It adapts the normalized 4x25 CF2
population to the production Think execution seam, while preserving source
metadata and installing source-authenticated grounding before enqueue.
"""

from __future__ import annotations

import hashlib
import json
import struct
from pathlib import Path
from typing import Any, Mapping
from uuid import UUID

import asyncpg

from lib.evaluation.epistemic_repair.core_fast_path_population import (
    build_core_fast_path_population,
)
from services.domain.observations.partitions import ensure_partitions
from services.evaluation.epistemic_repair.cf2_provider import (
    CF2ProviderFreeLLM,
    CF2ResponseHandler,
)
from services.evaluation.epistemic_repair.cf2_decisions import (
    compiled_batch_memory_decisions,
)
from services.evaluation.epistemic_repair.cf2_source_grounding import (
    SourceAuthenticatedSignal,
    persist_source_authenticated_grounding,
)
from services.evaluation.epistemic_repair.p6_think_runner import (
    P6ThinkExecutionDependencies,
    _jsonable,
    _persist_runtime_batch,
    _signal_occurred_at,
    _write_checkpoint,
    run_p6_think_with_dependencies,
)
class CF2DeterministicEmbedder:
    """Small provider-free, deterministic 768-dimensional embedder."""

    async def embed(self, text: str) -> list[float]:
        seed = hashlib.sha512((text or "").encode("utf-8")).digest()
        pool = b""
        while len(pool) < 768 * 4:
            pool += hashlib.sha512(pool + seed).digest()
        values: list[float] = []
        for index in range(768):
            raw = struct.unpack("<f", pool[index * 4:(index + 1) * 4])[0]
            if not (-1e6 < raw < 1e6):
                raw = 0.0
            values.append(max(-1.0, min(1.0, raw / 1e3)))
        return values

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        return [await self.embed(text) for text in texts]

    async def close(self) -> None:
        return None


async def run_cf2_provider_free(
    *,
    database_url: str,
    checkpoint_path: Path,
    handlers: Mapping[str, CF2ResponseHandler] | None = None,
    tenant_id: UUID | None = None,
    embedder: Any | None = None,
    max_batches: int = 4,
    per_batch_timeout_s: float = 650.0,
    attempt_timeout_s: float = 300.0,
    total_timeout_s: float = 1800.0,
) -> dict[str, Any]:
    """Run intact CF2 batches from zero seed through the actual Think worker."""

    population = build_core_fast_path_population()
    provider = CF2ProviderFreeLLM(handlers={
        "BatchMemoryDecisionSet": compiled_batch_memory_decisions,
        **dict(handlers or {}),
    })
    runtime_embedder = embedder or CF2DeterministicEmbedder()
    owns_embedder = embedder is None
    preparation: dict[int, dict[str, Any]] = {}

    async def persist_batch(
        conn: asyncpg.Connection, actual_tenant_id: UUID, batch: Any,
    ) -> dict[str, UUID]:
        # Reuse the production-shaped stable identity and observation writer.
        first = batch.signals[0]
        await ensure_partitions(
            conn,
            as_of=_signal_occurred_at(
                batch_number=first.batch_number, position=first.position,
            ).date(),
            months_ahead=0,
        )
        return await _persist_runtime_batch(
            conn, tenant_id=actual_tenant_id, batch=batch,
        )

    async def prepare_batch(
        conn: asyncpg.Connection,
        actual_tenant_id: UUID,
        batch: Any,
        observation_ids: dict[str, UUID],
    ) -> None:
        # The base P6 fixture intentionally defaults these fields.  CF2 carries
        # exact source/trust coordinates, so restore them before any enqueue.
        await conn.executemany(
            """UPDATE observations
                  SET trust_tier=$4,
                      content=content || $3::jsonb
                WHERE id=$1 AND tenant_id=$2""",
            [
                (
                    observation_ids[signal.signal_id],
                    actual_tenant_id,
                    json.dumps({"source_space": signal.source_space}),
                    signal.trust_tier,
                )
                for signal in batch.signals
            ],
        )
        grounded = 0
        abstained = 0
        for signal in batch.signals:
            episode = await persist_source_authenticated_grounding(
                conn,
                SourceAuthenticatedSignal(
                    tenant_id=actual_tenant_id,
                    observation_id=observation_ids[signal.signal_id],
                    occurred_at=_signal_occurred_at(
                        batch_number=signal.batch_number,
                        position=signal.position,
                    ),
                    source_channel=signal.source_channel,
                    source_container_id=signal.source_space,
                    content_text=signal.text,
                ),
            )
            if episode is None:
                abstained += 1
            else:
                grounded += 1
        preparation[batch.batch_number] = {
            "batch_number": batch.batch_number,
            "signal_count": len(batch.signals),
            "source_grounded_count": grounded,
            "source_grounding_abstained_count": abstained,
            "metadata_preserved_count": len(batch.signals),
        }

    dependencies = P6ThinkExecutionDependencies(
        llm_provider=provider,
        mention_candidate_adapter=None,
        embedder=runtime_embedder,
        run_provenance={
            "runtime_identity": "cf2-provider-free-v1",
            "population_digest": population.population_digest,
            "gold_visible_during_execution": False,
        },
        transport="in_process_provider_free",
        persist_runtime_batch=persist_batch,
        prepare_persisted_batch=prepare_batch,
        execution_mode=(
            "actual ThinkWorker production T1 policy selection with "
            "deterministic provider-free dependencies"
        ),
        execution_policy=None,
    )
    try:
        artifact = await run_p6_think_with_dependencies(
            database_url=database_url,
            population=population,  # structurally P6-compatible, intentionally gold-free
            checkpoint_path=checkpoint_path,
            dependencies=dependencies,
            tenant_id=tenant_id,
            per_batch_timeout_s=per_batch_timeout_s,
            attempt_timeout_s=attempt_timeout_s,
            total_timeout_s=total_timeout_s,
            max_batches=max_batches,
        )
        artifact.update({
            "schema_version": "epistemic-repair-cf2-provider-free-v1",
            "provider_telemetry": provider.telemetry(),
            "batch_preparation": [preparation[key] for key in sorted(preparation)],
            "per_batch_evidence": [
                {
                    "batch_number": wave.get("batch_number"),
                    "elapsed_s": wave.get("elapsed_s"),
                    "status": wave.get("status"),
                    "snapshot": wave.get("snapshot"),
                }
                for wave in artifact.get("waves", [])
            ],
            "gold_visible_during_execution": False,
            "zero_seed_requested": True,
        })
        normalized_artifact = _jsonable(artifact)
        _write_checkpoint(checkpoint_path, normalized_artifact)
        return normalized_artifact
    finally:
        close = getattr(runtime_embedder, "close", None)
        if owns_embedder and close is not None:
            await close()


__all__ = ["CF2DeterministicEmbedder", "run_cf2_provider_free"]
