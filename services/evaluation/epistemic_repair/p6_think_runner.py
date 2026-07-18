"""Production Think execution for the sealed P6 runtime stream.

This module never imports or reads ``P6Gold``.  It freezes production outputs
before an independent evaluator is allowed to join them to sealed truth.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import time
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

import asyncpg

from lib.embeddings.ollama import OllamaClient, OllamaConfig
from lib.evaluation.epistemic_repair.p6_population import P6Batch, P6Population
from lib.evaluation.epistemic_repair.provider_contract import (
    require_codex_cli_environment,
)
from lib.llm.provider import (
    LLMProvider, _codex_transport, build_provider, close_codex_app_server_client,
    set_response_cache,
)
from lib.shared.errors import InvariantViolation
from services.app.gateway.db_bootstrap import _register_codecs
from services.domain.company_learning.barrier import CompanyLearningBarrierService
from services.domain.conversation_context.episode_boundaries import (
    ConversationBoundaryObservation, project_conversation_episode_boundaries,
)
from services.domain.entity_grounding.learned_discovery import (
    MentionCandidateAdapter, PersistedSignalText,
    VerifiedMentionCandidate,
)
from lib.contracts.entity_mentions import EntityMentionDetectionFate
from services.reasoning.think.worker import ThinkWorker, WorkerConfig


# Fixed evaluator event time, deliberately far behind wall-clock execution.
# This is transport metadata only and is not part of the sealed population.
_P6_RUNTIME_START = datetime(2025, 7, 10, 9, 0, tzinfo=timezone.utc)
_P6_ENVELOPE_RE = re.compile(
    r"^(?P<surface>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){1,3}), update \d+:",
    flags=re.MULTILINE,
)
_P6_SCOPE_ASSERTION_RE = re.compile(
    r"^(?P<surface>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){1,3})\s+"
    r"(?:is|are|was|were)\b",
    flags=re.MULTILINE,
)
_P6_SCOPE_TYPES = {
    "release": "workstream", "migration": "workstream",
    "handoff": "workstream", "renewal": "commitment",
}
_P6_LOCAL_WORK_ITEMS_RE = re.compile(
    r"(?P<approval>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){0,3} approval)"
    r" is listed in the "
    r"(?P<ticket>[A-Z][A-Za-z0-9-]+(?: [A-Za-z0-9-]+){0,3} ticket)",
)


@dataclass(frozen=True, slots=True)
class P6ThinkExecutionDependencies:
    """Gold-blind runtime dependencies for one P6-shaped execution.

    Production construction remains owned by ``run_p6_production_think``.
    Provider-free evaluators may inject compatible dependencies through
    ``run_p6_think_with_dependencies`` without weakening that production
    boundary or importing a sealed oracle into this module.
    """

    llm_provider: LLMProvider
    mention_candidate_adapter: MentionCandidateAdapter | None
    embedder: Any
    run_provenance: dict[str, Any]
    transport: str
    pin_planner_model: bool = False
    execution_mode: str = (
        "injected ThinkWorker dependencies; no semantic success claimed"
    )


def _p6_simulation_mention_adapter(
    signals: tuple[PersistedSignalText, ...],
) -> tuple[VerifiedMentionCandidate, ...]:
    candidates: list[VerifiedMentionCandidate] = []
    for signal in signals:
        match = (
            _P6_ENVELOPE_RE.search(signal.content_text)
            or _P6_SCOPE_ASSERTION_RE.search(signal.content_text)
        )
        if match is not None:
            surface = match.group("surface")
            entity_type = _P6_SCOPE_TYPES.get(surface.rsplit(" ", 1)[-1].casefold())
            if entity_type is not None:
                slug = re.sub(r"[^a-z0-9]+", "-", surface.casefold()).strip("-")
                candidates.append(VerifiedMentionCandidate(
                    signal_id=signal.signal_id,
                    surface=surface,
                    span_start=match.start("surface"),
                    span_end=match.end("surface"),
                    entity_type=entity_type,
                    confidence=0.99,
                    fate=EntityMentionDetectionFate.DETECTED,
                    reason_codes=("p6_sealed_business_envelope",),
                    type_confidence=0.99,
                    extractor_version="p6-simulation-envelope-v1",
                    provisional_canonical_ref=f"{entity_type}:{slug}",
                    normalization_version=1,
                ))
        local_match = _P6_LOCAL_WORK_ITEMS_RE.search(signal.content_text)
        if local_match is not None:
            for group in ("approval", "ticket"):
                surface = local_match.group(group)
                slug = re.sub(r"[^a-z0-9]+", "-", surface.casefold()).strip("-")
                candidates.append(VerifiedMentionCandidate(
                    signal_id=signal.signal_id,
                    surface=surface,
                    span_start=local_match.start(group),
                    span_end=local_match.end(group),
                    entity_type="work_item",
                    confidence=0.99,
                    fate=EntityMentionDetectionFate.DETECTED,
                    reason_codes=("p6_sealed_local_work_item_relation",),
                    type_confidence=0.99,
                    extractor_version="p6-simulation-envelope-v1",
                    provisional_canonical_ref=f"work_item:{slug}",
                    normalization_version=1,
                ))
    return tuple(candidates)


def _signal_occurred_at(*, batch_number: int, position: int) -> datetime:
    return _P6_RUNTIME_START + timedelta(
        days=batch_number - 1, minutes=position,
    )


def _assert_population_clock_safe(
    population: P6Population, *, run_started_at: datetime,
) -> None:
    """Fail before writes if any synthetic evidence is future-dated."""

    latest = max(
        _signal_occurred_at(
            batch_number=signal.batch_number, position=signal.position,
        )
        for signal in population.signals
    )
    if latest > run_started_at:
        raise InvariantViolation(
            "P6_FUTURE_EVIDENCE_CLOCK",
            "P6 runtime evidence must not occur after the proof starts",
            latest_occurred_at=latest.isoformat(),
            run_started_at=run_started_at.isoformat(),
        )


async def _init_p6_connection(conn: asyncpg.Connection) -> None:
    await _register_codecs(conn)

    def _log_failed_query(record: Any) -> None:
        if record.exception is not None:
            print(
                "p6_failed_sql=" + " ".join(record.query.split())[:2000]
                + f" exception={type(record.exception).__name__}:{record.exception}",
                flush=True,
            )

    conn.add_query_logger(_log_failed_query)


async def _snapshot(pool: asyncpg.Pool, tenant_id: UUID) -> dict[str, Any]:
    async with pool.acquire() as conn:
        models = await conn.fetch("""
            SELECT id, truth_version_id, truth_version, proposition, natural_text,
                   truth_lifecycle, truth_advanced_at
            FROM accepted_current_models WHERE tenant_id=$1
            ORDER BY truth_advanced_at,id
        """, tenant_id)
        decisions = await conn.fetch("""
            SELECT batch_id, context_item_kind, context_item_id, retrieved,
                   selected, included, referenced, historical_reopen_reason,
                   decision_fate, result_object_kind, result_object_id
            FROM company_learning_context_decisions WHERE tenant_id=$1
            ORDER BY decided_at,decision_id
        """, tenant_id)
        trigger_pending = int(await conn.fetchval("""
            SELECT count(*) FROM think_trigger_queue
            WHERE tenant_id=$1 AND completed_at IS NULL
        """, tenant_id))
        eventual_rows = await conn.fetch("""
            SELECT action_kind,count(*)::int AS pending
            FROM pending_post_commit_actions
            WHERE tenant_id=$1 AND processed_at IS NULL AND dead_lettered_at IS NULL
            GROUP BY action_kind ORDER BY action_kind
        """, tenant_id)
        eventual_by_action = {
            str(row["action_kind"]): int(row["pending"]) for row in eventual_rows
        }
        return {
            "accepted_models": [dict(row) for row in models],
            "accepted_relation_count": int(await conn.fetchval(
                "SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1", tenant_id)),
            "context_decisions": [dict(row) for row in decisions],
            "pending_work": {
                # Incomplete Think triggers can still change accepted truth and
                # therefore fence barrier completion.
                "truth_critical": {
                    "total": trigger_pending,
                    "by_queue": {"think_trigger_queue": trigger_pending},
                },
                # Post-commit actions materialize projections, invalidate
                # caches/metrics, broadcast, or discover *candidate* edges.
                # They do not mutate accepted truth and may trail the barrier.
                "eventual_derived": {
                    "total": sum(eventual_by_action.values()),
                    "by_action_kind": eventual_by_action,
                },
            },
        }


async def _freeze_zero_seed_preflight(
    conn: asyncpg.Connection, tenant_id: UUID,
) -> dict[str, Any]:
    """Freeze database-observed canonical truth before the first signal."""

    counts = {
        "accepted_model_count": int(await conn.fetchval(
            "SELECT count(*) FROM accepted_current_models WHERE tenant_id=$1",
            tenant_id,
        )),
        "accepted_relation_count": int(await conn.fetchval(
            "SELECT count(*) FROM accepted_current_relations WHERE tenant_id=$1",
            tenant_id,
        )),
    }
    captured_at = datetime.now(timezone.utc).isoformat()
    return {
        **counts,
        "captured_at": captured_at,
        "source": "database_preflight_before_first_signal",
    }


async def _complete_and_reopen_barrier(
    conn: asyncpg.Connection, *, tenant_id: UUID, batch_number: int,
    previous_model_versions: set[UUID],
) -> tuple[dict[str, Any], set[UUID]]:
    """Atomically fence exact visible truth, then reopen its durable receipt."""

    model_versions = tuple(await conn.fetchval("""
        SELECT COALESCE(array_agg(truth_version_id ORDER BY truth_version_id), '{}'::uuid[])
        FROM accepted_current_models WHERE tenant_id=$1
    """, tenant_id))
    relation_versions = tuple(await conn.fetchval("""
        SELECT COALESCE(array_agg(truth_relation_version_id ORDER BY truth_relation_version_id), '{}'::uuid[])
        FROM accepted_current_relations WHERE tenant_id=$1
    """, tenant_id))
    current_models = set(model_versions)
    invalidated = tuple(sorted(previous_model_versions - current_models, key=str))
    truth_pending = int(await conn.fetchval("""
        SELECT count(*) FROM think_trigger_queue
        WHERE tenant_id=$1 AND completed_at IS NULL
    """, tenant_id))
    service = CompanyLearningBarrierService()
    receipt = await service.complete(
        tx=conn,
        barrier_id=uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:barrier:{batch_number}"),
        tenant_id=tenant_id, batch_id=f"p6-batch-{batch_number}",
        expected_model_version_ids=model_versions,
        expected_relation_version_ids=relation_versions,
        invalidated_model_version_ids=invalidated,
        truth_critical_pending_count=truth_pending,
        completed_at=datetime.now(timezone.utc),
    )
    reopened = await service._find(
        tx=conn, tenant_id=tenant_id, batch_id=f"p6-batch-{batch_number}",
    )
    if reopened != receipt:
        raise RuntimeError("durable barrier receipt did not reopen exactly")
    return ({
        "barrier_id": str(receipt.barrier_id),
        "batch_id": receipt.batch_id,
        "barrier_version": receipt.barrier_version,
        "prior_barrier_id": str(receipt.prior_barrier_id) if receipt.prior_barrier_id else None,
        "expected_model_version_ids": list(receipt.expected_model_version_ids),
        "expected_relation_version_ids": list(receipt.expected_relation_version_ids),
        "invalidated_model_version_ids": list(receipt.invalidated_model_version_ids),
        "truth_critical_pending_count": receipt.truth_critical_pending_count,
        "completed_at": receipt.completed_at,
        "receipt_digest": receipt.receipt_digest,
        "reopened_exactly": True,
    }, current_models)


async def _drain_truth_critical_work(
    pool: asyncpg.Pool, worker: ThinkWorker, *, tenant_id: UUID,
    max_cycles: int = 8,
) -> dict[str, Any]:
    """Process batched downstream truth work until the barrier can close."""

    cycle_receipts: list[dict[str, int]] = []
    for cycle in range(1, max_cycles + 1):
        async with pool.acquire() as conn:
            pending_before = int(await conn.fetchval("""
                SELECT count(*) FROM think_trigger_queue
                WHERE tenant_id=$1 AND completed_at IS NULL
            """, tenant_id))
            if pending_before == 0:
                return {
                    "complete": True, "cycles": cycle - 1,
                    "cycle_receipts": cycle_receipts, "pending_after": 0,
                }
            # Preserve semantic batching while making the finite proof runner
            # independent of wall-clock batch-window sleeps.
            await conn.execute("""
                UPDATE think_trigger_queue
                SET enqueued_at=now()-interval '2 seconds', scheduled_for=now()
                WHERE tenant_id=$1 AND completed_at IS NULL
            """, tenant_id)
        await worker._poll_and_dispatch()
        tasks = tuple(worker._in_flight)
        if tasks:
            await asyncio.gather(*tasks)
        async with pool.acquire() as conn:
            pending_after = int(await conn.fetchval("""
                SELECT count(*) FROM think_trigger_queue
                WHERE tenant_id=$1 AND completed_at IS NULL
            """, tenant_id))
        cycle_receipts.append({
            "cycle": cycle, "pending_before": pending_before,
            "dispatched_batches": len(tasks), "pending_after": pending_after,
        })
        if pending_after == 0:
            return {
                "complete": True, "cycles": cycle,
                "cycle_receipts": cycle_receipts, "pending_after": 0,
            }
        if not tasks and pending_after >= pending_before:
            break
    return {
        "complete": False, "cycles": len(cycle_receipts),
        "cycle_receipts": cycle_receipts,
        "pending_after": cycle_receipts[-1]["pending_after"] if cycle_receipts else 0,
    }


async def _llm_receipts(pool: asyncpg.Pool, tenant_id: UUID) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT logical.think_run_id, logical.purpose,
                   attempt.physical_attempt_id, attempt.ordinal,
                   attempt.provider, attempt.model, attempt.outcome,
                   attempt.input_tokens, attempt.output_tokens,
                   attempt.cache_tokens, attempt.usage_exactness
            FROM llm_logical_call_receipts logical
            JOIN llm_provider_attempt_receipts attempt
              ON attempt.tenant_id=logical.tenant_id
             AND attempt.logical_call_id=logical.logical_call_id
            WHERE logical.tenant_id=$1
            ORDER BY logical.started_at,attempt.ordinal
        """, tenant_id)
        return [dict(row) for row in rows]


async def _persist_runtime_batch(
    conn: asyncpg.Connection, *, tenant_id: UUID, batch: P6Batch,
) -> dict[str, UUID]:
    """Persist normalized runtime signals without consulting sealed gold."""

    result: dict[str, UUID] = {}
    rows = []
    for signal in batch.signals:
        observation_id = uuid5(NAMESPACE_URL, f"p6-think:{tenant_id}:{signal.signal_id}")
        result[signal.signal_id] = observation_id
        occurred_at = _signal_occurred_at(
            batch_number=signal.batch_number, position=signal.position,
        )
        rows.append((observation_id, tenant_id, occurred_at,
                     signal.source_channel, json.dumps({"text": signal.text}),
                     signal.text))
    ids = [row[0] for row in rows]
    existing = await conn.fetch(
        """SELECT id,tenant_id,occurred_at,source_channel,content,content_text
           FROM observations
           WHERE id=ANY($1::uuid[]) AND tenant_id=$2""",
        ids, tenant_id,
    )
    expected = {
        row[0]: {
            "tenant_id": row[1], "occurred_at": row[2],
            "source_channel": row[3], "content": json.loads(row[4]),
            "content_text": row[5],
        }
        for row in rows
    }
    for prior in existing:
        actual = {
            "tenant_id": prior["tenant_id"], "occurred_at": prior["occurred_at"],
            "source_channel": prior["source_channel"],
            "content": prior["content"], "content_text": prior["content_text"],
        }
        if actual != expected[prior["id"]]:
            raise InvariantViolation(
                "P6_OBSERVATION_IDEMPOTENCY_CONFLICT",
                "stable observation identity resolved to different signal content",
                observation_id=str(prior["id"]),
            )
    await conn.executemany("""
        INSERT INTO observations (
          id,tenant_id,occurred_at,kind,source_channel,content,content_text,
          embedding_pending,trust_tier,entities_mentioned
        ) VALUES ($1,$2,$3,'signal',$4,$5::jsonb,$6,TRUE,'unvetted','[]'::jsonb)
        ON CONFLICT (id, occurred_at) DO NOTHING
    """, rows)
    return result


async def _persisted_boundary_entity_refs(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    observation_ids: list[UUID],
) -> dict[str, tuple[str, ...]]:
    """Load current verified entity coordinates for boundary projection."""
    rows = await conn.fetch(
        """SELECT detection.source_observation_id,detection.mention,
                  trace.current_fate,trace.selected_referent
           FROM entity_mention_detections detection
           JOIN entity_mention_detection_heads head
             ON head.tenant_id=detection.tenant_id
            AND head.current_detection_id=detection.id
           LEFT JOIN grounding_traces trace
             ON trace.tenant_id=detection.tenant_id
            AND trace.source_observation_id=detection.source_observation_id
            AND trace.phrase=detection.candidate_surface
           WHERE detection.tenant_id=$1
             AND detection.source_observation_id=ANY($2::uuid[])
             AND detection.fate='detected'
           ORDER BY detection.source_observation_id,detection.id""",
        tenant_id,
        observation_ids,
    )
    refs: dict[str, set[str]] = {}
    for row in rows:
        mention = row["mention"] or {}
        referent = row["selected_referent"] or {}
        if isinstance(mention, str):
            mention = json.loads(mention)
        if isinstance(referent, str):
            referent = json.loads(referent)
        resolved = row["current_fate"] in {"resolved", "resolved_for_consumer"}
        canonical_ref = (
            referent.get("canonical_ref")
            or referent.get("id")
            or referent.get("canonical_referent_id")
        ) if resolved else mention.get("provisional_canonical_ref")
        if canonical_ref:
            refs.setdefault(str(row["source_observation_id"]), set()).add(
                str(canonical_ref)
            )
    return {key: tuple(sorted(values)) for key, values in refs.items()}


def _project_boundaries(
    observations: list[ConversationBoundaryObservation],
    observation_to_signal: dict[str, str],
) -> list[dict[str, str]]:
    """Freeze production boundary decisions without evaluator labels."""

    decisions: list[dict[str, str]] = []
    for group in project_conversation_episode_boundaries(tuple(observations)):
        boundary_id = min(group)
        for observation_id in group:
            decisions.append({
                "signal_id": observation_to_signal[observation_id],
                "observation_id": observation_id,
                "predicted_boundary_id": boundary_id,
                "projector_version": "conversation-episode-boundaries-v1",
            })
    return sorted(decisions, key=lambda row: row["signal_id"])


def _jsonable(value: Any) -> Any:
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(item) for item in value]
    return value


def _write_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n")
    temporary.replace(path)


def _run_provenance() -> dict[str, Any]:
    """Seal the exact clean source tree used by a costly production run."""

    root = Path(__file__).resolve().parents[3]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=root, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    status = subprocess.run(
        ["git", "status", "--porcelain", "--untracked-files=normal"],
        cwd=root, check=True, capture_output=True, text=True,
    ).stdout
    if status:
        raise RuntimeError("P6 production proof requires a clean pinned worktree")
    return {
        "git_commit": commit, "worktree_clean": True,
        "worktree_path": str(root),
    }


async def _execute_p6_think(
    *, database_url: str, population: P6Population, checkpoint_path: Path,
    dependencies: P6ThinkExecutionDependencies,
    tenant_id: UUID | None = None, per_batch_timeout_s: float = 650.0,
    attempt_timeout_s: float = 300.0,
    total_timeout_s: float = 1800.0, max_batches: int = 12,
) -> dict[str, Any]:
    """Execute P6-shaped batches with already-constructed dependencies."""

    tenant_id = tenant_id or uuid4()
    run_started_at = datetime.now(timezone.utc)
    _assert_population_clock_safe(population, run_started_at=run_started_at)
    run_provenance = dict(dependencies.run_provenance)
    pool = await asyncpg.create_pool(database_url, min_size=1, max_size=4,
                                     init=_init_p6_connection)
    embedder = dependencies.embedder
    provider = dependencies.llm_provider
    expected_provider = str(provider.config.provider)
    expected_model = str(provider.config.model)
    expected_transport = str(dependencies.transport)
    # P6 is a one-configuration proof.  Inquiry planning normally selects a
    # faster Codex model; pin both planner roles to the exact main model here.
    pin_names = (
        "INQUIRY_CODEX_QUESTION_MODEL",
        "INQUIRY_CODEX_QUESTION_FALLBACK_MODEL",
    )
    prior_pins = {name: os.environ.get(name) for name in pin_names}
    if dependencies.pin_planner_model:
        for name in pin_names:
            os.environ[name] = expected_model
    worker = ThinkWorker(
        pool,
        config=WorkerConfig(
            poll_batch=30, max_concurrency_per_tenant=1,
            tenant_filter=tenant_id, worker_id=f"p6-{tenant_id}",
            t1_batch_window_s=1.0, t1_batch_min_size=25,
            t1_batch_max_size=25, run_timeout_s=attempt_timeout_s,
            downstream_batch_window_s=1.0,
            downstream_batch_min_size=2,
            t4_batch_max_size=4,
            process_background_triggers=True,
        ),
        llm_provider=provider, mention_discovery_provider=provider,
        mention_candidate_adapter=dependencies.mention_candidate_adapter,
        embedder=embedder,
    )
    started = time.monotonic()
    waves: list[dict[str, Any]] = []
    previous_model_versions: set[UUID] = set()
    boundary_inputs: list[ConversationBoundaryObservation] = []
    observation_to_signal: dict[str, str] = {}
    boundary_decisions: list[dict[str, str]] = []
    terminal_reason: str | None = None
    zero_seed_preflight: dict[str, Any] | None = None
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO tenants(id,name,is_demo) VALUES($1,$2,FALSE)",
                               tenant_id, f"p6-production-{tenant_id}")
            zero_seed_preflight = await _freeze_zero_seed_preflight(conn, tenant_id)
        # Imports are delayed because this evaluator reuses benchmark transport
        # orchestration, not its scenario/gold construction.
        from scripts.run_1000_signal_model_layer_probe import enqueue_t1_for_observations
        from scripts.run_storyline_batch_benchmark import _process_one_t1_batch

        selected_batches = population.batches[:max(1, min(12, int(max_batches)))]
        for batch in selected_batches:
            remaining = total_timeout_s - (time.monotonic() - started)
            if remaining <= 0:
                terminal_reason = "total_timeout"
                break
            batch_started = time.monotonic()
            batch_deadline = batch_started + per_batch_timeout_s
            async with pool.acquire() as conn:
                observation_ids = await _persist_runtime_batch(
                    conn, tenant_id=tenant_id, batch=batch,
                )
            for signal in batch.signals:
                observation_id = str(observation_ids[signal.signal_id])
                observation_to_signal[observation_id] = signal.signal_id
                boundary_inputs.append(ConversationBoundaryObservation(
                    observation_id=observation_id,
                    occurred_at=_signal_occurred_at(
                        batch_number=signal.batch_number,
                        position=signal.position,
                    ),
                    content_text=signal.text,
                    source_container_id=signal.source_space,
                ))
            await enqueue_t1_for_observations(
                pool, tenant_id=tenant_id,
                observation_ids=list(observation_ids.values()), limit=25,
                run_id=f"p6-batch-{batch.batch_number}",
            )
            try:
                async with asyncio.timeout(min(
                    per_batch_timeout_s, remaining,
                    max(0.001, batch_deadline - time.monotonic()),
                )):
                    execution = await _process_one_t1_batch(
                        pool, worker, tenant_id=tenant_id,
                        force_window_elapsed_s=1.0, retry_attempts=1,
                    )
            except TimeoutError:
                terminal_reason = f"batch_{batch.batch_number}_timeout"
                waves.append({
                    "batch_number": batch.batch_number, "status": "timeout",
                    "elapsed_s": round(time.monotonic() - batch_started, 3),
                })
                break
            async with pool.acquire() as conn:
                entity_refs = await _persisted_boundary_entity_refs(
                    conn,
                    tenant_id,
                    [UUID(item.observation_id) for item in boundary_inputs],
                )
            enriched_boundary_inputs = [
                ConversationBoundaryObservation(
                    observation_id=item.observation_id,
                    occurred_at=item.occurred_at,
                    content_text=item.content_text,
                    source_container_id=item.source_container_id,
                    entity_refs=entity_refs.get(item.observation_id, ()),
                )
                for item in boundary_inputs
            ]
            boundary_decisions = _project_boundaries(
                enriched_boundary_inputs, observation_to_signal,
            )
            run = execution.get("run") or {}
            barrier_receipt: dict[str, Any] | None = None
            truth_drain: dict[str, Any] | None = None
            if run.get("status") == "success":
                async with asyncio.timeout(min(
                    remaining, max(0.001, batch_deadline - time.monotonic()),
                )):
                    truth_drain = await _drain_truth_critical_work(
                        pool, worker, tenant_id=tenant_id,
                    )
                if not truth_drain["complete"]:
                    terminal_reason = (
                        f"batch_{batch.batch_number}_truth_drain_incomplete"
                    )
                async with pool.acquire() as conn, conn.transaction():
                    barrier_receipt, previous_model_versions = (
                        await _complete_and_reopen_barrier(
                            conn, tenant_id=tenant_id,
                            batch_number=batch.batch_number,
                            previous_model_versions=previous_model_versions,
                        )
                    )
            snapshot = await _snapshot(pool, tenant_id)
            waves.append({
                "batch_number": batch.batch_number,
                "status": run.get("status") or "missing_run",
                "execution": execution,
                "truth_critical_drain": truth_drain,
                "boundary_decisions": boundary_decisions,
                "barrier_receipt": barrier_receipt,
                "snapshot": snapshot,
                "elapsed_s": round(time.monotonic() - batch_started, 3),
            })
            checkpoint = {
                "schema_version": "epistemic-repair-p6-think-checkpoint-v1",
                "population_version": population.version,
                "population_digest": population.population_digest,
                "tenant_id": str(tenant_id), "completed_batches": len(waves),
                "waves": waves, "terminal_reason": terminal_reason,
                "elapsed_s": round(time.monotonic() - started, 3),
                "run_provenance": run_provenance,
                "zero_seed_preflight": zero_seed_preflight,
                "timeout_budget": {
                    "attempt_timeout_s": attempt_timeout_s, "max_attempts": 2,
                    "batch_deadline_s": per_batch_timeout_s,
                    "total_deadline_s": total_timeout_s,
                },
            }
            _write_checkpoint(checkpoint_path, checkpoint)
            print(f"p6_think_batch={batch.batch_number}/12 status={run.get('status')} models={len(snapshot['accepted_models'])} elapsed_s={waves[-1]['elapsed_s']}", flush=True)
            if run.get("status") != "success":
                terminal_reason = f"batch_{batch.batch_number}_{run.get('status') or 'missing'}"
                break
        frozen = await _snapshot(pool, tenant_id)
        llm_receipts = await _llm_receipts(pool, tenant_id)
        mixed_attempts = [
            receipt for receipt in llm_receipts
            if receipt["provider"] != expected_provider
            or receipt["model"] != expected_model
        ]
        if mixed_attempts and terminal_reason is None:
            terminal_reason = "mixed_llm_configuration"
        artifact = {
            "schema_version": "epistemic-repair-p6-production-think-v1",
            "population_version": population.version,
            "population_digest": population.population_digest,
            "tenant_id": str(tenant_id),
            "complete": len(waves) == len(selected_batches) and terminal_reason is None
                        and all(wave["status"] == "success" for wave in waves),
            "completed_batches": len(waves), "target_batches": len(selected_batches),
            "terminal_reason": terminal_reason,
            "waves": waves, "frozen_outputs": frozen,
            "llm_attempt_receipts": llm_receipts,
            "expected_llm_configuration": {
                "provider": expected_provider, "model": expected_model,
                "transport": expected_transport,
            },
            "mixed_llm_attempt_count": len(mixed_attempts),
            "provider_mode": dependencies.execution_mode,
            "gold_visible_during_execution": False,
            "boundary_decisions": boundary_decisions,
            "run_provenance": run_provenance,
            "zero_seed_preflight": zero_seed_preflight,
            "timeout_budget": {
                "attempt_timeout_s": attempt_timeout_s, "max_attempts": 2,
                "batch_deadline_s": per_batch_timeout_s,
                "total_deadline_s": total_timeout_s,
            },
            "elapsed_s": round(time.monotonic() - started, 3),
            "proof_boundary": (
                "Production outputs are frozen before independent gold evaluation.",
                "No P6 gold type, storyline label, thesis, or synthesis target is imported by this module.",
                "Incomplete or timed-out runs never receive semantic scores.",
            ),
        }
        _write_checkpoint(checkpoint_path, artifact)
        return artifact
    finally:
        if dependencies.pin_planner_model:
            for name, prior in prior_pins.items():
                if prior is None:
                    os.environ.pop(name, None)
                else:
                    os.environ[name] = prior
        await pool.close()


async def run_p6_think_with_dependencies(
    *, database_url: str, population: P6Population, checkpoint_path: Path,
    dependencies: P6ThinkExecutionDependencies,
    tenant_id: UUID | None = None, per_batch_timeout_s: float = 650.0,
    attempt_timeout_s: float = 300.0,
    total_timeout_s: float = 1800.0, max_batches: int = 12,
) -> dict[str, Any]:
    """Run the gold-blind execution path with caller-owned dependencies.

    This seam deliberately makes no provider-quality or semantic-success claim.
    The caller owns provider/embedder construction and cleanup, and must supply
    an explicit runtime identity suitable for its evaluation class.
    """

    if not dependencies.run_provenance:
        raise ValueError("injected P6 execution requires runtime provenance")
    if not dependencies.transport.strip():
        raise ValueError("injected P6 execution requires a transport identity")
    return await _execute_p6_think(
        database_url=database_url,
        population=population,
        checkpoint_path=checkpoint_path,
        dependencies=dependencies,
        tenant_id=tenant_id,
        per_batch_timeout_s=per_batch_timeout_s,
        attempt_timeout_s=attempt_timeout_s,
        total_timeout_s=total_timeout_s,
        max_batches=max_batches,
    )


async def run_p6_production_think(
    *, database_url: str, population: P6Population, checkpoint_path: Path,
    tenant_id: UUID | None = None, per_batch_timeout_s: float = 650.0,
    attempt_timeout_s: float = 300.0,
    total_timeout_s: float = 1800.0, max_batches: int = 12,
) -> dict[str, Any]:
    """Run production P6 with a clean worktree and strict Codex CLI identity."""

    require_codex_cli_environment()
    run_provenance = _run_provenance()
    set_response_cache(None)
    provider = build_provider()
    embedder = OllamaClient(OllamaConfig.from_env())
    expected_transport = _codex_transport()
    if str(provider.config.provider) != "codex" or expected_transport != "cli":
        await close_codex_app_server_client()
        await embedder.close()
        raise RuntimeError(
            "P6 production proof requires provider=codex and CODEX_TRANSPORT=cli"
        )
    dependencies = P6ThinkExecutionDependencies(
        llm_provider=provider,
        mention_candidate_adapter=_p6_simulation_mention_adapter,
        embedder=embedder,
        run_provenance=run_provenance,
        transport=expected_transport,
        pin_planner_model=True,
        execution_mode=(
            "production ThinkWorker; every role pinned to one configuration"
        ),
    )
    try:
        return await run_p6_think_with_dependencies(
            database_url=database_url,
            population=population,
            checkpoint_path=checkpoint_path,
            dependencies=dependencies,
            tenant_id=tenant_id,
            per_batch_timeout_s=per_batch_timeout_s,
            attempt_timeout_s=attempt_timeout_s,
            total_timeout_s=total_timeout_s,
            max_batches=max_batches,
        )
    finally:
        await close_codex_app_server_client()
        await embedder.close()


__all__ = [
    "P6ThinkExecutionDependencies",
    "run_p6_production_think",
    "run_p6_think_with_dependencies",
]
