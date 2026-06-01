"""services/ingest/github_intel/worker.py — ordered state-advancement + enrichment worker.

Drains `github_intel_queue` oldest-first under a per-repo advisory lock, so one
repo's FSM is never reordered. Per item, in ONE transaction:
  - load the observation,
  - apply the authoritative FSM transition (state_store.apply_event),
  - compute blast radius (code_intel) + causal reasoning (rule, optional LLM),
  - upsert the github_signal_enrichment system-of-record row,
  - on a default-branch advance, emit a code_intel reindex trigger (self-update),
  - mark the queue item complete.

A failed item rolls back atomically; attempts are bumped out-of-band and the
item is dead-lettered (completed-with-error) after 5 tries.
"""
from __future__ import annotations

import json
from typing import Any
from uuid import UUID

from lib.shared.ids import uuid7
from lib.shared.tenant_context import tenant_transaction

from services.ingest.github_intel import fsm
from services.ingest.github_intel.code_client import blast_radius_for, extract_changed_paths
from services.ingest.github_intel.config import GITHUB_INTEL_LLM_ENABLED, MAX_BLAST_HOPS
from services.ingest.github_intel.enrichment import assemble_intelligence, _related_entities
from services.ingest.github_intel.state_store import apply_event

_DEAD_LETTER_ATTEMPTS = 5
_DEFAULT_BRANCHES = {"main", "master"}


async def enqueue_new_github_observations(
    pool: Any, tenant_id: UUID, *, limit: int = 1000
) -> int:
    """Feeder sweep: enqueue github:webhook observations not yet queued."""
    async with tenant_transaction(tenant_id, pool=pool) as ctx:
        status = await ctx.execute(
            """
            INSERT INTO github_intel_queue (id, tenant_id, observation_id, repo, occurred_at)
            SELECT gen_random_uuid(), o.tenant_id, o.id, o.content->>'repo', o.occurred_at
              FROM observations o
             WHERE o.tenant_id = $1
               AND o.source_channel = 'github:webhook'
               AND NOT EXISTS (
                   SELECT 1 FROM github_intel_queue q WHERE q.observation_id = o.id
               )
             ORDER BY o.occurred_at
             LIMIT $2
            ON CONFLICT (observation_id) DO NOTHING
            """,
            tenant_id, limit,
        )
    return _rowcount(status)


async def drain(
    pool: Any, tenant_id: UUID, *, worker_id: str = "github_intel", max_items: int = 10000,
    llm_enabled: bool | None = None, embedder: Any | None = None,
) -> int:
    """Process queued items oldest-first until the queue drains or max_items."""
    if llm_enabled is None:
        from services.ingest.ingestion.feature_flags.client import TenantFlags
        llm_enabled = await TenantFlags(pool).get_bool(
            tenant_id, GITHUB_INTEL_LLM_ENABLED, default=False
        )
    processed = 0
    for _ in range(max_items):
        did = await process_one(
            pool, tenant_id, worker_id=worker_id, llm_enabled=llm_enabled, embedder=embedder
        )
        if not did:
            break
        processed += 1
    return processed


async def process_one(
    pool: Any, tenant_id: UUID, *, worker_id: str, llm_enabled: bool, embedder: Any | None = None
) -> bool:
    """Claim + process the oldest queued item. Returns False when queue empty."""
    claimed_id: UUID | None = None
    try:
        async with tenant_transaction(tenant_id, pool=pool) as ctx:
            row = await ctx.fetchrow(
                """
                SELECT id, observation_id, repo, occurred_at
                  FROM github_intel_queue
                 WHERE tenant_id = $1 AND completed_at IS NULL
                 ORDER BY occurred_at ASC, enqueued_at ASC
                 FOR UPDATE SKIP LOCKED
                 LIMIT 1
                """,
                tenant_id,
            )
            if row is None:
                return False
            claimed_id = row["id"]
            # serialize one repo's FSM across workers
            await ctx.fetchval(
                "SELECT pg_advisory_xact_lock(hashtext($1))",
                f"{tenant_id}:{row['repo']}",
            )
            await _process_item(
                ctx, tenant_id=tenant_id, queue_row=row,
                llm_enabled=llm_enabled, embedder=embedder,
            )
            await ctx.execute(
                "UPDATE github_intel_queue SET completed_at=now(), claimed_by=$2, "
                "claimed_at=now() WHERE id=$1",
                row["id"], worker_id,
            )
        return True
    except Exception as exc:  # noqa: BLE001 — record + dead-letter, keep draining
        if claimed_id is not None:
            async with tenant_transaction(tenant_id, pool=pool) as ctx2:
                await ctx2.execute(
                    "UPDATE github_intel_queue SET attempts=attempts+1, last_error=$2, "
                    "completed_at = CASE WHEN attempts+1 >= $3 THEN now() ELSE completed_at END "
                    "WHERE id=$1",
                    claimed_id, f"{type(exc).__name__}: {exc}"[:500], _DEAD_LETTER_ATTEMPTS,
                )
        return True


async def _process_item(
    ctx: Any, *, tenant_id: UUID, queue_row: Any, llm_enabled: bool, embedder: Any | None,
) -> None:
    obs = await ctx.fetchrow(
        "SELECT id, content, content_text, occurred_at FROM observations WHERE id=$1",
        queue_row["observation_id"],
    )
    if obs is None:
        return
    content = obs["content"]
    if isinstance(content, str):
        content = json.loads(content)
    occurred_at = obs["occurred_at"]

    ev = fsm.classify(content)
    state = await apply_event(ctx, tenant_id=tenant_id, ev=ev, occurred_at=occurred_at)

    snapshot_sha, blast = (None, {})
    if ev.repo:
        changed_paths = extract_changed_paths(content, None)
        snapshot_sha, blast = await blast_radius_for(
            ctx, tenant_id=tenant_id, repo=ev.repo,
            changed_paths=changed_paths, max_hops=MAX_BLAST_HOPS,
        )

    reasoning = fsm.rule_reasoning(
        ev, before=_first(state["before"]), after=_first(state["after"])
    )
    if llm_enabled and not fsm.is_obvious(ev):
        try:
            from services.ingest.github_intel.reasoner import llm_causal
            llm = await llm_causal(
                ev, before=state["before"], after=state["after"],
                blast_radius=blast, content=content,
            )
            if llm:
                reasoning = llm
        except Exception:  # noqa: BLE001
            pass

    related = _related_entities(ev, content)
    label = _label(state["before"], state["after"], state["state_changed"])
    intel = assemble_intelligence(
        ev, before=state["before"], after=state["after"],
        state_changed=state["state_changed"], state_label=label,
        snapshot_sha=snapshot_sha, blast_radius=blast, reasoning=reasoning, related=related,
    )

    await _upsert_enrichment(
        ctx, tenant_id=tenant_id, observation_id=obs["id"], ev=ev,
        state=state, label=label, snapshot_sha=snapshot_sha, blast=blast,
        reasoning=reasoning, related=related, intel=intel,
    )

    # self-update: default-branch advance -> reindex the code graph
    await _maybe_emit_reindex(ctx, tenant_id=tenant_id, ev=ev, content=content)


async def _upsert_enrichment(
    ctx, *, tenant_id, observation_id, ev, state, label, snapshot_sha, blast, reasoning,
    related, intel,
) -> None:
    affected_files = (blast or {}).get("changed_files", [])
    affected_symbols = [s.get("qualified_name") for s in (blast or {}).get("changed_symbols", [])]
    await ctx.execute(
        """
        INSERT INTO github_signal_enrichment
          (id, tenant_id, observation_id, repo, event_type, action, entity_kind, entity_ref,
           state_before, state_after, state_changed, affected_files, affected_symbols,
           blast_radius, code_snapshot_sha, related_entities, cause, effect, explanation,
           confidence, reasoning_path)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11,$12::jsonb,$13::jsonb,
                $14::jsonb,$15,$16::jsonb,$17,$18,$19,$20,$21)
        ON CONFLICT (observation_id) DO UPDATE SET
          state_before=EXCLUDED.state_before, state_after=EXCLUDED.state_after,
          state_changed=EXCLUDED.state_changed, affected_files=EXCLUDED.affected_files,
          affected_symbols=EXCLUDED.affected_symbols, blast_radius=EXCLUDED.blast_radius,
          code_snapshot_sha=EXCLUDED.code_snapshot_sha, related_entities=EXCLUDED.related_entities,
          cause=EXCLUDED.cause, effect=EXCLUDED.effect, explanation=EXCLUDED.explanation,
          confidence=EXCLUDED.confidence, reasoning_path=EXCLUDED.reasoning_path,
          enriched_at=now()
        """,
        uuid7(), tenant_id, observation_id, ev.repo, ev.event_type, ev.action,
        ev.entity_kind, ev.entity_ref,
        json.dumps(state["before"]), json.dumps(state["after"]), state["state_changed"],
        json.dumps(affected_files), json.dumps(affected_symbols), json.dumps(blast or {}),
        snapshot_sha, json.dumps(related), reasoning.get("cause"), reasoning.get("effect"),
        reasoning.get("explanation"), reasoning.get("confidence"),
        reasoning.get("reasoning_path", "rule"),
    )


async def _maybe_emit_reindex(ctx, *, tenant_id, ev, content) -> None:
    repo = ev.repo
    if not repo:
        return
    branch = sha = None
    kind = None
    if ev.event_type == "push":
        branch = ev.fields.get("branch")
        sha = ev.fields.get("after")
        kind = "push"
    elif ev.event_type == "pull_request" and ev.action == "closed" and ev.fields.get("merged"):
        branch = ev.fields.get("base_ref")
        sha = content.get("merge_commit_sha") or ev.fields.get("head_sha")
        kind = "merge"
    if not (branch and sha):
        return
    # only the default branch advances "the codebase"
    default = await ctx.fetchval(
        "SELECT default_branch FROM github_repo_state WHERE repo=$1", repo
    )
    if branch not in _DEFAULT_BRANCHES and (default is None or branch != default):
        return
    await ctx.execute(
        """
        INSERT INTO code_intel_index_triggers
          (id, tenant_id, repo_full_name, branch, commit_sha, kind)
        VALUES (gen_random_uuid(), $1, $2, $3, $4, $5)
        ON CONFLICT (tenant_id, repo_full_name, commit_sha) DO NOTHING
        """,
        tenant_id, repo, branch, sha, kind,
    )


def _first(d: dict[str, Any]) -> Any:
    for k in ("lifecycle", "status", "head_sha", "ci_state"):
        if k in d:
            return d[k]
    return None


def _label(before: dict[str, Any], after: dict[str, Any], changed: bool) -> str:
    if not changed:
        return "none"
    for k in ("lifecycle", "status", "ci_state"):
        if k in before or k in after:
            return f"{before.get(k)}->{after.get(k)}"
    if "head_sha" in before or "head_sha" in after:
        return f"{(before.get('head_sha') or '')[:8]}->{(after.get('head_sha') or '')[:8]}"
    return "changed"


def _rowcount(status: str) -> int:
    # asyncpg returns e.g. "INSERT 0 5"
    try:
        return int(status.split()[-1])
    except (ValueError, IndexError):
        return 0
