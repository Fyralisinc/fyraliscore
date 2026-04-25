"""services/synthetic/core.py — direct injection API.

Per SYNTHETIC-BYPASS-PLAN §3. This is the blessed bypass into the
uniform ingestion path. Callers (tests, dev scripts, future workers
that simulate Slack/GitHub/Linear/etc traffic) hand in already-
extracted fields wrapped in a SyntheticSignal. We route the signal
through services.ingestion.core.ingest() so that entity resolution,
embedding, dedup, observation INSERT, T1 enqueue, and NOTIFY all run
exactly the way they do for real signals.

Design choices (per the plan):
- Every observation we insert carries `content.synthetic = true`
  plus `scenario_id` / `run_id` when set. Queryable and auditable;
  never silently indistinguishable from real data.
- We reuse ingest() by registering a passthrough handler the first
  time we see a given `source_channel`. The handler echoes the
  pre-built draft fields back, so the rest of the pipeline behaves
  identically to a real webhook.
- `skip_dedup` is implemented by suffixing the external_id with a
  process-local counter (the plan's §3.2 "run-local counter"
  approach) — `ingest()` stays pristine.
- `skip_t1_enqueue` maps onto ingest()'s existing
  `enqueue_trigger=False` path.
"""
from __future__ import annotations

import itertools
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Optional
from uuid import UUID, uuid4

import asyncpg

from lib.embeddings.ollama import OllamaClient
from lib.shared.errors import ValidationError
from lib.shared.types import ObservationKind, TrustTierValue
from services.actors.repo import ActorRepo
from services.entity_aliases.repo import EntityAliasRepo
from services.ingestion.core import IngestResult, ingest
from services.ingestion.handlers import (
    CHANNEL_TRUST_MAP,
    ObservationDraft,
    _HANDLERS,
)


_DEFAULT_TRUST_TIER: TrustTierValue = "inferential"

_SKIP_DEDUP_COUNTER = itertools.count()


@dataclass
class SyntheticSignal:
    """Fields a handler would have extracted from a real webhook.

    source_channel is the *pretended* channel (e.g. "slack:C01SYNTH",
    "github:acme/api"). Observation rows store this value verbatim.
    """

    source_channel: str
    content_text: str
    content: dict[str, Any]
    occurred_at: datetime
    source_actor_ref: Optional[str] = None
    external_id: Optional[str] = None
    entities_hint: list[dict[str, Any]] = field(default_factory=list)
    trust_tier: Optional[TrustTierValue] = None
    kind: ObservationKind = "signal"
    scenario_id: Optional[str] = None
    run_id: Optional[str] = None


def _resolve_trust_tier(signal: SyntheticSignal) -> TrustTierValue:
    if signal.trust_tier is not None:
        return signal.trust_tier
    mapped = CHANNEL_TRUST_MAP.get(signal.source_channel)
    if mapped is not None:
        return mapped  # type: ignore[return-value]
    return _DEFAULT_TRUST_TIER


def _ensure_passthrough_handler(source_channel: str) -> None:
    """Register a passthrough handler for `source_channel` if absent.

    The synthetic raw_payload is a dict produced by `_to_raw_payload`
    below. The handler simply rehydrates it into an ObservationDraft
    — no parsing, no signature checks. Idempotent: if *anything* is
    already registered for the channel (including a prior synthetic
    handler or, in principle, a real handler) we leave it alone.
    Callers should pick pretended channels that don't collide with
    real registrations (e.g. "slack:C01SYNTH" rather than
    "slack:message").
    """
    if source_channel in _HANDLERS:
        return

    async def _passthrough(
        payload: dict[str, Any], headers: dict[str, str]
    ) -> ObservationDraft:
        occurred_at_val = payload["occurred_at"]
        if isinstance(occurred_at_val, str):
            occurred_at = datetime.fromisoformat(occurred_at_val)
        else:
            occurred_at = occurred_at_val
        return ObservationDraft(
            source_channel=payload["source_channel"],
            content_text=payload["content_text"],
            content=dict(payload["content"]),
            occurred_at=occurred_at,
            trust_tier=payload["trust_tier"],
            kind=payload.get("kind", "signal"),
            source_actor_ref=payload.get("source_actor_ref"),
            external_id=payload.get("external_id"),
            entities_hint=list(payload.get("entities_hint") or []),
            raw_payload=payload,
        )

    _HANDLERS[source_channel] = _passthrough


def _build_content(signal: SyntheticSignal) -> dict[str, Any]:
    """Attach the synthetic marker + scenario/run ids to the content.

    We never mutate the caller's dict. Keys we add are reserved — a
    caller-supplied `synthetic` / `scenario_id` / `run_id` key in
    `signal.content` is overwritten; those keys belong to the bypass.
    """
    content = dict(signal.content)
    content["synthetic"] = True
    if signal.scenario_id is not None:
        content["scenario_id"] = signal.scenario_id
    if signal.run_id is not None:
        content["run_id"] = signal.run_id
    return content


def _to_raw_payload(
    signal: SyntheticSignal, *, skip_dedup: bool
) -> dict[str, Any]:
    if isinstance(signal.occurred_at, datetime):
        occurred_at_str = signal.occurred_at.isoformat()
    else:
        raise ValidationError(
            "SyntheticSignal.occurred_at must be a datetime",
            got=type(signal.occurred_at).__name__,
        )

    external_id = signal.external_id
    if skip_dedup:
        # Run-local counter suffix so the UNIQUE(source_channel,
        # external_id) constraint does not silently dedup repeated
        # load-test inserts. If external_id was None we synthesise
        # one — otherwise dedup would also "work" (no collision on
        # NULL) and the flag would be a no-op.
        base = external_id or f"synthetic:{uuid4()}"
        external_id = f"{base}#skipdedup-{next(_SKIP_DEDUP_COUNTER)}"

    return {
        "source_channel": signal.source_channel,
        "content_text": signal.content_text,
        "content": _build_content(signal),
        "occurred_at": occurred_at_str,
        "trust_tier": _resolve_trust_tier(signal),
        "kind": signal.kind,
        "source_actor_ref": signal.source_actor_ref,
        "external_id": external_id,
        "entities_hint": list(signal.entities_hint),
    }


async def inject(
    signal: SyntheticSignal,
    tenant_id: UUID,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    skip_dedup: bool = False,
    skip_t1_enqueue: bool = False,
) -> IngestResult:
    """Inject a synthetic signal through the uniform ingestion path.

    Returns the IngestResult from ingest(): the inserted observation,
    whether it was deduped, and the T1 trigger_queue_id (None when
    skip_t1_enqueue is True or when the row was deduped).

    Behavior:
    - Tags content.synthetic = true, plus scenario_id / run_id when
      set on the signal.
    - Resolves actor, runs fast-path entities, computes embedding,
      inserts, NOTIFY, and enqueues T1 normally.
    - skip_dedup suffixes external_id with a run-local counter so
      load tests can re-insert "the same" signal many times.
    - skip_t1_enqueue flows through to ingest()'s enqueue_trigger.
    """
    _ensure_passthrough_handler(signal.source_channel)
    raw_payload = _to_raw_payload(signal, skip_dedup=skip_dedup)
    return await ingest(
        signal.source_channel,
        raw_payload,
        pool=pool,
        tenant_id=tenant_id,
        actor_repo=actor_repo,
        alias_repo=alias_repo,
        embedder=embedder,
        enqueue_trigger=not skip_t1_enqueue,
    )


async def inject_batch(
    signals: list[SyntheticSignal],
    tenant_id: UUID,
    *,
    pool: asyncpg.Pool,
    actor_repo: ActorRepo | None = None,
    alias_repo: EntityAliasRepo | None = None,
    embedder: OllamaClient | None = None,
    skip_dedup: bool = False,
    skip_t1_enqueue: bool = False,
) -> list[IngestResult | None]:
    """Inject many signals sequentially.

    Preserves per-signal atomicity — each call is its own transaction
    inside ingest(). Failures do not abort the batch; the returned
    list has None at the failing positions and preserves input order.

    (The plan mentions controlled concurrency. We keep this serial
    for now — the bypass's consumers are dev/test scripts, where
    predictable ordering and clear error attribution matter more
    than throughput. Concurrency is a follow-up.)
    """
    results: list[IngestResult | None] = []
    for signal in signals:
        try:
            result = await inject(
                signal,
                tenant_id,
                pool=pool,
                actor_repo=actor_repo,
                alias_repo=alias_repo,
                embedder=embedder,
                skip_dedup=skip_dedup,
                skip_t1_enqueue=skip_t1_enqueue,
            )
        except Exception:
            results.append(None)
        else:
            results.append(result)
    return results


__all__ = [
    "SyntheticSignal",
    "inject",
    "inject_batch",
]
