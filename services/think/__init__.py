"""services/think — the cognitive pipeline (BUILD-PLAN §4 Prompt 3.B).

Four triggers invoke Think; operation is uniform:
    retrieve → reason → validate → apply → cascade.

Public surface (import these from callers):

  * think(trigger, conn) — single-shot invocation; runs the full
    pipeline on the caller's connection. Used by the worker loop and
    by tests.
  * TriggerContext — the common trigger payload.
  * ThinkRunOutcome — the return shape from think().
  * ValidatedDiff / ClaimOp / ActOp / ResourceOp — the validated diff
    schema. LLM is asked to produce this exact shape.
  * region_lock_key — pure function producing (tenant_hash, entity_hash)
    integers for pg_advisory_xact_lock.

Internals (do NOT import these from outside services/think):

  * worker.py — the per-tenant poller process.
  * reason.py — think() orchestration.
  * llm_reason.py / prompt.py — LLM path.
  * validator.py — diff validation.
  * applier.py — writes + state_change emission.
  * thresholds.py — compute_threshold pure function.
  * cascade.py — spec §3 cascade engine.
  * anomaly_integration.py — check_anomalies / publish_anomalies.
  * observability.py — structlog emitters + think_runs writes.
  * diff_schema.py — Pydantic discriminated union for the diff.
  * region_locks.py — pg_advisory_xact_lock wrapper + region_lock_key.

Wave 3-B — see BUILD-LOG entry "Wave 3 Prompt 3.B".
"""
from __future__ import annotations

from .diff_schema import (
    ActOp,
    ClaimOp,
    ResourceOp,
    ValidatedDiff,
)
from .reason import ThinkRunOutcome, think
from .region_locks import region_lock_key

# TriggerContext lives in services.retrieval.primary — re-export so
# Wave-3-B callers have a single import path.
from services.retrieval.primary import TriggerContext


__all__ = [
    "ActOp",
    "ClaimOp",
    "ResourceOp",
    "ValidatedDiff",
    "ThinkRunOutcome",
    "TriggerContext",
    "region_lock_key",
    "think",
]
