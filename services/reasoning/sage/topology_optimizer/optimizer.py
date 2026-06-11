"""services/reasoning/sage/topology_optimizer/optimizer.py — Phase 13 Topology Optimizer.

Implements the rule-based topology updates described in doc §16
(pseudocode at §16.3) using only Discovery Utility Layer writes
(doc §22.1):

  * `AffordanceProfilesRepo.reinforce` / `.decay` — adjust per-Model
    retrieval utility based on whether the Model showed up in a valid
    diff or was repeatedly retrieved then omitted.
  * `DiscoveryShortcutsRepo.upsert_from_outcome` / `.record_failure` —
    grow learned signature -> target shortcuts for useful paths;
    punish noisy paths.
  * `NegativeMemoryRepo.insert` — record rejected hypotheses, noisy
    paths, and failed shortcuts so future inquiry skips them. Every
    row carries `expires_at` (default 60 days) per doc §14.
  * `RegionSummariesRepo` + `refresh.refresh_region` — refresh region
    sufficient-state summaries for every region touched by a useful
    Model.

Canonical topology ops (merge, split, promote, demote — doc §16.3,
§1262-1271) are produced as `dict` candidate payloads only; the
optimizer NEVER writes to `models`, `model_edges`, or `observations`.
Multi-model candidates are persisted into the existing pre-truth
`relationship_candidates` review layer so Think/humans can validate
them before any canonical truth changes.

Trigger taxonomy (`trigger_event`) matches doc §16.1:

  validated_synthesis_diff_applied | reasoning_diff_failed_validation
  user_contested_node              | user_accepted_node
  prediction_confirmed             | prediction_falsified
  omitted_evidence_later_requested | inquiry_session_ended_insufficient
  background_region_scan_complete

The trigger is logged in the report metrics but does not currently
gate which steps run — the optimizer always derives whatever it can
from the outcome event stream. Future learned policies may use the
trigger to choose between cheap and expensive update paths.
"""
from __future__ import annotations

import json
import logging
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable
from uuid import UUID

import asyncpg

from lib.shared.ids import uuid7
from services.reasoning.sage.affordances.repo import AffordanceProfilesRepo
from services.reasoning.sage.discovery.negative_memory_repo import NegativeMemoryRepo
from services.reasoning.sage.discovery.shortcuts_repo import DiscoveryShortcutsRepo
from services.reasoning.sage.discovery.types import NegativeMemory
from services.reasoning.sage.inquiry_traces.repo import OutcomeEventsRepo
from services.reasoning.sage.inquiry_traces.types import OutcomeEventRow
from services.reasoning.sage.region_summaries.refresh import (
    refresh_region,
    should_refresh,
)
from services.reasoning.sage.region_summaries.repo import RegionSummariesRepo
from services.reasoning.sage.structural_features.repo import StructuralFeaturesRepo
from services.reasoning.sage.topology_optimizer.types import OptimizationRunReport
from services.reasoning.judgment.scoring import JudgmentScores, clamp_score
from services.reasoning.relationships.candidates import (
    RelationshipCandidate,
    make_edge_candidate,
    make_situation_candidate,
)
from services.reasoning.relationships.repo import RelationshipCandidatesRepo
from services.reasoning.sage.structural_features.compute import (
    build_adjacency,
    compute_edge_features,
    compute_model_features,
)
from services.reasoning.sage.structural_features.types import StructuralEdge


_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------
# Tunables — module-level so tests / ops can monkeypatch.
# ---------------------------------------------------------------------

REINFORCE_DELTA = 0.1
DECAY_FACTOR = 0.95
SHORTCUT_POSITIVE_DELTA = 0.2
DIRECT_NODE_SHORTCUT_POSITIVE_DELTA = 1.0
NEGATIVE_MEMORY_TTL = timedelta(days=60)

# Heuristic thresholds for canonical-topology-op proposals.
PROMOTE_RECURRENCE_THRESHOLD = 3      # composition pattern seen >= N times.
ACCESS_PROMOTE_MIN_EVENTS = 8        # access routes need broad within-run support.
ACCESS_PROMOTE_MIN_MODELS = 8        # avoid canonicalizing local conveniences.
SPLIT_PRED_ERROR_THRESHOLD = 0.5      # min prediction_error to consider.
SPLIT_DISTINCT_NEIGHBORS = 3          # min distinct useful-path neighbors.
DEMOTE_LOW_UTILITY = 0.1              # max affordance utility to demote.
DEMOTE_QUIET_AFTER = timedelta(days=30)  # min time since last reinforcement.
MERGE_SHARED_PATH_HITS = 2            # pair both seen in >=N useful paths.
PROMOTE_SOURCE_MODEL_ID_LIMIT = 64    # bounded candidate payload sample.
NEGATIVE_MEMORY_OMISSION_REASONS = frozenset({
    "generic_hub",
    "redundant",
    "access_denied",
    "low_confidence",
    "out_of_scope",
    "stale",
})

# Feedback-loop structural refresh is intentionally bounded. Full-tenant
# structural recomputation belongs to the scheduled structural-features job;
# doing it after every inquiry makes adaptation O(company size).
STRUCTURAL_REFRESH_FOCUS_LIMIT = 64
STRUCTURAL_REFRESH_LOCAL_NODE_LIMIT = 512
STRUCTURAL_REFRESH_EDGE_LIMIT = 2048

# Map from doc §16.1 triggers to RegionSummaries refresh reasons.
_TRIGGER_TO_REFRESH_REASON = {
    "validated_synthesis_diff_applied": "validated_model_update",
    "reasoning_diff_failed_validation": "prediction_error",
    "user_contested_node": "user_contestation",
    "user_accepted_node": "validated_model_update",
    "prediction_confirmed": "validated_model_update",
    "prediction_falsified": "prediction_error",
    "omitted_evidence_later_requested": "high_impact_signal",
    "inquiry_session_ended_insufficient": "scheduled",
    "background_region_scan_complete": "scheduled",
}


# ---------------------------------------------------------------------
# enqueue_for_validation — pure compatibility helper.
# ---------------------------------------------------------------------


def enqueue_for_validation(candidates: list[dict]) -> list[dict]:
    """Return canonical-op candidates for compatibility with older callers.

    The product path is `TopologyOptimizer._enqueue_for_validation`, which
    persists reviewable candidates using the caller's tenant/connection.
    This function remains a pure inspector helper for tests and scripts.
    """
    return list(candidates)


# ---------------------------------------------------------------------
# Payload coercion helpers — outcome event payloads are loosely typed.
# ---------------------------------------------------------------------


def _coerce_uuid(value: Any) -> UUID | None:
    """Tolerantly read a UUID from a JSON payload (str or UUID)."""
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    try:
        return UUID(str(value))
    except (ValueError, AttributeError, TypeError):
        return None


def _payload_uuid(payload: dict, *keys: str) -> UUID | None:
    """Read the first key from `keys` that yields a UUID."""
    for key in keys:
        if key in payload:
            uid = _coerce_uuid(payload[key])
            if uid is not None:
                return uid
    return None


def _payload_str(payload: dict, *keys: str) -> str | None:
    """Read the first key that yields a non-empty string."""
    for key in keys:
        v = payload.get(key)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _payload_str_list(payload: dict, key: str) -> list[str]:
    """Read a list of strings (filtering out non-string elements)."""
    raw = payload.get(key)
    if not isinstance(raw, list):
        return []
    out: list[str] = []
    for item in raw:
        if isinstance(item, str):
            value = item.strip()
        elif isinstance(item, dict):
            value = str(
                item.get("id") or item.get("name") or item.get("type") or ""
            ).strip()
        else:
            value = ""
        if value and value not in out:
            out.append(value)
    return out


def _path_key(payload: dict) -> tuple[Any, ...] | None:
    """Build a hashable identity for a retrieval path mentioned in a
    payload.

    Paths in `path_used_in_valid_diff` payloads have no canonical
    shape yet; we accept either an explicit `path_id` or a (from, to)
    pair. Returns None when neither is present, so the caller can
    skip the event.
    """
    pid = _payload_uuid(payload, "path_id")
    if pid is not None:
        return ("id", pid)
    src = _payload_uuid(payload, "from_model_id", "source_model_id")
    dst = _payload_uuid(payload, "to_model_id", "target_model_id")
    if src is not None or dst is not None:
        return ("pair", src, dst)
    return None


def _signature_from_payload(payload: dict) -> dict[str, Any]:
    """Derive a Discovery Shortcut signature from a payload.

    Doc §11.2 shape: signal_type + entities + question_primitive. We
    accept either an inline `signature` object or top-level fields the
    Outcome Evaluator may have used. At least one field is required;
    we return an empty dict when nothing is usable and the caller
    skips the upsert.
    """
    sig_inline = payload.get("signature")
    if isinstance(sig_inline, dict):
        signature = {
            k: sig_inline[k]
            for k in ("signal_type", "entities", "question_primitive")
            if k in sig_inline
        }
        if signature:
            return signature

    signature: dict[str, Any] = {}
    signal_type = _payload_str(payload, "signal_type")
    if signal_type:
        signature["signal_type"] = signal_type
    entities = _payload_str_list(payload, "entities")
    if entities:
        signature["entities"] = entities
    question_primitive = _payload_str(payload, "question_primitive")
    if question_primitive:
        signature["question_primitive"] = question_primitive
    return signature


def _route_signature_from_signature(signature: dict[str, Any]) -> dict[str, Any]:
    """Return a same-entity route signature without question primitive.

    Low-value no-answer packets may change question mix on the next attempt.
    If we only remember one primitive was useless, a follow-up read can
    rediscover the same irrelevant region through another primitive. Requiring
    entities keeps this from becoming a tenant-wide negative memory.
    """

    raw_entities = signature.get("entities")
    if not isinstance(raw_entities, list) or not raw_entities:
        return {}
    entities = [
        str(item).strip()
        for item in raw_entities
        if str(item).strip()
    ][:12]
    if not entities:
        return {}
    route_signature: dict[str, Any] = {"entities": entities}
    signal_type = signature.get("signal_type")
    if signal_type:
        route_signature["signal_type"] = signal_type
    return route_signature


# ---------------------------------------------------------------------
# Inference helpers — pure over the outcome event list.
# ---------------------------------------------------------------------


def _infer_useful_nodes(events: Iterable[OutcomeEventRow]) -> set[UUID]:
    """Distinct model_ids appearing in `node_used_in_valid_diff` events."""
    out: set[UUID] = set()
    for e in events:
        if e.event_type != "node_used_in_valid_diff":
            continue
        mid = _payload_uuid(e.payload, "model_id", "node_id")
        if mid is not None:
            out.add(mid)
    return out


def _infer_useful_node_events(
    events: Iterable[OutcomeEventRow],
) -> list[OutcomeEventRow]:
    event_list = list(events)
    ordered = [
        *[
            event for event in event_list
            if event.event_type == "reader_decision_used_in_valid_diff"
        ],
        *[
            event for event in event_list
            if event.event_type == "node_used_in_valid_diff"
        ],
    ]
    seen: set[UUID] = set()
    out: list[OutcomeEventRow] = []
    for event in ordered:
        model_id = _payload_uuid(event.payload, "model_id", "node_id")
        if model_id is None or model_id in seen:
            continue
        seen.add(model_id)
        out.append(event)
    return out


def _infer_useful_paths(
    events: Iterable[OutcomeEventRow],
) -> list[OutcomeEventRow]:
    """Return every `path_used_in_valid_diff` event with a usable payload.

    De-duplication on `_path_key` happens downstream so we keep the
    first occurrence (preserving the original payload for signature
    derivation).
    """
    seen: set[tuple[Any, ...]] = set()
    out: list[OutcomeEventRow] = []
    for e in events:
        if e.event_type != "path_used_in_valid_diff":
            continue
        key = _path_key(e.payload)
        if key is None or key in seen:
            continue
        seen.add(key)
        out.append(e)
    return out


def _activation_contexts_by_model(
    events: Iterable[OutcomeEventRow],
) -> dict[UUID, list[dict[str, Any]]]:
    contexts: dict[UUID, list[dict[str, Any]]] = defaultdict(list)
    for event in events:
        if event.event_type not in {
            "node_used_in_valid_diff",
            "reader_decision_used_in_valid_diff",
        }:
            continue
        model_id = _payload_uuid(event.payload, "model_id", "node_id")
        if model_id is None:
            continue
        signature = _signature_from_payload(event.payload)
        if signature:
            contexts[model_id].append(signature)
    return contexts


def _merge_activation_signatures(
    existing: dict[str, Any] | None,
    contexts: list[dict[str, Any]],
) -> dict[str, Any]:
    merged = dict(existing or {})

    def merge_list(key: str, values: Iterable[str]) -> None:
        current = [
            str(value)
            for value in merged.get(key, [])
            if value is not None
        ] if isinstance(merged.get(key), list) else []
        for value in values:
            text = str(value or "").strip()
            if text and text not in current:
                current.append(text)
        if current:
            merged[key] = current[:64]

    for context in contexts:
        merge_list("entities", _payload_str_list(context, "entities"))
        signal_type = _payload_str(context, "signal_type")
        if signal_type:
            merge_list("signal_types", [signal_type])
        primitive = _payload_str(context, "question_primitive")
        if primitive:
            merge_list("question_primitives", [primitive])
    return merged


def _infer_noisy_paths_and_omissions(
    events: list[OutcomeEventRow],
    *,
    useful_paths: list[OutcomeEventRow],
) -> tuple[list[OutcomeEventRow], Counter[UUID]]:
    """Find paths/items retrieved-but-omitted that were NOT used later.

    Returns:
      * list of `retrieved_evidence_omitted` events whose model_id was
        never later requested AND whose path key does not appear in
        `useful_paths`.
      * Counter of model_id -> times the same model_id was omitted (so
        the caller can decay repeat offenders).
    """
    later_requested_models: set[UUID] = set()
    for e in events:
        if e.event_type != "omitted_evidence_later_requested":
            continue
        mid = _payload_uuid(e.payload, "model_id", "source_ref_id", "node_id")
        if mid is not None:
            later_requested_models.add(mid)

    useful_path_keys = {_path_key(e.payload) for e in useful_paths}

    noisy: list[OutcomeEventRow] = []
    omit_counts: Counter[UUID] = Counter()
    for e in events:
        if e.event_type not in {
            "retrieved_evidence_omitted",
            "reader_decision_low_value",
        }:
            continue
        if e.event_type == "retrieved_evidence_omitted":
            reason = _payload_str(e.payload, "omission_reason")
            if reason not in NEGATIVE_MEMORY_OMISSION_REASONS:
                continue
        mid = _payload_uuid(e.payload, "model_id", "source_ref_id", "node_id")
        path_key = _path_key(e.payload)
        if mid is not None and mid in later_requested_models:
            continue
        if path_key is not None and path_key in useful_path_keys:
            continue
        if mid is not None:
            omit_counts[mid] += 1
        noisy.append(e)
    return noisy, omit_counts


def _infer_missing_anchors(
    events: Iterable[OutcomeEventRow],
) -> list[dict[str, Any]]:
    """Payloads from `validation_failed_due_to_missing_evidence` events."""
    return [
        dict(e.payload)
        for e in events
        if e.event_type == "validation_failed_due_to_missing_evidence"
    ]


def _latest_quality_payload(events: Iterable[OutcomeEventRow]) -> dict[str, Any]:
    """Most recent `outcome_quality_assessed` payload, if present."""
    latest: OutcomeEventRow | None = None
    for event in events:
        if event.event_type == "outcome_quality_assessed":
            latest = event
    return dict(latest.payload) if latest is not None else {}


# ---------------------------------------------------------------------
# TopologyOptimizer
# ---------------------------------------------------------------------


class TopologyOptimizer:
    """Rule-based topology updates following doc §16.

    Construction is tenant-scoped: all dependent repos share the same
    `tenant_id`. Callers may inject their own repo instances (handy
    for tests that want shared connections); otherwise the optimizer
    constructs default repos against `pool`.

    The optimizer never opens its own transaction — when a caller
    passes `conn=`, every read + write travels through it; otherwise
    each repo call acquires its own pool connection. This matches the
    Wave 1 repo style and lets the post-commit evaluator that triggers
    the optimizer choose whether to inherit its transaction.
    """

    def __init__(
        self,
        *,
        pool: asyncpg.Pool | None,
        tenant_id: UUID,
        affordances_repo: AffordanceProfilesRepo | None = None,
        shortcuts_repo: DiscoveryShortcutsRepo | None = None,
        negative_memory_repo: NegativeMemoryRepo | None = None,
        region_summaries_repo: RegionSummariesRepo | None = None,
        structural_features_repo: StructuralFeaturesRepo | None = None,
        outcome_events_repo: OutcomeEventsRepo | None = None,
        relationship_candidates_repo: RelationshipCandidatesRepo | None = None,
    ) -> None:
        self._pool = pool
        self._tenant_id = tenant_id
        self._affordances = affordances_repo or AffordanceProfilesRepo(
            pool, tenant_id=tenant_id,
        )
        self._shortcuts = shortcuts_repo or DiscoveryShortcutsRepo(
            pool, tenant_id=tenant_id,
        )
        self._negative_memory = negative_memory_repo or NegativeMemoryRepo(
            pool, tenant_id=tenant_id,
        )
        self._region_summaries = region_summaries_repo or RegionSummariesRepo(
            pool, tenant_id=tenant_id,
        )
        # StructuralFeaturesRepo requires a non-None pool when conn is None.
        # We only construct a default when pool is provided; otherwise the
        # caller MUST inject one if they expect region refresh to work.
        if structural_features_repo is not None:
            self._structural_features = structural_features_repo
        elif pool is not None:
            self._structural_features = StructuralFeaturesRepo(
                pool, tenant_id=tenant_id,
            )
        else:
            self._structural_features = None  # type: ignore[assignment]
        self._outcome_events = outcome_events_repo or OutcomeEventsRepo(
            pool, tenant_id=tenant_id,
        )
        self._relationship_candidates = (
            relationship_candidates_repo or RelationshipCandidatesRepo()
        )

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    async def optimize(
        self,
        *,
        inquiry_session_id: UUID,
        trigger_event: str,
        conn: asyncpg.Connection | None = None,
    ) -> OptimizationRunReport:
        """Run one rule-based topology update pass.

        Doc §16.3 pseudocode, concretized over the Phase 1 outcome
        event stream. Every step is best-effort: a missing affordance
        profile, an empty payload, or a region with no structural
        features is logged and skipped — the optimizer never fails the
        whole run for one degenerate event.
        """
        events = await self._outcome_events.list_for_session(
            inquiry_session_id, conn=conn,
        )

        useful_nodes = _infer_useful_nodes(events)
        useful_node_events = _infer_useful_node_events(events)
        useful_path_events = _infer_useful_paths(events)
        noisy_omissions, omit_counts = _infer_noisy_paths_and_omissions(
            events, useful_paths=useful_path_events,
        )
        missing_anchors = _infer_missing_anchors(events)
        quality_payload = _latest_quality_payload(events)

        # 1. Affordance updates (utility layer, never models table).
        reinforces, decays = await self._update_affordances(
            useful_nodes=useful_nodes,
            omit_counts=omit_counts,
            events=events,
            conn=conn,
        )

        # 2. Shortcut updates (utility layer).
        shortcut_creates, shortcut_failures, shortcut_missing_model_skips = (
            await self._update_shortcuts(
                useful_nodes=useful_node_events,
                useful_paths=useful_path_events,
                noisy_paths=noisy_omissions,
                conn=conn,
            )
        )

        # 3. Negative memory inserts (utility layer).
        negmem_inserts = await self._insert_negative_memory(
            events=events,
            noisy_paths=noisy_omissions,
            conn=conn,
        )

        # 4. Region summary refreshes (utility layer).
        region_refreshes = await self._refresh_regions(
            useful_nodes=useful_nodes,
            trigger_event=trigger_event,
            conn=conn,
        )

        # 4b. Structural feature refresh (utility layer). This keeps
        # bridge/hub scores and edge-overlap features current enough for
        # the next reader pass without mutating canonical graph truth.
        structural_counts = await self._refresh_structural_features(
            useful_nodes=useful_nodes,
            useful_paths=useful_path_events,
            conn=conn,
        )

        # 4c. Question-policy credit assignment. This is the bridge
        # from writer success back to reader decision quality.
        question_policy_updates = await self._update_question_policy_stats(
            inquiry_session_id=inquiry_session_id,
            events=events,
            conn=conn,
        )

        # 5. Canonical topology candidates — NEVER applied here.
        merge = tuple(self._propose_merges(
            useful_paths=useful_path_events,
            session_id=inquiry_session_id,
        ))
        split = tuple(self._propose_splits(
            events=events,
            useful_paths=useful_path_events,
            session_id=inquiry_session_id,
        ))
        promote = tuple(self._propose_promotes(
            events=events,
            useful_nodes=useful_node_events,
            useful_paths=useful_path_events,
            session_id=inquiry_session_id,
        ))
        demote = tuple(self._propose_demotes(
            events=events,
            session_id=inquiry_session_id,
        ))

        validation_enqueued = await self._enqueue_for_validation(
            list(merge + split + promote + demote),
            session_id=inquiry_session_id,
            conn=conn,
        )

        metrics: dict[str, float] = {
            "useful_nodes": float(len(useful_nodes)),
            "useful_paths": float(len(useful_path_events)),
            "noisy_paths": float(len(noisy_omissions)),
            "missing_anchors": float(len(missing_anchors)),
            "quality_failure_modes": float(
                len(quality_payload.get("failure_modes") or [])
            ),
            "objective_alignment_score": float(
                quality_payload.get("objective_alignment_score") or 0.0
            ),
            "trigger_recognized": float(
                1.0 if trigger_event in _TRIGGER_TO_REFRESH_REASON else 0.0
            ),
            "structural_models_written": float(
                structural_counts.get("models_written", 0)
            ),
            "structural_edges_written": float(
                structural_counts.get("edges_written", 0)
            ),
            "structural_missing_model_skips": float(
                structural_counts.get("missing_model_skips", 0)
            ),
            "shortcut_missing_model_skips": float(shortcut_missing_model_skips),
            "question_policy_updates": float(question_policy_updates),
            "canonical_validation_enqueued": float(validation_enqueued),
        }

        return OptimizationRunReport(
            inquiry_session_id=inquiry_session_id,
            affordance_reinforces=reinforces,
            affordance_decays=decays,
            shortcut_creates_or_bumps=shortcut_creates,
            shortcut_decays=shortcut_failures,
            negative_memory_inserts=negmem_inserts,
            region_refreshes=region_refreshes,
            question_policy_updates=question_policy_updates,
            canonical_merge_candidates=merge,
            canonical_split_candidates=split,
            canonical_promote_candidates=promote,
            canonical_demote_candidates=demote,
            metrics=metrics,
        )

    # ------------------------------------------------------------------
    # Step 1 — affordance reinforce / decay
    # ------------------------------------------------------------------

    async def _update_affordances(
        self,
        *,
        useful_nodes: set[UUID],
        omit_counts: Counter[UUID],
        events: list[OutcomeEventRow],
        conn: asyncpg.Connection | None,
    ) -> tuple[int, int]:
        activation_contexts = _activation_contexts_by_model(events)
        reinforces = 0
        for mid in useful_nodes:
            try:
                row = await self._affordances.reinforce(
                    mid, REINFORCE_DELTA, conn=conn,
                )
            except Exception:  # pragma: no cover — defensive
                _log.exception(
                    "affordance.reinforce failed", extra={"model_id": str(mid)},
                )
                continue
            if row is not None:
                reinforces += 1
                contexts = activation_contexts.get(mid, [])
                if contexts:
                    merged = _merge_activation_signatures(
                        row.activation_signatures,
                        contexts,
                    )
                    if merged != row.activation_signatures:
                        await self._update_affordance_activation_signatures(
                            model_id=mid,
                            activation_signatures=merged,
                            conn=conn,
                        )
            else:
                # No profile yet — the optimizer doesn't auto-seed
                # (see policy.derive_default_profile_from_model). Skip.
                _log.debug(
                    "affordance.reinforce skipped: no profile",
                    extra={"model_id": str(mid)},
                )

        decays = 0
        # Only decay model_ids retrieved-and-omitted more than once.
        # A single omission is not enough evidence to punish.
        for mid, count in omit_counts.items():
            if count < 2:
                continue
            if mid in useful_nodes:
                # The node was also useful — don't punish it.
                continue
            try:
                row = await self._affordances.decay(
                    mid, DECAY_FACTOR, conn=conn,
                )
            except Exception:  # pragma: no cover
                _log.exception(
                    "affordance.decay failed", extra={"model_id": str(mid)},
                )
                continue
            if row is not None:
                decays += 1
        return reinforces, decays

    async def _existing_model_ids(
        self,
        model_ids: Iterable[UUID],
        *,
        conn: asyncpg.Connection | None,
    ) -> set[UUID]:
        ids = list(dict.fromkeys(model_ids))
        if not ids:
            return set()

        async def _do(c: asyncpg.Connection) -> set[UUID]:
            rows = await c.fetch(
                """
                SELECT id
                FROM models
                WHERE tenant_id = $1
                  AND id = ANY($2::uuid[])
                """,
                self._tenant_id,
                ids,
            )
            return {row["id"] for row in rows}

        if conn is not None:
            return await _do(conn)
        if self._pool is None:
            return set(ids)
        async with self._pool.acquire() as owned:
            return await _do(owned)

    async def _update_affordance_activation_signatures(
        self,
        *,
        model_id: UUID,
        activation_signatures: dict[str, Any],
        conn: asyncpg.Connection | None,
    ) -> None:
        async def _do(c: asyncpg.Connection) -> None:
            await c.execute(
                """
                UPDATE retrieval_affordance_profiles
                   SET activation_signatures = $3::jsonb,
                       last_updated_at = now()
                 WHERE model_id = $1
                   AND tenant_id = $2
                """,
                model_id,
                self._tenant_id,
                json.dumps(activation_signatures, default=str),
            )

        try:
            if conn is not None:
                await _do(conn)
                return
            if self._pool is None:
                return
            async with self._pool.acquire() as owned:
                await _do(owned)
        except Exception:  # pragma: no cover
            _log.exception(
                "affordance.activation_signature_update failed",
                extra={"model_id": str(model_id)},
            )

    async def _update_question_policy_stats(
        self,
        *,
        inquiry_session_id: UUID,
        events: list[OutcomeEventRow],
        conn: asyncpg.Connection | None,
    ) -> int:
        credit_events = [
            e for e in events
            if e.event_type == "reader_decision_used_in_valid_diff"
        ]
        low_value_events = [
            e for e in events
            if e.event_type == "reader_decision_low_value"
        ]
        if not credit_events and not low_value_events:
            return 0
        if conn is None and self._pool is None:
            return 0

        async def _do(c: asyncpg.Connection) -> int:
            stats_table = await c.fetchval(
                "SELECT to_regclass('public.sage_question_policy_stats')"
            )
            attr_table = await c.fetchval(
                "SELECT to_regclass('public.sage_reader_decision_attributions')"
            )
            if stats_table is None or attr_table is None:
                return 0

            attempt_rows = await c.fetch(
                """
                SELECT signal_type, question_primitive,
                       COUNT(DISTINCT question_id) AS attempts,
                       COALESCE(SUM(expected_cost), 0.0) AS total_cost
                FROM (
                    SELECT DISTINCT ON (question_id, signal_type, question_primitive)
                           question_id, signal_type, question_primitive,
                           expected_cost
                    FROM sage_reader_decision_attributions
                    WHERE tenant_id = $1
                      AND inquiry_session_id = $2
                    ORDER BY question_id, signal_type, question_primitive,
                             activation_score DESC
                ) AS per_question
                GROUP BY signal_type, question_primitive
                """,
                self._tenant_id,
                inquiry_session_id,
            )
            success_by_key: dict[tuple[str, str], dict[str, float]] = {}
            for ev in credit_events:
                signal_type = str(ev.payload.get("signal_type") or "unknown")
                primitive = str(
                    ev.payload.get("question_primitive") or "UNKNOWN"
                ).upper()
                key = (signal_type, primitive)
                bucket = success_by_key.setdefault(
                    key, {"successes": 0.0, "credit": 0.0}
                )
                bucket["successes"] += 1.0
                try:
                    bucket["credit"] += float(ev.payload.get("credit_score") or 0.0)
                except (TypeError, ValueError):
                    pass

            updates = 0
            for row in attempt_rows:
                signal_type = str(row["signal_type"] or "unknown")
                primitive = str(row["question_primitive"] or "UNKNOWN").upper()
                key = (signal_type, primitive)
                success = success_by_key.get(key, {})
                successes = int(success.get("successes", 0.0))
                credit = float(success.get("credit", 0.0))
                attempts = int(row["attempts"] or 0)
                total_cost = float(row["total_cost"] or 0.0)
                await c.execute(
                    """
                    INSERT INTO sage_question_policy_stats (
                      id, tenant_id, signal_type, question_primitive,
                      attempts, successes, total_credit, total_cost,
                      utility_score, last_credit_at, updated_at
                    ) VALUES (
                      $1, $2, $3, $4,
                      $5, $6, $7, $8,
                      (($7::double precision - $8::double precision)
                        / GREATEST($5::integer, 1)),
                      CASE WHEN $6 > 0 THEN now() ELSE NULL END,
                      now()
                    )
                    ON CONFLICT (tenant_id, signal_type, question_primitive)
                    DO UPDATE SET
                      attempts = sage_question_policy_stats.attempts + EXCLUDED.attempts,
                      successes = sage_question_policy_stats.successes + EXCLUDED.successes,
                      total_credit = sage_question_policy_stats.total_credit
                        + EXCLUDED.total_credit,
                      total_cost = sage_question_policy_stats.total_cost
                        + EXCLUDED.total_cost,
                      utility_score = (
                        (sage_question_policy_stats.total_credit + EXCLUDED.total_credit)
                        - (sage_question_policy_stats.total_cost + EXCLUDED.total_cost)
                      ) / GREATEST(
                        sage_question_policy_stats.attempts + EXCLUDED.attempts,
                        1
                      ),
                      last_credit_at = COALESCE(
                        EXCLUDED.last_credit_at,
                        sage_question_policy_stats.last_credit_at
                      ),
                      updated_at = now()
                    """,
                    uuid7(),
                    self._tenant_id,
                    signal_type,
                    primitive,
                    attempts,
                    successes,
                    credit,
                    total_cost,
                )
                updates += 1
            return updates

        if conn is not None:
            return await _do(conn)
        async with self._pool.acquire() as owned:
            return await _do(owned)

    # ------------------------------------------------------------------
    # Step 2 — shortcut upserts / failures
    # ------------------------------------------------------------------

    async def _update_shortcuts(
        self,
        *,
        useful_nodes: list[OutcomeEventRow],
        useful_paths: list[OutcomeEventRow],
        noisy_paths: list[OutcomeEventRow],
        conn: asyncpg.Connection | None,
    ) -> tuple[int, int, int]:
        creates = 0
        missing_model_skips = 0
        model_targets: set[UUID] = set()
        for ev in useful_nodes:
            to_model_id = _payload_uuid(ev.payload, "model_id", "node_id")
            if to_model_id is not None:
                model_targets.add(to_model_id)
        for ev in useful_paths:
            to_model_id = _payload_uuid(
                ev.payload, "to_model_id", "target_model_id",
            )
            if to_model_id is not None:
                model_targets.add(to_model_id)
        existing_model_targets = await self._existing_model_ids(
            model_targets,
            conn=conn,
        )
        seen_direct: set[tuple[str, UUID]] = set()
        for ev in useful_nodes:
            signature = _signature_from_payload(ev.payload)
            to_model_id = _payload_uuid(ev.payload, "model_id", "node_id")
            if not signature or to_model_id is None:
                continue
            if to_model_id not in existing_model_targets:
                missing_model_skips += 1
                continue
            key = (json.dumps(signature, sort_keys=True), to_model_id)
            if key in seen_direct:
                continue
            seen_direct.add(key)
            try:
                await self._shortcuts.upsert_from_outcome(
                    signature,
                    to_model_id=to_model_id,
                    delta_utility=DIRECT_NODE_SHORTCUT_POSITIVE_DELTA,
                    conn=conn,
                )
            except Exception:  # pragma: no cover
                _log.exception("shortcut.upsert_from_useful_node failed")
                continue
            creates += 1

        for ev in useful_paths:
            signature = _signature_from_payload(ev.payload)
            if not signature:
                continue
            to_model_id = _payload_uuid(
                ev.payload, "to_model_id", "target_model_id",
            )
            to_region_id = _payload_uuid(ev.payload, "to_region_id")
            to_affordance = _payload_str(ev.payload, "to_affordance")
            if (
                to_model_id is None
                and to_region_id is None
                and to_affordance is None
            ):
                continue
            if to_model_id is not None and to_model_id not in existing_model_targets:
                missing_model_skips += 1
                continue
            try:
                await self._shortcuts.upsert_from_outcome(
                    signature,
                    to_model_id=to_model_id,
                    to_region_id=to_region_id,
                    to_affordance=to_affordance,
                    delta_utility=SHORTCUT_POSITIVE_DELTA,
                    conn=conn,
                )
            except Exception:  # pragma: no cover
                _log.exception("shortcut.upsert_from_outcome failed")
                continue
            creates += 1

        failures = 0
        for ev in noisy_paths:
            shortcut_id = _payload_uuid(ev.payload, "shortcut_id")
            if shortcut_id is None:
                # The omission was not tied to a known shortcut, so
                # there is nothing to decay; negative-memory insert
                # (step 3) is the only signal we record.
                continue
            try:
                row = await self._shortcuts.record_failure(
                    shortcut_id, conn=conn,
                )
            except Exception:  # pragma: no cover
                _log.exception(
                    "shortcut.record_failure failed",
                    extra={"shortcut_id": str(shortcut_id)},
                )
                continue
            if row is not None:
                failures += 1
        return creates, failures, missing_model_skips

    # ------------------------------------------------------------------
    # Step 3 — negative memory inserts
    # ------------------------------------------------------------------

    async def _insert_negative_memory(
        self,
        *,
        events: list[OutcomeEventRow],
        noisy_paths: list[OutcomeEventRow],
        conn: asyncpg.Connection | None,
    ) -> int:
        inserted = 0
        expires_at = datetime.now(timezone.utc) + NEGATIVE_MEMORY_TTL

        # 3a. Rejected hypotheses — surfaced via the payload field
        # `rejected_hypothesis` on any event (typically a
        # `validation_failed_*` or `model_later_falsified`).
        for ev in events:
            rejected = ev.payload.get("rejected_hypothesis")
            if not rejected:
                continue
            signature = _signature_from_payload(ev.payload) or {
                "signal_type": "rejected_hypothesis",
            }
            mem = NegativeMemory(
                id=uuid7(),
                tenant_id=self._tenant_id,
                memory_type="rejected_hypothesis",
                signature=signature,
                rejected_claim=(
                    rejected if isinstance(rejected, str) else None
                ),
                rejected_path=(
                    rejected if isinstance(rejected, (dict, list)) else None
                ),
                reason=_payload_str(ev.payload, "reason")
                or "hypothesis rejected during inquiry",
                evidence_snapshot_hash=_payload_str(
                    ev.payload, "evidence_snapshot_hash",
                ),
                confidence=None,
                expires_at=expires_at,
            )
            try:
                await self._negative_memory.insert(mem, conn=conn)
            except Exception:  # pragma: no cover
                _log.exception("negative_memory.insert(rejected) failed")
                continue
            inserted += 1

        # 3b. Noisy paths — insert one negative memory per noisy event.
        route_negative_memory_keys: set[str] = set()
        for ev in noisy_paths:
            signature = _signature_from_payload(ev.payload) or {
                "signal_type": "noisy_path",
            }
            model_id = _payload_uuid(ev.payload, "model_id", "node_id")
            if ev.event_type == "reader_decision_low_value" and model_id is not None:
                memory_type = "low_value_node"
                rejected_path: dict[str, Any] | list[Any] | None = {
                    "model_id": str(model_id),
                    "attribution_id": _payload_str(ev.payload, "attribution_id"),
                    "question_id": _payload_str(ev.payload, "question_id"),
                    "reason": _payload_str(ev.payload, "reason")
                    or "selected reader context was not used",
                }
            else:
                memory_type = "noisy_path"
                rejected_path = ev.payload.get("retrieval_paths") or [
                    {"path_key": list(_path_key(ev.payload) or ())}
                ]
            mem = NegativeMemory(
                id=uuid7(),
                tenant_id=self._tenant_id,
                memory_type=memory_type,
                signature=signature,
                rejected_claim=None,
                rejected_path=rejected_path,
                reason=_payload_str(ev.payload, "reason", "omission_reason")
                or "retrieved evidence omitted and not later requested",
                evidence_snapshot_hash=_payload_str(
                    ev.payload, "evidence_snapshot_hash",
                ),
                confidence=None,
                expires_at=expires_at,
            )
            try:
                await self._negative_memory.insert(mem, conn=conn)
            except Exception:  # pragma: no cover
                _log.exception("negative_memory.insert(noisy_path) failed")
                continue
            inserted += 1
            if ev.event_type == "reader_decision_low_value":
                route_signature = _route_signature_from_signature(signature)
                route_key = (
                    json.dumps(route_signature, sort_keys=True, default=str)
                    if route_signature else None
                )
                if route_signature and route_key not in route_negative_memory_keys:
                    route_negative_memory_keys.add(str(route_key))
                    route_mem = NegativeMemory(
                        id=uuid7(),
                        tenant_id=self._tenant_id,
                        memory_type="noisy_path",
                        signature=route_signature,
                        rejected_claim=None,
                        rejected_path={
                            "route": "selected_reader_context_unused",
                            "model_id": (
                                str(model_id) if model_id is not None else None
                            ),
                            "question_id": _payload_str(ev.payload, "question_id"),
                            "reason": _payload_str(ev.payload, "reason")
                            or "selected reader context was not used",
                        },
                        reason=(
                            "selected_reader_context_unused_by_valid_noop_route"
                        ),
                        evidence_snapshot_hash=_payload_str(
                            ev.payload, "evidence_snapshot_hash",
                        ),
                        confidence=None,
                        expires_at=expires_at,
                    )
                    try:
                        await self._negative_memory.insert(route_mem, conn=conn)
                    except Exception:  # pragma: no cover
                        _log.exception(
                            "negative_memory.insert(low_value_route) failed",
                        )
                        continue
                    inserted += 1

        # 3c. Failed shortcuts — payload field `shortcut_id` indicates
        # the shortcut that just misfired (recorded in step 2 too).
        for ev in noisy_paths:
            shortcut_id = _payload_uuid(ev.payload, "shortcut_id")
            if shortcut_id is None:
                continue
            signature = _signature_from_payload(ev.payload) or {
                "signal_type": "failed_shortcut",
            }
            mem = NegativeMemory(
                id=uuid7(),
                tenant_id=self._tenant_id,
                memory_type="failed_shortcut",
                signature=signature,
                rejected_claim=None,
                rejected_path={"shortcut_id": str(shortcut_id)},
                reason=_payload_str(ev.payload, "reason")
                or "shortcut surfaced evidence that was not used",
                evidence_snapshot_hash=None,
                confidence=None,
                expires_at=expires_at,
            )
            try:
                await self._negative_memory.insert(mem, conn=conn)
            except Exception:  # pragma: no cover
                _log.exception("negative_memory.insert(failed_shortcut) failed")
                continue
            inserted += 1

        return inserted

    # ------------------------------------------------------------------
    # Step 4 — utility topology refreshes
    # ------------------------------------------------------------------

    async def _refresh_structural_features(
        self,
        *,
        useful_nodes: set[UUID],
        useful_paths: list[OutcomeEventRow],
        conn: asyncpg.Connection | None,
    ) -> dict[str, int]:
        focus_ids = set(useful_nodes)
        for event in useful_paths:
            source = _payload_uuid(event.payload, "source_model_id")
            target = _payload_uuid(event.payload, "target_model_id")
            if source is not None:
                focus_ids.add(source)
            if target is not None:
                focus_ids.add(target)
        if not focus_ids:
            return {
                "models_written": 0,
                "edges_written": 0,
                "missing_model_skips": 0,
            }

        ordered_focus = sorted(focus_ids, key=str)[:STRUCTURAL_REFRESH_FOCUS_LIMIT]

        async def _do(c: asyncpg.Connection) -> dict[str, int]:
            existing_focus = await self._existing_model_ids(
                ordered_focus,
                conn=c,
            )
            missing_model_skips = len(ordered_focus) - len(existing_focus)
            if not existing_focus:
                return {
                    "models_written": 0,
                    "edges_written": 0,
                    "missing_model_skips": missing_model_skips,
                }
            ordered_existing_focus = sorted(existing_focus, key=str)
            try:
                incident_rows = await c.fetch(
                    """
                    SELECT id, source_model_id, target_model_id, edge_kind, weight
                    FROM model_edges
                    WHERE tenant_id = $1
                      AND status = 'active'
                      AND (
                        source_model_id = ANY($2::uuid[])
                        OR target_model_id = ANY($2::uuid[])
                      )
                    ORDER BY created_at DESC, id
                    LIMIT $3
                    """,
                    self._tenant_id,
                    ordered_existing_focus,
                    STRUCTURAL_REFRESH_EDGE_LIMIT,
                )
            except Exception:  # pragma: no cover
                _log.exception("structural_features.local_incident_fetch failed")
                return {
                    "models_written": 0,
                    "edges_written": 0,
                    "missing_model_skips": missing_model_skips,
                }

            local_ids = set(ordered_existing_focus)
            for row in incident_rows:
                local_ids.add(row["source_model_id"])
                local_ids.add(row["target_model_id"])
                if len(local_ids) >= STRUCTURAL_REFRESH_LOCAL_NODE_LIMIT:
                    break

            ordered_local_candidates = sorted(local_ids, key=str)[
                :STRUCTURAL_REFRESH_LOCAL_NODE_LIMIT
            ]
            existing_local_ids = await self._existing_model_ids(
                ordered_local_candidates,
                conn=c,
            )
            missing_model_skips += (
                len(ordered_local_candidates) - len(existing_local_ids)
            )
            if not existing_local_ids:
                return {
                    "models_written": 0,
                    "edges_written": 0,
                    "missing_model_skips": missing_model_skips,
                }
            ordered_local_ids = sorted(existing_local_ids, key=str)
            try:
                edge_rows = await c.fetch(
                    """
                    SELECT id, source_model_id, target_model_id, edge_kind, weight
                    FROM model_edges
                    WHERE tenant_id = $1
                      AND status = 'active'
                      AND source_model_id = ANY($2::uuid[])
                      AND target_model_id = ANY($2::uuid[])
                    ORDER BY created_at DESC, id
                    LIMIT $3
                    """,
                    self._tenant_id,
                    ordered_local_ids,
                    STRUCTURAL_REFRESH_EDGE_LIMIT,
                )
            except Exception:  # pragma: no cover
                _log.exception("structural_features.local_edge_fetch failed")
                return {
                    "models_written": 0,
                    "edges_written": 0,
                    "missing_model_skips": missing_model_skips,
                }

            edges = [
                StructuralEdge(
                    edge_id=row["id"],
                    source_model_id=row["source_model_id"],
                    target_model_id=row["target_model_id"],
                    edge_kind=row["edge_kind"],
                    weight=row["weight"],
                )
                for row in edge_rows
            ]
            try:
                undirected, _out, _in = build_adjacency(ordered_local_ids, edges)
                model_rows = await compute_model_features(
                    ordered_local_ids,
                    edges,
                    tenant_id=self._tenant_id,
                )
                edge_feature_rows = await compute_edge_features(
                    edges,
                    undirected,
                    tenant_id=self._tenant_id,
                )
                repo = self._structural_features
                if repo is None:
                    repo = StructuralFeaturesRepo(  # type: ignore[arg-type]
                        self._pool,
                        tenant_id=self._tenant_id,
                    )
                models_written = await repo.upsert_model_features(
                    model_rows,
                    conn=c,
                )
                edges_written = await repo.upsert_edge_features(
                    edge_feature_rows,
                    conn=c,
                )
                return {
                    "models_written": models_written,
                    "edges_written": edges_written,
                    "missing_model_skips": missing_model_skips,
                }
            except Exception:  # pragma: no cover
                _log.exception("structural_features.local_refresh failed")
                return {
                    "models_written": 0,
                    "edges_written": 0,
                    "missing_model_skips": missing_model_skips,
                }

        if conn is not None:
            return await _do(conn)
        if self._pool is None:
            return {
                "models_written": 0,
                "edges_written": 0,
                "missing_model_skips": 0,
            }
        async with self._pool.acquire() as owned:
            return await _do(owned)

    # ------------------------------------------------------------------
    # Step 4b — region summary refreshes
    # ------------------------------------------------------------------

    async def _refresh_regions(
        self,
        *,
        useful_nodes: set[UUID],
        trigger_event: str,
        conn: asyncpg.Connection | None,
    ) -> int:
        if not useful_nodes:
            return 0
        if self._structural_features is None:
            _log.debug("region refresh skipped: no structural features repo")
            return 0

        region_ids: set[UUID] = set()
        try:
            features = await self._structural_features.get_for_models(
                useful_nodes, conn=conn,
            )
        except Exception:  # pragma: no cover
            _log.exception("structural_features.get_for_models failed")
            return 0
        for feat in features:
            for rid in feat.region_ids or []:
                region_ids.add(rid)
        if not region_ids:
            return 0

        reason = _TRIGGER_TO_REFRESH_REASON.get(trigger_event, "scheduled")
        refreshes = 0
        for rid in region_ids:
            try:
                current = await self._region_summaries.get(rid, conn=conn)
            except Exception:  # pragma: no cover
                _log.exception("region_summaries.get failed",
                               extra={"region_id": str(rid)})
                continue
            if current is None:
                # No summary yet — caller responsible for seeding one
                # before the optimizer can refresh; skip rather than
                # crash. The Outcome Evaluator may upsert a stub later.
                continue
            if not should_refresh(current, reason):
                continue
            if conn is None:
                # refresh_region requires a connection (writes to repo
                # under the hood). Acquire from the pool if we have one.
                if self._pool is None:
                    continue
                async with self._pool.acquire() as owned:
                    try:
                        await refresh_region(
                            rid, self._tenant_id, owned, reason=reason,
                        )
                    except Exception:  # pragma: no cover
                        _log.exception(
                            "refresh_region failed",
                            extra={"region_id": str(rid)},
                        )
                        continue
            else:
                try:
                    await refresh_region(
                        rid, self._tenant_id, conn, reason=reason,
                    )
                except Exception:  # pragma: no cover
                    _log.exception(
                        "refresh_region failed",
                        extra={"region_id": str(rid)},
                    )
                    continue
            refreshes += 1
        return refreshes

    # ------------------------------------------------------------------
    # Step 4d — validation queue handoff
    # ------------------------------------------------------------------

    async def _enqueue_for_validation(
        self,
        candidates: list[dict],
        *,
        session_id: UUID,
        conn: asyncpg.Connection | None = None,
    ) -> int:
        """Persist multi-model canonical-op proposals into the pre-truth queue."""
        payloads = enqueue_for_validation(candidates)
        if not payloads:
            return 0

        if conn is not None:
            return await self._enqueue_for_validation_with_conn(
                payloads,
                session_id=session_id,
                conn=conn,
            )
        if self._pool is None:
            return 0
        async with self._pool.acquire() as owned_conn:
            return await self._enqueue_for_validation_with_conn(
                payloads,
                session_id=session_id,
                conn=owned_conn,
            )

    async def _enqueue_for_validation_with_conn(
        self,
        candidates: list[dict],
        *,
        session_id: UUID,
        conn: asyncpg.Connection,
    ) -> int:
        inserted = 0
        for payload in candidates:
            candidate = _canonical_op_to_relationship_candidate(
                tenant_id=self._tenant_id,
                payload=payload,
                session_id=session_id,
            )
            if candidate is None:
                continue
            op_key = candidate.metadata.get("canonical_op_key")
            if not isinstance(op_key, str) or not op_key:
                continue
            try:
                existing = await conn.fetchval(
                    """
                    SELECT id
                    FROM relationship_candidates
                    WHERE tenant_id = $1
                      AND source = 'sage_topology_optimizer'
                      AND metadata->>'canonical_op_key' = $2
                      AND review_status = ANY($3::text[])
                    LIMIT 1
                    """,
                    self._tenant_id,
                    op_key,
                    ["candidate", "needs_review", "accepted"],
                )
                if existing is not None:
                    continue
                await self._relationship_candidates.insert(conn, candidate)
            except asyncpg.UniqueViolationError:
                # Another concurrent optimizer worker inserted the same
                # canonical_op_key. The unique index is the real invariant;
                # this worker simply reports that it did not enqueue a new row.
                continue
            except (
                asyncpg.UndefinedTableError,
                asyncpg.UndefinedColumnError,
            ) as exc:
                _log.debug(
                    "topology_optimizer.validation_queue_unavailable",
                    error=str(exc),
                )
                return inserted
            except Exception as exc:  # noqa: BLE001
                _log.warning(
                    "topology_optimizer.validation_enqueue_failed",
                    error=str(exc),
                    error_type=type(exc).__name__,
                    canonical_op_key=op_key,
                )
                continue
            inserted += 1
        return inserted

    # ------------------------------------------------------------------
    # Step 5 — canonical-op candidate proposals (NEVER applied).
    # ------------------------------------------------------------------

    def _propose_merges(
        self,
        *,
        useful_paths: list[OutcomeEventRow],
        session_id: UUID,
    ) -> list[dict]:
        """Pairs of models seen together in many useful paths.

        Heuristic stub: if a (src, dst) pair appears in
        `MERGE_SHARED_PATH_HITS` or more useful path events AND the
        payloads include a `proposition_similarity` field >= 0.8,
        propose a merge candidate. The candidate is just a structured
        dict; the validation pipeline does the real work later.
        """
        pair_hits: dict[tuple[UUID, UUID], list[float]] = defaultdict(list)
        for ev in useful_paths:
            src = _payload_uuid(
                ev.payload, "from_model_id", "source_model_id",
            )
            dst = _payload_uuid(
                ev.payload, "to_model_id", "target_model_id",
            )
            if src is None or dst is None or src == dst:
                continue
            pair = tuple(sorted([src, dst], key=str))
            similarity = ev.payload.get("proposition_similarity")
            try:
                sim_f = float(similarity) if similarity is not None else 0.0
            except (TypeError, ValueError):
                sim_f = 0.0
            pair_hits[pair].append(sim_f)  # type: ignore[index]

        out: list[dict] = []
        for pair, sims in pair_hits.items():
            if len(sims) < MERGE_SHARED_PATH_HITS:
                continue
            max_sim = max(sims)
            if max_sim < 0.8:
                continue
            out.append({
                "op": "merge",
                "source_model_ids": [str(pair[0]), str(pair[1])],
                "proposed_kind": "merge_duplicates",
                "reason": (
                    f"co-occurred in {len(sims)} useful paths with "
                    f"max proposition_similarity={max_sim:.2f}"
                ),
                "evidence_session_ids": [str(session_id)],
            })
        return out

    def _propose_splits(
        self,
        *,
        events: list[OutcomeEventRow],
        useful_paths: list[OutcomeEventRow],
        session_id: UUID,
    ) -> list[dict]:
        """Models with high prediction error AND many distinct useful neighbors."""
        # Collect prediction errors per model.
        pred_err: dict[UUID, float] = {}
        for ev in events:
            if ev.event_type not in (
                "model_later_falsified",
                "validation_failed_due_to_missing_evidence",
            ):
                continue
            mid = _payload_uuid(ev.payload, "model_id", "node_id")
            if mid is None:
                continue
            err = ev.payload.get("prediction_error")
            try:
                err_f = float(err) if err is not None else 1.0
            except (TypeError, ValueError):
                err_f = 1.0
            pred_err[mid] = max(pred_err.get(mid, 0.0), err_f)

        # Distinct neighbors per model across useful paths.
        neighbors: dict[UUID, set[UUID]] = defaultdict(set)
        for ev in useful_paths:
            src = _payload_uuid(
                ev.payload, "from_model_id", "source_model_id",
            )
            dst = _payload_uuid(
                ev.payload, "to_model_id", "target_model_id",
            )
            if src is not None and dst is not None and src != dst:
                neighbors[src].add(dst)
                neighbors[dst].add(src)

        out: list[dict] = []
        for mid, err in pred_err.items():
            if err < SPLIT_PRED_ERROR_THRESHOLD:
                continue
            if len(neighbors.get(mid, set())) < SPLIT_DISTINCT_NEIGHBORS:
                continue
            out.append({
                "op": "split",
                "source_model_id": str(mid),
                "proposed_kind": "split_overloaded",
                "reason": (
                    f"prediction_error={err:.2f} with "
                    f"{len(neighbors[mid])} distinct useful-path neighbors"
                ),
                "evidence_session_ids": [str(session_id)],
            })
        return out

    def _propose_promotes(
        self,
        *,
        events: list[OutcomeEventRow],
        useful_nodes: list[OutcomeEventRow],
        useful_paths: list[OutcomeEventRow],
        session_id: UUID,
    ) -> list[dict]:
        """Stable useful access patterns that may deserve canonical structure.

        Explicit `composition_pattern_id` payloads still win when present.
        In normal product flow, however, the writer emits useful node/path
        events without benchmark-specific composition labels. Those repeated
        signature -> useful target patterns are also promoted as validation
        candidates so the canonical layer can eventually absorb durable read
        structure instead of keeping it forever as sidecar routing metadata.
        """
        pattern_counts: Counter[str] = Counter()
        sample_payloads: dict[str, dict] = {}
        for ev in events:
            pid = ev.payload.get("composition_pattern_id")
            if not isinstance(pid, str) or not pid:
                continue
            count = ev.payload.get("recurrence_count")
            try:
                step = int(count) if count is not None else 1
            except (TypeError, ValueError):
                step = 1
            pattern_counts[pid] += max(1, step)
            sample_payloads.setdefault(pid, dict(ev.payload))

        out: list[dict] = []
        for pid, count in pattern_counts.items():
            if count < PROMOTE_RECURRENCE_THRESHOLD:
                continue
            payload = sample_payloads.get(pid, {})
            raw_members = payload.get("member_model_ids", [])
            member_model_ids = (
                list(raw_members)
                if isinstance(raw_members, (list, tuple))
                else []
            )
            out.append({
                "op": "promote",
                "source_model_ids": [
                    str(u) for u in member_model_ids[
                        :PROMOTE_SOURCE_MODEL_ID_LIMIT
                    ]
                    if u is not None
                ],
                "source_model_count": len(member_model_ids),
                "source_model_id_limit": PROMOTE_SOURCE_MODEL_ID_LIMIT,
                "proposed_kind": "promote_composition_to_model",
                "composition_pattern_id": pid,
                "reason": (
                    f"composition pattern {pid!r} observed {count} times "
                    f"(threshold {PROMOTE_RECURRENCE_THRESHOLD})"
                ),
                "evidence_session_ids": [str(session_id)],
            })

        access_counts: Counter[str] = Counter()
        access_members: dict[str, set[UUID]] = defaultdict(set)
        access_signature: dict[str, dict[str, Any]] = {}
        for ev in [*useful_nodes, *useful_paths]:
            signature = _signature_from_payload(ev.payload)
            if not signature:
                continue
            target_ids = [
                uid for uid in (
                    _payload_uuid(ev.payload, "model_id", "node_id"),
                    _payload_uuid(ev.payload, "to_model_id", "target_model_id"),
                    _payload_uuid(ev.payload, "from_model_id", "source_model_id"),
                )
                if uid is not None
            ]
            if not target_ids:
                continue
            key = json.dumps(
                {
                    "signal_type": signature.get("signal_type"),
                    "question_primitive": signature.get("question_primitive"),
                    "entities": sorted(
                        str(item)
                        for item in signature.get("entities", [])
                        if item is not None
                    )[:8],
                },
                sort_keys=True,
                default=str,
            )
            access_counts[key] += 1
            access_signature.setdefault(key, signature)
            access_members[key].update(target_ids)

        existing_pattern_ids = {
            item.get("composition_pattern_id")
            for item in out
            if item.get("composition_pattern_id")
        }
        for key, count in access_counts.items():
            if count < ACCESS_PROMOTE_MIN_EVENTS:
                continue
            members = sorted(access_members.get(key, set()), key=str)
            if len(members) < ACCESS_PROMOTE_MIN_MODELS:
                continue
            pattern_id = f"access:{key}"
            if pattern_id in existing_pattern_ids:
                continue
            signature = access_signature.get(key, {})
            out.append({
                "op": "promote",
                "source_model_ids": [
                    str(mid)
                    for mid in members[:PROMOTE_SOURCE_MODEL_ID_LIMIT]
                ],
                "source_model_count": len(members),
                "source_model_id_limit": PROMOTE_SOURCE_MODEL_ID_LIMIT,
                "proposed_kind": "promote_access_pattern_to_canonical_route",
                "composition_pattern_id": pattern_id,
                "signature": signature,
                "reason": (
                    f"signature produced {count} useful node/path events "
                    f"across {len(members)} models in this inquiry; "
                    "propose validation-gated canonical route/grouping"
                ),
                "evidence_session_ids": [str(session_id)],
            })
        return out

    def _propose_demotes(
        self,
        *,
        events: list[OutcomeEventRow],
        session_id: UUID,
    ) -> list[dict]:
        """Models with consistently low affordance utility and no recent reinforcement.

        Stub heuristic: a model_id appearing in
        `recommendation_ignored` payloads with an explicit
        `affordance_utility` floor below `DEMOTE_LOW_UTILITY` and
        `last_reinforced_at` older than `DEMOTE_QUIET_AFTER`.
        """
        now = datetime.now(timezone.utc)
        out: list[dict] = []
        seen: set[UUID] = set()
        for ev in events:
            if ev.event_type != "recommendation_ignored":
                continue
            mid = _payload_uuid(ev.payload, "model_id", "node_id")
            if mid is None or mid in seen:
                continue
            utility = ev.payload.get("affordance_utility")
            try:
                util_f = float(utility) if utility is not None else 1.0
            except (TypeError, ValueError):
                util_f = 1.0
            if util_f > DEMOTE_LOW_UTILITY:
                continue
            last_reinforced = ev.payload.get("last_reinforced_at")
            quiet_enough = True
            if isinstance(last_reinforced, str):
                try:
                    last_dt = datetime.fromisoformat(
                        last_reinforced.replace("Z", "+00:00"),
                    )
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    quiet_enough = (now - last_dt) >= DEMOTE_QUIET_AFTER
                except ValueError:
                    quiet_enough = True
            if not quiet_enough:
                continue
            seen.add(mid)
            out.append({
                "op": "demote",
                "source_model_id": str(mid),
                "proposed_kind": "demote_low_utility",
                "reason": (
                    f"affordance_utility={util_f:.2f} <= "
                    f"{DEMOTE_LOW_UTILITY} with no recent reinforcement"
                ),
                "evidence_session_ids": [str(session_id)],
            })
        return out


def _canonical_op_to_relationship_candidate(
    *,
    tenant_id: UUID,
    payload: dict,
    session_id: UUID,
) -> RelationshipCandidate | None:
    """Map safe multi-model topology proposals into the pre-truth layer.

    Single-model split/demote proposals need a different review table; the
    existing relationship candidate schema represents only edges and
    multi-model situations, so we persist merge/promote candidates here.
    """
    op = str(payload.get("op") or "")
    proposed_kind = str(payload.get("proposed_kind") or "")
    model_ids = _candidate_model_ids(payload)
    if op == "merge" and len(model_ids) >= 2:
        source_id, target_id = tuple(sorted(model_ids[:2], key=str))
        metadata = _canonical_candidate_metadata(
            payload,
            session_id=session_id,
            op_key=_canonical_op_key(payload),
        )
        scores = JudgmentScores(
            impact=0.55,
            urgency=0.35,
            actionability=0.60,
            uncertainty=0.30,
            authority_required=0.55,
            reversibility=0.45,
            novelty=0.35,
            confidence=0.78,
        )
        return make_edge_candidate(
            tenant_id=tenant_id,
            source_model_id=source_id,
            target_model_id=target_id,
            edge_kind="same_issue_as",
            basis="topology_suggested",
            explanation=str(
                payload.get("reason")
                or "topology optimizer proposed a duplicate/merge review"
            ),
            scores=scores,
            evidence_model_ids=(source_id, target_id),
            metadata=metadata,
            source="sage_topology_optimizer",
            review_status="needs_review",
        )

    if op == "promote" and len(model_ids) >= 2:
        members = tuple(sorted(dict.fromkeys(model_ids), key=str))
        observed_member_count = _payload_int(
            payload,
            "source_model_count",
            default=len(members),
        )
        metadata = _canonical_candidate_metadata(
            payload,
            session_id=session_id,
            op_key=_canonical_op_key(payload),
        )
        signature = payload.get("signature") if isinstance(payload, dict) else None
        signature_hint = ""
        if isinstance(signature, dict):
            primitive = signature.get("question_primitive")
            signal_type = signature.get("signal_type")
            if primitive or signal_type:
                signature_hint = f" ({signal_type or 'signal'} / {primitive or 'read'})"
        summary = (
            f"Stable useful access pattern{signature_hint} across "
            f"{observed_member_count} models."
        )
        reason = str(
            payload.get("reason")
            or "topology optimizer proposed a canonical route/grouping"
        )
        scores = JudgmentScores(
            impact=clamp_score(0.45 + min(len(members), 8) * 0.04),
            urgency=0.35,
            actionability=0.55,
            uncertainty=0.35,
            authority_required=0.45,
            reversibility=0.50,
            novelty=0.50,
            confidence=0.74,
        )
        return make_situation_candidate(
            tenant_id=tenant_id,
            situation=(
                proposed_kind
                or "promote_access_pattern_to_canonical_route"
            ),
            summary=summary,
            relationship_summary=reason,
            member_model_ids=members,
            basis="topology_suggested",
            scores=scores,
            metadata={
                **metadata,
                "pressure_type": "execution",
                "observed_member_count": observed_member_count,
            },
            source="sage_topology_optimizer",
            review_status="needs_review",
        )
    return None


def _candidate_model_ids(payload: dict) -> list[UUID]:
    raw: list[Any] = []
    source_ids = payload.get("source_model_ids")
    if isinstance(source_ids, list):
        raw.extend(source_ids)
    for key in ("source_model_id", "target_model_id", "model_id"):
        if payload.get(key) is not None:
            raw.append(payload.get(key))
    out: list[UUID] = []
    for item in raw:
        uid = _coerce_uuid(item)
        if uid is not None and uid not in out:
            out.append(uid)
    return out


def _canonical_candidate_metadata(
    payload: dict,
    *,
    session_id: UUID,
    op_key: str,
) -> dict[str, Any]:
    evidence_session_ids = [
        str(item)
        for item in payload.get("evidence_session_ids", [])
        if item is not None
    ] or [str(session_id)]
    return {
        "origin": "sage_topology_optimizer",
        "canonical_op": payload.get("op"),
        "canonical_proposed_kind": payload.get("proposed_kind"),
        "canonical_op_key": op_key,
        "source_model_count": payload.get("source_model_count"),
        "source_model_id_limit": payload.get("source_model_id_limit"),
        "canonical_candidate": payload,
        "evidence_session_ids": evidence_session_ids,
    }


def _payload_int(payload: dict, key: str, *, default: int) -> int:
    try:
        return int(payload.get(key, default))
    except (TypeError, ValueError):
        return default


def _canonical_op_key(payload: dict) -> str:
    return json.dumps(
        {
            "op": payload.get("op"),
            "proposed_kind": payload.get("proposed_kind"),
            "source_model_ids": sorted(
                str(mid) for mid in _candidate_model_ids(payload)
            ),
            "composition_pattern_id": payload.get("composition_pattern_id"),
            "signature": payload.get("signature"),
        },
        sort_keys=True,
        default=str,
    )


__all__ = [
    "DECAY_FACTOR",
    "NEGATIVE_MEMORY_TTL",
    "REINFORCE_DELTA",
    "SHORTCUT_POSITIVE_DELTA",
    "TopologyOptimizer",
    "enqueue_for_validation",
]
