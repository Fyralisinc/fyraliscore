"""
services/retrieval/assembler.py — context assembler + access control
stub.

Spec reference: ARCHITECTURE-FINAL.md §8 "Context assembler" + §26
"Access control". BUILD-PLAN reference: §4 Prompt 3.A item 4.
RA-4 reference: RETRIEVAL-DESIGN-AUDIT §7 args 1-2 (MMR diversity +
don't-truncate-mid-item).

Converts a RetrievalResult into a ContextBundle:
  - Applies access control (Wave 3-A stub: visibility check on
    `visible_to_subjects` + `scope_actors` membership; real roles /
    materialized views are Wave 5-A).
  - Compresses to configured size budgets:
        * observations  ≤ 20
        * models        ≤ 40
        * acts          ≤ 10 (across goals + commitments + decisions
                             combined, deviation (c) documented below)
        * resources     ≤ 5
  - Attaches a bridge_context dict if any commitment has
    `external_counterparty_ref` set.

Compression ordering (deviation (c) BUILD-LOG):
  - Models — by `model_scores` descending (from primary_retrieve).
    Tie-break on activation descending.
  - Observations — occurred_at descending.
  - Acts — we flatten the three kinds into one list and take the top
    10 by last_state_change_at / created_at descending. The cap of 10
    is per BUILD-PLAN (not 10 per kind).
  - Resources — prefer those with an active customer_commitments
    linkage (hit the Bridge spine first); then by last_updated_at
    descending.

MMR diversity (RA-4): `mmr_select(items_with_scores, budget_tokens,
lambda_diversity)` is a public helper that combines an item's
relevance score with a diversity penalty (1 - max cosine similarity
to already-selected). Items that don't fit the remaining budget are
SKIPPED — we never truncate mid-item (audit §7 arg 2).
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable, Protocol, Sequence
from uuid import UUID

import asyncpg

from lib.shared.errors import CompanyOSError
from lib.shared.types import (
    CommitmentRow,
    ModelRow,
    ObservationRow,
    ResourceRow,
)

from .config import CONFIG, RetrievalConfig
from .primary import RetrievalResult


_BUDGET_OBSERVATIONS = 20
_BUDGET_MODELS = 40
_BUDGET_ACTS_TOTAL = 10   # combined across goals + commitments + decisions
_BUDGET_RESOURCES = 5

# RA-4 default MMR diversity trade-off. 0.5 balances relevance and
# diversity; 1.0 reduces to pure greedy-by-score; 0.0 prefers pure
# novelty at the expense of relevance.
_MMR_LAMBDA_DEFAULT = 0.5

# Cheap-and-correct token estimator used for the MMR path (FU-1). Real
# tokenization would cost a per-row tokenizer call; 4 chars/token is
# the industry rule-of-thumb and is well within the accuracy needed
# for a context-budget bound. Minimum 1 token so items with no
# `natural` text never get skipped as "too big".
_CHARS_PER_TOKEN = 4


def _estimate_model_tokens(m: ModelRow) -> int:
    """Rough token estimate for a Model row. Uses the `natural` text
    (the LLM-facing narrative) as the token cost proxy. Callers pack
    these estimates into `mmr_select` to stay under
    `context_budget_tokens`."""
    nat = getattr(m, "natural", None) or ""
    # Fall back to proposition if natural is empty.
    if not nat:
        nat = getattr(m, "proposition", None) or ""
    return max(1, len(str(nat)) // _CHARS_PER_TOKEN)


@dataclass
class _MMRModelWrapper:
    """Adapter: `ModelRow` lacks `score`/`tokens`; MMR needs both.
    Wraps the row alongside its retrieval score + token estimate +
    embedding (already on the row)."""
    model: ModelRow
    score: float
    tokens: int
    embedding: Any  # list[float] | None
    # Expose id for tests / debugging.
    @property
    def id(self) -> UUID:
        return self.model.id


class AssemblerError(CompanyOSError):
    default_code = "assembler_error"


# ---------------------------------------------------------------------
# RA-4 — MMR (Maximal Marginal Relevance) selection
# ---------------------------------------------------------------------


class _HasScoreTokensEmbedding(Protocol):
    score: float
    tokens: int
    embedding: Sequence[float] | None


def _cosine_similarity(a: Sequence[float] | None, b: Sequence[float] | None) -> float:
    """Cosine similarity in [-1, 1]. Returns 0 when either vector is
    missing or zero-norm. Safe for short inputs (defensive numerics
    for unit tests with tiny dims)."""
    if a is None or b is None:
        return 0.0
    na = 0.0
    nb = 0.0
    dot = 0.0
    for x, y in zip(a, b):
        dot += float(x) * float(y)
        na += float(x) * float(x)
        nb += float(y) * float(y)
    if na == 0.0 or nb == 0.0:
        return 0.0
    denom = math.sqrt(na) * math.sqrt(nb)
    if denom == 0.0:
        return 0.0
    return dot / denom


def mmr_select(
    items_with_scores: Iterable[Any],
    budget_tokens: int,
    *,
    lambda_diversity: float = _MMR_LAMBDA_DEFAULT,
) -> list[Any]:
    """
    Maximal Marginal Relevance selection under a token budget.

    Each item must expose:
      - `score` (float): relevance score
      - `tokens` (int): the item's size in tokens
      - `embedding` (sequence[float] | None): used for the diversity
        penalty (optional; missing embeddings treated as "dissimilar
        to everything" → diversity penalty contributes 0)

    Procedure:
      1. Sort items by relevance DESC.
      2. Pick the highest-scoring item that fits remaining budget.
      3. For each remaining item compute
             mmr = λ·score - (1-λ)·max_sim_to_selected
         Pick the best-mmr item that fits budget.
      4. Continue until no remaining item fits or all consumed.

    Items that don't fit remaining budget are SKIPPED (never
    truncated mid-item, per RETRIEVAL-DESIGN-AUDIT §7 arg 2).

    Edge cases:
      - λ=1.0 reduces to pure greedy-by-score.
      - λ=0.0 maximizes diversity (ignores relevance after the first
        pick).
      - Non-positive budget returns []; any item with tokens==0 is
        skipped (ambiguous — would fit an empty budget infinitely
        many times).

    Performance: pre-normalizes the embedding matrix once, then
    incrementally tracks per-remaining-item `max_sim_to_selected` so
    each pick costs O(n_remaining) rather than O(n_remaining *
    n_selected). 100 items / 100K budget completes in ~1-2ms.
    """
    if budget_tokens <= 0:
        return []
    if not (0.0 <= lambda_diversity <= 1.0):
        raise ValueError(
            f"lambda_diversity must be in [0, 1]; got {lambda_diversity}"
        )

    # Materialize and sort by score desc.
    items = sorted(
        items_with_scores,
        key=lambda it: -(float(getattr(it, "score", 0.0) or 0.0)),
    )
    n = len(items)
    if n == 0:
        return []

    # Pre-extract per-item arrays. Use numpy where possible.
    try:
        import numpy as _np  # local import — keeps assembler import
                              # cheap when MMR isn't called.
    except ImportError:
        _np = None

    scores = [float(getattr(it, "score", 0.0) or 0.0) for it in items]
    tokens_arr = [int(getattr(it, "tokens", 0) or 0) for it in items]

    # Build embedding matrix; rows of zeros mark "missing embedding".
    embeddings_raw = [getattr(it, "embedding", None) for it in items]
    has_emb = [emb is not None for emb in embeddings_raw]
    emb_matrix = None
    if _np is not None and any(has_emb):
        # Determine common dim from first non-None embedding.
        dim = next((len(list(e)) for e in embeddings_raw if e is not None), 0)
        if dim > 0:
            emb_matrix = _np.zeros((n, dim), dtype=_np.float32)
            for i, e in enumerate(embeddings_raw):
                if e is None:
                    continue
                ev = list(e)
                if len(ev) != dim:
                    # Mismatched dim — treat as missing.
                    has_emb[i] = False
                    continue
                emb_matrix[i, :] = _np.asarray(ev, dtype=_np.float32)
            # Normalize rows (safe handling of zero rows).
            norms = _np.linalg.norm(emb_matrix, axis=1, keepdims=True)
            norms[norms == 0.0] = 1.0
            emb_matrix = emb_matrix / norms

    # Track which indices remain.
    remaining_mask = [True] * n
    max_sim = [0.0] * n  # current max sim to any selected item
    used_tokens = 0
    selected_indices: list[int] = []

    def _fits(idx: int) -> bool:
        tk = tokens_arr[idx]
        return tk > 0 and used_tokens + tk <= budget_tokens

    # First pick: highest-score feasible item.
    first = None
    for i in range(n):
        if remaining_mask[i] and _fits(i):
            first = i
            break
    if first is None:
        return []
    selected_indices.append(first)
    remaining_mask[first] = False
    used_tokens += tokens_arr[first]

    # Update max_sim using the chosen one.
    if emb_matrix is not None and has_emb[first]:
        sims = emb_matrix @ emb_matrix[first]
        for i in range(n):
            if not remaining_mask[i] or not has_emb[i]:
                continue
            s = float(sims[i])
            if s > max_sim[i]:
                max_sim[i] = s

    # Subsequent picks.
    while True:
        best_idx = -1
        best_mmr = -float("inf")
        for i in range(n):
            if not remaining_mask[i]:
                continue
            if not _fits(i):
                continue
            mmr = lambda_diversity * scores[i] - (1.0 - lambda_diversity) * max_sim[i]
            if mmr > best_mmr:
                best_mmr = mmr
                best_idx = i
        if best_idx < 0:
            break
        selected_indices.append(best_idx)
        remaining_mask[best_idx] = False
        used_tokens += tokens_arr[best_idx]
        # Incrementally fold the new pick into max_sim.
        if emb_matrix is not None and has_emb[best_idx]:
            sims = emb_matrix @ emb_matrix[best_idx]
            for i in range(n):
                if not remaining_mask[i] or not has_emb[i]:
                    continue
                s = float(sims[i])
                if s > max_sim[i]:
                    max_sim[i] = s

    return [items[i] for i in selected_indices]


@dataclass
class AccessContext:
    """
    Thin access-control context. Wave 3-A stub — only `tenant_id` +
    `requestor_actor_id` + `roles` are used. Real role-based
    enforcement / materialized visibility views arrive in Wave 5-A.
    """

    tenant_id: UUID
    requestor_actor_id: UUID | None = None
    roles: list[str] = field(default_factory=list)


@dataclass
class ContextBundle:
    """
    The caller-facing return. Size bounds are hard caps (not target
    budgets); items over the cap are dropped (ordered by score).

    `bridge_context` is a dict of customer_resource_id → Bridge summary
    OR None when no commitment in `acts_summary` has an
    `external_counterparty_ref`.

    `access_redactions` is the count of Models filtered out for
    visibility; the caller uses this for observability.
    """

    observations: list[ObservationRow] = field(default_factory=list)
    models: list[ModelRow] = field(default_factory=list)
    acts_summary: dict[str, list] = field(
        default_factory=lambda: {"goals": [], "commitments": [], "decisions": []}
    )
    resources_summary: list[ResourceRow] = field(default_factory=list)
    bridge_context: dict[str, Any] | None = None
    access_redactions: int = 0
    notes: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------
# Access control — Wave 5-A full impl (replaces Wave 3-A stub)
# ---------------------------------------------------------------------


def _model_is_visible_fast(
    model: ModelRow,
    access: AccessContext,
) -> bool:
    """Fast in-memory pre-check for Models the caller loaded.

    Matches the cheap clauses in `services.access_control.checks`:
      - system identity (requestor None) bypasses all checks.
      - visible_to_subjects=True → visible.
      - scope_actors membership → visible.

    Additional clauses (pattern-scope via actor_visible_{commitments,
    goals}, admin/leadership override) require a DB round-trip and
    live in `_filter_models_via_db` below. The two functions together
    implement the full §26 Layer-5 rule-set; `_filter_models_via_db`
    is the authoritative one when there are any models that fail the
    fast path.
    """
    if access.requestor_actor_id is None:
        return True
    if model.visible_to_subjects:
        return True
    return access.requestor_actor_id in model.scope_actors


async def _filter_models_via_db(
    models: list[ModelRow],
    access: AccessContext,
    conn: asyncpg.Connection,
) -> tuple[list[ModelRow], int, dict[str, int]]:
    """Run the full Wave-5-A can_read check against each Model that
    failed the fast in-memory pre-check.

    Returns (visible, redacted_count, reason_counts). `reason_counts`
    groups denial reasons for observability (BUILD-PLAN §6 "Count
    redactions per filter kind").
    """
    from services.access_control.checks import can_read  # local import

    if access.requestor_actor_id is None:
        # System identity sees everything in the tenant.
        return models, 0, {}
    visible: list[ModelRow] = []
    redactions = 0
    reasons: dict[str, int] = {}
    for m in models:
        if _model_is_visible_fast(m, access):
            visible.append(m)
            continue
        # Fast path said no — consult full check (handles pattern-scope
        # and admin/leadership overrides).
        entity = {
            "kind": "model",
            "id": m.id,
            "tenant_id": m.tenant_id,
            "visible_to_subjects": m.visible_to_subjects,
            "scope_actors": list(m.scope_actors),
            "scope_entities": m.scope_entities,
        }
        decision = await can_read(
            access.requestor_actor_id,
            entity,
            conn=conn,
            tenant_id=access.tenant_id,
        )
        if decision.allowed:
            visible.append(m)
        else:
            redactions += 1
            reasons[decision.reason] = reasons.get(decision.reason, 0) + 1
    return visible, redactions, reasons


# ---------------------------------------------------------------------
# Bridge traversal
# ---------------------------------------------------------------------


async def _compute_bridge_context(
    conn: asyncpg.Connection,
    tenant_id: UUID,
    commitments: list[CommitmentRow],
) -> dict[str, Any] | None:
    """
    For each Commitment with `external_counterparty_ref` set, compute
    the Bridge summary: revenue_at_risk for that customer + list of
    at-risk commitment ids.

    Returns None if no commitment has a counterparty ref.
    """
    customer_commits: dict[UUID, list[UUID]] = {}
    # The ref might point at a customer via JSONB shape
    # `{"type": "customer_resource", "id": "<uuid>"}` OR directly hold
    # the customer id. We prefer the canonical shape. We also look up
    # customer_commitments rows directly as a cross-check.
    for c in commitments:
        if c.external_counterparty_ref is None:
            continue
        ref = c.external_counterparty_ref
        if isinstance(ref, dict):
            # Canonical: {"type": "customer_resource", "id": "<uuid>"}
            if ref.get("type") in ("customer_resource", "customer"):
                raw_id = ref.get("id")
                if raw_id is not None:
                    try:
                        customer_id = UUID(str(raw_id))
                        customer_commits.setdefault(customer_id, []).append(c.id)
                    except (ValueError, TypeError):
                        pass

    # Cross-check via the customer_commitments table; add discovered
    # Customer Resources that the canonical ref might have missed.
    commit_ids = [c.id for c in commitments]
    if commit_ids:
        rows = await conn.fetch(
            """
            SELECT customer_resource_id, commitment_id
            FROM customer_commitments
            WHERE commitment_id = ANY($1::uuid[])
            """,
            commit_ids,
        )
        for r in rows:
            cust = r["customer_resource_id"]
            cid = r["commitment_id"]
            customer_commits.setdefault(cust, []).append(cid)

    if not customer_commits:
        return None

    # For each customer, compute revenue_at_risk. Import the bridge
    # primitive lazily to avoid circular deps.
    from services.resources.bridge import (
        AT_RISK_COMMITMENT_STATES,
        revenue_at_risk_for_customer,
    )

    summaries: list[dict[str, Any]] = []
    for customer_id, cids in customer_commits.items():
        try:
            rar = await revenue_at_risk_for_customer(customer_id, conn=conn)
        except Exception:
            rar = None
        # At-risk commitments among those linked to the customer.
        at_risk: list[UUID] = []
        for c in commitments:
            if c.id in cids and c.state in AT_RISK_COMMITMENT_STATES:
                at_risk.append(c.id)
        summaries.append(
            {
                "customer_resource_id": customer_id,
                "revenue_at_risk": str(rar) if rar is not None else None,
                "linked_commitment_ids": [str(x) for x in cids],
                "at_risk_commitment_ids": [str(x) for x in at_risk],
            }
        )
    return {"customers": summaries}


# ---------------------------------------------------------------------
# assemble_context — public entry point
# ---------------------------------------------------------------------


async def assemble_context(
    retrieval_result: RetrievalResult,
    access_context: AccessContext,
    conn: asyncpg.Connection,
    *,
    budget_observations: int = _BUDGET_OBSERVATIONS,
    budget_models: int = _BUDGET_MODELS,
    budget_acts: int = _BUDGET_ACTS_TOTAL,
    budget_resources: int = _BUDGET_RESOURCES,
    config: RetrievalConfig | None = None,
) -> ContextBundle:
    """
    Compose a size-bounded ContextBundle from the retrieval result.

    Follow-up FU-1 (RA-4 wire): when `config.assembler_use_mmr=True`
    (env `RETRIEVAL_ASSEMBLER_USE_MMR=1`), the Models bucket is
    selected via `mmr_select` under `config.context_budget_tokens`
    with `config.mmr_lambda_diversity`. Retrieval scores drive the
    relevance term; ModelRow embeddings drive the diversity term. Items
    that don't fit the token budget are skipped (never truncated).
    Default False — count-cap path is unchanged.
    """
    cfg = config or CONFIG
    # --- Access control on Models ---
    # Tenant-scoped first pass (belt + braces — primary_retrieve
    # already did this, but enforce at the edge). Then delegate the
    # full §26 rule-set to the Wave-5-A filter.
    tenant_scoped: list[ModelRow] = []
    cross_tenant_redactions = 0
    for m in retrieval_result.models:
        if m.tenant_id != access_context.tenant_id:
            cross_tenant_redactions += 1
            continue
        tenant_scoped.append(m)

    visible_models, redactions_inner, reason_counts = await _filter_models_via_db(
        tenant_scoped, access_context, conn,
    )
    redactions = cross_tenant_redactions + redactions_inner

    # --- Rank Models by score (already sorted in retrieval_result.models
    #     but we re-sort in case caller did second_pass which appends) ---
    scores = retrieval_result.model_scores
    visible_models.sort(
        key=lambda m: (
            -scores.get(m.id, 0.0),
            -m.activation,
            str(m.id),
        )
    )

    mmr_notes: dict[str, Any] = {"used": False}
    if cfg.assembler_use_mmr and visible_models:
        # Token-budgeted MMR path (FU-1). Keeps the count cap as a hard
        # upper bound even when the token budget would otherwise let
        # more items through — callers care about both budgets.
        wrappers = [
            _MMRModelWrapper(
                model=m,
                score=float(scores.get(m.id, 0.0)),
                tokens=_estimate_model_tokens(m),
                embedding=(list(m.embedding) if m.embedding is not None else None),
            )
            for m in visible_models
        ]
        mmr_selected = mmr_select(
            wrappers,
            budget_tokens=int(cfg.context_budget_tokens),
            lambda_diversity=float(cfg.mmr_lambda_diversity),
        )
        models_cap = [w.model for w in mmr_selected][:budget_models]
        mmr_notes = {
            "used": True,
            "lambda_diversity": float(cfg.mmr_lambda_diversity),
            "budget_tokens": int(cfg.context_budget_tokens),
            "selected_count": len(models_cap),
            "candidate_count": len(visible_models),
        }
    else:
        models_cap = visible_models[:budget_models]

    # --- Observations: tenant filter + occurred_at DESC cap ---
    obs_tenant = [
        o for o in retrieval_result.observations
        if o.tenant_id == access_context.tenant_id
    ]
    obs_tenant.sort(key=lambda o: (o.occurred_at, o.id), reverse=True)
    observations_cap = obs_tenant[:budget_observations]

    # --- Acts: combined cap of `budget_acts` across all three kinds ---
    # Build a unified (kind, row, timestamp) list. Sort by timestamp
    # DESC and then take the top N, regrouping by kind for the final
    # dict. This honors the "10 across all three" language.
    flat_acts: list[tuple[str, Any, Any]] = []
    for g in retrieval_result.acts.get("goals", []):
        if g.tenant_id != access_context.tenant_id:
            continue
        flat_acts.append(("goals", g, g.last_state_change_at or g.created_at))
    for c in retrieval_result.acts.get("commitments", []):
        if c.tenant_id != access_context.tenant_id:
            continue
        flat_acts.append(("commitments", c, c.last_state_change_at or c.created_at))
    for d in retrieval_result.acts.get("decisions", []):
        if d.tenant_id != access_context.tenant_id:
            continue
        flat_acts.append(("decisions", d, d.last_state_change_at or d.created_at))
    flat_acts.sort(key=lambda x: x[2], reverse=True)
    flat_acts = flat_acts[:budget_acts]
    acts_cap: dict[str, list] = {"goals": [], "commitments": [], "decisions": []}
    for kind, row, _ts in flat_acts:
        acts_cap[kind].append(row)

    # --- Resources: prefer those with customer_commitments linkage ---
    res_tenant = [
        r for r in retrieval_result.resources
        if r.tenant_id == access_context.tenant_id
    ]
    if res_tenant:
        # Check linkage in one query.
        rids = [r.id for r in res_tenant]
        linked = set()
        if rids:
            link_rows = await conn.fetch(
                """
                SELECT DISTINCT customer_resource_id
                FROM customer_commitments
                WHERE customer_resource_id = ANY($1::uuid[])
                """,
                rids,
            )
            linked = {r["customer_resource_id"] for r in link_rows}
        res_tenant.sort(
            key=lambda r: (
                0 if r.id in linked else 1,
                -(r.last_updated_at.timestamp() if r.last_updated_at else 0),
            )
        )
    resources_cap = res_tenant[:budget_resources]

    # --- Bridge context ---
    bridge_context = await _compute_bridge_context(
        conn,
        access_context.tenant_id,
        acts_cap["commitments"],
    )

    notes: dict[str, Any] = {
        "budgets": {
            "observations": budget_observations,
            "models": budget_models,
            "acts_total": budget_acts,
            "resources": budget_resources,
        },
        "budget_overflow": {
            "observations": len(retrieval_result.observations) - len(observations_cap),
            "models": len(retrieval_result.models) - len(models_cap),
            "acts": sum(len(v) for v in retrieval_result.acts.values())
            - sum(len(v) for v in acts_cap.values()),
            "resources": len(retrieval_result.resources) - len(resources_cap),
        },
        "access_redactions": redactions,
        "access_redaction_reasons": reason_counts,
        "access_redactions_cross_tenant": cross_tenant_redactions,
        "retrieval_trigger_kind": retrieval_result.trigger.kind,
        "mmr": mmr_notes,
    }

    return ContextBundle(
        observations=observations_cap,
        models=models_cap,
        acts_summary=acts_cap,
        resources_summary=resources_cap,
        bridge_context=bridge_context,
        access_redactions=redactions,
        notes=notes,
    )


__all__ = [
    "AccessContext",
    "ContextBundle",
    "AssemblerError",
    "assemble_context",
    "mmr_select",
]
