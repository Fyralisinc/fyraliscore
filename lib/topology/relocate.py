"""
lib/topology/relocate.py — pure helpers for S4's `relocate` claim_op
and bounded topological cascade.

Two concerns, kept separate from `services/topology/topo_repo.py`:

  1. **Target resolution** — given a `RelocateTarget` (one of three
     shapes: another model_id, an explicit 128-d vector, or a
     neighborhood_id), produce the 128-d topo vector to relocate
     toward. Pure once the target's source data is in hand; the repo
     fetches the data and hands it in.

  2. **Bounded cascade selection** — given a Model's neighbors and
     their centralities, select the top-K neighbors (by centrality
     DESC) to enqueue at hop_depth + 1. Caps fan-out so a single
     relocate doesn't tsunami-propagate through the whole substrate.
     Reused by future producers (e.g. `contradicts` polarity gate)
     that want bounded propagation.

Constants
---------

  * RELOCATE_DEFAULT_ALPHA = 1.0        (full snap to target)
  * RELOCATE_CASCADE_MAX_DEPTH = 2      (relocate's blast radius)
  * RELOCATE_CASCADE_MAX_FANOUT = 20    (top-K neighbors per hop)
  * RELOCATE_CASCADE_DAMPING = 0.5      (γ — same as topology_updater)

These are env-tunable in production via TOPO_RELOCATE_* names.
"""
from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from typing import Literal, Sequence
from uuid import UUID

from lib.shared.errors import ValidationError
from lib.shared.types import TOPO_EMBEDDING_DIM


_TRUE_STRINGS = {"true", "1", "yes", "on", "y", "t"}


def _env_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        return default


RELOCATE_DEFAULT_ALPHA = _env_float("TOPO_RELOCATE_DEFAULT_ALPHA", 1.0)
RELOCATE_CASCADE_MAX_DEPTH = _env_int("TOPO_RELOCATE_CASCADE_MAX_DEPTH", 2)
RELOCATE_CASCADE_MAX_FANOUT = _env_int("TOPO_RELOCATE_CASCADE_MAX_FANOUT", 20)
RELOCATE_CASCADE_DAMPING = _env_float("TOPO_RELOCATE_CASCADE_DAMPING", 0.5)


RelocateTargetKind = Literal["model_id", "vector", "neighborhood_id"]


@dataclass(frozen=True)
class RelocateTarget:
    """Parsed claim_op.relocate_target. Kept frozen because the
    applier holds it across the bounded-cascade enqueue, and we want
    to avoid mutation surprises."""
    kind: RelocateTargetKind
    value: object  # UUID | list[float] | UUID — depending on kind
    alpha: float = RELOCATE_DEFAULT_ALPHA


def parse_relocate_target(raw: dict) -> RelocateTarget:
    """Validate + parse a claim_op `relocate_target` dict into a
    RelocateTarget. Raises ValidationError on shape mismatch.

    Accepted shapes:
      {"kind": "model_id", "value": "<uuid>", "alpha": <float?>}
      {"kind": "vector",   "value": [<128 floats>], "alpha": <float?>}
      {"kind": "neighborhood_id", "value": "<uuid>", "alpha": <float?>}

    `alpha` is clamped to (0, 1]. 0 is rejected (snap to current = no-op
    isn't a relocate; the LLM should just not emit one).
    """
    if not isinstance(raw, dict):
        raise ValidationError(
            "relocate_target must be a dict",
            got_type=type(raw).__name__,
        )
    kind = raw.get("kind")
    if kind not in ("model_id", "vector", "neighborhood_id"):
        raise ValidationError(
            f"relocate_target.kind must be one of model_id|vector|"
            f"neighborhood_id; got {kind!r}",
        )
    value = raw.get("value")
    if value is None:
        raise ValidationError("relocate_target.value is required")

    parsed_value: object
    if kind == "vector":
        if not isinstance(value, (list, tuple)):
            raise ValidationError(
                "relocate_target.value (vector) must be a list of floats",
            )
        if len(value) != TOPO_EMBEDDING_DIM:
            raise ValidationError(
                f"relocate_target.value (vector) dim "
                f"{len(value)} != {TOPO_EMBEDDING_DIM}",
            )
        try:
            floats = [float(x) for x in value]
        except (TypeError, ValueError) as e:
            raise ValidationError(
                f"relocate_target.value (vector) has non-float entries: {e}",
            ) from e
        # Reject NaN / Inf early — pgvector rejects them on INSERT
        # with an opaque error, so we surface a precise one here.
        for i, x in enumerate(floats):
            if x != x or x == float("inf") or x == float("-inf"):
                raise ValidationError(
                    f"relocate_target.value (vector) has non-finite "
                    f"entry at index {i}: {x}",
                )
        parsed_value = floats
    else:
        # model_id / neighborhood_id
        if isinstance(value, UUID):
            parsed_value = value
        else:
            try:
                parsed_value = UUID(str(value))
            except (ValueError, TypeError) as e:
                raise ValidationError(
                    f"relocate_target.value ({kind}) must be a UUID; got {value!r}",
                ) from e

    alpha_raw = raw.get("alpha", RELOCATE_DEFAULT_ALPHA)
    try:
        alpha = float(alpha_raw)
    except (TypeError, ValueError):
        raise ValidationError(
            f"relocate_target.alpha must be a float; got {alpha_raw!r}",
        )
    if alpha <= 0.0 or alpha > 1.0:
        raise ValidationError(
            f"relocate_target.alpha must be in (0, 1]; got {alpha}",
        )

    return RelocateTarget(kind=kind, value=parsed_value, alpha=alpha)


def blend_topo(
    current: Sequence[float],
    target: Sequence[float],
    alpha: float,
) -> list[float]:
    """Blend current topo toward target by alpha.

      result = (1 - alpha) · current + alpha · target

    L2-normalized. alpha=1.0 → snap to target. alpha=0.5 → halfway.

    Both inputs must be TOPO_EMBEDDING_DIM. Raises on dim mismatch.
    """
    if len(current) != TOPO_EMBEDDING_DIM:
        raise ValidationError(
            f"blend_topo current dim {len(current)} != {TOPO_EMBEDDING_DIM}",
        )
    if len(target) != TOPO_EMBEDDING_DIM:
        raise ValidationError(
            f"blend_topo target dim {len(target)} != {TOPO_EMBEDDING_DIM}",
        )
    if not (0.0 < alpha <= 1.0):
        raise ValidationError(f"blend_topo alpha out of range: {alpha}")
    out = [0.0] * TOPO_EMBEDDING_DIM
    one_minus = 1.0 - alpha
    for j in range(TOPO_EMBEDDING_DIM):
        out[j] = one_minus * float(current[j]) + alpha * float(target[j])
    norm = math.sqrt(sum(x * x for x in out))
    if norm > 0:
        out = [x / norm for x in out]
    return out


@dataclass(frozen=True)
class CascadeTarget:
    """One neighbor selected by the bounded cascade selector."""
    model_id: UUID
    centrality: float
    hop_depth: int


def select_bounded_neighbors(
    candidates: Sequence[tuple[UUID, float | None]],
    *,
    next_hop_depth: int,
    max_fanout: int = RELOCATE_CASCADE_MAX_FANOUT,
) -> list[CascadeTarget]:
    """Select up to max_fanout neighbors by centrality DESC.

    Each candidate is `(neighbor_model_id, centrality_or_None)`.
    None centralities are sorted last (treated as zero). Stable on
    UUID for deterministic ordering when centralities tie.

    Empty / max_fanout=0 → empty list.
    """
    if max_fanout <= 0 or not candidates:
        return []
    ordered = sorted(
        candidates,
        key=lambda kv: (-(float(kv[1]) if kv[1] is not None else 0.0), str(kv[0])),
    )
    chosen = ordered[:max_fanout]
    return [
        CascadeTarget(
            model_id=mid,
            centrality=(float(cent) if cent is not None else 0.0),
            hop_depth=next_hop_depth,
        )
        for mid, cent in chosen
    ]


def damped_magnitude(
    base_delta: float,
    *,
    hop_depth: int,
    gamma: float = RELOCATE_CASCADE_DAMPING,
) -> float:
    """Apply geometric damping for cascade priority.

      damped = base_delta · γ^hop_depth

    γ ∈ (0, 1) shrinks the magnitude per hop. With default γ=0.5,
    a hop_depth=1 cascade gets half priority; depth=2 gets a quarter.
    """
    if hop_depth < 0:
        raise ValidationError(f"hop_depth must be ≥ 0; got {hop_depth}")
    if not (0.0 < gamma <= 1.0):
        raise ValidationError(f"gamma must be in (0, 1]; got {gamma}")
    return float(base_delta) * (gamma ** hop_depth)


__all__ = [
    "RelocateTarget",
    "RelocateTargetKind",
    "RELOCATE_DEFAULT_ALPHA",
    "RELOCATE_CASCADE_MAX_DEPTH",
    "RELOCATE_CASCADE_MAX_FANOUT",
    "RELOCATE_CASCADE_DAMPING",
    "parse_relocate_target",
    "blend_topo",
    "CascadeTarget",
    "select_bounded_neighbors",
    "damped_magnitude",
]
