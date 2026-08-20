"""
services/reasoning/topology/umap_projector.py — per-tenant 2D UMAP projection of
the 128-d positional embeddings (`models.topo_embedding`).

Powers the CEO Map view (services/app/gateway/map_routes.py).

Why UMAP and not PCA
--------------------

PCA captures linear variance only. For a substrate with 128-d topo
embeddings shaped by neighborhood structure (the alpha-anchored
update rule from S2), PCA typically explains <30% of the variance in
2D — meaning ~70% of the topological information is dropped on the
floor. The watercolor-wash rendering that depended on PCA positions
was implying "near = semantically related" while the projection
itself was lying about which things were near.

UMAP preserves *local* structure (k-nearest neighbors stay neighbors
in the projection) at the cost of warping global distances. For the
CEO Map view that's exactly the right trade: the question users ask
is "what's near this belief?" — a local question. Cluster boundaries
in 2D become real visual claims again, not coincidences of force-
directed physics.

To stay honest about the lossiness we also compute the **trustworthiness
score** (sklearn.manifold.trustworthiness) — a 0-1 measure of how
many of each point's k-nearest neighbors in 128-d remain k-nearest
neighbors in 2D. The frontend surfaces this as a small badge so the
user sees the lie size.

Caching strategy
----------------

UMAP doesn't have a clean closed-form `transform` (the .transform()
method exists but is expensive and the new-point quality is shaky),
so we cache the **projected coordinates** directly rather than the
fitted model. Every refit re-projects every active model at once —
which is exactly when refits happen anyway (a new topology_event).

Cache lives in `view_ceo_cache` under
`cache_key='map_umap_projection_v1'`:

  {
    "coords": {"<model_id>": [x, y]},   # already normalised to [-1, 1]²
    "fitted_at": ISO-8601,
    "model_count": int,
    "trustworthiness": float,           # 0..1, higher = better
    "n_neighbors": int,                 # UMAP n_neighbors param
    "min_dist": float,                  # UMAP min_dist param
  }

Refit triggers (same as PCA was):
  (a) cache row missing,
  (b) any topology_event has occurred since `fitted_at`,
  (c) explicit refresh via POST /api/map/refresh_projection.

Public API mirrors the previous PCAProjector for drop-in replacement
in services/app/gateway/map_routes.py:

  UMAPProjector(pool: asyncpg.Pool)

    .project(tenant_id) -> dict[str, tuple[float, float]]
        Returns {model_id_str: (x, y)} for every active model with a
        non-null `topo_embedding`. Coords are normalised to [-1, 1]².
        Empty dict for tenants with fewer than MIN_MODELS_FOR_UMAP
        models — renderer falls back to force-directed.

    .refresh(tenant_id) -> dict
        Force a refit. Returns the cache payload (suitable for the
        /api/map/refresh_projection response).

    .read_cache_meta(tenant_id) -> dict | None
        Read just the metadata fields (fitted_at, model_count,
        trustworthiness, ...) without computing anything.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

import asyncpg


# Minimum number of models with a topo_embedding required to fit a
# 2-component UMAP. UMAP wants n_neighbors+1 points minimum; we
# default n_neighbors=5 so 6 is the floor with margin.
MIN_MODELS_FOR_UMAP = 6

# Cache key in `view_ceo_cache`. Note `_v1` suffix is reserved for
# future schema bumps (e.g. if we add the embedding hash to the cache
# for cheap diff detection).
CACHE_KEY = "map_umap_projection_v1"

# UMAP defaults tuned for substrate data:
#   n_neighbors=15 — picks up cluster structure without over-merging
#   min_dist=0.15  — moderately tight clusters; visually interpretable
DEFAULT_N_NEIGHBORS = 15
DEFAULT_MIN_DIST = 0.15
# Trustworthiness score's k. Should be smaller than n_neighbors used
# during fitting so we measure local-structure preservation, not the
# fit's own neighborhood definition.
TRUSTWORTHINESS_K = 7


def _embedding_to_floats(value: Any) -> list[float]:
    """Normalize pgvector codec values across supported driver versions."""
    to_list = getattr(value, "to_list", None)
    if callable(to_list):
        value = to_list()
    else:
        to_numpy = getattr(value, "to_numpy", None)
        if callable(to_numpy):
            value = to_numpy()
    return [float(component) for component in value]


class UMAPProjector:
    """Per-tenant 2D UMAP projector for `models.topo_embedding`."""

    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def project(
        self, tenant_id: UUID
    ) -> dict[str, tuple[float, float]]:
        """Project every active model with a topo_embedding to 2D.

        Returns `{model_id_str: (x, y)}` with each coord in [-1, 1].
        Empty dict when the tenant has too few models (renderer falls
        back to force-directed)."""
        cache = await self._read_cache(tenant_id)
        needs_refit = await self._needs_refit(tenant_id, cache)
        if needs_refit:
            cache = await self._fit_and_cache(tenant_id)
            if cache is None:
                return {}
        coords_raw = cache.get("coords") or {}
        out: dict[str, tuple[float, float]] = {}
        for mid, xy in coords_raw.items():
            if isinstance(xy, list) and len(xy) == 2:
                out[str(mid)] = (float(xy[0]), float(xy[1]))
        return out

    async def refresh(self, tenant_id: UUID) -> dict[str, Any]:
        """Force a refit + cache write. Returns the cache payload."""
        cache = await self._fit_and_cache(tenant_id)
        if cache is None:
            return {
                "fitted_at": datetime.now(timezone.utc).isoformat(),
                "model_count": await self._count_active_embeddings(tenant_id),
                "trustworthiness": 0.0,
                "n_neighbors": DEFAULT_N_NEIGHBORS,
                "min_dist": DEFAULT_MIN_DIST,
                "coords": {},
            }
        return cache

    async def read_cache_meta(
        self, tenant_id: UUID
    ) -> dict[str, Any] | None:
        """Return just the metadata (no coords) from the cache."""
        cache = await self._read_cache(tenant_id)
        if cache is None:
            return None
        return {
            "fitted_at": cache.get("fitted_at"),
            "model_count": cache.get("model_count"),
            "trustworthiness": cache.get("trustworthiness", 0.0),
            "n_neighbors": cache.get("n_neighbors", DEFAULT_N_NEIGHBORS),
            "min_dist": cache.get("min_dist", DEFAULT_MIN_DIST),
        }

    # ------------------------------------------------------------------
    # Internals — cache + fit
    # ------------------------------------------------------------------

    async def _read_cache(
        self, tenant_id: UUID
    ) -> dict[str, Any] | None:
        row = await self._pool.fetchrow(
            """
            SELECT cached_content
            FROM view_ceo_cache
            WHERE tenant_id = $1 AND cache_key = $2
            """,
            tenant_id,
            CACHE_KEY,
        )
        if row is None:
            return None
        content = row["cached_content"]
        if isinstance(content, str):
            content = json.loads(content)
        return content

    async def _needs_refit(
        self, tenant_id: UUID, cache: dict[str, Any] | None
    ) -> bool:
        if cache is None:
            return True
        fitted_at_raw = cache.get("fitted_at")
        if not fitted_at_raw:
            return True
        try:
            fitted_at = datetime.fromisoformat(fitted_at_raw)
        except ValueError:
            return True
        evt = await self._pool.fetchval(
            """
            SELECT 1 FROM topology_events
            WHERE tenant_id = $1 AND occurred_at > $2
            LIMIT 1
            """,
            tenant_id,
            fitted_at,
        )
        return evt is not None

    async def _fetch_active_embeddings(
        self, tenant_id: UUID
    ) -> list[dict[str, Any]]:
        rows = await self._pool.fetch(
            """
            SELECT id, topo_embedding
            FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND topo_embedding IS NOT NULL
            ORDER BY id
            """,
            tenant_id,
        )
        return [dict(r) for r in rows]

    async def _count_active_embeddings(self, tenant_id: UUID) -> int:
        n = await self._pool.fetchval(
            """
            SELECT COUNT(*) FROM models
            WHERE tenant_id = $1
              AND status = 'active'
              AND topo_embedding IS NOT NULL
            """,
            tenant_id,
        )
        return int(n or 0)

    async def _fit_and_cache(
        self, tenant_id: UUID
    ) -> dict[str, Any] | None:
        """Fit the 128 → 2 UMAP, project all active embeddings, compute
        trustworthiness, write to view_ceo_cache. Returns the cache
        payload or None when there aren't enough models."""
        rows = await self._fetch_active_embeddings(tenant_id)
        if len(rows) < MIN_MODELS_FOR_UMAP:
            await self._pool.execute(
                "DELETE FROM view_ceo_cache "
                "WHERE tenant_id = $1 AND cache_key = $2",
                tenant_id,
                CACHE_KEY,
            )
            return None

        import numpy as np
        try:
            from sklearn.manifold import trustworthiness
        except Exception:  # pragma: no cover - optional metric dependency
            trustworthiness = None  # type: ignore[assignment]

        ids = [str(r["id"]) for r in rows]
        embeddings = np.asarray(
            [_embedding_to_floats(r["topo_embedding"]) for r in rows],
            dtype=float,
        )

        # n_neighbors must be < n_samples; cap for tiny tenants so UMAP
        # doesn't crash. With MIN_MODELS_FOR_UMAP = 6 this is always
        # respected for the default 15 by capping to n-1.
        n_neighbors = min(DEFAULT_N_NEIGHBORS, len(rows) - 1)
        projection_backend = "umap"
        try:
            import umap

            # Deterministic RNG — same input → same projection. Critical
            # for the "stable map" design promise.
            reducer = umap.UMAP(
                n_components=2,
                n_neighbors=n_neighbors,
                min_dist=DEFAULT_MIN_DIST,
                # Topo embeddings are L2-normalised; cosine matches
                # the substrate's notion of similarity.
                metric="cosine",
                random_state=42,
            )
            coords_2d = reducer.fit_transform(embeddings)
        except (ImportError, ModuleNotFoundError):
            # Production should install `umap-learn`, but the map must
            # degrade instead of 500ing when an optional projection
            # dependency is absent in a dev/test or constrained runtime.
            coords_2d = _pca_2d(embeddings)
            projection_backend = "pca_fallback"

        # Trustworthiness — 0..1. Computed against the same data we
        # fitted on; uses k separate from UMAP's n_neighbors so it
        # measures preservation, not the algorithm's own definition.
        k = min(TRUSTWORTHINESS_K, len(rows) - 1)
        try:
            trust = (
                float(trustworthiness(embeddings, coords_2d, n_neighbors=k))
                if trustworthiness is not None
                else 0.0
            )
        except Exception:
            # Defensive: trustworthiness can fail on degenerate inputs.
            # Don't block caching the projection on a metric.
            trust = 0.0

        # Normalise per-axis to [-1, 1].
        max_abs = np.maximum(np.abs(coords_2d).max(axis=0), 1e-9)
        normed = coords_2d / max_abs
        normed = np.clip(normed, -1.0, 1.0)
        coords_payload: dict[str, list[float]] = {
            ids[i]: [float(normed[i, 0]), float(normed[i, 1])]
            for i in range(len(ids))
        }

        fitted_at = datetime.now(timezone.utc).isoformat()
        payload: dict[str, Any] = {
            "coords": coords_payload,
            "fitted_at": fitted_at,
            "model_count": int(len(ids)),
            "trustworthiness": trust,
            "n_neighbors": int(n_neighbors),
            "min_dist": DEFAULT_MIN_DIST,
            "projection_backend": projection_backend,
        }
        await self._pool.execute(
            """
            INSERT INTO view_ceo_cache
              (tenant_id, cache_key, cached_content, cached_at,
               recomputed_reason)
            VALUES ($1, $2, $3::jsonb, now(), 'manual')
            ON CONFLICT (tenant_id, cache_key) DO UPDATE
            SET cached_content = EXCLUDED.cached_content,
                cached_at = EXCLUDED.cached_at,
                recomputed_reason = EXCLUDED.recomputed_reason
            """,
            tenant_id,
            CACHE_KEY,
            json.dumps(payload),
        )
        return payload


def _pca_2d(embeddings: Any) -> Any:
    import numpy as np

    centered = embeddings - embeddings.mean(axis=0, keepdims=True)
    try:
        _u, _s, vt = np.linalg.svd(centered, full_matrices=False)
        components = vt[:2].T
        coords = centered @ components
    except Exception:
        coords = centered[:, :2]
    if coords.shape[1] < 2:
        coords = np.pad(coords, ((0, 0), (0, 2 - coords.shape[1])))
    return coords[:, :2]


__all__ = [
    "CACHE_KEY",
    "MIN_MODELS_FOR_UMAP",
    "DEFAULT_N_NEIGHBORS",
    "DEFAULT_MIN_DIST",
    "TRUSTWORTHINESS_K",
    "UMAPProjector",
]
