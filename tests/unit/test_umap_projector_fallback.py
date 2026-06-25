from __future__ import annotations

import numpy as np

from services.reasoning.topology.umap_projector import _pca_2d


def test_pca_fallback_returns_stable_two_dimensional_projection() -> None:
    embeddings = np.asarray(
        [
            [1.0, 0.0, 0.0],
            [0.0, 1.0, 0.0],
            [0.0, 0.0, 1.0],
            [1.0, 1.0, 0.0],
        ],
        dtype=float,
    )

    first = _pca_2d(embeddings)
    second = _pca_2d(embeddings)

    assert first.shape == (4, 2)
    assert np.allclose(first, second)
