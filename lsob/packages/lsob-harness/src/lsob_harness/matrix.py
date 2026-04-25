"""Matrix expansion for ``lsob bulk-run``.

YAML shape:

.. code-block:: yaml

    suts: [mock]
    corpora: [fixtures/mini_corpus_a.json]
    ablations:
      - name: none
      - name: no_bridge
        disable_bridge: true
    layers: [1, 2, 3, 4, 5, 6]
    seeds: [42, 7]
    sut_params: {}

Every combination of ``(sut, corpus, ablation, seed)`` becomes a
:class:`~lsob_harness.runner.RunRequest`.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from lsob_contracts import AblationConfig

from lsob_harness.registry import list_known_suts
from lsob_harness.runner import RunRequest


@dataclass
class MatrixSpec:
    suts: list[str]
    corpora: list[str]
    ablations: list[dict[str, Any]]
    layers: list[int]
    seeds: list[int]
    sut_params: dict[str, Any]
    runs_root: Path
    concurrency: int

    @classmethod
    def from_yaml(cls, path: str | Path) -> "MatrixSpec":
        data = yaml.safe_load(Path(path).read_text()) or {}
        return cls(
            suts=list(data.get("suts") or []),
            corpora=list(data.get("corpora") or []),
            ablations=list(data.get("ablations") or [{"name": "none"}]),
            layers=list(data.get("layers") or [1, 2, 3, 4, 5, 6]),
            seeds=list(data.get("seeds") or [42]),
            sut_params=dict(data.get("sut_params") or {}),
            runs_root=Path(data.get("runs_root") or "runs"),
            concurrency=int(data.get("concurrency") or 4),
        )


def expand(spec: MatrixSpec, *, known_suts: list[str] | None = None) -> list[RunRequest]:
    names = known_suts if known_suts is not None else list_known_suts()
    requests: list[RunRequest] = []
    for sut in spec.suts:
        if names and sut not in names:
            raise ValueError(f"unknown sut in matrix: {sut!r} (known: {names})")
        for corpus in spec.corpora:
            for ablation_raw in spec.ablations:
                ablation = AblationConfig(**(ablation_raw or {"name": "none"}))
                for seed in spec.seeds:
                    requests.append(
                        RunRequest(
                            corpus_path=Path(corpus),
                            sut_name=sut,
                            layers=list(spec.layers),
                            ablation=ablation,
                            runs_root=spec.runs_root,
                            sut_params=dict(spec.sut_params),
                            seed=int(seed),
                        )
                    )
    return requests
