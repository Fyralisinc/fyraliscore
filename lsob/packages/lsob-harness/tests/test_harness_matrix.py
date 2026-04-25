"""YAML-to-RunRequest matrix expansion."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from lsob_harness.matrix import MatrixSpec, expand


def test_expand_cross_product(tmp_path: Path) -> None:
    matrix_yaml = {
        "suts": ["mock"],
        "corpora": [
            "fixtures/mini_corpus_a.json",
            "fixtures/mini_corpus_b.json",
        ],
        "ablations": [
            {"name": "none"},
            {"name": "no_bridge", "disable_bridge": True},
        ],
        "seeds": [42, 7],
        "layers": [1, 2],
    }
    p = tmp_path / "matrix.yaml"
    p.write_text(yaml.safe_dump(matrix_yaml))

    spec = MatrixSpec.from_yaml(p)
    reqs = expand(spec, known_suts=["mock"])
    # 1 sut * 2 corpora * 2 ablations * 2 seeds = 8
    assert len(reqs) == 8

    # spot-check a combination is present
    abls = sorted({r.ablation.name for r in reqs})
    assert abls == ["no_bridge", "none"]

    seeds = sorted({r.seed for r in reqs})
    assert seeds == [7, 42]

    corpora = sorted({str(r.corpus_path) for r in reqs})
    assert "fixtures/mini_corpus_a.json" in corpora


def test_unknown_sut_rejected(tmp_path: Path) -> None:
    p = tmp_path / "m.yaml"
    p.write_text(
        yaml.safe_dump(
            {"suts": ["does-not-exist"], "corpora": ["fixtures/mini_corpus_a.json"]}
        )
    )
    spec = MatrixSpec.from_yaml(p)
    with pytest.raises(ValueError):
        expand(spec, known_suts=["mock"])


def test_defaults_applied(tmp_path: Path) -> None:
    p = tmp_path / "m.yaml"
    p.write_text(
        yaml.safe_dump(
            {"suts": ["mock"], "corpora": ["fixtures/mini_corpus_a.json"]}
        )
    )
    spec = MatrixSpec.from_yaml(p)
    assert spec.concurrency == 4
    assert spec.layers == [1, 2, 3, 4, 5, 6]
    reqs = expand(spec, known_suts=["mock"])
    assert len(reqs) == 1
    assert reqs[0].ablation.name == "none"
