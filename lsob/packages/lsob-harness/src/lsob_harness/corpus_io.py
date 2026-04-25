"""Load a :class:`Corpus` from ``.json`` or ``.jsonl.zst`` on disk.

Sniffing rule (intentionally dumb): if the path ends in ``.jsonl.zst`` we
decompress with ``zstandard`` and expect a line-delimited stream whose first
line is the ``meta`` object and remaining lines are individual ``signals``.
``ground_truth`` must live alongside at ``<stem>.ground_truth.json`` if any.
Plain ``.json`` is a single blob matching :class:`Corpus` directly.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lsob_contracts import Corpus


def load_corpus(path: str | Path) -> Corpus:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"corpus not found: {p}")
    name = p.name.lower()
    if name.endswith(".jsonl.zst"):
        return _load_jsonl_zst(p)
    if name.endswith(".json"):
        raw = json.loads(p.read_text())
        return Corpus.model_validate(raw)
    raise ValueError(f"unsupported corpus extension: {p.name}")


def _load_jsonl_zst(p: Path) -> Corpus:
    try:
        import zstandard  # type: ignore
    except ImportError as e:  # pragma: no cover - dep always present
        raise RuntimeError("zstandard is required to read .jsonl.zst") from e

    dctx = zstandard.ZstdDecompressor()
    with p.open("rb") as fh:
        raw = dctx.stream_reader(fh).read()
    lines = [ln for ln in raw.decode("utf-8").splitlines() if ln.strip()]
    if not lines:
        raise ValueError(f"corpus file is empty: {p}")
    meta = json.loads(lines[0])
    signals: list[dict[str, Any]] = [json.loads(ln) for ln in lines[1:]]
    gt_path = p.with_name(p.name.removesuffix(".jsonl.zst") + ".ground_truth.json")
    ground_truth: list[dict[str, Any]] = []
    if gt_path.exists():
        ground_truth = json.loads(gt_path.read_text())
    return Corpus.model_validate(
        {"meta": meta, "signals": signals, "ground_truth": ground_truth}
    )
