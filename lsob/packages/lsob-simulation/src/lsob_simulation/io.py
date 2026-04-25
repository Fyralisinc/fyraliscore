"""Read/write Corpus objects. Supports JSON (plain) and JSONL+zstd."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import zstandard as zstd

from lsob_contracts import Corpus, CorpusMeta, GroundTruth, Signal


def _is_zst(path: Path) -> bool:
    return path.suffix in (".zst", ".zstd") or path.name.endswith(".jsonl.zst")


def _is_plain_json(path: Path) -> bool:
    return path.suffix == ".json" and not path.name.endswith(".jsonl.zst")


def write_corpus(corpus: Corpus, path: str | Path) -> Path:
    """Write a Corpus. Format inferred from suffix.

    - `.json` ⇒ single JSON document (used for fixtures and small outputs).
    - `.jsonl.zst` / `.zst` ⇒ zstd-compressed JSON Lines: first line is meta,
      then one line per signal (tagged), then one line per ground-truth snapshot.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    if _is_plain_json(p):
        p.write_text(corpus.model_dump_json(indent=2))
        return p
    # Default: JSONL + zstd.
    lines: list[bytes] = []
    meta_line = {"kind": "meta", "data": json.loads(corpus.meta.model_dump_json())}
    lines.append((json.dumps(meta_line) + "\n").encode("utf-8"))
    for sig in corpus.signals:
        lines.append(
            (
                json.dumps({"kind": "signal", "data": json.loads(sig.model_dump_json())})
                + "\n"
            ).encode("utf-8")
        )
    for gt in corpus.ground_truth:
        lines.append(
            (
                json.dumps({"kind": "ground_truth", "data": json.loads(gt.model_dump_json())})
                + "\n"
            ).encode("utf-8")
        )
    blob = b"".join(lines)
    cctx = zstd.ZstdCompressor(level=10)
    compressed = cctx.compress(blob)
    p.write_bytes(compressed)
    return p


def read_corpus(path: str | Path) -> Corpus:
    """Read a Corpus. Supports `.json` plain and `.jsonl.zst` compressed."""
    p = Path(path)
    if _is_plain_json(p):
        raw = json.loads(p.read_text())
        return Corpus.model_validate(raw)
    if _is_zst(p):
        dctx = zstd.ZstdDecompressor()
        blob = dctx.decompress(p.read_bytes())
        text = blob.decode("utf-8")
        meta: CorpusMeta | None = None
        signals: list[Signal] = []
        gts: list[GroundTruth] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            item = json.loads(line)
            kind = item.get("kind")
            data = item.get("data", {})
            if kind == "meta":
                meta = CorpusMeta.model_validate(data)
            elif kind == "signal":
                signals.append(Signal.model_validate(data))
            elif kind == "ground_truth":
                gts.append(GroundTruth.model_validate(data))
        if meta is None:
            raise ValueError(f"corpus file {p} missing meta line")
        return Corpus(meta=meta, signals=signals, ground_truth=gts)
    raise ValueError(
        f"unsupported corpus path suffix: {p.suffix!r} (expected .json or .jsonl.zst/.zst)"
    )
