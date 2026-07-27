#!/usr/bin/env python3
"""Merge deterministic source-certification shards into one verified bundle."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from services.ingest.source_certification.producer import (
    EvidenceProducerError,
    merge_evidence_shards,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--shard-root",
        required=True,
        type=Path,
        help="directory whose immediate children are downloaded shard bundles",
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--target-sha", required=True)
    return parser


def _shard_directories(root: Path) -> tuple[Path, ...]:
    if root.is_symlink() or not root.is_dir():
        raise EvidenceProducerError("shard root must be a regular directory")
    shard_dirs = tuple(
        sorted(
            (
                child
                for child in root.iterdir()
                if child.is_dir()
                and not child.is_symlink()
                and (child / "provenance/producer-manifest.json").is_file()
            ),
            key=lambda path: path.name,
        )
    )
    unexpected = sorted(
        child.name
        for child in root.iterdir()
        if child not in shard_dirs
    )
    if unexpected:
        raise EvidenceProducerError(
            "shard root contains unexpected entries: " + ", ".join(unexpected)
        )
    if not shard_dirs:
        raise EvidenceProducerError("shard root contains no producer bundles")
    return shard_dirs


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        manifest = merge_evidence_shards(
            repo_root=REPO_ROOT,
            shard_dirs=_shard_directories(args.shard_root),
            output_dir=args.output_dir,
            expected_commit_sha=args.target_sha,
        )
    except (EvidenceProducerError, OSError) as exc:
        print(f"source certification shard merge error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "state": manifest["state"],
                "run_id": manifest["run_id"],
                "commit_sha": manifest["commit_sha"],
                "merged_sources": len(manifest["source_order"]),
                "output_dir": str(args.output_dir.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
