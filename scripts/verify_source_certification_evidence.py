#!/usr/bin/env python3
"""Verify source-certification inputs against their producer receipts."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from services.ingest.source_certification.producer import (
    EvidenceProducerError,
    verify_evidence_bundle,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", required=True, type=Path)
    parser.add_argument("--provenance-dir", required=True, type=Path)
    parser.add_argument("--target-sha", required=True)
    parser.add_argument(
        "--allow-blocked",
        action="store_true",
        help="verify diagnostic integrity without accepting it for release",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = verify_evidence_bundle(
            repo_root=REPO_ROOT,
            input_dir=args.input_dir,
            provenance_dir=args.provenance_dir,
            expected_commit_sha=args.target_sha,
            require_complete=not args.allow_blocked,
        )
    except (EvidenceProducerError, OSError) as exc:
        print(f"source certification evidence verification error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
