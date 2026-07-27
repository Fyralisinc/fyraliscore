#!/usr/bin/env python3
"""Execute receipt-backed source certification bindings for one commit."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Sequence

from services.ingest.source_certification.producer import (
    EvidenceProducerError,
    deterministic_source_shard,
    load_secret_environment_bundle,
    produce_evidence,
)
from services.ingest.source_contract.catalog import CANONICAL_SOURCE_IDS


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_BINDING_DIR = (
    REPO_ROOT / "services/ingest/source_certification/execution_bindings"
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--binding-dir", type=Path, default=DEFAULT_BINDING_DIR)
    parser.add_argument("--commit-sha")
    parser.add_argument("--run-id")
    parser.add_argument(
        "--secret-bundle",
        type=Path,
        help=(
            "mode-0600 JSON object of explicitly supported environment "
            "values; values remain process-local and are never receipted"
        ),
    )
    parser.add_argument(
        "--secret-bundle-source",
        choices=CANONICAL_SOURCE_IDS,
        help=(
            "canonical source allowed to appear in FYRALIS_CANARY_* bundle "
            "keys; must identify the command's one-source shard"
        ),
    )
    parser.add_argument(
        "--shard-index",
        type=int,
        help="zero-based deterministic source shard index",
    )
    parser.add_argument(
        "--shard-count",
        type=int,
        help="total deterministic source shard count",
    )
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="return non-zero after writing the diagnostic bundle unless it passed",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if (args.secret_bundle is None) != (
            args.secret_bundle_source is None
        ):
            raise EvidenceProducerError(
                "--secret-bundle and --secret-bundle-source are required together"
            )
        if args.secret_bundle_source is not None:
            if args.shard_index is None or args.shard_count is None:
                raise EvidenceProducerError(
                    "a source-scoped secret bundle requires a deterministic shard"
                )
            selected = deterministic_source_shard(
                args.shard_index,
                args.shard_count,
            )
            if selected != (args.secret_bundle_source,):
                raise EvidenceProducerError(
                    "secret bundle source differs from the one-source shard"
                )
        ambient_env = dict(os.environ)
        if args.secret_bundle is not None:
            ambient_env.update(
                load_secret_environment_bundle(
                    args.secret_bundle.resolve(),
                    source_id=args.secret_bundle_source,
                )
            )
        manifest = produce_evidence(
            repo_root=REPO_ROOT,
            binding_dir=args.binding_dir,
            output_dir=args.output_dir,
            commit_sha=args.commit_sha,
            run_id=args.run_id,
            shard_index=args.shard_index,
            shard_count=args.shard_count,
            ambient_env=ambient_env,
        )
    except EvidenceProducerError as exc:
        print(f"source certification evidence producer error: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                "state": manifest["state"],
                "run_id": manifest["run_id"],
                "commit_sha": manifest["commit_sha"],
                "required_sources": manifest["required_sources"],
                "passed_sources": sum(
                    entry["decision_state"] == "passed"
                    for entry in manifest["sources"]
                ),
                "output_dir": str(args.output_dir.resolve()),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return int(args.require_complete and manifest["state"] != "passed")


if __name__ == "__main__":
    raise SystemExit(main())
