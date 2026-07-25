#!/usr/bin/env python3
"""Validate a signed 27-source manifest for production promotion."""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Sequence


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from services.ingest.source_certification.promotion import (  # noqa: E402
    PromotionManifestError,
    validate_promotion_manifest_file,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--target-sha", required=True)
    parser.add_argument(
        "--signing-key-env",
        required=True,
        help="environment variable containing the HMAC verification key",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    signing_key = os.environ.get(args.signing_key_env)
    if not signing_key:
        print(
            "source certification promotion blocked: signing key environment "
            f"variable {args.signing_key_env!r} is empty or unset",
            file=sys.stderr,
        )
        return 2
    try:
        result = validate_promotion_manifest_file(
            args.manifest,
            target_sha=args.target_sha,
            signing_key=signing_key.encode("utf-8"),
        )
    except PromotionManifestError as exc:
        print(f"source certification promotion blocked: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
