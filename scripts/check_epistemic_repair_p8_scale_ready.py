#!/usr/bin/env python3
"""Fail closed unless one-head deterministic P8 scale evidence is fully green."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.contracts.kernel import canonical_sha256


def require_scale_ready(path: Path, *, expected_head: str) -> None:
    artifact = json.loads(path.read_text(encoding="utf-8"))
    body = dict(artifact)
    embedded_digest = body.pop("artifact_digest", None)
    if embedded_digest != canonical_sha256(body):
        raise RuntimeError("P8 scale artifact digest mismatch")
    if artifact.get("commit") != expected_head:
        raise RuntimeError("P8 scale artifact commit does not match coherent rerun HEAD")
    if artifact.get("evaluation", {}).get("scale_execution_ready") is not True:
        raise RuntimeError("P8 deterministic scale gates are not fully green; provider canary is gated off")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scale", type=Path, required=True)
    parser.add_argument("--expected-head", required=True)
    args = parser.parse_args()
    require_scale_ready(args.scale, expected_head=args.expected_head)
    print(f"p8_deterministic_scale_ready=true commit={args.expected_head}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
