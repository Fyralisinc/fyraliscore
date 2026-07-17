#!/usr/bin/env python3
"""Compose matched adaptive-vs-frozen feedback evidence from a sealed artifact."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from lib.evaluation.feedback_learning_effect import compose_feedback_learning_effect


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--active-surfaces-evidence", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.active_surfaces_evidence.read_bytes()
    source = json.loads(raw)
    evidence = compose_feedback_learning_effect(
        source_payload=source,
        source_artifact_sha256=hashlib.sha256(raw).hexdigest(),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(evidence.artifact_payload(), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    print(args.output.resolve())
    print(evidence.digest)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
