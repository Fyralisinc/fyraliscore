#!/usr/bin/env python3
"""Independently reproduce a frozen P9 manifest and issue a reviewer receipt."""

from __future__ import annotations

import argparse
from hashlib import sha256
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if __package__ in {None, ""}:
    sys.path.insert(0, str(ROOT))

from lib.evaluation.epistemic_repair.p9_release import REVIEW_SCHEMA_VERSION, reproduce


def _digest(value):
    return sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reviewer-id", required=True)
    args = parser.parse_args()
    manifest = json.loads(args.manifest.read_text())
    if args.reviewer_id == manifest.get("coordinator_id"):
        raise SystemExit("reviewer-id must differ from coordinator-id")
    result = reproduce(manifest)
    body = {
        "schema_version": REVIEW_SCHEMA_VERSION, "reviewer_id": args.reviewer_id,
        "status": "reproduced", "reviewed_manifest_digest": manifest["manifest_digest"],
        "reproduced_report_digest": result["reproduced_report_digest"],
    }
    receipt = {**body, "receipt_digest": _digest(body)}
    args.output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n")
    print(f"receipt_digest={receipt['receipt_digest']} evidence_green={result['required_evidence_green']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
