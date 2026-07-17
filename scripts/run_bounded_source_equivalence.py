#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from lib.evaluation.entity_evidence_composer import write_atomic_json
from services.source_equivalence_vertical import run_bounded_source_equivalence


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_bounded_source_equivalence()
    write_atomic_json(args.output, result)
    print(json.dumps({"output": str(args.output.resolve()),
                      "objective_sha256": result["objective_sha256"],
                      "verdict": result["evaluation"]["verdict"]}, sort_keys=True))
    return 0 if result["evaluation"]["verdict"] == "meets_policy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
