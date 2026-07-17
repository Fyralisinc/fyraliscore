#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

from lib.evaluation.entity_evidence_composer import write_atomic_json
from services.retrieval_evolution_postfix_vertical import run_bounded_retrieval_evolution_postfix


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    result = run_bounded_retrieval_evolution_postfix()
    write_atomic_json(args.output, result)
    print(json.dumps({"output": str(args.output.resolve()),
                      "objective_sha256": result["objective_sha256"],
                      "verdict": result["evaluation"]["verdict"]}, sort_keys=True))
    return 0 if result["evaluation"]["verdict"] == "meets_preregistered_policy" else 1


if __name__ == "__main__":
    raise SystemExit(main())
