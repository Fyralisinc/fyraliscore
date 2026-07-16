#!/usr/bin/env python3
"""Evaluate saved normalized-signal batches against gold entity annotations."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from lib.evaluation.entity_extraction_gold import (
    GoldMention,
    GoldSignal,
    PredictedMention,
    evaluate_gold_entity_extraction,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    payload = json.loads(args.input.read_text())
    report = evaluate_gold_entity_extraction(
        signals=tuple(GoldSignal.model_validate(item) for item in payload["signals"]),
        gold_mentions=tuple(
            GoldMention.model_validate(item) for item in payload["gold_mentions"]
        ),
        predictions=tuple(
            PredictedMention.model_validate(item) for item in payload["predictions"]
        ),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report.model_dump_json(indent=2) + "\n")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
