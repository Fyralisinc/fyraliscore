#!/usr/bin/env python3
"""Run one explicitly authorized Codex CLI canary and preserve raw JSONL receipts."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess

from lib.evaluation.epistemic_repair.provider_contract import (
    require_codex_cli_environment,
)


def main() -> int:
    require_codex_cli_environment()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--authorization-id", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    command = [
        "codex", "exec", "--ephemeral", "--ignore-rules", "-s", "read-only",
        "-m", "gpt-5.4", "--json",
        "Return exactly one concise sentence confirming this bounded P8 provider canary.",
    ]
    result = subprocess.run(command, check=False, capture_output=True, text=True, timeout=180)
    rows = []
    for line in result.stdout.splitlines():
        try:
            row = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(row, dict):
            rows.append(row)
    completed = [
        row for row in rows
        if row.get("type") == "turn.completed" and isinstance(row.get("usage"), dict)
        and int(row["usage"].get("input_tokens", 0)) > 0
    ]
    if result.returncode != 0 or not completed:
        raise RuntimeError("authorized P8 canary lacked a successful exact-usage receipt")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    authorization = {
        "type": "p8.canary.authorization", "authorization_id": args.authorization_id,
        "provider": "codex", "model": "gpt-5.4", "transport": "cli",
        "commit": subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip(),
    }
    args.output.write_text("\n".join(
        json.dumps(row, sort_keys=True) for row in (authorization, *rows)
    ) + "\n")
    print(f"authorized_canary=true output={args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
