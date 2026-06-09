"""Download HotpotQA validation rows from the Hugging Face datasets server.

This keeps the benchmark harness dependency-light: the Hugging Face mirror is
stored as Parquet, but the datasets-server rows endpoint returns ordinary JSON.
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any


def main() -> int:
    parser = argparse.ArgumentParser(description="Download HotpotQA validation JSON.")
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("benchmarks/datasets/raw/hotpotqa_distractor_validation.json"),
    )
    parser.add_argument("--page-size", type=int, default=100)
    parser.add_argument("--max-rows", type=int, default=None)
    args = parser.parse_args()

    rows: list[dict[str, Any]] = []
    if args.out.exists():
        try:
            existing = json.loads(args.out.read_text(encoding="utf-8"))
            if isinstance(existing, list):
                rows = [row for row in existing if isinstance(row, dict)]
                print(f"resuming from {len(rows)} existing rows")
        except json.JSONDecodeError:
            rows = []
    offset = 0
    total = None
    if rows:
        offset = len(rows)
    page_size = min(max(1, args.page_size), 100)
    while total is None or offset < total:
        length = page_size
        if args.max_rows is not None:
            remaining = args.max_rows - len(rows)
            if remaining <= 0:
                break
            length = min(length, remaining)
        payload = _fetch_rows(offset=offset, length=length)
        total = int(payload["num_rows_total"])
        page_rows = [item["row"] for item in payload["rows"]]
        rows.extend(page_rows)
        offset += len(page_rows)
        if not page_rows:
            break
        print(f"downloaded {len(rows)}/{total}")
        _write_rows(args.out, rows)
        time.sleep(0.25)

    _write_rows(args.out, rows)
    print(f"wrote {len(rows)} rows to {args.out}")
    return 0


def _fetch_rows(*, offset: int, length: int) -> dict[str, Any]:
    params = urllib.parse.urlencode({
        "dataset": "hotpotqa/hotpot_qa",
        "config": "distractor",
        "split": "validation",
        "offset": offset,
        "length": length,
    })
    url = f"https://datasets-server.huggingface.co/rows?{params}"
    delay = 1.0
    for attempt in range(8):
        try:
            with urllib.request.urlopen(url, timeout=60) as response:
                return json.loads(response.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == 7:
                raise
            print(f"retrying offset {offset} after HTTP {exc.code}")
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
        except TimeoutError:
            if attempt == 7:
                raise
            print(f"retrying offset {offset} after timeout")
            time.sleep(delay)
            delay = min(delay * 2, 30.0)
    raise RuntimeError("unreachable")


def _write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    tmp.replace(path)


if __name__ == "__main__":
    raise SystemExit(main())
