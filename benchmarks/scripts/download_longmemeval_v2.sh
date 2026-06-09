#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="${1:-benchmarks/datasets/raw/longmemeval-v2}"
INCLUDE_TRAJECTORIES="${INCLUDE_TRAJECTORIES:-0}"
BASE_URL="https://huggingface.co/datasets/xiaowu0162/longmemeval-v2/resolve/main"

mkdir -p "$OUT_DIR/haystacks"

curl -L "$BASE_URL/README.md" -o "$OUT_DIR/README.md"
curl -L "$BASE_URL/SCHEMA.md" -o "$OUT_DIR/SCHEMA.md"
curl -L "$BASE_URL/questions.jsonl" -o "$OUT_DIR/questions.jsonl"
curl -L "$BASE_URL/haystacks/lme_v2_small.json" -o "$OUT_DIR/haystacks/lme_v2_small.json"
curl -L "$BASE_URL/haystacks/lme_v2_medium.json" -o "$OUT_DIR/haystacks/lme_v2_medium.json"

if [[ "$INCLUDE_TRAJECTORIES" == "1" ]]; then
  curl -L "$BASE_URL/trajectories.jsonl" -o "$OUT_DIR/trajectories.jsonl"
else
  cat <<MSG
Downloaded lightweight LongMemEval-V2 files to $OUT_DIR.

The trajectory file is large (~1.2GB) and was not downloaded.
Fetch it when you are ready to run LME-V2:

  INCLUDE_TRAJECTORIES=1 bash benchmarks/scripts/download_longmemeval_v2.sh "$OUT_DIR"
MSG
fi
