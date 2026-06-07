#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${1:-"$ROOT_DIR/benchmarks/datasets/raw"}"

mkdir -p "$OUT_DIR"

base_url="https://huggingface.co/datasets/xiaowu0162/longmemeval-cleaned/resolve/main"

curl -L "$base_url/longmemeval_oracle.json" -o "$OUT_DIR/longmemeval_oracle.json"
curl -L "$base_url/longmemeval_s_cleaned.json" -o "$OUT_DIR/longmemeval_s_cleaned.json"
curl -L "$base_url/longmemeval_m_cleaned.json" -o "$OUT_DIR/longmemeval_m_cleaned.json"

shasum -a 256 "$OUT_DIR"/longmemeval*.json > "$OUT_DIR/longmemeval_cleaned.sha256"
echo "Downloaded LongMemEval cleaned files to $OUT_DIR"
