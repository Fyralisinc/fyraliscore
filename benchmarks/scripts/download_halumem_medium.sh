#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OUT_DIR="${1:-"$ROOT_DIR/benchmarks/datasets/raw"}"

mkdir -p "$OUT_DIR"
curl -L \
  "https://huggingface.co/datasets/IAAR-Shanghai/HaluMem/resolve/main/HaluMem-Medium.jsonl" \
  -o "$OUT_DIR/HaluMem-Medium.jsonl"

shasum -a 256 "$OUT_DIR/HaluMem-Medium.jsonl" > "$OUT_DIR/HaluMem-Medium.sha256"
echo "Downloaded HaluMem-Medium to $OUT_DIR"
