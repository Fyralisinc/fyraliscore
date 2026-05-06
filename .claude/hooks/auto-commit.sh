#!/usr/bin/env bash
# Stop-hook: auto-commit any pending changes locally. Never pushes.
# Skips during in-progress rebase/merge/cherry-pick/revert/bisect.
# Respects .gitignore (git add -A leaves ignored untracked files alone).

set -e

REPO_ROOT="$(git rev-parse --show-toplevel 2>/dev/null)" || exit 0
cd "$REPO_ROOT"

GITDIR="$(git rev-parse --git-dir)"
for state in MERGE_HEAD CHERRY_PICK_HEAD REVERT_HEAD rebase-apply rebase-merge BISECT_LOG; do
  if [ -e "$GITDIR/$state" ]; then
    printf '{"systemMessage":"auto-commit: skipped (git operation in progress: %s)"}\n' "$state"
    exit 0
  fi
done

if [ -z "$(git status --porcelain)" ]; then
  exit 0
fi

git add -A

if git diff --cached --quiet; then
  exit 0
fi

COUNT="$(git diff --cached --name-only | wc -l | tr -d ' ')"
TOP="$(git diff --cached --name-only | head -3 | tr '\n' ',' | sed 's/,$//')"
MSG="auto: snapshot ${COUNT} file(s) — ${TOP}"

if ! ERR="$(git commit -m "$MSG" 2>&1 >/dev/null)"; then
  ESCAPED="$(printf '%s' "$ERR" | head -c 200 | tr -d '\r' | sed 's/\\/\\\\/g; s/"/\\"/g; s/\t/ /g; :a; N; $!ba; s/\n/ | /g')"
  printf '{"systemMessage":"auto-commit: commit failed — %s"}\n' "$ESCAPED"
  exit 0
fi

SHA="$(git rev-parse --short HEAD)"
printf '{"systemMessage":"auto-commit: %s (%s files)"}\n' "$SHA" "$COUNT"
