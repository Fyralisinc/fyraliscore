#!/usr/bin/env bash
# scripts/dogfood_logs.sh — tail every service log with a service-name prefix.
LOGDIR=/tmp/fyralis_logs
if command -v multitail >/dev/null 2>&1; then
  multitail -m 2000 \
    -l "tail -n 50 -f $LOGDIR/gateway.log" \
    -l "tail -n 50 -f $LOGDIR/think_worker.log" \
    -l "tail -n 50 -f $LOGDIR/post_commit_worker.log" \
    -l "tail -n 50 -f $LOGDIR/ui.log"
else
  # No multitail — prefix each line with its source filename.
  tail -n 20 -f \
    $LOGDIR/gateway.log \
    $LOGDIR/think_worker.log \
    $LOGDIR/post_commit_worker.log \
    $LOGDIR/ui.log 2>/dev/null
fi
