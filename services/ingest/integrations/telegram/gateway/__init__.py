"""Telegram live gateway — the persistent MTProto updates connection (IN-TELEGRAM).

The Discord-gateway analog: a single long-running worker holds an authenticated
MTProto connection (the LIVE session), receives pushed updates
(`updateNewMessage`, …), and shadow-writes each onto `ingestion.raw.telegram`
(`ingress_kind="gateway"`) so live flows through the SAME
normalizer→observation_writer chain as backfill — landing in `observations`
while backfill is still in flight (the concurrent overlap, ADR-0003).

  - dispatch.py — `handle_update`: the load-bearing, test-drivable bridge from an
    update to the pipeline (cutover shadow-write, inline fallback). Bound to one
    install/tenant by construction (one account = one tenant), so there is no
    per-update tenant resolution.
  - worker.py   — the Telethon event loop + pts/qts/seq/date gap recovery
    (`updates.getDifference`) + the single-instance Redis lease.
"""
