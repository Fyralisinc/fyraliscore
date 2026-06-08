"""Signal live gateway — the persistent linked-device session (IN-SIGNAL).

The Telegram-gateway analog: a single long-running worker holds an authenticated
Signal linked-device session (the LIVE session), receives pushed messages, and
shadow-writes each onto `ingestion.raw.signal` (`ingress_kind="gateway"`) so live
flows through the SAME normalizer→observation_writer chain as backfill — landing
in `observations` while backfill is still in flight (the concurrent overlap,
ADR-0003).

  - dispatch.py — `handle_update`: the load-bearing, test-drivable bridge from a
    live message to the pipeline (cutover shadow-write, inline fallback). Bound to
    one install/tenant by construction (one linked account = one tenant), so there
    is no per-update tenant resolution.
  - worker.py   — the linked-device receive loop + sync gap recovery + the
    single-instance Redis lease.
"""
