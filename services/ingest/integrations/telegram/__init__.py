"""Telegram (MTProto) integration package — IN-TELEGRAM.

Telegram is the 12th ingestion source. Unlike the Bot API, the MTProto
user-account API can page a dialog's history (messages.getHistory) and delivers
live updates over a persistent connection (no HTTP webhook). See ADR-0003 and
docs/ingestion/sources/telegram.md.

Layout:
  - records.py   — the canonical message-record contract shared by the backfill
                   fetcher, the live gateway worker, and the synthetic
                   generators (so backfill + live derive an identical
                   external_id and dedup across paths).
  - client.py    — TelegramClient: a thin Telethon wrapper (import-guarded) for
                   history backfill + dialog enumeration + the reconciler probe.
  - onboarding.py — finalize_install: UPSERT the install + dialogs + live-state
                   seed + the onboarding trigger that fires the M6 chain.
  - gateway/      — the live persistent-updates worker (Discord-gateway analog).
"""
