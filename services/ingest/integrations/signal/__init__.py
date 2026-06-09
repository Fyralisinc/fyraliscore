"""Signal (linked-device messaging) integration package — IN-SIGNAL.

Signal is one of the final ingestion sources. Like Telegram (its archetype), it
is a USER-ACCOUNT messaging surface — not a bot/webhook API — so it can page a
thread's message history for backfill AND delivers live messages over a
persistent linked-device session (there is NO HTTP webhook, NO OAuth). See
ADR-0003 (Topology B) and the telegram source it clones.

COVERAGE: own/linked-account only. A Signal authorization is a LINKED DEVICE on a
single account (the service's own number / a linked companion device). It sees
only the threads that account participates in — exactly the conversations the
linked account is party to, nothing org-wide. This is the same self-coverage
posture as Telegram's user-account session.

Layout:
  - records.py   — the canonical message-record contract shared by the backfill
                   fetcher, the live gateway worker, and the synthetic
                   generators (so backfill + live derive an identical
                   external_id and dedup across paths).
  - client.py    — SignalClient: a thin (TODO-stubbed) wrapper over the real
                   signal-cli / libsignal surface for history backfill + thread
                   enumeration + the reconciler probe. The synthetic path uses
                   MockSignalClient, so the real client is a stub shell.
  - onboarding.py — finalize_install: UPSERT the install + threads + live-state
                   seed + the onboarding trigger that fires the M6 chain.
  - gateway/      — the live persistent-session worker (Telegram-gateway analog).
"""
