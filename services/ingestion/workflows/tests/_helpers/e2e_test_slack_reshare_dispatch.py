"""Slack reshare-path helper. Reconciler pass-0 sees
newer messages; pass-1 sees no newer messages."""
from services.ingestion.fetchers import slack as sl_fetcher
from services.ingestion.reconcilers import slack as sl_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _FetcherClient:
    """Used by source_onboarding (planner) AND shard_fetch (fetcher).
    Returns 2 backfill messages; for gap-fill shards (oldest=ts),
    returns 1 newer message and ends.
    """
    async def conversations_list(self):
        return [{"id": "C001", "name": "general", "team_id": "T1"}]

    async def conversations_history(self, *, channel, cursor=None,
                                    oldest=None, limit=None):
        if oldest is not None:
            # Gap-fill shard's fetch (the shard_identifier has gap_baseline_ts).
            # In practice the fetcher passes cursor (next_cursor), not oldest,
            # so this branch is mostly defensive.
            return [{"ts": "1800000.000"}], None
        # Backfill: 2 messages.
        return [
            {"ts": "1700000.001"},
            {"ts": "1700000.002"},
        ], None


class _ReconcilerClient:
    """Stateful: first call returns newer; subsequent calls return empty."""
    def __init__(self):
        self.calls = 0
    async def conversations_history(self, *, channel, cursor=None,
                                    oldest=None, limit=None):
        self.calls += 1
        if self.calls == 1:
            return [{"ts": "1800000.000"}], None
        return [], None


_FC = _FetcherClient()
_RC = _ReconcilerClient()


async def _build(source, pool, install):
    if source == "slack":
        return _FC
    return None


async def _fopen(install):
    async def close(): return None
    return _FC, close


async def _ropen(install):
    async def close(): return None
    return _RC, close


so_mod._build_source_client = _build
sl_fetcher._open_slack_client = _fopen
sl_reconciler._open_slack_client = _ropen
