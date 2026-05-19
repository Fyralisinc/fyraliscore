"""Slack clean-path helper."""
from services.ingestion.fetchers import slack as sl_fetcher
from services.ingestion.reconcilers import slack as sl_reconciler
from services.ingestion.workflows import source_onboarding as so_mod


class _FakeClient:
    async def conversations_list(self):
        return [{"id": "C001", "name": "general", "team_id": "T1"}]

    async def conversations_history(self, *, channel, cursor=None,
                                    oldest=None, limit=None):
        if cursor is None and oldest is None:
            # First/backfill call.
            return [
                {"ts": "1700000.000001", "text": "m1"},
                {"ts": "1700000.000002", "text": "m2"},
            ], None
        # Reconciler check (oldest=newest_seen_ts): no newer messages.
        return [], None


async def _build(source, pool, install):
    if source == "slack":
        return _FakeClient()
    return None


async def _open(install):
    async def close(): return None
    return _FakeClient(), close


so_mod._build_source_client = _build
sl_fetcher._open_slack_client = _open
sl_reconciler._open_slack_client = _open
