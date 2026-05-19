"""Discord clean-path helper."""
from services.ingestion.fetchers import discord as fdc
from services.ingestion.reconcilers import discord as rdc
from services.ingestion.workflows import source_onboarding as so_mod


class _FakeDC:
    async def list_guilds(self):
        return [{"id": "G1"}]
    async def list_guild_channels(self, guild_id):
        # 1 text channel — 5% sampling rounds to max(1, 0) = 1.
        return [{"id": "C1", "name": "general", "type": 0}]
    async def get_messages(self, *, channel_id, before=None, after=None, limit=None):
        if after is not None:
            # Reconciler probe: no newer.
            return []
        # Backfill — return 2 messages then end.
        return [{"id": "200", "content": "m2"},
                {"id": "100", "content": "m1"}]


async def _build(source, pool, install):
    if source == "discord":
        return _FakeDC()
    return None


async def _open(install):
    async def close(): return None
    return _FakeDC(), close


so_mod._build_source_client = _build
fdc._open_discord_client = _open
rdc._open_discord_client = _open
