"""Discord reshare-path helper."""
from services.ingestion.fetchers import discord as fdc
from services.ingestion.reconcilers import discord as rdc
from services.ingestion.workflows import source_onboarding as so_mod


class _FetcherDC:
    async def list_guilds(self):
        return [{"id": "G1"}]
    async def list_guild_channels(self, guild_id):
        return [{"id": "C1", "name": "general", "type": 0}]
    async def get_messages(self, *, channel_id, before=None, after=None, limit=None):
        return [{"id": "200"}, {"id": "100"}]


class _ReconcilerDC:
    def __init__(self):
        self.calls = 0
    async def get_messages(self, *, channel_id, before=None, after=None, limit=None):
        self.calls += 1
        if self.calls == 1:
            return [{"id": "999"}]  # newer → gap
        return []  # subsequent → clean


_FC = _FetcherDC()
_RC = _ReconcilerDC()


async def _build(source, pool, install):
    if source == "discord":
        return _FC
    return None


async def _fopen(install):
    async def close(): return None
    return _FC, close


async def _ropen(install):
    async def close(): return None
    return _RC, close


so_mod._build_source_client = _build
fdc._open_discord_client = _fopen
rdc._open_discord_client = _ropen
