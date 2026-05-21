"""Local source-mock "spammer" — an out-of-process web server impersonating
each source's outbound API, with rate limiting.

This is the target you point the pipeline at via lib.integrations.endpoints
(set `SYNTHETIC_SOURCE_API_BASE=http://localhost:<port>`, or per-source
`*_API_BASE_URL`). The real source clients then make real HTTP calls here
instead of to the providers — exercising the genuine outbound path
(token exchange → authed request → pagination → 429 backoff) on synthetic
data.

Mounted layout (matches lib/integrations/endpoints._SPAMMER_SUBPATH):
  /gmail/token                      — DWD token exchange (decodes assertion sub)
  /gmail/gmail/v1/users/me/*        — Gmail data API (messages/history/profile)
  /github/app/installations/{id}/access_tokens — App→installation token mint
  /github/installation/repositories — repos for the token's installation
  /github/repos/{owner}/{repo}/{issues|pulls} — issue / PR pages (ETag + Link)
  /slack/api/conversations.{list,history}      — Slack Web API (ok=true wrapped)
  /discord/api/v10/users/@me/guilds            — bot's guilds
  /discord/api/v10/guilds/{gid}/channels       — guild channels
  /discord/api/v10/channels/{cid}/messages     — channel messages (snowflake page)

Fixture seeding: pass `fixtures={"github": [..], "slack": [..], ...}` to
`build_spammer_app` (the make_<source>_* generator dicts), or point
`SPAMMER_FIXTURE_REGISTRY` at the harness registry.json. Without seeded
fixtures the gmail vertical still self-generates a deterministic mailbox
per requested email (back-compat).

Rate limiting (per env, so a load run can dial it):
  SPAMMER_429_EVERY   — return HTTP 429 on every Nth data request (0 = off).
  SPAMMER_RETRY_AFTER — Retry-After seconds on those 429s (default 1).

Identity model (how the spammer routes a request to the right tenant):
  - gmail: the DWD token endpoint returns `spam::<email>`; data requests
    carry it as the bearer, so the mailbox is keyed by email.
  - github: the access-token endpoint returns `spam-gh::<installation_id>`;
    `/installation/repositories` resolves repos from it. Per-repo endpoints
    key on the `{owner}/{repo}` path (globally unique across tenants).
  - slack: per-channel endpoints key on the `channel` param (globally
    unique). `conversations.list` filters by `spam-slack::<team>` when the
    bearer encodes it, else returns every seeded channel.
  - discord: ONE app-level bot is in every guild, so `/users/@me/guilds`
    returns all seeded guilds; channels/messages key on the path id.
"""
from __future__ import annotations

import base64
import json
import os
from typing import Any

from fastapi import APIRouter, FastAPI, Header, Request
from fastapi.responses import JSONResponse

from services.synthetic.fixtures import make_gmail_mailbox


# =====================================================================
# Token helpers.
# =====================================================================
def _decode_jwt_sub(assertion: str) -> str | None:
    """Read the `sub` (impersonated email) from a DWD assertion JWT WITHOUT
    verifying the signature — the spammer trusts the caller (it's a mock)."""
    try:
        _, payload_b64, _ = assertion.split(".")
        pad = "=" * (-len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(payload_b64 + pad))
        return payload.get("sub")
    except Exception:  # noqa: BLE001
        return None


def _bearer(authorization: str | None) -> str | None:
    if not authorization:
        return None
    parts = authorization.split(None, 1)
    if len(parts) == 2 and parts[0].lower() in ("bearer", "token", "bot"):
        return parts[1].strip()
    return None


def _email_from_bearer(authorization: str | None) -> str | None:
    """The gmail token endpoint returns `spam::<email>`; extract it back."""
    token = _bearer(authorization)
    if token and token.startswith("spam::"):
        return token[len("spam::"):]
    return None


# =====================================================================
# Per-source fixture stores (built from seeded fixtures + on-demand gmail).
# =====================================================================
class _MailboxStore:
    """Deterministic Gmail mailbox per email. Seeded fixtures (keyed by
    email) win; otherwise a mailbox is generated on first request."""

    def __init__(
        self,
        messages_per_mailbox: int,
        seeded: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self._n = messages_per_mailbox
        self._cache: dict[str, dict[str, Any]] = dict(seeded or {})

    def get(self, email: str) -> dict[str, Any]:
        key = email.lower()
        if key not in self._cache:
            self._cache[key] = make_gmail_mailbox(
                email=key, messages=self._n, starting_history_id=1000,
            )
        return self._cache[key]


class _GithubStore:
    def __init__(self, fixtures: list[dict[str, Any]]) -> None:
        # full_name -> {event_type: [events]}
        self._repos: dict[str, dict[str, list[dict[str, Any]]]] = {}
        # installation_id -> [full_name]
        self._installs: dict[str, list[str]] = {}
        for fx in fixtures:
            inst = str(fx.get("installation_id", "12345"))
            for r in fx.get("repos", []):
                self._repos[r["full_name"]] = r.get("events_by_type", {})
                self._installs.setdefault(inst, []).append(r["full_name"])

    def repos_for_install(self, installation_id: str | None) -> list[str]:
        if installation_id and installation_id in self._installs:
            return self._installs[installation_id]
        # Single-install fixtures: any token resolves to the lone install.
        if len(self._installs) == 1:
            return next(iter(self._installs.values()))
        return []

    def events(self, full_name: str, event_type: str) -> list[dict[str, Any]]:
        return list(self._repos.get(full_name, {}).get(event_type, []))


class _SlackStore:
    def __init__(self, fixtures: list[dict[str, Any]]) -> None:
        self._channels_by_team: dict[str, list[dict[str, Any]]] = {}
        self._messages: dict[str, list[dict[str, Any]]] = {}
        self._all_channels: list[dict[str, Any]] = []
        for fx in fixtures:
            team = fx.get("team_id", "T_TEST")
            for c in fx.get("channels", []):
                entry = {"id": c["id"], "name": c.get("name"),
                         "team_id": c.get("team_id", team)}
                self._channels_by_team.setdefault(team, []).append(entry)
                self._all_channels.append(entry)
                self._messages[c["id"]] = list(c.get("messages", []))

    def channels(self, team: str | None) -> list[dict[str, Any]]:
        if team and team in self._channels_by_team:
            return self._channels_by_team[team]
        return self._all_channels

    def messages(self, channel_id: str) -> list[dict[str, Any]]:
        return self._messages.get(channel_id, [])


class _DiscordStore:
    def __init__(self, fixtures: list[dict[str, Any]]) -> None:
        self._guilds: list[str] = []
        self._channels_by_guild: dict[str, list[dict[str, Any]]] = {}
        self._messages: dict[str, list[dict[str, Any]]] = {}
        for fx in fixtures:
            gid = str(fx["guild_id"])
            self._guilds.append(gid)
            for c in fx.get("channels", []):
                self._channels_by_guild.setdefault(gid, []).append({
                    "id": c["id"], "name": c.get("name"),
                    "type": c.get("type", 0),
                })
                self._messages[c["id"]] = list(c.get("messages", []))

    def guilds(self) -> list[dict[str, Any]]:
        return [{"id": g} for g in self._guilds]

    def channels(self, guild_id: str) -> list[dict[str, Any]]:
        return self._channels_by_guild.get(guild_id, [])

    def messages(self, channel_id: str) -> list[dict[str, Any]]:
        return self._messages.get(channel_id, [])


# =====================================================================
# Rate-limit middleware state.
# =====================================================================
class _RateLimiter:
    def __init__(self, every: int, retry_after_s: int) -> None:
        self._every = every
        self._retry_after_s = retry_after_s
        self._count = 0

    def should_429(self) -> bool:
        if self._every <= 0:
            return False
        self._count += 1
        return self._count % self._every == 0

    @property
    def retry_after_s(self) -> int:
        return self._retry_after_s


def _load_registry_fixtures(path: str) -> dict[str, list[dict[str, Any]]]:
    """Group a harness registry.json's per-tenant entries by source.

    Gmail fixtures are additionally keyed by email so the mailbox store
    serves the EXACT messages the harness expects (count parity)."""
    out: dict[str, list[dict[str, Any]]] = {}
    with open(path) as f:
        reg = json.load(f)
    for entry in reg.get("entries", []):
        out.setdefault(entry["source"], []).append(entry["fixture"])
    return out


# =====================================================================
# App factory.
# =====================================================================
def build_spammer_app(
    *,
    gmail_messages_per_mailbox: int | None = None,
    rate_limit_every: int | None = None,
    retry_after_s: int | None = None,
    fixtures: dict[str, list[dict[str, Any]]] | None = None,
) -> FastAPI:
    n_msgs = (
        gmail_messages_per_mailbox
        if gmail_messages_per_mailbox is not None
        else int(os.environ.get("SPAMMER_GMAIL_MESSAGES", "5"))
    )
    every = (
        rate_limit_every if rate_limit_every is not None
        else int(os.environ.get("SPAMMER_429_EVERY", "0"))
    )
    retry_after = (
        retry_after_s if retry_after_s is not None
        else int(os.environ.get("SPAMMER_RETRY_AFTER", "1"))
    )

    if fixtures is None:
        reg_path = os.environ.get("SPAMMER_FIXTURE_REGISTRY")
        fixtures = (
            _load_registry_fixtures(reg_path)
            if reg_path and os.path.exists(reg_path) else {}
        )

    # Gmail seeded fixtures are keyed by email (lower-cased).
    gmail_seeded = {
        fx["email"].lower(): fx for fx in fixtures.get("gmail", [])
        if fx.get("email")
    }

    app = FastAPI(title="fyralis-source-spammer")
    app.state.mailboxes = _MailboxStore(n_msgs, gmail_seeded)
    app.state.github = _GithubStore(fixtures.get("github", []))
    app.state.slack = _SlackStore(fixtures.get("slack", []))
    app.state.discord = _DiscordStore(fixtures.get("discord", []))
    app.state.ratelimiter = _RateLimiter(every, retry_after)

    @app.middleware("http")
    async def _rate_limit(request: Request, call_next):  # noqa: ANN001
        # Auth endpoints are never throttled (auth must succeed); only data.
        path = request.url.path
        if (path.endswith("/token") or path.endswith("/access_tokens")
                or path == "/healthz"):
            return await call_next(request)
        rl: _RateLimiter = request.app.state.ratelimiter
        if rl.should_429():
            return JSONResponse(
                {"error": {"code": 429, "message": "rateLimitExceeded"}},
                status_code=429,
                headers={"Retry-After": str(rl.retry_after_s)},
            )
        return await call_next(request)

    @app.get("/healthz")
    async def healthz() -> dict:
        return {"ok": True}

    app.include_router(_gmail_router())
    app.include_router(_github_router())
    app.include_router(_slack_router())
    app.include_router(_discord_router())
    return app


# =====================================================================
# Gmail router (reference vertical).
# =====================================================================
def _gmail_router() -> APIRouter:
    r = APIRouter()

    @r.post("/gmail/token")
    async def gmail_token(request: Request) -> JSONResponse:
        # DWD posts application/x-www-form-urlencoded (grant_type + assertion).
        # Parse manually to avoid a python-multipart dependency.
        from urllib.parse import parse_qs
        raw = (await request.body()).decode("utf-8", "replace")
        form = parse_qs(raw)
        assertion = (form.get("assertion") or [""])[0]
        sub = _decode_jwt_sub(assertion) or "unknown@spammer"
        return JSONResponse({
            "access_token": f"spam::{sub}",
            "expires_in": 3600,
            "token_type": "Bearer",
        })

    base = "/gmail/gmail/v1/users/me"

    @r.get(base + "/profile")
    async def profile(
        request: Request, authorization: str | None = Header(default=None),
    ) -> dict:
        email = _email_from_bearer(authorization) or "unknown@spammer"
        store: _MailboxStore = request.app.state.mailboxes
        return {
            "emailAddress": email,
            "historyId": str(store.get(email)["current_history_id"]),
        }

    @r.get(base + "/messages")
    async def messages_list(
        request: Request,
        authorization: str | None = Header(default=None),
        pageToken: str | None = None,  # noqa: N803 — Gmail param name
        maxResults: int = 100,  # noqa: N803
    ) -> dict:
        email = _email_from_bearer(authorization) or "unknown@spammer"
        store: _MailboxStore = request.app.state.mailboxes
        msgs = store.get(email)["messages"]
        # Single page (fixtures are small); return id+threadId stubs.
        return {
            "messages": [
                {"id": m["id"], "threadId": m["threadId"]} for m in msgs
            ],
            "resultSizeEstimate": len(msgs),
        }

    @r.get(base + "/messages/{message_id}")
    async def get_message(
        message_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        format: str = "metadata",  # noqa: A002 — Gmail param name
    ) -> JSONResponse:
        email = _email_from_bearer(authorization) or "unknown@spammer"
        store: _MailboxStore = request.app.state.mailboxes
        for m in store.get(email)["messages"]:
            if m["id"] == message_id:
                return JSONResponse(m)
        return JSONResponse({"error": {"code": 404, "message": "not found"}},
                            status_code=404)

    @r.get(base + "/history")
    async def history_list(
        request: Request,
        authorization: str | None = Header(default=None),
        startHistoryId: str = "0",  # noqa: N803
        pageToken: str | None = None,  # noqa: N803
    ) -> dict:
        email = _email_from_bearer(authorization) or "unknown@spammer"
        store: _MailboxStore = request.app.state.mailboxes
        mb = store.get(email)
        events = mb.get("history_events", [])
        added = [
            {"id": str(i), "messagesAdded": [{"message": {"id": e["message_id"]}}]}
            for i, e in enumerate(events)
            if str(e.get("history_id", "0")) > str(startHistoryId)
        ]
        return {"history": added, "historyId": str(mb["current_history_id"])}

    @r.post(base + "/watch")
    async def watch() -> dict:
        return {"historyId": "1000", "expiration": "9999999999999"}

    @r.post(base + "/stop")
    async def stop() -> dict:
        return {}

    return r


# =====================================================================
# GitHub router.
# =====================================================================
def _github_etag(full_name: str, event_type: str, n: int) -> str:
    # Same shape MockGithubClient emits, so the reconciler's conditional
    # fast-path behaves identically against the spammer.
    return f'W/"{full_name}:{event_type}:v{n}"'


def _github_router() -> APIRouter:
    r = APIRouter()

    @r.post("/github/app/installations/{installation_id}/access_tokens")
    async def mint_token(installation_id: str) -> JSONResponse:
        # GitHub returns 201 with {token, expires_at}. The token encodes
        # the installation so /installation/repositories can resolve it.
        return JSONResponse(
            {
                "token": f"spam-gh::{installation_id}",
                "expires_at": "2099-01-01T00:00:00Z",
            },
            status_code=201,
        )

    @r.get("/github/installation/repositories")
    async def installation_repositories(
        request: Request,
        authorization: str | None = Header(default=None),
        per_page: int = 30,
        page: int = 1,
    ) -> dict:
        store: _GithubStore = request.app.state.github
        token = _bearer(authorization) or ""
        inst = token[len("spam-gh::"):] if token.startswith("spam-gh::") else None
        full_names = store.repos_for_install(inst)
        start = (page - 1) * per_page
        page_repos = full_names[start:start + per_page]
        return {
            "total_count": len(full_names),
            "repository_selection": "selected",
            "repositories": [{"full_name": fn} for fn in page_repos],
        }

    @r.get("/github/repos/{owner}/{repo}/{collection}")
    async def repo_events(
        owner: str,
        repo: str,
        collection: str,
        request: Request,
        per_page: int = 30,
        page: int = 1,
    ) -> JSONResponse:
        # collection ∈ {"issues","pulls"} → fixture event_type.
        event_type = "issues" if collection == "issues" else "pull_requests"
        store: _GithubStore = request.app.state.github
        full_name = f"{owner}/{repo}"
        events = store.events(full_name, event_type)
        etag = _github_etag(full_name, event_type, len(events))

        inm = request.headers.get("If-None-Match")
        if inm and inm == etag:
            return JSONResponse(None, status_code=304, headers={"ETag": etag})

        start = (page - 1) * per_page
        end = start + per_page
        page_events = events[start:end]
        headers = {"ETag": etag}
        if end < len(events):
            # Minimal Link header with a rel="next" the client parses.
            base = str(request.url).split("?")[0]
            headers["Link"] = (
                f'<{base}?per_page={per_page}&page={page + 1}>; rel="next"'
            )
        return JSONResponse(page_events, headers=headers)

    return r


# =====================================================================
# Slack router (ok=true wrapped Web API).
# =====================================================================
def _slack_router() -> APIRouter:
    r = APIRouter()

    @r.get("/slack/api/conversations.list")
    async def conversations_list(
        request: Request,
        authorization: str | None = Header(default=None),
        types: str = "public_channel",
        limit: int = 1000,
    ) -> dict:
        store: _SlackStore = request.app.state.slack
        token = _bearer(authorization) or ""
        team = (
            token[len("spam-slack::"):]
            if token.startswith("spam-slack::") else None
        )
        return {"ok": True, "channels": store.channels(team)}

    @r.get("/slack/api/conversations.history")
    async def conversations_history(
        request: Request,
        channel: str,
        authorization: str | None = Header(default=None),
        cursor: str | None = None,
        oldest: str | None = None,
        limit: int = 10,
    ) -> dict:
        store: _SlackStore = request.app.state.slack
        msgs = store.messages(channel)
        ordered = sorted(msgs, key=lambda m: m["ts"], reverse=True)
        if oldest is not None:
            ordered = [m for m in ordered if float(m["ts"]) > float(oldest)]
        start = int(cursor) if cursor else 0
        end = start + limit
        page = ordered[start:end]
        body: dict[str, Any] = {"ok": True, "messages": page}
        if end < len(ordered):
            body["response_metadata"] = {"next_cursor": str(end)}
        return body

    return r


# =====================================================================
# Discord router.
# =====================================================================
def _discord_router() -> APIRouter:
    r = APIRouter()
    base = "/discord/api/v10"

    @r.get(base + "/gateway/bot")
    async def gateway_bot() -> dict:
        # The live Gateway is WSS; point the real DiscordGatewayClient at
        # the local WSS mock (services/synthetic/spammer/discord_gateway.py)
        # by setting SPAMMER_DISCORD_WSS_URL to its ws://host:port.
        return {
            "url": os.environ.get("SPAMMER_DISCORD_WSS_URL", "ws://127.0.0.1:0"),
            "shards": 1,
            "session_start_limit": {
                "total": 1000, "remaining": 999,
                "reset_after": 0, "max_concurrency": 1,
            },
        }

    @r.get(base + "/users/@me/guilds")
    async def list_guilds(
        request: Request, authorization: str | None = Header(default=None),
    ) -> JSONResponse:
        store: _DiscordStore = request.app.state.discord
        # The app-level bot is in every guild, so the real endpoint returns
        # all. For multi-tenant routing the spammer-mode client carries a
        # `spam-bot::<guild>` token so we scope to that tenant's guild.
        token = _bearer(authorization) or ""
        if token.startswith("spam-bot::"):
            gid = token[len("spam-bot::"):]
            scoped = [g for g in store.guilds() if g["id"] == gid]
            if scoped:
                return JSONResponse(scoped)
        return JSONResponse(store.guilds())

    @r.get(base + "/guilds/{guild_id}/channels")
    async def list_guild_channels(
        guild_id: str, request: Request,
    ) -> JSONResponse:
        store: _DiscordStore = request.app.state.discord
        return JSONResponse(store.channels(guild_id))

    @r.get(base + "/channels/{channel_id}/messages")
    async def get_messages(
        channel_id: str,
        request: Request,
        before: str | None = None,
        after: str | None = None,
        limit: int = 100,
    ) -> JSONResponse:
        store: _DiscordStore = request.app.state.discord
        msgs = store.messages(channel_id)
        ordered = sorted(msgs, key=lambda m: int(m["id"]), reverse=True)
        if before is not None:
            ordered = [m for m in ordered if int(m["id"]) < int(before)]
        if after is not None:
            ordered = [m for m in ordered if int(m["id"]) > int(after)]
        return JSONResponse(ordered[:limit])

    return r


def main() -> None:
    """Run the spammer as a real server: `python -m
    services.synthetic.spammer.server` (reads SPAMMER_PORT, default 9100,
    SPAMMER_FIXTURE_REGISTRY for seeded fixtures, SPAMMER_WORKERS for
    concurrency).

    Multiple workers (default 4) keep a single async worker from wedging
    under fan-out backfill load. Workers are safe: every worker rebuilds
    identical, read-only state from the same fixture registry, so any
    worker can serve any request deterministically."""
    import uvicorn

    port = int(os.environ.get("SPAMMER_PORT", "9100"))
    workers = int(os.environ.get("SPAMMER_WORKERS", "4"))
    log_level = os.environ.get("SPAMMER_LOG_LEVEL", "warning")
    if workers > 1:
        # Workers require an import string + factory (uvicorn spawns
        # subprocesses that re-import and call the factory per worker).
        uvicorn.run(
            "services.synthetic.spammer.server:build_spammer_app",
            factory=True, host="127.0.0.1", port=port,
            workers=workers, log_level=log_level,
        )
    else:
        uvicorn.run(build_spammer_app(), host="127.0.0.1", port=port,
                    log_level=log_level)


__all__ = ["build_spammer_app", "main"]


if __name__ == "__main__":
    main()
