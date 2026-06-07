"""GooglePushGenerator — synthetic Calendar/Drive push notifications.

Drives the PRODUCTION Google push ingress in-process for **google_calendar**
and **google_drive**, the way `gmail_pubsub.py` does for Gmail:

  Generator → advance an in-process live mock (one fresh delta item)
            → httpx.AsyncClient(ASGITransport(google push app))
            → POST /webhooks/google_calendar/push  (X-Goog-* headers)
                 (or /webhooks/google_drive/push)
            → resolve_push() (channel-id lookup + constant-time token verify)
            → drain_push() → drain_live() → the REAL fetcher
                 (`_open_calendar_client` / `_open_drive_client` seam, here
                  monkeypatched to the live mock)
            → core.ingest()  (INLINE observation write — the Google push path
                 is inherently inline in production: drain_live calls
                 core.ingest directly, NOT the Kafka cutover).

So a Google push observation lands directly in `observations` during backfill —
the "live received while backfill in progress" property holds via the real
inline drain. The cursor on the watch row advances exactly as production.

Watch-row seeding (done by the runner via `composition.seed_live_google_watches`):
a dedicated calendar / drive_target row per tenant with `watch_channel_id` +
`watch_token` + a warm cursor — distinct from the backfill rows so the live
delta never perturbs backfill's corpus.

Each `simulate_push` mints a brand-new event / file (unique id + a current
partition-window timestamp), so N pushes ⇒ N distinct observations.
"""
from __future__ import annotations

import logging
import time
from contextlib import AsyncExitStack
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import httpx
from fastapi import FastAPI

from services.ingest.synthetic.mock_clients.google_calendar import (
    MockGoogleCalendarClient,
)
from services.ingest.synthetic.mock_clients.google_drive import MockGoogleDriveClient


log = logging.getLogger(__name__)

_DOC_MIME = "application/vnd.google-apps.document"
# Current-window base (2026-06-xx) so live timestamps are inside the
# observations partition coverage and distinct from the 2026-01 backfill window.
_LIVE_BASE_MS = 1781000000000


def _iso(ms: int) -> str:
    return (
        time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000.0))
        + f".{ms % 1000:03d}Z"
    )


@dataclass
class GooglePushResult:
    source: str
    http_status: int
    ingested: int
    external_hint: str
    tenant_id: UUID | None = None


class GooglePushGenerator:
    """Drives Calendar + Drive push ingress for all live targets.

    A single shared `MockGoogleCalendarClient` / `MockGoogleDriveClient` serves
    every tenant's live delta (keyed by calendar_id / drive_id); the
    `_open_*_client` fetcher seams are monkeypatched (in THIS process only) to
    return them. Backfill runs in subprocesses with their own seam patch, so the
    two never collide.
    """

    def __init__(self, *, app: FastAPI, pool: Any) -> None:
        self._app = app
        self._pool = pool
        self._exit_stack = AsyncExitStack()
        self._client: httpx.AsyncClient | None = None
        self._seq = 0
        # Live mocks (delta-only; the backfill corpus lives in the subprocess).
        self._cal_mock = MockGoogleCalendarClient(
            fixture={"events": {}, "delta": {}, "page_size": 250},
        )
        self._drive_mock = MockGoogleDriveClient(
            fixture={"targets": [], "page_size": 200},
        )
        self._drive_targets: dict[str, dict[str, Any]] = {}
        self._patches: list[tuple[Any, str, Any]] = []

    async def __aenter__(self) -> "GooglePushGenerator":
        self._client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self._app),
            base_url="http://live-google",
        )
        await self._exit_stack.enter_async_context(self._client)
        # Monkeypatch the fetcher client seams to the in-process live mocks.
        from services.ingest.ingestion.fetchers import google_calendar as _cal_f
        from services.ingest.ingestion.fetchers import google_drive as _drv_f

        async def _open_cal(_install):  # noqa: ANN001, ANN202
            return self._cal_mock, _noop

        async def _open_drv(_install):  # noqa: ANN001, ANN202
            return self._drive_mock, _noop

        async def _noop() -> None:
            return None

        for mod, name, repl in (
            (_cal_f, "_open_calendar_client", _open_cal),
            (_drv_f, "_open_drive_client", _open_drv),
        ):
            self._patches.append((mod, name, getattr(mod, name)))
            setattr(mod, name, repl)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        for mod, name, original in reversed(self._patches):
            setattr(mod, name, original)
        self._patches.clear()
        await self._exit_stack.aclose()

    # ---- live-delta minting ----
    def _mint_calendar_event(self, calendar_id: str) -> str:
        self._seq += 1
        ms = _LIVE_BASE_MS + self._seq * 1000
        eid = f"live-{calendar_id}-{self._seq}"
        event = {
            "kind": "calendar#event",
            "id": eid,
            "status": "confirmed",
            "summary": f"live event {self._seq}",
            "eventType": "default",
            "organizer": {"email": calendar_id},
            "creator": {"email": calendar_id},
            "start": {"dateTime": _iso(ms)},
            "end": {"dateTime": _iso(ms + 1800_000)},
            "updated": _iso(ms),
        }
        # Replace the delta for this calendar with exactly the new event.
        self._cal_mock._fixture.setdefault("delta", {})[calendar_id] = [event]
        return eid

    def _mint_drive_change(self, drive_id: str, drive_kind: str) -> str:
        self._seq += 1
        ms = _LIVE_BASE_MS + self._seq * 1000
        fid = f"live-{drive_id}-{self._seq}"
        change = {
            "file": {
                "id": fid,
                "name": f"live-doc-{self._seq}",
                "mimeType": _DOC_MIME,
                "version": str(self._seq),
                "modifiedTime": _iso(ms),
                "createdTime": _iso(ms),
            },
            "time": _iso(ms),
        }
        tgt = self._drive_targets.get(drive_id)
        if tgt is None:
            tgt = {
                "drive_id": drive_id, "drive_kind": drive_kind,
                "files": [], "comments": {}, "revisions": {},
                "extracted_text": {}, "start_page_token": "live-start",
            }
            self._drive_targets[drive_id] = tgt
            self._drive_mock._fixture.setdefault("targets", []).append(tgt)
            self._drive_mock._by_drive[drive_id] = tgt
        tgt["changes"] = [change]
        return f"gdrive:{fid}:{self._seq}"

    async def simulate_push(self, *, target: "Any") -> GooglePushResult:
        """Mint one fresh delta item and POST a verified push ping."""
        assert self._client is not None
        if target.source == "google_calendar":
            eid = self._mint_calendar_event(target.gcal_calendar_id)
            path = "/webhooks/google_calendar/push"
            channel_id, token = target.gcal_channel_id, target.gcal_watch_token
            hint = f"gcal:{target.gcal_calendar_id}:{eid}"
        else:
            hint = self._mint_drive_change(target.gdrive_drive_id, target.gdrive_kind)
            path = "/webhooks/google_drive/push"
            channel_id, token = target.gdrive_channel_id, target.gdrive_watch_token
        response = await self._client.post(
            path,
            content=b"",
            headers={
                "X-Goog-Channel-ID": channel_id,
                "X-Goog-Channel-Token": token,
                "X-Goog-Resource-State": "exists",
                "X-Goog-Resource-ID": f"res-{channel_id}",
            },
        )
        try:
            data = response.json()
        except Exception:  # noqa: BLE001
            data = {}
        return GooglePushResult(
            source=target.source,
            http_status=response.status_code,
            ingested=int(data.get("ingested", 0)) if isinstance(data, dict) else 0,
            external_hint=hint,
            tenant_id=getattr(target, "tenant_id", None),
        )


__all__ = ["GooglePushGenerator", "GooglePushResult"]
